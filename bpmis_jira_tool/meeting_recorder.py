from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo

import google_auth_httplib2
import httplib2
from googleapiclient.discovery import build

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.gmail_sender import credentials_from_payload, send_gmail_message


CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"
MEETING_RECORDER_PROMPT_VERSION = "v2_audio_only_english_minutes"
MEETING_RECORD_STATUSES = {
    "scheduled",
    "recording",
    "recorded",
    "processing",
    "completed",
    "failed",
    "deleted",
}
MEETING_LINK_PATTERN = re.compile(
    r"https?://(?:meet\.google\.com/[a-z0-9-]+|(?:[\w.-]+\.)?zoom\.us/[^\s<>\"')]+)",
    re.IGNORECASE,
)
MEETING_AUDIO_PREFLIGHT_SECONDS = 5
MEETING_AUDIO_LOW_MEAN_DB = -45.0
MEETING_TRANSCRIPT_SEGMENT_SECONDS = 60
MEETING_PLAYBACK_PROFILE = "browser_compatible_v1"
MEETING_PLAYBACK_AUDIO_CHANNELS = 2
MEETING_PLAYBACK_AUDIO_SAMPLE_RATE = 48000
MEETING_RECORDING_MODE_AUDIO_ONLY = "audio_only"
MEETING_RECORDING_MODE_SCREEN_AUDIO = "screen_audio"
MEETING_TRANSCRIPT_REPETITIVE_DOMINANT_RATIO = 0.6
MEETING_TRANSCRIPT_REPETITIVE_UNIQUE_RATIO = 0.35
MEETING_TRANSCRIPT_EXPECTED_LANGUAGES = {
    "auto",
    "en",
    "zh",
    "cn",
    "yue",
    "ja",
    "ko",
    "ms",
    "id",
    "ta",
    "hi",
    "th",
    "vi",
    "fil",
    "tl",
    "es",
    "fr",
    "de",
}


@dataclass(frozen=True)
class MeetingRecorderConfig:
    ffmpeg_bin: str = "ffmpeg"
    video_input: str = "Capture screen 0"
    audio_input: str = "default"
    video_fps: int = 15
    video_max_width: int = 1920
    video_max_height: int = 1080
    avfoundation_pixel_format: str = "bgr0"
    screen_preflight_timeout_seconds: int = 20
    frame_interval_seconds: int = 60
    vision_model: str = "gpt-4.1-mini"
    transcribe_provider: str = "whisper_cpp"
    whisper_cpp_bin: str = "whisper-cli"
    whisper_model: str = "~/.cache/whisper.cpp/ggml-medium.bin"
    whisper_language: str = "auto"


def build_calendar_api_service(credentials: Any, *, cache_discovery: bool = False) -> Any:
    http = httplib2.Http(timeout=20)
    authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)
    return build("calendar", "v3", http=authed_http, cache_discovery=cache_discovery)


def extract_meeting_links(*values: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for value in values:
        for match in MEETING_LINK_PATTERN.findall(str(value or "")):
            normalized = match.rstrip(".,;")
            key = normalized.lower()
            if key not in seen:
                seen.add(key)
                links.append(normalized)
    return links


def meeting_platform_from_link(link: str) -> str:
    lowered = str(link or "").lower()
    if "meet.google.com" in lowered:
        return "google_meet"
    if "zoom.us" in lowered:
        return "zoom"
    return "unknown"


def normalize_calendar_event(event: dict[str, Any]) -> dict[str, Any] | None:
    meeting_links = extract_meeting_links(
        event.get("hangoutLink") or "",
        event.get("location") or "",
        event.get("description") or "",
        json.dumps(event.get("conferenceData") or {}, ensure_ascii=False),
    )
    if not meeting_links:
        return None

    start_payload = event.get("start") if isinstance(event.get("start"), dict) else {}
    end_payload = event.get("end") if isinstance(event.get("end"), dict) else {}
    attendees = []
    for attendee in event.get("attendees") or []:
        if not isinstance(attendee, dict):
            continue
        email = str(attendee.get("email") or "").strip().lower()
        name = str(attendee.get("displayName") or "").strip()
        if email or name:
            attendees.append({"email": email, "name": name})

    first_link = meeting_links[0]
    return {
        "calendar_event_id": str(event.get("id") or "").strip(),
        "title": str(event.get("summary") or "Untitled meeting").strip(),
        "start": str(start_payload.get("dateTime") or start_payload.get("date") or ""),
        "end": str(end_payload.get("dateTime") or end_payload.get("date") or ""),
        "meeting_link": first_link,
        "meeting_links": meeting_links,
        "platform": meeting_platform_from_link(first_link),
        "organizer": event.get("organizer") if isinstance(event.get("organizer"), dict) else {},
        "attendees": attendees,
    }


class GoogleCalendarMeetingService:
    def __init__(self, credentials: Any, *, calendar_service: Any | None = None) -> None:
        self.calendar_service = calendar_service or build_calendar_api_service(credentials)

    def upcoming_meetings(self, *, now: datetime | None = None, days: int = 2, max_results: int = 20) -> list[dict[str, Any]]:
        start = now or datetime.now(timezone.utc)
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        end = start + timedelta(days=max(1, min(days, 14)))
        payload = (
            self.calendar_service.events()
            .list(
                calendarId="primary",
                timeMin=start.isoformat(),
                timeMax=end.isoformat(),
                singleEvents=True,
                orderBy="startTime",
                maxResults=max(1, min(max_results, 50)),
            )
            .execute()
        )
        meetings = []
        for event in payload.get("items") or []:
            if isinstance(event, dict):
                normalized = normalize_calendar_event(event)
                if normalized:
                    meetings.append(normalized)
        return meetings


def meeting_reminder_suppression_key(meeting: dict[str, Any], *, timezone_name: str = "Asia/Singapore") -> str:
    event_id = str(meeting.get("calendar_event_id") or meeting.get("id") or meeting.get("meeting_link") or meeting.get("title") or "").strip()
    start_at = _parse_meeting_datetime(str(meeting.get("start") or meeting.get("scheduled_start") or ""), timezone_name=timezone_name)
    if start_at is None:
        date_key = datetime.now(ZoneInfo(timezone_name)).strftime("%Y%m%d")
    else:
        date_key = start_at.astimezone(ZoneInfo(timezone_name)).strftime("%Y%m%d")
    safe_event = re.sub(r"[^a-zA-Z0-9_.:-]", "-", event_id)[:160] or "meeting"
    return f"{date_key}:{safe_event}"


def reminder_eligible_meetings(
    meetings: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    timezone_name: str = "Asia/Singapore",
    workday_start_hour: int = 9,
    workday_end_hour: int = 20,
    lead_seconds: int = 120,
    grace_seconds: int = 600,
) -> list[dict[str, Any]]:
    local_tz = ZoneInfo(timezone_name)
    current = now or datetime.now(local_tz)
    if current.tzinfo is None:
        current = current.replace(tzinfo=local_tz)
    current = current.astimezone(local_tz)
    eligible: list[dict[str, Any]] = []
    for meeting in meetings:
        if str(meeting.get("platform") or "").strip() not in {"google_meet", "zoom"}:
            continue
        start_at = _parse_meeting_datetime(str(meeting.get("start") or meeting.get("scheduled_start") or ""), timezone_name=timezone_name)
        if start_at is None:
            continue
        local_start = start_at.astimezone(local_tz)
        if not (workday_start_hour <= local_start.hour < workday_end_hour):
            continue
        seconds_until_start = int((local_start - current).total_seconds())
        if seconds_until_start > lead_seconds or seconds_until_start < -grace_seconds:
            continue
        item = dict(meeting)
        item["start"] = str(meeting.get("start") or meeting.get("scheduled_start") or "")
        item["suppression_key"] = meeting_reminder_suppression_key(item, timezone_name=timezone_name)
        item["seconds_until_start"] = seconds_until_start
        eligible.append(item)
    eligible.sort(key=lambda item: int(item.get("seconds_until_start") or 0))
    return eligible


class MeetingRecordStore:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.records_dir = self.root_dir / "records"
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def create_record(
        self,
        *,
        owner_email: str,
        title: str,
        platform: str,
        meeting_link: str,
        calendar_event_id: str = "",
        scheduled_start: str = "",
        scheduled_end: str = "",
        attendees: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        record_id = f"meeting-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{os.urandom(3).hex()}"
        record_dir = self.record_dir(record_id)
        record_dir.mkdir(parents=True, exist_ok=True)
        now = _utc_now()
        record = {
            "record_id": record_id,
            "owner_email": str(owner_email or "").strip().lower(),
            "title": str(title or "Untitled meeting").strip() or "Untitled meeting",
            "platform": str(platform or meeting_platform_from_link(meeting_link)).strip() or "unknown",
            "meeting_link": str(meeting_link or "").strip(),
            "calendar_event_id": str(calendar_event_id or "").strip(),
            "scheduled_start": str(scheduled_start or "").strip(),
            "scheduled_end": str(scheduled_end or "").strip(),
            "attendees": attendees or [],
            "status": "scheduled",
            "recording_started_at": "",
            "recording_stopped_at": "",
            "created_at": now,
            "updated_at": now,
            "media": {},
            "diagnostics_snapshot": {},
            "audio_preflight": {},
            "recording_health": {},
            "transcript": {"status": "pending", "text": "", "chunks": []},
            "visual_evidence": [],
            "minutes": {"status": "pending", "markdown": "", "prompt_version": MEETING_RECORDER_PROMPT_VERSION},
            "email": {"status": "pending", "sent_at": "", "message_id": ""},
            "error": "",
        }
        self.save_record(record)
        return record

    def record_dir(self, record_id: str) -> Path:
        return self.records_dir / _safe_record_id(record_id)

    def metadata_path(self, record_id: str) -> Path:
        return self.record_dir(record_id) / "metadata.json"

    def get_record(self, record_id: str) -> dict[str, Any]:
        path = self.metadata_path(record_id)
        if not path.exists():
            raise ToolError("Meeting record was not found.")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ToolError("Meeting record metadata is unreadable.") from error
        if not isinstance(payload, dict):
            raise ToolError("Meeting record metadata is invalid.")
        return payload

    def save_record(self, record: dict[str, Any]) -> None:
        record_id = str(record.get("record_id") or "").strip()
        if not record_id:
            raise ToolError("Meeting record id is missing.")
        status = str(record.get("status") or "").strip()
        if status not in MEETING_RECORD_STATUSES:
            raise ToolError(f"Invalid meeting record status: {status}")
        record = dict(record)
        record["updated_at"] = _utc_now()
        path = self.metadata_path(record_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.tmp")
        with self._lock:
            temp_path.write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
            temp_path.replace(path)

    def list_records(self, *, owner_email: str) -> list[dict[str, Any]]:
        owner = str(owner_email or "").strip().lower()
        records: list[dict[str, Any]] = []
        for metadata_path in sorted(self.records_dir.glob("*/metadata.json"), reverse=True):
            try:
                payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            if owner and str(payload.get("owner_email") or "").strip().lower() != owner:
                continue
            if payload.get("status") == "deleted":
                continue
            records.append(payload)
        return records

    def delete_record(self, *, record_id: str, owner_email: str) -> dict[str, Any]:
        record = self.get_record(record_id)
        _assert_record_owner(record, owner_email)
        record["status"] = "deleted"
        self.save_record(record)
        record_dir = self.record_dir(record_id)
        if record_dir.exists():
            for child in record_dir.iterdir():
                if child.name != "metadata.json":
                    if child.is_dir():
                        shutil.rmtree(child, ignore_errors=True)
                    else:
                        child.unlink(missing_ok=True)
        return record


class MeetingRecorderRuntime:
    def __init__(self, *, store: MeetingRecordStore, config: MeetingRecorderConfig) -> None:
        self.store = store
        self.config = config
        self._processes: dict[str, subprocess.Popen[str]] = {}
        self._lock = threading.Lock()

    def diagnostics(self) -> dict[str, Any]:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        whisper_path = _resolve_whisper_cpp_bin(self.config.whisper_cpp_bin)
        devices = _avfoundation_devices(ffmpeg_path)
        effective_audio_input = _effective_audio_input(self.config.audio_input, devices.get("audio_devices") or [])
        audio_status = _audio_capture_status(effective_audio_input, devices.get("audio_devices") or [])
        return {
            "ffmpeg_configured": bool(ffmpeg_path),
            "ffmpeg_path": ffmpeg_path or "",
            "video_input": self.config.video_input,
            "audio_input": effective_audio_input,
            "configured_audio_input": self.config.audio_input,
            "video_fps": _bounded_int(self.config.video_fps, default=15, minimum=5, maximum=30),
            "video_max_width": _bounded_int(self.config.video_max_width, default=1920, minimum=640, maximum=2560),
            "video_max_height": _bounded_int(self.config.video_max_height, default=1080, minimum=360, maximum=1600),
            "avfoundation_pixel_format": _safe_avfoundation_pixel_format(self.config.avfoundation_pixel_format),
            "audio_devices": devices.get("audio_devices") or [],
            "video_devices": devices.get("video_devices") or [],
            **audio_status,
            "audio_signal_verified": False,
            "audio_signal_note": "Signal is verified only during the recording preflight because device presence alone does not prove Zoom output or microphone audio is flowing.",
            "frame_interval_seconds": self.config.frame_interval_seconds,
            "transcribe_provider": self.config.transcribe_provider,
            "whisper_cpp_configured": bool(whisper_path),
            "whisper_cpp_bin": whisper_path or "",
            "whisper_model": str(Path(self.config.whisper_model).expanduser()),
            "whisper_model_exists": Path(self.config.whisper_model).expanduser().exists(),
            "whisper_language": self.config.whisper_language,
            "mac_permissions_note": "macOS Screen Recording and Microphone permissions are required for the Python/terminal process that runs ffmpeg.",
            "system_audio_note": "Install/configure a virtual audio device such as BlackHole and set MEETING_RECORDER_AUDIO_INPUT when Zoom/Meet system audio is not captured.",
            "onsite_note": "For in-person meetings, leave the meeting link blank and record the Mac microphone. Screen video will only show what is visible on this Mac.",
        }

    def start_recording(
        self,
        *,
        owner_email: str,
        title: str,
        platform: str,
        meeting_link: str,
        recording_mode: str = "",
        calendar_event_id: str = "",
        scheduled_start: str = "",
        scheduled_end: str = "",
        attendees: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            raise ConfigError("ffmpeg is required for local meeting recording. Install ffmpeg or set MEETING_RECORDER_FFMPEG_BIN.")
        devices = _avfoundation_devices(ffmpeg_path)
        audio_input = _effective_audio_input(self.config.audio_input, devices.get("audio_devices") or [])
        audio_status = _audio_capture_status(audio_input, devices.get("audio_devices") or [])
        preflight = self._audio_preflight(ffmpeg_path=ffmpeg_path, audio_input=audio_input)
        requested_mode = str(recording_mode or "").strip().lower().replace("-", "_")
        effective_mode = _normalize_recording_mode(recording_mode=recording_mode, meeting_link=meeting_link)
        screen_preflight: dict[str, Any] | None = None
        if effective_mode == MEETING_RECORDING_MODE_SCREEN_AUDIO:
            screen_preflight = self._screen_capture_preflight(ffmpeg_path=ffmpeg_path)
            if screen_preflight.get("status") != "ok":
                warning = screen_preflight.get("warning") or "Screen capture is unavailable."
                raise ToolError(
                    "Screen recording is required for Zoom/Google Meet links but is not available. "
                    f"{warning} Grant macOS Screen Recording permission to the process running the local agent, "
                    "verify MEETING_RECORDER_VIDEO_INPUT, then restart the local agent."
                )
        record = self.store.create_record(
            owner_email=owner_email,
            title=title,
            platform=platform,
            meeting_link=meeting_link,
            calendar_event_id=calendar_event_id,
            scheduled_start=scheduled_start,
            scheduled_end=scheduled_end,
            attendees=attendees,
        )
        record_dir = self.store.record_dir(record["record_id"])
        video_path = record_dir / "meeting.mp4"
        audio_path = record_dir / "meeting.wav"
        log_path = record_dir / "ffmpeg.log"
        record["audio_preflight"] = preflight
        record["diagnostics_snapshot"] = {
            "ffmpeg_path": ffmpeg_path,
            "video_input": self.config.video_input,
            "audio_input": audio_input,
            "requested_recording_mode": requested_mode or ("screen_audio" if meeting_link else "audio_only"),
            "recording_mode": effective_mode,
            "effective_recording_mode": effective_mode,
            "audio_capture_mode": audio_status.get("audio_capture_mode"),
            "audio_capture_label": audio_status.get("audio_capture_label"),
            "system_audio_configured": audio_status.get("system_audio_configured"),
            "audio_signal_verified": preflight.get("status") == "ok",
            "audio_devices": devices.get("audio_devices") or [],
        }
        if screen_preflight is not None:
            record["diagnostics_snapshot"]["screen_capture_status"] = screen_preflight.get("status") or "unknown"
            record["diagnostics_snapshot"]["screen_capture_warning"] = screen_preflight.get("warning", "")
        record["recording_health"] = {
            "status": "warning" if preflight.get("status") == "too_quiet" else preflight.get("status", "unknown"),
            "warning": preflight.get("warning", ""),
            "checked_at": preflight.get("checked_at", ""),
        }
        self.store.save_record(record)
        if effective_mode == MEETING_RECORDING_MODE_AUDIO_ONLY:
            command = _build_ffmpeg_audio_recording_command(
                ffmpeg_path=ffmpeg_path,
                audio_input=audio_input,
                audio_path=audio_path,
            )
        else:
            command = _build_ffmpeg_recording_command(
                ffmpeg_path=ffmpeg_path,
                video_input=self.config.video_input,
                audio_input=audio_input,
                video_path=video_path,
                video_fps=self.config.video_fps,
                video_max_width=self.config.video_max_width,
                video_max_height=self.config.video_max_height,
                avfoundation_pixel_format=self.config.avfoundation_pixel_format,
            )
        try:
            log_handle = log_path.open("w", encoding="utf-8")
            process = subprocess.Popen(  # noqa: S603
                command,
                cwd=str(record_dir),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=log_handle,
                text=True,
                start_new_session=True,
            )
            log_handle.close()
        except OSError as error:
            record["status"] = "failed"
            record["error"] = str(error)
            self.store.save_record(record)
            raise ToolError(f"Could not start local meeting recorder: {error}") from error

        time.sleep(0.5)
        if process.poll() is not None:
            stderr = log_path.read_text(encoding="utf-8")[-1200:] if log_path.exists() else ""
            record["status"] = "failed"
            record["error"] = stderr or "ffmpeg exited before recording started."
            self.store.save_record(record)
            raise ToolError(record["error"])

        with self._lock:
            self._processes[record["record_id"]] = process
        record["status"] = "recording"
        record["recording_started_at"] = _utc_now()
        media = {
            "recording_mode": effective_mode,
            "recorder_command": _redact_command(command),
        }
        if effective_mode == MEETING_RECORDING_MODE_AUDIO_ONLY:
            media.update(
                {
                    "audio_path": str(audio_path.relative_to(self.store.root_dir)),
                    "audio_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.wav",
                    "audio_format": "wav",
                    "audio_sample_rate": 16000,
                    "audio_channels": 1,
                }
            )
        else:
            media.update(
                {
                    "video_path": str(video_path.relative_to(self.store.root_dir)),
                    "video_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.mp4",
                    "playback_profile": MEETING_PLAYBACK_PROFILE,
                    "playback_audio_channels": MEETING_PLAYBACK_AUDIO_CHANNELS,
                    "playback_audio_sample_rate": MEETING_PLAYBACK_AUDIO_SAMPLE_RATE,
                }
            )
        record["media"] = media
        self.store.save_record(record)
        return record

    def stop_recording(self, *, record_id: str, owner_email: str) -> dict[str, Any]:
        record = self.store.get_record(record_id)
        _assert_record_owner(record, owner_email)
        with self._lock:
            process = self._processes.pop(record_id, None)
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except OSError:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        record["status"] = "recorded"
        record["recording_stopped_at"] = _utc_now()
        record["recording_health"] = self._recording_health(record)
        self.store.save_record(record)
        return record

    def repair_video_playback(self, *, record_id: str, owner_email: str) -> dict[str, Any]:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            raise ConfigError("ffmpeg is required to repair meeting video playback.")
        record = self.store.get_record(record_id)
        _assert_record_owner(record, owner_email)
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        if str(media.get("recording_mode") or "") == MEETING_RECORDING_MODE_AUDIO_ONLY:
            raise ToolError("Audio-only recordings do not have a video playback copy.")
        relative = str(media.get("video_path") or "").strip()
        if not relative:
            raise ToolError("Recorded meeting video is missing.")
        record_dir = self.store.record_dir(record_id)
        source_path = (self.store.root_dir / relative).resolve()
        if not source_path.exists() or not source_path.is_file():
            raise ToolError("Recorded meeting video file was not found.")
        output_path = record_dir / "meeting.playback.mp4"
        command = _build_ffmpeg_playback_repair_command(
            ffmpeg_path=ffmpeg_path,
            source_path=source_path,
            output_path=output_path,
        )
        _run_command(command, "Could not repair meeting video playback.", timeout_seconds=7200)
        if not output_path.exists() or output_path.stat().st_size <= 0:
            raise ToolError("Meeting video playback repair produced no playable file.")
        media = dict(media)
        media.update(
            {
                "playback_video_path": str(output_path.relative_to(self.store.root_dir)),
                "playback_video_url": f"/meeting-recorder/assets/{record_id}/meeting.playback.mp4",
                "playback_repair_command": _redact_command(command),
                "playback_profile": MEETING_PLAYBACK_PROFILE,
                "playback_audio_channels": MEETING_PLAYBACK_AUDIO_CHANNELS,
                "playback_audio_sample_rate": MEETING_PLAYBACK_AUDIO_SAMPLE_RATE,
                "playback_repaired_at": _utc_now(),
                "playback_bytes": output_path.stat().st_size,
            }
        )
        record["media"] = media
        record["recording_health"] = {
            **(record.get("recording_health") if isinstance(record.get("recording_health"), dict) else {}),
            "status": "ok",
            "checked_at": _utc_now(),
            "playback_bytes": output_path.stat().st_size,
            "warning": "",
        }
        self.store.save_record(record)
        return record

    def _audio_preflight(self, *, ffmpeg_path: str, audio_input: str) -> dict[str, Any]:
        checked_at = _utc_now()
        with _temporary_path("meeting-audio-preflight", ".wav") as audio_path:
            command = [
                ffmpeg_path,
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-f",
                "avfoundation",
                "-i",
                f":{audio_input}",
                "-t",
                str(MEETING_AUDIO_PREFLIGHT_SECONDS),
                "-acodec",
                "pcm_s16le",
                "-ar",
                "16000",
                "-ac",
                "1",
                str(audio_path),
            ]
            try:
                _run_command(command, "Could not verify meeting audio input.", timeout_seconds=15)
                metrics = _audio_volume_metrics(audio_path)
            except Exception as error:  # noqa: BLE001
                return {
                    "status": "unavailable",
                    "checked_at": checked_at,
                    "duration_seconds": MEETING_AUDIO_PREFLIGHT_SECONDS,
                    "warning": f"Audio preflight failed: {error}",
                }
        status = "ok"
        warning = ""
        mean_db = metrics.get("mean_volume_db")
        if mean_db is None or mean_db <= MEETING_AUDIO_LOW_MEAN_DB:
            status = "too_quiet"
            warning = "Too quiet - Zoom/system audio or microphone may not be captured."
        return {
            "status": status,
            "checked_at": checked_at,
            "duration_seconds": MEETING_AUDIO_PREFLIGHT_SECONDS,
            "mean_volume_db": mean_db,
            "max_volume_db": metrics.get("max_volume_db"),
            "warning": warning,
        }

    def _screen_capture_preflight(self, *, ffmpeg_path: str) -> dict[str, Any]:
        checked_at = _utc_now()
        with _temporary_path("meeting-screen-preflight", ".mp4") as video_path:
            command = _build_ffmpeg_screen_preflight_command(
                ffmpeg_path=ffmpeg_path,
                video_input=self.config.video_input,
                video_path=video_path,
                avfoundation_pixel_format=self.config.avfoundation_pixel_format,
            )
            try:
                _run_command(
                    command,
                    "Could not verify meeting screen capture.",
                    timeout_seconds=max(8, int(self.config.screen_preflight_timeout_seconds or 20)),
                )
                if not video_path.exists() or video_path.stat().st_size <= 0:
                    raise ToolError("Screen preflight produced no video bytes.")
            except Exception as error:  # noqa: BLE001
                return {
                    "status": "unavailable",
                    "checked_at": checked_at,
                    "warning": f"Screen capture is unavailable. {error}",
                }
        return {"status": "ok", "checked_at": checked_at, "warning": ""}

    def _recording_health(self, record: dict[str, Any]) -> dict[str, Any]:
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        is_audio_only = str(media.get("recording_mode") or "") == MEETING_RECORDING_MODE_AUDIO_ONLY
        relative = str(media.get("audio_path" if is_audio_only else "video_path") or "").strip()
        noun = "audio" if is_audio_only else "video"
        if not relative:
            return {"status": "unknown", "checked_at": _utc_now(), "warning": f"Recorded {noun} path is missing."}
        media_path = (self.store.root_dir / relative).resolve()
        if not media_path.exists():
            return {"status": "missing", "checked_at": _utc_now(), "warning": f"Recorded {noun} file is missing."}
        byte_key = "audio_bytes" if is_audio_only else "video_bytes"
        return {
            "status": "ok" if media_path.stat().st_size > 0 else "warning",
            "checked_at": _utc_now(),
            byte_key: media_path.stat().st_size,
            "warning": "" if media_path.stat().st_size > 0 else f"Recorded {noun} file is empty.",
        }


class MeetingProcessingService:
    def __init__(
        self,
        *,
        store: MeetingRecordStore,
        config: MeetingRecorderConfig,
        text_client: Any,
        credential_store: Any | None = None,
        portal_base_url: str | None = None,
    ) -> None:
        self.store = store
        self.config = config
        self.text_client = text_client
        self.credential_store = credential_store
        self.portal_base_url = (portal_base_url or "").rstrip("/")

    def process_recording(self, *, record_id: str, owner_email: str) -> dict[str, Any]:
        record = self.store.get_record(record_id)
        _assert_record_owner(record, owner_email)
        if record.get("status") not in {"recorded", "failed", "completed"}:
            raise ToolError("Stop the recording before processing this meeting.")
        record["status"] = "processing"
        record["error"] = ""
        self.store.save_record(record)
        try:
            media = record.get("media") if isinstance(record.get("media"), dict) else {}
            is_audio_only = str(media.get("recording_mode") or "") == MEETING_RECORDING_MODE_AUDIO_ONLY
            video_path = None if is_audio_only else self._video_path(record)
            audio_path = self._recorded_audio_path(record) if is_audio_only else self._extract_audio(record, video_path)
            transcript = self._transcribe_audio(audio_path)
            transcript_text = str(transcript.get("text") or "").strip()
            snapshots = [] if video_path is None else self._extract_visual_evidence(record, video_path)
            minutes = self._generate_minutes(
                record=record,
                transcript_text=transcript_text,
                transcript_quality=transcript.get("quality") or {},
            )
            record["transcript"] = {
                "status": "completed",
                "text": transcript_text,
                "chunks": transcript.get("chunks") or [{"start_seconds": 0, "text": transcript_text}],
                "segments": transcript.get("segments") or [],
                "quality": transcript.get("quality") or {},
                "asset_url": f"/meeting-recorder/assets/{record_id}/transcript.txt",
            }
            (self.store.record_dir(record_id) / "transcript.txt").write_text(transcript_text, encoding="utf-8")
            record["visual_evidence"] = snapshots
            record["minutes"] = {
                "status": "completed",
                "markdown": minutes,
                "prompt_version": MEETING_RECORDER_PROMPT_VERSION,
                "asset_url": f"/meeting-recorder/assets/{record_id}/minutes.md",
            }
            (self.store.record_dir(record_id) / "minutes.md").write_text(minutes, encoding="utf-8")
            record["status"] = "completed"
            self.store.save_record(record)
            return record
        except Exception as error:  # noqa: BLE001
            record["status"] = "failed"
            record["error"] = str(error)
            self.store.save_record(record)
            raise

    def send_minutes_email(self, *, record_id: str, owner_email: str, recipient: str | None = None) -> dict[str, Any]:
        if self.credential_store is None:
            raise ConfigError("Google credential store is not configured.")
        record = self.store.get_record(record_id)
        _assert_record_owner(record, owner_email)
        minutes = str((record.get("minutes") or {}).get("markdown") or "").strip()
        if not minutes:
            raise ToolError("Meeting minutes are not available yet.")
        credentials_payload = self.credential_store.load(owner_email=owner_email)
        credentials = credentials_from_payload(credentials_payload)
        target = str(recipient or owner_email or "").strip().lower()
        if not target:
            raise ToolError("Email recipient is missing.")
        portal_url = self._record_url(record_id)
        subject = f"Meeting Minutes - {record.get('title') or 'Untitled meeting'}"
        body = f"{minutes}\n\nFull transcript and recording archive: {portal_url}\n"
        attachments = self._transcript_email_attachments(record_id=record_id, record=record)
        result = send_gmail_message(
            credentials=credentials,
            sender=owner_email,
            recipient=target,
            subject=subject,
            text_body=body,
            attachments=attachments,
        )
        record["email"] = {
            "status": "sent",
            "sent_at": _utc_now(),
            "message_id": str(result.get("id") or result.get("message_id") or ""),
            "recipient": target,
            "transcript_attached": bool(attachments),
        }
        self.store.save_record(record)
        return record["email"]

    def _record_url(self, record_id: str) -> str:
        path = f"/meeting-recorder?record={record_id}"
        return f"{self.portal_base_url}{path}" if self.portal_base_url else path

    def _transcript_email_attachments(self, *, record_id: str, record: dict[str, Any]) -> list[dict[str, Any]]:
        transcript_path = self.store.record_dir(record_id) / "transcript.txt"
        content = b""
        if transcript_path.exists() and transcript_path.is_file():
            content = transcript_path.read_bytes()
        if not content:
            transcript_text = str((record.get("transcript") or {}).get("text") or "").strip()
            if transcript_text:
                content = transcript_text.encode("utf-8")
        if not content:
            return []
        return [
            {
                "filename": "meeting-transcript.txt",
                "mime_type": "text/plain",
                "content": content,
            }
        ]

    def _video_path(self, record: dict[str, Any]) -> Path:
        relative = str((record.get("media") or {}).get("video_path") or "").strip()
        if not relative:
            raise ToolError("Recorded meeting video is missing.")
        video_path = (self.store.root_dir / relative).resolve()
        if not video_path.exists():
            raise ToolError("Recorded meeting video file was not found.")
        return video_path

    def _recorded_audio_path(self, record: dict[str, Any]) -> Path:
        relative = str((record.get("media") or {}).get("audio_path") or "").strip()
        if not relative:
            raise ToolError("Recorded meeting audio is missing.")
        audio_path = (self.store.root_dir / relative).resolve()
        if not audio_path.exists():
            raise ToolError("Recorded meeting audio file was not found.")
        return audio_path

    def _extract_audio(self, record: dict[str, Any], video_path: Path) -> Path:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            raise ConfigError("ffmpeg is required to extract meeting audio.")
        audio_path = self.store.record_dir(record["record_id"]) / "audio.wav"
        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(video_path),
            "-vn",
            "-acodec",
            "pcm_s16le",
            "-ar",
            "16000",
            "-ac",
            "1",
            str(audio_path),
        ]
        _run_command(command, "Could not extract audio from meeting video.")
        return audio_path

    def _transcribe_audio(self, audio_path: Path) -> dict[str, Any]:
        provider = str(self.config.transcribe_provider or "whisper_cpp").strip().lower()
        if provider != "whisper_cpp":
            raise ConfigError("Meeting Recorder transcription is restricted to whisper.cpp.")
        whisper_bin = _resolve_whisper_cpp_bin(self.config.whisper_cpp_bin)
        if not whisper_bin:
            raise ConfigError("whisper.cpp is required for transcription. Set MEETING_RECORDER_WHISPER_CPP_BIN to whisper-cli.")
        model_path = Path(self.config.whisper_model).expanduser()
        if not model_path.exists():
            raise ConfigError(f"whisper.cpp model was not found at {model_path}. Set MEETING_RECORDER_WHISPER_MODEL.")
        if ".en" in model_path.name.lower():
            raise ConfigError("Use a multilingual whisper.cpp model for Chinese/English mixed meetings, not a .en model.")
        duration = _audio_duration_seconds(audio_path)
        if duration > MEETING_TRANSCRIPT_SEGMENT_SECONDS:
            return self._transcribe_audio_by_segments(
                audio_path=audio_path,
                duration_seconds=duration,
                whisper_bin=whisper_bin,
                model_path=model_path,
            )
        output_base = self.store.record_dir(audio_path.parent.name) / "whisper-transcript"
        transcript = self._transcribe_audio_once(
            audio_path=audio_path,
            output_base=output_base,
            whisper_bin=whisper_bin,
            model_path=model_path,
            language=str(self.config.whisper_language or "auto").strip().lower(),
            offset_seconds=0,
        )
        segments, quality = self._transcript_quality(
            audio_path=audio_path,
            chunks=transcript["chunks"],
            detected_language=transcript["language"],
        )
        configured_language = str(self.config.whisper_language or "auto").strip().lower()
        if configured_language in {"", "auto"} and _should_retry_transcript_in_english(quality):
            english_base = self.store.record_dir(audio_path.parent.name) / "whisper-transcript-en"
            english_transcript = self._transcribe_audio_once(
                audio_path=audio_path,
                output_base=english_base,
                whisper_bin=whisper_bin,
                model_path=model_path,
                language="en",
                offset_seconds=0,
            )
            english_segments, english_quality = self._transcript_quality(
                audio_path=audio_path,
                chunks=english_transcript["chunks"],
                detected_language=english_transcript["language"],
            )
            english_quality["retry_language"] = "en"
            english_quality["original_language"] = quality.get("language") or transcript["language"]
            if _transcript_quality_score(english_quality) > _transcript_quality_score(quality):
                english_quality["warnings"] = [
                    *english_quality.get("warnings", []),
                    "Auto language transcription looked unreliable, so the accepted transcript was retried with English.",
                ]
                return {
                    "text": english_transcript["text"],
                    "chunks": english_transcript["chunks"],
                    "segments": english_segments,
                    "quality": english_quality,
                }
            quality["retry_language"] = "en"
            quality["warnings"] = [
                *quality.get("warnings", []),
                "Auto language transcription looked unreliable; English retry did not improve transcript quality.",
            ]
        return {"text": transcript["text"], "chunks": transcript["chunks"], "segments": segments, "quality": quality}

    def _transcribe_audio_once(
        self,
        *,
        audio_path: Path,
        output_base: Path,
        whisper_bin: str,
        model_path: Path,
        language: str,
        offset_seconds: float,
    ) -> dict[str, Any]:
        command = [
            whisper_bin,
            "-m",
            str(model_path),
            "-f",
            str(audio_path),
            "-otxt",
            "-osrt",
            "-of",
            str(output_base),
        ]
        if language:
            command.extend(["-l", language])
        completed = _run_command(command, "whisper.cpp transcription failed.", timeout_seconds=7200)
        detected_language = _parse_whisper_language(f"{completed.stderr}\n{completed.stdout}", fallback=language or "auto")
        transcript_path = output_base.with_suffix(".txt")
        if transcript_path.exists():
            transcript = transcript_path.read_text(encoding="utf-8").strip()
        else:
            transcript = (completed.stdout or "").strip()
        if not transcript:
            raise ToolError("whisper.cpp transcription produced no text.")
        chunks = _parse_srt_transcript(output_base.with_suffix(".srt"))
        if not chunks:
            chunks = [{"start_seconds": 0, "end_seconds": 0, "text": transcript}]
        if offset_seconds:
            for chunk in chunks:
                chunk["start_seconds"] = float(chunk.get("start_seconds") or 0) + offset_seconds
                chunk["end_seconds"] = float(chunk.get("end_seconds") or 0) + offset_seconds
        return {"text": transcript, "chunks": chunks, "language": detected_language}

    def _transcribe_audio_by_segments(
        self,
        *,
        audio_path: Path,
        duration_seconds: float,
        whisper_bin: str,
        model_path: Path,
    ) -> dict[str, Any]:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            raise ConfigError("ffmpeg is required to split meeting audio for mixed-language transcription.")
        record_dir = self.store.record_dir(audio_path.parent.name)
        segment_seconds = MEETING_TRANSCRIPT_SEGMENT_SECONDS
        all_chunks: list[dict[str, Any]] = []
        text_parts: list[str] = []
        segment_metadata: list[dict[str, Any]] = []
        configured_language = str(self.config.whisper_language or "auto").strip().lower()
        for index, start in enumerate(range(0, int(duration_seconds + segment_seconds - 1), segment_seconds)):
            remaining = max(0.0, duration_seconds - start)
            if remaining <= 0:
                continue
            current_duration = min(segment_seconds, remaining)
            segment_audio = record_dir / f"audio-segment-{index:04d}.wav"
            output_base = record_dir / f"whisper-segment-{index:04d}"
            extract_command = [
                ffmpeg_path,
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-ss",
                str(start),
                "-t",
                str(current_duration),
                "-i",
                str(audio_path),
                "-acodec",
                "pcm_s16le",
                "-ar",
                "16000",
                "-ac",
                "1",
                str(segment_audio),
            ]
            _run_command(extract_command, "Could not split meeting audio for transcription.")
            transcript = self._transcribe_audio_once(
                audio_path=segment_audio,
                output_base=output_base,
                whisper_bin=whisper_bin,
                model_path=model_path,
                language=configured_language or "auto",
                offset_seconds=float(start),
            )
            text_parts.append(str(transcript.get("text") or "").strip())
            chunks = transcript.get("chunks") or []
            all_chunks.extend(chunks)
            metrics = _audio_volume_metrics(audio_path, start_seconds=float(start), duration_seconds=current_duration)
            has_no_audio = any("[no audio]" in str(chunk.get("text") or "").lower() for chunk in chunks)
            low_audio = bool(metrics.get("low_audio")) or has_no_audio
            segment_metadata.append(
                {
                    "index": index,
                    "start_seconds": float(start),
                    "end_seconds": float(start + current_duration),
                    "language": transcript.get("language") or configured_language or "auto",
                    "language_confidence": None,
                    "mean_volume_db": metrics.get("mean_volume_db"),
                    "max_volume_db": metrics.get("max_volume_db"),
                    "quality": "low_audio" if low_audio else "ok",
                    "possible_missed_speech": low_audio,
                    "chunk_count": len(chunks),
                }
            )
        text = "\n".join(part for part in text_parts if part).strip()
        if not text:
            raise ToolError("whisper.cpp transcription produced no text.")
        quality = self._quality_from_segments(segment_metadata, chunks=all_chunks)
        return {"text": text, "chunks": all_chunks, "segments": segment_metadata, "quality": quality}

    def _extract_visual_evidence(self, record: dict[str, Any], video_path: Path) -> list[dict[str, Any]]:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            return []
        keyframe_dir = self.store.record_dir(record["record_id"]) / "keyframes"
        keyframe_dir.mkdir(parents=True, exist_ok=True)
        frame_pattern = keyframe_dir / "frame-%04d.jpg"
        interval = max(15, min(int(self.config.frame_interval_seconds or 60), 600))
        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(video_path),
            "-vf",
            f"fps=1/{interval}",
            "-q:v",
            "3",
            str(frame_pattern),
        ]
        _run_command(command, "Could not extract meeting screen keyframes.")
        evidence = []
        for index, frame_path in enumerate(sorted(keyframe_dir.glob("frame-*.jpg"))[:30], start=1):
            timestamp = (index - 1) * interval
            evidence.append(
                {
                    "timestamp_seconds": timestamp,
                    "image_url": f"/meeting-recorder/assets/{record['record_id']}/keyframes/{frame_path.name}",
                    "summary": f"Video snapshot captured around {timestamp // 60:02d}:{timestamp % 60:02d}.",
                }
            )
        return evidence

    def _transcript_quality(self, *, audio_path: Path, chunks: list[dict[str, Any]], detected_language: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        duration = _audio_duration_seconds(audio_path)
        if duration <= 0:
            duration = max((float(chunk.get("end_seconds") or 0) for chunk in chunks), default=0.0)
        segment_seconds = MEETING_TRANSCRIPT_SEGMENT_SECONDS
        segment_count = max(1, int((duration + segment_seconds - 1) // segment_seconds)) if duration else 1
        segments: list[dict[str, Any]] = []
        low_audio_count = 0
        no_audio_count = 0
        for index in range(segment_count):
            start = index * segment_seconds
            end = min(duration, start + segment_seconds) if duration else start + segment_seconds
            metrics = _audio_volume_metrics(audio_path, start_seconds=start, duration_seconds=max(1, end - start))
            segment_chunks = [
                chunk
                for chunk in chunks
                if float(chunk.get("start_seconds") or 0) < end and float(chunk.get("end_seconds") or 0) >= start
            ]
            has_no_audio = any("[no audio]" in str(chunk.get("text") or "").lower() for chunk in segment_chunks)
            low_audio = bool(metrics.get("low_audio")) or has_no_audio
            low_audio_count += 1 if low_audio else 0
            no_audio_count += 1 if has_no_audio else 0
            segments.append(
                {
                    "index": index,
                    "start_seconds": start,
                    "end_seconds": end,
                    "language": detected_language or "auto",
                    "language_confidence": None,
                    "mean_volume_db": metrics.get("mean_volume_db"),
                    "max_volume_db": metrics.get("max_volume_db"),
                    "quality": "low_audio" if low_audio else "ok",
                    "possible_missed_speech": low_audio,
                    "chunk_count": len(segment_chunks),
                }
            )
        quality = self._quality_from_segments(segments, default_language=detected_language or "auto", chunks=chunks)
        return segments, quality

    def _quality_from_segments(
        self,
        segments: list[dict[str, Any]],
        *,
        default_language: str = "auto",
        chunks: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        low_audio_count = sum(1 for segment in segments if segment.get("quality") == "low_audio")
        no_audio_count = sum(1 for segment in segments if segment.get("possible_missed_speech") and segment.get("chunk_count"))
        languages = sorted({str(segment.get("language") or "").strip() for segment in segments if segment.get("language")})
        language = ",".join(languages) or default_language
        language_codes = {code.strip().lower() for code in language.split(",") if code.strip()}
        unusual_language = bool(language_codes) and not language_codes.issubset(MEETING_TRANSCRIPT_EXPECTED_LANGUAGES)
        repetition = _transcript_repetition_metrics(chunks or [])
        warnings = []
        if low_audio_count:
            warnings.append("Some transcript segments have low audio and may miss Zoom/system audio or microphone speech.")
        if no_audio_count:
            warnings.append("One or more transcript segments may contain [no audio] or missed speech.")
        if unusual_language:
            warnings.append(f"Whisper detected unexpected language '{language}', so transcript may be unreliable.")
        if repetition.get("is_repetitive"):
            warnings.append("Transcript contains repeated chunks and may be a Whisper hallucination or captured looped/unclear audio.")
        return {
            "language": language,
            "segment_seconds": MEETING_TRANSCRIPT_SEGMENT_SECONDS,
            "segment_count": len(segments),
            "low_audio_segment_count": low_audio_count,
            "no_audio_segment_count": no_audio_count,
            "repetitive_chunk_count": repetition.get("repetitive_chunk_count", 0),
            "unique_text_ratio": repetition.get("unique_text_ratio"),
            "dominant_chunk_ratio": repetition.get("dominant_chunk_ratio"),
            "possible_incomplete": bool(low_audio_count or no_audio_count or unusual_language or repetition.get("is_repetitive")),
            "warnings": warnings,
        }

    def _generate_minutes(self, *, record: dict[str, Any], transcript_text: str, transcript_quality: dict[str, Any] | None = None) -> str:
        quality = transcript_quality or {}
        system_prompt = (
            "You write concise, evidence-grounded English meeting minutes for product managers. "
            "Use only the provided spoken transcript and meeting metadata. "
            "Do not use screen recordings, screenshots, keyframes, or visual context. "
            "Do not invent owners, decisions, dates, or follow-ups. "
            "If transcript quality warnings are present, include a short warning before the Summary section and do not infer decisions or action items from repeated low-audio text."
        )
        user_prompt = (
            f"Meeting title: {record.get('title')}\n"
            f"Platform: {record.get('platform')}\n"
            f"Scheduled start: {record.get('scheduled_start')}\n"
            f"Attendees from calendar: {json.dumps(record.get('attendees') or [], ensure_ascii=False)}\n"
            f"Transcript quality: {json.dumps(quality, ensure_ascii=False)}\n\n"
            "# Transcript\n"
            f"{transcript_text or 'No transcript text was produced.'}\n\n"
            "Return Markdown with these sections: Warning if needed, Summary, Decisions, Action Items, Risks/Blockers, Open Questions, Follow-ups."
        )
        return self.text_client.create_answer(system_prompt=system_prompt, user_prompt=user_prompt).strip()


def _assert_record_owner(record: dict[str, Any], owner_email: str) -> None:
    owner = str(owner_email or "").strip().lower()
    if not owner or str(record.get("owner_email") or "").strip().lower() != owner:
        raise ToolError("Meeting record is not available for this Google account.")


def _run_command(command: list[str], error_message: str, *, timeout_seconds: int = 600) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout_seconds)  # noqa: S603
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ToolError(f"{error_message} {error}") from error
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[-1200:]
        raise ToolError(f"{error_message} {detail}".strip())
    return completed


@contextmanager
def _temporary_path(prefix: str, suffix: str):
    handle = tempfile.NamedTemporaryFile(prefix=prefix, suffix=suffix, delete=False)
    path = Path(handle.name)
    handle.close()
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


def _audio_volume_metrics(audio_path: Path, *, start_seconds: float | None = None, duration_seconds: float | None = None) -> dict[str, Any]:
    ffmpeg_path = _resolve_ffmpeg_bin("ffmpeg")
    if not ffmpeg_path or not audio_path.exists():
        return {"status": "unavailable"}
    command = [ffmpeg_path, "-hide_banner", "-nostats"]
    if start_seconds is not None:
        command.extend(["-ss", str(max(0.0, float(start_seconds)))])
    if duration_seconds is not None:
        command.extend(["-t", str(max(0.1, float(duration_seconds)))])
    command.extend(["-i", str(audio_path), "-af", "volumedetect", "-f", "null", "-"])
    try:
        completed = _run_command(command, "Could not inspect meeting audio volume.", timeout_seconds=60)
    except ToolError as error:
        return {"status": "unavailable", "warning": str(error)}
    output = f"{completed.stderr}\n{completed.stdout}"
    mean_match = re.search(r"mean_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", output)
    max_match = re.search(r"max_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", output)
    mean_db = float(mean_match.group(1)) if mean_match else None
    max_db = float(max_match.group(1)) if max_match else None
    status = "low_audio" if mean_db is None or mean_db <= MEETING_AUDIO_LOW_MEAN_DB else "ok"
    return {"status": status, "mean_volume_db": mean_db, "max_volume_db": max_db, "low_audio": status == "low_audio"}


def _audio_duration_seconds(audio_path: Path) -> float:
    ffprobe_path = _resolve_executable("ffprobe", ("/opt/homebrew/bin/ffprobe", "/usr/local/bin/ffprobe"))
    if not ffprobe_path or not audio_path.exists():
        return 0.0
    command = [
        ffprobe_path,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(audio_path),
    ]
    try:
        completed = _run_command(command, "Could not inspect meeting audio duration.", timeout_seconds=30)
        return max(0.0, float((completed.stdout or "").strip() or 0))
    except (ToolError, ValueError):
        return 0.0


def _parse_whisper_language(output: str, fallback: str = "auto") -> str:
    match = re.search(r"auto-detected language:\s*([a-zA-Z_-]+)", str(output or ""))
    return (match.group(1).lower() if match else fallback) or "auto"


def _normalize_transcript_text(value: str) -> str:
    lowered = str(value or "").strip().lower()
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s\u4e00-\u9fff]", " ", lowered)).strip()


def _transcript_repetition_metrics(chunks: list[dict[str, Any]]) -> dict[str, Any]:
    texts = [_normalize_transcript_text(str(chunk.get("text") or "")) for chunk in chunks]
    texts = [text for text in texts if text]
    total = len(texts)
    if total < 5:
        return {
            "is_repetitive": False,
            "repetitive_chunk_count": 0,
            "unique_text_ratio": None,
            "dominant_chunk_ratio": None,
        }
    counts: dict[str, int] = {}
    adjacent_repeats = 0
    previous = ""
    for text in texts:
        counts[text] = counts.get(text, 0) + 1
        if previous and previous == text:
            adjacent_repeats += 1
        previous = text
    dominant_count = max(counts.values(), default=0)
    unique_ratio = len(counts) / total
    dominant_ratio = dominant_count / total
    is_repetitive = (
        dominant_ratio >= MEETING_TRANSCRIPT_REPETITIVE_DOMINANT_RATIO
        or (unique_ratio <= MEETING_TRANSCRIPT_REPETITIVE_UNIQUE_RATIO and adjacent_repeats >= 3)
    )
    return {
        "is_repetitive": is_repetitive,
        "repetitive_chunk_count": dominant_count if is_repetitive else 0,
        "unique_text_ratio": round(unique_ratio, 3),
        "dominant_chunk_ratio": round(dominant_ratio, 3),
    }


def _should_retry_transcript_in_english(quality: dict[str, Any]) -> bool:
    language = str(quality.get("language") or "auto").strip().lower()
    language_codes = {code.strip() for code in language.split(",") if code.strip()}
    unusual_language = bool(language_codes) and not language_codes.issubset(MEETING_TRANSCRIPT_EXPECTED_LANGUAGES)
    return bool(unusual_language or quality.get("repetitive_chunk_count"))


def _transcript_quality_score(quality: dict[str, Any]) -> int:
    score = 100
    score -= 40 if quality.get("repetitive_chunk_count") else 0
    language = str(quality.get("language") or "auto").strip().lower()
    language_codes = {code.strip() for code in language.split(",") if code.strip()}
    if language_codes and not language_codes.issubset(MEETING_TRANSCRIPT_EXPECTED_LANGUAGES):
        score -= 30
    score -= 10 * int(quality.get("low_audio_segment_count") or 0)
    score -= 10 * int(quality.get("no_audio_segment_count") or 0)
    return score


def _parse_srt_transcript(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = path.read_text(encoding="utf-8")
    except OSError:
        return []
    chunks: list[dict[str, Any]] = []
    for block in re.split(r"\n\s*\n", payload.strip()):
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if len(lines) < 2:
            continue
        time_line_index = 0
        if re.fullmatch(r"\d+", lines[0]) and len(lines) >= 3:
            time_line_index = 1
        time_match = re.match(
            r"(?P<start>\d{2}:\d{2}:\d{2}[,.]\d{3})\s*-->\s*(?P<end>\d{2}:\d{2}:\d{2}[,.]\d{3})",
            lines[time_line_index],
        )
        if not time_match:
            continue
        text = " ".join(lines[time_line_index + 1 :]).strip()
        if not text:
            continue
        chunks.append(
            {
                "start_seconds": _srt_timestamp_seconds(time_match.group("start")),
                "end_seconds": _srt_timestamp_seconds(time_match.group("end")),
                "text": text,
            }
        )
    return chunks


def _srt_timestamp_seconds(value: str) -> float:
    normalized = str(value or "").replace(",", ".")
    match = re.fullmatch(r"(\d{2}):(\d{2}):(\d{2})\.(\d{3})", normalized)
    if not match:
        return 0
    hours, minutes, seconds, millis = (int(part) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds + millis / 1000


def _parse_meeting_datetime(value: str, *, timezone_name: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    return parsed


def _avfoundation_devices(ffmpeg_path: str | None) -> dict[str, list[str]]:
    if not ffmpeg_path:
        return {"video_devices": [], "audio_devices": []}
    command = [ffmpeg_path, "-hide_banner", "-f", "avfoundation", "-list_devices", "true", "-i", ""]
    try:
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=10)  # noqa: S603
    except (OSError, subprocess.TimeoutExpired):
        return {"video_devices": [], "audio_devices": []}
    return _parse_avfoundation_devices(f"{completed.stderr}\n{completed.stdout}")


def _build_ffmpeg_recording_command(
    *,
    ffmpeg_path: str,
    video_input: str,
    audio_input: str,
    video_path: Path,
    video_fps: int,
    video_max_width: int,
    video_max_height: int,
    avfoundation_pixel_format: str,
) -> list[str]:
    fps = _bounded_int(video_fps, default=15, minimum=5, maximum=30)
    max_width = _bounded_int(video_max_width, default=1920, minimum=640, maximum=2560)
    max_height = _bounded_int(video_max_height, default=1080, minimum=360, maximum=1600)
    pixel_format = _safe_avfoundation_pixel_format(avfoundation_pixel_format)
    scale_filter = (
        f"scale='if(gt(iw/ih,{max_width}/{max_height}),min({max_width},iw),-2)'"
        f":'if(gt(iw/ih,{max_width}/{max_height}),-2,min({max_height},ih))'"
        f":flags=bicubic,fps={fps},format=yuv420p"
    )
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-thread_queue_size",
        "512",
        "-f",
        "avfoundation",
        "-framerate",
        str(fps),
        "-pixel_format",
        pixel_format,
        "-i",
        f"{video_input}:",
        "-thread_queue_size",
        "512",
        "-f",
        "avfoundation",
        "-i",
        f":{audio_input}",
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-vf",
        scale_filter,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-profile:v",
        "high",
        "-level",
        "4.2",
        "-g",
        str(fps * 2),
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-ac",
        str(MEETING_PLAYBACK_AUDIO_CHANNELS),
        "-ar",
        str(MEETING_PLAYBACK_AUDIO_SAMPLE_RATE),
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        str(video_path),
    ]


def _build_ffmpeg_screen_preflight_command(
    *,
    ffmpeg_path: str,
    video_input: str,
    video_path: Path,
    avfoundation_pixel_format: str,
) -> list[str]:
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "avfoundation",
        "-framerate",
        "5",
        "-pixel_format",
        _safe_avfoundation_pixel_format(avfoundation_pixel_format),
        "-i",
        f"{video_input}:",
        "-t",
        "1",
        "-frames:v",
        "1",
        "-an",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        str(video_path),
    ]


def _build_ffmpeg_audio_recording_command(*, ffmpeg_path: str, audio_input: str, audio_path: Path) -> list[str]:
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-f",
        "avfoundation",
        "-i",
        f":{audio_input}",
        "-acodec",
        "pcm_s16le",
        "-ar",
        "16000",
        "-ac",
        "1",
        str(audio_path),
    ]


def _build_ffmpeg_playback_repair_command(*, ffmpeg_path: str, source_path: Path, output_path: Path) -> list[str]:
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(source_path),
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-ac",
        str(MEETING_PLAYBACK_AUDIO_CHANNELS),
        "-ar",
        str(MEETING_PLAYBACK_AUDIO_SAMPLE_RATE),
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]


def _parse_avfoundation_devices(output: str) -> dict[str, list[str]]:
    video_devices: list[str] = []
    audio_devices: list[str] = []
    section = ""
    for line in str(output or "").splitlines():
        lowered = line.lower()
        if "avfoundation video devices" in lowered:
            section = "video"
            continue
        if "avfoundation audio devices" in lowered:
            section = "audio"
            continue
        match = re.search(r"\[\d+\]\s+(.+)$", line.strip())
        if not match:
            continue
        name = match.group(1).strip()
        if section == "video":
            video_devices.append(name)
        elif section == "audio":
            audio_devices.append(name)
    return {"video_devices": video_devices, "audio_devices": audio_devices}


def _bounded_int(value: int | str | None, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value if value is not None else default)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _safe_avfoundation_pixel_format(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    supported = {"bgr0", "0rgb", "nv12", "uyvy422", "yuyv422"}
    return normalized if normalized in supported else "bgr0"


def _effective_audio_input(configured_audio_input: str, audio_devices: list[str]) -> str:
    configured = str(configured_audio_input or "").strip() or "default"
    if configured.lower() != "default":
        return configured
    for name in audio_devices:
        if str(name or "").strip().lower() == "meeting recorder aggregate":
            return str(name).strip()
    return configured


def _audio_capture_status(audio_input: str, audio_devices: list[str]) -> dict[str, Any]:
    normalized_input = str(audio_input or "").strip().lower()
    normalized_devices = [str(item or "").strip().lower() for item in audio_devices]
    system_audio_available = any(_looks_like_system_audio_device(name) for name in normalized_devices)
    aggregate_available = any("aggregate" in name for name in normalized_devices)
    if "aggregate" in normalized_input:
        mode = "aggregate_device"
        label = "Aggregate device configured; signal not yet verified"
        configured = True
    elif _looks_like_system_audio_device(normalized_input):
        mode = "system_audio"
        label = "System audio configured"
        configured = True
    elif normalized_input not in {"", "default", "0"} and "microphone" not in normalized_input:
        mode = "custom_audio"
        label = "Custom audio input configured"
        configured = True
    else:
        mode = "microphone_only"
        label = "Microphone only"
        configured = False
    warning = ""
    if not configured:
        warning = (
            "Current audio input is microphone-only. Meet/Zoom participants are captured reliably only after configuring "
            "a system-audio or aggregate input such as BlackHole plus MacBook microphone."
        )
    return {
        "audio_capture_mode": mode,
        "audio_capture_label": label,
        "system_audio_configured": configured,
        "audio_device_configured": configured,
        "audio_signal_verified": False,
        "system_audio_available": system_audio_available,
        "aggregate_audio_available": aggregate_available,
        "recommended_audio_input": "Meeting Recorder Aggregate",
        "audio_capture_warning": warning,
    }


def _looks_like_system_audio_device(name: str) -> bool:
    lowered = str(name or "").strip().lower()
    return any(token in lowered for token in ("blackhole", "soundflower", "loopback", "aggregate", "multi-output"))


def _normalize_recording_mode(*, recording_mode: str, meeting_link: str) -> str:
    normalized = str(recording_mode or "").strip().lower().replace("-", "_")
    if normalized in {"audio", "audio_only", "onsite"}:
        return MEETING_RECORDING_MODE_AUDIO_ONLY
    if normalized in {"screen", "screen_audio", "video", "video_audio"}:
        return MEETING_RECORDING_MODE_SCREEN_AUDIO
    return MEETING_RECORDING_MODE_SCREEN_AUDIO if str(meeting_link or "").strip() else MEETING_RECORDING_MODE_AUDIO_ONLY


def _safe_record_id(record_id: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]", "-", str(record_id or "").strip())
    if not cleaned:
        raise ToolError("Meeting record id is missing.")
    return cleaned


def _redact_command(command: list[str]) -> list[str]:
    return [str(part) for part in command]


def _resolve_executable(value: str, candidates: tuple[str, ...]) -> str | None:
    configured = str(value or "").strip()
    if configured:
        configured_path = Path(configured).expanduser()
        if configured_path.is_absolute() and configured_path.exists():
            return str(configured_path)
        resolved = shutil.which(configured)
        if resolved:
            return resolved
    for candidate in candidates:
        path = Path(candidate).expanduser()
        if path.exists():
            return str(path)
    return None


def _resolve_ffmpeg_bin(value: str) -> str | None:
    return _resolve_executable(
        value,
        (
            "/opt/homebrew/bin/ffmpeg",
            "/opt/homebrew/opt/ffmpeg/bin/ffmpeg",
            "/usr/local/bin/ffmpeg",
        ),
    )


def _resolve_whisper_cpp_bin(value: str) -> str | None:
    return _resolve_executable(
        value,
        (
            "/opt/homebrew/bin/whisper-cli",
            "/opt/homebrew/opt/whisper-cpp/bin/whisper-cli",
            "/usr/local/bin/whisper-cli",
        ),
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
