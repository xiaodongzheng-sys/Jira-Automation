from __future__ import annotations

import html
import json
import os
from concurrent.futures import ThreadPoolExecutor
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
MEETING_RECORDER_PROMPT_VERSION = "v3_topic_bullets_english_minutes"
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
MEETING_AUDIO_NEAR_SILENT_MAX_DB = -80.0
MEETING_AUDIO_READY_TIMEOUT_SECONDS = 1.2
MEETING_AUDIO_SEGMENT_SECONDS = 4 * 60 * 60
MEETING_AUDIO_EARLY_SIGNAL_SAMPLE_SECONDS = 1.0
MEETING_AUDIO_EARLY_SIGNAL_MIN_BYTES = 4096
MEETING_AUDIO_EARLY_SIGNAL_MIN_GROWTH_BYTES = 2048
MEETING_AUDIO_MONITOR_OUTPUT_DEVICE_INDEX = "2"
MEETING_TRANSCRIPT_SEGMENT_SECONDS = 60
MEETING_PLAYBACK_AUDIO_CHANNELS = 2
MEETING_PLAYBACK_AUDIO_SAMPLE_RATE = 48000
MEETING_RECORDING_MODE_AUDIO_ONLY = "audio_only"
MEETING_RECORDING_MODE_SCREEN_AUDIO = "screen_audio"
MEETING_AUDIO_POST_STOP_PAD_PROFILE = "post_stop_pad_v1"
MEETING_BROWSER_AUDIO_PROFILE = "browser_media_recorder_v1"
MEETING_BROWSER_LINKED_MIN_SECONDS = 10.0
MEETING_SEGMENTED_AUDIO_PROFILE = "segmented_avfoundation_v1"
MEETING_SCREENCAPTUREKIT_AUDIO_PROFILE = "screencapturekit_audio_v1"
MEETING_TRANSCRIPT_REPETITIVE_DOMINANT_RATIO = 0.6
MEETING_TRANSCRIPT_REPETITIVE_UNIQUE_RATIO = 0.35
MEETING_TRANSCRIPT_EXPECTED_LANGUAGES = {
    "auto",
    "en",
    "zh",
    "cn",
}
MEETING_TRANSCRIPT_LANGUAGE_DEFAULT = "mixed"
MEETING_TRANSCRIPT_LANGUAGE_MODES = {"en", "zh", "mixed"}
MEETING_TRANSCRIPT_LANGUAGE_LABELS = {
    "en": "English",
    "zh": "Chinese",
    "mixed": "Mixed Chinese/English",
}


@dataclass(frozen=True)
class MeetingRecorderConfig:
    ffmpeg_bin: str = "ffmpeg"
    video_input: str = "Capture screen 0"
    audio_input: str = "Meeting Recorder Aggregate"
    video_fps: int = 15
    video_max_width: int = 1920
    video_max_height: int = 1080
    avfoundation_pixel_format: str = "bgr0"
    screen_preflight_timeout_seconds: int = 20
    audio_only_fallback_on_screen_failure: bool = True
    frame_interval_seconds: int = 60
    vision_model: str = "gpt-4.1-mini"
    transcribe_provider: str = "whisper_cpp"
    whisper_cpp_bin: str = "whisper-cli"
    whisper_model: str = "~/.cache/whisper.cpp/ggml-medium.bin"
    whisper_language: str = "auto"
    transcript_segment_workers: int = 2
    whisper_threads: int = 0
    background_nice: int = 10
    capture_status_every_buffers: int = 250
    startup_silence_grace_seconds: int = 300


def normalize_meeting_transcript_language(value: object) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    aliases = {
        "english": "en",
        "eng": "en",
        "chinese": "zh",
        "mandarin": "zh",
        "zh_cn": "zh",
        "cn": "zh",
        "auto": "mixed",
        "mix": "mixed",
        "zh_en": "mixed",
        "en_zh": "mixed",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in MEETING_TRANSCRIPT_LANGUAGE_MODES else MEETING_TRANSCRIPT_LANGUAGE_DEFAULT


def meeting_transcript_whisper_language(value: object, *, fallback: str = "auto") -> str:
    normalized = normalize_meeting_transcript_language(value)
    if normalized == "mixed":
        return str(fallback or "auto").strip().lower() or "auto"
    return normalized


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
    event_type = str(event.get("eventType") or "default").strip() or "default"
    if event_type != "default":
        return None

    meeting_links = extract_meeting_links(
        event.get("hangoutLink") or "",
        event.get("location") or "",
        event.get("description") or "",
        json.dumps(event.get("conferenceData") or {}, ensure_ascii=False),
    )

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

    first_link = meeting_links[0] if meeting_links else ""
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
        self._processes: dict[str, Any] = {}
        self._lock = threading.Lock()

    def diagnostics(self) -> dict[str, Any]:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        whisper_path = _resolve_whisper_cpp_bin(self.config.whisper_cpp_bin)
        devices = _avfoundation_devices(ffmpeg_path)
        effective_audio_input = _effective_audio_input(self.config.audio_input, devices.get("audio_devices") or [])
        audio_status = _audio_capture_status(effective_audio_input, devices.get("audio_devices") or [])
        screencapture_helper_source = Path(__file__).resolve().parents[1] / "tools" / "meeting_screencapture_helper.swift"
        screencapture_ready = screencapture_helper_source.exists() and bool(_resolve_executable("xcrun", ("/usr/bin/xcrun",)))
        return {
            "ffmpeg_configured": bool(ffmpeg_path),
            "ffmpeg_path": ffmpeg_path or "",
            "audio_input": "ScreenCaptureKit system audio + microphone" if screencapture_ready else effective_audio_input,
            "configured_audio_input": self.config.audio_input,
            "audio_devices": devices.get("audio_devices") or [],
            **(
                {
                    "audio_capture_mode": "screencapturekit_audio",
                    "audio_capture_label": "ScreenCaptureKit system audio + microphone",
                    "system_audio_available": True,
                    "system_audio_configured": True,
                    "audio_device_configured": True,
                    "audio_capture_warning": "",
                }
                if screencapture_ready
                else audio_status
            ),
            "audio_signal_verified": False,
            "audio_signal_note": "Signal is verified during recording; ScreenCaptureKit captures normal system output audio plus microphone without changing meeting audio routing.",
            "transcribe_provider": self.config.transcribe_provider,
            "whisper_cpp_configured": bool(whisper_path),
            "whisper_cpp_bin": whisper_path or "",
            "whisper_model": str(Path(self.config.whisper_model).expanduser()),
            "whisper_model_exists": Path(self.config.whisper_model).expanduser().exists(),
            "whisper_language": self.config.whisper_language,
            "recording_background_nice": _normalized_background_nice(self.config.background_nice),
            "capture_status_every_buffers": _normalized_status_every_buffers(self.config.capture_status_every_buffers),
            "startup_silence_grace_seconds": _normalized_startup_silence_grace_seconds(self.config.startup_silence_grace_seconds),
            "transcript_segment_workers": _effective_transcript_segment_workers(self.config.transcript_segment_workers),
            "whisper_threads": _effective_whisper_threads(
                whisper_threads=self.config.whisper_threads,
                segment_workers=_effective_transcript_segment_workers(self.config.transcript_segment_workers),
            ),
            "meeting_audio_setup_note": "For linked or in-person recordings, keep speaker and microphone on normal devices; the local ScreenCaptureKit helper captures system output audio plus microphone.",
            "mac_permissions_note": "Microphone permission is required for the Python/terminal process that runs ffmpeg.",
            "system_audio_note": "Meetings use ScreenCaptureKit instead of BlackHole/Aggregate routing, so you can still hear linked meetings normally.",
            "onsite_note": "For in-person meetings, leave the meeting link blank; the local helper records the Mac microphone and may also capture system audio.",
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
        transcript_language: str = "",
    ) -> dict[str, Any]:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            raise ConfigError("ffmpeg is required for local meeting recording. Install ffmpeg or set MEETING_RECORDER_FFMPEG_BIN.")
        requested_mode = str(recording_mode or "").strip().lower().replace("-", "_")
        effective_mode = MEETING_RECORDING_MODE_AUDIO_ONLY
        devices = _avfoundation_devices(ffmpeg_path)
        audio_input = _effective_recording_audio_input(
            self.config.audio_input,
            devices.get("audio_devices") or [],
            recording_mode=effective_mode,
            meeting_link=meeting_link,
        )
        audio_status = _audio_capture_status(audio_input, devices.get("audio_devices") or [])
        preflight = {
            "status": "not_checked",
            "checked_at": _utc_now(),
            "duration_seconds": 0,
            "warning": "",
        }
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
        transcript_language = normalize_meeting_transcript_language(transcript_language)
        record["transcript_language"] = transcript_language
        record["transcript_language_label"] = MEETING_TRANSCRIPT_LANGUAGE_LABELS[transcript_language]
        record_dir = self.store.record_dir(record["record_id"])
        audio_path = record_dir / "meeting.wav"
        log_path = record_dir / "ffmpeg.log"
        segment_dir = record_dir / "audio_segments"
        record["audio_preflight"] = preflight
        record["diagnostics_snapshot"] = {
            "ffmpeg_path": ffmpeg_path,
            "audio_input": audio_input,
            "requested_recording_mode": requested_mode or "audio_only",
            "recording_mode": effective_mode,
            "effective_recording_mode": effective_mode,
            "audio_capture_mode": audio_status.get("audio_capture_mode"),
            "audio_capture_label": audio_status.get("audio_capture_label"),
            "system_audio_configured": audio_status.get("system_audio_configured"),
            "configured_audio_input": self.config.audio_input,
            "audio_signal_verified": False,
            "audio_signal_note": "Signal is verified from the real recorder output after Stop; Start avoids a separate microphone preflight because reopening macOS audio devices can delay or break short recordings.",
            "audio_devices": devices.get("audio_devices") or [],
            "meeting_audio_setup_note": "Zoom/Meet speaker can stay on MacBook speakers or Default; Zoom/Meet microphone should stay on a real microphone.",
            "transcript_language": transcript_language,
            "transcript_language_label": MEETING_TRANSCRIPT_LANGUAGE_LABELS[transcript_language],
            "recording_background_nice": _normalized_background_nice(self.config.background_nice),
            "capture_status_every_buffers": _normalized_status_every_buffers(self.config.capture_status_every_buffers),
            "startup_silence_grace_seconds": _normalized_startup_silence_grace_seconds(self.config.startup_silence_grace_seconds),
        }
        record["recording_health"] = {
            "status": "warning" if preflight.get("status") == "too_quiet" else preflight.get("status", "unknown"),
            "warning": preflight.get("warning", ""),
            "checked_at": preflight.get("checked_at", ""),
        }
        self.store.save_record(record)
        is_linked_meeting = bool(str(meeting_link or "").strip())
        capture_source = "screencapturekit_audio" if is_linked_meeting else "screencapturekit_f2f"
        system_audio_path = record_dir / "screencapture-system.caf"
        microphone_audio_path = record_dir / "screencapture-microphone.caf"
        helper_status_path = record_dir / "screencapture-status.json"
        try:
            recorder: Any = _ScreenCaptureKitAudioRecorder(
                helper_path=_resolve_screencapturekit_helper(self.store.root_dir),
                system_audio_path=system_audio_path,
                microphone_audio_path=microphone_audio_path,
                status_path=helper_status_path,
                log_path=log_path,
                background_nice=self.config.background_nice,
                status_every_buffers=self.config.capture_status_every_buffers,
            )
            _stop_external_audio_monitor(self.store.root_dir)
            ready = recorder.start()
        except (ConfigError, OSError, ToolError) as error:
            record["status"] = "failed"
            record["error"] = str(error)
            self.store.save_record(record)
            raise ToolError(f"Could not start ScreenCaptureKit meeting recorder: {error}") from error

        if ready.get("status") != "ok":
            recorder.stop()
            stderr = str(ready.get("warning") or "").strip()
            record["status"] = "failed"
            record["error"] = stderr or "ScreenCaptureKit helper did not start writing audio."
            self.store.save_record(record)
            raise ToolError(record["error"])

        with self._lock:
            self._processes[record["record_id"]] = recorder
        record["status"] = "recording"
        record["recording_started_at"] = _utc_now()
        media = {
            "recording_mode": effective_mode,
            "recorder_command": _redact_command(recorder.command),
            "recorder_pid": _safe_int(recorder.pid),
            "audio_ready_checked_at": _utc_now(),
            "audio_ready_latency_seconds": ready.get("latency_seconds"),
            "audio_ready_bytes": ready.get("bytes"),
            "recording_background_nice": _normalized_background_nice(self.config.background_nice),
            "capture_status_every_buffers": _normalized_status_every_buffers(self.config.capture_status_every_buffers),
        }
        media.update(
            {
                "audio_capture_profile": MEETING_SCREENCAPTUREKIT_AUDIO_PROFILE,
                "screencapture_capture_source": capture_source,
                "system_audio_path": str(system_audio_path.relative_to(self.store.root_dir)),
                "microphone_audio_path": str(microphone_audio_path.relative_to(self.store.root_dir)),
                "screencapture_status_path": str(helper_status_path.relative_to(self.store.root_dir)),
                "audio_capture_label": (
                    "ScreenCaptureKit system audio + microphone"
                    if is_linked_meeting
                    else "ScreenCaptureKit microphone + system audio"
                ),
                "audio_capture_note": (
                    "Zoom/Meet can keep normal speaker and microphone settings; macOS ScreenCaptureKit captures system output audio and microphone."
                    if is_linked_meeting
                    else "In-person recording uses the local ScreenCaptureKit helper to capture microphone audio and may also capture system audio."
                ),
            }
        )
        record["diagnostics_snapshot"]["audio_capture_mode"] = capture_source
        record["diagnostics_snapshot"]["audio_capture_label"] = media["audio_capture_label"]
        record["diagnostics_snapshot"]["system_audio_configured"] = True
        record["diagnostics_snapshot"]["audio_signal_note"] = (
            "Signal is captured by ScreenCaptureKit from normal system output audio plus microphone; "
            "meeting speaker output and microphone settings can stay normal."
        )
        record["diagnostics_snapshot"]["meeting_audio_setup_note"] = (
            "Zoom/Meet speaker can stay on MacBook speakers or Default; Zoom/Meet microphone should stay on a real microphone."
            if is_linked_meeting
            else "For in-person meetings, leave the meeting link blank; the helper records the Mac microphone and may include system audio."
        )
        media.update(
            {
                "audio_path": str(audio_path.relative_to(self.store.root_dir)),
                "audio_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.wav",
                "audio_format": "wav",
                "audio_sample_rate": 16000,
                "audio_channels": 1,
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
        recorder_summary: dict[str, Any] = {}
        if isinstance(process, _SegmentedAudioRecorder):
            recorder_summary = process.stop()
        elif isinstance(process, _ScreenCaptureKitAudioRecorder):
            recorder_summary = process.stop()
        elif process is not None and process.poll() is None:
            _terminate_recorder_process(process)
        else:
            self._terminate_persisted_recorder_process(record)
        if recorder_summary:
            media = record.get("media") if isinstance(record.get("media"), dict) else {}
            record["media"] = {**media, **recorder_summary}
        record["status"] = "recorded"
        record["recording_stopped_at"] = _utc_now()
        record = self._finalize_segmented_audio_recording(record)
        record = self._finalize_screencapturekit_audio_recording(record)
        record = self._finalize_audio_only_recording(record)
        record["recording_health"] = self._recording_health(record)
        if record["recording_health"].get("status") == "failed":
            record["status"] = "failed"
            record["error"] = str(record["recording_health"].get("warning") or "").strip()
        self.store.save_record(record)
        return record

    def check_recording_signal(self, *, record_id: str, owner_email: str) -> dict[str, Any]:
        record = self.store.get_record(record_id)
        _assert_record_owner(record, owner_email)
        if str(record.get("status") or "") != "recording":
            return record
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        if str(media.get("audio_capture_profile") or "") == MEETING_SCREENCAPTUREKIT_AUDIO_PROFILE:
            with self._lock:
                recorder = self._processes.get(record_id)
            if isinstance(recorder, _ScreenCaptureKitAudioRecorder):
                check = recorder.signal_snapshot(sample_seconds=MEETING_AUDIO_EARLY_SIGNAL_SAMPLE_SECONDS)
            else:
                check = {
                    "status": "warning",
                    "checked_at": _utc_now(),
                    "warning": "ScreenCaptureKit recorder process is not attached to this app instance; stop and start a new recording if audio does not appear.",
                }
            media["early_audio_check"] = check
            record["media"] = media
            if check.get("status") in {"ok", "pending"}:
                record["recording_health"] = {
                    "status": "recording",
                    "checked_at": check.get("checked_at", _utc_now()),
                    "warning": "",
                    "early_audio_check": check,
                }
                self.store.save_record(record)
                return record
            warning = str(check.get("warning") or "ScreenCaptureKit audio is not growing. Confirm macOS Screen Recording and Microphone permissions, then start a new recording.").strip()
            with self._lock:
                active_recorder = self._processes.pop(record_id, None)
            if isinstance(active_recorder, _ScreenCaptureKitAudioRecorder):
                summary = active_recorder.stop()
                record["media"] = {**media, **summary}
            record["status"] = "failed"
            record["recording_stopped_at"] = _utc_now()
            record["error"] = warning
            record["recording_health"] = {
                "status": "failed",
                "checked_at": check.get("checked_at", _utc_now()),
                "warning": warning,
                "early_audio_check": check,
            }
            self.store.save_record(record)
            return record
        if str(media.get("audio_capture_profile") or "") != MEETING_SEGMENTED_AUDIO_PROFILE:
            return record

        with self._lock:
            recorder = self._processes.get(record_id)
        if not isinstance(recorder, _SegmentedAudioRecorder):
            check = {
                "status": "warning",
                "checked_at": _utc_now(),
                "warning": "Recorder process is not attached to this app instance; stop and start a new recording if audio does not appear.",
            }
        else:
            check = recorder.signal_snapshot(sample_seconds=MEETING_AUDIO_EARLY_SIGNAL_SAMPLE_SECONDS)

        media["early_audio_check"] = check
        record["media"] = media
        if check.get("status") in {"ok", "pending"}:
            record["recording_health"] = {
                "status": "recording",
                "checked_at": check.get("checked_at", _utc_now()),
                "warning": "",
                "early_audio_check": check,
            }
            self.store.save_record(record)
            return record

        warning = str(check.get("warning") or "Recorder audio is not growing. Check Zoom speaker and microphone settings, then start a new recording.").strip()
        with self._lock:
            active_recorder = self._processes.pop(record_id, None)
        if isinstance(active_recorder, _SegmentedAudioRecorder):
            summary = active_recorder.stop()
            record["media"] = {**media, **summary}
        record["status"] = "failed"
        record["recording_stopped_at"] = _utc_now()
        record["error"] = warning
        record["recording_health"] = {
            "status": "failed",
            "checked_at": check.get("checked_at", _utc_now()),
            "warning": warning,
            "early_audio_check": check,
        }
        self.store.save_record(record)
        return record

    def import_browser_audio_recording(
        self,
        *,
        owner_email: str,
        title: str,
        platform: str,
        meeting_link: str,
        started_at: str,
        stopped_at: str,
        audio_bytes: bytes,
        mime_type: str = "",
        device_label: str = "",
        capture_source: str = "",
        preflight_metrics: dict[str, Any] | None = None,
        transcript_language: str = "",
    ) -> dict[str, Any]:
        if not audio_bytes:
            raise ToolError("Browser audio upload was empty.")
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            raise ConfigError("MEETING_RECORDER_FFMPEG_BIN must point to a working ffmpeg binary.")
        record = self.store.create_record(
            owner_email=owner_email,
            title=title,
            platform=platform,
            meeting_link=meeting_link,
        )
        transcript_language = normalize_meeting_transcript_language(transcript_language)
        record["transcript_language"] = transcript_language
        record["transcript_language_label"] = MEETING_TRANSCRIPT_LANGUAGE_LABELS[transcript_language]
        record_dir = self.store.record_dir(record["record_id"])
        extension = _browser_audio_extension(mime_type)
        source_path = record_dir / f"browser-audio{extension}"
        audio_path = record_dir / "meeting.wav"
        try:
            source_path.write_bytes(audio_bytes)
        except OSError as error:
            raise ToolError(f"Could not save browser audio upload: {error}") from error

        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(source_path),
            "-acodec",
            "pcm_s16le",
            "-ar",
            "16000",
            "-ac",
            "1",
            str(audio_path),
        ]
        try:
            _run_command(command, "Could not convert browser audio upload.", timeout_seconds=120)
        except ToolError as error:
            record["status"] = "failed"
            record["error"] = str(error)
            self.store.save_record(record)
            raise
        duration_seconds = _audio_duration_seconds(audio_path)
        now = _utc_now()
        record["status"] = "recorded"
        record["recording_started_at"] = started_at or record.get("created_at") or now
        record["recording_stopped_at"] = stopped_at or now
        normalized_capture_source = str(capture_source or "").strip()
        is_meeting_tab_capture = normalized_capture_source == "browser_tab_audio_linked"
        capture_mode = "browser_tab_audio" if is_meeting_tab_capture else "browser_microphone"
        capture_label = "Browser meeting tab audio + microphone" if is_meeting_tab_capture else "Browser microphone"
        capture_note = (
            "Linked meeting recording used browser tab audio plus microphone capture to avoid macOS virtual audio devices."
            if is_meeting_tab_capture
            else "F2F manual recording used the browser microphone recorder to avoid long-running macOS AVFoundation capture interruptions."
        )
        checked_preflight = preflight_metrics if isinstance(preflight_metrics, dict) else {}
        record["diagnostics_snapshot"] = {
            "audio_input": str(device_label or capture_label).strip(),
            "audio_capture_mode": capture_mode,
            "audio_capture_label": capture_label,
            "effective_recording_mode": MEETING_RECORDING_MODE_AUDIO_ONLY,
            "recording_mode": MEETING_RECORDING_MODE_AUDIO_ONLY,
            "requested_recording_mode": MEETING_RECORDING_MODE_AUDIO_ONLY,
            "system_audio_configured": False,
            "configured_audio_input": self.config.audio_input,
            "audio_signal_verified": duration_seconds > 0,
            "audio_signal_note": capture_note,
            "transcript_language": transcript_language,
            "transcript_language_label": MEETING_TRANSCRIPT_LANGUAGE_LABELS[transcript_language],
        }
        if device_label:
            record["diagnostics_snapshot"]["browser_audio_device_label"] = str(device_label).strip()
        record["audio_preflight"] = {
            "status": str(checked_preflight.get("status") or "not_checked").strip(),
            "checked_at": now,
            "duration_seconds": _safe_float(checked_preflight.get("duration_seconds")) or 0,
            "mean_volume_db": _safe_float(checked_preflight.get("rms_db")),
            "max_volume_db": _safe_float(checked_preflight.get("peak_db")),
            "warning": "",
        }
        record["media"] = {
            "recording_mode": MEETING_RECORDING_MODE_AUDIO_ONLY,
            "audio_path": str(audio_path.relative_to(self.store.root_dir)),
            "audio_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.wav",
            "audio_format": "wav",
            "audio_sample_rate": 16000,
            "audio_channels": 1,
            "source_audio_path": str(source_path.relative_to(self.store.root_dir)),
            "source_audio_mime_type": str(mime_type or ""),
            "audio_original_duration_seconds": duration_seconds,
            "audio_capture_profile": MEETING_BROWSER_AUDIO_PROFILE,
            "browser_audio_capture_source": normalized_capture_source or "browser_audio_f2f",
            "audio_conversion_command": _redact_command(command),
        }
        record["recording_health"] = self._recording_health(record)
        if record["recording_health"].get("status") == "failed":
            record["status"] = "failed"
            record["error"] = str(record["recording_health"].get("warning") or "").strip()
        self.store.save_record(record)
        return record

    def _finalize_segmented_audio_recording(self, record: dict[str, Any]) -> dict[str, Any]:
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        if str(media.get("audio_capture_profile") or "") != MEETING_SEGMENTED_AUDIO_PROFILE:
            return record
        segment_relative = str(media.get("audio_segment_dir") or "").strip()
        if not segment_relative:
            return record
        segment_dir = (self.store.root_dir / segment_relative).resolve()
        if not segment_dir.exists() or not segment_dir.is_dir():
            media = dict(media)
            media["audio_segment_warning"] = "Segmented recorder did not produce an audio segment directory."
            record["media"] = media
            return record
        segment_paths = [
            path
            for path in sorted(segment_dir.glob("segment-*.wav"))
            if path.is_file() and path.stat().st_size > 44
        ]
        media = dict(media)
        media["audio_segment_count"] = len(segment_paths)
        if not segment_paths:
            media["audio_segment_warning"] = "Segmented recorder produced no usable audio segments."
            record["media"] = media
            return record
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            media["audio_segment_warning"] = "ffmpeg is required to combine segmented meeting audio."
            record["media"] = media
            return record
        record_id = str(record.get("record_id") or "")
        record_dir = self.store.record_dir(record_id)
        audio_path = record_dir / "meeting.wav"
        concat_path = record_dir / "audio_segments.txt"
        concat_path.write_text(
            "\n".join(f"file '{_escape_ffmpeg_concat_path(path)}'" for path in segment_paths) + "\n",
            encoding="utf-8",
        )
        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_path),
            "-c",
            "copy",
            str(audio_path),
        ]
        try:
            _run_command(command, "Could not combine segmented meeting audio.", timeout_seconds=max(60, len(segment_paths) * 10))
        except ToolError as error:
            if len(segment_paths) == 1:
                try:
                    shutil.copy2(segment_paths[0], audio_path)
                except OSError:
                    media["audio_segment_warning"] = str(error)
                    record["media"] = media
                    return record
            else:
                media["audio_segment_warning"] = str(error)
                record["media"] = media
                return record
        if not audio_path.exists() or audio_path.stat().st_size <= 44:
            media["audio_segment_warning"] = "Segmented recorder combined audio file is empty."
            record["media"] = media
            return record
        media.update(
            {
                "audio_path": str(audio_path.relative_to(self.store.root_dir)),
                "audio_url": f"/meeting-recorder/assets/{record_id}/meeting.wav",
                "audio_format": "wav",
                "audio_sample_rate": 16000,
                "audio_channels": 1,
                "audio_concat_command": _redact_command(command),
                "audio_original_duration_seconds": _audio_duration_seconds(audio_path),
            }
        )
        media.pop("audio_segment_warning", None)
        record["media"] = media
        return record

    def _finalize_screencapturekit_audio_recording(self, record: dict[str, Any]) -> dict[str, Any]:
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        if str(media.get("audio_capture_profile") or "") != MEETING_SCREENCAPTUREKIT_AUDIO_PROFILE:
            return record
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            media = dict(media)
            media["audio_finalization_warning"] = "ffmpeg is required to mix ScreenCaptureKit meeting audio."
            record["media"] = media
            return record

        record_id = str(record.get("record_id") or "")
        record_dir = self.store.record_dir(record_id)
        system_path = (self.store.root_dir / str(media.get("system_audio_path") or "")).resolve()
        microphone_path = (self.store.root_dir / str(media.get("microphone_audio_path") or "")).resolve()
        audio_path = record_dir / "meeting.wav"
        input_paths = [
            path
            for path in (system_path, microphone_path)
            if path.exists() and path.is_file() and path.stat().st_size > 0 and _audio_duration_seconds(path) > 0
        ]
        media = dict(media)
        system_duration = _audio_duration_seconds(system_path) if system_path.exists() else 0
        microphone_duration = _audio_duration_seconds(microphone_path) if microphone_path.exists() else 0
        media["screencapture_system_duration_seconds"] = system_duration
        media["screencapture_microphone_duration_seconds"] = microphone_duration
        longest_duration = max(system_duration, microphone_duration, 0.0)
        if longest_duration > 0:
            media["screencapture_system_duration_ratio"] = round(system_duration / longest_duration, 4)
            media["screencapture_microphone_duration_ratio"] = round(microphone_duration / longest_duration, 4)
            if system_duration >= longest_duration * 0.9 and microphone_duration > 0 and microphone_duration < longest_duration * 0.8:
                media["screencapture_track_warning"] = "Microphone track is shorter than system audio; system audio is available for transcription."
        if not input_paths:
            media["audio_finalization_warning"] = "ScreenCaptureKit produced no usable system or microphone audio."
            record["media"] = media
            return record
        if len(input_paths) == 1:
            command = _build_ffmpeg_single_audio_convert_command(
                ffmpeg_path=ffmpeg_path,
                source_path=input_paths[0],
                output_path=audio_path,
            )
        else:
            command = _build_ffmpeg_screencapturekit_mix_command(
                ffmpeg_path=ffmpeg_path,
                system_path=system_path,
                microphone_path=microphone_path,
                output_path=audio_path,
            )
        command = _background_command(command, self.config.background_nice)
        try:
            _run_command(command, "Could not mix ScreenCaptureKit meeting audio.", timeout_seconds=120)
        except ToolError as error:
            media["audio_finalization_warning"] = str(error)
            record["media"] = media
            return record
        if not audio_path.exists() or audio_path.stat().st_size <= 44:
            media["audio_finalization_warning"] = "ScreenCaptureKit audio mix produced no audio bytes."
            record["media"] = media
            return record
        media.update(
            {
                "audio_path": str(audio_path.relative_to(self.store.root_dir)),
                "audio_url": f"/meeting-recorder/assets/{record_id}/meeting.wav",
                "audio_format": "wav",
                "audio_sample_rate": 16000,
                "audio_channels": 1,
                "audio_mix_command": _redact_command(command),
                "audio_original_duration_seconds": _audio_duration_seconds(audio_path),
            }
        )
        media.pop("audio_finalization_warning", None)
        record["media"] = media
        return record

    def _finalize_audio_only_recording(self, record: dict[str, Any]) -> dict[str, Any]:
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        if str(media.get("recording_mode") or "") != MEETING_RECORDING_MODE_AUDIO_ONLY:
            return record
        relative = str(media.get("audio_path") or "").strip()
        if not relative:
            return record
        audio_path = (self.store.root_dir / relative).resolve()
        if not audio_path.exists() or not audio_path.is_file():
            return record
        elapsed_seconds = _recording_elapsed_seconds(record)
        duration_seconds = _audio_duration_seconds(audio_path)
        if not elapsed_seconds or duration_seconds <= 0:
            return record
        minimum_expected_seconds = max(10.0, elapsed_seconds * 0.7)
        if duration_seconds >= minimum_expected_seconds:
            return record
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            media = dict(media)
            media["audio_finalization_warning"] = "ffmpeg is required to pad short meeting audio after Stop."
            record["media"] = media
            return record
        record_id = str(record.get("record_id") or "")
        record_dir = self.store.record_dir(record_id)
        output_path = record_dir / "meeting.padded.wav"
        command = _build_ffmpeg_audio_post_stop_pad_command(
            ffmpeg_path=ffmpeg_path,
            source_path=audio_path,
            output_path=output_path,
            target_duration_seconds=elapsed_seconds,
        )
        media = dict(media)
        try:
            _run_command(
                command,
                "Could not finalize meeting audio timeline.",
                timeout_seconds=max(60, int(elapsed_seconds) + 30),
            )
        except ToolError as error:
            media["audio_finalization_warning"] = str(error)
            record["media"] = media
            return record
        if not output_path.exists() or output_path.stat().st_size <= 0:
            media["audio_finalization_warning"] = "Meeting audio finalization produced no audio bytes."
            record["media"] = media
            return record
        media.update(
            {
                "source_audio_path": relative,
                "audio_path": str(output_path.relative_to(self.store.root_dir)),
                "audio_url": f"/meeting-recorder/assets/{record_id}/meeting.padded.wav",
                "audio_finalization_profile": MEETING_AUDIO_POST_STOP_PAD_PROFILE,
                "audio_finalized_at": _utc_now(),
                "audio_finalization_command": _redact_command(command),
                "audio_original_duration_seconds": duration_seconds,
                "audio_target_duration_seconds": elapsed_seconds,
            }
        )
        media.pop("audio_finalization_warning", None)
        record["media"] = media
        return record

    def _terminate_persisted_recorder_process(self, record: dict[str, Any]) -> None:
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        candidates = _recorder_process_candidates(record=record, store_root=self.store.root_dir)
        pid = _safe_int(media.get("recorder_pid"))
        if pid and _pid_command_contains(pid, candidates):
            _terminate_process_id(pid)
            return
        for candidate_pid in _find_recorder_processes_for_paths(candidates):
            _terminate_process_id(candidate_pid)

    def repair_video_playback(self, *, record_id: str, owner_email: str) -> dict[str, Any]:
        record = self.store.get_record(record_id)
        _assert_record_owner(record, owner_email)
        raise ToolError("Video recording and playback repair are no longer supported. Meeting Recorder is audio-only.")

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
        relative = str(media.get("audio_path") or "").strip()
        noun = "audio"
        if not relative:
            return {"status": "unknown", "checked_at": _utc_now(), "warning": f"Recorded {noun} path is missing."}
        media_path = (self.store.root_dir / relative).resolve()
        if not media_path.exists():
            return {"status": "missing", "checked_at": _utc_now(), "warning": f"Recorded {noun} file is missing."}
        byte_key = "audio_bytes"
        media_bytes = media_path.stat().st_size
        media_duration_seconds = _audio_duration_seconds(media_path)
        elapsed_seconds = _recording_elapsed_seconds(record)
        source_duration_seconds = _safe_float(media.get("audio_original_duration_seconds"))
        source_relative = str(media.get("source_audio_path") or "").strip()
        if source_relative and source_duration_seconds is None:
            source_path = (self.store.root_dir / source_relative).resolve()
            if source_path.exists():
                source_duration_seconds = _audio_duration_seconds(source_path)
        status = "ok" if media_bytes > 0 else "warning"
        warning = "" if media_bytes > 0 else f"Recorded {noun} file is empty."
        volume_metrics = _audio_volume_metrics(media_path) if media_bytes > 0 else {}
        mean_volume_db = volume_metrics.get("mean_volume_db")
        max_volume_db = volume_metrics.get("max_volume_db")
        capture_profile = str(media.get("audio_capture_profile") or "")
        capture_source = str(media.get("browser_audio_capture_source") or "")
        is_linked_browser_capture = (
            capture_profile == MEETING_BROWSER_AUDIO_PROFILE
            and capture_source == "browser_tab_audio_linked"
        )
        if media_duration_seconds is not None and elapsed_seconds:
            minimum_expected_seconds = elapsed_seconds * 0.7
            if is_linked_browser_capture and elapsed_seconds < MEETING_BROWSER_LINKED_MIN_SECONDS:
                status = "failed"
                warning = (
                    f"Browser meeting-tab recording lasted only {elapsed_seconds:.0f}s. "
                    "This is too short to be a useful meeting recording; keep the Zoom/Meet tab audio share active "
                    "and do not stop until the meeting audio has played."
                )
            elif source_duration_seconds is not None and source_duration_seconds < minimum_expected_seconds:
                status = "failed"
                warning = (
                    f"Recorder captured only {source_duration_seconds:.0f}s of source audio for a "
                    f"{elapsed_seconds:.0f}s recording. The file was padded with silence after the recorder stopped early. "
                    "Check the microphone input and start a new recording."
                )
            elif elapsed_seconds >= 10.0 and media_duration_seconds < minimum_expected_seconds:
                status = "warning"
                warning = (
                    f"Recorded audio is only {media_duration_seconds:.0f}s for a {elapsed_seconds:.0f}s recording. "
                    "Check the microphone input and start a new recording."
                )
            elif media_duration_seconds > elapsed_seconds * 1.3 + 10:
                status = "warning"
                warning = (
                    f"Recorded audio is {media_duration_seconds:.0f}s for a {elapsed_seconds:.0f}s recording. "
                    "The recorder process may have continued after Stop."
                )
        if max_volume_db is not None and max_volume_db <= MEETING_AUDIO_NEAR_SILENT_MAX_DB:
            status = "warning"
            warning = (
                "Recorded audio is silent or nearly silent. Chrome/macOS is likely using a muted or virtual microphone input. "
                "Check the microphone input and start a new recording."
            )
        return {
            "status": status,
            "checked_at": _utc_now(),
            byte_key: media_bytes,
            "duration_seconds": media_duration_seconds,
            "source_duration_seconds": source_duration_seconds,
            "elapsed_seconds": elapsed_seconds,
            "mean_volume_db": mean_volume_db,
            "max_volume_db": max_volume_db,
            "warning": warning,
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
            audio_path = self._recorded_audio_path(record)
            transcript = self._transcribe_audio(audio_path, transcript_language=record.get("transcript_language"))
            owner_speech = self._transcribe_owner_speech_candidates(record, transcript_language=record.get("transcript_language"))
            transcript_text = str(transcript.get("text") or "").strip()
            minutes = self._generate_minutes(
                record=record,
                transcript_text=transcript_text,
                transcript_quality=transcript.get("quality") or {},
            )
            owner_speech_asset_url = ""
            if owner_speech.get("status") == "completed" and str(owner_speech.get("text") or "").strip():
                owner_speech_asset_url = f"/meeting-recorder/assets/{record_id}/owner-microphone-transcript.txt"
                (self.store.record_dir(record_id) / "owner-microphone-transcript.txt").write_text(
                    str(owner_speech.get("text") or "").strip(),
                    encoding="utf-8",
                )
            record["transcript"] = {
                "status": "completed",
                "text": transcript_text,
                "chunks": transcript.get("chunks") or [{"start_seconds": 0, "text": transcript_text}],
                "segments": transcript.get("segments") or [],
                "quality": transcript.get("quality") or {},
                "owner_speech_status": owner_speech.get("status") or "skipped",
                "owner_speech_candidates": owner_speech.get("chunks") or [],
                "owner_speech_quality": owner_speech.get("quality") or {},
                "owner_speech_warning": owner_speech.get("warning") or "",
                "owner_speech_asset_url": owner_speech_asset_url,
                "asset_url": f"/meeting-recorder/assets/{record_id}/transcript.txt",
            }
            (self.store.record_dir(record_id) / "transcript.txt").write_text(transcript_text, encoding="utf-8")
            record["visual_evidence"] = []
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
        html_body = _meeting_minutes_markdown_to_html(minutes, portal_url=portal_url)
        attachments = self._transcript_email_attachments(record_id=record_id, record=record)
        result = send_gmail_message(
            credentials=credentials,
            sender=owner_email,
            recipient=target,
            subject=subject,
            text_body=body,
            html_body=html_body,
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
            "-fflags",
            "+genpts",
            "-i",
            str(video_path),
            "-map",
            "0:a:0",
            "-vn",
            "-af",
            "aresample=async=1:first_pts=0",
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

    def _transcribe_audio(
        self,
        audio_path: Path,
        *,
        transcript_language: str | None = None,
        output_prefix: str = "whisper-transcript",
        segment_audio_prefix: str = "audio-segment",
        segment_output_prefix: str = "whisper-segment",
    ) -> dict[str, Any]:
        started_at = time.monotonic()
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
                transcript_language=transcript_language,
                segment_audio_prefix=segment_audio_prefix,
                segment_output_prefix=segment_output_prefix,
            )
        output_base = self.store.record_dir(audio_path.parent.name) / output_prefix
        configured_language = meeting_transcript_whisper_language(transcript_language, fallback=self.config.whisper_language)
        return self._transcribe_audio_with_language_selection(
            audio_path=audio_path,
            output_base=output_base,
            whisper_bin=whisper_bin,
            model_path=model_path,
            configured_language=configured_language,
            offset_seconds=0,
            whisper_threads=self._whisper_threads(segment_workers=1),
            started_at=started_at,
            duration_seconds=duration,
        )

    def _transcribe_audio_with_language_selection(
        self,
        *,
        audio_path: Path,
        output_base: Path,
        whisper_bin: str,
        model_path: Path,
        configured_language: str,
        offset_seconds: float,
        whisper_threads: int,
        started_at: float | None = None,
        duration_seconds: float | None = None,
    ) -> dict[str, Any]:
        language = configured_language or "auto"
        transcript = self._transcribe_audio_once(
            audio_path=audio_path,
            output_base=output_base,
            whisper_bin=whisper_bin,
            model_path=model_path,
            language=language,
            offset_seconds=offset_seconds,
            whisper_threads=whisper_threads,
        )
        segments, quality = self._transcript_quality(
            audio_path=audio_path,
            chunks=transcript["chunks"],
            detected_language=transcript["language"],
        )
        candidate = {
            "text": transcript["text"],
            "chunks": transcript["chunks"],
            "segments": segments,
            "quality": quality,
            "language": quality.get("language") or transcript["language"],
            "language_retry_count": 0,
        }
        if configured_language not in {"", "auto"} or not _should_retry_transcript_languages(quality):
            self._attach_transcript_telemetry(
                candidate,
                duration_seconds=duration_seconds,
                segment_count=1,
                segment_workers=1,
                whisper_threads=whisper_threads,
                started_at=started_at,
            )
            return candidate

        original_language = quality.get("language") or transcript["language"]
        candidates = [candidate]
        retry_count = 0
        for retry_language in ("zh", "en"):
            retry_base = output_base.with_name(f"{output_base.name}-{retry_language}")
            retry_transcript = self._transcribe_audio_once(
                audio_path=audio_path,
                output_base=retry_base,
                whisper_bin=whisper_bin,
                model_path=model_path,
                language=retry_language,
                offset_seconds=offset_seconds,
                whisper_threads=whisper_threads,
            )
            retry_count += 1
            retry_segments, retry_quality = self._transcript_quality(
                audio_path=audio_path,
                chunks=retry_transcript["chunks"],
                detected_language=retry_transcript["language"],
            )
            retry_quality["retry_language"] = retry_language
            retry_quality["original_language"] = original_language
            candidates.append(
                {
                    "text": retry_transcript["text"],
                    "chunks": retry_transcript["chunks"],
                    "segments": retry_segments,
                    "quality": retry_quality,
                    "language": retry_quality.get("language") or retry_transcript["language"],
                    "language_retry_count": retry_count,
                }
            )
        best = max(candidates, key=lambda item: _transcript_quality_score(item.get("quality") or {}))
        best["language_retry_count"] = retry_count
        best_quality = best.get("quality") or {}
        if best is not candidate:
            best_quality["warnings"] = [
                *best_quality.get("warnings", []),
                f"Auto language transcription looked unreliable, so the accepted transcript was retried with {best_quality.get('retry_language')}.",
            ]
        else:
            best_quality["retry_language"] = "zh,en"
            best_quality["original_language"] = original_language
            best_quality["warnings"] = [
                *best_quality.get("warnings", []),
                "Auto language transcription looked unreliable; Chinese and English retries did not improve transcript quality.",
            ]
        self._attach_transcript_telemetry(
            best,
            duration_seconds=duration_seconds,
            segment_count=1,
            segment_workers=1,
            whisper_threads=whisper_threads,
            started_at=started_at,
        )
        return best

    def _transcribe_audio_once(
        self,
        *,
        audio_path: Path,
        output_base: Path,
        whisper_bin: str,
        model_path: Path,
        language: str,
        offset_seconds: float,
        whisper_threads: int,
    ) -> dict[str, Any]:
        command = [
            whisper_bin,
            "-m",
            str(model_path),
            "-f",
            str(audio_path),
            "-t",
            str(max(1, int(whisper_threads or 1))),
            "-otxt",
            "-osrt",
            "-of",
            str(output_base),
        ]
        if language:
            command.extend(["-l", language])
        command = _background_command(command, self.config.background_nice)
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
        transcript_language: str | None = None,
        segment_audio_prefix: str = "audio-segment",
        segment_output_prefix: str = "whisper-segment",
    ) -> dict[str, Any]:
        ffmpeg_path = _resolve_ffmpeg_bin(self.config.ffmpeg_bin)
        if not ffmpeg_path:
            raise ConfigError("ffmpeg is required to split meeting audio for mixed-language transcription.")
        record_dir = self.store.record_dir(audio_path.parent.name)
        segment_seconds = MEETING_TRANSCRIPT_SEGMENT_SECONDS
        segment_workers = self._transcript_segment_workers()
        whisper_threads = self._whisper_threads(segment_workers=segment_workers)
        configured_language = meeting_transcript_whisper_language(transcript_language, fallback=self.config.whisper_language)
        started_at = time.monotonic()
        segment_specs = []
        for index, start in enumerate(range(0, int(duration_seconds + segment_seconds - 1), segment_seconds)):
            remaining = max(0.0, duration_seconds - start)
            if remaining <= 0:
                continue
            current_duration = min(segment_seconds, remaining)
            segment_specs.append((index, start, current_duration))

        def process_segment(spec: tuple[int, int, float]) -> dict[str, Any]:
            index, start, current_duration = spec
            segment_audio = record_dir / f"{segment_audio_prefix}-{index:04d}.wav"
            output_base = record_dir / f"{segment_output_prefix}-{index:04d}"
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
            extract_command = _background_command(extract_command, self.config.background_nice)
            _run_command(extract_command, "Could not split meeting audio for transcription.")
            transcript = self._transcribe_audio_with_language_selection(
                audio_path=segment_audio,
                output_base=output_base,
                whisper_bin=whisper_bin,
                model_path=model_path,
                configured_language=configured_language,
                offset_seconds=float(start),
                whisper_threads=whisper_threads,
            )
            chunks = transcript.get("chunks") or []
            metrics = _audio_volume_metrics(audio_path, start_seconds=float(start), duration_seconds=current_duration)
            has_no_audio = any("[no audio]" in str(chunk.get("text") or "").lower() for chunk in chunks)
            low_audio = bool(metrics.get("low_audio")) or has_no_audio
            startup_silence = low_audio and float(start) < _normalized_startup_silence_grace_seconds(self.config.startup_silence_grace_seconds)
            return {
                "index": index,
                "start_seconds": float(start),
                "end_seconds": float(start + current_duration),
                "language": transcript.get("language") or configured_language or "auto",
                "language_retry_count": int(transcript.get("language_retry_count") or 0),
                "text": str(transcript.get("text") or "").strip(),
                "chunks": chunks,
                "metadata": {
                    "index": index,
                    "start_seconds": float(start),
                    "end_seconds": float(start + current_duration),
                    "language": transcript.get("language") or configured_language or "auto",
                    "language_confidence": None,
                    "mean_volume_db": metrics.get("mean_volume_db"),
                    "max_volume_db": metrics.get("max_volume_db"),
                    "quality": "low_audio" if low_audio else "ok",
                    "possible_missed_speech": low_audio,
                    "no_audio": has_no_audio,
                    "startup_silence": startup_silence,
                    "chunk_count": len(chunks),
                },
            }

        if segment_workers == 1:
            segment_results = [process_segment(spec) for spec in segment_specs]
        else:
            with ThreadPoolExecutor(max_workers=segment_workers) as executor:
                segment_results = list(executor.map(process_segment, segment_specs))

        all_chunks: list[dict[str, Any]] = []
        text_parts: list[str] = []
        segment_metadata: list[dict[str, Any]] = []
        language_retry_count = 0
        for result in sorted(segment_results, key=lambda item: int(item.get("index") or 0)):
            text_parts.append(str(result.get("text") or "").strip())
            chunks = result.get("chunks") or []
            all_chunks.extend(chunks)
            language_retry_count += int(result.get("language_retry_count") or 0)
            segment_metadata.append(result["metadata"])
        text = "\n".join(part for part in text_parts if part).strip()
        if not text:
            raise ToolError("whisper.cpp transcription produced no text.")
        quality = self._quality_from_segments(segment_metadata, chunks=all_chunks)
        transcript = {
            "text": text,
            "chunks": all_chunks,
            "segments": segment_metadata,
            "quality": quality,
            "language_retry_count": language_retry_count,
        }
        self._attach_transcript_telemetry(
            transcript,
            duration_seconds=duration_seconds,
            segment_count=len(segment_specs),
            segment_workers=segment_workers,
            whisper_threads=whisper_threads,
            started_at=started_at,
        )
        return transcript

    def _transcribe_owner_speech_candidates(self, record: dict[str, Any], *, transcript_language: str | None = None) -> dict[str, Any]:
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        if str(media.get("audio_capture_profile") or "") != MEETING_SCREENCAPTUREKIT_AUDIO_PROFILE:
            return {"status": "skipped", "warning": "No separate local microphone track is available for owner speech candidates."}
        relative = str(media.get("microphone_audio_path") or "").strip()
        if not relative:
            return {"status": "skipped", "warning": "No local microphone track is available for owner speech candidates."}
        microphone_path = (self.store.root_dir / relative).resolve()
        if not microphone_path.exists() or not microphone_path.is_file() or microphone_path.stat().st_size <= 44:
            return {"status": "skipped", "warning": "Local microphone track is empty or missing."}
        try:
            duration_seconds = _audio_duration_seconds(microphone_path)
        except Exception:
            duration_seconds = 0
        if not duration_seconds or duration_seconds <= 0:
            return {"status": "skipped", "warning": "Local microphone track has no usable audio duration."}
        try:
            transcript = self._transcribe_audio(
                microphone_path,
                transcript_language=transcript_language,
                output_prefix="owner-microphone-transcript",
                segment_audio_prefix="owner-microphone-segment",
                segment_output_prefix="owner-whisper-segment",
            )
        except Exception as error:  # noqa: BLE001
            return {"status": "failed", "warning": f"Could not transcribe local microphone owner speech candidates: {error}"}

        chunks = []
        for chunk in transcript.get("chunks") or []:
            text = str(chunk.get("text") or "").strip()
            if not text or "[no audio]" in text.casefold():
                continue
            chunks.append(
                {
                    **dict(chunk),
                    "text": text,
                    "speaker": "me_candidate",
                    "speaker_source": "local_microphone",
                    "speaker_confidence": "candidate",
                    "attribution_note": "Candidate owner speech from the local microphone track; not diarized speaker proof.",
                }
            )
        return {
            "status": "completed" if chunks else "empty",
            "text": "\n".join(str(chunk.get("text") or "").strip() for chunk in chunks),
            "chunks": chunks,
            "segments": transcript.get("segments") or [],
            "quality": {
                **(transcript.get("quality") or {}),
                "speaker_source": "local_microphone",
                "speaker_confidence": "candidate",
                "capture_source": media.get("screencapture_capture_source") or "",
            },
        }

    def _transcript_segment_workers(self) -> int:
        return _effective_transcript_segment_workers(self.config.transcript_segment_workers)

    def _whisper_threads(self, *, segment_workers: int) -> int:
        return _effective_whisper_threads(
            whisper_threads=self.config.whisper_threads,
            segment_workers=segment_workers,
        )

    def _attach_transcript_telemetry(
        self,
        transcript: dict[str, Any],
        *,
        duration_seconds: float | None,
        segment_count: int,
        segment_workers: int,
        whisper_threads: int,
        started_at: float | None,
    ) -> None:
        quality = transcript.setdefault("quality", {})
        quality["duration_seconds"] = float(duration_seconds or 0)
        quality["segment_count"] = int(segment_count)
        quality["segment_workers"] = int(segment_workers)
        quality["whisper_threads"] = int(whisper_threads)
        elapsed = round(max(0.0, time.monotonic() - started_at), 3) if started_at else None
        quality["transcribe_elapsed_seconds"] = elapsed
        quality["transcribe_realtime_ratio"] = round(elapsed / float(duration_seconds or 0), 4) if elapsed is not None and float(duration_seconds or 0) > 0 else None
        quality["language_retry_count"] = int(transcript.get("language_retry_count") or 0)

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
            startup_silence = low_audio and start < _normalized_startup_silence_grace_seconds(self.config.startup_silence_grace_seconds)
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
                    "no_audio": has_no_audio,
                    "startup_silence": startup_silence,
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
        no_audio_count = sum(1 for segment in segments if segment.get("no_audio"))
        startup_silence_count = sum(1 for segment in segments if segment.get("startup_silence"))
        risk_low_audio_count = sum(1 for segment in segments if segment.get("quality") == "low_audio" and not segment.get("startup_silence"))
        risk_no_audio_count = sum(1 for segment in segments if segment.get("no_audio") and not segment.get("startup_silence"))
        all_segments_startup_silence = bool(segments) and startup_silence_count == len(segments)
        all_segments_no_audio = bool(segments) and all(bool(segment.get("no_audio")) for segment in segments)
        recording_duration = max((float(segment.get("end_seconds") or 0) for segment in segments), default=0.0)
        short_all_no_audio = all_segments_startup_silence and all_segments_no_audio and recording_duration <= _normalized_startup_silence_grace_seconds(self.config.startup_silence_grace_seconds)
        if short_all_no_audio:
            risk_no_audio_count = max(risk_no_audio_count, no_audio_count)
        languages = sorted({str(segment.get("language") or "").strip() for segment in segments if segment.get("language")})
        language = ",".join(languages) or default_language
        language_codes = {code.strip().lower() for code in language.split(",") if code.strip()}
        unusual_language = bool(language_codes) and not language_codes.issubset(MEETING_TRANSCRIPT_EXPECTED_LANGUAGES)
        repetition = _transcript_repetition_metrics(chunks or [])
        warnings = []
        if risk_low_audio_count:
            warnings.append("Some transcript segments have low audio and may miss Zoom/system audio or microphone speech.")
        if risk_no_audio_count:
            warnings.append("One or more transcript segments contain [no audio] and may miss speech.")
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
            "startup_silence_segment_count": startup_silence_count,
            "risk_low_audio_segment_count": risk_low_audio_count,
            "risk_no_audio_segment_count": risk_no_audio_count,
            "repetitive_chunk_count": repetition.get("repetitive_chunk_count", 0),
            "unique_text_ratio": repetition.get("unique_text_ratio"),
            "dominant_chunk_ratio": repetition.get("dominant_chunk_ratio"),
            "possible_incomplete": bool(risk_low_audio_count or risk_no_audio_count or unusual_language or repetition.get("is_repetitive")),
            "warnings": warnings,
        }

    def _generate_minutes(self, *, record: dict[str, Any], transcript_text: str, transcript_quality: dict[str, Any] | None = None) -> str:
        quality = transcript_quality or {}
        system_prompt = (
            "You write concise, evidence-grounded English meeting minutes for product managers. "
            "Use only the provided spoken transcript and meeting metadata. "
            "Do not use screen recordings, screenshots, keyframes, or visual context. "
            "Do not invent owners, decisions, dates, deadlines, or action items. "
            "If a next step is implied but owner, timing, or decision is unclear, write it as a [Follow up] item instead of pretending it is confirmed. "
            "If transcript quality warnings are present, include a short warning before Key Discussion Topics and do not infer decisions or follow-ups from repeated low-audio text."
        )
        user_prompt = (
            f"Meeting title: {record.get('title')}\n"
            f"Platform: {record.get('platform')}\n"
            f"Scheduled start: {record.get('scheduled_start')}\n"
            f"Attendees from calendar: {json.dumps(record.get('attendees') or [], ensure_ascii=False)}\n"
            f"Transcript quality: {json.dumps(quality, ensure_ascii=False)}\n\n"
            "# Transcript\n"
            f"{transcript_text or 'No transcript text was produced.'}\n\n"
            "Return English Markdown in this exact PM-readable format:\n"
            "## Key Discussion Topics\n"
            "- **Topic Name**\n"
            "  - Factual discussion point grounded in the transcript.\n"
            "  - [Follow up] Action, question, or check needed before the next discussion.\n\n"
            "Rules:\n"
            "- Group related points under 4-8 concise topic bullets.\n"
            "- Use top-level bullets only for topic names and make every topic name bold.\n"
            "- Use indented second-level bullets for details under each topic.\n"
            "- Prefix uncertain next steps, unresolved questions, or checks with [Follow up].\n"
            "- Do not return separate Summary, Decisions, Action Items, Risks/Blockers, Open Questions, or Follow-ups sections.\n"
            "- Do not include transcript excerpts unless needed for clarity."
        )
        return self.text_client.create_answer(system_prompt=system_prompt, user_prompt=user_prompt).strip()


def cleanup_legacy_video_assets(
    *,
    store: MeetingRecordStore,
    config: MeetingRecorderConfig,
    owner_email: str = "",
) -> dict[str, Any]:
    """Convert legacy screen-video records to audio-only metadata and remove video assets."""
    records = store.list_records(owner_email=owner_email) if owner_email else _load_all_records(store)
    summary = {
        "checked": 0,
        "updated": 0,
        "audio_extracted": 0,
        "video_assets_deleted": 0,
        "warnings": [],
    }
    ffmpeg_path = _resolve_ffmpeg_bin(config.ffmpeg_bin)
    for record in records:
        summary["checked"] += 1
        media = record.get("media") if isinstance(record.get("media"), dict) else {}
        if not _record_has_legacy_video(media, record):
            continue
        record_id = str(record.get("record_id") or "").strip()
        record_dir = store.record_dir(record_id)
        media = dict(media)
        warnings = list(record.get("cleanup_warnings") or [])
        audio_relative = str(media.get("audio_path") or "").strip()
        video_relative = str(media.get("video_path") or "").strip()
        video_path = (store.root_dir / video_relative).resolve() if video_relative else None
        audio_path = (store.root_dir / audio_relative).resolve() if audio_relative else None

        if (not audio_path or not audio_path.exists()) and video_path and video_path.exists() and ffmpeg_path:
            output_path = record_dir / "meeting.wav"
            command = _build_ffmpeg_legacy_video_audio_extract_command(
                ffmpeg_path=ffmpeg_path,
                video_path=video_path,
                audio_path=output_path,
            )
            try:
                _run_command(command, "Could not extract audio from legacy meeting video.", timeout_seconds=7200)
                if output_path.exists() and output_path.stat().st_size > 0:
                    media.update(
                        {
                            "audio_path": str(output_path.relative_to(store.root_dir)),
                            "audio_url": f"/meeting-recorder/assets/{record_id}/meeting.wav",
                            "audio_format": "wav",
                            "audio_sample_rate": 16000,
                            "audio_channels": 1,
                            "legacy_video_audio_extracted_at": _utc_now(),
                            "legacy_video_audio_extract_command": _redact_command(command),
                        }
                    )
                    summary["audio_extracted"] += 1
                else:
                    warnings.append("Legacy video audio extraction produced no audio bytes.")
            except ToolError as error:
                warnings.append(str(error))
        elif (not audio_path or not audio_path.exists()) and video_path and video_path.exists() and not ffmpeg_path:
            warnings.append("ffmpeg is required to extract audio from legacy meeting video before deleting it.")

        for key in ("video_path", "playback_video_path"):
            deleted = _delete_legacy_media_path(store=store, record_dir=record_dir, relative_path=str(media.get(key) or ""))
            if deleted:
                summary["video_assets_deleted"] += 1
        keyframes_dir = record_dir / "keyframes"
        if keyframes_dir.exists():
            shutil.rmtree(keyframes_dir, ignore_errors=True)
            summary["video_assets_deleted"] += 1

        for key in (
            "video_path",
            "video_url",
            "playback_video_path",
            "playback_video_url",
            "playback_repair_command",
            "playback_profile",
            "playback_audio_channels",
            "playback_audio_sample_rate",
            "playback_repaired_at",
            "playback_bytes",
        ):
            media.pop(key, None)
        media["recording_mode"] = MEETING_RECORDING_MODE_AUDIO_ONLY
        media["legacy_video_cleanup_at"] = _utc_now()
        record["media"] = media
        record["visual_evidence"] = []
        if warnings:
            record["cleanup_warnings"] = warnings
            summary["warnings"].append({"record_id": record_id, "warnings": warnings})
        store.save_record(record)
        summary["updated"] += 1
    return summary


def _load_all_records(store: MeetingRecordStore) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for metadata_path in sorted(store.records_dir.glob("*/metadata.json"), reverse=True):
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _record_has_legacy_video(media: dict[str, Any], record: dict[str, Any]) -> bool:
    return bool(
        media.get("video_path")
        or media.get("video_url")
        or media.get("playback_video_path")
        or media.get("playback_video_url")
        or record.get("visual_evidence")
    )


def _delete_legacy_media_path(*, store: MeetingRecordStore, record_dir: Path, relative_path: str) -> bool:
    relative = str(relative_path or "").strip()
    if not relative:
        return False
    candidate = (store.root_dir / relative).resolve()
    root = record_dir.resolve()
    if root not in candidate.parents and candidate != root:
        candidate = (root / Path(relative).name).resolve()
    if root not in candidate.parents and candidate != root:
        return False
    if candidate.exists() and candidate.is_file():
        candidate.unlink(missing_ok=True)
        return True
    return False


def _assert_record_owner(record: dict[str, Any], owner_email: str) -> None:
    owner = str(owner_email or "").strip().lower()
    if not owner or str(record.get("owner_email") or "").strip().lower() != owner:
        raise ToolError("Meeting record is not available for this Google account.")


def _normalized_background_nice(value: object) -> int:
    try:
        configured = int(value or 0)
    except (TypeError, ValueError):
        configured = 0
    return max(0, min(configured, 20))


def _normalized_status_every_buffers(value: object) -> int:
    try:
        configured = int(value or 250)
    except (TypeError, ValueError):
        configured = 250
    return max(1, configured)


def _normalized_startup_silence_grace_seconds(value: object) -> int:
    try:
        configured = int(value or 300)
    except (TypeError, ValueError):
        configured = 300
    return max(0, configured)


def _background_command(command: list[str], nice_value: object) -> list[str]:
    normalized = _normalized_background_nice(nice_value)
    if normalized <= 0 or not shutil.which("nice"):
        return command
    return ["nice", "-n", str(normalized), *command]


def _effective_transcript_segment_workers(configured_workers: object) -> int:
    try:
        configured = int(configured_workers or 1)
    except (TypeError, ValueError):
        configured = 1
    cpu_count = os.cpu_count() or configured
    return max(1, min(configured, cpu_count))


def _effective_whisper_threads(*, whisper_threads: object, segment_workers: object) -> int:
    try:
        configured = int(whisper_threads or 0)
    except (TypeError, ValueError):
        configured = 0
    if configured > 0:
        return configured
    cpu_count = os.cpu_count() or 1
    try:
        workers = int(segment_workers or 1)
    except (TypeError, ValueError):
        workers = 1
    return max(1, cpu_count // max(1, workers))


def _run_command(command: list[str], error_message: str, *, timeout_seconds: int = 600) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout_seconds)  # noqa: S603
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ToolError(f"{error_message} {error}") from error
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[-1200:]
        raise ToolError(f"{error_message} {detail}".strip())
    return completed


def _meeting_minutes_inline_html(value: str) -> str:
    escaped = html.escape(str(value or ""))
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
    return re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)


def _meeting_minutes_markdown_to_html(markdown: str, *, portal_url: str) -> str:
    lines = str(markdown or "").splitlines()
    body: list[str] = []
    list_depth = 0

    def close_lists(target_depth: int = 0) -> None:
        nonlocal list_depth
        while list_depth > target_depth:
            body.append("</ul>")
            list_depth -= 1

    for line in lines:
        if not line.strip():
            close_lists()
            continue
        heading = line.strip()
        heading_match = re.match(r"^#{1,6}\s+(.+)$", heading)
        if heading_match:
            close_lists()
            body.append(f"<h3>{_meeting_minutes_inline_html(heading_match.group(1).strip())}</h3>")
            continue
        bullet_match = re.match(r"^(\s*)[-*]\s+(.+)$", line)
        if bullet_match:
            depth = 2 if len(bullet_match.group(1).replace("\t", "  ")) >= 2 else 1
            while list_depth < depth:
                body.append("<ul>")
                list_depth += 1
            close_lists(depth)
            body.append(f"<li>{_meeting_minutes_inline_html(bullet_match.group(2).strip())}</li>")
            continue
        close_lists()
        body.append(f"<p>{_meeting_minutes_inline_html(heading)}</p>")
    close_lists()
    if portal_url:
        safe_url = html.escape(portal_url, quote=True)
        body.append(f'<p>Full transcript and recording archive: <a href="{safe_url}">{safe_url}</a></p>')
    return "<div>" + "\n".join(body) + "</div>"


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


class _SegmentedAudioRecorder:
    def __init__(
        self,
        *,
        ffmpeg_path: str,
        audio_input: str,
        segment_dir: Path,
        log_path: Path,
        segment_seconds: int,
    ) -> None:
        self.ffmpeg_path = ffmpeg_path
        self.audio_input = audio_input
        self.segment_dir = segment_dir
        self.log_path = log_path
        self.segment_seconds = max(5, int(segment_seconds or MEETING_AUDIO_SEGMENT_SECONDS))
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._current_process: subprocess.Popen[str] | None = None
        self._current_segment: Path | None = None
        self._next_index = 0
        self._restart_count = 0
        self._failures: list[dict[str, Any]] = []
        self.command: list[str] = _build_ffmpeg_audio_segment_command(
            ffmpeg_path=self.ffmpeg_path,
            audio_input=self.audio_input,
            audio_path=self.segment_dir / "segment-000000.wav",
            duration_seconds=self.segment_seconds,
        )

    @property
    def pid(self) -> int:
        with self._lock:
            return _safe_int(getattr(self._current_process, "pid", 0))

    def poll(self) -> int | None:
        if self._thread and self._thread.is_alive():
            return None
        with self._lock:
            if self._current_process is None:
                return 0
            return self._current_process.poll()

    def start(self) -> dict[str, Any]:
        self.segment_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.write_text("", encoding="utf-8")
        first_process, first_segment = self._start_next_process()
        ready = _wait_for_audio_recorder_ready(process=first_process, audio_path=first_segment, log_path=self.log_path)
        if ready.get("status") != "ok":
            _terminate_recorder_process(first_process)
            return ready
        self._thread = threading.Thread(target=self._run, name="meeting-audio-segment-recorder", daemon=True)
        self._thread.start()
        return {**ready, "pid": self.pid}

    def stop(self) -> dict[str, Any]:
        self._stop_event.set()
        with self._lock:
            process = self._current_process
        if process is not None and process.poll() is None:
            _terminate_recorder_process(process)
        if self._thread is not None:
            self._thread.join(timeout=max(5, self.segment_seconds + 5))
        return {
            "audio_segment_count": len(_usable_audio_segments(self.segment_dir)),
            "audio_segment_restart_count": self._restart_count,
            "audio_segment_failures": self._failures[-10:],
            "recorder_pid": self.pid,
        }

    def signal_snapshot(self, *, sample_seconds: float) -> dict[str, Any]:
        sample_seconds = max(0.2, float(sample_seconds or MEETING_AUDIO_EARLY_SIGNAL_SAMPLE_SECONDS))
        with self._lock:
            process = self._current_process
            segment = self._current_segment
            failures = list(self._failures)
            restart_count = self._restart_count
            thread_alive = bool(self._thread and self._thread.is_alive())
        if len(failures) >= 2:
            latest = failures[-1]
            duration = _safe_float(latest.get("duration_seconds")) or 0.0
            return {
                "status": "failed",
                "checked_at": _utc_now(),
                "segment": str(latest.get("segment") or ""),
                "duration_seconds": duration,
                "restart_count": restart_count,
                "failure_count": len(failures),
                "warning": (
                    f"Recorder audio stopped repeatedly; latest segment lasted {duration:.0f}s. "
                    "Check Zoom speaker is Meeting Recorder Output and Zoom microphone is a real microphone, then start a new recording."
                ),
            }
        if process is None or segment is None:
            if thread_alive and failures:
                latest = failures[-1]
                duration = _safe_float(latest.get("duration_seconds")) or 0.0
                return {
                    "status": "pending",
                    "checked_at": _utc_now(),
                    "segment": str(latest.get("segment") or ""),
                    "duration_seconds": duration,
                    "restart_count": restart_count,
                    "failure_count": len(failures),
                    "warning": "Recorder segment ended early; waiting for automatic restart.",
                }
            return {
                "status": "failed",
                "checked_at": _utc_now(),
                "warning": "Recorder process is not running. Start a new recording.",
            }
        before_size = _safe_file_size(segment)
        time.sleep(sample_seconds)
        after_size = _safe_file_size(segment)
        growth = max(0, after_size - before_size)
        exit_code = process.poll()
        if exit_code is not None:
            duration = _audio_duration_seconds(segment) if segment.exists() else 0.0
            if thread_alive and len(failures) < 2:
                return {
                    "status": "pending",
                    "checked_at": _utc_now(),
                    "segment": segment.name,
                    "bytes_before": before_size,
                    "bytes_after": after_size,
                    "growth_bytes": growth,
                    "duration_seconds": duration,
                    "exit_code": exit_code,
                    "restart_count": restart_count,
                    "failure_count": len(failures),
                    "warning": f"Recorder segment ended after {duration:.0f}s; waiting for automatic restart.",
                }
            return {
                "status": "failed",
                "checked_at": _utc_now(),
                "segment": segment.name,
                "bytes_before": before_size,
                "bytes_after": after_size,
                "growth_bytes": growth,
                "duration_seconds": duration,
                "exit_code": exit_code,
                "restart_count": restart_count,
                "failure_count": len(failures),
                "warning": (
                    f"Recorder audio stopped after {duration:.0f}s during startup. "
                    "Check Zoom speaker is Meeting Recorder Output and Zoom microphone is a real microphone, then start a new recording."
                ),
            }
        if after_size < MEETING_AUDIO_EARLY_SIGNAL_MIN_BYTES:
            return {
                "status": "pending",
                "checked_at": _utc_now(),
                "segment": segment.name,
                "bytes_before": before_size,
                "bytes_after": after_size,
                "growth_bytes": growth,
                "warning": "Recorder process is running; waiting for macOS to flush audio samples.",
            }
        if growth < MEETING_AUDIO_EARLY_SIGNAL_MIN_GROWTH_BYTES:
            return {
                "status": "pending",
                "checked_at": _utc_now(),
                "segment": segment.name,
                "bytes_before": before_size,
                "bytes_after": after_size,
                "growth_bytes": growth,
                "warning": "Recorder process is running; audio file size has not flushed yet.",
            }
        return {
            "status": "ok",
            "checked_at": _utc_now(),
            "segment": segment.name,
            "bytes_before": before_size,
            "bytes_after": after_size,
            "growth_bytes": growth,
            "restart_count": restart_count,
            "failure_count": len(failures),
        }

    def _run(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                process = self._current_process
                segment = self._current_segment
            if process is None:
                return
            while not self._stop_event.is_set() and process.poll() is None:
                time.sleep(0.2)
            if self._stop_event.is_set():
                return
            exit_code = process.poll()
            duration = _audio_duration_seconds(segment) if segment and segment.exists() else 0.0
            if duration < max(2.0, self.segment_seconds * 0.6):
                self._restart_count += 1
                self._failures.append(
                    {
                        "segment": segment.name if segment else "",
                        "exit_code": exit_code,
                        "duration_seconds": duration,
                        "logged_at": _utc_now(),
                    }
                )
                time.sleep(0.5)
            try:
                self._start_next_process()
            except OSError as error:
                self._failures.append({"error": str(error), "logged_at": _utc_now()})
                time.sleep(1.0)

    def _start_next_process(self) -> tuple[subprocess.Popen[str], Path]:
        segment_path = self.segment_dir / f"segment-{self._next_index:06d}.wav"
        self._next_index += 1
        command = _build_ffmpeg_audio_segment_command(
            ffmpeg_path=self.ffmpeg_path,
            audio_input=self.audio_input,
            audio_path=segment_path,
            duration_seconds=self.segment_seconds,
        )
        with self.log_path.open("a", encoding="utf-8") as log_handle:
            log_handle.write(f"\n[{_utc_now()}] starting audio segment {segment_path.name}\n")
            log_handle.flush()
            process = subprocess.Popen(  # noqa: S603
                command,
                cwd=str(self.segment_dir.parent),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=log_handle,
                text=True,
                start_new_session=True,
            )
        with self._lock:
            self._current_process = process
            self._current_segment = segment_path
            if self._next_index == 1:
                self.command = command
        return process, segment_path


class _ScreenCaptureKitAudioRecorder:
    def __init__(
        self,
        *,
        helper_path: Path,
        system_audio_path: Path,
        microphone_audio_path: Path,
        status_path: Path,
        log_path: Path,
        background_nice: int = 10,
        status_every_buffers: int = 250,
    ) -> None:
        self.helper_path = helper_path
        self.system_audio_path = system_audio_path
        self.microphone_audio_path = microphone_audio_path
        self.status_path = status_path
        self.log_path = log_path
        self._process: subprocess.Popen[str] | None = None
        self.background_nice = _normalized_background_nice(background_nice)
        self.status_every_buffers = _normalized_status_every_buffers(status_every_buffers)
        self.command = _background_command([
            str(self.helper_path),
            "--system-output",
            str(self.system_audio_path),
            "--microphone-output",
            str(self.microphone_audio_path),
            "--status-output",
            str(self.status_path),
            "--status-every-buffers",
            str(self.status_every_buffers),
        ], self.background_nice)

    @property
    def pid(self) -> int:
        return _safe_int(getattr(self._process, "pid", 0))

    def poll(self) -> int | None:
        return self._process.poll() if self._process is not None else 0

    def start(self) -> dict[str, Any]:
        self.system_audio_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.write_text("", encoding="utf-8")
        log_handle = self.log_path.open("a", encoding="utf-8")
        self._process = subprocess.Popen(
            self.command,
            stdout=log_handle,
            stderr=log_handle,
            text=True,
            close_fds=True,
        )
        started = time.monotonic()
        while time.monotonic() - started < 8.0:
            if self._process.poll() is not None:
                warning = _read_tail(self.log_path) or "ScreenCaptureKit helper exited before recording started."
                return {"status": "failed", "checked_at": _utc_now(), "warning": warning}
            status = _read_json_file(self.status_path)
            if str(status.get("status") or "") == "recording":
                return {
                    "status": "ok",
                    "checked_at": _utc_now(),
                    "latency_seconds": round(time.monotonic() - started, 3),
                    "bytes": _safe_file_size(self.system_audio_path) + _safe_file_size(self.microphone_audio_path),
                    "screencapture_status": status,
                    "pid": self.pid,
                }
            if str(status.get("status") or "") == "failed":
                return {
                    "status": "failed",
                    "checked_at": _utc_now(),
                    "warning": str(status.get("message") or "ScreenCaptureKit helper failed to start."),
                    "screencapture_status": status,
                }
            time.sleep(0.1)
        return {
            "status": "failed",
            "checked_at": _utc_now(),
            "warning": "ScreenCaptureKit helper did not start within 8s. Check macOS Screen Recording and Microphone permissions.",
        }

    def stop(self) -> dict[str, Any]:
        process = self._process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        return {
            "recorder_pid": self.pid,
            "screencapture_status": _read_json_file(self.status_path),
            "screencapture_system_bytes": _safe_file_size(self.system_audio_path),
            "screencapture_microphone_bytes": _safe_file_size(self.microphone_audio_path),
        }

    def signal_snapshot(self, *, sample_seconds: float) -> dict[str, Any]:
        sample_seconds = max(0.2, float(sample_seconds or MEETING_AUDIO_EARLY_SIGNAL_SAMPLE_SECONDS))
        before_system = _safe_file_size(self.system_audio_path)
        before_microphone = _safe_file_size(self.microphone_audio_path)
        time.sleep(sample_seconds)
        after_system = _safe_file_size(self.system_audio_path)
        after_microphone = _safe_file_size(self.microphone_audio_path)
        growth = max(0, after_system - before_system) + max(0, after_microphone - before_microphone)
        status = _read_json_file(self.status_path)
        if self._process is not None and self._process.poll() is not None:
            return {
                "status": "failed",
                "checked_at": _utc_now(),
                "growth_bytes": growth,
                "warning": _read_tail(self.log_path) or str(status.get("message") or "ScreenCaptureKit helper stopped."),
                "screencapture_status": status,
            }
        if growth >= MEETING_AUDIO_EARLY_SIGNAL_MIN_GROWTH_BYTES:
            return {
                "status": "ok",
                "checked_at": _utc_now(),
                "growth_bytes": growth,
                "system_bytes_after": after_system,
                "microphone_bytes_after": after_microphone,
                "screencapture_status": status,
            }
        return {
            "status": "pending",
            "checked_at": _utc_now(),
            "growth_bytes": growth,
            "system_bytes_after": after_system,
            "microphone_bytes_after": after_microphone,
            "warning": "ScreenCaptureKit recorder is running; waiting for system or microphone audio samples.",
            "screencapture_status": status,
        }


def _terminate_recorder_process(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except OSError:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _safe_file_size(path: Path) -> int:
    try:
        return path.stat().st_size if path.exists() else 0
    except OSError:
        return 0


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _read_tail(path: Path, *, max_chars: int = 1200) -> str:
    try:
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8", errors="replace")[-max_chars:].strip()
    except OSError:
        return ""


def _resolve_screencapturekit_helper(store_root: Path) -> Path:
    source_path = Path(__file__).resolve().parents[1] / "tools" / "meeting_screencapture_helper.swift"
    if not source_path.exists():
        raise ConfigError(f"ScreenCaptureKit helper source was not found at {source_path}.")
    output_dir = store_root.parent / "bin"
    app_path = output_dir / "Meeting Recorder Capture Helper.app"
    macos_dir = app_path / "Contents" / "MacOS"
    resources_dir = app_path / "Contents" / "Resources"
    helper_path = macos_dir / "meeting-screencapture-helper"
    info_plist_path = app_path / "Contents" / "Info.plist"
    bundle_is_current = (
        helper_path.exists()
        and info_plist_path.exists()
        and helper_path.stat().st_mtime >= source_path.stat().st_mtime
    )
    if bundle_is_current:
        return helper_path
    swiftc = _resolve_executable("xcrun", ("/usr/bin/xcrun",))
    if not swiftc:
        raise ConfigError("xcrun is required to build the ScreenCaptureKit helper.")
    macos_dir.mkdir(parents=True, exist_ok=True)
    resources_dir.mkdir(parents=True, exist_ok=True)
    command = [
        swiftc,
        "swiftc",
        "-parse-as-library",
        str(source_path),
        "-o",
        str(helper_path),
        "-framework",
        "ScreenCaptureKit",
        "-framework",
        "AVFoundation",
        "-framework",
        "CoreMedia",
    ]
    _run_command(command, "Could not build ScreenCaptureKit helper.", timeout_seconds=120)
    info_plist_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleDisplayName</key>
  <string>Meeting Recorder Capture Helper</string>
  <key>CFBundleExecutable</key>
  <string>meeting-screencapture-helper</string>
  <key>CFBundleIdentifier</key>
  <string>sg.npt.meeting-recorder.capture-helper</string>
  <key>CFBundleName</key>
  <string>Meeting Recorder Capture Helper</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>1.0</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>LSBackgroundOnly</key>
  <true/>
  <key>NSMicrophoneUsageDescription</key>
  <string>Meeting Recorder captures microphone audio for local meeting transcription.</string>
  <key>NSScreenCaptureUsageDescription</key>
  <string>Meeting Recorder captures system audio for local meeting transcription.</string>
</dict>
</plist>
""",
        encoding="utf-8",
    )
    codesign = _resolve_executable("codesign", ("/usr/bin/codesign",))
    if codesign:
        _run_command(
            [codesign, "--force", "--deep", "--sign", "-", str(app_path)],
            "Could not sign ScreenCaptureKit helper app.",
            timeout_seconds=60,
        )
    return helper_path


def _stop_external_audio_monitor(store_root: Path) -> None:
    pid_path = store_root.parent / "run" / "meeting_audio_monitor.pid"
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass
    try:
        pid_path.unlink()
    except OSError:
        pass


def _wait_for_audio_recorder_ready(*, process: subprocess.Popen[str], audio_path: Path, log_path: Path) -> dict[str, Any]:
    started = time.monotonic()
    deadline = started + MEETING_AUDIO_READY_TIMEOUT_SECONDS
    last_size = 0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = log_path.read_text(encoding="utf-8")[-1200:] if log_path.exists() else ""
            return {
                "status": "failed",
                "latency_seconds": round(time.monotonic() - started, 3),
                "bytes": last_size,
                "warning": stderr or "ffmpeg exited before audio recording started.",
            }
        try:
            last_size = audio_path.stat().st_size if audio_path.exists() else 0
        except OSError:
            last_size = 0
        time.sleep(0.2)
    return {
        "status": "ok",
        "latency_seconds": round(time.monotonic() - started, 3),
        "bytes": last_size,
    }


def _browser_audio_extension(mime_type: str) -> str:
    normalized = str(mime_type or "").split(";", 1)[0].strip().lower()
    if normalized in {"audio/wav", "audio/x-wav", "audio/wave"}:
        return ".wav"
    if normalized in {"audio/mp4", "audio/aac", "audio/x-m4a"}:
        return ".m4a"
    if normalized in {"audio/ogg", "application/ogg"}:
        return ".ogg"
    return ".webm"


def _usable_audio_segments(segment_dir: Path) -> list[Path]:
    if not segment_dir.exists() or not segment_dir.is_dir():
        return []
    return [
        path
        for path in sorted(segment_dir.glob("segment-*.wav"))
        if path.is_file() and path.stat().st_size > 44
    ]


def _escape_ffmpeg_concat_path(path: Path) -> str:
    return str(path).replace("'", "'\\''")


def _terminate_process_id(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGTERM)
    except OSError:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            return
    deadline = time.time() + 10
    while time.time() < deadline:
        if not _process_exists(pid):
            return
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        return


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _recorder_process_candidates(*, record: dict[str, Any], store_root: Path) -> list[str]:
    media = record.get("media") if isinstance(record.get("media"), dict) else {}
    candidates: list[str] = []
    for key in ("audio_path", "video_path", "source_audio_path"):
        relative = str(media.get(key) or "").strip()
        if relative:
            candidates.append(str((store_root / relative).resolve()))
            candidates.append(str((store_root / "records" / str(record.get("record_id") or "") / Path(relative).name).resolve()))
    segment_relative = str(media.get("audio_segment_dir") or "").strip()
    if segment_relative:
        candidates.append(str((store_root / segment_relative).resolve()))
    return [candidate for candidate in dict.fromkeys(candidates) if candidate]


def _pid_command_contains(pid: int, candidates: list[str]) -> bool:
    if not candidates:
        return False
    try:
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    command = completed.stdout or ""
    return any(candidate in command for candidate in candidates)


def _find_recorder_processes_for_paths(candidates: list[str]) -> list[int]:
    if not candidates:
        return []
    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    pids: list[int] = []
    for line in (completed.stdout or "").splitlines():
        if "ffmpeg" not in line:
            continue
        if not any(candidate in line for candidate in candidates):
            continue
        parts = line.strip().split(None, 1)
        if not parts:
            continue
        pid = _safe_int(parts[0])
        if pid:
            pids.append(pid)
    return pids


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _recording_elapsed_seconds(record: dict[str, Any]) -> float | None:
    started_at = _parse_utc_timestamp(str(record.get("recording_started_at") or ""))
    stopped_at = _parse_utc_timestamp(str(record.get("recording_stopped_at") or ""))
    if not started_at or not stopped_at:
        return None
    elapsed = (stopped_at - started_at).total_seconds()
    return max(0.0, elapsed)


def _parse_utc_timestamp(value: str) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def _should_retry_transcript_languages(quality: dict[str, Any]) -> bool:
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
    score -= 10 * int(quality.get("risk_low_audio_segment_count", quality.get("low_audio_segment_count")) or 0)
    score -= 10 * int(quality.get("risk_no_audio_segment_count", quality.get("no_audio_segment_count")) or 0)
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
        "-nostdin",
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
        "-nostdin",
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


def _build_ffmpeg_single_audio_convert_command(*, ffmpeg_path: str, source_path: Path, output_path: Path) -> list[str]:
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(source_path),
        "-acodec",
        "pcm_s16le",
        "-ar",
        "16000",
        "-ac",
        "1",
        str(output_path),
    ]


def _build_ffmpeg_screencapturekit_mix_command(
    *,
    ffmpeg_path: str,
    system_path: Path,
    microphone_path: Path,
    output_path: Path,
) -> list[str]:
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(system_path),
        "-i",
        str(microphone_path),
        "-filter_complex",
        (
            "[0:a]aresample=16000,pan=mono|c0=0.5*c0+0.5*c1[system];"
            "[1:a]aresample=16000,pan=mono|c0=c0[mic];"
            "[system][mic]amix=inputs=2:duration=longest:dropout_transition=0[a]"
        ),
        "-map",
        "[a]",
        "-acodec",
        "pcm_s16le",
        "-ar",
        "16000",
        "-ac",
        "1",
        str(output_path),
    ]


def _build_ffmpeg_audio_segment_command(
    *,
    ffmpeg_path: str,
    audio_input: str,
    audio_path: Path,
    duration_seconds: int,
) -> list[str]:
    duration = str(max(5, int(duration_seconds or MEETING_AUDIO_SEGMENT_SECONDS)))
    if str(audio_input or "").strip().lower() == "meeting recorder aggregate":
        return [
            ffmpeg_path,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "warning",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=48000:cl=stereo",
            "-f",
            "avfoundation",
            "-i",
            ":MacBook Air Microphone",
            "-f",
            "avfoundation",
            "-i",
            ":BlackHole 2ch",
            "-filter_complex",
            (
                "[0:a]asplit=2[nullrecsrc][nullmonsrc];"
                "[nullrecsrc]aresample=16000,pan=mono|c0=0.5*c0+0.5*c1[nullrec];"
                "[1:a]aresample=16000,pan=mono|c0=c0[mic];"
                "[2:a]aresample=16000,pan=mono|c0=0.5*c0+0.5*c1[zoom];"
                "[nullrec][mic][zoom]amix=inputs=3:duration=first:dropout_transition=0[rec];"
                "[2:a]aresample=48000[bhmon];"
                "[nullmonsrc][bhmon]amix=inputs=2:duration=first:dropout_transition=0[monitor]"
            ),
            "-map",
            "[rec]",
            "-t",
            duration,
            "-acodec",
            "pcm_s16le",
            "-ar",
            "16000",
            "-ac",
            "1",
            str(audio_path),
            "-map",
            "[monitor]",
            "-t",
            duration,
            "-ac",
            "2",
            "-ar",
            "48000",
            "-audio_device_index",
            MEETING_AUDIO_MONITOR_OUTPUT_DEVICE_INDEX,
            "-f",
            "audiotoolbox",
            "-",
        ]
    return [
        ffmpeg_path,
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "warning",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "anullsrc=r=16000:cl=mono",
        "-f",
        "avfoundation",
        "-i",
        f":{audio_input}",
        "-filter_complex",
        "[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0",
        "-t",
        duration,
        "-acodec",
        "pcm_s16le",
        "-ar",
        "16000",
        "-ac",
        "1",
        str(audio_path),
    ]


def _build_ffmpeg_audio_post_stop_pad_command(
    *,
    ffmpeg_path: str,
    source_path: Path,
    output_path: Path,
    target_duration_seconds: float,
) -> list[str]:
    target = f"{max(0.1, float(target_duration_seconds)):.3f}"
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(source_path),
        "-af",
        f"apad=whole_dur={target}",
        "-t",
        target,
        "-acodec",
        "pcm_s16le",
        "-ar",
        "16000",
        "-ac",
        "1",
        str(output_path),
    ]


def _build_ffmpeg_legacy_video_audio_extract_command(*, ffmpeg_path: str, video_path: Path, audio_path: Path) -> list[str]:
    return [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-fflags",
        "+genpts",
        "-i",
        str(video_path),
        "-map",
        "0:a:0",
        "-vn",
        "-af",
        "aresample=async=1:first_pts=0",
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


def _effective_recording_audio_input(
    configured_audio_input: str,
    audio_devices: list[str],
    *,
    recording_mode: str,
    meeting_link: str,
) -> str:
    effective = _effective_audio_input(configured_audio_input, audio_devices)
    if str(recording_mode or "").strip() == MEETING_RECORDING_MODE_AUDIO_ONLY and not str(meeting_link or "").strip():
        microphone = _preferred_microphone_input(audio_devices)
        if microphone:
            return microphone
    if _looks_like_system_audio_device(effective):
        return effective
    return effective


def _preferred_microphone_input(audio_devices: list[str]) -> str:
    for device in audio_devices:
        name = str(device or "").strip()
        lowered = name.lower()
        if name and ("microphone" in lowered or lowered.endswith(" mic") or " mic " in lowered):
            return name
    for device in audio_devices:
        name = str(device or "").strip()
        lowered = name.lower()
        if name and not _looks_like_system_audio_device(name) and "output" not in lowered:
            return name
    return ""


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
    if normalized in {"audio", "audio_only", "onsite", "screen", "screen_audio", "video", "video_audio"}:
        return MEETING_RECORDING_MODE_AUDIO_ONLY
    return MEETING_RECORDING_MODE_AUDIO_ONLY


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
