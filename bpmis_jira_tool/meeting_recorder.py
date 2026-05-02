from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import shutil
import signal
import subprocess
import threading
import time
from typing import Any

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


@dataclass(frozen=True)
class MeetingRecorderConfig:
    ffmpeg_bin: str = "ffmpeg"
    video_input: str = "Capture screen 0"
    audio_input: str = "default"
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
            "audio_devices": devices.get("audio_devices") or [],
            "video_devices": devices.get("video_devices") or [],
            **audio_status,
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
        log_path = record_dir / "ffmpeg.log"
        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-y",
            "-f",
            "avfoundation",
            "-framerate",
            "15",
            "-i",
            f"{self.config.video_input}:{audio_input}",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-movflags",
            "+faststart",
            str(video_path),
        ]
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
        record["media"] = {
            "video_path": str(video_path.relative_to(self.store.root_dir)),
            "video_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.mp4",
            "recorder_command": _redact_command(command),
        }
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
        self.store.save_record(record)
        return record


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
            video_path = self._video_path(record)
            audio_path = self._extract_audio(record, video_path)
            transcript = self._transcribe_audio(audio_path)
            transcript_text = str(transcript.get("text") or "").strip()
            snapshots = self._extract_visual_evidence(record, video_path)
            minutes = self._generate_minutes(record=record, transcript_text=transcript_text)
            record["transcript"] = {
                "status": "completed",
                "text": transcript_text,
                "chunks": transcript.get("chunks") or [{"start_seconds": 0, "text": transcript_text}],
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
        body = f"{minutes}\n\nFull transcript and video archive: {portal_url}\n"
        result = send_gmail_message(
            credentials=credentials,
            sender=owner_email,
            recipient=target,
            subject=subject,
            text_body=body,
        )
        record["email"] = {
            "status": "sent",
            "sent_at": _utc_now(),
            "message_id": str(result.get("id") or result.get("message_id") or ""),
            "recipient": target,
        }
        self.store.save_record(record)
        return record["email"]

    def _record_url(self, record_id: str) -> str:
        path = f"/meeting-recorder?record={record_id}"
        return f"{self.portal_base_url}{path}" if self.portal_base_url else path

    def _video_path(self, record: dict[str, Any]) -> Path:
        relative = str((record.get("media") or {}).get("video_path") or "").strip()
        if not relative:
            raise ToolError("Recorded meeting video is missing.")
        video_path = (self.store.root_dir / relative).resolve()
        if not video_path.exists():
            raise ToolError("Recorded meeting video file was not found.")
        return video_path

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
        output_base = self.store.record_dir(audio_path.parent.name) / "whisper-transcript"
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
        language = str(self.config.whisper_language or "auto").strip().lower()
        if language:
            command.extend(["-l", language])
        completed = _run_command(command, "whisper.cpp transcription failed.", timeout_seconds=7200)
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
        return {"text": transcript, "chunks": chunks}

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

    def _generate_minutes(self, *, record: dict[str, Any], transcript_text: str) -> str:
        system_prompt = (
            "You write concise, evidence-grounded English meeting minutes for product managers. "
            "Use only the provided spoken transcript and meeting metadata. "
            "Do not use screen recordings, screenshots, keyframes, or visual context. "
            "Do not invent owners, decisions, dates, or follow-ups."
        )
        user_prompt = (
            f"Meeting title: {record.get('title')}\n"
            f"Platform: {record.get('platform')}\n"
            f"Scheduled start: {record.get('scheduled_start')}\n"
            f"Attendees from calendar: {json.dumps(record.get('attendees') or [], ensure_ascii=False)}\n\n"
            "# Transcript\n"
            f"{transcript_text or 'No transcript text was produced.'}\n\n"
            "Return Markdown with exactly these sections: Summary, Decisions, Action Items, Risks/Blockers, Open Questions, Follow-ups."
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


def _avfoundation_devices(ffmpeg_path: str | None) -> dict[str, list[str]]:
    if not ffmpeg_path:
        return {"video_devices": [], "audio_devices": []}
    command = [ffmpeg_path, "-hide_banner", "-f", "avfoundation", "-list_devices", "true", "-i", ""]
    try:
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=10)  # noqa: S603
    except (OSError, subprocess.TimeoutExpired):
        return {"video_devices": [], "audio_devices": []}
    return _parse_avfoundation_devices(f"{completed.stderr}\n{completed.stdout}")


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
        label = "Aggregate device configured"
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
        "system_audio_available": system_audio_available,
        "aggregate_audio_available": aggregate_available,
        "recommended_audio_input": "Meeting Recorder Aggregate",
        "audio_capture_warning": warning,
    }


def _looks_like_system_audio_device(name: str) -> bool:
    lowered = str(name or "").strip().lower()
    return any(token in lowered for token in ("blackhole", "soundflower", "loopback", "aggregate", "multi-output"))


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
