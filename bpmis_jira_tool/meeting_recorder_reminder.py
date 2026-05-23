from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode

from google.oauth2.credentials import Credentials

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.gmail_sender import StoredGoogleCredentials
from bpmis_jira_tool.meeting_recorder import CALENDAR_READONLY_SCOPE, GoogleCalendarMeetingService


MEETING_RECORDER_REMINDER_STATE_RELATIVE_PATH = Path("meeting_recorder") / "reminders.json"
DEFAULT_REMINDER_WINDOW_SECONDS = 60
DEFAULT_REMINDER_LOOKAHEAD_MINUTES = 2
DEFAULT_REMINDER_POLL_INTERVAL_SECONDS = 30
DEFAULT_DIALOG_TIMEOUT_SECONDS = 120


@dataclass(frozen=True)
class MeetingReminderCandidate:
    key: str
    title: str
    scheduled_start: str
    meeting_link: str = ""
    calendar_event_id: str = ""


def _parse_bool(value: str | None, *, default: bool = True) -> bool:
    if value is None or not str(value).strip():
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _parse_positive_int(value: str | None, *, default: int, minimum: int = 1) -> int:
    try:
        parsed = int(str(value or "").strip())
    except ValueError:
        return default
    return max(minimum, parsed)


def _parse_event_start(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _reminder_key(meeting: dict[str, Any]) -> str:
    event_id = str(meeting.get("calendar_event_id") or meeting.get("id") or "").strip()
    scheduled_start = str(meeting.get("scheduled_start") or "").strip()
    if event_id:
        return f"{event_id}:{scheduled_start}"
    title = str(meeting.get("title") or "Untitled meeting").strip()
    return f"{title}:{scheduled_start}:{meeting.get('meeting_link') or ''}"


def due_meeting_reminders(
    meetings: list[dict[str, Any]],
    *,
    now: datetime,
    window_seconds: int = DEFAULT_REMINDER_WINDOW_SECONDS,
    reminded_keys: set[str] | None = None,
) -> list[MeetingReminderCandidate]:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    reminded = reminded_keys or set()
    due: list[MeetingReminderCandidate] = []
    for meeting in meetings:
        if not isinstance(meeting, dict):
            continue
        scheduled_start = str(meeting.get("scheduled_start") or "").strip()
        start_at = _parse_event_start(scheduled_start)
        if start_at is None:
            continue
        age_seconds = (now - start_at).total_seconds()
        if age_seconds < 0 or age_seconds > window_seconds:
            continue
        key = _reminder_key(meeting)
        if key in reminded:
            continue
        due.append(
            MeetingReminderCandidate(
                key=key,
                title=str(meeting.get("title") or "Untitled meeting").strip() or "Untitled meeting",
                scheduled_start=scheduled_start,
                meeting_link=str(meeting.get("meeting_link") or "").strip(),
                calendar_event_id=str(meeting.get("calendar_event_id") or "").strip(),
            )
        )
    due.sort(key=lambda item: item.scheduled_start)
    return due


class MeetingReminderStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load_keys(self) -> set[str]:
        if not self.path.exists():
            return set()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return set()
        reminders = payload.get("reminders") if isinstance(payload, dict) else {}
        if not isinstance(reminders, dict):
            return set()
        return {str(key) for key in reminders if str(key).strip()}

    def mark_sent(self, candidate: MeetingReminderCandidate, *, reminded_at: datetime) -> None:
        payload = self._load_payload()
        reminders = payload.setdefault("reminders", {})
        reminders[candidate.key] = {
            "title": candidate.title,
            "scheduled_start": candidate.scheduled_start,
            "meeting_link": candidate.meeting_link,
            "calendar_event_id": candidate.calendar_event_id,
            "reminded_at": reminded_at.astimezone(timezone.utc).isoformat(),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_name(f".{self.path.name}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        temp_path.replace(self.path)

    def _load_payload(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"reminders": {}}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"reminders": {}}
        if not isinstance(payload, dict):
            return {"reminders": {}}
        if not isinstance(payload.get("reminders"), dict):
            payload["reminders"] = {}
        return payload


def build_meeting_recorder_url(base_url: str, candidate: MeetingReminderCandidate) -> str:
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        base = "http://127.0.0.1:5000"
    params = {
        "calendar_event_id": candidate.calendar_event_id,
        "meeting_title": candidate.title,
        "scheduled_start": candidate.scheduled_start,
    }
    query = urlencode({key: value for key, value in params.items() if value})
    url = f"{base}/meeting-recorder"
    return f"{url}?{query}" if query else url


def build_meeting_reminder_dialog_script(candidate: MeetingReminderCandidate) -> str:
    title = candidate.title.replace("\r", " ").replace("\n", " ").strip() or "Untitled meeting"
    message = f"Meeting starting now:\\n{title}\\n\\nOpen Meeting Recorder and start recording if needed."
    return (
        f'display dialog {json.dumps(message)} '
        'buttons {"Dismiss", "Open Meeting Recorder"} '
        'default button "Open Meeting Recorder" '
        f'giving up after {DEFAULT_DIALOG_TIMEOUT_SECONDS}'
    )


def open_meeting_recorder_url(url: str, *, runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run) -> None:
    runner(["open", url], check=False)


def show_meeting_reminder_dialog(
    candidate: MeetingReminderCandidate,
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> None:
    script = build_meeting_reminder_dialog_script(candidate)
    runner(["osascript", "-e", script], check=False)


class MeetingRecorderReminderRunner:
    def __init__(
        self,
        *,
        settings: Settings,
        calendar_service: GoogleCalendarMeetingService,
        state_store: MeetingReminderStateStore,
        opener: Callable[[str], None] = open_meeting_recorder_url,
        dialog: Callable[[MeetingReminderCandidate], None] = show_meeting_reminder_dialog,
    ) -> None:
        self.settings = settings
        self.calendar_service = calendar_service
        self.state_store = state_store
        self.opener = opener
        self.dialog = dialog

    def run_once(
        self,
        *,
        now: datetime | None = None,
        window_seconds: int = DEFAULT_REMINDER_WINDOW_SECONDS,
        lookahead_minutes: int = DEFAULT_REMINDER_LOOKAHEAD_MINUTES,
    ) -> list[MeetingReminderCandidate]:
        current = now or datetime.now(timezone.utc)
        reminded_keys = self.state_store.load_keys()
        query_days = max(1, math.ceil(max(1, lookahead_minutes) / (24 * 60)))
        meetings = self.calendar_service.upcoming_meetings(
            now=current,
            days=query_days,
            max_results=20,
        )
        due = due_meeting_reminders(
            meetings,
            now=current,
            window_seconds=window_seconds,
            reminded_keys=reminded_keys,
        )
        base_url = self._portal_base_url()
        for candidate in due:
            self.opener(build_meeting_recorder_url(base_url, candidate))
            self.dialog(candidate)
            self.state_store.mark_sent(candidate, reminded_at=current)
        return due

    def _portal_base_url(self) -> str:
        if self.settings.team_portal_base_url:
            return self.settings.team_portal_base_url
        host = self.settings.team_portal_host or "127.0.0.1"
        port = self.settings.team_portal_port or 5000
        return f"http://{host}:{port}"


def _build_credentials(settings: Settings) -> Credentials:
    store = StoredGoogleCredentials(
        settings.team_portal_data_dir / "google" / "credentials.json",
        encryption_key=settings.team_portal_config_encryption_key,
    )
    payload = store.load(owner_email=settings.meeting_recorder_owner_email)
    scopes = {str(scope).strip() for scope in (payload.get("scopes") or []) if str(scope).strip()}
    if CALENDAR_READONLY_SCOPE not in scopes:
        raise ConfigError("Google Calendar readonly permission is missing. Reconnect Google once to grant calendar.readonly.")
    return Credentials(**payload)


def build_runner(settings: Settings) -> MeetingRecorderReminderRunner:
    credentials = _build_credentials(settings)
    calendar_service = GoogleCalendarMeetingService(credentials)
    state_store = MeetingReminderStateStore(settings.team_portal_data_dir / MEETING_RECORDER_REMINDER_STATE_RELATIVE_PATH)
    return MeetingRecorderReminderRunner(settings=settings, calendar_service=calendar_service, state_store=state_store)


def run_loop(
    *,
    runner: MeetingRecorderReminderRunner,
    poll_interval_seconds: int,
    window_seconds: int,
    lookahead_minutes: int,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    while True:
        try:
            runner.run_once(window_seconds=window_seconds, lookahead_minutes=lookahead_minutes)
        except (ConfigError, ToolError) as error:
            print(f"Meeting Recorder reminder skipped: {error}", flush=True)
        except Exception as error:  # pragma: no cover - defensive daemon boundary
            print(f"Meeting Recorder reminder failed unexpectedly: {error}", flush=True)
        sleep(poll_interval_seconds)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show Meeting Recorder start reminders without macOS Notification Center.")
    parser.add_argument("--once", action="store_true", help="Run one poll and exit.")
    parser.add_argument("--now", default="", help="Override current time as an ISO timestamp for tests/manual dry runs.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    enabled = _parse_bool(os.getenv("MEETING_RECORDER_REMINDER_ENABLED"), default=True)
    if not enabled:
        print("Meeting Recorder reminder disabled by MEETING_RECORDER_REMINDER_ENABLED.", flush=True)
        return 0
    settings = Settings.from_env()
    runner = build_runner(settings)
    window_seconds = _parse_positive_int(
        os.getenv("MEETING_RECORDER_REMINDER_WINDOW_SECONDS"),
        default=DEFAULT_REMINDER_WINDOW_SECONDS,
    )
    poll_interval_seconds = _parse_positive_int(
        os.getenv("MEETING_RECORDER_REMINDER_POLL_INTERVAL_SECONDS"),
        default=DEFAULT_REMINDER_POLL_INTERVAL_SECONDS,
    )
    lookahead_minutes = _parse_positive_int(
        os.getenv("MEETING_RECORDER_REMINDER_LOOKAHEAD_MINUTES"),
        default=DEFAULT_REMINDER_LOOKAHEAD_MINUTES,
    )
    if args.once:
        now = _parse_event_start(args.now) if args.now else None
        runner.run_once(now=now, window_seconds=window_seconds, lookahead_minutes=lookahead_minutes)
        return 0
    run_loop(
        runner=runner,
        poll_interval_seconds=poll_interval_seconds,
        window_seconds=window_seconds,
        lookahead_minutes=lookahead_minutes,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
