import os
import json
import hashlib
import runpy
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.meeting_recorder import (
    CALENDAR_READONLY_SCOPE,
    GoogleCalendarMeetingService,
    MeetingProcessingService,
    MeetingRecorderConfig,
    MeetingRecorderRuntime,
    MeetingRecordStore,
    _audio_duration_seconds,
    _audio_volume_metrics,
    _assert_record_owner,
    _background_command,
    _bounded_int,
    _build_ffmpeg_screencapturekit_mix_command,
    _build_ffmpeg_single_audio_convert_command,
    _effective_transcript_segment_workers,
    _effective_whisper_threads,
    _find_recorder_processes_for_paths,
    _meeting_minutes_sender_name,
    _meeting_transcript_hash,
    _meeting_minutes_markdown_to_html,
    _meeting_minutes_prompt_metadata,
    _normalized_auto_stop_grace_seconds,
    _normalized_background_nice,
    _normalized_startup_silence_grace_seconds,
    _normalized_status_every_buffers,
    _parse_meeting_datetime,
    _parse_srt_transcript,
    _parse_utc_timestamp,
    _parse_whisper_language,
    _pid_command_contains,
    _process_exists,
    _read_json_file,
    _read_tail,
    _recorder_process_candidates,
    _recording_elapsed_seconds,
    _resolve_executable,
    _resolve_screencapturekit_helper,
    _run_command,
    _safe_file_size,
    _safe_float,
    _safe_int,
    _safe_record_id,
    _scheduled_auto_stop_deadline,
    _ScreenCaptureKitAudioRecorder,
    _should_retry_transcript_languages,
    _split_meeting_transcript_chunks,
    _srt_timestamp_seconds,
    _terminate_process_id,
    _terminate_recorder_process,
    _transcript_quality_score,
    _transcript_repetition_metrics,
    build_calendar_api_service,
    extract_meeting_links,
    meeting_transcript_whisper_language,
    meeting_platform_from_link,
    normalize_calendar_event,
)
from bpmis_jira_tool.meeting_recorder_reminder import (
    MeetingRecorderReminderRunner,
    MeetingReminderCandidate,
    MeetingReminderStateStore,
    build_runner,
    build_meeting_recorder_url,
    build_meeting_reminder_dialog_script,
    due_meeting_reminders,
    main as meeting_reminder_main,
    open_meeting_recorder_url,
    parse_args as parse_meeting_reminder_args,
    run_loop as run_meeting_reminder_loop,
    show_meeting_reminder_dialog,
)
from bpmis_jira_tool import meeting_recorder_reminder


class MeetingRecorderParsingTests(unittest.TestCase):
    def test_frontend_formats_recording_timestamps_in_singapore_time(self):
        script = Path("static/meeting_recorder.js").read_text(encoding="utf-8")

        self.assertIn("timeZone: 'Asia/Singapore'", script)
        self.assertIn("${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} SGT", script)
        self.assertNotIn("new Intl.DateTimeFormat(undefined, {\n      month: 'short'", script)

    def test_extract_meeting_links_dedupes_meet_and_zoom_links(self):
        links = extract_meeting_links(
            "Join https://meet.google.com/abc-defg-hij",
            "Zoom: https://npt-sg.zoom.us/j/123456789?pwd=abc. Meet again https://meet.google.com/abc-defg-hij",
        )

        self.assertEqual(
            links,
            [
                "https://meet.google.com/abc-defg-hij",
                "https://npt-sg.zoom.us/j/123456789?pwd=abc",
            ],
        )

    def test_meeting_platform_from_link(self):
        self.assertEqual(meeting_platform_from_link("https://meet.google.com/abc-defg-hij"), "google_meet")
        self.assertEqual(meeting_platform_from_link("https://npt-sg.zoom.us/j/123"), "zoom")
        self.assertEqual(meeting_platform_from_link("https://example.com"), "unknown")

    def test_normalize_calendar_event_uses_conference_data_and_attendees(self):
        event = {
            "id": "event-1",
            "summary": "Risk review",
            "start": {"dateTime": "2026-05-04T09:00:00+08:00"},
            "end": {"dateTime": "2026-05-04T09:30:00+08:00"},
            "conferenceData": {"entryPoints": [{"uri": "https://meet.google.com/abc-defg-hij"}]},
            "attendees": [{"email": "alice@npt.sg", "displayName": "Alice"}, "bad-attendee"],
        }

        payload = normalize_calendar_event(event)

        self.assertEqual(payload["calendar_event_id"], "event-1")
        self.assertEqual(payload["title"], "Risk review")
        self.assertEqual(payload["platform"], "google_meet")
        self.assertEqual(payload["meeting_link"], "https://meet.google.com/abc-defg-hij")
        self.assertEqual(payload["attendees"], [{"email": "alice@npt.sg", "name": "Alice"}])

    def test_normalize_calendar_event_keeps_calendar_invites_without_meeting_link(self):
        event = {
            "id": "event-2",
            "summary": "In-person review",
            "start": {"dateTime": "2026-05-04T11:00:00+08:00"},
            "end": {"dateTime": "2026-05-04T11:30:00+08:00"},
            "attendees": [{"email": "bob@npt.sg", "displayName": "Bob"}],
        }

        payload = normalize_calendar_event(event)

        self.assertIsNotNone(payload)
        self.assertEqual(payload["calendar_event_id"], "event-2")
        self.assertEqual(payload["title"], "In-person review")
        self.assertEqual(payload["platform"], "unknown")
        self.assertEqual(payload["meeting_link"], "")
        self.assertEqual(payload["meeting_links"], [])
        self.assertEqual(payload["attendees"], [{"email": "bob@npt.sg", "name": "Bob"}])

    def test_normalize_calendar_event_filters_working_location_events(self):
        event = {
            "id": "office-1",
            "summary": "Office",
            "eventType": "workingLocation",
            "start": {"date": "2026-05-04"},
            "end": {"date": "2026-05-05"},
        }

        self.assertIsNone(normalize_calendar_event(event))

    def test_google_calendar_meeting_service_normalizes_events_and_bounds_query(self):
        class FakeEvents:
            def __init__(self):
                self.kwargs = None

            def list(self, **kwargs):
                self.kwargs = kwargs
                return self

            def execute(self):
                return {
                    "items": [
                        {
                            "id": "event-1",
                            "summary": "Meet",
                            "start": {"dateTime": "2026-05-04T09:00:00+08:00"},
                            "end": {"dateTime": "2026-05-04T09:30:00+08:00"},
                            "hangoutLink": "https://meet.google.com/abc-defg-hij",
                        },
                        {"id": "office", "eventType": "workingLocation"},
                        "not-a-dict",
                    ]
                }

        class FakeCalendar:
            def __init__(self):
                self.events_resource = FakeEvents()

            def events(self):
                return self.events_resource

        calendar = FakeCalendar()
        service = GoogleCalendarMeetingService(credentials=object(), calendar_service=calendar)
        meetings = service.upcoming_meetings(
            now=datetime(2026, 5, 4, 9, 0, 0),
            days=30,
            max_results=99,
        )

        self.assertEqual(len(meetings), 1)
        self.assertEqual(meetings[0]["platform"], "google_meet")
        self.assertEqual(calendar.events_resource.kwargs["maxResults"], 50)
        self.assertIn("+00:00", calendar.events_resource.kwargs["timeMin"])

    def test_meeting_reminder_due_at_start_time_only_within_window(self):
        now = datetime(2026, 5, 21, 10, 0, 0, tzinfo=timezone.utc)
        meetings = [
            {
                "calendar_event_id": "due",
                "title": "Due now",
                "scheduled_start": "2026-05-21T10:00:00+00:00",
                "meeting_link": "https://meet.google.com/abc-defg-hij",
            },
            {
                "calendar_event_id": "future",
                "title": "Future",
                "scheduled_start": "2026-05-21T10:01:00+00:00",
            },
            {
                "calendar_event_id": "old",
                "title": "Old",
                "scheduled_start": "2026-05-21T09:58:00+00:00",
            },
        ]

        due = due_meeting_reminders(meetings, now=now, window_seconds=60)

        self.assertEqual([item.calendar_event_id for item in due], ["due"])
        self.assertEqual(due[0].title, "Due now")

    def test_meeting_reminder_ignores_missing_start_and_dedupes(self):
        now = datetime(2026, 5, 21, 10, 0, 30, tzinfo=timezone.utc)
        meetings = [
            {"calendar_event_id": "missing", "title": "Missing"},
            {
                "calendar_event_id": "sent",
                "title": "Already sent",
                "scheduled_start": "2026-05-21T10:00:00+00:00",
            },
            {
                "calendar_event_id": "new",
                "title": "New",
                "scheduled_start": "2026-05-21T10:00:00+00:00",
            },
        ]

        due = due_meeting_reminders(
            meetings,
            now=now,
            window_seconds=60,
            reminded_keys={"sent:2026-05-21T10:00:00+00:00"},
        )

        self.assertEqual([item.calendar_event_id for item in due], ["new"])

    def test_meeting_reminder_state_store_persists_sent_keys(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_store = MeetingReminderStateStore(Path(temp_dir) / "meeting_recorder" / "reminders.json")
            candidate = MeetingReminderCandidate(
                key="event-1:2026-05-21T10:00:00+00:00",
                title="Risk review",
                scheduled_start="2026-05-21T10:00:00+00:00",
                meeting_link="https://meet.google.com/abc-defg-hij",
                calendar_event_id="event-1",
            )

            state_store.mark_sent(candidate, reminded_at=datetime(2026, 5, 21, 10, 0, 1, tzinfo=timezone.utc))

            self.assertEqual(state_store.load_keys(), {"event-1:2026-05-21T10:00:00+00:00"})
            payload = json.loads(state_store.path.read_text(encoding="utf-8"))
            self.assertEqual(payload["reminders"][candidate.key]["title"], "Risk review")

    def test_meeting_reminder_commands_are_escaped_and_dry_runnable(self):
        candidate = MeetingReminderCandidate(
            key="event-1:start",
            title='Risk "review"\nwith team',
            scheduled_start="2026-05-21T10:00:00+08:00",
            calendar_event_id="event-1",
        )

        url = build_meeting_recorder_url("https://app.bankpmtool.uk/", candidate)
        script = build_meeting_reminder_dialog_script(candidate)

        self.assertTrue(url.startswith("https://app.bankpmtool.uk/meeting-recorder?"))
        self.assertIn("calendar_event_id=event-1", url)
        self.assertIn("meeting_title=Risk+%22review%22%0Awith+team", url)
        self.assertIn('buttons {"Dismiss", "Open Meeting Recorder"}', script)
        self.assertIn('\\"review\\"', script)
        self.assertNotIn("with team\n", script)

    def test_meeting_reminder_runner_opens_portal_marks_state_and_dedupes(self):
        class FakeCalendarService:
            def __init__(self):
                self.calls = 0

            def upcoming_meetings(self, *, now, days, max_results):
                self.calls += 1
                return [
                    {
                        "calendar_event_id": "event-1",
                        "title": "Risk review",
                        "scheduled_start": "2026-05-21T10:00:00+00:00",
                    }
                ]

        with tempfile.TemporaryDirectory() as temp_dir:
            settings = SimpleNamespace(team_portal_base_url="", team_portal_host="127.0.0.1", team_portal_port=5000)
            state_store = MeetingReminderStateStore(Path(temp_dir) / "meeting_recorder" / "reminders.json")
            opened_urls = []
            dialogs = []
            runner = MeetingRecorderReminderRunner(
                settings=settings,
                calendar_service=FakeCalendarService(),
                state_store=state_store,
                opener=opened_urls.append,
                dialog=dialogs.append,
            )
            now = datetime(2026, 5, 21, 10, 0, 0, tzinfo=timezone.utc)

            first = runner.run_once(now=now, window_seconds=60)
            second = runner.run_once(now=now, window_seconds=60)

            self.assertEqual([item.calendar_event_id for item in first], ["event-1"])
            self.assertEqual(second, [])
            self.assertEqual(len(opened_urls), 1)
            self.assertIn("http://127.0.0.1:5000/meeting-recorder", opened_urls[0])
            self.assertEqual(len(dialogs), 1)

    def test_meeting_reminder_helpers_cover_invalid_inputs_and_default_url(self):
        now = datetime(2026, 5, 21, 10, 0, 30)
        meetings = [
            "skip",
            {"title": "No start"},
            {"title": "Bad start", "scheduled_start": "not-a-date"},
            {"id": "event-id", "title": "", "scheduled_start": "2026-05-21T10:00:00Z"},
            {"title": "", "scheduled_start": "2026-05-21T10:00:00", "meeting_link": "https://meet.google.com/a"},
        ]

        due = due_meeting_reminders(meetings, now=now, window_seconds=60)

        self.assertEqual([item.key for item in due], ["Untitled meeting:2026-05-21T10:00:00:https://meet.google.com/a", "event-id:2026-05-21T10:00:00Z"])
        self.assertIn("http://127.0.0.1:5000/meeting-recorder", build_meeting_recorder_url("", due[0]))
        self.assertFalse(meeting_recorder_reminder._parse_bool("off"))
        self.assertTrue(meeting_recorder_reminder._parse_bool("", default=True))
        self.assertEqual(meeting_recorder_reminder._parse_positive_int("bad", default=7), 7)
        self.assertEqual(meeting_recorder_reminder._parse_positive_int("-1", default=7, minimum=3), 3)

        calls = []
        open_meeting_recorder_url("https://portal/meeting-recorder", runner=lambda *args, **kwargs: calls.append((args, kwargs)))
        show_meeting_reminder_dialog(due[0], runner=lambda *args, **kwargs: calls.append((args, kwargs)))
        self.assertEqual(calls[0][0][0], ["open", "https://portal/meeting-recorder"])
        self.assertEqual(calls[1][0][0][0], "osascript")

    def test_meeting_reminder_state_store_handles_missing_corrupt_and_legacy_payloads(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "meeting_recorder" / "reminders.json"
            store = MeetingReminderStateStore(path)
            self.assertEqual(store.load_keys(), set())

            path.parent.mkdir(parents=True)
            path.write_text("{bad json", encoding="utf-8")
            self.assertEqual(store.load_keys(), set())
            self.assertEqual(store._load_payload(), {"reminders": {}})

            path.write_text(json.dumps(["bad"]), encoding="utf-8")
            self.assertEqual(store.load_keys(), set())
            self.assertEqual(store._load_payload(), {"reminders": {}})

            path.write_text(json.dumps({"reminders": ["bad"]}), encoding="utf-8")
            self.assertEqual(store.load_keys(), set())
            self.assertEqual(store._load_payload(), {"reminders": {}})

            path.write_text(json.dumps({"reminders": {"": {}, "event:start": {}}}), encoding="utf-8")
            self.assertEqual(store.load_keys(), {"event:start"})

            with patch.object(Path, "read_text", side_effect=OSError("denied")):
                self.assertEqual(store.load_keys(), set())
                self.assertEqual(store._load_payload(), {"reminders": {}})

    def test_meeting_reminder_runner_credentials_loop_and_cli_boundaries(self):
        class FakeCalendarService:
            def upcoming_meetings(self, *, now, days, max_results):
                return [
                    {
                        "calendar_event_id": "event-1",
                        "title": "Risk review",
                        "scheduled_start": "2026-05-21T10:00:00+00:00",
                    }
                ]

        settings = SimpleNamespace(team_portal_base_url="https://portal.example", team_portal_host="", team_portal_port=0)
        state_store = Mock(load_keys=Mock(return_value=set()), mark_sent=Mock())
        opened = []
        runner = MeetingRecorderReminderRunner(
            settings=settings,
            calendar_service=FakeCalendarService(),
            state_store=state_store,
            opener=opened.append,
            dialog=lambda _candidate: None,
        )
        due = runner.run_once(now=datetime(2026, 5, 21, 10, 0, 0, tzinfo=timezone.utc), lookahead_minutes=3000)
        self.assertEqual([item.calendar_event_id for item in due], ["event-1"])
        self.assertTrue(opened[0].startswith("https://portal.example/meeting-recorder"))

        sleep_calls = []

        class FailingRunner:
            def __init__(self, errors):
                self.errors = list(errors)

            def run_once(self, **kwargs):
                raise self.errors.pop(0)

        for error in [ConfigError("missing config"), ToolError("tool failed"), RuntimeError("boom")]:
            with self.subTest(error=type(error).__name__), patch("builtins.print") as print_mock:
                with self.assertRaises(KeyboardInterrupt):
                    run_meeting_reminder_loop(
                        runner=FailingRunner([error]),
                        poll_interval_seconds=5,
                        window_seconds=60,
                        lookahead_minutes=2,
                        sleep=lambda seconds: sleep_calls.append(seconds) or (_ for _ in ()).throw(KeyboardInterrupt()),
                    )
                self.assertTrue(print_mock.called)

        self.assertTrue(parse_meeting_reminder_args(["--once", "--now", "2026-05-21T10:00:00Z"]).once)

        with patch.object(meeting_recorder_reminder.StoredGoogleCredentials, "load", return_value={"scopes": []}):
            with self.assertRaisesRegex(ConfigError, "calendar.readonly"):
                meeting_recorder_reminder._build_credentials(
                    SimpleNamespace(
                        team_portal_data_dir=Path("/tmp"),
                        team_portal_config_encryption_key="",
                        meeting_recorder_owner_email="owner@npt.sg",
                    )
                )

        credential_payload = {
            "token": "token",
            "refresh_token": "refresh",
            "token_uri": "https://oauth2.googleapis.com/token",
            "client_id": "client",
            "client_secret": "secret",
            "scopes": [CALENDAR_READONLY_SCOPE],
        }
        with patch.object(meeting_recorder_reminder.StoredGoogleCredentials, "load", return_value=credential_payload):
            credentials = meeting_recorder_reminder._build_credentials(
                SimpleNamespace(
                    team_portal_data_dir=Path("/tmp"),
                    team_portal_config_encryption_key="",
                    meeting_recorder_owner_email="owner@npt.sg",
                )
            )
            self.assertEqual(credentials.token, "token")

        with patch("bpmis_jira_tool.meeting_recorder_reminder._build_credentials", return_value=object()), patch(
            "bpmis_jira_tool.meeting_recorder_reminder.GoogleCalendarMeetingService", return_value="calendar"
        ):
            built = build_runner(SimpleNamespace(team_portal_data_dir=Path("/tmp/team-portal")))
            self.assertEqual(built.calendar_service, "calendar")

        with patch.dict(os.environ, {"MEETING_RECORDER_REMINDER_ENABLED": "0"}, clear=False):
            self.assertEqual(meeting_reminder_main([]), 0)

        fake_runner = Mock()
        with patch("bpmis_jira_tool.meeting_recorder_reminder.Settings.from_env", return_value=SimpleNamespace()), patch(
            "bpmis_jira_tool.meeting_recorder_reminder.build_runner", return_value=fake_runner
        ), patch.dict(
            os.environ,
            {
                "MEETING_RECORDER_REMINDER_ENABLED": "1",
                "MEETING_RECORDER_REMINDER_WINDOW_SECONDS": "bad",
                "MEETING_RECORDER_REMINDER_POLL_INTERVAL_SECONDS": "0",
                "MEETING_RECORDER_REMINDER_LOOKAHEAD_MINUTES": "3",
            },
            clear=False,
        ):
            self.assertEqual(meeting_reminder_main(["--once", "--now", "2026-05-21T10:00:00Z"]), 0)
            self.assertTrue(fake_runner.run_once.called)

        with patch("bpmis_jira_tool.meeting_recorder_reminder.Settings.from_env", return_value=SimpleNamespace()), patch(
            "bpmis_jira_tool.meeting_recorder_reminder.build_runner", return_value=fake_runner
        ), patch("bpmis_jira_tool.meeting_recorder_reminder.run_loop") as loop_mock, patch.dict(
            os.environ, {"MEETING_RECORDER_REMINDER_ENABLED": "1"}, clear=False
        ):
            self.assertEqual(meeting_reminder_main([]), 0)
            self.assertTrue(loop_mock.called)

        with patch.dict(os.environ, {"MEETING_RECORDER_REMINDER_ENABLED": "0"}, clear=False), patch(
            "sys.argv", ["meeting_recorder_reminder"]
        ):
            with self.assertRaises(SystemExit) as raised:
                runpy.run_module("bpmis_jira_tool.meeting_recorder_reminder", run_name="__main__")
            self.assertEqual(raised.exception.code, 0)

    def test_transcript_whisper_language_and_calendar_builder_defaults(self):
        self.assertEqual(meeting_transcript_whisper_language("english"), "en")
        self.assertEqual(meeting_transcript_whisper_language("mixed", fallback="zh"), "zh")

        credentials = object()
        with patch("bpmis_jira_tool.meeting_recorder.httplib2.Http") as http_cls:
            with patch("bpmis_jira_tool.meeting_recorder.google_auth_httplib2.AuthorizedHttp") as auth_http_cls:
                with patch("bpmis_jira_tool.meeting_recorder.build") as build_mock:
                    service = build_calendar_api_service(credentials, cache_discovery=True)

        http_cls.assert_called_once_with(timeout=20)
        auth_http_cls.assert_called_once_with(credentials, http=http_cls.return_value)
        build_mock.assert_called_once_with("calendar", "v3", http=auth_http_cls.return_value, cache_discovery=True)
        self.assertEqual(service, build_mock.return_value)

    def test_meeting_record_store_rejects_missing_invalid_and_deleted_records(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MeetingRecordStore(Path(temp_dir))
            with self.assertRaisesRegex(ToolError, "not found"):
                store.get_record("missing")
            with self.assertRaisesRegex(ToolError, "id is missing"):
                store.save_record({"status": "recorded"})
            with self.assertRaisesRegex(ToolError, "Invalid meeting record status"):
                store.save_record({"record_id": "bad", "status": "unknown"})

            unreadable = store.metadata_path("bad-json")
            unreadable.parent.mkdir(parents=True, exist_ok=True)
            unreadable.write_text("{bad", encoding="utf-8")
            with self.assertRaisesRegex(ToolError, "unreadable"):
                store.get_record("bad-json")
            invalid_payload = store.metadata_path("invalid-payload")
            invalid_payload.parent.mkdir(parents=True, exist_ok=True)
            invalid_payload.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(ToolError, "invalid"):
                store.get_record("invalid-payload")

            valid = store.create_record(owner_email="owner@npt.sg", title="Keep", platform="zoom", meeting_link="")
            ignored = store.metadata_path("ignored")
            ignored.parent.mkdir(parents=True, exist_ok=True)
            ignored.write_text("[]", encoding="utf-8")
            deleted = store.create_record(owner_email="owner@npt.sg", title="Delete", platform="zoom", meeting_link="")
            deleted["status"] = "deleted"
            store.save_record(deleted)
            self.assertEqual([item["record_id"] for item in store.list_records(owner_email="owner@npt.sg")], [valid["record_id"]])

            artifact_record = store.create_record(owner_email="owner@npt.sg", title="Artifacts", platform="zoom", meeting_link="")
            artifact_dir = store.record_dir(artifact_record["record_id"])
            (artifact_dir / "audio.wav").write_bytes(b"audio")
            nested_dir = artifact_dir / "segments"
            nested_dir.mkdir()
            (nested_dir / "chunk.wav").write_bytes(b"chunk")
            removed = store.delete_record(record_id=artifact_record["record_id"], owner_email="owner@npt.sg")

            self.assertEqual(removed["status"], "deleted")
            self.assertTrue(store.metadata_path(artifact_record["record_id"]).exists())
            self.assertFalse((artifact_dir / "audio.wav").exists())
            self.assertFalse(nested_dir.exists())

class MeetingRecordStoreTests(unittest.TestCase):
    def test_create_list_get_and_delete_record(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MeetingRecordStore(Path(temp_dir))
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
                calendar_event_id="event-1",
                scheduled_start="2026-05-04T09:00:00+08:00",
                scheduled_end="2026-05-04T09:30:00+08:00",
                attendees=[{"email": "alice@npt.sg"}],
            )

            self.assertEqual(record["status"], "scheduled")
            loaded = store.get_record(record["record_id"])
            self.assertEqual(loaded["title"], "Review")
            self.assertEqual(len(store.list_records(owner_email="owner@npt.sg")), 1)
            self.assertEqual(store.list_records(owner_email="other@npt.sg"), [])

            store.delete_record(record_id=record["record_id"], owner_email="owner@npt.sg")

            self.assertEqual(store.list_records(owner_email="owner@npt.sg"), [])


class MeetingRecorderRuntimeTests(unittest.TestCase):
    class _FakeTimer:
        instances = []

        def __init__(self, delay, callback, args=None, kwargs=None):
            self.delay = delay
            self.callback = callback
            self.args = args or ()
            self.kwargs = kwargs or {}
            self.daemon = False
            self.started = False
            self.cancelled = False
            self.__class__.instances.append(self)

        def start(self):
            self.started = True

        def cancel(self):
            self.cancelled = True

        def fire(self):
            self.callback(*self.args, **self.kwargs)

    def test_audio_only_blank_link_uses_screencapturekit_f2f(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_screencapturekit_helper",
                return_value=Path("/tmp/meeting-screencapture-helper"),
            ), patch(
                "bpmis_jira_tool.meeting_recorder._ScreenCaptureKitAudioRecorder.start",
                return_value={"status": "ok", "latency_seconds": 0.2, "bytes": 48000, "pid": 12345},
            ):
                record = runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Face to face",
                    platform="unknown",
                    meeting_link="",
                    recording_mode="audio_only",
                )

        self.assertEqual(record["media"]["recording_mode"], "audio_only")
        self.assertEqual(record["media"]["audio_capture_profile"], "screencapturekit_audio_v1")
        self.assertEqual(record["media"]["screencapture_capture_source"], "screencapturekit_f2f")
        self.assertIn("system_audio_path", record["media"])
        self.assertIn("microphone_audio_path", record["media"])
        self.assertEqual(record["diagnostics_snapshot"]["audio_capture_mode"], "screencapturekit_f2f")
        command = runtime._processes[record["record_id"]].command
        self.assertEqual(command[:3], ["nice", "-n", "10"])
        self.assertIn("--system-output", command)
        self.assertIn("--microphone-output", command)
        self.assertIn("--status-every-buffers", command)
        self.assertNotIn("--sample-rate", command)
        self.assertNotIn("--channel-count", command)
        self.assertEqual(command[command.index("--status-every-buffers") + 1], "250")
        self.assertEqual(record["media"]["recording_background_nice"], 10)
        self.assertEqual(record["media"]["capture_status_every_buffers"], 250)

    def test_calendar_meeting_schedules_auto_stop_twenty_minutes_after_planned_end(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )
            self._FakeTimer.instances = []
            with patch("bpmis_jira_tool.meeting_recorder.threading.Timer", self._FakeTimer), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin",
                return_value="/opt/homebrew/bin/ffmpeg",
            ), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_screencapturekit_helper",
                return_value=Path("/tmp/meeting-screencapture-helper"),
            ), patch(
                "bpmis_jira_tool.meeting_recorder._ScreenCaptureKitAudioRecorder.start",
                return_value={"status": "ok", "latency_seconds": 0.2, "bytes": 48000, "pid": 12345},
            ):
                record = runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Calendar review",
                    platform="google_meet",
                    meeting_link="https://meet.google.com/abc-defg-hij",
                    recording_mode="audio_only",
                    calendar_event_id="calendar-1",
                    scheduled_start="2099-05-04T09:00:00+08:00",
                    scheduled_end="2099-05-04T09:30:00+08:00",
                )

        auto_stop = record["scheduled_auto_stop"]
        self.assertEqual(auto_stop["status"], "scheduled")
        self.assertEqual(auto_stop["mode"], "scheduled_end_plus_grace")
        self.assertEqual(auto_stop["grace_seconds"], 1200)
        self.assertEqual(auto_stop["scheduled_for"], "2099-05-04T01:50:00+00:00")
        self.assertEqual(len(self._FakeTimer.instances), 2)
        self.assertTrue(self._FakeTimer.instances[0].started)
        self.assertTrue(self._FakeTimer.instances[0].daemon)
        self.assertEqual(self._FakeTimer.instances[1].delay, 15.0)

    def test_scheduled_auto_stop_callback_stops_recording(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Calendar review",
                platform="google_meet",
                meeting_link="https://meet.google.com/abc-defg-hij",
                calendar_event_id="calendar-1",
                scheduled_start="2026-05-04T09:00:00+08:00",
                scheduled_end="2026-05-04T09:30:00+08:00",
            )
            record["status"] = "recording"
            record["recording_started_at"] = "2026-05-04T01:00:00+00:00"
            record["scheduled_auto_stop"] = {
                "status": "scheduled",
                "mode": "scheduled_end_plus_grace",
                "grace_seconds": 1200,
                "scheduled_for": "2026-05-04T01:50:00+00:00",
            }
            store.save_record(record)

            with patch("bpmis_jira_tool.meeting_recorder._utc_now", return_value="2026-05-04T01:50:00+00:00"), patch.object(
                runtime,
                "_terminate_persisted_recorder_process",
            ) as terminate:
                runtime._scheduled_auto_stop_callback(
                    record_id=record["record_id"],
                    owner_email="owner@npt.sg",
                    scheduled_for="2026-05-04T01:50:00+00:00",
                )

            updated = store.get_record(record["record_id"])

        self.assertEqual(updated["status"], "recorded")
        self.assertEqual(updated["recording_stop_reason"], "scheduled_auto_stop")
        self.assertEqual(updated["recording_stopped_at"], "2026-05-04T01:50:00+00:00")
        self.assertEqual(updated["scheduled_auto_stop"]["status"], "completed")
        self.assertEqual(updated["scheduled_auto_stop"]["stopped_at"], "2026-05-04T01:50:00+00:00")
        terminate.assert_called_once()

    def test_scheduled_auto_stop_callback_queues_post_stop_work(self):
        queued_records = []

        def queue_after_stop(record):
            queued_records.append(dict(record))
            return {"state": "queued", "job_id": "job-1"}

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(),
                scheduled_auto_stop_callback=queue_after_stop,
            )
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Calendar review",
                platform="google_meet",
                meeting_link="https://meet.google.com/abc-defg-hij",
                calendar_event_id="calendar-1",
                scheduled_start="2026-05-04T09:00:00+08:00",
                scheduled_end="2026-05-04T09:30:00+08:00",
            )
            record["status"] = "recording"
            record["recording_started_at"] = "2026-05-04T01:00:00+00:00"
            record["scheduled_auto_stop"] = {
                "status": "scheduled",
                "mode": "scheduled_end_plus_grace",
                "grace_seconds": 1200,
                "scheduled_for": "2026-05-04T01:50:00+00:00",
            }
            store.save_record(record)

            with patch("bpmis_jira_tool.meeting_recorder._utc_now", return_value="2026-05-04T01:50:00+00:00"), patch.object(
                runtime,
                "_terminate_persisted_recorder_process",
            ):
                runtime._scheduled_auto_stop_callback(
                    record_id=record["record_id"],
                    owner_email="owner@npt.sg",
                    scheduled_for="2026-05-04T01:50:00+00:00",
                )

            updated = store.get_record(record["record_id"])

        self.assertEqual(len(queued_records), 1)
        self.assertEqual(queued_records[0]["status"], "recorded")
        self.assertEqual(queued_records[0]["recording_stop_reason"], "scheduled_auto_stop")
        self.assertEqual(updated["scheduled_auto_stop"]["status"], "completed")
        self.assertEqual(updated["scheduled_auto_stop"]["process_queue_status"], "queued")
        self.assertEqual(updated["scheduled_auto_stop"]["process_job_id"], "job-1")

    def test_manual_stop_cancels_scheduled_auto_stop(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Calendar review",
                platform="google_meet",
                meeting_link="https://meet.google.com/abc-defg-hij",
                calendar_event_id="calendar-1",
                scheduled_end="2026-05-04T09:30:00+08:00",
            )
            record["status"] = "recording"
            record["recording_started_at"] = "2026-05-04T01:00:00+00:00"
            record["scheduled_auto_stop"] = {"status": "scheduled", "scheduled_for": "2026-05-04T01:50:00+00:00"}
            store.save_record(record)
            timer = self._FakeTimer(1200, lambda: None)
            runtime._auto_stop_timers[record["record_id"]] = timer

            with patch("bpmis_jira_tool.meeting_recorder._utc_now", return_value="2026-05-04T01:20:00+00:00"), patch.object(
                runtime,
                "_terminate_persisted_recorder_process",
            ):
                updated = runtime.stop_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertTrue(timer.cancelled)
        self.assertEqual(updated["recording_stop_reason"], "manual")
        self.assertEqual(updated["scheduled_auto_stop"]["status"], "cancelled")

    def test_scheduled_auto_stop_ignores_invalid_records_and_persists_failures(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())

            recorded = store.create_record(owner_email="owner@npt.sg", title="Done", platform="zoom", meeting_link="")
            recorded["status"] = "recorded"
            store.save_record(recorded)
            ignored = runtime._schedule_auto_stop_if_needed(recorded)

            missing_identity = dict(recorded)
            missing_identity["record_id"] = ""
            missing_identity["status"] = "recording"
            no_identity = runtime._schedule_auto_stop_if_needed(missing_identity)

            failing = store.create_record(owner_email="owner@npt.sg", title="Failing", platform="zoom", meeting_link="")
            failing["status"] = "recording"
            failing["scheduled_auto_stop"] = {"status": "scheduled"}
            store.save_record(failing)

            with patch.object(runtime, "stop_recording", side_effect=ToolError("stop failed")):
                runtime._scheduled_auto_stop_callback(
                    record_id=failing["record_id"],
                    owner_email="owner@npt.sg",
                    scheduled_for="2026-05-04T01:50:00+00:00",
                )
            failed = store.get_record(failing["record_id"])

            callback_record = store.create_record(owner_email="owner@npt.sg", title="Callback", platform="zoom", meeting_link="")
            callback_record["scheduled_auto_stop"] = {"status": "completed"}
            store.save_record(callback_record)
            runtime_with_empty_callback = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(),
                scheduled_auto_stop_callback=lambda _record: {},
            )
            runtime_with_empty_callback._run_scheduled_auto_stop_callback_after_stop(callback_record)
            skipped = store.get_record(callback_record["record_id"])

            queue_error = store.create_record(owner_email="owner@npt.sg", title="Queue Error", platform="zoom", meeting_link="")
            queue_error["scheduled_auto_stop"] = {"status": "completed"}
            store.save_record(queue_error)
            runtime_with_bad_callback = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(),
                scheduled_auto_stop_callback=lambda _record: (_ for _ in ()).throw(ToolError("queue failed")),
            )
            runtime_with_bad_callback._run_scheduled_auto_stop_callback_after_stop(queue_error)
            failed_queue = store.get_record(queue_error["record_id"])

            auto_process_error = store.create_record(owner_email="owner@npt.sg", title="Auto Error", platform="zoom", meeting_link="")
            auto_process_error["scheduled_auto_stop"] = {"status": "completed"}
            store.save_record(auto_process_error)
            runtime_with_error_payload = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(),
                scheduled_auto_stop_callback=lambda _record: {"auto_process_error": "local agent unavailable"},
            )
            runtime_with_error_payload._run_scheduled_auto_stop_callback_after_stop(auto_process_error)
            failed_payload = store.get_record(auto_process_error["record_id"])

        self.assertEqual(ignored["status"], "recorded")
        self.assertEqual(no_identity["status"], "recording")
        self.assertEqual(failed["scheduled_auto_stop"]["status"], "failed")
        self.assertIn("stop failed", failed["scheduled_auto_stop"]["error"])
        self.assertEqual(skipped["scheduled_auto_stop"]["process_queue_status"], "skipped")
        self.assertEqual(failed_queue["scheduled_auto_stop"]["process_queue_status"], "failed")
        self.assertIn("queue failed", failed_queue["scheduled_auto_stop"]["process_queue_error"])
        self.assertEqual(failed_payload["scheduled_auto_stop"]["process_queue_status"], "failed")
        self.assertEqual(failed_payload["scheduled_auto_stop"]["process_queue_error"], "local agent unavailable")

    def test_audio_only_recording_health_warns_when_media_duration_is_short(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Face to face",
                platform="unknown",
                meeting_link="",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record.update(
                {
                    "recording_started_at": "2026-05-02T13:27:28+00:00",
                    "recording_stopped_at": "2026-05-02T13:28:29+00:00",
                    "media": {
                        "recording_mode": "audio_only",
                        "audio_path": str(audio_path.relative_to(store.root_dir)),
                    },
                }
            )

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=16.0):
                health = runtime._recording_health(record)

        self.assertEqual(health["status"], "failed")
        self.assertEqual(health["duration_seconds"], 16.0)
        self.assertEqual(health["elapsed_seconds"], 61.0)
        self.assertIn("captured only 16s of audio for a 61s recording", health["warning"])
        self.assertIn("No silence padding was added", health["warning"])

    def test_audio_only_recording_health_warns_when_padded_source_audio_is_short(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Face to face",
                platform="unknown",
                meeting_link="",
            )
            original_path = store.record_dir(record["record_id"]) / "meeting.wav"
            original_path.write_bytes(b"short-audio")
            padded_path = store.record_dir(record["record_id"]) / "meeting.padded.wav"
            padded_path.write_bytes(b"padded-audio")
            record.update(
                {
                    "recording_started_at": "2026-05-03T03:37:49+00:00",
                    "recording_stopped_at": "2026-05-03T03:38:20+00:00",
                    "media": {
                        "recording_mode": "audio_only",
                        "source_audio_path": str(original_path.relative_to(store.root_dir)),
                        "audio_path": str(padded_path.relative_to(store.root_dir)),
                        "audio_finalization_profile": "post_stop_pad_v1",
                        "audio_original_duration_seconds": 8.554688,
                        "audio_target_duration_seconds": 31.0,
                    },
                }
            )

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=31.0):
                health = runtime._recording_health(record)

        self.assertEqual(health["status"], "failed")
        self.assertEqual(health["duration_seconds"], 31.0)
        self.assertEqual(health["source_duration_seconds"], 8.554688)
        self.assertEqual(health["elapsed_seconds"], 31.0)
        self.assertIn("captured only 9s of source audio for a 31s recording", health["warning"])
        self.assertIn("No silence padding was added", health["warning"])

    def test_audio_only_recording_health_warns_for_short_padded_recording(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Short face to face",
                platform="unknown",
                meeting_link="",
            )
            original_path = store.record_dir(record["record_id"]) / "meeting.wav"
            original_path.write_bytes(b"short-audio")
            padded_path = store.record_dir(record["record_id"]) / "meeting.padded.wav"
            padded_path.write_bytes(b"padded-audio")
            record.update(
                {
                    "recording_started_at": "2026-05-03T04:24:28+00:00",
                    "recording_stopped_at": "2026-05-03T04:24:36+00:00",
                    "media": {
                        "recording_mode": "audio_only",
                        "source_audio_path": str(original_path.relative_to(store.root_dir)),
                        "audio_path": str(padded_path.relative_to(store.root_dir)),
                        "audio_original_duration_seconds": 2.613313,
                        "audio_target_duration_seconds": 8.0,
                    },
                }
            )

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=8.0):
                health = runtime._recording_health(record)

        self.assertEqual(health["status"], "failed")
        self.assertEqual(health["duration_seconds"], 8.0)
        self.assertEqual(health["source_duration_seconds"], 2.613313)
        self.assertEqual(health["elapsed_seconds"], 8.0)
        self.assertIn("captured only 3s of source audio for a 8s recording", health["warning"])

    def test_audio_only_recording_health_warns_when_source_audio_stops_despite_signal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Zoom",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            original_path = store.record_dir(record["record_id"]) / "meeting.wav"
            original_path.write_bytes(b"short-audio")
            padded_path = store.record_dir(record["record_id"]) / "meeting.padded.wav"
            padded_path.write_bytes(b"padded-audio")
            record.update(
                {
                    "recording_started_at": "2026-05-03T07:25:54+00:00",
                    "recording_stopped_at": "2026-05-03T07:26:05+00:00",
                    "media": {
                        "audio_capture_profile": "screencapturekit_audio_v1",
                        "audio_segment_failures": [],
                        "recording_mode": "audio_only",
                        "source_audio_path": str(original_path.relative_to(store.root_dir)),
                        "audio_path": str(padded_path.relative_to(store.root_dir)),
                        "audio_original_duration_seconds": 3.061313,
                        "audio_target_duration_seconds": 11.0,
                    },
                }
            )

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=11.0), patch(
                "bpmis_jira_tool.meeting_recorder._audio_volume_metrics",
                return_value={"mean_volume_db": -41.9, "max_volume_db": -14.9},
            ):
                health = runtime._recording_health(record)

        self.assertEqual(health["status"], "failed")
        self.assertIn("captured only 3s of source audio for a 11s recording", health["warning"])
        self.assertEqual(health["source_duration_seconds"], 3.061313)

    def test_audio_only_recording_health_warns_when_media_duration_is_too_long(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Restarted recorder",
                platform="unknown",
                meeting_link="",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record.update(
                {
                    "recording_started_at": "2026-05-03T00:43:20+00:00",
                    "recording_stopped_at": "2026-05-03T00:43:52+00:00",
                    "media": {
                        "recording_mode": "audio_only",
                        "audio_path": str(audio_path.relative_to(store.root_dir)),
                    },
                }
            )

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=277.0):
                health = runtime._recording_health(record)

        self.assertEqual(health["status"], "warning")
        self.assertIn("277s for a 32s recording", health["warning"])

    def test_recording_health_covers_missing_source_duration_and_silent_audio(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            missing = store.create_record(
                owner_email="owner@npt.sg",
                title="Missing audio",
                platform="zoom",
                meeting_link="https://zoom.us/j/missing",
            )
            missing["media"] = {"audio_path": "records/missing/meeting.wav"}
            missing_health = runtime._recording_health(missing)

            source_record = store.create_record(
                owner_email="owner@npt.sg",
                title="Short source",
                platform="zoom",
                meeting_link="https://zoom.us/j/source",
            )
            source_audio = store.record_dir(source_record["record_id"]) / "source.wav"
            final_audio = store.record_dir(source_record["record_id"]) / "meeting.wav"
            source_audio.write_bytes(b"source-audio")
            final_audio.write_bytes(b"final-audio")
            source_record.update(
                {
                    "recording_started_at": "2026-05-03T03:37:49+00:00",
                    "recording_stopped_at": "2026-05-03T03:38:20+00:00",
                    "media": {
                        "audio_path": str(final_audio.relative_to(store.root_dir)),
                        "source_audio_path": str(source_audio.relative_to(store.root_dir)),
                    },
                }
            )
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", side_effect=[31.0, 8.0]):
                source_health = runtime._recording_health(source_record)

            silent_record = store.create_record(
                owner_email="owner@npt.sg",
                title="Silent audio",
                platform="zoom",
                meeting_link="https://zoom.us/j/silent",
            )
            silent_audio = store.record_dir(silent_record["record_id"]) / "meeting.wav"
            silent_audio.write_bytes(b"silent-audio")
            silent_record["media"] = {"audio_path": str(silent_audio.relative_to(store.root_dir))}
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=3.0), patch(
                "bpmis_jira_tool.meeting_recorder._audio_volume_metrics",
                return_value={"mean_volume_db": -85.0, "max_volume_db": -90.0},
            ):
                silent_health = runtime._recording_health(silent_record)

        self.assertEqual(missing_health["status"], "missing")
        self.assertIn("file is missing", missing_health["warning"])
        self.assertEqual(source_health["source_duration_seconds"], 8.0)
        self.assertEqual(source_health["status"], "failed")
        self.assertIn("No silence padding was added", source_health["warning"])
        self.assertEqual(silent_health["status"], "warning")
        self.assertIn("silent or nearly silent", silent_health["warning"])

    def test_audio_only_stop_finalization_keeps_short_audio_unpadded_after_recording(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Face to face",
                platform="unknown",
                meeting_link="",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record.update(
                {
                    "recording_started_at": "2026-05-03T00:00:00+00:00",
                    "recording_stopped_at": "2026-05-03T00:00:43+00:00",
                    "media": {
                        "recording_mode": "audio_only",
                        "audio_path": str(audio_path.relative_to(store.root_dir)),
                        "audio_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.wav",
                    },
                }
            )

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=11.0), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
            ) as run_command:
                finalized = runtime._finalize_audio_only_recording(record)

        media = finalized["media"]
        self.assertEqual(media["audio_path"], f"records/{record['record_id']}/meeting.wav")
        self.assertEqual(media["audio_url"], f"/meeting-recorder/assets/{record['record_id']}/meeting.wav")
        self.assertEqual(media["audio_finalization_profile"], "short_source_no_pad_v2")
        self.assertEqual(media["audio_original_duration_seconds"], 11.0)
        self.assertEqual(media["audio_recording_clock_seconds"], 43.0)
        self.assertTrue(media["audio_short_capture"])
        self.assertIn("no silence padding was added", media["audio_finalization_warning"])
        self.assertNotIn("source_audio_path", media)
        self.assertNotIn("audio_target_duration_seconds", media)
        run_command.assert_not_called()

    def test_audio_only_stop_finalization_leaves_normal_duration_audio_untouched(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Face to face",
                platform="unknown",
                meeting_link="",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record.update(
                {
                    "recording_started_at": "2026-05-03T00:00:00+00:00",
                    "recording_stopped_at": "2026-05-03T00:00:43+00:00",
                    "media": {
                        "recording_mode": "audio_only",
                        "audio_path": str(audio_path.relative_to(store.root_dir)),
                    },
                }
            )

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=42.0), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
            ) as run_command:
                finalized = runtime._finalize_audio_only_recording(record)

        self.assertEqual(finalized["media"]["audio_path"], f"records/{record['record_id']}/meeting.wav")
        self.assertNotIn("source_audio_path", finalized["media"])
        run_command.assert_not_called()

    def test_runtime_diagnostics_and_no_ffmpeg_start_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MeetingRecordStore(Path(temp_dir))
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_whisper_cpp_bin",
                return_value="whisper",
            ), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_executable",
                return_value="/usr/bin/xcrun",
            ):
                diagnostics = runtime.diagnostics()

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value=""):
                with self.assertRaisesRegex(ConfigError, "ffmpeg is required"):
                    runtime.start_recording(
                        owner_email="xiaodong.zheng@npt.sg",
                        title="No ffmpeg",
                        platform="zoom",
                        meeting_link="https://zoom.us/j/no-ffmpeg",
                    )

        self.assertTrue(diagnostics["ffmpeg_configured"])
        self.assertEqual(diagnostics["whisper_cpp_bin"], "whisper")
        self.assertIn("audio_capture_mode", diagnostics)

    def test_scheduled_auto_stop_edge_branches(self):
        class FakeTimer:
            created = []

            def __init__(self, delay, callback, kwargs=None):
                self.delay = delay
                self.callback = callback
                self.kwargs = kwargs or {}
                self.daemon = False
                self.cancelled = False
                self.started = False
                FakeTimer.created.append(self)

            def start(self):
                self.started = True

            def cancel(self):
                self.cancelled = True

        with tempfile.TemporaryDirectory() as temp_dir:
            store = MeetingRecordStore(Path(temp_dir))
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            missing_owner = {
                "record_id": "missing-owner",
                "owner_email": "",
                "status": "recording",
                "scheduled_end": "2026-05-01T00:00:00Z",
            }
            unchanged = runtime._schedule_auto_stop_if_needed(missing_owner)
            record = store.create_record(
                owner_email="xiaodong.zheng@npt.sg",
                title="Auto",
                platform="zoom",
                meeting_link="https://zoom.us/j/auto",
                scheduled_end="2999-05-01T00:00:00Z",
            )
            record["status"] = "recording"
            runtime._auto_stop_timers[record["record_id"]] = FakeTimer(1, lambda: None)
            with patch("bpmis_jira_tool.meeting_recorder.threading.Timer", FakeTimer):
                scheduled = runtime._schedule_auto_stop_if_needed(record)
            record["status"] = "recorded"
            store.save_record(record)
            runtime._scheduled_auto_stop_callback(
                record_id=record["record_id"],
                owner_email="xiaodong.zheng@npt.sg",
                scheduled_for="2026-05-01T00:20:00+00:00",
            )

        self.assertIs(unchanged, missing_owner)
        self.assertEqual(scheduled["scheduled_auto_stop"]["status"], "scheduled")
        self.assertTrue(FakeTimer.created[0].cancelled)
        self.assertTrue(FakeTimer.created[-1].started)

        failing_store = Mock()
        failing_store.list_records.return_value = []
        failing_store.get_record.side_effect = [ToolError("outer"), ToolError("inner")]
        runtime = MeetingRecorderRuntime(store=failing_store, config=MeetingRecorderConfig())
        runtime._scheduled_auto_stop_callback(
            record_id="missing",
            owner_email="xiaodong.zheng@npt.sg",
            scheduled_for="2026-05-01T00:20:00+00:00",
        )
        self.assertEqual(failing_store.get_record.call_count, 2)

    def test_signal_check_early_returns_and_ok_segmented_health(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MeetingRecordStore(Path(temp_dir))
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            recorded = store.create_record(
                owner_email="xiaodong.zheng@npt.sg",
                title="Done",
                platform="zoom",
                meeting_link="https://zoom.us/j/done",
            )
            recorded["status"] = "recorded"
            store.save_record(recorded)
            non_recording = runtime.check_recording_signal(
                record_id=recorded["record_id"],
                owner_email="xiaodong.zheng@npt.sg",
            )

            active = store.create_record(
                owner_email="xiaodong.zheng@npt.sg",
                title="Active",
                platform="zoom",
                meeting_link="https://zoom.us/j/active",
            )
            active["status"] = "recording"
            active["media"] = {"audio_capture_profile": "plain_audio"}
            store.save_record(active)
            plain = runtime.check_recording_signal(
                record_id=active["record_id"],
                owner_email="xiaodong.zheng@npt.sg",
            )

        self.assertEqual(non_recording["status"], "recorded")
        self.assertEqual(plain["media"]["audio_capture_profile"], "plain_audio")

    def test_audio_finalization_boundary_warnings_and_persisted_process_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MeetingRecordStore(Path(temp_dir))
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            sc_no_ffmpeg = {
                "record_id": "sc-no-ffmpeg",
                "media": {
                    "audio_capture_profile": "screencapturekit_audio_v1",
                    "system_audio_path": "records/sc-no-ffmpeg/system.caf",
                    "microphone_audio_path": "records/sc-no-ffmpeg/mic.caf",
                },
            }
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value=""):
                no_ffmpeg = runtime._finalize_screencapturekit_audio_recording(sc_no_ffmpeg)

            sc_single = store.create_record(
                owner_email="xiaodong.zheng@npt.sg",
                title="Single Track",
                platform="zoom",
                meeting_link="https://zoom.us/j/single-track",
            )
            sc_single_dir = store.record_dir(sc_single["record_id"])
            sc_single_system = sc_single_dir / "system.caf"
            sc_single_system.write_bytes(b"system")
            sc_single["media"] = {
                "audio_capture_profile": "screencapturekit_audio_v1",
                "system_audio_path": str(sc_single_system.relative_to(store.root_dir)),
                "microphone_audio_path": str((sc_single_dir / "missing.caf").relative_to(store.root_dir)),
            }

            def create_single_output(command, message, *, timeout_seconds=120):
                (sc_single_dir / "meeting.wav").write_bytes(b"RIFF" + b"1" * 100)
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=1.0,
            ), patch("bpmis_jira_tool.meeting_recorder._run_command", side_effect=create_single_output):
                sc_single_finalized = runtime._finalize_screencapturekit_audio_recording(sc_single)

            sc_record = store.create_record(
                owner_email="xiaodong.zheng@npt.sg",
                title="Screen",
                platform="zoom",
                meeting_link="https://zoom.us/j/screen",
            )
            sc_dir = store.record_dir(sc_record["record_id"])
            system_path = sc_dir / "system.caf"
            microphone_path = sc_dir / "mic.caf"
            system_path.write_bytes(b"system")
            microphone_path.write_bytes(b"mic")
            sc_record["media"] = {
                "audio_capture_profile": "screencapturekit_audio_v1",
                "system_audio_path": str(system_path.relative_to(store.root_dir)),
                "microphone_audio_path": str(microphone_path.relative_to(store.root_dir)),
            }
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=1.0,
            ), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=ToolError("mix failed"),
            ):
                sc_mix_failed = runtime._finalize_screencapturekit_audio_recording(sc_record)
            sc_mix_failed_warning = sc_mix_failed["media"]["audio_finalization_warning"]
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=1.0,
            ), patch("bpmis_jira_tool.meeting_recorder._run_command", return_value=Mock(stdout="", stderr="")):
                sc_empty_output = runtime._finalize_screencapturekit_audio_recording(sc_record)

            audio_only = {"media": {"recording_mode": "audio_only"}}
            missing_audio = {"media": {"recording_mode": "audio_only", "audio_path": "missing.wav"}}
            persisted = {
                "record_id": "persisted",
                "media": {"audio_path": "records/persisted/meeting.wav", "recorder_pid": 123},
            }
            with patch("bpmis_jira_tool.meeting_recorder._pid_command_contains", return_value=True), patch(
                "bpmis_jira_tool.meeting_recorder._terminate_process_id"
            ) as terminate_direct:
                runtime._terminate_persisted_recorder_process(persisted)
            with patch("bpmis_jira_tool.meeting_recorder._pid_command_contains", return_value=False), patch(
                "bpmis_jira_tool.meeting_recorder._find_recorder_processes_for_paths",
                return_value=[456, 789],
            ), patch("bpmis_jira_tool.meeting_recorder._terminate_process_id") as terminate:
                runtime._terminate_persisted_recorder_process(persisted)

        self.assertIn("ffmpeg is required", no_ffmpeg["media"]["audio_finalization_warning"])
        self.assertEqual(sc_single_finalized["media"]["audio_format"], "wav")
        self.assertIn("mix failed", sc_mix_failed_warning)
        self.assertIn("no audio bytes", sc_empty_output["media"]["audio_finalization_warning"])
        self.assertIs(runtime._finalize_audio_only_recording(audio_only), audio_only)
        self.assertIs(runtime._finalize_audio_only_recording(missing_audio), missing_audio)
        terminate_direct.assert_called_once_with(123)
        self.assertEqual([call.args[0] for call in terminate.call_args_list], [456, 789])

    def test_low_level_file_process_and_datetime_helpers_cover_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            record_dir = root / "records" / "rec-1"
            record_dir.mkdir(parents=True)
            audio_path = record_dir / "meeting.wav"
            audio_path.write_bytes(b"RIFF" + b"0" * 100)
            json_path = root / "payload.json"
            json_path.write_text('{"status":"ok"}', encoding="utf-8")
            invalid_json_path = root / "invalid.json"
            invalid_json_path.write_text("[1,2,3]", encoding="utf-8")
            tail_path = root / "tail.log"
            tail_path.write_text("first\nsecond\nthird", encoding="utf-8")

            candidates = _recorder_process_candidates(
                record={
                    "record_id": "rec-1",
                    "media": {
                        "audio_path": "records/rec-1/meeting.wav",
                        "source_audio_path": "records/rec-1/source.wav",
                    },
                },
                store_root=root,
            )
            audio_size = _safe_file_size(audio_path)
            missing_size = _safe_file_size(root / "missing.wav")
            json_payload = _read_json_file(json_path)
            invalid_json_payload = _read_json_file(invalid_json_path)
            missing_json_payload = _read_json_file(root / "missing.json")
            tail = _read_tail(tail_path, max_chars=5)
            bad_size_path = Mock()
            bad_size_path.exists.return_value = True
            bad_size_path.stat.side_effect = OSError("stat failed")
            bad_json_path = Mock()
            bad_json_path.exists.return_value = True
            bad_json_path.read_text.side_effect = OSError("read failed")
            bad_tail_path = Mock()
            bad_tail_path.exists.return_value = True
            bad_tail_path.read_text.side_effect = OSError("read failed")

        self.assertEqual(audio_size, 104)
        self.assertEqual(missing_size, 0)
        self.assertEqual(_safe_file_size(bad_size_path), 0)
        self.assertEqual(json_payload, {"status": "ok"})
        self.assertEqual(invalid_json_payload, {})
        self.assertEqual(missing_json_payload, {})
        self.assertEqual(_read_json_file(bad_json_path), {})
        self.assertEqual(tail, "third")
        self.assertEqual(_read_tail(bad_tail_path), "")
        self.assertIn(str(audio_path.resolve()), candidates)
        self.assertEqual(_safe_int("42"), 42)
        self.assertEqual(_safe_int("bad"), 0)
        self.assertEqual(_safe_float("4.5"), 4.5)
        self.assertIsNone(_safe_float("bad"))
        self.assertEqual(
            _recording_elapsed_seconds(
                {
                    "recording_started_at": "2026-05-01T00:00:00Z",
                    "recording_stopped_at": "2026-05-01T00:00:30+00:00",
                }
            ),
            30.0,
        )
        self.assertIsNone(_recording_elapsed_seconds({"recording_started_at": ""}))
        self.assertIsNone(_scheduled_auto_stop_deadline({"scheduled_end": "2026-05-01"}, grace_seconds=60))
        self.assertEqual(
            _scheduled_auto_stop_deadline({"scheduled_end": "2026-05-01T00:00:00Z"}, grace_seconds=60).isoformat(),
            "2026-05-01T00:01:00+00:00",
        )
        self.assertEqual(_parse_utc_timestamp("2026-05-01T08:00:00+08:00").isoformat(), "2026-05-01T00:00:00+00:00")
        self.assertIsNone(_parse_utc_timestamp("not-a-date"))
        self.assertEqual(_parse_meeting_datetime("2026-05-01T09:00:00", timezone_name="Asia/Singapore").tzinfo.key, "Asia/Singapore")
        self.assertIsNone(_parse_meeting_datetime("", timezone_name="Asia/Singapore"))
        self.assertEqual(_parse_whisper_language("auto-detected language: zh"), "zh")
        self.assertEqual(_parse_whisper_language("no language", fallback="en"), "en")
        self.assertEqual(_srt_timestamp_seconds("01:02:03,456"), 3723.456)
        self.assertEqual(_srt_timestamp_seconds("bad"), 0)
        with self.assertRaisesRegex(ToolError, "missing"):
            _safe_record_id("")

    def test_audio_process_helpers_handle_ps_and_tool_failures(self):
        completed = Mock(stdout="123 ffmpeg /tmp/meeting.wav\nbad ffmpeg /tmp/meeting.wav\n456 other\n", stderr="")
        with patch("bpmis_jira_tool.meeting_recorder.subprocess.run", return_value=completed):
            self.assertTrue(_pid_command_contains(123, ["/tmp/meeting.wav"]))
            self.assertEqual(_find_recorder_processes_for_paths(["/tmp/meeting.wav"]), [123])
        with patch("bpmis_jira_tool.meeting_recorder.subprocess.run", side_effect=subprocess.TimeoutExpired("ps", 5)):
            self.assertFalse(_pid_command_contains(123, ["/tmp/meeting.wav"]))
            self.assertEqual(_find_recorder_processes_for_paths(["/tmp/meeting.wav"]), [])
        self.assertFalse(_pid_command_contains(123, []))
        self.assertEqual(_find_recorder_processes_for_paths([]), [])
        with patch("bpmis_jira_tool.meeting_recorder.os.kill", return_value=None):
            self.assertTrue(_process_exists(123))
        with patch("bpmis_jira_tool.meeting_recorder.os.kill", side_effect=OSError("missing")):
            self.assertFalse(_process_exists(123))

    def test_process_termination_helpers_cover_group_fallback_and_kill(self):
        class FakeProcess:
            pid = 123

            def __init__(self):
                self.terminated = False
                self.killed = False
                self.wait_calls = 0

            def terminate(self):
                self.terminated = True

            def wait(self, timeout=None):
                self.wait_calls += 1
                if self.wait_calls == 1:
                    raise subprocess.TimeoutExpired("recorder", timeout)
                return 0

            def kill(self):
                self.killed = True

        process = FakeProcess()
        with patch("bpmis_jira_tool.meeting_recorder.os.killpg", side_effect=OSError("no group")):
            _terminate_recorder_process(process)

        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)
        with patch("bpmis_jira_tool.meeting_recorder.os.killpg", side_effect=OSError("no group")), patch(
            "bpmis_jira_tool.meeting_recorder.os.kill",
            side_effect=[None, OSError("gone")],
        ) as kill, patch("bpmis_jira_tool.meeting_recorder.time.sleep"):
            _terminate_process_id(123)
        self.assertGreaterEqual(kill.call_count, 2)

    def test_command_and_audio_helper_boundaries(self):
        convert_command = _build_ffmpeg_single_audio_convert_command(
            ffmpeg_path="ffmpeg",
            source_path=Path("source.wav"),
            output_path=Path("out.wav"),
        )
        mix_command = _build_ffmpeg_screencapturekit_mix_command(
            ffmpeg_path="ffmpeg",
            system_path=Path("system.caf"),
            microphone_path=Path("mic.caf"),
            output_path=Path("out.wav"),
        )

        self.assertIn("source.wav", convert_command)
        self.assertIn("[system][mic]amix", " ".join(mix_command))
        self.assertEqual(_bounded_int("bad", default=4, minimum=1, maximum=8), 4)
        self.assertEqual(_bounded_int(20, default=4, minimum=1, maximum=8), 8)
        with tempfile.TemporaryDirectory() as temp_dir:
            configured = Path(temp_dir) / "tool"
            configured.write_text("#!/bin/sh\n", encoding="utf-8")
            self.assertEqual(_resolve_executable(str(configured), ()), str(configured))
        with patch("bpmis_jira_tool.meeting_recorder.shutil.which", return_value="/usr/bin/tool"):
            self.assertEqual(_resolve_executable("tool", ()), "/usr/bin/tool")
        with patch("bpmis_jira_tool.meeting_recorder.shutil.which", return_value=None):
            self.assertIsNone(_resolve_executable("definitely-missing-tool", ("/tmp/also-missing",)))

    def test_minutes_and_process_configuration_helpers_cover_boundaries(self):
        long_line = "x" * 20
        chunks = _split_meeting_transcript_chunks(f"short\n{long_line}\nend", target_tokens=2)

        self.assertEqual(_meeting_minutes_sender_name({"owner_name": "Alice"}), "Alice")
        self.assertEqual(_meeting_minutes_sender_name({"owner_email": "xiaodong.zheng@npt.sg"}), "Xiaodong Zheng")
        self.assertEqual(_meeting_minutes_sender_name({"owner_email": "nodot@npt.sg"}), "Nodot")
        self.assertEqual(_meeting_minutes_sender_name({"owner_email": "...@npt.sg"}), "...")
        self.assertEqual(_meeting_minutes_sender_name({}), "Xiaodong")
        self.assertEqual(_split_meeting_transcript_chunks("", target_tokens=100), [""])
        self.assertGreater(len(chunks), 2)
        with self.assertRaisesRegex(ToolError, "not available"):
            _assert_record_owner({"owner_email": "owner@npt.sg"}, "other@npt.sg")
        self.assertEqual(_normalized_background_nice("bad"), 0)
        self.assertEqual(_normalized_background_nice(50), 20)
        self.assertEqual(_normalized_status_every_buffers("bad"), 250)
        self.assertEqual(_normalized_status_every_buffers(0), 250)
        self.assertEqual(_normalized_status_every_buffers(-5), 1)
        self.assertEqual(_normalized_startup_silence_grace_seconds("bad"), 300)
        self.assertEqual(_normalized_startup_silence_grace_seconds(-5), 0)
        self.assertEqual(_normalized_auto_stop_grace_seconds("bad"), 20 * 60)
        self.assertEqual(_normalized_auto_stop_grace_seconds(999999), 24 * 60 * 60)
        with patch("bpmis_jira_tool.meeting_recorder.shutil.which", return_value=None):
            self.assertEqual(_background_command(["ffmpeg"], 10), ["ffmpeg"])
        with patch("bpmis_jira_tool.meeting_recorder.shutil.which", return_value="/usr/bin/nice"):
            self.assertEqual(_background_command(["ffmpeg"], 5), ["nice", "-n", "5", "ffmpeg"])
        with patch("bpmis_jira_tool.meeting_recorder.os.cpu_count", return_value=2):
            self.assertEqual(_effective_transcript_segment_workers(8), 2)
            self.assertEqual(_effective_whisper_threads(whisper_threads=0, segment_workers="bad"), 2)
            self.assertEqual(_effective_whisper_threads(whisper_threads=4, segment_workers=8), 4)
            self.assertEqual(_effective_transcript_segment_workers("bad"), 1)
            self.assertEqual(_effective_whisper_threads(whisper_threads="bad", segment_workers=2), 1)

    def test_datetime_srt_and_resolve_helpers_cover_tail_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = root / "candidate-tool"
            candidate.write_text("#!/bin/sh\n", encoding="utf-8")
            srt_path = root / "edge.srt"
            srt_path.write_text(
                "1\n\n"
                "2\n00:00:00,000 --> 00:00:01,000\n\n"
                "3\n00:00:01,000 --> 00:00:02,000\nValid text\n",
                encoding="utf-8",
            )

            deadline = _scheduled_auto_stop_deadline({"scheduled_end": "not-a-dateTbad"}, grace_seconds=60)
            naive = _parse_utc_timestamp("2026-05-01T00:00:00")
            chunks = _parse_srt_transcript(srt_path)
            resolved_candidate = _resolve_executable("", (str(candidate),))

        self.assertIsNone(deadline)
        self.assertEqual(naive.tzinfo, timezone.utc)
        self.assertEqual(chunks, [{"start_seconds": 1.0, "end_seconds": 2.0, "text": "Valid text"}])
        self.assertEqual(resolved_candidate, str(candidate))

    def test_run_command_covers_failure_edges(self):
        successful = subprocess.CompletedProcess(["ffmpeg"], 0, stdout="ok", stderr="")
        with patch("bpmis_jira_tool.meeting_recorder.subprocess.run", return_value=successful):
            self.assertIs(_run_command(["ffmpeg"], "Failed:"), successful)
        with patch("bpmis_jira_tool.meeting_recorder.subprocess.run", side_effect=OSError("missing binary")):
            with self.assertRaisesRegex(ToolError, "missing binary"):
                _run_command(["ffmpeg"], "Failed:")
        with patch(
            "bpmis_jira_tool.meeting_recorder.subprocess.run",
            return_value=subprocess.CompletedProcess(["ffmpeg"], 1, stdout="stdout detail", stderr="stderr detail"),
        ):
            with self.assertRaisesRegex(ToolError, "stderr detail"):
                _run_command(["ffmpeg"], "Failed:")

    def test_screencapturekit_helper_resolution_uses_current_bundle_and_builds_when_stale(self):
        source_path = Path("tools/meeting_screencapture_helper.swift").resolve()
        source_digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
        with tempfile.TemporaryDirectory() as temp_dir:
            store_root = Path(temp_dir) / "data" / "meeting_recorder"
            output_dir = store_root.parent / "bin"
            app_path = output_dir / "Meeting Recorder Capture Helper.app"
            macos_dir = app_path / "Contents" / "MacOS"
            resources_dir = app_path / "Contents" / "Resources"
            helper_path = macos_dir / "meeting-screencapture-helper"
            info_plist_path = app_path / "Contents" / "Info.plist"
            source_digest_path = resources_dir / "source.sha256"
            macos_dir.mkdir(parents=True)
            resources_dir.mkdir(parents=True)
            helper_path.write_text("#!/bin/sh\n", encoding="utf-8")
            info_plist_path.write_text("<plist/>", encoding="utf-8")
            source_digest_path.write_text(f"{source_digest}\n", encoding="utf-8")

            current_helper = _resolve_screencapturekit_helper(store_root)
            source_digest_path.write_text("stale\n", encoding="utf-8")
            with patch("bpmis_jira_tool.meeting_recorder._resolve_executable", side_effect=["/usr/bin/xcrun", "/usr/bin/codesign"]), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                return_value=Mock(stdout="", stderr=""),
            ) as run_command:
                rebuilt_helper = _resolve_screencapturekit_helper(store_root)
            source_digest_path.write_text("stale\n", encoding="utf-8")
            with patch("bpmis_jira_tool.meeting_recorder._resolve_executable", return_value=""):
                with self.assertRaisesRegex(ConfigError, "xcrun"):
                    _resolve_screencapturekit_helper(store_root)
            rebuilt_info_plist = info_plist_path.read_text(encoding="utf-8")

        self.assertEqual(current_helper, helper_path)
        self.assertEqual(rebuilt_helper, helper_path)
        self.assertEqual(run_command.call_count, 2)
        self.assertIn("NSScreenCaptureUsageDescription", rebuilt_info_plist)

    def test_screencapturekit_helper_resolution_reports_source_and_digest_failures(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store_root = Path(temp_dir) / "data" / "meeting_recorder"
            with patch.object(Path, "exists", return_value=False):
                with self.assertRaisesRegex(ConfigError, "helper source was not found"):
                    _resolve_screencapturekit_helper(store_root)

            with patch.object(Path, "exists", return_value=True), patch.object(Path, "read_bytes", side_effect=OSError("denied")):
                with self.assertRaisesRegex(ConfigError, "could not be read"):
                    _resolve_screencapturekit_helper(store_root)

            def fake_exists(path):
                return Path(path).name in {
                    "meeting_screencapture_helper.swift",
                    "meeting-screencapture-helper",
                    "Info.plist",
                    "source.sha256",
                }

            with patch.object(Path, "exists", fake_exists), patch.object(Path, "read_bytes", return_value=b"source"), patch.object(
                Path,
                "read_text",
                side_effect=OSError("digest denied"),
            ), patch("bpmis_jira_tool.meeting_recorder._resolve_executable", return_value=""):
                with self.assertRaisesRegex(ConfigError, "xcrun"):
                    _resolve_screencapturekit_helper(store_root)

    def test_audio_volume_duration_helpers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            audio_path = Path(temp_dir) / "audio.wav"
            audio_path.write_bytes(b"audio")
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value=""), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_executable",
                return_value="",
            ):
                unavailable_volume = _audio_volume_metrics(audio_path)
                zero_duration = _audio_duration_seconds(audio_path)
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=ToolError("volumedetect failed"),
            ):
                failed_volume = _audio_volume_metrics(audio_path, start_seconds=-1, duration_seconds=0)
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                return_value=Mock(stdout="", stderr="mean_volume: -12.0 dB\nmax_volume: -2.0 dB"),
            ):
                ok_volume = _audio_volume_metrics(audio_path)
            with patch("bpmis_jira_tool.meeting_recorder._resolve_executable", return_value="ffprobe"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                return_value=Mock(stdout="12.5\n"),
            ):
                duration = _audio_duration_seconds(audio_path)
            with patch("bpmis_jira_tool.meeting_recorder._resolve_executable", return_value="ffprobe"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=ToolError("ffprobe failed"),
            ):
                failed_duration = _audio_duration_seconds(audio_path)
        self.assertEqual(unavailable_volume["status"], "unavailable")
        self.assertEqual(zero_duration, 0.0)
        self.assertEqual(failed_volume["status"], "unavailable")
        self.assertEqual(ok_volume["status"], "ok")
        self.assertEqual(duration, 12.5)
        self.assertEqual(failed_duration, 0.0)

    def test_transcript_quality_helpers_cover_repetition_and_language_boundaries(self):
        short = _transcript_repetition_metrics([{"text": "hello"}])
        repetitive = _transcript_repetition_metrics([{"text": "same"}] * 5)
        varied = _transcript_repetition_metrics([{"text": f"chunk {index}"} for index in range(5)])
        bad_srt_path = Mock()
        bad_srt_path.exists.return_value = True
        bad_srt_path.read_text.side_effect = OSError("read denied")
        malformed_srt_path = Mock()
        malformed_srt_path.exists.return_value = True
        malformed_srt_path.read_text.return_value = "1\nnot a timestamp\n\n2\n00:00:00,000 --> 00:00:01,000\n"

        self.assertFalse(short["is_repetitive"])
        self.assertTrue(repetitive["is_repetitive"])
        self.assertFalse(varied["is_repetitive"])
        self.assertTrue(_should_retry_transcript_languages({"language": "fr", "repetitive_chunk_count": 0}))
        self.assertTrue(_should_retry_transcript_languages({"language": "en", "repetitive_chunk_count": 5}))
        self.assertFalse(_should_retry_transcript_languages({"language": "en,zh", "repetitive_chunk_count": 0}))
        self.assertLess(
            _transcript_quality_score({"language": "fr", "repetitive_chunk_count": 4, "risk_no_audio_segment_count": 2}),
            50,
        )
        self.assertEqual(_parse_srt_transcript(bad_srt_path), [])
        self.assertEqual(_parse_srt_transcript(malformed_srt_path), [])
        self.assertIsNone(_parse_meeting_datetime("not-a-date", timezone_name="Asia/Singapore"))

    def test_srt_parser_skips_empty_text_after_valid_timestamp(self):
        class TruthyEmpty(str):
            def __new__(cls):
                return super().__new__(cls, "")

            def __bool__(self):
                return True

            def strip(self):
                return self

        class FakeBlock(str):
            def strip(self):
                return self

            def splitlines(self):
                return ["1", "00:00:00,000 --> 00:00:01,000", TruthyEmpty()]

        fake_path = Mock()
        fake_path.exists.return_value = True
        fake_path.read_text.return_value = "payload"
        with patch("bpmis_jira_tool.meeting_recorder.re.split", return_value=[FakeBlock("block")]):
            chunks = _parse_srt_transcript(fake_path)

        self.assertEqual(chunks, [])

    def test_process_detection_and_file_helpers_cover_failure_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch("bpmis_jira_tool.meeting_recorder.os.kill", side_effect=OSError("missing")):
                _terminate_process_id(54321)
            kill_calls = []

            def fake_kill(pid, sig):
                kill_calls.append((pid, sig))

            with patch("bpmis_jira_tool.meeting_recorder.os.kill", side_effect=fake_kill), patch(
                "bpmis_jira_tool.meeting_recorder._process_exists",
                side_effect=[True, False],
            ), patch("bpmis_jira_tool.meeting_recorder.time.sleep"):
                _terminate_process_id(54322)
            kill_after_timeout_calls = []

            def fake_timeout_kill(pid, sig):
                kill_after_timeout_calls.append((pid, sig))
                if sig == signal.SIGKILL:
                    raise OSError("already gone")

            with patch("bpmis_jira_tool.meeting_recorder.os.kill", side_effect=fake_timeout_kill), patch(
                "bpmis_jira_tool.meeting_recorder._process_exists",
                return_value=True,
            ), patch("bpmis_jira_tool.meeting_recorder.time.time", side_effect=[0, 0, 11]), patch(
                "bpmis_jira_tool.meeting_recorder.time.sleep"
            ):
                _terminate_process_id(54323)

            missing_tail = _read_tail(root / "missing.log")
            with patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    ["ps"],
                    0,
                    stdout=(
                        "100 ffmpeg /tmp/not-this-record/meeting.wav\n"
                        "\n"
                        "not-a-pid ffmpeg /tmp/record/meeting.wav\n"
                        "200 ffmpeg /tmp/record/meeting.wav\n"
                    ),
                    stderr="",
                ),
            ):
                pids = _find_recorder_processes_for_paths(["/tmp/record/meeting.wav"])

            class FakeProcessLine(str):
                def strip(self):
                    return self

                def split(self, *args, **kwargs):
                    return []

            class FakeProcessStdout(str):
                def splitlines(self):
                    return [FakeProcessLine("ffmpeg /tmp/record/meeting.wav")]

            with patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.run",
                return_value=subprocess.CompletedProcess(["ps"], 0, stdout=FakeProcessStdout("nonempty"), stderr=""),
            ):
                malformed_pid_line = _find_recorder_processes_for_paths(["/tmp/record/meeting.wav"])

        self.assertEqual(missing_tail, "")
        self.assertEqual(kill_calls[0], (54322, signal.SIGTERM))
        self.assertEqual(kill_after_timeout_calls[-1], (54323, signal.SIGKILL))
        self.assertEqual(pids, [200])
        self.assertEqual(malformed_pid_line, [])

    def test_screencapturekit_recorder_start_stop_and_signal_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            status_path = root / "status.json"
            system_path = root / "system.caf"
            mic_path = root / "mic.caf"
            log_path = root / "capture.log"

            class FakeProcess:
                pid = 321

                def __init__(self, poll_value=None, wait_timeout=False):
                    self.poll_value = poll_value
                    self.wait_timeout = wait_timeout
                    self.terminated = False
                    self.killed = False

                def poll(self):
                    return self.poll_value

                def terminate(self):
                    self.terminated = True

                def wait(self, timeout=None):
                    if self.wait_timeout:
                        self.wait_timeout = False
                        raise subprocess.TimeoutExpired("helper", timeout)
                    return 0

                def kill(self):
                    self.killed = True

            def build_recorder():
                return _ScreenCaptureKitAudioRecorder(
                    helper_path=Path("/tmp/helper"),
                    status_path=status_path,
                    system_audio_path=system_path,
                    microphone_audio_path=mic_path,
                    log_path=log_path,
                )

            no_process_poll = build_recorder().poll()
            exited_process = FakeProcess(poll_value=1)
            with patch("bpmis_jira_tool.meeting_recorder.subprocess.Popen", return_value=exited_process):
                failed_start = build_recorder().start()

            status_path.write_text('{"status":"failed","message":"permission denied"}', encoding="utf-8")
            failed_status_process = FakeProcess(poll_value=None)
            with patch("bpmis_jira_tool.meeting_recorder.subprocess.Popen", return_value=failed_status_process), patch(
                "bpmis_jira_tool.meeting_recorder.time.sleep"
            ):
                failed_status = build_recorder().start()

            status_path.unlink()
            timeout_process = FakeProcess(poll_value=None)
            with patch("bpmis_jira_tool.meeting_recorder.subprocess.Popen", return_value=timeout_process), patch(
                "bpmis_jira_tool.meeting_recorder.time.monotonic",
                side_effect=[0, 0, 9],
            ), patch("bpmis_jira_tool.meeting_recorder.time.sleep") as start_sleep:
                timeout_start = build_recorder().start()

            status_path.write_text('{"status":"recording"}', encoding="utf-8")
            system_path.write_bytes(b"system")
            mic_path.write_bytes(b"mic")
            running_process = FakeProcess(poll_value=None, wait_timeout=True)
            recorder = build_recorder()
            with patch("bpmis_jira_tool.meeting_recorder.subprocess.Popen", return_value=running_process), patch(
                "bpmis_jira_tool.meeting_recorder.time.sleep"
            ):
                ok_start = recorder.start()
            running_poll = recorder.poll()
            stop_summary = recorder.stop()

            recorder._process = FakeProcess(poll_value=1)
            log_path.write_text("helper crashed", encoding="utf-8")
            failed_signal = recorder.signal_snapshot(sample_seconds=0)
            recorder._process = FakeProcess(poll_value=None)
            with patch("bpmis_jira_tool.meeting_recorder.time.sleep"), patch(
                "bpmis_jira_tool.meeting_recorder._safe_file_size",
                side_effect=[0, 0, 6000, 6000],
            ):
                ok_signal = recorder.signal_snapshot(sample_seconds=0)
            with patch("bpmis_jira_tool.meeting_recorder.time.sleep"), patch(
                "bpmis_jira_tool.meeting_recorder._safe_file_size",
                side_effect=[100, 100, 101, 101],
            ):
                pending_signal = recorder.signal_snapshot(sample_seconds=0)

        self.assertEqual(no_process_poll, 0)
        self.assertEqual(failed_start["status"], "failed")
        self.assertEqual(failed_status["warning"], "permission denied")
        self.assertIn("did not start within 8s", timeout_start["warning"])
        start_sleep.assert_called_once_with(0.1)
        self.assertEqual(ok_start["status"], "ok")
        self.assertIsNone(running_poll)
        self.assertTrue(running_process.terminated)
        self.assertTrue(running_process.killed)
        self.assertEqual(stop_summary["recorder_pid"], 321)
        self.assertEqual(failed_signal["status"], "failed")
        self.assertEqual(ok_signal["status"], "ok")
        self.assertEqual(pending_signal["status"], "pending")

    def test_screencapturekit_audio_mix_runs_at_background_priority(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig(background_nice=8))
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Zoom review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record_dir = store.record_dir(record["record_id"])
            system_path = record_dir / "screencapture-system.caf"
            microphone_path = record_dir / "screencapture-microphone.caf"
            system_path.write_bytes(b"system-audio")
            microphone_path.write_bytes(b"mic-audio")
            record["media"] = {
                "audio_capture_profile": "screencapturekit_audio_v1",
                "system_audio_path": str(system_path.relative_to(store.root_dir)),
                "microphone_audio_path": str(microphone_path.relative_to(store.root_dir)),
            }

            def fake_run(command, *_args, **_kwargs):
                Path(command[-1]).write_bytes(b"mixed-audio" * 8)
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=60.0,
            ), patch("bpmis_jira_tool.meeting_recorder._run_command", side_effect=fake_run) as run_command:
                finalized = runtime._finalize_screencapturekit_audio_recording(record)

        command = run_command.call_args.args[0]
        self.assertEqual(command[:3], ["nice", "-n", "8"])
        self.assertIn("amix=inputs=2", " ".join(command))
        self.assertEqual(finalized["media"]["audio_path"], f"records/{record['record_id']}/meeting.wav")
        self.assertEqual(finalized["media"]["audio_mix_command"][:3], ["nice", "-n", "8"])
        self.assertEqual(finalized["media"]["screencapture_system_duration_ratio"], 1.0)
        self.assertEqual(finalized["media"]["screencapture_microphone_duration_ratio"], 1.0)

    def test_screencapturekit_short_microphone_track_records_diagnostic_warning(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig(background_nice=8))
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Zoom review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record_dir = store.record_dir(record["record_id"])
            system_path = record_dir / "screencapture-system.caf"
            microphone_path = record_dir / "screencapture-microphone.caf"
            system_path.write_bytes(b"system-audio")
            microphone_path.write_bytes(b"mic-audio")
            record["media"] = {
                "audio_capture_profile": "screencapturekit_audio_v1",
                "system_audio_path": str(system_path.relative_to(store.root_dir)),
                "microphone_audio_path": str(microphone_path.relative_to(store.root_dir)),
            }

            def fake_duration(path):
                return 1000.0 if Path(path).name == "screencapture-system.caf" else 120.0

            def fake_run(command, *_args, **_kwargs):
                Path(command[-1]).write_bytes(b"mixed-audio" * 8)
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                side_effect=fake_duration,
            ), patch("bpmis_jira_tool.meeting_recorder._run_command", side_effect=fake_run):
                finalized = runtime._finalize_screencapturekit_audio_recording(record)

        media = finalized["media"]
        self.assertEqual(media["screencapture_system_duration_seconds"], 1000.0)
        self.assertEqual(media["screencapture_microphone_duration_seconds"], 120.0)
        self.assertEqual(media["screencapture_system_duration_ratio"], 1.0)
        self.assertEqual(media["screencapture_microphone_duration_ratio"], 0.12)
        self.assertIn("Microphone track is shorter", media["screencapture_track_warning"])
        self.assertNotIn("audio_finalization_warning", media)

    def test_stop_recording_terminates_persisted_recorder_process_after_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Restarted recorder",
                platform="unknown",
                meeting_link="",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record.update(
                {
                    "status": "recording",
                    "recording_started_at": "2026-05-03T00:43:20+00:00",
                    "media": {
                        "recording_mode": "audio_only",
                        "audio_path": str(audio_path.relative_to(store.root_dir)),
                        "recorder_pid": 12345,
                    },
                }
            )
            store.save_record(record)

            with patch.object(runtime, "_terminate_persisted_recorder_process") as terminate, patch.object(
                runtime,
                "_recording_health",
                return_value={"status": "ok", "checked_at": "2026-05-03T00:43:52+00:00", "warning": ""},
            ):
                stopped = runtime.stop_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(stopped["status"], "recorded")
        terminate.assert_called_once()

    def test_linked_meeting_recording_uses_screencapturekit_audio_helper(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_screencapturekit_helper",
                return_value=Path("/tmp/meeting-screencapture-helper"),
            ), patch(
                "bpmis_jira_tool.meeting_recorder._ScreenCaptureKitAudioRecorder.start",
                return_value={"status": "ok", "latency_seconds": 0.2, "bytes": 48000, "pid": 1234},
            ):
                record = runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Zoom review",
                    platform="zoom",
                    meeting_link="https://zoom.us/j/123",
                    recording_mode="audio_only",
                )

        self.assertEqual(record["media"]["recording_mode"], "audio_only")
        self.assertEqual(record["media"]["audio_capture_profile"], "screencapturekit_audio_v1")
        self.assertIn("audio_path", record["media"])
        self.assertNotIn("video_path", record["media"])
        self.assertEqual(record["diagnostics_snapshot"]["requested_recording_mode"], "audio_only")
        command = runtime._processes[record["record_id"]].command
        self.assertEqual(command[:3], ["nice", "-n", "10"])
        self.assertIn("--system-output", command)
        self.assertIn("--microphone-output", command)
        self.assertIn("--status-every-buffers", command)

    def test_start_recording_rejects_duplicate_active_recording(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            active = store.create_record(owner_email="owner@npt.sg", title="Active review", platform="zoom", meeting_link="")
            active["status"] = "recording"
            store.save_record(active)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )

            with self.assertRaisesRegex(ToolError, "already active"):
                runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="New review",
                    platform="zoom",
                    meeting_link="https://zoom.us/j/123",
                    recording_mode="audio_only",
                )

    def test_screencapturekit_start_failure_marks_record_failed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_screencapturekit_helper",
                return_value=Path("/tmp/meeting-screencapture-helper"),
            ), patch(
                "bpmis_jira_tool.meeting_recorder._ScreenCaptureKitAudioRecorder.start",
                return_value={"status": "failed", "warning": "The user declined TCCs for application, window, display capture"},
            ), self.assertRaisesRegex(ToolError, "declined TCCs"):
                runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Face to face",
                    platform="unknown",
                    meeting_link="",
                    recording_mode="audio_only",
                )

            records = store.list_records(owner_email="owner@npt.sg")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["status"], "failed")
        self.assertIn("declined TCCs", records[0]["error"])

    def test_screencapturekit_start_exception_marks_record_failed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_screencapturekit_helper",
                side_effect=ConfigError("helper missing"),
            ), self.assertRaisesRegex(ToolError, "helper missing"):
                runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Face to face",
                    platform="unknown",
                    meeting_link="",
                    recording_mode="audio_only",
                )

            records = store.list_records(owner_email="owner@npt.sg")

        self.assertEqual(records[0]["status"], "failed")
        self.assertIn("helper missing", records[0]["error"])

    def test_stop_recording_covers_attached_recorders_and_subprocess_termination(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())

            screen_record = store.create_record(owner_email="owner@npt.sg", title="Screen", platform="zoom", meeting_link="")
            screen_record.update(
                {
                    "status": "recording",
                    "recording_started_at": "2026-05-03T00:00:00+00:00",
                    "media": {"audio_capture_profile": "screencapturekit_audio_v1"},
                }
            )
            store.save_record(screen_record)
            screen_recorder = _ScreenCaptureKitAudioRecorder(
                helper_path=Path("/tmp/helper"),
                system_audio_path=root / "system.caf",
                microphone_audio_path=root / "mic.caf",
                status_path=root / "status.json",
                log_path=root / "screen.log",
            )
            runtime._processes[screen_record["record_id"]] = screen_recorder

            process_record = store.create_record(owner_email="owner@npt.sg", title="Process", platform="zoom", meeting_link="")
            process_record.update({"status": "recording", "recording_started_at": "2026-05-03T00:00:00+00:00"})
            store.save_record(process_record)
            process = Mock()
            process.poll.return_value = None
            runtime._processes[process_record["record_id"]] = process

            with patch.object(
                _ScreenCaptureKitAudioRecorder,
                "stop",
                return_value={"screencapture_stop_status": "stopped"},
            ) as screen_stop, patch(
                "bpmis_jira_tool.meeting_recorder._terminate_recorder_process",
            ) as terminate_process, patch.object(
                runtime,
                "_recording_health",
                return_value={"status": "ok", "checked_at": "2026-05-03T00:01:00+00:00", "warning": ""},
            ):
                stopped_screen = runtime.stop_recording(record_id=screen_record["record_id"], owner_email="owner@npt.sg")
                stopped_process = runtime.stop_recording(record_id=process_record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(stopped_screen["media"]["screencapture_stop_status"], "stopped")
        self.assertEqual(stopped_process["status"], "recorded")
        screen_stop.assert_called_once()
        terminate_process.assert_called_once_with(process)

    def test_stop_recording_marks_failed_when_health_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MeetingRecordStore(Path(temp_dir))
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(owner_email="owner@npt.sg", title="Bad audio", platform="zoom", meeting_link="")
            record["status"] = "recording"
            record["recording_started_at"] = "2026-05-03T00:00:00+00:00"
            store.save_record(record)

            with patch.object(runtime, "_terminate_persisted_recorder_process"), patch.object(
                runtime,
                "_recording_health",
                return_value={"status": "failed", "warning": "Recorded audio file is missing."},
            ):
                stopped = runtime.stop_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(stopped["status"], "failed")
        self.assertEqual(stopped["error"], "Recorded audio file is missing.")

    def test_signal_check_keeps_running_when_wav_file_has_not_flushed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_screencapturekit_helper",
                return_value=Path("/tmp/meeting-screencapture-helper"),
            ), patch(
                "bpmis_jira_tool.meeting_recorder._ScreenCaptureKitAudioRecorder.start",
                return_value={"status": "ok", "latency_seconds": 0.2, "bytes": 48000, "pid": 12345},
            ), patch(
                "bpmis_jira_tool.meeting_recorder._ScreenCaptureKitAudioRecorder.signal_snapshot",
                return_value={
                    "status": "pending",
                    "checked_at": "2026-05-03T00:43:23+00:00",
                    "growth_bytes": 0,
                    "warning": "ScreenCaptureKit recorder is running; waiting for system or microphone audio samples.",
                },
            ):
                record = runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Zoom review",
                    platform="zoom",
                    meeting_link="",
                    recording_mode="audio_only",
                )
                checked = runtime.check_recording_signal(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(checked["status"], "recording")
        self.assertEqual(checked.get("error") or "", "")
        self.assertEqual(checked["recording_health"]["status"], "recording")
        self.assertEqual(checked["media"]["early_audio_check"]["status"], "pending")
        self.assertIn("early_audio_check", checked["media"])

    def test_signal_check_fails_unattached_and_stops_failed_recorders(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())

            unattached_screen = store.create_record(owner_email="owner@npt.sg", title="Screen", platform="zoom", meeting_link="")
            unattached_screen.update(
                {
                    "status": "recording",
                    "media": {"audio_capture_profile": "screencapturekit_audio_v1"},
                }
            )
            store.save_record(unattached_screen)

            attached_screen = store.create_record(owner_email="owner@npt.sg", title="Attached Screen", platform="zoom", meeting_link="")
            attached_screen.update(
                {
                    "status": "recording",
                    "media": {"audio_capture_profile": "screencapturekit_audio_v1"},
                }
            )
            store.save_record(attached_screen)
            screen_recorder = _ScreenCaptureKitAudioRecorder(
                helper_path=Path("/tmp/helper"),
                system_audio_path=root / "system.caf",
                microphone_audio_path=root / "mic.caf",
                status_path=root / "status.json",
                log_path=root / "screen.log",
            )
            runtime._processes[attached_screen["record_id"]] = screen_recorder

            failed_check = {
                "status": "failed",
                "checked_at": "2026-05-03T00:00:05+00:00",
                "warning": "audio is not growing",
            }
            with patch.object(_ScreenCaptureKitAudioRecorder, "signal_snapshot", return_value=failed_check), patch.object(
                _ScreenCaptureKitAudioRecorder,
                "stop",
                return_value={"screen_summary": "stopped"},
            ):
                screen_missing = runtime.check_recording_signal(record_id=unattached_screen["record_id"], owner_email="owner@npt.sg")
                screen_failed = runtime.check_recording_signal(record_id=attached_screen["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(screen_missing["status"], "failed")
        self.assertIn("not attached", screen_missing["recording_health"]["warning"])
        self.assertEqual(screen_failed["status"], "failed")
        self.assertEqual(screen_failed["media"]["screen_summary"], "stopped")

    def test_records_reconcile_failed_screencapturekit_helper_without_frontend_poll(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(owner_email="owner@npt.sg", title="Display lost", platform="google_meet", meeting_link="")
            record_dir = store.record_dir(record["record_id"])
            status_path = record_dir / "screencapture-status.json"
            system_path = record_dir / "screencapture-system.caf"
            microphone_path = record_dir / "screencapture-microphone.caf"
            system_path.write_bytes(b"system-audio")
            microphone_path.write_bytes(b"microphone-audio")
            status_path.write_text(
                json.dumps(
                    {
                        "status": "failed",
                        "message": "stream_stopped: Failed to find any displays or windows to capture",
                        "updated_at": "2026-05-26T02:30:34Z",
                    }
                ),
                encoding="utf-8",
            )
            record.update(
                {
                    "status": "recording",
                    "recording_started_at": "2026-05-26T02:05:10+00:00",
                    "media": {
                        "audio_capture_profile": "screencapturekit_audio_v1",
                        "recorder_pid": 12345,
                        "system_audio_path": str(system_path.relative_to(root)),
                        "microphone_audio_path": str(microphone_path.relative_to(root)),
                        "screencapture_status_path": str(status_path.relative_to(root)),
                    },
                }
            )
            store.save_record(record)

            with patch.object(runtime, "_terminate_persisted_recorder_process") as terminate:
                records = runtime.list_records(owner_email="owner@npt.sg")
                stored_status = store.get_record(records[0]["record_id"])["status"]

            self.assertEqual(records[0]["status"], "failed")
            self.assertEqual(records[0]["recording_stop_reason"], "screencapturekit_failed")
            self.assertEqual(records[0]["recording_stopped_at"], "2026-05-26T02:30:34+00:00")
            self.assertIn("Failed to find any displays", records[0]["error"])
            self.assertEqual(records[0]["media"]["screencapture_status"]["status"], "failed")
            self.assertEqual(records[0]["media"]["screencapture_system_bytes"], len(b"system-audio"))
            self.assertEqual(records[0]["media"]["screencapture_microphone_bytes"], len(b"microphone-audio"))
            terminate.assert_called_once()
            self.assertEqual(stored_status, "failed")

    def test_start_recording_reconciles_failed_active_record_before_duplicate_check(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"))
            stale = store.create_record(owner_email="owner@npt.sg", title="Stale active", platform="zoom", meeting_link="")
            record_dir = store.record_dir(stale["record_id"])
            status_path = record_dir / "screencapture-status.json"
            status_path.write_text(
                json.dumps({"status": "failed", "message": "stream_stopped: display unavailable"}),
                encoding="utf-8",
            )
            stale.update(
                {
                    "status": "recording",
                    "media": {
                        "audio_capture_profile": "screencapturekit_audio_v1",
                        "screencapture_status_path": str(status_path.relative_to(root)),
                    },
                }
            )
            store.save_record(stale)

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_screencapturekit_helper",
                return_value=Path("/tmp/meeting-screencapture-helper"),
            ), patch.object(runtime, "_terminate_persisted_recorder_process"), patch(
                "bpmis_jira_tool.meeting_recorder._ScreenCaptureKitAudioRecorder.start",
                return_value={"status": "ok", "latency_seconds": 0.1, "bytes": 4096, "pid": 456},
            ):
                new_record = runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="New active",
                    platform="zoom",
                    meeting_link="https://zoom.us/j/new",
                    recording_mode="audio_only",
                )
                stale_status = store.get_record(stale["record_id"])["status"]

            self.assertEqual(stale_status, "failed")
            self.assertEqual(new_record["status"], "recording")

class FakeTextClient:
    def __init__(self):
        self.calls = []

    def create_answer(self, *, system_prompt, user_prompt):
        self.calls.append({"system_prompt": system_prompt, "user_prompt": user_prompt})
        return "## Summary\nCodex minutes"


class FakeStreamingResponse:
    def __init__(self, *, status_code=206, headers=None, chunks=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._chunks = chunks or []
        self.closed = False

    def iter_content(self, chunk_size=1):
        del chunk_size
        yield from self._chunks

    def close(self):
        self.closed = True


class MeetingProcessingServiceTests(unittest.TestCase):
    def test_generate_minutes_uses_audio_transcript_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            text_client = FakeTextClient()
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(),
                text_client=text_client,
            )

            minutes = service._generate_minutes(
                record={"title": "Review", "platform": "zoom", "scheduled_start": "", "attendees": []},
                transcript_text="Alice: approve the launch.",
            )

        self.assertIn("Codex minutes", minutes)
        self.assertEqual(len(text_client.calls), 1)
        self.assertIn("Alice: approve the launch.", text_client.calls[0]["user_prompt"])
        self.assertIn("Hi all,", text_client.calls[0]["user_prompt"])
        self.assertIn("Below are the key alignments and follow-up items from today's meeting.", text_client.calls[0]["user_prompt"])
        self.assertIn("1. Topic Name", text_client.calls[0]["user_prompt"])
        self.assertIn("- Confirmed alignment, decision, or agreed direction.", text_client.calls[0]["user_prompt"])
        self.assertIn("- Future direction, concern, risk, dependency, or constraint.", text_client.calls[0]["user_prompt"])
        self.assertIn("- Owner or team to do the next step; use owner TBD if unclear.", text_client.calls[0]["user_prompt"])
        self.assertIn("Regards", text_client.calls[0]["user_prompt"])
        self.assertIn("Extract up to 5 most important topics", text_client.calls[0]["user_prompt"])
        self.assertIn("confirmed alignment or decision; named owner or next step; blocker, dependency, or risk", text_client.calls[0]["user_prompt"])
        self.assertIn("Omit lower-value background, chronology, side discussions", text_client.calls[0]["user_prompt"])
        self.assertIn("Under each topic, write 2-3 bullets maximum.", text_client.calls[0]["user_prompt"])
        self.assertIn("Do not use nested bullets, long explanations, filler", text_client.calls[0]["user_prompt"])
        self.assertIn("Do not invent owners, decisions, dates, deadlines, slide links", text_client.calls[0]["system_prompt"])
        self.assertNotIn("Scheduled start: \n", text_client.calls[0]["user_prompt"])
        self.assertNotIn("Attendees from calendar: []", text_client.calls[0]["user_prompt"])
        self.assertNotIn("Transcript quality: {}", text_client.calls[0]["user_prompt"])
        self.assertNotIn("Screen Evidence", text_client.calls[0]["user_prompt"])
        self.assertNotIn("keyframe", text_client.calls[0]["user_prompt"])
        self.assertNotIn("screen evidence", text_client.calls[0]["system_prompt"].lower())

    def test_meeting_minutes_prompt_metadata_includes_schedule_and_attendees(self):
        metadata = _meeting_minutes_prompt_metadata(
            record={
                "title": "Launch Readiness",
                "platform": "Google Meet",
                "scheduled_start": "2026-05-24T09:00:00+08:00",
                "attendees": [{"email": "alice@npt.sg", "name": "Alice"}],
            },
            transcript_quality={"status": "ok"},
            sender_name="Xiaodong",
        )

        self.assertIn("Scheduled start: 2026-05-24T09:00:00+08:00", metadata)
        self.assertIn('"email": "alice@npt.sg"', metadata)

    def test_meeting_minutes_markdown_to_html_renders_nested_bullets_and_escapes_text(self):
        html = _meeting_minutes_markdown_to_html(
            "## Key Discussion Topics\n"
            "- **Collection <Ownership>**\n"
            "  - [Follow up] Check `owner` & confirm.\n"
            "\n"
            "Plain paragraph after list.",
            portal_url="https://portal.example.test/meeting?x=1&y=2",
        )

        self.assertIn("<h3>Key Discussion Topics</h3>", html)
        self.assertIn("<strong>Collection &lt;Ownership&gt;</strong>", html)
        self.assertIn("[Follow up] Check <code>owner</code> &amp; confirm.", html)
        self.assertIn("<p>Plain paragraph after list.</p>", html)
        self.assertIn('href="https://portal.example.test/meeting?x=1&amp;y=2"', html)

    def test_process_audio_only_recording_transcribes_recorded_audio_directly(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Face to face",
                platform="unknown",
                meeting_link="",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record["status"] = "recorded"
            record["media"] = {
                "recording_mode": "audio_only",
                "audio_path": str(audio_path.relative_to(root)),
                "audio_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.wav",
            }
            store.save_record(record)
            text_client = FakeTextClient()
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=text_client,
            )

            with patch.object(service, "_transcribe_audio", return_value={"text": "Alice approved.", "chunks": [], "segments": [], "quality": {}}) as transcribe:
                processed = service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(processed["status"], "completed")
        self.assertEqual(transcribe.call_args.args[0], audio_path.resolve())
        self.assertEqual(processed["visual_evidence"], [])
        self.assertEqual(processed["transcript"]["text"], "Alice approved.")
        self.assertEqual(len(text_client.calls), 1)
        self.assertEqual(processed["minutes"]["generation_mode"], "direct")
        self.assertEqual(processed["minutes"]["estimated_transcript_tokens"], 4)
        self.assertEqual(processed["minutes"]["transcript_hash"], _meeting_transcript_hash("Alice approved."))
        self.assertEqual(processed["minutes"]["chunk_count"], 1)
        self.assertEqual(processed["minutes"]["cache_status"], "miss")

    def test_recover_stale_processing_record_rebuilds_from_whisper_segments(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Recovered Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record_dir = store.record_dir(record["record_id"])
            audio_path = record_dir / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record["status"] = "processing"
            record["media"] = {"recording_mode": "audio_only", "audio_path": str(audio_path.relative_to(root))}
            store.save_record(record)
            (record_dir / "whisper-segment-0000.txt").write_text("Alice opened the meeting.", encoding="utf-8")
            (record_dir / "whisper-segment-0000.srt").write_text(
                "1\n00:00:01,000 --> 00:00:03,000\nAlice opened the meeting.\n",
                encoding="utf-8",
            )
            (record_dir / "whisper-segment-0001.txt").write_text("Bob confirmed the launch.", encoding="utf-8")
            (record_dir / "whisper-segment-0001.srt").write_text(
                "1\n00:00:02,000 --> 00:00:05,000\nBob confirmed the launch.\n",
                encoding="utf-8",
            )
            text_client = FakeTextClient()
            service = MeetingProcessingService(store=store, config=MeetingRecorderConfig(), text_client=text_client)

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=120.0), patch.object(
                service,
                "_record_has_active_whisper_process",
                return_value=False,
            ), patch.object(service, "_transcribe_audio") as transcribe:
                result = service.recover_stale_processing_record(record_id=record["record_id"], owner_email="owner@npt.sg")
            self.assertEqual(result["status"], "recovered")
            recovered = result["record"]
            self.assertEqual(recovered["status"], "completed")
            transcribe.assert_not_called()
            self.assertEqual(recovered["transcript"]["text"], "Alice opened the meeting.\nBob confirmed the launch.")
            self.assertEqual(recovered["transcript"]["chunks"][1]["start_seconds"], 62.0)
            self.assertEqual(recovered["processing_recovery"]["status"], "recovered_from_segments")
            self.assertEqual(recovered["processing_recovery"]["segment_count"], 2)
            self.assertEqual(recovered["minutes"]["generation_mode"], "direct")
            self.assertTrue((store.record_dir(recovered["record_id"]) / "transcript.txt").exists())
            self.assertTrue((store.record_dir(recovered["record_id"]) / "minutes.md").exists())
            self.assertEqual(len(text_client.calls), 1)

    def test_recover_stale_processing_record_marks_incomplete_segments_retryable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Incomplete Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record_dir = store.record_dir(record["record_id"])
            audio_path = record_dir / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record["status"] = "processing"
            record["media"] = {"recording_mode": "audio_only", "audio_path": str(audio_path.relative_to(root))}
            store.save_record(record)
            (record_dir / "whisper-segment-0000.txt").write_text("Only first segment finished.", encoding="utf-8")
            service = MeetingProcessingService(store=store, config=MeetingRecorderConfig(), text_client=FakeTextClient())

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=120.0), patch.object(
                service,
                "_record_has_active_whisper_process",
                return_value=False,
            ):
                result = service.recover_stale_processing_record(record_id=record["record_id"], owner_email="owner@npt.sg")
            self.assertEqual(result["status"], "failed")
            failed = store.get_record(record["record_id"])
            self.assertEqual(failed["status"], "failed")
            self.assertTrue(failed["stalled_retryable"])
            self.assertIn("stalled before transcription completed", failed["error"])
            self.assertEqual(failed["processing_recovery"]["status"], "failed_incomplete_segments")

    def test_long_transcript_generates_chunk_summaries_before_final_minutes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Long Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record["status"] = "recorded"
            record["media"] = {
                "recording_mode": "audio_only",
                "audio_path": str(audio_path.relative_to(root)),
            }
            store.save_record(record)
            text_client = FakeTextClient()
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=text_client,
            )
            long_transcript = "\n".join(
                f"{index:03d} Alice and Bob discussed launch readiness, owner follow-ups, and risk checks."
                for index in range(360)
            )

            with patch.object(
                service,
                "_transcribe_audio",
                return_value={"text": long_transcript, "chunks": [], "segments": [], "quality": {}},
            ), patch.object(
                service,
                "_transcribe_owner_speech_candidates",
                return_value={"status": "skipped"},
            ):
                processed = service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(processed["status"], "completed")
        self.assertEqual(processed["minutes"]["generation_mode"], "chunked_summary")
        self.assertGreater(processed["minutes"]["chunk_count"], 1)
        self.assertEqual(len(text_client.calls), processed["minutes"]["chunk_count"] + 1)
        self.assertEqual(processed["minutes"]["transcript_hash"], _meeting_transcript_hash(long_transcript))
        self.assertGreater(processed["minutes"]["estimated_transcript_tokens"], 3000)
        self.assertTrue(all(long_transcript not in call["user_prompt"] for call in text_client.calls))
        self.assertIn("Preserve only meaningful topics, alignments, explicit decisions, owners, risks, dependencies, and next steps.", text_client.calls[0]["user_prompt"])
        self.assertIn("# Chunk Summaries", text_client.calls[-1]["user_prompt"])
        self.assertNotIn("# Transcript\n", text_client.calls[-1]["user_prompt"])
        self.assertIn("Hi all,", text_client.calls[-1]["user_prompt"])
        self.assertIn("1. Topic Name", text_client.calls[-1]["user_prompt"])
        self.assertIn("Extract up to 5 most important topics", text_client.calls[-1]["user_prompt"])
        self.assertIn("confirmed alignment or decision; named owner or next step; blocker, dependency, or risk", text_client.calls[-1]["user_prompt"])
        self.assertIn("Omit lower-value background, chronology, side discussions", text_client.calls[-1]["user_prompt"])
        self.assertIn("Under each topic, write 2-3 bullets maximum.", text_client.calls[-1]["user_prompt"])
        self.assertIn("Do not use nested bullets, long explanations, filler", text_client.calls[-1]["user_prompt"])

    def test_process_reuses_cached_minutes_for_same_transcript_hash(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Cached Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            transcript_text = "Alice approved the cached launch."
            transcript_hash = _meeting_transcript_hash(transcript_text)
            record["status"] = "completed"
            record["media"] = {
                "recording_mode": "audio_only",
                "audio_path": str(audio_path.relative_to(root)),
            }
            record["minutes"] = {
                "status": "completed",
                "markdown": "Hi all,\n\n1. Cached\n- Existing alignment.\n\nRegards\nOwner",
                "prompt_version": "v4_alignment_next_steps_email",
                "generation_version": "v3_alignment_email_max5",
                "transcript_hash": transcript_hash,
                "estimated_transcript_tokens": 9,
                "generation_mode": "direct",
                "chunk_count": 1,
                "cache_status": "miss",
            }
            store.save_record(record)
            text_client = FakeTextClient()
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=text_client,
            )

            with patch.object(
                service,
                "_transcribe_audio",
                return_value={"text": transcript_text, "chunks": [], "segments": [], "quality": {}},
            ), patch.object(
                service,
                "_transcribe_owner_speech_candidates",
                return_value={"status": "skipped"},
            ):
                processed = service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(len(text_client.calls), 0)
        self.assertEqual(processed["minutes"]["markdown"], "Hi all,\n\n1. Cached\n- Existing alignment.\n\nRegards\nOwner")
        self.assertEqual(processed["minutes"]["cache_status"], "hit")
        self.assertEqual(processed["minutes"]["transcript_hash"], transcript_hash)

    def test_process_regenerates_minutes_when_transcript_hash_changes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Changed Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record["status"] = "completed"
            record["media"] = {
                "recording_mode": "audio_only",
                "audio_path": str(audio_path.relative_to(root)),
            }
            record["minutes"] = {
                "status": "completed",
                "markdown": "Old minutes",
                "prompt_version": "v4_alignment_next_steps_email",
                "generation_version": "v3_alignment_email_max5",
                "transcript_hash": _meeting_transcript_hash("old transcript"),
                "estimated_transcript_tokens": 4,
                "generation_mode": "direct",
                "chunk_count": 1,
            }
            store.save_record(record)
            text_client = FakeTextClient()
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=text_client,
            )
            new_transcript = "New transcript requiring regeneration."

            with patch.object(
                service,
                "_transcribe_audio",
                return_value={"text": new_transcript, "chunks": [], "segments": [], "quality": {}},
            ), patch.object(
                service,
                "_transcribe_owner_speech_candidates",
                return_value={"status": "skipped"},
            ):
                processed = service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(len(text_client.calls), 1)
        self.assertEqual(processed["minutes"]["cache_status"], "miss")
        self.assertEqual(processed["minutes"]["transcript_hash"], _meeting_transcript_hash(new_transcript))

    def test_process_regenerates_minutes_when_generation_version_changes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Versioned Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            transcript_text = "Alice approved the versioned launch."
            record["status"] = "completed"
            record["media"] = {
                "recording_mode": "audio_only",
                "audio_path": str(audio_path.relative_to(root)),
            }
            record["minutes"] = {
                "status": "completed",
                "markdown": "Old minutes",
                "prompt_version": "v4_alignment_next_steps_email",
                "generation_version": "old_generation_version",
                "transcript_hash": _meeting_transcript_hash(transcript_text),
                "estimated_transcript_tokens": 9,
                "generation_mode": "direct",
                "chunk_count": 1,
            }
            store.save_record(record)
            text_client = FakeTextClient()
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=text_client,
            )

            with patch.object(
                service,
                "_transcribe_audio",
                return_value={"text": transcript_text, "chunks": [], "segments": [], "quality": {}},
            ), patch.object(
                service,
                "_transcribe_owner_speech_candidates",
                return_value={"status": "skipped"},
            ):
                processed = service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(len(text_client.calls), 1)
        self.assertEqual(processed["minutes"]["cache_status"], "miss")
        self.assertEqual(processed["minutes"]["generation_version"], "v3_alignment_email_max5")

    def test_process_regenerates_minutes_when_prompt_version_changes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Prompt Version Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            transcript_text = "Alice approved the prompt version launch."
            record["status"] = "completed"
            record["media"] = {
                "recording_mode": "audio_only",
                "audio_path": str(audio_path.relative_to(root)),
            }
            record["minutes"] = {
                "status": "completed",
                "markdown": "Old minutes",
                "prompt_version": "old_prompt_version",
                "generation_version": "v3_alignment_email_max5",
                "transcript_hash": _meeting_transcript_hash(transcript_text),
                "estimated_transcript_tokens": 10,
                "generation_mode": "direct",
                "chunk_count": 1,
            }
            store.save_record(record)
            text_client = FakeTextClient()
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=text_client,
            )

            with patch.object(
                service,
                "_transcribe_audio",
                return_value={"text": transcript_text, "chunks": [], "segments": [], "quality": {}},
            ), patch.object(
                service,
                "_transcribe_owner_speech_candidates",
                return_value={"status": "skipped"},
            ):
                processed = service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(len(text_client.calls), 1)
        self.assertEqual(processed["minutes"]["cache_status"], "miss")
        self.assertEqual(processed["minutes"]["prompt_version"], "v4_alignment_next_steps_email")

    def test_process_screencapture_recording_marks_local_microphone_owner_speech_candidates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Zoom",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record_dir = store.record_dir(record["record_id"])
            mixed_path = record_dir / "meeting.wav"
            microphone_path = record_dir / "screencapture-microphone.caf"
            mixed_path.write_bytes(b"audio")
            microphone_path.write_bytes(b"microphone audio" * 8)
            record["status"] = "recorded"
            record["media"] = {
                "audio_capture_profile": "screencapturekit_audio_v1",
                "screencapture_capture_source": "screencapturekit_audio",
                "audio_path": str(mixed_path.relative_to(root)),
                "audio_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.wav",
                "microphone_audio_path": str(microphone_path.relative_to(root)),
            }
            store.save_record(record)
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
            )

            def transcribe(audio_path, **kwargs):
                if Path(audio_path).name == "screencapture-microphone.caf":
                    self.assertEqual(kwargs["output_prefix"], "owner-microphone-transcript")
                    return {
                        "text": "I will confirm the release date.",
                        "chunks": [{"start_seconds": 3.0, "end_seconds": 5.0, "text": "I will confirm the release date."}],
                        "segments": [],
                        "quality": {},
                    }
                return {
                    "text": "Team discussed release date.",
                    "chunks": [{"start_seconds": 0.0, "end_seconds": 8.0, "text": "Team discussed release date."}],
                    "segments": [],
                    "quality": {},
                }

            with patch.object(service, "_transcribe_audio", side_effect=transcribe) as transcribe_audio, patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=10.0,
            ):
                processed = service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")
                owner_transcript_exists = (store.record_dir(record["record_id"]) / "owner-microphone-transcript.txt").exists()

        self.assertEqual(transcribe_audio.call_count, 2)
        candidates = processed["transcript"]["owner_speech_candidates"]
        self.assertEqual(candidates[0]["speaker"], "me_candidate")
        self.assertEqual(candidates[0]["speaker_source"], "local_microphone")
        self.assertEqual(candidates[0]["speaker_confidence"], "candidate")
        self.assertTrue(owner_transcript_exists)

    def test_send_minutes_email_attaches_transcript_text_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record["status"] = "completed"
            record["minutes"] = {
                "status": "completed",
                "markdown": "## Key Discussion Topics\n- **Launch**\n  - Approved.",
            }
            record["transcript"] = {"status": "completed", "text": "Alice approved the launch."}
            transcript_path = store.record_dir(record["record_id"]) / "transcript.txt"
            transcript_path.write_text("Alice approved the launch.", encoding="utf-8")
            store.save_record(record)
            credential_store = Mock()
            credential_store.load.return_value = {"token": "x"}
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
                credential_store=credential_store,
                portal_base_url="https://portal.example.test",
            )

            with patch("bpmis_jira_tool.meeting_recorder.credentials_from_payload", return_value=object()), patch(
                "bpmis_jira_tool.meeting_recorder.send_gmail_message",
                return_value={"id": "msg-1"},
            ) as send_message:
                email = service.send_minutes_email(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(email["status"], "sent")
        self.assertTrue(email["transcript_attached"])
        self.assertIn("## Key Discussion Topics", send_message.call_args.kwargs["text_body"])
        self.assertIn("<h3>Key Discussion Topics</h3>", send_message.call_args.kwargs["html_body"])
        self.assertIn("<strong>Launch</strong>", send_message.call_args.kwargs["html_body"])
        self.assertIn("Full transcript and recording archive", send_message.call_args.kwargs["html_body"])
        attachment = send_message.call_args.kwargs["attachments"][0]
        self.assertEqual(attachment["filename"], "meeting-transcript.txt")
        self.assertEqual(attachment["mime_type"], "text/plain")
        self.assertEqual(attachment["content"], b"Alice approved the launch.")

    def test_processing_service_failure_and_email_boundary_states(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Processing boundaries",
                platform="zoom",
                meeting_link="https://zoom.us/j/boundaries",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record["status"] = "recorded"
            record["media"] = {"audio_path": str(audio_path.relative_to(root))}
            store.save_record(record)
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
            )

            scheduled = store.create_record(
                owner_email="owner@npt.sg",
                title="Not stopped",
                platform="zoom",
                meeting_link="https://zoom.us/j/scheduled",
            )
            with self.assertRaisesRegex(ToolError, "Stop the recording"):
                service.process_recording(record_id=scheduled["record_id"], owner_email="owner@npt.sg")

            with patch.object(service, "_transcribe_audio", side_effect=ToolError("transcribe failed")):
                with self.assertRaisesRegex(ToolError, "transcribe failed"):
                    service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")
            failed_record = store.get_record(record["record_id"])
            failed_status = failed_record["status"]
            failed_error = failed_record["error"]

            not_applicable = service.recover_stale_processing_record(
                record_id=failed_record["record_id"],
                owner_email="owner@npt.sg",
            )
            failed_record["status"] = "processing"
            store.save_record(failed_record)
            with patch.object(service, "_record_has_active_whisper_process", return_value=True):
                active = service.recover_stale_processing_record(record_id=failed_record["record_id"], owner_email="owner@npt.sg")

            missing_audio = dict(record)
            missing_audio["media"] = {}
            with self.assertRaisesRegex(ToolError, "audio is missing"):
                service._recorded_audio_path(missing_audio)
            missing_file = dict(record)
            missing_file["media"] = {"audio_path": "records/missing/meeting.wav"}
            with self.assertRaisesRegex(ToolError, "audio file was not found"):
                service._recorded_audio_path(missing_file)

            completed = store.create_record(
                owner_email="owner@npt.sg",
                title="Email boundaries",
                platform="zoom",
                meeting_link="https://zoom.us/j/email",
            )
            completed["status"] = "completed"
            completed["minutes"] = {"status": "completed", "markdown": "Minutes"}
            completed["transcript"] = {"status": "completed", "text": "Transcript fallback."}
            store.save_record(completed)
            with self.assertRaisesRegex(ConfigError, "credential store"):
                service.send_minutes_email(record_id=completed["record_id"], owner_email="owner@npt.sg")
            empty_minutes = dict(completed)
            empty_minutes["record_id"] = "empty-minutes"
            empty_minutes["minutes"] = {"status": "pending", "markdown": ""}
            store.save_record(empty_minutes)
            credential_store = Mock()
            credential_store.load.return_value = {"token": "x"}
            email_service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
                credential_store=credential_store,
            )
            with self.assertRaisesRegex(ToolError, "minutes are not available"):
                email_service.send_minutes_email(record_id="empty-minutes", owner_email="owner@npt.sg")
            fallback_attachment = email_service._transcript_email_attachments(
                record_id=completed["record_id"],
                record=completed,
            )
            no_attachment = email_service._transcript_email_attachments(
                record_id=completed["record_id"],
                record={**completed, "transcript": {"text": ""}},
            )

        self.assertEqual(failed_status, "failed")
        self.assertEqual(failed_error, "transcribe failed")
        self.assertEqual(not_applicable["status"], "not_applicable")
        self.assertEqual(active["status"], "active")
        self.assertEqual(fallback_attachment[0]["content"], b"Transcript fallback.")
        self.assertEqual(no_attachment, [])

    def test_stale_processing_recovery_and_whisper_process_detection_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Recover stale",
                platform="zoom",
                meeting_link="https://zoom.us/j/recover",
            )
            record_dir = store.record_dir(record["record_id"])
            audio_path = record_dir / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record["status"] = "processing"
            record["media"] = {"audio_path": str(audio_path.relative_to(root))}
            store.save_record(record)
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
            )

            missing_audio = service._recover_transcript_from_segments({"record_id": "missing", "media": {}})
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=0):
                zero_duration = service._recover_transcript_from_segments(record)

            empty_segment = record_dir / "whisper-segment-0000.txt"
            empty_segment.write_text("", encoding="utf-8")
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=1):
                empty_recovery = service._recover_transcript_from_segments(record)

            class TruthyEmpty(str):
                def __new__(cls):
                    return super().__new__(cls, "")

                def __bool__(self):
                    return True

            class EmptySegmentPayload(str):
                def strip(self):
                    return TruthyEmpty()

            with patch.object(service, "_recorded_audio_path", return_value=audio_path), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=1,
            ), patch.object(Path, "read_text", return_value=EmptySegmentPayload("empty")):
                empty_join_recovery = service._recover_transcript_from_segments(record)

            empty_segment.write_text("Recovered discussion.", encoding="utf-8")
            (record_dir / "whisper-segment-0000.srt").write_text(
                "1\n00:00:00,000 --> 00:00:01,000\nRecovered discussion.\n",
                encoding="utf-8",
            )
            completed_record = {**record, "status": "completed"}
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=1), patch.object(
                service,
                "_record_has_active_whisper_process",
                return_value=False,
            ), patch.object(
                service,
                "_complete_record_from_transcript",
                return_value=completed_record,
            ) as complete_record:
                recovered = service.recover_stale_processing_record(record_id=record["record_id"], owner_email="owner@npt.sg")

            record_dir_text = str(record_dir)
            with patch("bpmis_jira_tool.meeting_recorder.subprocess.run", side_effect=OSError("ps missing")):
                ps_oserror_active = service._record_has_active_whisper_process(record["record_id"])
            with patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.run",
                return_value=subprocess.CompletedProcess(["ps"], 1, stdout="", stderr="failed"),
            ):
                ps_returncode_active = service._record_has_active_whisper_process(record["record_id"])
            with patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.run",
                return_value=subprocess.CompletedProcess(["ps"], 0, stdout=f"whisper-cli -of {record_dir_text}/whisper-segment", stderr=""),
            ):
                ps_match_active = service._record_has_active_whisper_process(record["record_id"])
            with patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.run",
                return_value=subprocess.CompletedProcess(["ps"], 0, stdout="python app.py", stderr=""),
            ):
                ps_inactive = service._record_has_active_whisper_process(record["record_id"])

            email_record = store.create_record(
                owner_email="owner@npt.sg",
                title="Missing recipient",
                platform="zoom",
                meeting_link="https://zoom.us/j/email",
            )
            email_record["status"] = "completed"
            email_record["minutes"] = {"markdown": "Minutes"}
            store.save_record(email_record)
            credential_store = Mock()
            credential_store.load.return_value = {"token": "x"}
            email_service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
                credential_store=credential_store,
            )
            with patch("bpmis_jira_tool.meeting_recorder._assert_record_owner"), patch(
                "bpmis_jira_tool.meeting_recorder.credentials_from_payload",
                return_value=object(),
            ), self.assertRaisesRegex(ToolError, "recipient is missing"):
                email_service.send_minutes_email(record_id=email_record["record_id"], owner_email="", recipient="")

        self.assertIsNone(missing_audio)
        self.assertIsNone(zero_duration)
        self.assertIsNone(empty_recovery)
        self.assertIsNone(empty_join_recovery)
        self.assertEqual(recovered["status"], "recovered")
        complete_record.assert_called_once()
        transcript = complete_record.call_args.kwargs["transcript"]
        self.assertEqual(transcript["text"], "Recovered discussion.")
        self.assertEqual(transcript["chunks"][0]["start_seconds"], 0.0)
        self.assertTrue(ps_oserror_active)
        self.assertTrue(ps_returncode_active)
        self.assertTrue(ps_match_active)
        self.assertFalse(ps_inactive)

    def test_language_retry_keeps_auto_candidate_when_retries_do_not_improve_quality(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Retry language",
                platform="zoom",
                meeting_link="https://zoom.us/j/retry",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
            )
            transcript_calls = []

            def fake_transcribe_once(**kwargs):
                transcript_calls.append(kwargs["language"])
                return {
                    "text": f"{kwargs['language']} repeated repeated repeated",
                    "chunks": [{"start_seconds": 0, "end_seconds": 1, "text": "same"} for _ in range(5)],
                    "language": "fr",
                }

            with patch.object(service, "_transcribe_audio_once", side_effect=fake_transcribe_once), patch.object(
                service,
                "_transcript_quality",
                return_value=(
                    [{"start_seconds": 0, "end_seconds": 1, "text": "same"}],
                    {"language": "fr", "repetitive_chunk_count": 5, "warnings": []},
                ),
            ):
                transcript = service._transcribe_audio_with_language_selection(
                    audio_path=audio_path,
                    output_base=store.record_dir(record["record_id"]) / "whisper-transcript",
                    whisper_bin="whisper-cli",
                    model_path=root / "model.bin",
                    configured_language="auto",
                    offset_seconds=0,
                    whisper_threads=1,
                    started_at=1.0,
                    duration_seconds=2.0,
                )

        self.assertEqual(transcript_calls, ["auto", "zh", "en"])
        self.assertEqual(transcript["language_retry_count"], 2)
        self.assertEqual(transcript["quality"]["retry_language"], "zh,en")
        self.assertEqual(transcript["quality"]["original_language"], "fr")
        self.assertIn("did not improve", transcript["quality"]["warnings"][-1])

    def test_transcription_configuration_and_owner_speech_boundary_states(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Transcription boundaries",
                platform="zoom",
                meeting_link="https://zoom.us/j/transcribe",
            )
            record_dir = store.record_dir(record["record_id"])
            audio_path = record_dir / "meeting.wav"
            audio_path.write_bytes(b"audio")
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
            )

            with patch("bpmis_jira_tool.meeting_recorder._resolve_whisper_cpp_bin", return_value=""):
                with self.assertRaisesRegex(ConfigError, "whisper.cpp is required"):
                    service._transcribe_audio(audio_path)
            missing_model_service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(whisper_model=str(root / "missing-model.bin")),
                text_client=FakeTextClient(),
            )
            with patch("bpmis_jira_tool.meeting_recorder._resolve_whisper_cpp_bin", return_value="whisper"):
                with self.assertRaisesRegex(ConfigError, "model was not found"):
                    missing_model_service._transcribe_audio(audio_path)

            english_model = root / "ggml-medium.en.bin"
            english_model.write_bytes(b"model")
            english_service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(whisper_model=str(english_model)),
                text_client=FakeTextClient(),
            )
            with patch("bpmis_jira_tool.meeting_recorder._resolve_whisper_cpp_bin", return_value="whisper"):
                with self.assertRaisesRegex(ConfigError, "multilingual"):
                    english_service._transcribe_audio(audio_path)

            output_base = record_dir / "whisper-fallback"
            with patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                return_value=Mock(stdout="stdout transcript", stderr="detected language: en"),
            ):
                transcript = service._transcribe_audio_once(
                    audio_path=audio_path,
                    output_base=output_base,
                    whisper_bin="whisper",
                    model_path=english_model,
                    language="en",
                    offset_seconds=0,
                    whisper_threads=1,
                )
            with patch("bpmis_jira_tool.meeting_recorder._run_command", return_value=Mock(stdout="", stderr="")):
                with self.assertRaisesRegex(ToolError, "produced no text"):
                    service._transcribe_audio_once(
                        audio_path=audio_path,
                        output_base=record_dir / "whisper-empty",
                        whisper_bin="whisper",
                        model_path=english_model,
                        language="en",
                        offset_seconds=0,
                        whisper_threads=1,
                    )

            no_microphone = service._transcribe_owner_speech_candidates(
                {"media": {"audio_capture_profile": "screencapturekit_audio_v1"}},
            )
            missing_microphone = service._transcribe_owner_speech_candidates(
                {"media": {"audio_capture_profile": "screencapturekit_audio_v1", "microphone_audio_path": "missing.caf"}},
            )
            microphone_path = record_dir / "microphone.caf"
            microphone_path.write_bytes(b"0" * 100)
            microphone_record = {
                "media": {
                    "audio_capture_profile": "screencapturekit_audio_v1",
                    "microphone_audio_path": str(microphone_path.relative_to(root)),
                }
            }
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", side_effect=OSError("duration failed")):
                no_duration = service._transcribe_owner_speech_candidates(microphone_record)
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=0.0):
                zero_duration = service._transcribe_owner_speech_candidates(microphone_record)
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=3.0), patch.object(
                service,
                "_transcribe_audio",
                side_effect=ToolError("owner transcript failed"),
            ):
                failed_owner = service._transcribe_owner_speech_candidates(microphone_record)
            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=3.0), patch.object(
                service,
                "_transcribe_audio",
                return_value={"text": "[no audio]", "chunks": [{"text": "[no audio]"}], "segments": [], "quality": {}},
            ):
                empty_owner = service._transcribe_owner_speech_candidates(microphone_record)

        self.assertEqual(transcript["text"], "stdout transcript")
        self.assertEqual(transcript["chunks"], [{"start_seconds": 0, "end_seconds": 0, "text": "stdout transcript"}])
        self.assertEqual(no_microphone["status"], "skipped")
        self.assertIn("No local microphone track", no_microphone["warning"])
        self.assertEqual(missing_microphone["status"], "skipped")
        self.assertEqual(no_duration["status"], "skipped")
        self.assertEqual(zero_duration["status"], "skipped")
        self.assertEqual(failed_owner["status"], "failed")
        self.assertIn("owner transcript failed", failed_owner["warning"])
        self.assertEqual(empty_owner["status"], "empty")

    def test_segmented_transcription_configuration_and_empty_output_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Segmented transcription boundaries",
                platform="zoom",
                meeting_link="https://zoom.us/j/segments",
            )
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            model_path = root / "ggml-medium.bin"
            model_path.write_bytes(b"model")
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(transcript_segment_workers=1),
                text_client=FakeTextClient(),
            )

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value=""):
                with self.assertRaisesRegex(ConfigError, "ffmpeg is required"):
                    service._transcribe_audio_by_segments(
                        audio_path=audio_path,
                        duration_seconds=1.0,
                        whisper_bin="whisper",
                        model_path=model_path,
                    )

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                return_value=Mock(stdout="", stderr=""),
            ) as run_command, patch.object(
                service,
                "_transcribe_audio_with_language_selection",
                return_value={
                    "text": "",
                    "chunks": [{"start_seconds": 0.0, "end_seconds": 1.0, "text": "[no audio]"}],
                    "language": "en",
                    "language_retry_count": 0,
                },
            ), patch(
                "bpmis_jira_tool.meeting_recorder._audio_volume_metrics",
                return_value={"low_audio": True, "mean_volume_db": -90.0, "max_volume_db": -80.0},
            ):
                with self.assertRaisesRegex(ToolError, "produced no text"):
                    service._transcribe_audio_by_segments(
                        audio_path=audio_path,
                        duration_seconds=1.0,
                        whisper_bin="whisper",
                        model_path=model_path,
                    )

        self.assertTrue(run_command.called)

    def test_parse_srt_transcript_chunks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            srt_path = Path(temp_dir) / "whisper-transcript.srt"
            srt_path.write_text(
                "1\n00:00:01,500 --> 00:00:03,000\n你好，今天开始。\n\n"
                "2\n00:00:05,000 --> 00:00:07,250\nWe approve launch.\n",
                encoding="utf-8",
            )

            chunks = _parse_srt_transcript(srt_path)

        self.assertEqual(
            chunks,
            [
                {"start_seconds": 1.5, "end_seconds": 3.0, "text": "你好，今天开始。"},
                {"start_seconds": 5.0, "end_seconds": 7.25, "text": "We approve launch."},
            ],
        )

    def test_transcribe_audio_uses_whisper_cpp(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            model_path = root / "ggml-medium.bin"
            model_path.write_text("model", encoding="utf-8")
            record_dir = root / "records" / "meeting-1"
            record_dir.mkdir(parents=True)
            audio_path = record_dir / "audio.wav"
            audio_path.write_bytes(b"audio")
            (record_dir / "whisper-transcript.txt").write_text("hello 中文", encoding="utf-8")
            (record_dir / "whisper-transcript.srt").write_text(
                "1\n00:00:00,000 --> 00:00:02,000\nhello 中文\n",
                encoding="utf-8",
            )
            service = MeetingProcessingService(
                store=MeetingRecordStore(root),
                config=MeetingRecorderConfig(whisper_cpp_bin="whisper-cli", whisper_model=str(model_path), whisper_threads=4),
                text_client=FakeTextClient(),
            )

            with patch("bpmis_jira_tool.meeting_recorder.shutil.which", return_value="/usr/local/bin/whisper-cli"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command"
            ) as run_command:
                run_command.return_value = Mock(stdout="")
                transcript = service._transcribe_audio(audio_path)

        self.assertEqual(transcript["text"], "hello 中文")
        self.assertEqual(transcript["chunks"], [{"start_seconds": 0.0, "end_seconds": 2.0, "text": "hello 中文"}])
        self.assertIn("quality", transcript)
        self.assertIn("segments", transcript)
        command = next(call.args[0] for call in run_command.call_args_list if "-osrt" in call.args[0])
        self.assertEqual(command[:3], ["nice", "-n", "10"])
        self.assertIn("/usr/local/bin/whisper-cli", command)
        self.assertIn(str(model_path), command)
        self.assertIn("-t", command)
        self.assertEqual(command[command.index("-t") + 1], "4")
        self.assertIn("-osrt", command)
        self.assertIn("-l", command)
        self.assertIn("auto", command)
        self.assertEqual(transcript["quality"]["segment_count"], 1)
        self.assertEqual(transcript["quality"]["whisper_threads"], 4)

    def test_transcribe_audio_selects_english_when_auto_language_is_repetitive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            model_path = root / "ggml-medium.bin"
            model_path.write_text("model", encoding="utf-8")
            record_dir = root / "records" / "meeting-1"
            record_dir.mkdir(parents=True)
            audio_path = record_dir / "audio.wav"
            audio_path.write_bytes(b"audio")
            service = MeetingProcessingService(
                store=MeetingRecordStore(root),
                config=MeetingRecorderConfig(whisper_cpp_bin="whisper-cli", whisper_model=str(model_path)),
                text_client=FakeTextClient(),
            )

            def fake_run(command, *_args, **_kwargs):
                output_base = Path(command[command.index("-of") + 1])
                if output_base.name.endswith("-en"):
                    output_base.with_suffix(".txt").write_text("Project launch was approved.", encoding="utf-8")
                    output_base.with_suffix(".srt").write_text(
                        "1\n00:00:00,000 --> 00:00:04,000\nProject launch was approved.\n",
                        encoding="utf-8",
                    )
                    return Mock(stdout="whisper_full_with_state: auto-detected language: en (p = 0.90)", stderr="")
                output_base.with_suffix(".txt").write_text("There are a lot of them.\n" * 8, encoding="utf-8")
                output_base.with_suffix(".srt").write_text(
                    "".join(
                        f"{index}\n00:00:{index:02d},000 --> 00:00:{index + 1:02d},000\nThere are a lot of them.\n\n"
                        for index in range(1, 9)
                    ),
                    encoding="utf-8",
                )
                return Mock(stdout="whisper_full_with_state: auto-detected language: nn (p = 0.50)", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder.shutil.which", return_value="/usr/local/bin/whisper-cli"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=42,
            ), patch("bpmis_jira_tool.meeting_recorder._audio_volume_metrics", return_value={"low_audio": False}), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=fake_run,
            ) as run_command:
                transcript = service._transcribe_audio(audio_path)

        whisper_calls = [call.args[0] for call in run_command.call_args_list if "-of" in call.args[0]]
        self.assertEqual(len(whisper_calls), 3)
        self.assertEqual(transcript["text"], "Project launch was approved.")
        self.assertEqual(transcript["quality"]["retry_language"], "en")
        self.assertEqual(transcript["quality"]["original_language"], "nn")

    def test_transcribe_audio_selects_chinese_when_auto_language_is_unexpected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            model_path = root / "ggml-medium.bin"
            model_path.write_text("model", encoding="utf-8")
            record_dir = root / "records" / "meeting-1"
            record_dir.mkdir(parents=True)
            audio_path = record_dir / "audio.wav"
            audio_path.write_bytes(b"audio")
            service = MeetingProcessingService(
                store=MeetingRecordStore(root),
                config=MeetingRecorderConfig(whisper_cpp_bin="whisper-cli", whisper_model=str(model_path)),
                text_client=FakeTextClient(),
            )

            def fake_run(command, *_args, **_kwargs):
                output_base = Path(command[command.index("-of") + 1])
                if output_base.name.endswith("-zh"):
                    output_base.with_suffix(".txt").write_text("我们确认今天上线。", encoding="utf-8")
                    output_base.with_suffix(".srt").write_text(
                        "1\n00:00:00,000 --> 00:00:04,000\n我们确认今天上线。\n",
                        encoding="utf-8",
                    )
                    return Mock(stdout="", stderr="")
                if output_base.name.endswith("-en"):
                    output_base.with_suffix(".txt").write_text("There are a lot of them.\n" * 8, encoding="utf-8")
                    output_base.with_suffix(".srt").write_text(
                        "".join(
                            f"{index}\n00:00:{index:02d},000 --> 00:00:{index + 1:02d},000\nThere are a lot of them.\n\n"
                            for index in range(1, 9)
                        ),
                        encoding="utf-8",
                    )
                    return Mock(stdout="whisper_full_with_state: auto-detected language: en (p = 0.40)", stderr="")
                output_base.with_suffix(".txt").write_text("잘 자요 잘 자요 잘 자요", encoding="utf-8")
                output_base.with_suffix(".srt").write_text(
                    "1\n00:00:00,000 --> 00:00:03,000\n잘 자요 잘 자요 잘 자요\n",
                    encoding="utf-8",
                )
                return Mock(stdout="whisper_full_with_state: auto-detected language: ko (p = 0.70)", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder.shutil.which", return_value="/usr/local/bin/whisper-cli"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=42,
            ), patch("bpmis_jira_tool.meeting_recorder._audio_volume_metrics", return_value={"low_audio": False}), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=fake_run,
            ) as run_command:
                transcript = service._transcribe_audio(audio_path)

        whisper_calls = [call.args[0] for call in run_command.call_args_list if "-of" in call.args[0]]
        self.assertEqual(len(whisper_calls), 3)
        self.assertEqual(transcript["text"], "我们确认今天上线。")
        self.assertEqual(transcript["quality"]["retry_language"], "zh")
        self.assertEqual(transcript["quality"]["original_language"], "ko")

    def test_transcript_quality_flags_repetitive_unexpected_language(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            record_dir = root / "records" / "meeting-1"
            record_dir.mkdir(parents=True)
            audio_path = record_dir / "audio.wav"
            audio_path.write_bytes(b"audio")
            service = MeetingProcessingService(
                store=MeetingRecordStore(root),
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
            )
            chunks = [
                {"start_seconds": float(index), "end_seconds": float(index + 1), "text": "There are a lot of them."}
                for index in range(8)
            ]

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=42), patch(
                "bpmis_jira_tool.meeting_recorder._audio_volume_metrics",
                return_value={"low_audio": False},
            ):
                _segments, quality = service._transcript_quality(audio_path=audio_path, chunks=chunks, detected_language="nn")

        self.assertTrue(quality["possible_incomplete"])
        self.assertEqual(quality["repetitive_chunk_count"], 8)
        self.assertIn("unexpected language", " ".join(quality["warnings"]))
        self.assertIn("repeated chunks", " ".join(quality["warnings"]))

    def test_transcript_quality_keeps_startup_silence_out_of_incomplete_warning(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(startup_silence_grace_seconds=300),
                text_client=FakeTextClient(),
            )
            segments = [
                {
                    "index": 0,
                    "start_seconds": 0.0,
                    "end_seconds": 60.0,
                    "language": "en",
                    "quality": "low_audio",
                    "possible_missed_speech": True,
                    "no_audio": True,
                    "startup_silence": True,
                    "chunk_count": 1,
                },
                {
                    "index": 1,
                    "start_seconds": 60.0,
                    "end_seconds": 120.0,
                    "language": "en",
                    "quality": "low_audio",
                    "possible_missed_speech": True,
                    "no_audio": False,
                    "startup_silence": True,
                    "chunk_count": 2,
                },
                {
                    "index": 6,
                    "start_seconds": 360.0,
                    "end_seconds": 420.0,
                    "language": "en",
                    "quality": "ok",
                    "possible_missed_speech": False,
                    "no_audio": False,
                    "startup_silence": False,
                    "chunk_count": 8,
                },
            ]

            quality = service._quality_from_segments(segments, chunks=[{"text": "normal discussion"}])

        self.assertFalse(quality["possible_incomplete"])
        self.assertEqual(quality["low_audio_segment_count"], 2)
        self.assertEqual(quality["no_audio_segment_count"], 1)
        self.assertEqual(quality["startup_silence_segment_count"], 2)
        self.assertEqual(quality["risk_low_audio_segment_count"], 0)
        self.assertEqual(quality["risk_no_audio_segment_count"], 0)
        self.assertEqual(quality["warnings"], [])

    def test_transcript_quality_flags_no_audio_after_startup_grace(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(startup_silence_grace_seconds=300),
                text_client=FakeTextClient(),
            )
            segments = [
                {
                    "index": 6,
                    "start_seconds": 360.0,
                    "end_seconds": 420.0,
                    "language": "en",
                    "quality": "low_audio",
                    "possible_missed_speech": True,
                    "no_audio": True,
                    "startup_silence": False,
                    "chunk_count": 1,
                }
            ]

            quality = service._quality_from_segments(segments, chunks=[{"text": "[no audio]"}])

        self.assertTrue(quality["possible_incomplete"])
        self.assertEqual(quality["no_audio_segment_count"], 1)
        self.assertEqual(quality["risk_no_audio_segment_count"], 1)
        self.assertIn("[no audio]", " ".join(quality["warnings"]))

    def test_transcript_quality_marks_short_all_no_audio_as_incomplete(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(startup_silence_grace_seconds=300),
                text_client=FakeTextClient(),
            )
            segments = [
                {
                    "index": 0,
                    "start_seconds": 0.0,
                    "end_seconds": 30.0,
                    "language": "en",
                    "quality": "low_audio",
                    "possible_missed_speech": True,
                    "no_audio": True,
                    "startup_silence": True,
                    "chunk_count": 1,
                }
            ]

            quality = service._quality_from_segments(segments, chunks=[{"text": "[no audio]"}])

        self.assertTrue(quality["possible_incomplete"])
        self.assertEqual(quality["startup_silence_segment_count"], 1)
        self.assertEqual(quality["risk_no_audio_segment_count"], 1)

    def test_transcript_quality_does_not_count_low_audio_as_no_audio(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(startup_silence_grace_seconds=0),
                text_client=FakeTextClient(),
            )
            segments = [
                {
                    "index": 0,
                    "start_seconds": 0.0,
                    "end_seconds": 60.0,
                    "language": "en",
                    "quality": "low_audio",
                    "possible_missed_speech": True,
                    "no_audio": False,
                    "startup_silence": False,
                    "chunk_count": 3,
                }
            ]

            quality = service._quality_from_segments(segments, chunks=[{"text": "quiet speech"}])

        self.assertEqual(quality["low_audio_segment_count"], 1)
        self.assertEqual(quality["no_audio_segment_count"], 0)
        self.assertTrue(quality["possible_incomplete"])
        self.assertNotIn("[no audio]", " ".join(quality["warnings"]))

    def test_transcribe_audio_splits_long_mixed_language_recordings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            model_path = root / "ggml-medium.bin"
            model_path.write_text("model", encoding="utf-8")
            record_dir = root / "records" / "meeting-1"
            record_dir.mkdir(parents=True)
            audio_path = record_dir / "audio.wav"
            audio_path.write_bytes(b"audio")
            service = MeetingProcessingService(
                store=MeetingRecordStore(root),
                config=MeetingRecorderConfig(
                    whisper_cpp_bin="whisper-cli",
                    whisper_model=str(model_path),
                    transcript_segment_workers=3,
                    whisper_threads=2,
                ),
                text_client=FakeTextClient(),
            )

            def fake_run(command, *_args, **_kwargs):
                if "-of" in command:
                    output_base = Path(command[command.index("-of") + 1])
                    index = int(output_base.name.rsplit("-", 1)[-1])
                    if index == 0:
                        time.sleep(0.05)
                    text = "中文内容" if index == 1 else "English content"
                    output_base.with_suffix(".txt").write_text(text, encoding="utf-8")
                    output_base.with_suffix(".srt").write_text(
                        f"1\n00:00:00,000 --> 00:00:05,000\n{text}\n",
                        encoding="utf-8",
                    )
                    language = "zh" if index == 1 else "en"
                    return Mock(stdout=f"whisper_full_with_state: auto-detected language: {language} (p = 0.90)", stderr="")
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder.shutil.which", return_value="/usr/local/bin/tool"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=125,
            ), patch("bpmis_jira_tool.meeting_recorder._audio_volume_metrics", return_value={"low_audio": False}), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=fake_run,
            ) as run_command:
                transcript = service._transcribe_audio(audio_path)

        whisper_calls = [call.args[0] for call in run_command.call_args_list if "-of" in call.args[0]]
        split_calls = [call.args[0] for call in run_command.call_args_list if "audio-segment-" in " ".join(call.args[0])]
        self.assertTrue(split_calls)
        self.assertTrue(all(command[:3] == ["nice", "-n", "10"] for command in split_calls))
        self.assertEqual(len(whisper_calls), 3)
        self.assertTrue(all(command[:3] == ["nice", "-n", "10"] for command in whisper_calls))
        self.assertTrue(all(command[command.index("-t") + 1] == "2" for command in whisper_calls))
        self.assertTrue(transcript["text"].startswith("English content\n中文内容\nEnglish content"))
        self.assertIn("中文内容", transcript["text"])
        self.assertIn("English content", transcript["text"])
        self.assertEqual(transcript["segments"][1]["language"], "zh")
        self.assertEqual([segment["index"] for segment in transcript["segments"]], [0, 1, 2])
        self.assertEqual([chunk["start_seconds"] for chunk in transcript["chunks"]], [0.0, 60.0, 120.0])
        self.assertEqual(transcript["chunks"][1]["start_seconds"], 60.0)
        self.assertEqual(transcript["quality"]["duration_seconds"], 125.0)
        self.assertEqual(transcript["quality"]["segment_count"], 3)
        self.assertEqual(transcript["quality"]["segment_workers"], 3)
        self.assertEqual(transcript["quality"]["whisper_threads"], 2)
        self.assertEqual(transcript["quality"]["language_retry_count"], 0)
        self.assertIsInstance(transcript["quality"]["transcribe_elapsed_seconds"], float)
        self.assertIsInstance(transcript["quality"]["transcribe_realtime_ratio"], float)

    def test_transcribe_audio_auto_threads_split_cpu_across_workers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(transcript_segment_workers=3, whisper_threads=0),
                text_client=FakeTextClient(),
            )

            with patch("bpmis_jira_tool.meeting_recorder.os.cpu_count", return_value=10):
                self.assertEqual(service._transcript_segment_workers(), 3)
                self.assertEqual(service._whisper_threads(segment_workers=3), 3)

    def test_default_transcript_workers_are_conservative(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
            )

            with patch("bpmis_jira_tool.meeting_recorder.os.cpu_count", return_value=10):
                self.assertEqual(service._transcript_segment_workers(), 2)
                self.assertEqual(service._whisper_threads(segment_workers=2), 5)

    def test_conservative_transcribe_speed_config_uses_four_workers_two_threads(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(transcript_segment_workers=4, whisper_threads=2),
                text_client=FakeTextClient(),
            )

            self.assertEqual(service._transcript_segment_workers(), 4)
            self.assertEqual(service._whisper_threads(segment_workers=4), 2)

    def test_transcribe_audio_caps_segment_workers_to_cpu_count(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(transcript_segment_workers=99, whisper_threads=0),
                text_client=FakeTextClient(),
            )

            with patch("bpmis_jira_tool.meeting_recorder.os.cpu_count", return_value=4):
                self.assertEqual(service._transcript_segment_workers(), 4)

    def test_transcribe_rejects_non_whisper_provider(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(transcribe_provider="remote_api"),
                text_client=FakeTextClient(),
            )

            with self.assertRaisesRegex(Exception, "restricted to whisper.cpp"):
                service._transcribe_audio(Path(temp_dir) / "audio.wav")


class MeetingRecorderRouteTests(unittest.TestCase):
    def setUp(self):
        from bpmis_jira_tool.web import create_app

        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "MEETING_RECORDER_OWNER_EMAIL": "owner@npt.sg",
            },
            clear=False,
        ):
            self.app = create_app()
            self.app.testing = True

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def _login(client, email="owner@npt.sg", scopes=None):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": email, "name": "Owner"}
            session["google_credentials"] = {"token": "x", "scopes": scopes or []}

    def _wait_for_process_job(self, client, job_id, *, terminal_state="completed", timeout=2.0):
        deadline = time.time() + timeout
        last_payload = {}
        while time.time() < deadline:
            response = client.get(f"/api/meeting-recorder/process-jobs/{job_id}")
            self.assertEqual(response.status_code, 200)
            last_payload = response.get_json()
            if last_payload.get("state") == terminal_state:
                return last_payload
            time.sleep(0.02)
        self.fail(f"Meeting process job did not reach {terminal_state}: {last_payload}")

    def test_admin_can_open_page_and_non_admin_owner_is_denied(self):
        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg")
            admin_response = client.get("/meeting-recorder")
            translation_response = client.get("/meeting-translation")
            self._login(client, email="owner@npt.sg")
            owner_response = client.get("/meeting-recorder", follow_redirects=False)

        self.assertEqual(admin_response.status_code, 200)
        self.assertIn(b"Meeting Recorder", admin_response.data)
        self.assertEqual(translation_response.status_code, 200)
        self.assertIn(b"Meeting Translation", translation_response.data)
        self.assertEqual(owner_response.status_code, 302)

    def test_meeting_translation_start_requires_access(self):
        with self.app.test_client() as client:
            response = client.post("/api/meeting-translation/start", json={"target_language": "zh"})

        self.assertIn(response.status_code, {302, 401, 403})

    def test_records_api_lists_owner_records(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        store.create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/123",
        )

        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg")
            response = client.get("/api/meeting-recorder/records")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["records"][0]["title"], "Review")

    def test_upcoming_calendar_api_returns_next_three_meetings(self):
        fake_calendar = Mock()
        fake_calendar.upcoming_meetings.return_value = [
            {
                "calendar_event_id": f"event-{index}",
                "title": f"Meeting {index}",
                "platform": "google_meet",
                "start": f"2026-05-0{index}T09:00:00+08:00",
                "end": f"2026-05-0{index}T09:30:00+08:00",
                "meeting_link": f"https://meet.google.com/event-{index}",
            }
            for index in range(1, 6)
        ]

        with patch("bpmis_jira_tool.web._build_calendar_meeting_service", return_value=fake_calendar):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg", scopes=[CALENDAR_READONLY_SCOPE])
                response = client.get("/api/meeting-recorder/calendar/upcoming")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual([item["calendar_event_id"] for item in payload["meetings"]], ["event-1", "event-2", "event-3"])

    def test_diagnostics_and_records_use_local_agent_when_configured(self):
        fake_client = Mock()
        fake_client.meeting_recorder_diagnostics.return_value = {"ffmpeg_configured": True, "ffmpeg_path": "/opt/homebrew/bin/ffmpeg"}
        fake_client.meeting_recorder_records.return_value = [{"record_id": "meeting-1", "title": "Review"}]

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            client_patch = patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client)
            with client_patch:
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    diagnostics = client.get("/api/meeting-recorder/diagnostics")
                    records = client.get("/api/meeting-recorder/records")

        self.assertEqual(diagnostics.status_code, 200)
        self.assertEqual(records.status_code, 200)
        self.assertEqual(diagnostics.get_json()["ffmpeg_path"], "/opt/homebrew/bin/ffmpeg")
        self.assertEqual(records.get_json()["records"][0]["record_id"], "meeting-1")
        fake_client.meeting_recorder_diagnostics.assert_called_once_with()
        fake_client.meeting_recorder_records.assert_called_once_with(owner_email="xiaodong.zheng@npt.sg")

    def test_meeting_asset_proxy_forwards_range_to_local_agent(self):
        fake_response = FakeStreamingResponse(
            status_code=206,
            headers={
                "Content-Type": "video/mp4",
                "Content-Range": "bytes 0-99/1000",
                "Accept-Ranges": "bytes",
                "Content-Length": "100",
            },
            chunks=[b"a" * 40, b"b" * 60],
        )
        fake_client = Mock()
        fake_client.meeting_recorder_asset_response.return_value = fake_response

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    response = client.get(
                        "/meeting-recorder/assets/meeting-1/meeting.mp4",
                        headers={"Range": "bytes=0-99"},
                    )

        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.headers.get("Content-Type"), "video/mp4")
        self.assertEqual(response.headers.get("Content-Range"), "bytes 0-99/1000")
        self.assertEqual(response.headers.get("Accept-Ranges"), "bytes")
        self.assertEqual(response.data, b"a" * 40 + b"b" * 60)
        self.assertTrue(fake_response.closed)
        fake_client.meeting_recorder_asset_response.assert_called_once_with(
            record_id="meeting-1",
            owner_email="xiaodong.zheng@npt.sg",
            relative_path="meeting.mp4",
            range_header="bytes=0-99",
            method="GET",
            download=False,
        )

    def test_meeting_asset_download_sets_attachment_header_for_local_agent(self):
        fake_response = FakeStreamingResponse(
            status_code=200,
            headers={
                "Content-Type": "video/mp4",
                "Content-Length": "11",
                "X-Meeting-Recorder-Filename": "meeting.mp4",
            },
            chunks=[b"video-bytes"],
        )
        fake_client = Mock()
        fake_client.meeting_recorder_asset_response.return_value = fake_response

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    response = client.get("/meeting-recorder/assets/meeting-1/meeting.mp4?download=1")

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment", response.headers.get("Content-Disposition", ""))
        self.assertIn("meeting.mp4", response.headers.get("Content-Disposition", ""))
        self.assertEqual(response.data, b"video-bytes")
        self.assertTrue(fake_response.closed)
        fake_client.meeting_recorder_asset_response.assert_called_once_with(
            record_id="meeting-1",
            owner_email="xiaodong.zheng@npt.sg",
            relative_path="meeting.mp4",
            range_header="",
            method="GET",
            download=True,
        )

    def test_meeting_asset_download_rejects_html_from_local_agent(self):
        fake_response = FakeStreamingResponse(
            status_code=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            chunks=[b"<html>not video</html>"],
        )
        fake_client = Mock()
        fake_client.meeting_recorder_asset_response.return_value = fake_response

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    response = client.get("/meeting-recorder/assets/meeting-1/meeting.mp4?download=1")

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.headers.get("Content-Type"), "application/json")
        self.assertNotIn("attachment", response.headers.get("Content-Disposition", ""))
        self.assertTrue(fake_response.closed)

    def test_meeting_recorder_script_reports_audio_status_and_transcript_quality(self):
        source = Path("static/meeting_recorder.js").read_text(encoding="utf-8")
        template = Path("templates/meeting_recorder.html").read_text(encoding="utf-8")

        self.assertNotIn("Download video file", source)
        self.assertIn("Download audio file", source)
        self.assertIn("Audio download will be available after stopping the recording.", source)
        self.assertIn("Download transcript", source)
        self.assertIn("meeting-transcript-panel", source)
        self.assertIn("meeting-transcript-scroll", source)
        self.assertIn("let listDepth = 0", source)
        self.assertIn("const depth = bullet[1].replace", source)
        self.assertIn("closeLists(depth)", source)
        self.assertNotIn("data-meeting-stop-current", source)
        self.assertIn("download=1", source)
        self.assertIn("data-record-download-asset", source)
        self.assertIn("Download returned an HTML page", source)
        self.assertIn("Checking microphone/audio input", source)
        self.assertIn("Starting...", source)
        self.assertIn("audio_only", source)
        self.assertIn("data-meeting-transcript-language", template)
        self.assertNotIn("Meet or Zoom link (optional)", template)
        self.assertNotIn("name=\"meeting_link\"", template)
        self.assertIn("transcriptLanguageOptionsHtml", source)
        self.assertIn("data-meeting-row-transcript-language", source)
        self.assertIn("const rowLanguage = nodes.upcoming.querySelector", source)
        self.assertIn("transcript_language: rowLanguage", source)
        self.assertIn("meeting_link: ''", source)
        self.assertIn("transcript_language", source)
        self.assertIn("Chinese", template)
        self.assertIn("English", template)
        self.assertIn(">Mixed<", template)
        self.assertIn("const value = String(nodes.transcriptLanguage?.value || 'zh')", source)
        self.assertIn("?.value || 'zh'", source)
        self.assertIn("Grant Screen & System Audio Recording and Microphone permissions, then start recording again.", source)
        self.assertNotIn("browser", source.lower())
        self.assertNotIn("screen_audio", source)
        self.assertNotIn("/repair-video", source)
        self.assertIn("Transcript may be incomplete", source)
        self.assertIn("low_audio", source)
        self.assertIn("repeated chunk", source)
        self.assertIn('data-meeting-record-date', template)
        self.assertNotIn("data-meeting-stop-current", template)
        self.assertIn("recordDateValue(record) === selectedDate", source)
        self.assertIn("selectRecordDate(payload.record)", source)
        self.assertIn("loadRecords({ restoreActive: true })", source)
        self.assertIn("status || '').trim().toLowerCase() === 'recording'", source)
        self.assertIn("No meeting recordings on", source)

    def test_screencapturekit_helper_has_no_fixed_recording_duration(self):
        source = Path("tools/meeting_screencapture_helper.swift").read_text(encoding="utf-8")
        runtime_source = Path("bpmis_jira_tool/meeting_recorder.py").read_text(encoding="utf-8")

        self.assertIn("await withCheckedContinuation", source)
        self.assertIn("try await stream.stopCapture()", source)
        self.assertIn("statusEveryBuffers", source)
        self.assertIn("options: [])", source)
        self.assertNotIn(".prettyPrinted", source)
        self.assertNotIn("asyncAfter", source)
        self.assertIn("Meeting Recorder Capture Helper.app", runtime_source)
        self.assertIn("sg.npt.meeting-recorder.capture-helper", runtime_source)
        self.assertIn("NSScreenCaptureUsageDescription", runtime_source)
        self.assertIn("source.sha256", runtime_source)
        self.assertIn("built_digest == source_digest", runtime_source)
        self.assertIn("[codesign, \"--force\", \"--deep\", \"--sign\", \"-\", str(app_path)]", runtime_source)
        self.assertNotIn("--duration", runtime_source)
        self.assertNotIn("--timeout", runtime_source)

    def test_start_stop_process_and_email_routes_delegate_to_services(self):
        stored_record = self.app.config["MEETING_RECORD_STORE"].create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/123",
        )
        stored_record["status"] = "recorded"
        self.app.config["MEETING_RECORD_STORE"].save_record(stored_record)
        record_id = stored_record["record_id"]
        fake_runtime = Mock()
        fake_runtime.start_recording.return_value = {
            "record_id": record_id,
            "title": "Review",
            "platform": "zoom",
            "meeting_link": "https://zoom.us/j/123",
            "status": "recording",
        }
        fake_runtime.stop_recording.return_value = {
            "record_id": record_id,
            "title": "Review",
            "platform": "zoom",
            "meeting_link": "https://zoom.us/j/123",
            "status": "recorded",
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        fake_processing = Mock()
        fake_processing.process_recording.return_value = {
            "record_id": record_id,
            "title": "Review",
            "platform": "zoom",
            "meeting_link": "https://zoom.us/j/123",
            "status": "completed",
        }
        fake_processing.send_minutes_email.return_value = {"status": "sent", "message_id": "msg-1"}

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                start = client.post(
                    "/api/meeting-recorder/start",
                    json={
                        "title": "Review",
                        "platform": "zoom",
                        "meeting_link": "https://zoom.us/j/123",
                        "calendar_event_id": "event-1",
                        "scheduled_start": "2026-05-04T10:00:00+08:00",
                        "scheduled_end": "2026-05-04T10:30:00+08:00",
                        "attendees": [{"email": "alice@npt.sg"}],
                        "transcript_language": "en",
                    },
                )
                stop = client.post(f"/api/meeting-recorder/records/{record_id}/stop")
                stop_payload = stop.get_json()
                stop_status = self._wait_for_process_job(client, stop_payload["job_id"])
                process = client.post(f"/api/meeting-recorder/records/{record_id}/process")
                process_payload = process.get_json()
                process_status = self._wait_for_process_job(client, process_payload["job_id"])
                email = client.post(f"/api/meeting-recorder/records/{record_id}/send-email", json={})

        self.assertEqual(start.status_code, 200)
        self.assertEqual(stop.status_code, 200)
        self.assertEqual(stop_payload["status"], "ok")
        self.assertEqual(stop_payload["state"], "queued")
        self.assertTrue(stop_payload["job_id"])
        self.assertEqual(stop_status["state"], "completed")
        self.assertEqual(process.status_code, 200)
        self.assertEqual(process.get_json()["status"], "queued")
        self.assertEqual(process_status["state"], "completed")
        self.assertEqual(email.status_code, 200)
        fake_runtime.start_recording.assert_called_once_with(
            owner_email="xiaodong.zheng@npt.sg",
            title="Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/123",
            recording_mode="audio_only",
            calendar_event_id="event-1",
            scheduled_start="2026-05-04T10:00:00+08:00",
            scheduled_end="2026-05-04T10:30:00+08:00",
            attendees=[{"email": "alice@npt.sg"}],
            transcript_language="en",
        )
        fake_runtime.stop_recording.assert_called_once_with(record_id=record_id, owner_email="xiaodong.zheng@npt.sg")
        self.assertEqual(fake_processing.process_recording.call_count, 2)
        fake_processing.process_recording.assert_called_with(record_id=record_id, owner_email="xiaodong.zheng@npt.sg")
        self.assertEqual(fake_processing.send_minutes_email.call_count, 2)
        fake_processing.send_minutes_email.assert_called_with(
            record_id=record_id,
            owner_email="xiaodong.zheng@npt.sg",
            recipient="xiaodong.zheng@npt.sg",
        )

    def test_stop_route_auto_queues_processing_without_waiting_for_slow_processing(self):
        record = self.app.config["MEETING_RECORD_STORE"].create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Auto Process",
            platform="zoom",
            meeting_link="https://zoom.us/j/auto",
        )
        record["status"] = "recorded"
        self.app.config["MEETING_RECORD_STORE"].save_record(record)
        fake_runtime = Mock()
        fake_runtime.stop_recording.return_value = {
            "record_id": record["record_id"],
            "title": "Auto Process",
            "platform": "zoom",
            "status": "recorded",
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime
        release_processing = threading.Event()
        processing_started = threading.Event()
        fake_processing = Mock()

        def process_recording(**kwargs):
            processing_started.set()
            release_processing.wait(timeout=1)
            return {
                "record_id": kwargs["record_id"],
                "title": "Auto Process",
                "platform": "zoom",
                "status": "completed",
            }

        fake_processing.process_recording.side_effect = process_recording
        fake_processing.send_minutes_email.return_value = {
            "status": "sent",
            "recipient": "xiaodong.zheng@npt.sg",
            "message_id": "msg-auto",
        }

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                started = time.perf_counter()
                response = client.post(f"/api/meeting-recorder/records/{record['record_id']}/stop")
                elapsed = time.perf_counter() - started
                payload = response.get_json()
                self.assertEqual(response.status_code, 200)
                self.assertEqual(payload["status"], "ok")
                self.assertEqual(payload["state"], "queued")
                self.assertTrue(payload["job_id"])
                self.assertLess(elapsed, 0.5)
                self.assertTrue(processing_started.wait(timeout=1))
                duplicate = client.post(f"/api/meeting-recorder/records/{record['record_id']}/process")
                self.assertEqual(duplicate.get_json()["job_id"], payload["job_id"])
                release_processing.set()
                completed = self._wait_for_process_job(client, payload["job_id"])

        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["results"][0]["email"]["status"], "sent")
        fake_processing.process_recording.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="xiaodong.zheng@npt.sg",
        )
        fake_processing.send_minutes_email.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="xiaodong.zheng@npt.sg",
            recipient="xiaodong.zheng@npt.sg",
        )

    def test_process_route_recovers_stale_processing_record_without_queueing_job(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Recoverable Processing",
            platform="zoom",
            meeting_link="https://zoom.us/j/recover",
        )
        record["status"] = "processing"
        store.save_record(record)
        recovered_record = {
            **record,
            "status": "completed",
            "transcript": {"status": "completed"},
            "minutes": {"status": "completed"},
            "processing_recovery": {"status": "recovered_from_segments", "segment_count": 2},
        }
        fake_processing = Mock()
        fake_processing.recover_stale_processing_record.return_value = {
            "status": "recovered",
            "record": recovered_record,
        }

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                response = client.post(f"/api/meeting-recorder/records/{record['record_id']}/process")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["state"], "completed")
        self.assertEqual(payload["job_id"], "")
        self.assertEqual(payload["record"]["status"], "completed")
        self.assertEqual(payload["record"]["processing_recovery"]["status"], "recovered_from_segments")
        fake_processing.recover_stale_processing_record.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="xiaodong.zheng@npt.sg",
        )
        fake_processing.process_recording.assert_not_called()

    def test_process_route_marks_stale_processing_record_failed_retryable(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Incomplete Processing",
            platform="zoom",
            meeting_link="https://zoom.us/j/retry",
        )
        record["status"] = "processing"
        store.save_record(record)
        failed_record = {
            **record,
            "status": "failed",
            "error": "Meeting processing stalled before transcription completed. Re-run Process to retry.",
            "stalled_retryable": True,
        }
        fake_processing = Mock()
        fake_processing.recover_stale_processing_record.return_value = {
            "status": "failed",
            "record": failed_record,
        }

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                response = client.post(f"/api/meeting-recorder/records/{record['record_id']}/process")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["state"], "failed")
        self.assertTrue(payload["stalled_retryable"])
        self.assertTrue(payload["error_retryable"])
        self.assertEqual(payload["record"]["status"], "failed")
        self.assertTrue(payload["record"]["stalled_retryable"])

    def test_scheduled_auto_stop_queues_processing_and_email(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Scheduled Auto Stop",
            platform="google_meet",
            meeting_link="https://meet.google.com/abc-defg-hij",
            calendar_event_id="event-1",
            scheduled_start="2026-05-04T09:00:00+08:00",
            scheduled_end="2026-05-04T09:30:00+08:00",
        )
        record["status"] = "recording"
        record["recording_started_at"] = "2026-05-04T01:00:00+00:00"
        record["scheduled_auto_stop"] = {
            "status": "scheduled",
            "mode": "scheduled_end_plus_grace",
            "grace_seconds": 1200,
            "scheduled_for": "2026-05-04T01:50:00+00:00",
        }
        store.save_record(record)
        fake_processing = Mock()
        fake_processing.process_recording.return_value = {
            "record_id": record["record_id"],
            "title": "Scheduled Auto Stop",
            "platform": "google_meet",
            "status": "completed",
        }
        fake_processing.send_minutes_email.return_value = {
            "status": "sent",
            "recipient": "xiaodong.zheng@npt.sg",
            "message_id": "msg-1",
        }

        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg")
            with self.app.app_context(), patch(
                "bpmis_jira_tool.web._build_meeting_processing_service",
                return_value=fake_processing,
            ), patch.object(
                self.app.config["MEETING_RECORDER_RUNTIME"],
                "_terminate_persisted_recorder_process",
            ):
                self.app.config["MEETING_RECORDER_RUNTIME"]._scheduled_auto_stop_callback(
                    record_id=record["record_id"],
                    owner_email="xiaodong.zheng@npt.sg",
                    scheduled_for="2026-05-04T01:50:00+00:00",
                )
                updated = store.get_record(record["record_id"])
                completed = self._wait_for_process_job(client, updated["scheduled_auto_stop"]["process_job_id"])

        self.assertEqual(updated["recording_stop_reason"], "scheduled_auto_stop")
        self.assertEqual(updated["scheduled_auto_stop"]["process_queue_status"], "queued")
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["results"][0]["email"]["status"], "sent")
        fake_processing.process_recording.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="xiaodong.zheng@npt.sg",
        )
        fake_processing.send_minutes_email.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="xiaodong.zheng@npt.sg",
            recipient="xiaodong.zheng@npt.sg",
        )

    def test_local_agent_stop_auto_queues_processing(self):
        fake_client = Mock()
        fake_client.meeting_recorder_stop.return_value = {
            "record": {"record_id": "meeting-1", "title": "Review", "status": "recorded"}
        }
        fake_client.meeting_recorder_process_start.return_value = {
            "status": "queued",
            "state": "queued",
            "job_id": "job-1",
            "record": {"record_id": "meeting-1", "title": "Review", "status": "recorded"},
        }

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    response = client.post("/api/meeting-recorder/records/meeting-1/stop")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["job_id"], "job-1")
        self.assertEqual(payload["state"], "queued")
        fake_client.meeting_recorder_stop.assert_called_once_with(
            record_id="meeting-1",
            owner_email="xiaodong.zheng@npt.sg",
        )
        fake_client.meeting_recorder_process_start.assert_called_once_with(
            record_id="meeting-1",
            owner_email="xiaodong.zheng@npt.sg",
            send_email_on_complete=True,
        )

    def test_local_agent_recorder_routes_proxy_lifecycle_and_failures(self):
        fake_client = Mock()
        fake_client.meeting_recorder_start.return_value = {
            "record": {"record_id": "meeting-1", "title": "Review", "status": "recording"}
        }
        fake_client.meeting_recorder_signal_check.return_value = {
            "record": {"record_id": "meeting-1", "status": "recording", "audio_status": "ok"}
        }
        fake_client.meeting_recorder_process_start.return_value = {
            "state": "queued",
            "job_id": "job-1",
            "record": {"record_id": "meeting-1", "status": "recorded"},
        }
        fake_client.meeting_recorder_process_job.return_value = {
            "job_id": "job-1",
            "state": "running",
            "owner_email": "xiaodong.zheng@npt.sg",
            "stage": "transcribe",
            "message": "Transcribing",
        }
        fake_client.meeting_recorder_send_email.return_value = {
            "email": {"status": "sent", "recipient": "pm@npt.sg"}
        }
        fake_client.meeting_recorder_record.return_value = {
            "record": {"record_id": "meeting-1", "owner_email": "xiaodong.zheng@npt.sg"}
        }
        fake_client.meeting_recorder_delete.return_value = {"status": "ok"}
        fake_client.meeting_recorder_diagnostics.side_effect = [ToolError("local-agent unavailable"), {"ffmpeg_configured": True}]

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    diagnostics_failure = client.get("/api/meeting-recorder/diagnostics")
                    diagnostics = client.get("/api/meeting-recorder/diagnostics")
                    start = client.post(
                        "/api/meeting-recorder/start",
                        json={
                            "title": "Review",
                            "meetingLink": "https://meet.google.com/abc-defg-hij",
                            "recordingMode": "",
                            "transcriptLanguage": "english",
                            "attendees": "not-a-list",
                        },
                    )
                    signal = client.post("/api/meeting-recorder/records/meeting-1/signal-check")
                    process = client.post("/api/meeting-recorder/records/meeting-1/process")
                    job = client.get("/api/meeting-recorder/process-jobs/job-1")
                    email = client.post("/api/meeting-recorder/records/meeting-1/send-email", json={"recipient": "pm@npt.sg"})
                    record = client.get("/api/meeting-recorder/records/meeting-1")
                    delete = client.delete("/api/meeting-recorder/records/meeting-1")

        self.assertEqual(diagnostics_failure.status_code, 502)
        self.assertEqual(diagnostics.status_code, 200)
        self.assertEqual(start.status_code, 200)
        self.assertEqual(signal.get_json()["record"]["audio_status"], "ok")
        self.assertEqual(process.get_json()["job_id"], "job-1")
        self.assertEqual(job.get_json()["state"], "running")
        self.assertNotIn("owner_email", job.get_json())
        self.assertEqual(email.get_json()["email"]["recipient"], "pm@npt.sg")
        self.assertEqual(record.get_json()["record"]["record_id"], "meeting-1")
        self.assertEqual(delete.get_json()["status"], "ok")
        fake_client.meeting_recorder_start.assert_called_once()
        start_payload = fake_client.meeting_recorder_start.call_args.args[0]
        self.assertEqual(start_payload["transcript_language"], "en")
        self.assertEqual(start_payload["recording_mode"], "audio_only")
        self.assertEqual(start_payload["platform"], "google_meet")
        fake_client.meeting_recorder_send_email.assert_called_once_with(
            record_id="meeting-1",
            owner_email="xiaodong.zheng@npt.sg",
            recipient="pm@npt.sg",
        )

    def test_meeting_recorder_route_access_gates_cover_api_and_page_handlers(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Denied",
            platform="zoom",
            meeting_link="https://zoom.us/j/denied",
        )
        asset_path = store.record_dir(record["record_id"]) / "meeting.wav"
        asset_path.write_bytes(b"asset")

        with self.app.test_client() as client:
            self._login(client, email="owner@npt.sg")
            responses = [
                client.get("/meeting-translation"),
                client.post("/api/meeting-translation/sessions/session-1/stop"),
                client.get("/api/meeting-translation/sessions/session-1/events"),
                client.get("/api/meeting-recorder/diagnostics"),
                client.get("/api/meeting-recorder/calendar/upcoming"),
                client.get("/api/meeting-recorder/records"),
                client.get(f"/api/meeting-recorder/records/{record['record_id']}"),
                client.post("/api/meeting-recorder/start", json={}),
                client.post(f"/api/meeting-recorder/records/{record['record_id']}/stop"),
                client.post(f"/api/meeting-recorder/records/{record['record_id']}/signal-check"),
                client.post(f"/api/meeting-recorder/records/{record['record_id']}/process"),
                client.get("/api/meeting-recorder/process-jobs/missing-job"),
                client.post(f"/api/meeting-recorder/records/{record['record_id']}/send-email", json={}),
                client.delete(f"/api/meeting-recorder/records/{record['record_id']}"),
                client.get(f"/meeting-recorder/assets/{record['record_id']}/meeting.wav"),
            ]

        self.assertTrue(all(response.status_code in {302, 403} for response in responses))

    def test_local_translation_runtime_success_and_not_found_errors(self):
        fake_runtime = Mock()
        fake_runtime.start_session.return_value = {
            "session": {"session_id": "session-1", "status": "running", "target_language": "zh"}
        }
        fake_runtime.stop_session.side_effect = [
            {"session": {"session_id": "session-1", "status": "stopped"}},
            ToolError("missing local session"),
        ]
        fake_runtime.event_stream.side_effect = [
            [{"event": "ready"}],
            ToolError("missing event stream"),
        ]
        self.app.config["MEETING_TRANSLATION_RUNTIME"] = fake_runtime

        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg")
            started = client.post("/api/meeting-translation/start", json={"target_language": "zh"})
            stopped = client.post("/api/meeting-translation/sessions/session-1/stop")
            stop_missing = client.post("/api/meeting-translation/sessions/session-1/stop")
            events = client.get("/api/meeting-translation/sessions/session-1/events")
            events_missing = client.get("/api/meeting-translation/sessions/session-1/events")

        self.assertEqual(started.status_code, 200)
        self.assertEqual(stopped.status_code, 200)
        self.assertEqual(stop_missing.status_code, 404)
        self.assertEqual(events.status_code, 200)
        self.assertIn(b'"event":"ready"', events.data)
        self.assertEqual(events_missing.status_code, 404)
        fake_runtime.start_session.assert_called_once_with(
            owner_email="xiaodong.zheng@npt.sg",
            target_language="zh",
        )

    def test_local_meeting_recorder_route_error_boundaries(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Route Errors",
            platform="zoom",
            meeting_link="https://zoom.us/j/errors",
        )
        record["status"] = "recorded"
        store.save_record(record)
        fake_runtime = Mock()
        fake_runtime.diagnostics.return_value = {"ffmpeg_configured": True}
        fake_runtime.start_recording.side_effect = ConfigError("ffmpeg missing")
        fake_runtime.stop_recording.side_effect = ToolError("cannot stop")
        fake_runtime.check_recording_signal.side_effect = ToolError("cannot check signal")
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime
        fake_calendar = Mock()
        fake_calendar.upcoming_meetings.side_effect = RuntimeError("calendar exploded")
        fake_processing = Mock()
        fake_processing.send_minutes_email.side_effect = ToolError("gmail unavailable")

        with patch("bpmis_jira_tool.web._build_calendar_meeting_service", return_value=fake_calendar), patch(
            "bpmis_jira_tool.web._queue_meeting_recorder_process_job",
            side_effect=ConfigError("queue unavailable"),
        ), patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg", scopes=[CALENDAR_READONLY_SCOPE])
                diagnostics = client.get("/api/meeting-recorder/diagnostics")
                calendar_error = client.get("/api/meeting-recorder/calendar/upcoming")
                start_error = client.post("/api/meeting-recorder/start", json={"meeting_link": "https://zoom.us/j/errors"})
                stop_error = client.post(f"/api/meeting-recorder/records/{record['record_id']}/stop")
                signal_error = client.post(f"/api/meeting-recorder/records/{record['record_id']}/signal-check")
                process_error = client.post(f"/api/meeting-recorder/records/{record['record_id']}/process")
                missing_job = client.get("/api/meeting-recorder/process-jobs/missing-job")
                email_error = client.post(f"/api/meeting-recorder/records/{record['record_id']}/send-email", json={})
                missing_asset_record = client.get("/meeting-recorder/assets/missing-record/meeting.wav")

        self.assertEqual(diagnostics.status_code, 200)
        self.assertEqual(calendar_error.status_code, 500)
        self.assertEqual(start_error.status_code, 400)
        self.assertEqual(stop_error.status_code, 400)
        self.assertEqual(signal_error.status_code, 400)
        self.assertEqual(process_error.status_code, 400)
        self.assertEqual(missing_job.status_code, 404)
        self.assertEqual(missing_job.get_json()["error_code"], "job_not_found")
        self.assertEqual(email_error.status_code, 400)
        self.assertEqual(missing_asset_record.status_code, 400)

    def test_local_meeting_recorder_route_success_and_owner_boundaries(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        owned_record = store.create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Owned Route",
            platform="zoom",
            meeting_link="https://zoom.us/j/owned",
        )
        owned_record["status"] = "recorded"
        owned_record["media"] = {"audio_path": f"records/{owned_record['record_id']}/meeting.wav"}
        store.save_record(owned_record)
        asset_path = store.record_dir(owned_record["record_id"]) / "meeting.wav"
        asset_path.write_bytes(b"meeting-audio")
        denied_record = store.create_record(
            owner_email="other@npt.sg",
            title="Other Route",
            platform="zoom",
            meeting_link="https://zoom.us/j/other",
        )
        fake_runtime = Mock()
        fake_runtime.check_recording_signal.return_value = {
            **owned_record,
            "status": "recording",
            "recording_health": {"status": "recording"},
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg")
            forbidden_record = client.get(f"/api/meeting-recorder/records/{denied_record['record_id']}")
            signal = client.post(f"/api/meeting-recorder/records/{owned_record['record_id']}/signal-check")
            asset = client.get(f"/meeting-recorder/assets/{owned_record['record_id']}/meeting.wav")
            deleted = client.delete(f"/api/meeting-recorder/records/{owned_record['record_id']}")

        self.assertEqual(forbidden_record.status_code, 403)
        self.assertEqual(signal.status_code, 200)
        self.assertEqual(signal.get_json()["record"]["recording_health"]["status"], "recording")
        self.assertEqual(asset.status_code, 200)
        self.assertEqual(asset.data, b"meeting-audio")
        self.assertEqual(deleted.status_code, 200)
        self.assertEqual(store.get_record(owned_record["record_id"])["status"], "deleted")
        self.assertFalse(asset_path.exists())

    def test_local_agent_meeting_recorder_error_mapping_and_asset_head(self):
        head_response = FakeStreamingResponse(
            status_code=200,
            headers={
                "Content-Type": "audio/wav",
                "Content-Length": "10",
                "Connection": "keep-alive",
                "X-Meeting-Recorder-Filename": "meeting.wav",
            },
            chunks=[],
        )
        fake_client = Mock()
        fake_client.meeting_recorder_records.side_effect = ToolError("local-agent unavailable")
        fake_client.meeting_recorder_record.side_effect = ToolError("record missing")
        fake_client.meeting_recorder_start.side_effect = ToolError("start failed")
        fake_client.meeting_recorder_stop.side_effect = ToolError("stop failed")
        fake_client.meeting_recorder_signal_check.side_effect = ToolError("signal failed")
        fake_client.meeting_recorder_process_start.side_effect = ToolError("process failed")
        fake_client.meeting_recorder_process_job.side_effect = ToolError("job failed")
        fake_client.meeting_recorder_send_email.side_effect = ToolError("email failed")
        fake_client.meeting_recorder_delete.side_effect = ToolError("delete failed")
        fake_client.meeting_recorder_asset_response.return_value = head_response

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    records = client.get("/api/meeting-recorder/records")
                    record = client.get("/api/meeting-recorder/records/meeting-1")
                    start = client.post("/api/meeting-recorder/start", json={"meeting_link": "https://zoom.us/j/1"})
                    stop = client.post("/api/meeting-recorder/records/meeting-1/stop")
                    signal = client.post("/api/meeting-recorder/records/meeting-1/signal-check")
                    process = client.post("/api/meeting-recorder/records/meeting-1/process")
                    job = client.get("/api/meeting-recorder/process-jobs/job-1")
                    email = client.post("/api/meeting-recorder/records/meeting-1/send-email", json={})
                    delete = client.delete("/api/meeting-recorder/records/meeting-1")
                    asset_head = client.head("/meeting-recorder/assets/meeting-1/meeting.wav?download=1")

        self.assertEqual(records.status_code, 502)
        self.assertEqual(record.status_code, 400)
        self.assertEqual(start.status_code, 400)
        self.assertEqual(stop.status_code, 400)
        self.assertEqual(signal.status_code, 400)
        self.assertEqual(process.status_code, 400)
        self.assertEqual(job.status_code, 400)
        self.assertEqual(email.status_code, 400)
        self.assertEqual(delete.status_code, 400)
        self.assertEqual(asset_head.status_code, 200)
        self.assertIn("attachment", asset_head.headers.get("Content-Disposition", ""))
        self.assertNotIn("Connection", asset_head.headers)
        self.assertTrue(head_response.closed)

    def test_meeting_translation_routes_cover_local_agent_stream_and_error_mapping(self):
        fake_stream = FakeStreamingResponse(
            status_code=200,
            headers={"Content-Type": "text/event-stream", "X-Upstream": "local"},
            chunks=[b"data: ready\n\n"],
        )
        fake_client = Mock()
        fake_client.meeting_translation_start.side_effect = [
            {"session": {"session_id": "session-1", "status": "running", "target_language": "en"}},
            ToolError("local-agent unavailable"),
        ]
        fake_client.meeting_translation_stop.side_effect = [
            {"session": {"session_id": "session-1", "status": "stopped"}},
            ToolError("missing session"),
        ]
        fake_client.meeting_translation_events_response.side_effect = [fake_stream, ToolError("local-agent unavailable")]

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    started = client.post("/api/meeting-translation/start", json={"target_language": "en"})
                    start_error = client.post("/api/meeting-translation/start", json={"target_language": "en"})
                    stopped = client.post("/api/meeting-translation/sessions/session-1/stop")
                    stop_error = client.post("/api/meeting-translation/sessions/session-1/stop")
                    events = client.get("/api/meeting-translation/sessions/session-1/events")
                    events_error = client.get("/api/meeting-translation/sessions/session-1/events")

        self.assertEqual(started.status_code, 200)
        self.assertEqual(start_error.status_code, 502)
        self.assertEqual(stopped.status_code, 200)
        self.assertEqual(stop_error.status_code, 404)
        self.assertEqual(events.status_code, 200)
        self.assertEqual(events.data, b"data: ready\n\n")
        self.assertEqual(events_error.status_code, 502)
        fake_client.meeting_translation_start.assert_any_call(
            {"owner_email": "xiaodong.zheng@npt.sg", "target_language": "en"}
        )

    def test_local_recorder_routes_cover_calendar_and_asset_guards(self):
        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg", scopes=[])
            missing_calendar = client.get("/api/meeting-recorder/calendar/upcoming")

        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Active",
            platform="zoom",
            meeting_link="https://zoom.us/j/active",
        )
        record["status"] = "recording"
        record["media"] = {"audio_path": f"{record['record_id']}/meeting.wav"}
        store.save_record(record)
        asset_dir = store.record_dir(record["record_id"])
        asset_path = asset_dir / "meeting.wav"
        asset_path.write_bytes(b"active-audio")
        other_record = store.create_record(
            owner_email="owner@npt.sg",
            title="Other",
            platform="zoom",
            meeting_link="https://zoom.us/j/other",
        )
        other_asset = store.record_dir(other_record["record_id"]) / "meeting.wav"
        other_asset.write_bytes(b"other-audio")

        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg")
            forbidden_owner = client.get(f"/api/meeting-recorder/records/{record['record_id']}")
            active_download = client.get(f"/meeting-recorder/assets/{record['record_id']}/meeting.wav?download=1")
            traversal = client.get(f"/meeting-recorder/assets/{record['record_id']}/../secret.txt")
            missing_asset = client.get(f"/meeting-recorder/assets/{record['record_id']}/missing.wav")
            wrong_owner_asset = client.get(f"/meeting-recorder/assets/{other_record['record_id']}/meeting.wav")

        self.assertEqual(missing_calendar.status_code, 400)
        self.assertEqual(forbidden_owner.status_code, 200)
        self.assertEqual(active_download.status_code, 409)
        self.assertEqual(traversal.status_code, 400)
        self.assertEqual(missing_asset.status_code, 404)
        self.assertEqual(wrong_owner_asset.status_code, 403)

    def test_auto_process_email_failure_does_not_fail_completed_minutes_job(self):
        record = self.app.config["MEETING_RECORD_STORE"].create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Email Failure",
            platform="zoom",
            meeting_link="https://zoom.us/j/email",
        )
        record["status"] = "recorded"
        self.app.config["MEETING_RECORD_STORE"].save_record(record)
        fake_runtime = Mock()
        fake_runtime.stop_recording.return_value = {
            "record_id": record["record_id"],
            "title": "Email Failure",
            "platform": "zoom",
            "status": "recorded",
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime
        fake_processing = Mock()

        def process_recording(**kwargs):
            stored = self.app.config["MEETING_RECORD_STORE"].get_record(kwargs["record_id"])
            stored["status"] = "completed"
            stored["minutes"] = {"status": "completed", "markdown": "Minutes"}
            self.app.config["MEETING_RECORD_STORE"].save_record(stored)
            return stored

        fake_processing.process_recording.side_effect = process_recording
        fake_processing.send_minutes_email.side_effect = ToolError("token=secret traceback")

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                response = client.post(f"/api/meeting-recorder/records/{record['record_id']}/stop")
                payload = response.get_json()
                completed = self._wait_for_process_job(client, payload["job_id"])
                record_response = client.get(f"/api/meeting-recorder/records/{record['record_id']}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["results"][0]["email"]["status"], "failed")
        self.assertNotIn("secret", completed["results"][0]["email"]["error"])
        updated = record_response.get_json()["record"]
        self.assertEqual(updated["status"], "completed")
        self.assertEqual(updated["email"]["status"], "failed")
        self.assertNotIn("traceback", updated["email"]["error"].lower())

    def test_process_route_returns_job_without_waiting_for_slow_processing(self):
        record = self.app.config["MEETING_RECORD_STORE"].create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Slow Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/slow",
        )
        record["status"] = "recorded"
        self.app.config["MEETING_RECORD_STORE"].save_record(record)
        release_processing = threading.Event()
        processing_started = threading.Event()
        fake_processing = Mock()

        def process_recording(**kwargs):
            processing_started.set()
            release_processing.wait(timeout=1)
            return {
                "record_id": kwargs["record_id"],
                "title": "Slow Review",
                "platform": "zoom",
                "status": "completed",
            }

        fake_processing.process_recording.side_effect = process_recording

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                started = time.perf_counter()
                response = client.post(f"/api/meeting-recorder/records/{record['record_id']}/process")
                elapsed = time.perf_counter() - started
                payload = response.get_json()
                self.assertEqual(response.status_code, 200)
                self.assertEqual(payload["status"], "queued")
                self.assertLess(elapsed, 0.5)
                self.assertTrue(processing_started.wait(timeout=1))
                release_processing.set()
                completed = self._wait_for_process_job(client, payload["job_id"])

        self.assertEqual(completed["state"], "completed")
        fake_processing.process_recording.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="xiaodong.zheng@npt.sg",
        )

    def test_failed_process_job_marks_record_failed_without_sensitive_error_leak(self):
        record = self.app.config["MEETING_RECORD_STORE"].create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Bad Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/bad",
        )
        record["status"] = "recorded"
        self.app.config["MEETING_RECORD_STORE"].save_record(record)
        fake_processing = Mock()
        fake_processing.process_recording.side_effect = RuntimeError("Traceback token=secret")

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                response = client.post(f"/api/meeting-recorder/records/{record['record_id']}/process")
                payload = response.get_json()
                failed = self._wait_for_process_job(client, payload["job_id"], terminal_state="failed")
                record_response = client.get(f"/api/meeting-recorder/records/{record['record_id']}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(failed["state"], "failed")
        self.assertNotIn("Traceback", failed["error"])
        self.assertNotIn("secret", failed["error"])
        failed_record = record_response.get_json()["record"]
        self.assertEqual(failed_record["status"], "failed")
        self.assertNotIn("token", failed_record["error"])

    def test_stale_processing_record_with_active_worker_is_not_requeued(self):
        record = self.app.config["MEETING_RECORD_STORE"].create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Stale Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/stale",
        )
        record["status"] = "processing"
        self.app.config["MEETING_RECORD_STORE"].save_record(record)
        fake_processing = Mock()
        fake_processing.recover_stale_processing_record.return_value = {
            "status": "active",
            "record": record,
        }

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                response = client.post(f"/api/meeting-recorder/records/{record['record_id']}/process")
                payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["state"], "running")
        self.assertEqual(payload["job_id"], "")
        fake_processing.recover_stale_processing_record.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="xiaodong.zheng@npt.sg",
        )
        fake_processing.process_recording.assert_not_called()

    def test_meeting_recorder_script_polls_process_jobs_and_uses_clear_errors(self):
        source = Path("static/meeting_recorder.js").read_text(encoding="utf-8")

        self.assertIn("/api/meeting-recorder/process-jobs/", source)
        self.assertIn("pollMeetingProcessJob", source)
        self.assertIn("Meeting processing failed.", source)
        self.assertIn("Meeting processing is still running.", source)
        self.assertIn("Connection interrupted. Refreshing status...", source)
        self.assertIn("isNetworkError", source)
        self.assertIn("monitorAutoProcessJob", source)
        self.assertIn("Transcribing audio and generating meeting minutes...", source)
        self.assertIn("Meeting processing was not queued:", source)
        self.assertNotIn("button.textContent = 'Failed to fetch'", source)

    def test_manual_empty_link_start_route_uses_backend_screencapturekit(self):
        fake_runtime = Mock()
        fake_runtime.start_recording.return_value = {
            "record_id": "meeting-2",
            "title": "Face to face",
            "platform": "unknown",
            "meeting_link": "",
            "status": "recording",
            "media": {
                "recording_mode": "audio_only",
                "audio_capture_profile": "screencapturekit_audio_v1",
                "screencapture_capture_source": "screencapturekit_f2f",
            },
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg")
            response = client.post(
                "/api/meeting-recorder/start",
                json={"title": "Face to face", "meeting_link": "", "recording_mode": "audio_only", "transcript_language": "zh"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["record"]["media"]["screencapture_capture_source"], "screencapturekit_f2f")
        fake_runtime.start_recording.assert_called_once_with(
            owner_email="xiaodong.zheng@npt.sg",
            title="Face to face",
            platform="unknown",
            meeting_link="",
            recording_mode="audio_only",
            calendar_event_id="",
            scheduled_start="",
            scheduled_end="",
            attendees=[],
            transcript_language="zh",
        )


if __name__ == "__main__":
    unittest.main()
