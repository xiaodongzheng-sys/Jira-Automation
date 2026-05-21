import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.meeting_recorder import (
    CALENDAR_READONLY_SCOPE,
    GoogleCalendarMeetingService,
    MeetingProcessingService,
    MeetingRecorderConfig,
    MeetingRecorderRuntime,
    MeetingRecordStore,
    _audio_capture_status,
    _build_ffmpeg_audio_post_stop_pad_command,
    _build_ffmpeg_audio_recording_command,
    _build_ffmpeg_audio_segment_command,
    _effective_audio_input,
    _effective_recording_audio_input,
    _meeting_transcript_hash,
    _meeting_minutes_markdown_to_html,
    _parse_avfoundation_devices,
    _parse_srt_transcript,
    _SegmentedAudioRecorder,
    _ScreenCaptureKitAudioRecorder,
    build_calendar_api_service,
    extract_meeting_links,
    meeting_transcript_whisper_language,
    meeting_platform_from_link,
    normalize_calendar_event,
)


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
            "attendees": [{"email": "alice@npt.sg", "displayName": "Alice"}],
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

    def test_parse_avfoundation_devices_and_audio_capture_modes(self):
        output = """
        [AVFoundation indev @ 0x123] AVFoundation video devices:
        [AVFoundation indev @ 0x123] [0] MacBook Air Camera
        [AVFoundation indev @ 0x123] [1] Capture screen 0
        [AVFoundation indev @ 0x123] AVFoundation audio devices:
        [AVFoundation indev @ 0x123] [0] MacBook Air Microphone
        [AVFoundation indev @ 0x123] [1] BlackHole 2ch
        [AVFoundation indev @ 0x123] [2] Meeting Recorder Aggregate
        """

        devices = _parse_avfoundation_devices(output)

        self.assertEqual(devices["video_devices"], ["MacBook Air Camera", "Capture screen 0"])
        self.assertEqual(devices["audio_devices"], ["MacBook Air Microphone", "BlackHole 2ch", "Meeting Recorder Aggregate"])
        self.assertEqual(_effective_audio_input("default", devices["audio_devices"]), "Meeting Recorder Aggregate")
        self.assertEqual(_audio_capture_status("MacBook Air Microphone", devices["audio_devices"])["audio_capture_label"], "Microphone only")
        self.assertEqual(_audio_capture_status("BlackHole 2ch", devices["audio_devices"])["audio_capture_label"], "System audio configured")
        aggregate_status = _audio_capture_status("Meeting Recorder Aggregate", devices["audio_devices"])
        self.assertTrue(aggregate_status["system_audio_configured"])
        self.assertFalse(aggregate_status["audio_signal_verified"])
        self.assertEqual(
            _effective_recording_audio_input(
                "Meeting Recorder Aggregate",
                devices["audio_devices"],
                recording_mode="audio_only",
                meeting_link="",
            ),
            "MacBook Air Microphone",
        )
        self.assertEqual(
            _effective_recording_audio_input(
                "Meeting Recorder Aggregate",
                devices["audio_devices"],
                recording_mode="audio_only",
                meeting_link="https://zoom.us/j/123",
            ),
            "Meeting Recorder Aggregate",
        )

    def test_ffmpeg_audio_recording_command_uses_audio_input_only(self):
        command = _build_ffmpeg_audio_recording_command(
            ffmpeg_path="/opt/homebrew/bin/ffmpeg",
            audio_input="Meeting Recorder Aggregate",
            audio_path=Path("/tmp/meeting.wav"),
        )

        self.assertIn("-i", command)
        self.assertEqual(command[command.index("-i") + 1], ":Meeting Recorder Aggregate")
        self.assertIn("-nostdin", command)
        self.assertNotIn("-use_wallclock_as_timestamps", command)
        self.assertNotIn("aresample=async=1:first_pts=0", command)
        self.assertNotIn("-af", command)
        self.assertNotIn("Capture screen 0", command)
        self.assertNotIn("-framerate", command)
        self.assertNotIn("-c:v", command)
        self.assertIn("-acodec", command)
        self.assertEqual(command[command.index("-acodec") + 1], "pcm_s16le")
        self.assertEqual(command[-1], "/tmp/meeting.wav")

    def test_ffmpeg_audio_segment_command_mixes_silence_clock(self):
        command = _build_ffmpeg_audio_segment_command(
            ffmpeg_path="/opt/homebrew/bin/ffmpeg",
            audio_input="Meeting Recorder Aggregate",
            audio_path=Path("/tmp/segment.wav"),
            duration_seconds=30,
        )

        self.assertIn("anullsrc=r=48000:cl=stereo", command)
        filter_complex = command[command.index("-filter_complex") + 1]
        self.assertIn("[nullrec][mic][zoom]amix=inputs=3:duration=first:dropout_transition=0[rec]", filter_complex)
        self.assertIn("[nullmonsrc][bhmon]amix=inputs=2:duration=first:dropout_transition=0[monitor]", filter_complex)
        input_indexes = [index for index, value in enumerate(command) if value == "-i"]
        self.assertEqual(command[input_indexes[0] + 1], "anullsrc=r=48000:cl=stereo")
        self.assertEqual(command[input_indexes[1] + 1], ":MacBook Air Microphone")
        self.assertEqual(command[input_indexes[2] + 1], ":BlackHole 2ch")
        self.assertIn("[rec]", command)
        self.assertIn("[monitor]", command)
        self.assertEqual(command[command.index("-t") + 1], "30")
        self.assertIn("/tmp/segment.wav", command)
        self.assertIn("audiotoolbox", command)

    def test_ffmpeg_audio_post_stop_pad_command_pads_after_recording(self):
        command = _build_ffmpeg_audio_post_stop_pad_command(
            ffmpeg_path="/opt/homebrew/bin/ffmpeg",
            source_path=Path("/tmp/meeting.wav"),
            output_path=Path("/tmp/meeting.padded.wav"),
            target_duration_seconds=43.2,
        )

        self.assertEqual(command[command.index("-i") + 1], "/tmp/meeting.wav")
        self.assertEqual(command[command.index("-af") + 1], "apad=whole_dur=43.200")
        self.assertEqual(command[command.index("-t") + 1], "43.200")
        self.assertEqual(command[command.index("-acodec") + 1], "pcm_s16le")
        self.assertEqual(command[-1], "/tmp/meeting.padded.wav")

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
                config=MeetingRecorderConfig(
                    ffmpeg_bin="/opt/homebrew/bin/ffmpeg",
                    audio_input="Meeting Recorder Aggregate",
                ),
            )
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["MacBook Air Microphone", "Meeting Recorder Aggregate"],
                },
            ), patch(
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
        self.assertEqual(record["diagnostics_snapshot"]["configured_audio_input"], "Meeting Recorder Aggregate")
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
                config=MeetingRecorderConfig(
                    ffmpeg_bin="/opt/homebrew/bin/ffmpeg",
                    audio_input="Meeting Recorder Aggregate",
                ),
            )
            self._FakeTimer.instances = []
            with patch("bpmis_jira_tool.meeting_recorder.threading.Timer", self._FakeTimer), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin",
                return_value="/opt/homebrew/bin/ffmpeg",
            ), patch(
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["MacBook Air Microphone", "Meeting Recorder Aggregate"],
                },
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
        self.assertEqual(len(self._FakeTimer.instances), 1)
        self.assertTrue(self._FakeTimer.instances[0].started)
        self.assertTrue(self._FakeTimer.instances[0].daemon)

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

    def test_audio_only_linked_fallback_keeps_aggregate_input(self):
        self.assertEqual(
            _effective_recording_audio_input(
                "Meeting Recorder Aggregate",
                ["MacBook Air Microphone", "Meeting Recorder Aggregate"],
                recording_mode="audio_only",
                meeting_link="https://zoom.us/j/123",
            ),
            "Meeting Recorder Aggregate",
        )

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
        self.assertIn("padded with silence", health["warning"])

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
                        "audio_capture_profile": "segmented_avfoundation_v1",
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

    def test_segmented_audio_recording_combines_segments_on_stop(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Zoom review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record_dir = store.record_dir(record["record_id"])
            segment_dir = record_dir / "audio_segments"
            segment_dir.mkdir()
            (segment_dir / "segment-000000.wav").write_bytes(b"0" * 100)
            (segment_dir / "segment-000001.wav").write_bytes(b"1" * 100)
            record["media"] = {
                "recording_mode": "audio_only",
                "audio_capture_profile": "segmented_avfoundation_v1",
                "audio_segment_dir": str(segment_dir.relative_to(store.root_dir)),
                "audio_path": f"records/{record['record_id']}/meeting.wav",
            }

            def fake_run(command, *_args, **_kwargs):
                Path(command[-1]).write_bytes(b"combined-audio" * 8)
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=fake_run,
            ), patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=60.0):
                finalized = runtime._finalize_segmented_audio_recording(record)

        media = finalized["media"]
        self.assertEqual(media["audio_segment_count"], 2)
        self.assertEqual(media["audio_path"], f"records/{record['record_id']}/meeting.wav")
        self.assertEqual(media["audio_url"], f"/meeting-recorder/assets/{record['record_id']}/meeting.wav")
        self.assertEqual(media["audio_original_duration_seconds"], 60.0)
        self.assertIn("audio_concat_command", media)

    def test_segmented_audio_finalization_records_warning_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())

            missing_dir_record = store.create_record(owner_email="owner@npt.sg", title="Missing Dir", platform="zoom", meeting_link="")
            missing_dir_record["media"] = {
                "audio_capture_profile": "segmented_avfoundation_v1",
                "audio_segment_dir": "records/missing/audio_segments",
            }
            missing_dir = runtime._finalize_segmented_audio_recording(missing_dir_record)

            empty_dir_record = store.create_record(owner_email="owner@npt.sg", title="Empty Dir", platform="zoom", meeting_link="")
            empty_segment_dir = store.record_dir(empty_dir_record["record_id"]) / "audio_segments"
            empty_segment_dir.mkdir()
            empty_dir_record["media"] = {
                "audio_capture_profile": "segmented_avfoundation_v1",
                "audio_segment_dir": str(empty_segment_dir.relative_to(store.root_dir)),
            }
            empty_dir = runtime._finalize_segmented_audio_recording(empty_dir_record)

            no_ffmpeg_record = store.create_record(owner_email="owner@npt.sg", title="No FFMPEG", platform="zoom", meeting_link="")
            no_ffmpeg_dir = store.record_dir(no_ffmpeg_record["record_id"]) / "audio_segments"
            no_ffmpeg_dir.mkdir()
            (no_ffmpeg_dir / "segment-000000.wav").write_bytes(b"0" * 100)
            no_ffmpeg_record["media"] = {
                "audio_capture_profile": "segmented_avfoundation_v1",
                "audio_segment_dir": str(no_ffmpeg_dir.relative_to(store.root_dir)),
            }
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value=""):
                no_ffmpeg = runtime._finalize_segmented_audio_recording(no_ffmpeg_record)

            single_copy_record = store.create_record(owner_email="owner@npt.sg", title="Single Copy", platform="zoom", meeting_link="")
            single_copy_dir = store.record_dir(single_copy_record["record_id"]) / "audio_segments"
            single_copy_dir.mkdir()
            (single_copy_dir / "segment-000000.wav").write_bytes(b"1" * 100)
            single_copy_record["media"] = {
                "audio_capture_profile": "segmented_avfoundation_v1",
                "audio_segment_dir": str(single_copy_dir.relative_to(store.root_dir)),
            }
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=ToolError("concat failed"),
            ), patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=5.0):
                single_copy = runtime._finalize_segmented_audio_recording(single_copy_record)

            multi_fail_record = store.create_record(owner_email="owner@npt.sg", title="Multi Fail", platform="zoom", meeting_link="")
            multi_fail_dir = store.record_dir(multi_fail_record["record_id"]) / "audio_segments"
            multi_fail_dir.mkdir()
            (multi_fail_dir / "segment-000000.wav").write_bytes(b"1" * 100)
            (multi_fail_dir / "segment-000001.wav").write_bytes(b"2" * 100)
            multi_fail_record["media"] = {
                "audio_capture_profile": "segmented_avfoundation_v1",
                "audio_segment_dir": str(multi_fail_dir.relative_to(store.root_dir)),
            }
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=ToolError("concat failed"),
            ):
                multi_fail = runtime._finalize_segmented_audio_recording(multi_fail_record)

            empty_output_record = store.create_record(owner_email="owner@npt.sg", title="Empty Output", platform="zoom", meeting_link="")
            empty_output_dir = store.record_dir(empty_output_record["record_id"]) / "audio_segments"
            empty_output_dir.mkdir()
            (empty_output_dir / "segment-000000.wav").write_bytes(b"1" * 100)
            empty_output_record["media"] = {
                "audio_capture_profile": "segmented_avfoundation_v1",
                "audio_segment_dir": str(empty_output_dir.relative_to(store.root_dir)),
            }
            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                return_value=Mock(stdout="", stderr=""),
            ):
                empty_output = runtime._finalize_segmented_audio_recording(empty_output_record)

        self.assertIn("did not produce", missing_dir["media"]["audio_segment_warning"])
        self.assertIn("no usable", empty_dir["media"]["audio_segment_warning"])
        self.assertIn("ffmpeg is required", no_ffmpeg["media"]["audio_segment_warning"])
        self.assertEqual(single_copy["media"]["audio_path"], f"records/{single_copy_record['record_id']}/meeting.wav")
        self.assertNotIn("audio_segment_warning", single_copy["media"])
        self.assertIn("concat failed", multi_fail["media"]["audio_segment_warning"])
        self.assertIn("combined audio file is empty", empty_output["media"]["audio_segment_warning"])

    def test_audio_preflight_reports_unavailable_quiet_and_ok_states(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = MeetingRecordStore(Path(temp_dir))
            runtime = MeetingRecorderRuntime(store=store, config=MeetingRecorderConfig())

            with patch("bpmis_jira_tool.meeting_recorder._run_command", side_effect=ToolError("device busy")):
                unavailable = runtime._audio_preflight(ffmpeg_path="/opt/homebrew/bin/ffmpeg", audio_input="Microphone")
            with patch("bpmis_jira_tool.meeting_recorder._run_command", return_value=Mock(stdout="", stderr="")), patch(
                "bpmis_jira_tool.meeting_recorder._audio_volume_metrics",
                return_value={"mean_volume_db": -70.0, "max_volume_db": -60.0},
            ):
                quiet = runtime._audio_preflight(ffmpeg_path="/opt/homebrew/bin/ffmpeg", audio_input="Microphone")
            with patch("bpmis_jira_tool.meeting_recorder._run_command", return_value=Mock(stdout="", stderr="")), patch(
                "bpmis_jira_tool.meeting_recorder._audio_volume_metrics",
                return_value={"mean_volume_db": -25.0, "max_volume_db": -10.0},
            ):
                ok = runtime._audio_preflight(ffmpeg_path="/opt/homebrew/bin/ffmpeg", audio_input="Microphone")

        self.assertEqual(unavailable["status"], "unavailable")
        self.assertIn("device busy", unavailable["warning"])
        self.assertEqual(quiet["status"], "too_quiet")
        self.assertEqual(ok["status"], "ok")
        self.assertEqual(ok["warning"], "")

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
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["Meeting Recorder Aggregate"],
                },
            ), patch(
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
        self.assertNotIn(":BlackHole 2ch", command)

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
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["Meeting Recorder Aggregate"],
                },
            ), patch(
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
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["Meeting Recorder Aggregate"],
                },
            ), patch(
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

            segmented_record = store.create_record(owner_email="owner@npt.sg", title="Segmented", platform="zoom", meeting_link="")
            segmented_record.update(
                {
                    "status": "recording",
                    "recording_started_at": "2026-05-03T00:00:00+00:00",
                    "media": {"audio_capture_profile": "segmented_avfoundation_v1"},
                }
            )
            store.save_record(segmented_record)
            segmented_recorder = _SegmentedAudioRecorder(
                ffmpeg_path="/opt/homebrew/bin/ffmpeg",
                audio_input="Meeting Recorder Aggregate",
                segment_dir=root / "segments",
                log_path=root / "segmented.log",
                segment_seconds=30,
            )
            runtime._processes[segmented_record["record_id"]] = segmented_recorder

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
            ) as screen_stop, patch.object(
                _SegmentedAudioRecorder,
                "stop",
                return_value={"audio_segment_dir": "records/segmented/audio_segments"},
            ) as segmented_stop, patch(
                "bpmis_jira_tool.meeting_recorder._terminate_recorder_process",
            ) as terminate_process, patch.object(
                runtime,
                "_recording_health",
                return_value={"status": "ok", "checked_at": "2026-05-03T00:01:00+00:00", "warning": ""},
            ):
                stopped_screen = runtime.stop_recording(record_id=screen_record["record_id"], owner_email="owner@npt.sg")
                stopped_segmented = runtime.stop_recording(record_id=segmented_record["record_id"], owner_email="owner@npt.sg")
                stopped_process = runtime.stop_recording(record_id=process_record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(stopped_screen["media"]["screencapture_stop_status"], "stopped")
        self.assertEqual(stopped_segmented["media"]["audio_segment_dir"], "records/segmented/audio_segments")
        self.assertEqual(stopped_process["status"], "recorded")
        screen_stop.assert_called_once()
        segmented_stop.assert_called_once()
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
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["Meeting Recorder Aggregate"],
                },
            ), patch(
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

    def test_signal_check_waits_for_single_early_segment_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            segment = root / "segment-000000.wav"
            segment.write_bytes(b"0" * 8192)
            recorder = _SegmentedAudioRecorder(
                ffmpeg_path="/opt/homebrew/bin/ffmpeg",
                audio_input="Meeting Recorder Aggregate",
                segment_dir=root,
                log_path=root / "ffmpeg.log",
                segment_seconds=30,
            )
            fake_process = Mock()
            fake_process.poll.return_value = 255
            fake_thread = Mock()
            fake_thread.is_alive.return_value = True
            recorder._current_process = fake_process
            recorder._current_segment = segment
            recorder._thread = fake_thread

            with patch("bpmis_jira_tool.meeting_recorder.time.sleep"), patch(
                "bpmis_jira_tool.meeting_recorder._audio_duration_seconds",
                return_value=3.3,
            ):
                check = recorder.signal_snapshot(sample_seconds=1.0)

        self.assertEqual(check["status"], "pending")
        self.assertEqual(check["duration_seconds"], 3.3)
        self.assertIn("automatic restart", check["warning"])

    def test_signal_check_fails_after_repeated_early_segment_restarts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            recorder = _SegmentedAudioRecorder(
                ffmpeg_path="/opt/homebrew/bin/ffmpeg",
                audio_input="Meeting Recorder Aggregate",
                segment_dir=root,
                log_path=root / "ffmpeg.log",
                segment_seconds=30,
            )
            recorder._failures = [
                {"segment": "segment-000000.wav", "duration_seconds": 3.0},
                {"segment": "segment-000001.wav", "duration_seconds": 4.0},
            ]

            check = recorder.signal_snapshot(sample_seconds=1.0)

        self.assertEqual(check["status"], "failed")
        self.assertEqual(check["failure_count"], 2)
        self.assertIn("stopped repeatedly", check["warning"])

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

            unattached_segmented = store.create_record(owner_email="owner@npt.sg", title="Segmented", platform="zoom", meeting_link="")
            unattached_segmented.update(
                {
                    "status": "recording",
                    "media": {"audio_capture_profile": "segmented_avfoundation_v1"},
                }
            )
            store.save_record(unattached_segmented)

            attached_segmented = store.create_record(owner_email="owner@npt.sg", title="Attached Segmented", platform="zoom", meeting_link="")
            attached_segmented.update(
                {
                    "status": "recording",
                    "media": {"audio_capture_profile": "segmented_avfoundation_v1"},
                }
            )
            store.save_record(attached_segmented)
            segmented_recorder = _SegmentedAudioRecorder(
                ffmpeg_path="/opt/homebrew/bin/ffmpeg",
                audio_input="Meeting Recorder Aggregate",
                segment_dir=root / "segments",
                log_path=root / "segmented.log",
                segment_seconds=30,
            )
            runtime._processes[attached_segmented["record_id"]] = segmented_recorder

            failed_check = {
                "status": "failed",
                "checked_at": "2026-05-03T00:00:05+00:00",
                "warning": "audio is not growing",
            }
            with patch.object(_ScreenCaptureKitAudioRecorder, "signal_snapshot", return_value=failed_check), patch.object(
                _ScreenCaptureKitAudioRecorder,
                "stop",
                return_value={"screen_summary": "stopped"},
            ), patch.object(_SegmentedAudioRecorder, "signal_snapshot", return_value=failed_check), patch.object(
                _SegmentedAudioRecorder,
                "stop",
                return_value={"segment_summary": "stopped"},
            ):
                screen_missing = runtime.check_recording_signal(record_id=unattached_screen["record_id"], owner_email="owner@npt.sg")
                screen_failed = runtime.check_recording_signal(record_id=attached_screen["record_id"], owner_email="owner@npt.sg")
                segmented_missing = runtime.check_recording_signal(record_id=unattached_segmented["record_id"], owner_email="owner@npt.sg")
                segmented_failed = runtime.check_recording_signal(record_id=attached_segmented["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(screen_missing["status"], "failed")
        self.assertIn("not attached", screen_missing["recording_health"]["warning"])
        self.assertEqual(screen_failed["status"], "failed")
        self.assertEqual(screen_failed["media"]["screen_summary"], "stopped")
        self.assertEqual(segmented_missing["status"], "failed")
        self.assertIn("not attached", segmented_missing["recording_health"]["warning"])
        self.assertEqual(segmented_failed["status"], "failed")
        self.assertEqual(segmented_failed["media"]["segment_summary"], "stopped")

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
        self.assertIn("## Key Discussion Topics", text_client.calls[0]["user_prompt"])
        self.assertIn("- **Topic Name**", text_client.calls[0]["user_prompt"])
        self.assertIn("  - Factual discussion point grounded in the transcript.", text_client.calls[0]["user_prompt"])
        self.assertIn("  - [Follow up] Action, question, or check needed before the next discussion.", text_client.calls[0]["user_prompt"])
        self.assertIn("Do not return separate Summary, Decisions, Action Items", text_client.calls[0]["user_prompt"])
        self.assertIn("[Follow up]", text_client.calls[0]["system_prompt"])
        self.assertNotIn("Screen Evidence", text_client.calls[0]["user_prompt"])
        self.assertNotIn("keyframe", text_client.calls[0]["user_prompt"])
        self.assertNotIn("screen evidence", text_client.calls[0]["system_prompt"].lower())

    def test_meeting_minutes_markdown_to_html_renders_nested_bullets_and_escapes_text(self):
        html = _meeting_minutes_markdown_to_html(
            "## Key Discussion Topics\n"
            "- **Collection <Ownership>**\n"
            "  - [Follow up] Check `owner` & confirm.\n",
            portal_url="https://portal.example.test/meeting?x=1&y=2",
        )

        self.assertIn("<h3>Key Discussion Topics</h3>", html)
        self.assertIn("<strong>Collection &lt;Ownership&gt;</strong>", html)
        self.assertIn("[Follow up] Check <code>owner</code> &amp; confirm.", html)
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
        self.assertIn("# Chunk Summaries", text_client.calls[-1]["user_prompt"])
        self.assertNotIn("# Transcript\n", text_client.calls[-1]["user_prompt"])

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
                "markdown": "## Key Discussion Topics\n- **Cached**\n  - Existing minutes.",
                "prompt_version": "v3_topic_bullets_english_minutes",
                "generation_version": "v1_token_safe_chunked_minutes",
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
        self.assertEqual(processed["minutes"]["markdown"], "## Key Discussion Topics\n- **Cached**\n  - Existing minutes.")
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
                "prompt_version": "v3_topic_bullets_english_minutes",
                "generation_version": "v1_token_safe_chunked_minutes",
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
                "prompt_version": "v3_topic_bullets_english_minutes",
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
        self.assertEqual(processed["minutes"]["generation_version"], "v1_token_safe_chunked_minutes")

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
                "generation_version": "v1_token_safe_chunked_minutes",
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
        self.assertEqual(processed["minutes"]["prompt_version"], "v3_topic_bullets_english_minutes")

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
            self._login(client, email="owner@npt.sg")
            owner_response = client.get("/meeting-recorder", follow_redirects=False)

        self.assertEqual(admin_response.status_code, 200)
        self.assertIn(b"Meeting Recorder", admin_response.data)
        self.assertEqual(owner_response.status_code, 302)

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
