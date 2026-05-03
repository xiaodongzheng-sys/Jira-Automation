import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from bpmis_jira_tool.meeting_recorder import (
    CALENDAR_READONLY_SCOPE,
    MeetingProcessingService,
    MeetingRecorderConfig,
    MeetingRecorderRuntime,
    MeetingRecordStore,
    _audio_capture_status,
    _build_ffmpeg_audio_recording_command,
    _build_ffmpeg_playback_repair_command,
    _build_ffmpeg_recording_command,
    _build_ffmpeg_screen_preflight_command,
    _effective_audio_input,
    _effective_recording_audio_input,
    _parse_avfoundation_devices,
    _parse_srt_transcript,
    extract_meeting_links,
    meeting_platform_from_link,
    meeting_reminder_suppression_key,
    normalize_calendar_event,
    reminder_eligible_meetings,
)


class MeetingRecorderParsingTests(unittest.TestCase):
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
                recording_mode="screen_audio",
                meeting_link="https://zoom.us/j/123",
            ),
            "Meeting Recorder Aggregate",
        )

    def test_ffmpeg_recording_command_uses_browser_safe_video_encoding(self):
        command = _build_ffmpeg_recording_command(
            ffmpeg_path="/opt/homebrew/bin/ffmpeg",
            video_input="Capture screen 0",
            audio_input="default",
            video_path=Path("/tmp/meeting.mp4"),
            video_fps=15,
            video_max_width=1920,
            video_max_height=1080,
            avfoundation_pixel_format="bgr0",
        )

        self.assertIn("-pixel_format", command)
        self.assertEqual(command[command.index("-pixel_format") + 1], "bgr0")
        self.assertIn("Capture screen 0:", command)
        self.assertIn(":default", command)
        self.assertNotIn("Capture screen 0:default", command)
        self.assertIn("-map", command)
        self.assertIn("-vf", command)
        self.assertEqual(
            command[command.index("-vf") + 1],
            "scale='if(gt(iw/ih,1920/1080),min(1920,iw),-2)':'if(gt(iw/ih,1920/1080),-2,min(1080,ih))':flags=bicubic,fps=15,format=yuv420p",
        )
        self.assertEqual(command[command.index("-profile:v") + 1], "high")
        self.assertEqual(command[command.index("-level") + 1], "4.2")
        self.assertEqual(command[command.index("-pix_fmt") + 1], "yuv420p")
        self.assertIn("-c:a", command)
        self.assertEqual(command[command.index("-c:a") + 1], "aac")
        self.assertIn("-ac", command)
        self.assertEqual(command[command.index("-ac") + 1], "2")
        self.assertIn("-ar", command)
        self.assertEqual(command[command.index("-ar") + 1], "48000")

    def test_ffmpeg_screen_preflight_command_writes_video_file(self):
        command = _build_ffmpeg_screen_preflight_command(
            ffmpeg_path="/opt/homebrew/bin/ffmpeg",
            video_input="Capture screen 0",
            video_path=Path("/tmp/preflight.mp4"),
            avfoundation_pixel_format="bgr0",
        )

        self.assertIn("Capture screen 0:", command)
        self.assertIn("-frames:v", command)
        self.assertIn("-an", command)
        self.assertEqual(command[-1], "/tmp/preflight.mp4")

    def test_screen_preflight_uses_configured_timeout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = MeetingRecorderRuntime(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(screen_preflight_timeout_seconds=30),
            )

            def fake_run(command, *_args, **_kwargs):
                Path(command[-1]).write_bytes(b"video")
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder._run_command", side_effect=fake_run) as run_command:
                result = runtime._screen_capture_preflight(ffmpeg_path="/opt/homebrew/bin/ffmpeg")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(run_command.call_args.kwargs["timeout_seconds"], 30)

    def test_ffmpeg_audio_recording_command_uses_audio_input_only(self):
        command = _build_ffmpeg_audio_recording_command(
            ffmpeg_path="/opt/homebrew/bin/ffmpeg",
            audio_input="Meeting Recorder Aggregate",
            audio_path=Path("/tmp/meeting.wav"),
        )

        self.assertIn("-i", command)
        self.assertIn("-use_wallclock_as_timestamps", command)
        self.assertEqual(command[command.index("-use_wallclock_as_timestamps") + 1], "1")
        self.assertEqual(command[command.index("-i") + 1], ":Meeting Recorder Aggregate")
        self.assertIn("-af", command)
        self.assertEqual(command[command.index("-af") + 1], "aresample=async=1:first_pts=0")
        self.assertNotIn("Capture screen 0", command)
        self.assertNotIn("-framerate", command)
        self.assertNotIn("-c:v", command)
        self.assertIn("-acodec", command)
        self.assertEqual(command[command.index("-acodec") + 1], "pcm_s16le")
        self.assertEqual(command[-1], "/tmp/meeting.wav")

    def test_ffmpeg_playback_repair_command_copies_video_and_rebuilds_stereo_audio(self):
        command = _build_ffmpeg_playback_repair_command(
            ffmpeg_path="/opt/homebrew/bin/ffmpeg",
            source_path=Path("/tmp/meeting.mp4"),
            output_path=Path("/tmp/meeting.playback.mp4"),
        )

        self.assertEqual(command[command.index("-c:v") + 1], "copy")
        self.assertEqual(command[command.index("-c:a") + 1], "aac")
        self.assertEqual(command[command.index("-ac") + 1], "2")
        self.assertEqual(command[command.index("-ar") + 1], "48000")
        self.assertEqual(command[command.index("-movflags") + 1], "+faststart")

    def test_reminder_eligible_meetings_filters_by_window_hours_and_platform(self):
        now = datetime(2026, 5, 4, 9, 58, tzinfo=ZoneInfo("Asia/Singapore"))
        meetings = [
            {
                "calendar_event_id": "meet-soon",
                "title": "Record me",
                "platform": "google_meet",
                "start": "2026-05-04T10:00:00+08:00",
                "meeting_link": "https://meet.google.com/abc-defg-hij",
            },
            {
                "calendar_event_id": "too-early",
                "title": "Too early",
                "platform": "zoom",
                "start": "2026-05-04T08:59:00+08:00",
                "meeting_link": "https://zoom.us/j/123",
            },
            {
                "calendar_event_id": "too-late",
                "title": "Too late",
                "platform": "google_meet",
                "start": "2026-05-04T20:00:00+08:00",
                "meeting_link": "https://meet.google.com/late-one",
            },
            {
                "calendar_event_id": "not-meeting",
                "title": "Office",
                "platform": "unknown",
                "start": "2026-05-04T10:00:00+08:00",
                "meeting_link": "https://example.com",
            },
            {
                "calendar_event_id": "future",
                "title": "Future",
                "platform": "zoom",
                "start": "2026-05-04T10:04:00+08:00",
                "meeting_link": "https://zoom.us/j/456",
            },
        ]

        eligible = reminder_eligible_meetings(meetings, now=now)

        self.assertEqual([item["calendar_event_id"] for item in eligible], ["meet-soon"])
        self.assertEqual(eligible[0]["seconds_until_start"], 120)
        self.assertEqual(eligible[0]["suppression_key"], "20260504:meet-soon")

    def test_reminder_eligible_meetings_keeps_grace_window_after_start(self):
        now = datetime(2026, 5, 4, 10, 9, tzinfo=ZoneInfo("Asia/Singapore"))
        meetings = [
            {
                "calendar_event_id": "within-grace",
                "title": "Within grace",
                "platform": "zoom",
                "start": "2026-05-04T10:00:00+08:00",
                "meeting_link": "https://zoom.us/j/123",
            },
            {
                "calendar_event_id": "outside-grace",
                "title": "Outside grace",
                "platform": "zoom",
                "start": "2026-05-04T09:58:00+08:00",
                "meeting_link": "https://zoom.us/j/456",
            },
        ]

        eligible = reminder_eligible_meetings(meetings, now=now)

        self.assertEqual([item["calendar_event_id"] for item in eligible], ["within-grace"])
        self.assertEqual(eligible[0]["seconds_until_start"], -540)

    def test_meeting_reminder_suppression_key_is_stable_by_event_and_date(self):
        meeting = {
            "calendar_event_id": "event/with spaces",
            "title": "Review",
            "platform": "google_meet",
            "start": "2026-05-04T10:00:00+08:00",
        }

        self.assertEqual(meeting_reminder_suppression_key(meeting), "20260504:event-with-spaces")


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
    def test_audio_only_blank_link_prefers_microphone_over_aggregate(self):
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
            fake_process = Mock()
            fake_process.pid = 12345
            fake_process.poll.return_value = None

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["MacBook Air Microphone", "Meeting Recorder Aggregate"],
                },
            ), patch.object(
                runtime,
                "_audio_preflight",
                return_value={"status": "ok", "checked_at": "2026-05-02T10:00:00+00:00", "warning": ""},
            ), patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.Popen",
                return_value=fake_process,
            ) as popen:
                record = runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Face to face",
                    platform="unknown",
                    meeting_link="",
                    recording_mode="audio_only",
                )

        self.assertEqual(record["media"]["recording_mode"], "audio_only")
        self.assertEqual(record["media"]["recorder_pid"], 12345)
        self.assertEqual(record["diagnostics_snapshot"]["audio_input"], "MacBook Air Microphone")
        self.assertEqual(record["diagnostics_snapshot"]["configured_audio_input"], "Meeting Recorder Aggregate")
        command = popen.call_args.args[0]
        self.assertIn(":MacBook Air Microphone", command)
        self.assertNotIn(":Meeting Recorder Aggregate", command)

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

        self.assertEqual(health["status"], "warning")
        self.assertEqual(health["duration_seconds"], 16.0)
        self.assertEqual(health["elapsed_seconds"], 61.0)
        self.assertIn("only 16s for a 61s recording", health["warning"])

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

    def test_screen_recording_falls_back_to_audio_only_when_screen_capture_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )
            fake_process = Mock()
            fake_process.poll.return_value = None

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["Meeting Recorder Aggregate"],
                },
            ), patch.object(
                runtime,
                "_audio_preflight",
                return_value={"status": "ok", "checked_at": "2026-05-02T10:00:00+00:00", "warning": ""},
            ), patch.object(
                runtime,
                "_screen_capture_preflight",
                return_value={"status": "unavailable", "warning": "screen failed"},
            ), patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.Popen",
                return_value=fake_process,
            ) as popen:
                record = runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Zoom review",
                    platform="zoom",
                    meeting_link="https://zoom.us/j/123",
                    recording_mode="screen_audio",
                )

        self.assertEqual(record["media"]["recording_mode"], "audio_only")
        self.assertIn("audio_path", record["media"])
        self.assertNotIn("video_path", record["media"])
        self.assertEqual(record["diagnostics_snapshot"]["requested_recording_mode"], "screen_audio")
        self.assertEqual(record["diagnostics_snapshot"]["screen_capture_status"], "unavailable")
        self.assertIn("Continuing with audio-only", record["diagnostics_snapshot"]["screen_capture_warning"])
        command = popen.call_args.args[0]
        self.assertIn(":Meeting Recorder Aggregate", command)
        self.assertNotIn("Capture screen 0:", command)

    def test_screen_recording_can_disable_audio_only_fallback_when_screen_capture_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(
                    ffmpeg_bin="/opt/homebrew/bin/ffmpeg",
                    audio_only_fallback_on_screen_failure=False,
                ),
            )
            fake_process = Mock()
            fake_process.poll.return_value = None

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["Meeting Recorder Aggregate"],
                },
            ), patch.object(
                runtime,
                "_audio_preflight",
                return_value={"status": "ok", "checked_at": "2026-05-02T10:00:00+00:00", "warning": ""},
            ), patch.object(
                runtime,
                "_screen_capture_preflight",
                return_value={"status": "unavailable", "warning": "screen failed"},
            ), patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.Popen",
                return_value=fake_process,
            ) as popen:
                with self.assertRaisesRegex(Exception, "Screen recording is required"):
                    runtime.start_recording(
                        owner_email="owner@npt.sg",
                        title="Zoom review",
                        platform="zoom",
                        meeting_link="https://zoom.us/j/123",
                        recording_mode="screen_audio",
                    )

        popen.assert_not_called()
        self.assertEqual(store.list_records(owner_email="owner@npt.sg"), [])

    def test_screen_recording_start_keeps_video_mode_when_screen_capture_passes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )
            fake_process = Mock()
            fake_process.poll.return_value = None

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["Meeting Recorder Aggregate"],
                },
            ), patch.object(
                runtime,
                "_audio_preflight",
                return_value={"status": "ok", "checked_at": "2026-05-02T10:00:00+00:00", "warning": ""},
            ), patch.object(
                runtime,
                "_screen_capture_preflight",
                return_value={"status": "ok", "checked_at": "2026-05-02T10:00:00+00:00", "warning": ""},
            ), patch(
                "bpmis_jira_tool.meeting_recorder.subprocess.Popen",
                return_value=fake_process,
            ) as popen:
                record = runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Zoom review",
                    platform="zoom",
                    meeting_link="https://zoom.us/j/123",
                    recording_mode="screen_audio",
                )

        self.assertEqual(record["media"]["recording_mode"], "screen_audio")
        self.assertIn("video_path", record["media"])
        self.assertNotIn("audio_path", record["media"])
        self.assertEqual(record["diagnostics_snapshot"]["screen_capture_status"], "ok")
        command = popen.call_args.args[0]
        self.assertIn("Capture screen 0:", command)
        self.assertIn(":Meeting Recorder Aggregate", command)
        self.assertNotIn("Capture screen 0:Meeting Recorder Aggregate", command)

    def test_repair_video_playback_creates_browser_playback_asset(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            video_path = store.record_dir(record["record_id"]) / "meeting.mp4"
            video_path.write_bytes(b"source-video")
            record["media"] = {
                "video_path": str(video_path.relative_to(root)),
                "video_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.mp4",
            }
            store.save_record(record)
            runtime = MeetingRecorderRuntime(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
            )

            def fake_run(command, *_args, **_kwargs):
                Path(command[-1]).write_bytes(b"playback-video")
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=fake_run,
            ) as run_command:
                repaired = runtime.repair_video_playback(record_id=record["record_id"], owner_email="owner@npt.sg")

        media = repaired["media"]
        self.assertEqual(media["playback_video_url"], f"/meeting-recorder/assets/{record['record_id']}/meeting.playback.mp4")
        self.assertEqual(media["playback_profile"], "browser_compatible_v1")
        self.assertEqual(media["playback_audio_channels"], 2)
        repair_command = run_command.call_args.args[0]
        self.assertEqual(repair_command[repair_command.index("-c:v") + 1], "copy")
        self.assertEqual(repair_command[repair_command.index("-ac") + 1], "2")


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
        self.assertNotIn("Screen Evidence", text_client.calls[0]["user_prompt"])
        self.assertNotIn("keyframe", text_client.calls[0]["user_prompt"])
        self.assertNotIn("screen evidence", text_client.calls[0]["system_prompt"].lower())

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
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(),
                text_client=FakeTextClient(),
            )

            with patch.object(service, "_transcribe_audio", return_value={"text": "Alice approved.", "chunks": [], "segments": [], "quality": {}}) as transcribe:
                with patch.object(service, "_extract_audio") as extract_audio:
                    with patch.object(service, "_extract_visual_evidence") as visual:
                        processed = service.process_recording(record_id=record["record_id"], owner_email="owner@npt.sg")

        self.assertEqual(processed["status"], "completed")
        self.assertEqual(transcribe.call_args.args[0], audio_path.resolve())
        extract_audio.assert_not_called()
        visual.assert_not_called()
        self.assertEqual(processed["visual_evidence"], [])
        self.assertEqual(processed["transcript"]["text"], "Alice approved.")

    def test_extract_audio_preserves_sparse_meeting_audio_timeline(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Zoom",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            video_path = store.record_dir(record["record_id"]) / "meeting.mp4"
            video_path.write_bytes(b"video")
            service = MeetingProcessingService(
                store=store,
                config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
                text_client=FakeTextClient(),
            )

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command"
            ) as run_command:
                audio_path = service._extract_audio(record, video_path)

        command = run_command.call_args.args[0]
        self.assertEqual(audio_path, store.record_dir(record["record_id"]) / "audio.wav")
        self.assertIn("-fflags", command)
        self.assertEqual(command[command.index("-fflags") + 1], "+genpts")
        self.assertIn("-map", command)
        self.assertEqual(command[command.index("-map") + 1], "0:a:0")
        self.assertIn("-af", command)
        self.assertEqual(command[command.index("-af") + 1], "aresample=async=1:first_pts=0")

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
            record["minutes"] = {"status": "completed", "markdown": "## Summary\nApproved."}
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
                config=MeetingRecorderConfig(whisper_cpp_bin="whisper-cli", whisper_model=str(model_path)),
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
        self.assertIn("/usr/local/bin/whisper-cli", command)
        self.assertIn(str(model_path), command)
        self.assertIn("-osrt", command)
        self.assertIn("-l", command)
        self.assertIn("auto", command)

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
                config=MeetingRecorderConfig(whisper_cpp_bin="whisper-cli", whisper_model=str(model_path)),
                text_client=FakeTextClient(),
            )

            def fake_run(command, *_args, **_kwargs):
                if "-of" in command:
                    output_base = Path(command[command.index("-of") + 1])
                    index = int(output_base.name.rsplit("-", 1)[-1])
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
        self.assertEqual(len(whisper_calls), 3)
        self.assertIn("中文内容", transcript["text"])
        self.assertIn("English content", transcript["text"])
        self.assertEqual(transcript["segments"][1]["language"], "zh")
        self.assertEqual(transcript["chunks"][1]["start_seconds"], 60.0)

    def test_transcribe_rejects_non_whisper_provider(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MeetingProcessingService(
                store=MeetingRecordStore(Path(temp_dir)),
                config=MeetingRecorderConfig(transcribe_provider="openai"),
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

    def test_non_admin_cannot_access_reminders_api(self):
        with self.app.test_client() as client:
            self._login(client, email="owner@npt.sg", scopes=[CALENDAR_READONLY_SCOPE])
            response = client.get("/api/meeting-recorder/reminders")

        self.assertEqual(response.status_code, 403)

    def test_reminders_api_returns_eligible_meetings_and_active_recording(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        active = store.create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Current",
            platform="google_meet",
            meeting_link="https://meet.google.com/current",
        )
        active["status"] = "recording"
        store.save_record(active)
        fixed_now = datetime(2026, 5, 2, 12, 38, 30, tzinfo=ZoneInfo("Asia/Singapore"))
        start = fixed_now + timedelta(seconds=90)

        fake_calendar = Mock()
        fake_calendar.upcoming_meetings.return_value = [
            {
                "calendar_event_id": "event-1",
                "title": "Upcoming",
                "platform": "google_meet",
                "start": start.isoformat(),
                "end": (start + timedelta(minutes=30)).isoformat(),
                "meeting_link": "https://meet.google.com/abc-defg-hij",
            }
        ]

        with patch("bpmis_jira_tool.web._build_calendar_meeting_service", return_value=fake_calendar), patch(
            "bpmis_jira_tool.web.reminder_eligible_meetings",
            side_effect=lambda meetings, **kwargs: reminder_eligible_meetings(meetings, now=fixed_now, **kwargs),
        ), patch(
            "bpmis_jira_tool.web._meeting_recorder_diagnostics_payload",
            return_value={"audio_capture_label": "Aggregate device configured", "system_audio_configured": True},
        ):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg", scopes=[CALENDAR_READONLY_SCOPE])
                response = client.get("/api/meeting-recorder/reminders")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["calendar_connected"])
        self.assertEqual(payload["debug"]["reason"], "active_recording")
        self.assertEqual(payload["meetings"][0]["calendar_event_id"], "event-1")
        self.assertEqual(payload["meetings"][0]["suppression_key"].split(":", 1)[1], "event-1")
        self.assertEqual(payload["active_recording"]["record_id"], active["record_id"])
        self.assertEqual(payload["diagnostics"]["audio_capture_label"], "Aggregate device configured")

    def test_reminders_api_returns_debug_when_calendar_not_connected(self):
        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg", scopes=[])
            response = client.get("/api/meeting-recorder/reminders")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertFalse(payload["calendar_connected"])
        self.assertEqual(payload["debug"]["reason"], "calendar_not_connected")

    def test_reminder_telemetry_endpoint_logs_compact_event(self):
        with patch("bpmis_jira_tool.web._log_portal_event") as log_event:
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                response = client.post(
                    "/api/meeting-recorder/reminder-telemetry",
                    json={
                        "event": "poll_success",
                        "reason": "visible",
                        "meeting_count": 1,
                        "suppressed_count": 2,
                        "active_recording": False,
                        "page_path": "/",
                    },
                )

        self.assertEqual(response.status_code, 200)
        log_event.assert_called_once()
        logged = log_event.call_args.kwargs
        self.assertEqual(logged["telemetry_event"], "poll_success")
        self.assertEqual(logged["reason"], "visible")
        self.assertEqual(logged["meeting_count"], 1)

    def test_repair_video_route_uses_runtime_for_admin_and_blocks_non_admin(self):
        fake_runtime = Mock()
        fake_runtime.repair_video_playback.return_value = {
            "record_id": "meeting-1",
            "title": "Review",
            "platform": "zoom",
            "meeting_link": "https://zoom.us/j/123",
            "status": "completed",
            "media": {"playback_video_url": "/meeting-recorder/assets/meeting-1/meeting.playback.mp4"},
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        with self.app.test_client() as client:
            self._login(client, email="owner@npt.sg")
            denied = client.post("/api/meeting-recorder/records/meeting-1/repair-video")
            self._login(client, email="xiaodong.zheng@npt.sg")
            response = client.post("/api/meeting-recorder/records/meeting-1/repair-video")

        self.assertEqual(denied.status_code, 403)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["record"]["media"]["playback_video_url"], "/meeting-recorder/assets/meeting-1/meeting.playback.mp4")
        fake_runtime.repair_video_playback.assert_called_once_with(record_id="meeting-1", owner_email="xiaodong.zheng@npt.sg")

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

    def test_repair_video_route_delegates_to_local_agent_when_configured(self):
        fake_client = Mock()
        fake_client.meeting_recorder_repair_video.return_value = {
            "record": {
                "record_id": "meeting-1",
                "media": {"playback_video_url": "/meeting-recorder/assets/meeting-1/meeting.playback.mp4"},
            }
        }

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    response = client.post("/api/meeting-recorder/records/meeting-1/repair-video")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["record"]["media"]["playback_video_url"], "/meeting-recorder/assets/meeting-1/meeting.playback.mp4")
        fake_client.meeting_recorder_repair_video.assert_called_once_with(record_id="meeting-1", owner_email="xiaodong.zheng@npt.sg")

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

    def test_base_template_renders_meeting_indicator_and_reminder_script(self):
        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg", scopes=[CALENDAR_READONLY_SCOPE])
            response = client.get("/meeting-recorder")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"data-meeting-recorder-indicator", response.data)
        self.assertIn(b"meeting_recorder_reminder.js", response.data)

    def test_reminder_script_polls_on_visibility_focus_and_reports_telemetry(self):
        source = Path("static/meeting_recorder_reminder.js").read_text(encoding="utf-8")

        self.assertIn("visibilitychange", source)
        self.assertIn("window.addEventListener('focus'", source)
        self.assertIn("/api/meeting-recorder/reminder-telemetry", source)
        self.assertIn("poll_success", source)
        self.assertIn("readSuppressed", source)

    def test_meeting_recorder_script_reports_video_status_and_transcript_quality(self):
        source = Path("static/meeting_recorder.js").read_text(encoding="utf-8")

        self.assertIn("Download video file", source)
        self.assertIn("Download audio file", source)
        self.assertIn("Audio download will be available after stopping the recording.", source)
        self.assertIn("Download transcript", source)
        self.assertIn("download=1", source)
        self.assertIn("data-record-download-asset", source)
        self.assertIn("Download returned an HTML page", source)
        self.assertIn("Checking microphone/audio input", source)
        self.assertIn("Starting...", source)
        self.assertIn("audio_only", source)
        self.assertIn("screen_audio", source)
        self.assertIn("recording_mode: meetingLink ? 'screen_audio' : 'audio_only'", source)
        self.assertIn("Build downloadable playback copy", source)
        self.assertIn("/repair-video", source)
        self.assertIn("Transcript may be incomplete", source)
        self.assertIn("low_audio", source)
        self.assertIn("repeated chunk", source)

    def test_start_stop_process_and_email_routes_delegate_to_services(self):
        fake_runtime = Mock()
        fake_runtime.start_recording.return_value = {
            "record_id": "meeting-1",
            "title": "Review",
            "platform": "zoom",
            "meeting_link": "https://zoom.us/j/123",
            "status": "recording",
        }
        fake_runtime.stop_recording.return_value = {
            "record_id": "meeting-1",
            "title": "Review",
            "platform": "zoom",
            "meeting_link": "https://zoom.us/j/123",
            "status": "recorded",
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        fake_processing = Mock()
        fake_processing.process_recording.return_value = {
            "record_id": "meeting-1",
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
                    },
                )
                stop = client.post("/api/meeting-recorder/records/meeting-1/stop")
                process = client.post("/api/meeting-recorder/records/meeting-1/process")
                email = client.post("/api/meeting-recorder/records/meeting-1/send-email", json={})

        self.assertEqual(start.status_code, 200)
        self.assertEqual(stop.status_code, 200)
        self.assertEqual(process.status_code, 200)
        self.assertEqual(email.status_code, 200)
        fake_runtime.start_recording.assert_called_once_with(
            owner_email="xiaodong.zheng@npt.sg",
            title="Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/123",
            recording_mode="screen_audio",
            calendar_event_id="event-1",
            scheduled_start="2026-05-04T10:00:00+08:00",
            scheduled_end="2026-05-04T10:30:00+08:00",
            attendees=[{"email": "alice@npt.sg"}],
        )
        fake_runtime.stop_recording.assert_called_once_with(record_id="meeting-1", owner_email="xiaodong.zheng@npt.sg")
        fake_processing.process_recording.assert_called_once_with(record_id="meeting-1", owner_email="xiaodong.zheng@npt.sg")
        fake_processing.send_minutes_email.assert_called_once()

    def test_manual_empty_link_start_route_defaults_to_audio_only(self):
        fake_runtime = Mock()
        fake_runtime.start_recording.return_value = {
            "record_id": "meeting-2",
            "title": "Face to face",
            "platform": "unknown",
            "meeting_link": "",
            "status": "recording",
            "media": {"recording_mode": "audio_only"},
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        with self.app.test_client() as client:
            self._login(client, email="xiaodong.zheng@npt.sg")
            response = client.post(
                "/api/meeting-recorder/start",
                json={"title": "Face to face", "meeting_link": "", "recording_mode": "audio_only"},
            )

        self.assertEqual(response.status_code, 200)
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
        )


if __name__ == "__main__":
    unittest.main()
