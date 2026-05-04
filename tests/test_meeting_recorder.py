import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.meeting_recorder import (
    CALENDAR_READONLY_SCOPE,
    MeetingProcessingService,
    MeetingRecorderConfig,
    MeetingRecorderRuntime,
    MeetingRecordStore,
    cleanup_legacy_video_assets,
    _audio_capture_status,
    _build_ffmpeg_audio_post_stop_pad_command,
    _build_ffmpeg_audio_recording_command,
    _build_ffmpeg_audio_segment_command,
    _build_ffmpeg_playback_repair_command,
    _build_ffmpeg_recording_command,
    _build_ffmpeg_screen_preflight_command,
    _effective_audio_input,
    _effective_recording_audio_input,
    _meeting_minutes_markdown_to_html,
    _parse_avfoundation_devices,
    _parse_srt_transcript,
    _SegmentedAudioRecorder,
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
        self.assertIn("--system-output", command)
        self.assertIn("--microphone-output", command)

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

    def test_linked_browser_audio_recording_health_fails_when_too_short(self):
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
            audio_path = store.record_dir(record["record_id"]) / "meeting.wav"
            audio_path.write_bytes(b"audio")
            record.update(
                {
                    "recording_started_at": "2026-05-03T09:39:46+00:00",
                    "recording_stopped_at": "2026-05-03T09:39:49+00:00",
                    "media": {
                        "audio_capture_profile": "browser_media_recorder_v1",
                        "browser_audio_capture_source": "browser_tab_audio_linked",
                        "recording_mode": "audio_only",
                        "audio_path": str(audio_path.relative_to(store.root_dir)),
                        "audio_original_duration_seconds": 2.58,
                    },
                }
            )

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=2.58), patch(
                "bpmis_jira_tool.meeting_recorder._audio_volume_metrics",
                return_value={"mean_volume_db": -33.0, "max_volume_db": -17.8},
            ):
                health = runtime._recording_health(record)

        self.assertEqual(health["status"], "failed")
        self.assertIn("lasted only 3s", health["warning"])
        self.assertIn("too short", health["warning"])

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

    def test_audio_only_stop_finalization_pads_short_audio_after_recording(self):
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

            def fake_run(command, *_args, **_kwargs):
                Path(command[-1]).write_bytes(b"padded-audio")
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder._audio_duration_seconds", return_value=11.0), patch(
                "bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin",
                return_value="/opt/homebrew/bin/ffmpeg",
            ), patch("bpmis_jira_tool.meeting_recorder._run_command", side_effect=fake_run) as run_command:
                finalized = runtime._finalize_audio_only_recording(record)

        media = finalized["media"]
        self.assertEqual(media["source_audio_path"], f"records/{record['record_id']}/meeting.wav")
        self.assertEqual(media["audio_path"], f"records/{record['record_id']}/meeting.padded.wav")
        self.assertEqual(media["audio_url"], f"/meeting-recorder/assets/{record['record_id']}/meeting.padded.wav")
        self.assertEqual(media["audio_finalization_profile"], "post_stop_pad_v1")
        self.assertEqual(media["audio_original_duration_seconds"], 11.0)
        self.assertEqual(media["audio_target_duration_seconds"], 43.0)
        self.assertEqual(run_command.call_args.args[0][run_command.call_args.args[0].index("-af") + 1], "apad=whole_dur=43.000")

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
                    recording_mode="screen_audio",
                )

        self.assertEqual(record["media"]["recording_mode"], "audio_only")
        self.assertEqual(record["media"]["audio_capture_profile"], "screencapturekit_audio_v1")
        self.assertIn("audio_path", record["media"])
        self.assertNotIn("video_path", record["media"])
        self.assertEqual(record["diagnostics_snapshot"]["requested_recording_mode"], "screen_audio")
        command = runtime._processes[record["record_id"]].command
        self.assertIn("--system-output", command)
        self.assertIn("--microphone-output", command)
        self.assertNotIn(":BlackHole 2ch", command)

    def test_linked_meeting_audio_only_never_calls_legacy_screen_preflight(self):
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

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._avfoundation_devices",
                return_value={
                    "video_devices": ["Capture screen 0"],
                    "audio_devices": ["Meeting Recorder Aggregate"],
                },
            ), patch.object(
                runtime,
                "_screen_capture_preflight",
            ) as screen_preflight, patch(
                "bpmis_jira_tool.meeting_recorder._resolve_screencapturekit_helper",
                return_value=Path("/tmp/meeting-screencapture-helper"),
            ), patch(
                "bpmis_jira_tool.meeting_recorder._ScreenCaptureKitAudioRecorder.start",
                return_value={"status": "ok", "latency_seconds": 0.2, "bytes": 48000, "pid": 1234},
            ):
                runtime.start_recording(
                    owner_email="owner@npt.sg",
                    title="Zoom review",
                    platform="zoom",
                    meeting_link="https://zoom.us/j/123",
                    recording_mode="screen_audio",
                )

        screen_preflight.assert_not_called()

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

    def test_repair_video_playback_is_unsupported(self):
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

            with self.assertRaisesRegex(Exception, "audio-only"):
                runtime.repair_video_playback(record_id=record["record_id"], owner_email="owner@npt.sg")


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

    def test_cleanup_legacy_video_assets_extracts_audio_and_removes_video_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = MeetingRecordStore(root)
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Legacy Zoom",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record_dir = store.record_dir(record["record_id"])
            video_path = record_dir / "meeting.mp4"
            playback_path = record_dir / "meeting.playback.mp4"
            keyframe_dir = record_dir / "keyframes"
            keyframe_dir.mkdir()
            (keyframe_dir / "frame-0001.jpg").write_bytes(b"jpg")
            video_path.write_bytes(b"video")
            playback_path.write_bytes(b"playback")
            record["media"] = {
                "recording_mode": "screen_audio",
                "video_path": str(video_path.relative_to(root)),
                "video_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.mp4",
                "playback_video_path": str(playback_path.relative_to(root)),
                "playback_video_url": f"/meeting-recorder/assets/{record['record_id']}/meeting.playback.mp4",
            }
            record["visual_evidence"] = [{"image_url": "x"}]
            store.save_record(record)

            def fake_run(command, *_args, **_kwargs):
                Path(command[-1]).write_bytes(b"audio")
                return Mock(stdout="", stderr="")

            with patch("bpmis_jira_tool.meeting_recorder._resolve_ffmpeg_bin", return_value="/opt/homebrew/bin/ffmpeg"), patch(
                "bpmis_jira_tool.meeting_recorder._run_command",
                side_effect=fake_run,
            ):
                summary = cleanup_legacy_video_assets(
                    store=store,
                    config=MeetingRecorderConfig(ffmpeg_bin="/opt/homebrew/bin/ffmpeg"),
                )

            cleaned = store.get_record(record["record_id"])
            self.assertEqual(summary["updated"], 1)
            self.assertEqual(summary["audio_extracted"], 1)
            self.assertEqual(cleaned["media"]["recording_mode"], "audio_only")
            self.assertEqual(cleaned["media"]["audio_path"], f"records/{record['record_id']}/meeting.wav")
            self.assertNotIn("video_path", cleaned["media"])
            self.assertNotIn("playback_video_path", cleaned["media"])
            self.assertEqual(cleaned["visual_evidence"], [])
            self.assertFalse(video_path.exists())
            self.assertFalse(playback_path.exists())
            self.assertFalse(keyframe_dir.exists())

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
        self.assertEqual(len(whisper_calls), 3)
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

    def test_repair_video_route_is_unsupported_for_admin_and_blocks_non_admin(self):
        fake_runtime = Mock()
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        with self.app.test_client() as client:
            self._login(client, email="owner@npt.sg")
            denied = client.post("/api/meeting-recorder/records/meeting-1/repair-video")
            self._login(client, email="xiaodong.zheng@npt.sg")
            response = client.post("/api/meeting-recorder/records/meeting-1/repair-video")

        self.assertEqual(denied.status_code, 403)
        self.assertEqual(response.status_code, 400)
        self.assertIn("audio-only", response.get_json()["message"])
        fake_runtime.repair_video_playback.assert_not_called()

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

    def test_repair_video_route_does_not_delegate_to_local_agent_when_configured(self):
        fake_client = Mock()

        with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True):
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
                with self.app.test_client() as client:
                    self._login(client, email="xiaodong.zheng@npt.sg")
                    response = client.post("/api/meeting-recorder/records/meeting-1/repair-video")

        self.assertEqual(response.status_code, 400)
        self.assertIn("audio-only", response.get_json()["message"])
        fake_client.meeting_recorder_repair_video.assert_not_called()

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
        self.assertIn("Mixed Chinese/English", template)
        self.assertIn("English", template)
        self.assertIn("Chinese", template)
        self.assertNotIn("screen_audio", source)
        self.assertNotIn("/repair-video", source)
        self.assertIn("Transcript may be incomplete", source)
        self.assertIn("low_audio", source)
        self.assertIn("repeated chunk", source)
        self.assertIn('data-meeting-record-date', template)
        self.assertNotIn("data-meeting-stop-current", template)
        self.assertIn("recordDateValue(record) === selectedDate", source)
        self.assertIn("No meeting recordings on", source)

    def test_screencapturekit_helper_has_no_fixed_recording_duration(self):
        source = Path("tools/meeting_screencapture_helper.swift").read_text(encoding="utf-8")
        runtime_source = Path("bpmis_jira_tool/meeting_recorder.py").read_text(encoding="utf-8")

        self.assertIn("await withCheckedContinuation", source)
        self.assertIn("try await stream.stopCapture()", source)
        self.assertNotIn("asyncAfter", source)
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
                process = client.post(f"/api/meeting-recorder/records/{record_id}/process")
                process_payload = process.get_json()
                process_status = self._wait_for_process_job(client, process_payload["job_id"])
                email = client.post(f"/api/meeting-recorder/records/{record_id}/send-email", json={})

        self.assertEqual(start.status_code, 200)
        self.assertEqual(stop.status_code, 200)
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
        fake_processing.process_recording.assert_called_once_with(record_id=record_id, owner_email="xiaodong.zheng@npt.sg")
        fake_processing.send_minutes_email.assert_called_once()

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

    def test_stale_processing_record_can_be_requeued(self):
        record = self.app.config["MEETING_RECORD_STORE"].create_record(
            owner_email="xiaodong.zheng@npt.sg",
            title="Stale Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/stale",
        )
        record["status"] = "processing"
        self.app.config["MEETING_RECORD_STORE"].save_record(record)
        fake_processing = Mock()
        fake_processing.process_recording.return_value = {
            "record_id": record["record_id"],
            "title": "Stale Review",
            "platform": "zoom",
            "status": "completed",
        }

        with patch("bpmis_jira_tool.web._build_meeting_processing_service", return_value=fake_processing):
            with self.app.test_client() as client:
                self._login(client, email="xiaodong.zheng@npt.sg")
                response = client.post(f"/api/meeting-recorder/records/{record['record_id']}/process")
                payload = response.get_json()
                completed = self._wait_for_process_job(client, payload["job_id"])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(completed["state"], "completed")
        fake_processing.process_recording.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="xiaodong.zheng@npt.sg",
        )

    def test_meeting_recorder_script_polls_process_jobs_and_uses_clear_errors(self):
        source = Path("static/meeting_recorder.js").read_text(encoding="utf-8")

        self.assertIn("/api/meeting-recorder/process-jobs/", source)
        self.assertIn("pollMeetingProcessJob", source)
        self.assertIn("Meeting processing failed.", source)
        self.assertIn("Meeting processing is still running.", source)
        self.assertIn("Connection interrupted. Refreshing status...", source)
        self.assertIn("isNetworkError", source)
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
