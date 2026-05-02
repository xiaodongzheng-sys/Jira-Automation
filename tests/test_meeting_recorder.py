import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from bpmis_jira_tool.meeting_recorder import (
    MeetingProcessingService,
    MeetingRecorderConfig,
    MeetingRecordStore,
    _audio_capture_status,
    _effective_audio_input,
    _parse_avfoundation_devices,
    _parse_srt_transcript,
    extract_meeting_links,
    meeting_platform_from_link,
    normalize_calendar_event,
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
        self.assertTrue(_audio_capture_status("Meeting Recorder Aggregate", devices["audio_devices"])["system_audio_configured"])


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
        command = run_command.call_args.args[0]
        self.assertIn("/usr/local/bin/whisper-cli", command)
        self.assertIn(str(model_path), command)
        self.assertIn("-osrt", command)
        self.assertIn("-l", command)
        self.assertIn("auto", command)

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
        )

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
                    json={"title": "Review", "meeting_link": "https://zoom.us/j/123"},
                )
                stop = client.post("/api/meeting-recorder/records/meeting-1/stop")
                process = client.post("/api/meeting-recorder/records/meeting-1/process")
                email = client.post("/api/meeting-recorder/records/meeting-1/send-email", json={})

        self.assertEqual(start.status_code, 200)
        self.assertEqual(stop.status_code, 200)
        self.assertEqual(process.status_code, 200)
        self.assertEqual(email.status_code, 200)
        fake_runtime.start_recording.assert_called_once()
        fake_runtime.stop_recording.assert_called_once_with(record_id="meeting-1", owner_email="xiaodong.zheng@npt.sg")
        fake_processing.process_recording.assert_called_once_with(record_id="meeting-1", owner_email="xiaodong.zheng@npt.sg")
        fake_processing.send_minutes_email.assert_called_once()


if __name__ == "__main__":
    unittest.main()
