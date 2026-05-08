import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.meeting_translation import (
    MeetingTranslationConfig,
    MeetingTranslationRuntime,
    MeetingTranslationSession,
    normalize_meeting_translation_language,
)


class _FakeWebSocketModule:
    def __init__(self):
        self.created = []

    def create_connection(self, url, header, timeout):
        websocket = _FakeWebSocket()
        websocket.url = url
        websocket.header = header
        websocket.timeout = timeout
        self.created.append(websocket)
        return websocket


class _FakeWebSocket:
    def __init__(self):
        self.sent = []
        self.closed = False
        self._events = []

    def settimeout(self, _timeout):
        return None

    def send(self, payload):
        self.sent.append(payload)

    def recv(self):
        if self._events:
            return self._events.pop(0)
        raise RuntimeError("no event")

    def close(self):
        self.closed = True


class _FakePCMRecorder:
    def __init__(self, chunks):
        self.chunks = list(chunks)

    def read_pcm_chunk(self, _size):
        if self.chunks:
            return self.chunks.pop(0)
        return b""

    def queued_chunks(self):
        return len(self.chunks)


class MeetingTranslationTests(unittest.TestCase):
    def test_normalizes_supported_output_languages(self):
        self.assertEqual(normalize_meeting_translation_language("English"), "en")
        self.assertEqual(normalize_meeting_translation_language("Bahasa Indonesia"), "id")
        self.assertEqual(normalize_meeting_translation_language("Mandarin"), "zh")
        with self.assertRaises(ToolError):
            normalize_meeting_translation_language("French")

    def test_connect_websocket_uses_translate_model_and_safety_identifier(self):
        fake_module = _FakeWebSocketModule()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(sys.modules, {"websocket": fake_module}):
            runtime = MeetingTranslationRuntime(
                root_dir=Path(temp_dir),
                config=MeetingTranslationConfig(openai_api_key="key", model="gpt-realtime-translate"),
            )
            session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="id")

            websocket = runtime._connect_websocket(session=session, api_key="key")

        self.assertIs(websocket, fake_module.created[0])
        self.assertIn("gpt-realtime-translate", websocket.url)
        self.assertIn("Authorization: Bearer key", websocket.header)
        self.assertTrue(any(item.startswith("OpenAI-Safety-Identifier: ") for item in websocket.header))

    def test_audio_chunks_are_base64_encoded_for_realtime_append_events(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = MeetingTranslationRuntime(
                root_dir=Path(temp_dir),
                config=MeetingTranslationConfig(openai_api_key="key"),
            )
            session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
            websocket = _FakeWebSocket()
            pcm = b"\x01\x02" * 128

            def stop_after_first_sleep(_seconds):
                session.stop_event.set()

            with patch.object(runtime, "_pcm_delta", return_value=pcm), patch("bpmis_jira_tool.meeting_translation.time.sleep", side_effect=stop_after_first_sleep):
                runtime._stream_audio(
                    session=session,
                    websocket=websocket,
                    recorder=_FakePCMRecorder([pcm]),
                )

        payload = json.loads(websocket.sent[0])
        self.assertEqual(payload["type"], "session.input_audio_buffer.append")
        self.assertEqual(base64.b64decode(payload["audio"]), pcm)
        event = session.events.get_nowait()
        self.assertEqual(event["type"], "audio_activity")
        self.assertEqual(event["bytes"], len(pcm))
        self.assertGreaterEqual(event["duration_ms"], 0)
        self.assertGreater(event["level"], 0)
        self.assertIn("sent_audio_seconds", event)
        self.assertIn("queued_audio_chunks", event)

    def test_audio_stream_emits_waiting_diagnostic_when_openai_has_no_transcript(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        websocket = _FakeWebSocket()
        pcm = b"\x01\x02" * (24_000 * 6)

        def stop_after_first_sleep(_seconds):
            session.stop_event.set()

        with patch("bpmis_jira_tool.meeting_translation.time.sleep", side_effect=stop_after_first_sleep):
            runtime._stream_audio(
                session=session,
                websocket=websocket,
                recorder=_FakePCMRecorder([pcm]),
            )

        events = []
        while not session.events.empty():
            events.append(session.events.get_nowait())
        self.assertIn("transcript_waiting", [event["type"] for event in events])

    def test_pcm_level_reports_silence_and_signal(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))

        self.assertEqual(runtime._pcm_level(b"\x00\x00" * 100), 0.0)
        self.assertGreater(runtime._pcm_level(b"\xff\x7f" * 100), 0.9)

    def test_transcript_events_map_to_translated_and_original_streams(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")
        websocket = _FakeWebSocket()
        websocket._events = [
            json.dumps({"type": "session.output_transcript.delta", "delta": "你好"}),
            json.dumps({"type": "session.input_transcript.delta", "delta": "hello"}),
        ]

        def recv_with_stop():
            if websocket._events:
                value = websocket._events.pop(0)
                if not websocket._events:
                    session.stop_event.set()
                return value
            raise RuntimeError("done")

        websocket.recv = recv_with_stop
        runtime._receive_events(session, websocket)

        events = [session.events.get_nowait(), session.events.get_nowait()]
        self.assertEqual(events[0]["type"], "translated_delta")
        self.assertEqual(events[0]["delta"], "你好")
        self.assertEqual(events[1]["type"], "original_delta")
        self.assertEqual(events[1]["delta"], "hello")

    def test_completed_transcript_events_map_when_openai_uses_transcript_field(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")
        websocket = _FakeWebSocket()
        websocket._events = [
            json.dumps({"type": "session.output_transcript.completed", "transcript": "你好"}),
            json.dumps({"type": "session.input_transcript.completed", "transcript": "hello"}),
        ]

        def recv_with_stop():
            if websocket._events:
                value = websocket._events.pop(0)
                if not websocket._events:
                    session.stop_event.set()
                return value
            raise RuntimeError("done")

        websocket.recv = recv_with_stop
        runtime._receive_events(session, websocket)

        events = [session.events.get_nowait(), session.events.get_nowait()]
        self.assertEqual(events[0]["type"], "translated_delta")
        self.assertEqual(events[0]["delta"], "你好")
        self.assertEqual(events[1]["type"], "original_delta")
        self.assertEqual(events[1]["delta"], "hello")

    def test_stop_closes_capture_and_websocket(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        websocket = _FakeWebSocket()
        session.websocket = websocket
        with runtime._lock:
            runtime._sessions[session.session_id] = session

        result = runtime.stop_session(session_id="s1", owner_email="pm@npt.sg")

        self.assertEqual(result["session"]["status"], "stopped")
        self.assertTrue(session.stop_event.is_set())
        self.assertTrue(websocket.closed)


if __name__ == "__main__":
    unittest.main()
