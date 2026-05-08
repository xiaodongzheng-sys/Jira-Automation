import base64
import io
import json
import queue
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.meeting_translation import (
    MeetingTranslationConfig,
    MeetingTranslationRuntime,
    MeetingTranslationSession,
    _ScreenCaptureKitPCMStreamer,
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


class _FailingCloseWebSocket(_FakeWebSocket):
    def close(self):
        raise RuntimeError("close failed")


class _FakePCMRecorder:
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.stopped = False
        self.stop_error = None

    def read_pcm_chunk(self, _size):
        if self.chunks:
            return self.chunks.pop(0)
        return b""

    def queued_chunks(self):
        return len(self.chunks)

    def start(self):
        return {"status": "ok", "pid": 123}

    def stop(self):
        self.stopped = True
        if self.stop_error is not None:
            raise self.stop_error
        return {"status": "stopped"}


class _FakeThread:
    def __init__(self, target=None, args=(), daemon=None):
        self.target = target
        self.args = args
        self.daemon = daemon
        self.name = "fake-thread"
        self.started = False

    def start(self):
        self.started = True


class _FakeProcess:
    def __init__(self, *, pid=42, poll_values=None, wait_error=None):
        self.pid = pid
        self.poll_values = list(poll_values or [None])
        self.wait_error = wait_error
        self.terminated = False
        self.killed = False
        self.stdout = io.BytesIO(b"")

    def poll(self):
        if len(self.poll_values) > 1:
            return self.poll_values.pop(0)
        return self.poll_values[0]

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True

    def wait(self, timeout=None):
        if self.wait_error is not None:
            error = self.wait_error
            self.wait_error = None
            raise error
        return 0


class _FakeEventQueue:
    def __init__(self, events):
        self.events = list(events)

    def get(self, timeout=None):
        if self.events:
            value = self.events.pop(0)
            if isinstance(value, BaseException):
                raise value
            return value
        raise queue.Empty()


class _FullThenEmptyQueue:
    def put_nowait(self, _event):
        raise queue.Full()

    def get_nowait(self):
        raise queue.Empty()


class MeetingTranslationTests(unittest.TestCase):
    def test_normalizes_supported_output_languages(self):
        self.assertEqual(normalize_meeting_translation_language("English"), "en")
        self.assertEqual(normalize_meeting_translation_language("en-SG"), "en")
        self.assertEqual(normalize_meeting_translation_language("indonesian"), "id")
        self.assertEqual(normalize_meeting_translation_language("Chinese"), "zh")
        self.assertEqual(normalize_meeting_translation_language("Bahasa Indonesia"), "id")
        self.assertEqual(normalize_meeting_translation_language("Mandarin"), "zh")
        with self.assertRaises(ToolError):
            normalize_meeting_translation_language("French")

    def test_session_snapshot_emit_and_status_handle_full_queue(self):
        session = MeetingTranslationSession(session_id="s1", owner_email=" PM@NPT.SG ", target_language="zh")
        session.events = queue.Queue(maxsize=1)

        session.emit("first", value=1)
        session.emit("second", value=2)
        session.set_status("listening", "Ready")

        self.assertEqual(session.owner_email, "pm@npt.sg")
        self.assertEqual(session.target_language_label, "Mandarin")
        self.assertEqual(session.snapshot()["status"], "listening")
        self.assertEqual(session.events.qsize(), 1)
        self.assertEqual(session.events.get_nowait()["type"], "status")

        session.events = _FullThenEmptyQueue()
        session.emit("dropped")

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

    def test_connect_websocket_reports_missing_dependency_and_ignores_settimeout_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(sys.modules, {"websocket": None}):
            runtime = MeetingTranslationRuntime(root_dir=Path(temp_dir), config=MeetingTranslationConfig(openai_api_key="key"))
            session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
            with self.assertRaises(ConfigError):
                runtime._connect_websocket(session=session, api_key="key")

        class NoTimeoutModule(_FakeWebSocketModule):
            def create_connection(self, url, header, timeout):
                websocket = super().create_connection(url, header, timeout)

                def fail_timeout(_timeout):
                    raise RuntimeError("unsupported")

                websocket.settimeout = fail_timeout
                return websocket

        fake_module = NoTimeoutModule()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(sys.modules, {"websocket": fake_module}):
            runtime = MeetingTranslationRuntime(root_dir=Path(temp_dir), config=MeetingTranslationConfig(openai_api_key="key", model=" "))
            session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
            websocket = runtime._connect_websocket(session=session, api_key="key")
        self.assertIs(websocket, fake_module.created[0])
        self.assertIn("gpt-realtime-translate", websocket.url)

    def test_session_update_enables_original_input_transcription(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")

        payload = runtime._session_update_payload(session)

        self.assertEqual(payload["type"], "session.update")
        audio = payload["session"]["audio"]
        self.assertEqual(audio["output"]["language"], "zh")
        self.assertEqual(audio["input"]["transcription"]["model"], "gpt-realtime-whisper")
        self.assertNotIn("language", audio["input"]["transcription"])

    def test_start_session_validates_owner_and_key_then_registers_worker(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        with self.assertRaises(ToolError):
            runtime.start_session(owner_email="", target_language="en")
        with self.assertRaises(ConfigError):
            MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig()).start_session(
                owner_email="pm@npt.sg",
                target_language="en",
            )

        with patch("bpmis_jira_tool.meeting_translation.threading.Thread", _FakeThread):
            result = runtime.start_session(owner_email="PM@NPT.SG", target_language="Bahasa")

        self.assertEqual(result["status"], "ok")
        session_id = result["session"]["session_id"]
        session = runtime._sessions[session_id]
        self.assertEqual(session.owner_email, "pm@npt.sg")
        self.assertEqual(session.target_language, "id")
        self.assertTrue(session.thread.started)

    def test_run_session_success_path_sends_session_update_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = MeetingTranslationRuntime(root_dir=Path(temp_dir), config=MeetingTranslationConfig(openai_api_key="key"))
            session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")
            websocket = _FakeWebSocket()
            recorder = _FakePCMRecorder([])

            with (
                patch("bpmis_jira_tool.meeting_translation._resolve_screencapturekit_helper", return_value=Path("/helper.app")),
                patch("bpmis_jira_tool.meeting_translation._ScreenCaptureKitPCMStreamer", return_value=recorder),
                patch.object(runtime, "_connect_websocket", return_value=websocket),
                patch.object(runtime, "_receive_events"),
                patch.object(runtime, "_stream_audio", side_effect=lambda **_kwargs: session.stop_event.set()),
                patch("bpmis_jira_tool.meeting_translation.threading.Thread", _FakeThread),
            ):
                runtime._run_session(session, "key")

        self.assertTrue(recorder.stopped)
        self.assertTrue(websocket.closed)
        self.assertEqual(session.status, "stopped")
        self.assertTrue(any(json.loads(payload)["type"] == "session.update" for payload in websocket.sent))

    def test_run_session_logs_websocket_cleanup_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = MeetingTranslationRuntime(root_dir=Path(temp_dir), config=MeetingTranslationConfig(openai_api_key="key"))
            session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")
            websocket = _FailingCloseWebSocket()
            recorder = _FakePCMRecorder([])

            with (
                patch("bpmis_jira_tool.meeting_translation._resolve_screencapturekit_helper", return_value=Path("/helper.app")),
                patch("bpmis_jira_tool.meeting_translation._ScreenCaptureKitPCMStreamer", return_value=recorder),
                patch.object(runtime, "_connect_websocket", return_value=websocket),
                patch.object(runtime, "_receive_events"),
                patch.object(runtime, "_stream_audio", side_effect=lambda **_kwargs: session.stop_event.set()),
                patch("bpmis_jira_tool.meeting_translation.threading.Thread", _FakeThread),
            ):
                runtime._run_session(session, "key")

        self.assertTrue(recorder.stopped)

    def test_run_session_capture_failure_and_cleanup_failures_set_public_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = MeetingTranslationRuntime(root_dir=Path(temp_dir), config=MeetingTranslationConfig(openai_api_key="key"))
            session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")
            recorder = _FakePCMRecorder([])
            recorder.start = lambda: {"status": "failed", "warning": "ScreenCaptureKit denied"}
            recorder.stop_error = RuntimeError("stop failed")

            with (
                patch("bpmis_jira_tool.meeting_translation._resolve_screencapturekit_helper", return_value=Path("/helper.app")),
                patch("bpmis_jira_tool.meeting_translation._ScreenCaptureKitPCMStreamer", return_value=recorder),
                patch("bpmis_jira_tool.meeting_translation.shutil.rmtree", side_effect=OSError("rm failed")),
            ):
                runtime._run_session(session, "key")

        self.assertEqual(session.status, "error")
        self.assertIn("Screen Recording", session.error)

    def test_run_session_stops_without_error_when_failure_happens_after_stop_requested(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = MeetingTranslationRuntime(root_dir=Path(temp_dir), config=MeetingTranslationConfig(openai_api_key="key"))
            session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")
            session.stop_event.set()
            with patch("bpmis_jira_tool.meeting_translation._resolve_screencapturekit_helper", side_effect=RuntimeError("boom")):
                runtime._run_session(session, "key")
        self.assertEqual(session.status, "stopped")
        self.assertEqual(session.error, "")

    def test_stop_session_handles_close_and_capture_stop_failures(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        session.websocket = _FailingCloseWebSocket()
        recorder = _FakePCMRecorder([])
        recorder.stop_error = RuntimeError("stop failed")
        session.recorder = recorder
        with runtime._lock:
            runtime._sessions[session.session_id] = session

        result = runtime.stop_session(session_id="s1", owner_email="pm@npt.sg")

        self.assertEqual(result["status"], "ok")
        self.assertTrue(session.stop_event.is_set())
        self.assertEqual(result["session"]["status"], "stopped")

    def test_session_lookup_rejects_wrong_owner(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        with runtime._lock:
            runtime._sessions["s1"] = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")

        with self.assertRaises(ToolError):
            runtime.stop_session(session_id="missing", owner_email="pm@npt.sg")
        with self.assertRaises(ToolError):
            runtime.stop_session(session_id="s1", owner_email="other@npt.sg")

    def test_event_stream_yields_snapshot_events_ping_and_closes(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        session.events = _FakeEventQueue([{"type": "translated_delta", "delta": "hi"}, queue.Empty(), queue.Empty()])
        with runtime._lock:
            runtime._sessions[session.session_id] = session

        stream = runtime.event_stream(session_id="s1", owner_email="pm@npt.sg")
        self.assertEqual(next(stream)["type"], "snapshot")
        self.assertEqual(next(stream)["type"], "translated_delta")
        session.status = "stopped"
        self.assertEqual(next(stream)["type"], "ping")
        with self.assertRaises(StopIteration):
            next(stream)

    def test_event_stream_closes_after_terminal_status_event(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        session.events = _FakeEventQueue([{"type": "status", "status": "stopped"}])
        with runtime._lock:
            runtime._sessions[session.session_id] = session

        stream = runtime.event_stream(session_id="s1", owner_email="pm@npt.sg")
        self.assertEqual(next(stream)["type"], "snapshot")
        self.assertEqual(next(stream)["status"], "stopped")
        with self.assertRaises(StopIteration):
            next(stream)

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

    def test_stop_session_logs_successful_capture_stop(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        recorder = _FakePCMRecorder([])
        session.recorder = recorder
        with runtime._lock:
            runtime._sessions[session.session_id] = session

        runtime.stop_session(session_id="s1", owner_email="pm@npt.sg")

        self.assertTrue(recorder.stopped)

    def test_audio_stream_trims_odd_bytes_skips_empty_and_propagates_send_errors(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        websocket = _FakeWebSocket()

        def stop_after_first_sleep(_seconds):
            session.stop_event.set()

        with patch("bpmis_jira_tool.meeting_translation.time.sleep", side_effect=stop_after_first_sleep):
            runtime._stream_audio(session=session, websocket=websocket, recorder=_FakePCMRecorder([b"\x01"]))
        self.assertEqual(websocket.sent, [])

        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        websocket = _FakeWebSocket()

        def fail_send(_payload):
            raise RuntimeError("send failed")

        websocket.send = fail_send
        with self.assertRaises(RuntimeError):
            runtime._stream_audio(session=session, websocket=websocket, recorder=_FakePCMRecorder([b"\x01\x02"]))

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

        self.assertEqual(runtime._pcm_level(b""), 0.0)
        self.assertEqual(runtime._pcm_level(b"\x00\x00" * 100), 0.0)
        self.assertGreater(runtime._pcm_level(b"\xff\x7f" * 100), 0.9)

    def test_pcm_delta_and_mixed_pcm_bytes_cover_sources_and_failures(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="en")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            system_path = root / "system.caf"
            microphone_path = root / "mic.caf"

            self.assertEqual(runtime._pcm_delta(session=session, ffmpeg_path="ffmpeg", system_path=system_path, microphone_path=microphone_path), b"")
            self.assertEqual(runtime._mixed_pcm_bytes(ffmpeg_path="ffmpeg", system_path=system_path, microphone_path=microphone_path), b"")

            system_path.write_bytes(b"s" * 2048)
            with patch("bpmis_jira_tool.meeting_translation.subprocess.run") as run:
                run.return_value = subprocess.CompletedProcess(["ffmpeg"], 0, stdout=b"abcd", stderr=b"")
                self.assertEqual(runtime._mixed_pcm_bytes(ffmpeg_path="ffmpeg", system_path=system_path, microphone_path=microphone_path), b"abcd")
                command = run.call_args.args[0]
                self.assertIn(str(system_path), command)
                self.assertNotIn(str(microphone_path), command)

            microphone_path.write_bytes(b"m" * 2048)
            with patch("bpmis_jira_tool.meeting_translation.subprocess.run") as run:
                run.return_value = subprocess.CompletedProcess(["ffmpeg"], 0, stdout=b"abcdef", stderr=b"")
                self.assertEqual(runtime._pcm_delta(session=session, ffmpeg_path="ffmpeg", system_path=system_path, microphone_path=microphone_path), b"abcdef")
                self.assertEqual(runtime._pcm_delta(session=session, ffmpeg_path="ffmpeg", system_path=system_path, microphone_path=microphone_path), b"")
                command = run.call_args.args[0]
                self.assertIn("[system][mic]amix", " ".join(command))

            session.pcm_offset = 100
            with patch("bpmis_jira_tool.meeting_translation.subprocess.run") as run:
                run.return_value = subprocess.CompletedProcess(["ffmpeg"], 0, stdout=b"abc", stderr=b"")
                self.assertEqual(runtime._pcm_delta(session=session, ffmpeg_path="ffmpeg", system_path=system_path, microphone_path=microphone_path), b"ab")

            with patch("bpmis_jira_tool.meeting_translation.subprocess.run", side_effect=OSError("missing")):
                self.assertEqual(runtime._mixed_pcm_bytes(ffmpeg_path="ffmpeg", system_path=system_path, microphone_path=microphone_path), b"")
            with patch("bpmis_jira_tool.meeting_translation.subprocess.run") as run:
                run.return_value = subprocess.CompletedProcess(["ffmpeg"], 1, stdout=b"bad", stderr=b"")
                self.assertEqual(runtime._mixed_pcm_bytes(ffmpeg_path="ffmpeg", system_path=system_path, microphone_path=microphone_path), b"")

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

    def test_receiver_handles_diagnostics_errors_empty_frames_and_retries(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")
        websocket = _FakeWebSocket()
        websocket._events = [
            RuntimeError("temporary"),
            "",
            "{bad json",
            json.dumps({"type": "session.created"}),
            json.dumps({"type": "error", "error": {"message": "bad realtime"}}),
        ]

        def recv_sequence():
            value = websocket._events.pop(0)
            if isinstance(value, BaseException):
                raise value
            return value

        websocket.recv = recv_sequence
        runtime._receive_events(session, websocket)

        events = []
        while not session.events.empty():
            events.append(session.events.get_nowait())
        self.assertEqual(session.recv_error_count, 1)
        self.assertIn("translation_diagnostics", [event["type"] for event in events])
        self.assertEqual(session.status, "error")
        self.assertEqual(session.error, "bad realtime")

    def test_receiver_stops_quietly_when_recv_fails_after_stop(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        session = MeetingTranslationSession(session_id="s1", owner_email="pm@npt.sg", target_language="zh")
        websocket = _FakeWebSocket()

        def stop_then_fail():
            session.stop_event.set()
            raise RuntimeError("closed")

        websocket.recv = stop_then_fail

        runtime._receive_events(session, websocket)

        self.assertEqual(session.recv_error_count, 0)

    def test_text_delta_helpers_support_item_payloads_and_filter_unrelated_events(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        self.assertEqual(runtime._event_text_delta({"item": {"text": "hello"}}), "hello")
        self.assertEqual(runtime._event_text_delta({"item": {"transcript": "spoken"}}), "spoken")
        self.assertEqual(runtime._translated_delta_from_event(event_type="session.output_audio.delta", event={"delta": "audio"}), "")
        self.assertEqual(runtime._original_delta_from_event(event_type="session.output_audio.delta", event={"delta": "audio"}), "")
        session = MeetingTranslationSession(session_id="s123456789", owner_email="pm@npt.sg", target_language="zh")
        session.openai_event_counts["custom.event"] = 2
        runtime._log_openai_event(session=session, event={"type": "custom.event"}, event_type="custom.event")

    def test_public_error_message_classifies_auth_capture_and_generic_errors(self):
        runtime = MeetingTranslationRuntime(root_dir=Path(tempfile.gettempdir()), config=MeetingTranslationConfig(openai_api_key="key"))
        self.assertIn("MEETING_TRANSLATION_OPENAI_API_KEY", runtime._public_error_message(ConfigError("API key missing")))
        self.assertIn("Screen Recording", runtime._public_error_message(ToolError("ScreenCaptureKit denied")))
        self.assertEqual(runtime._public_openai_error({"message": "plain"}), "plain")
        self.assertEqual(runtime._public_error_message(RuntimeError("x" * 600)), "x" * 500)

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

    def test_screencapture_streamer_reads_chunks_reports_pid_and_stops(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            streamer = _ScreenCaptureKitPCMStreamer(
                helper_path=root / "helper",
                system_audio_path=root / "system.caf",
                microphone_audio_path=root / "mic.caf",
                status_path=root / "status.json",
                log_path=root / "capture.log",
            )
            streamer._process = _FakeProcess(pid=77)
            streamer._stdout_queue.put_nowait(b"ab")
            streamer._stdout_queue.put_nowait(b"cd")

            self.assertEqual(streamer.pid, 77)
            self.assertEqual(streamer.queued_chunks(), 2)
            self.assertEqual(streamer.read_pcm_chunk(4), b"abcd")
            self.assertEqual(streamer.read_pcm_chunk(4), b"")

            streamer._stdout_queue.put_nowait(b"ab")
            self.assertEqual(streamer.read_pcm_chunk(4), b"ab")

            stop_result = streamer.stop()
            self.assertTrue(streamer._process.terminated)
            self.assertEqual(stop_result["recorder_pid"], 77)

    def test_screencapture_streamer_kills_process_after_terminate_timeout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            streamer = _ScreenCaptureKitPCMStreamer(
                helper_path=root / "helper",
                system_audio_path=root / "system.caf",
                microphone_audio_path=root / "mic.caf",
                status_path=root / "status.json",
                log_path=root / "capture.log",
            )
            process = _FakeProcess(wait_error=subprocess.TimeoutExpired("helper", 8))
            streamer._process = process

            streamer.stop()

            self.assertTrue(process.terminated)
            self.assertTrue(process.killed)

    def test_screencapture_streamer_start_reports_recording_failed_timeout_and_early_exit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            status_path = root / "status.json"
            streamer = _ScreenCaptureKitPCMStreamer(
                helper_path=root / "helper",
                system_audio_path=root / "system.caf",
                microphone_audio_path=root / "mic.caf",
                status_path=status_path,
                log_path=root / "capture.log",
            )

            def fake_popen_recording(*_args, **_kwargs):
                status_path.write_text(json.dumps({"status": "recording"}), encoding="utf-8")
                return _FakeProcess()

            with patch("bpmis_jira_tool.meeting_translation.subprocess.Popen", side_effect=fake_popen_recording):
                result = streamer.start()
            self.assertEqual(result["status"], "ok")
            streamer.stop()

            streamer = _ScreenCaptureKitPCMStreamer(
                helper_path=root / "helper",
                system_audio_path=root / "system2.caf",
                microphone_audio_path=root / "mic2.caf",
                status_path=status_path,
                log_path=root / "capture2.log",
            )

            def fake_popen_failed(*_args, **_kwargs):
                status_path.write_text(json.dumps({"status": "failed", "message": "no permission"}), encoding="utf-8")
                return _FakeProcess()

            with patch("bpmis_jira_tool.meeting_translation.subprocess.Popen", side_effect=fake_popen_failed):
                result = streamer.start()
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["warning"], "no permission")
            streamer.stop()

            streamer = _ScreenCaptureKitPCMStreamer(
                helper_path=root / "helper",
                system_audio_path=root / "system3.caf",
                microphone_audio_path=root / "mic3.caf",
                status_path=root / "missing-status.json",
                log_path=root / "capture3.log",
            )
            with (
                patch("bpmis_jira_tool.meeting_translation.subprocess.Popen", return_value=_FakeProcess(poll_values=[1])),
                patch("bpmis_jira_tool.meeting_translation._read_tail", return_value="helper exited"),
            ):
                result = streamer.start()
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["warning"], "helper exited")

            streamer = _ScreenCaptureKitPCMStreamer(
                helper_path=root / "helper",
                system_audio_path=root / "system4.caf",
                microphone_audio_path=root / "mic4.caf",
                status_path=root / "never-status.json",
                log_path=root / "capture4.log",
            )
            with (
                patch("bpmis_jira_tool.meeting_translation.subprocess.Popen", return_value=_FakeProcess()),
                patch("bpmis_jira_tool.meeting_translation.time.monotonic", side_effect=[0, 0, 9]),
                patch("bpmis_jira_tool.meeting_translation.time.sleep"),
            ):
                result = streamer.start()
            self.assertEqual(result["status"], "failed")
            self.assertIn("did not start", result["warning"])

    def test_screencapture_stdout_reader_handles_missing_stdout_exit_error_and_full_queue(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            streamer = _ScreenCaptureKitPCMStreamer(
                helper_path=root / "helper",
                system_audio_path=root / "system.caf",
                microphone_audio_path=root / "mic.caf",
                status_path=root / "status.json",
                log_path=root / "capture.log",
            )
            streamer._process = None
            streamer._read_stdout_loop()

            class OSErrorStdout:
                def read(self, _size):
                    raise OSError("broken")

            streamer._process = _FakeProcess()
            streamer._process.stdout = OSErrorStdout()
            streamer._read_stdout_loop()

            streamer._process = _FakeProcess(poll_values=[1])
            streamer._process.stdout = io.BytesIO(b"")
            streamer._read_stdout_loop()

            class ChunkThenStop:
                def __init__(self):
                    self.calls = 0

                def read(self, _size):
                    self.calls += 1
                    return b"chunk" if self.calls == 1 else b""

            streamer._stdout_queue = queue.Queue(maxsize=1)
            streamer._stdout_queue.put_nowait(b"old")
            streamer._process = _FakeProcess(poll_values=[None, 1])
            streamer._process.stdout = ChunkThenStop()
            streamer._read_stdout_loop()
            self.assertEqual(streamer._stdout_queue.get_nowait(), b"chunk")

            class AlwaysFullQueue:
                def put_nowait(self, _chunk):
                    raise queue.Full()

                def get_nowait(self):
                    raise queue.Empty()

                def qsize(self):
                    return 0

            streamer._stdout_queue = AlwaysFullQueue()
            streamer._process = _FakeProcess(poll_values=[None, 1])
            streamer._process.stdout = ChunkThenStop()
            streamer._read_stdout_loop()


if __name__ == "__main__":
    unittest.main()
