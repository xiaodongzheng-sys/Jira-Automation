from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import logging
import queue
from pathlib import Path
import shutil
import subprocess
import threading
import time
from typing import Any, BinaryIO, Iterator
import uuid

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.meeting_recorder import (
    _background_command,
    _read_json_file,
    _read_tail,
    _resolve_screencapturekit_helper,
    _safe_int,
    _safe_file_size,
    _utc_now,
)


MEETING_TRANSLATION_LANGUAGES: dict[str, dict[str, str]] = {
    "en": {"code": "en", "label": "English"},
    "id": {"code": "id", "label": "Bahasa Indonesia"},
    "zh": {"code": "zh", "label": "Mandarin"},
}
MEETING_TRANSLATION_TERMINAL_STATUSES = {"stopped", "error"}
MEETING_TRANSLATION_PCM_RATE = 24000
MEETING_TRANSLATION_PCM_CHUNK_BYTES = 48_000
MEETING_TRANSLATION_PCM_STREAM_READ_BYTES = 4_800
MEETING_TRANSLATION_PCM_BYTES_PER_SECOND = MEETING_TRANSLATION_PCM_RATE * 2
MEETING_TRANSLATION_TRANSCRIPT_WAIT_SECONDS = 5.0
MEETING_TRANSLATION_INPUT_TRANSCRIPTION_MODEL = "gpt-realtime-whisper"

LOGGER = logging.getLogger(__name__)


def normalize_meeting_translation_language(value: object) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "english": "en",
        "en-us": "en",
        "en-sg": "en",
        "bahasa": "id",
        "bahasa indonesia": "id",
        "indonesian": "id",
        "indonesia": "id",
        "mandarin": "zh",
        "chinese": "zh",
        "zh-cn": "zh",
        "zh-sg": "zh",
    }
    normalized = aliases.get(raw, raw)
    if normalized not in MEETING_TRANSLATION_LANGUAGES:
        raise ToolError("Choose a supported translation language: English, Bahasa Indonesia, or Mandarin.")
    return normalized


@dataclass(frozen=True)
class MeetingTranslationConfig:
    ffmpeg_bin: str = "ffmpeg"
    openai_api_key: str | None = None
    model: str = "gpt-realtime-translate"
    background_nice: int = 10
    capture_status_every_buffers: int = 250


class MeetingTranslationSession:
    def __init__(self, *, session_id: str, owner_email: str, target_language: str) -> None:
        self.session_id = session_id
        self.owner_email = owner_email.strip().lower()
        self.target_language = target_language
        self.status = "queued"
        self.message = "Queued live translation."
        self.error = ""
        self.created_at = _utc_now()
        self.updated_at = self.created_at
        self.events: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=500)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.recorder: Any = None
        self.websocket: Any = None
        self.pcm_offset = 0
        self.event_seq = 0
        self.audio_bytes_sent = 0
        self.last_audio_at = 0.0
        self.last_transcript_at = 0.0
        self.translated_delta_count = 0
        self.original_delta_count = 0
        self.openai_event_counts: dict[str, int] = {}
        self._transcript_waiting_emitted = False
        self.audio_append_count = 0
        self.recv_error_count = 0

    @property
    def target_language_label(self) -> str:
        return MEETING_TRANSLATION_LANGUAGES[self.target_language]["label"]

    def snapshot(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "owner_email": self.owner_email,
            "status": self.status,
            "message": self.message,
            "error": self.error,
            "target_language": self.target_language,
            "target_language_label": self.target_language_label,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def emit(self, event_type: str, **payload: Any) -> None:
        self.event_seq += 1
        event = {
            "seq": self.event_seq,
            "type": event_type,
            "session_id": self.session_id,
            "at": _utc_now(),
            **payload,
        }
        try:
            self.events.put_nowait(event)
        except queue.Full:
            try:
                self.events.get_nowait()
                self.events.put_nowait(event)
            except queue.Empty:
                pass

    def set_status(self, status: str, message: str = "", *, error: str = "") -> None:
        self.status = status
        self.message = message or status
        self.error = error
        self.updated_at = _utc_now()
        self.emit("status", status=self.status, message=self.message, error=self.error)


class _ScreenCaptureKitPCMStreamer:
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
        self._process: subprocess.Popen[bytes] | None = None
        self._stdout_queue: queue.Queue[bytes] = queue.Queue(maxsize=200)
        self._reader_stop = threading.Event()
        self._reader_thread: threading.Thread | None = None
        self._queue_drop_count = 0
        self.command = _background_command([
            str(self.helper_path),
            "--system-output",
            str(self.system_audio_path),
            "--microphone-output",
            str(self.microphone_audio_path),
            "--status-output",
            str(self.status_path),
            "--status-every-buffers",
            str(max(1, int(status_every_buffers or 250))),
            "--raw-pcm-output",
            "stdout",
        ], background_nice)

    @property
    def pid(self) -> int:
        return _safe_int(getattr(self._process, "pid", 0))

    def start(self) -> dict[str, Any]:
        self.system_audio_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.write_text("", encoding="utf-8")
        log_handle = self.log_path.open("ab")
        self._process = subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=log_handle,
            close_fds=True,
        )
        self._reader_thread = threading.Thread(target=self._read_stdout_loop, daemon=True)
        self._reader_thread.start()
        started = time.monotonic()
        while time.monotonic() - started < 8.0:
            if self._process.poll() is not None:
                warning = _read_tail(self.log_path) or "ScreenCaptureKit helper exited before translation started."
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
                    "streaming_audio": "pcm16_24khz_mono_stdout",
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

    def read_pcm_chunk(self, size: int) -> bytes:
        target_size = max(2, int(size or MEETING_TRANSLATION_PCM_STREAM_READ_BYTES))
        chunks: list[bytes] = []
        total = 0
        try:
            first = self._stdout_queue.get(timeout=0.1)
        except queue.Empty:
            return b""
        chunks.append(first)
        total += len(first)
        while total < target_size:
            try:
                chunk = self._stdout_queue.get_nowait()
            except queue.Empty:
                break
            chunks.append(chunk)
            total += len(chunk)
        return b"".join(chunks)

    def stop(self) -> dict[str, Any]:
        self._reader_stop.set()
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

    def queued_chunks(self) -> int:
        return self._stdout_queue.qsize()

    def _read_stdout_loop(self) -> None:
        process = self._process
        stdout: BinaryIO | None = process.stdout if process is not None else None
        if stdout is None:
            return
        while not self._reader_stop.is_set():
            try:
                chunk = stdout.read(MEETING_TRANSLATION_PCM_STREAM_READ_BYTES)
            except OSError as error:
                LOGGER.warning("meeting_translation_pcm_reader %s", json.dumps({"event": "stdout_read_failed", "error": str(error)[:500]}, sort_keys=True))
                return
            if not chunk:
                if process is not None and process.poll() is not None:
                    LOGGER.warning("meeting_translation_pcm_reader %s", json.dumps({"event": "helper_process_exited", "pid": self.pid}, sort_keys=True))
                    return
                time.sleep(0.02)
                continue
            try:
                self._stdout_queue.put_nowait(chunk)
            except queue.Full:
                try:
                    self._queue_drop_count += 1
                    self._stdout_queue.get_nowait()
                    self._stdout_queue.put_nowait(chunk)
                    if self._queue_drop_count in {1, 10, 50} or self._queue_drop_count % 100 == 0:
                        LOGGER.warning(
                            "meeting_translation_pcm_reader %s",
                            json.dumps(
                                {
                                    "event": "stdout_queue_full_drop_oldest",
                                    "drop_count": self._queue_drop_count,
                                    "queued_chunks": self._stdout_queue.qsize(),
                                },
                                sort_keys=True,
                            ),
                        )
                except queue.Empty:
                    pass


class MeetingTranslationRuntime:
    def __init__(self, *, root_dir: Path, config: MeetingTranslationConfig) -> None:
        self.root_dir = Path(root_dir)
        self.config = config
        self._sessions: dict[str, MeetingTranslationSession] = {}
        self._lock = threading.Lock()

    def start_session(self, *, owner_email: str, target_language: object) -> dict[str, Any]:
        owner = str(owner_email or "").strip().lower()
        if not owner:
            raise ToolError("Owner email is required before starting Meeting Translation.")
        language = normalize_meeting_translation_language(target_language)
        api_key = str(self.config.openai_api_key or "").strip()
        if not api_key:
            self._log_debug(
                "start_rejected_missing_openai_key",
                owner_hash=self._hash_for_log(owner),
                target_language=language,
                model=self.config.model,
            )
            raise ConfigError("MEETING_TRANSLATION_OPENAI_API_KEY or OPENAI_API_KEY is required for Meeting Translation.")

        session = MeetingTranslationSession(session_id=uuid.uuid4().hex, owner_email=owner, target_language=language)
        self._log_session_event(session, "start_requested", model=self.config.model)
        with self._lock:
            self._sessions[session.session_id] = session
        session.thread = threading.Thread(target=self._run_session, args=(session, api_key), daemon=True)
        session.thread.start()
        self._log_session_event(session, "worker_started", thread_name=session.thread.name)
        return {"status": "ok", "session": session.snapshot()}

    def stop_session(self, *, session_id: str, owner_email: str) -> dict[str, Any]:
        session = self._session_for_owner(session_id=session_id, owner_email=owner_email)
        self._log_session_event(session, "stop_requested")
        session.stop_event.set()
        websocket = session.websocket
        if websocket is not None:
            try:
                websocket.close()
                self._log_session_event(session, "websocket_close_requested")
            except Exception as error:
                self._log_session_event(session, "websocket_close_failed", error=self._safe_error(error))
        recorder = session.recorder
        if recorder is not None:
            try:
                stop_result = recorder.stop()
                self._log_session_event(session, "capture_stop_requested", capture_stop=stop_result)
            except Exception as error:
                self._log_session_event(session, "capture_stop_failed", error=self._safe_error(error))
        if session.status not in MEETING_TRANSLATION_TERMINAL_STATUSES:
            session.set_status("stopped", "Translation stopped.")
        self._log_session_event(session, "stop_completed")
        return {"status": "ok", "session": session.snapshot()}

    def event_stream(self, *, session_id: str, owner_email: str) -> Iterator[dict[str, Any]]:
        session = self._session_for_owner(session_id=session_id, owner_email=owner_email)
        self._log_session_event(session, "sse_opened")
        yield {"type": "snapshot", **session.snapshot()}
        idle_ticks = 0
        while True:
            try:
                event = session.events.get(timeout=5)
                idle_ticks = 0
                self._log_session_event(
                    session,
                    "sse_event_yield",
                    sse_event_type=str(event.get("type") or ""),
                    sse_event_status=str(event.get("status") or ""),
                    event_seq=event.get("seq"),
                )
                yield event
                if event.get("type") == "status" and event.get("status") in MEETING_TRANSLATION_TERMINAL_STATUSES:
                    break
            except queue.Empty:
                idle_ticks += 1
                self._log_session_event(session, "sse_ping", idle_ticks=idle_ticks)
                yield {"type": "ping", "session_id": session.session_id, "at": _utc_now()}
                if session.status in MEETING_TRANSLATION_TERMINAL_STATUSES and idle_ticks >= 1:
                    break
        self._log_session_event(session, "sse_closed", idle_ticks=idle_ticks)

    def _session_for_owner(self, *, session_id: str, owner_email: str) -> MeetingTranslationSession:
        safe_session_id = str(session_id or "").strip()
        owner = str(owner_email or "").strip().lower()
        with self._lock:
            session = self._sessions.get(safe_session_id)
        if session is None or session.owner_email != owner:
            raise ToolError("Meeting Translation session was not found.")
        return session

    def _run_session(self, session: MeetingTranslationSession, api_key: str) -> None:
        session_dir = self.root_dir / session.session_id
        system_path = session_dir / "screencapture-system.caf"
        microphone_path = session_dir / "screencapture-microphone.caf"
        status_path = session_dir / "screencapture-status.json"
        log_path = session_dir / "screencapture.log"
        try:
            self._log_session_event(session, "run_started", session_dir=str(session_dir))
            session.set_status("connecting", "Starting system audio and microphone capture.")
            helper_store_root = self.root_dir.parent.parent / "meeting_records" if self.root_dir.parent.name == "run" else self.root_dir
            self._log_session_event(session, "capture_config_resolved", helper_store_root=str(helper_store_root))
            session.recorder = _ScreenCaptureKitPCMStreamer(
                helper_path=_resolve_screencapturekit_helper(helper_store_root),
                system_audio_path=system_path,
                microphone_audio_path=microphone_path,
                status_path=status_path,
                log_path=log_path,
                background_nice=self.config.background_nice,
                status_every_buffers=self.config.capture_status_every_buffers,
            )
            self._log_session_event(
                session,
                "capture_starting",
                helper_path=str(getattr(session.recorder, "helper_path", "")),
                status_path=str(status_path),
                stream_read_bytes=MEETING_TRANSLATION_PCM_STREAM_READ_BYTES,
            )
            ready = session.recorder.start()
            self._log_session_event(session, "capture_start_result", capture_ready=ready)
            if str(ready.get("status") or "") != "ok":
                warning = str(ready.get("warning") or "ScreenCaptureKit audio capture failed to start.")
                raise ToolError(warning)
            session.emit("capture_ready", **ready)
            session.set_status("connecting", "Audio capture started. Connecting to OpenAI translation.")

            self._log_session_event(session, "websocket_connecting", model=self.config.model)
            websocket = self._connect_websocket(session=session, api_key=api_key)
            session.websocket = websocket
            self._log_session_event(session, "websocket_connected")
            self._send_json(websocket, self._session_update_payload(session))
            self._log_session_event(
                session,
                "session_update_sent",
                output_language=MEETING_TRANSLATION_LANGUAGES[session.target_language]["code"],
                input_transcription_model=MEETING_TRANSLATION_INPUT_TRANSCRIPTION_MODEL,
            )
            receiver = threading.Thread(target=self._receive_events, args=(session, websocket), daemon=True)
            receiver.start()
            self._log_session_event(session, "receiver_started", thread_name=receiver.name)
            session.set_status("listening", f"Translating to {session.target_language_label}.")
            self._stream_audio(session=session, websocket=websocket, recorder=session.recorder)
        except Exception as error:
            self._log_session_event(session, "run_error", error=self._safe_error(error))
            if not session.stop_event.is_set():
                session.set_status("error", self._public_error_message(error), error=self._public_error_message(error))
        finally:
            self._log_session_event(session, "cleanup_started")
            session.stop_event.set()
            if session.websocket is not None:
                try:
                    session.websocket.close()
                    self._log_session_event(session, "cleanup_websocket_closed")
                except Exception as error:
                    self._log_session_event(session, "cleanup_websocket_close_failed", error=self._safe_error(error))
            if session.recorder is not None:
                try:
                    stop_result = session.recorder.stop()
                    self._log_session_event(session, "cleanup_capture_stopped", capture_stop=stop_result)
                except Exception as error:
                    self._log_session_event(session, "cleanup_capture_stop_failed", error=self._safe_error(error))
            if session.status not in MEETING_TRANSLATION_TERMINAL_STATUSES:
                session.set_status("stopped", "Translation stopped.")
            try:
                shutil.rmtree(session_dir, ignore_errors=True)
                self._log_session_event(session, "cleanup_session_dir_removed")
            except OSError as error:
                self._log_session_event(session, "cleanup_session_dir_remove_failed", error=self._safe_error(error))
            self._log_session_event(session, "run_finished")

    def _connect_websocket(self, *, session: MeetingTranslationSession, api_key: str) -> Any:
        try:
            import websocket  # type: ignore[import-not-found]
        except ImportError as error:
            raise ConfigError("websocket-client is required for Meeting Translation. Install requirements.txt dependencies.") from error
        model = str(self.config.model or "gpt-realtime-translate").strip() or "gpt-realtime-translate"
        url = f"wss://api.openai.com/v1/realtime/translations?model={model}"
        safety_id = hashlib.sha256(session.owner_email.encode("utf-8")).hexdigest()
        ws = websocket.create_connection(
            url,
            header=[
                f"Authorization: Bearer {api_key}",
                f"OpenAI-Safety-Identifier: {safety_id}",
            ],
            timeout=10,
        )
        try:
            ws.settimeout(1)
        except Exception:
            pass
        return ws

    def _receive_events(self, session: MeetingTranslationSession, websocket: Any) -> None:
        self._log_session_event(session, "receiver_loop_started")
        while not session.stop_event.is_set():
            try:
                data = websocket.recv()
            except Exception as error:
                if session.stop_event.is_set():
                    self._log_session_event(session, "receiver_loop_stopped_after_stop")
                    return
                session.recv_error_count += 1
                if session.recv_error_count in {1, 5, 20} or session.recv_error_count % 100 == 0:
                    self._log_session_event(session, "receiver_recv_retry", recv_error_count=session.recv_error_count, error=self._safe_error(error))
                continue
            if not data:
                self._log_session_event(session, "receiver_empty_frame")
                continue
            try:
                event = json.loads(data)
            except json.JSONDecodeError as error:
                self._log_session_event(session, "receiver_json_decode_failed", error=self._safe_error(error), frame_bytes=len(str(data)))
                continue
            event_type = str(event.get("type") or "")
            session.openai_event_counts[event_type] = session.openai_event_counts.get(event_type, 0) + 1
            self._log_openai_event(session=session, event=event, event_type=event_type)
            translated_delta = self._translated_delta_from_event(event_type=event_type, event=event)
            original_delta = self._original_delta_from_event(event_type=event_type, event=event)
            if translated_delta:
                session.translated_delta_count += 1
                session.last_transcript_at = time.monotonic()
                session.emit("translated_delta", delta=translated_delta)
            if original_delta:
                session.original_delta_count += 1
                session.last_transcript_at = time.monotonic()
                session.emit("original_delta", delta=original_delta)
            if translated_delta or original_delta:
                continue
            if event_type in {"session.updated", "session.created"}:
                session.emit(
                    "translation_diagnostics",
                    openai_event=event_type,
                    translated_deltas=session.translated_delta_count,
                    original_deltas=session.original_delta_count,
                )
            elif event_type == "error":
                message = self._public_openai_error(event)
                self._log_session_event(session, "openai_error_event", error=message)
                session.set_status("error", message, error=message)
                session.stop_event.set()
                return
        self._log_session_event(session, "receiver_loop_exited")

    def _stream_audio(
        self,
        *,
        session: MeetingTranslationSession,
        websocket: Any,
        recorder: Any,
    ) -> None:
        self._log_session_event(session, "audio_stream_loop_started", stream_read_bytes=MEETING_TRANSLATION_PCM_STREAM_READ_BYTES)
        while not session.stop_event.is_set():
            chunk = recorder.read_pcm_chunk(MEETING_TRANSLATION_PCM_STREAM_READ_BYTES)
            if not chunk:
                time.sleep(0.05)
                continue
            if len(chunk) % 2:
                chunk = chunk[:-1]
            if not chunk:
                continue
            try:
                self._send_json(
                    websocket,
                    {
                        "type": "session.input_audio_buffer.append",
                        "audio": base64.b64encode(chunk).decode("ascii"),
                    },
                )
            except Exception as error:
                self._log_session_event(session, "audio_append_failed", error=self._safe_error(error))
                raise
            session.audio_append_count += 1
            session.audio_bytes_sent += len(chunk)
            session.last_audio_at = time.monotonic()
            queued_chunks = int(getattr(recorder, "queued_chunks", lambda: 0)())
            if session.audio_append_count <= 5 or session.audio_append_count % 10 == 0:
                self._log_session_event(
                    session,
                    "audio_append_sent",
                    audio_append_count=session.audio_append_count,
                    chunk_bytes=len(chunk),
                    sent_audio_seconds=round(session.audio_bytes_sent / MEETING_TRANSLATION_PCM_BYTES_PER_SECOND, 1),
                    queued_audio_chunks=queued_chunks,
                    level=self._pcm_level(chunk),
                )
            session.emit(
                "audio_activity",
                bytes=len(chunk),
                duration_ms=round((len(chunk) / MEETING_TRANSLATION_PCM_BYTES_PER_SECOND) * 1000),
                level=self._pcm_level(chunk),
                sent_audio_seconds=round(session.audio_bytes_sent / MEETING_TRANSLATION_PCM_BYTES_PER_SECOND, 1),
                queued_audio_chunks=queued_chunks,
            )
            if (
                not session._transcript_waiting_emitted
                and session.audio_bytes_sent >= int(MEETING_TRANSLATION_TRANSCRIPT_WAIT_SECONDS * MEETING_TRANSLATION_PCM_BYTES_PER_SECOND)
                and session.translated_delta_count == 0
                and session.original_delta_count == 0
            ):
                session._transcript_waiting_emitted = True
                session.emit(
                    "transcript_waiting",
                    message="Audio is streaming to OpenAI; waiting for transcript deltas.",
                    sent_audio_seconds=round(session.audio_bytes_sent / MEETING_TRANSLATION_PCM_BYTES_PER_SECOND, 1),
                )
                self._log_session_event(
                    session,
                    "transcript_waiting_emitted",
                    sent_audio_seconds=round(session.audio_bytes_sent / MEETING_TRANSLATION_PCM_BYTES_PER_SECOND, 1),
                )
        self._log_session_event(session, "audio_stream_loop_exited")

    def _pcm_delta(self, *, session: MeetingTranslationSession, ffmpeg_path: str, system_path: Path, microphone_path: Path) -> bytes:
        pcm = self._mixed_pcm_bytes(ffmpeg_path=ffmpeg_path, system_path=system_path, microphone_path=microphone_path)
        if not pcm:
            return b""
        if len(pcm) < session.pcm_offset:
            session.pcm_offset = 0
        delta = pcm[session.pcm_offset:]
        session.pcm_offset = len(pcm)
        return delta[: len(delta) - (len(delta) % 2)]

    def _mixed_pcm_bytes(self, *, ffmpeg_path: str, system_path: Path, microphone_path: Path) -> bytes:
        system_ready = _safe_file_size(system_path) > 1024
        microphone_ready = _safe_file_size(microphone_path) > 1024
        if not system_ready and not microphone_ready:
            return b""
        if system_ready and microphone_ready:
            command = [
                ffmpeg_path,
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(system_path),
                "-i",
                str(microphone_path),
                "-filter_complex",
                (
                    f"[0:a]aresample={MEETING_TRANSLATION_PCM_RATE},pan=mono|c0=0.5*c0+0.5*c1[system];"
                    f"[1:a]aresample={MEETING_TRANSLATION_PCM_RATE},pan=mono|c0=c0[mic];"
                    "[system][mic]amix=inputs=2:duration=longest:dropout_transition=0[a]"
                ),
                "-map",
                "[a]",
                "-f",
                "s16le",
                "-acodec",
                "pcm_s16le",
                "-ar",
                str(MEETING_TRANSLATION_PCM_RATE),
                "-ac",
                "1",
                "pipe:1",
            ]
        else:
            source_path = system_path if system_ready else microphone_path
            command = [
                ffmpeg_path,
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(source_path),
                "-f",
                "s16le",
                "-acodec",
                "pcm_s16le",
                "-ar",
                str(MEETING_TRANSLATION_PCM_RATE),
                "-ac",
                "1",
                "pipe:1",
            ]
        try:
            result = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return b""
        return result.stdout if result.returncode == 0 else b""

    def _pcm_level(self, chunk: bytes) -> float:
        sample_count = len(chunk) // 2
        if sample_count <= 0:
            return 0.0
        stride = max(1, sample_count // 1200)
        peak = 0
        total = 0
        used = 0
        for sample_index in range(0, sample_count, stride):
            offset = sample_index * 2
            sample = int.from_bytes(chunk[offset:offset + 2], byteorder="little", signed=True)
            magnitude = abs(sample)
            peak = max(peak, magnitude)
            total += magnitude
            used += 1
        if not used:
            return 0.0
        peak_level = peak / 32768
        average_level = (total / used) / 32768
        return round(min(1.0, max(peak_level, average_level * 2)), 3)

    def _send_json(self, websocket: Any, payload: dict[str, Any]) -> None:
        websocket.send(json.dumps(payload, separators=(",", ":")))

    def _session_update_payload(self, session: MeetingTranslationSession) -> dict[str, Any]:
        return {
            "type": "session.update",
            "session": {
                "audio": {
                    "input": {
                        "transcription": {
                            "model": MEETING_TRANSLATION_INPUT_TRANSCRIPTION_MODEL,
                        },
                    },
                    "output": {
                        "language": MEETING_TRANSLATION_LANGUAGES[session.target_language]["code"],
                    },
                }
            },
        }

    def _translated_delta_from_event(self, *, event_type: str, event: dict[str, Any]) -> str:
        if event_type == "session.output_transcript.delta" or "output_transcript" in event_type:
            return self._event_text_delta(event)
        return ""

    def _original_delta_from_event(self, *, event_type: str, event: dict[str, Any]) -> str:
        if event_type == "session.input_transcript.delta" or "input_transcript" in event_type:
            return self._event_text_delta(event)
        return ""

    def _event_text_delta(self, event: dict[str, Any]) -> str:
        for key in ("delta", "text", "transcript"):
            value = event.get(key)
            if value:
                return str(value)
        item = event.get("item")
        if isinstance(item, dict):
            for key in ("text", "transcript"):
                value = item.get(key)
                if value:
                    return str(value)
        return ""

    def _log_openai_event(self, *, session: MeetingTranslationSession, event: dict[str, Any], event_type: str) -> None:
        count = session.openai_event_counts.get(event_type, 0)
        has_text = bool(self._event_text_delta(event))
        if count not in {1, 10, 50} and count % 100 != 0:
            return
        LOGGER.warning(
            "meeting_translation_openai_event %s",
            json.dumps(
                {
                    "session_id": session.session_id[:8],
                    "event_type": event_type,
                    "count": count,
                    "keys": sorted(str(key) for key in event.keys()),
                    "has_text_delta": has_text,
                    "translated_deltas": session.translated_delta_count,
                    "original_deltas": session.original_delta_count,
                    "audio_seconds_sent": round(session.audio_bytes_sent / MEETING_TRANSLATION_PCM_BYTES_PER_SECOND, 1),
                },
                sort_keys=True,
            ),
        )

    def _log_session_event(self, session: MeetingTranslationSession, event: str, **payload: Any) -> None:
        data = {
            "event": event,
            "session_id": session.session_id[:8],
            "owner_hash": self._hash_for_log(session.owner_email),
            "target_language": session.target_language,
            "status": session.status,
            "audio_seconds_sent": round(session.audio_bytes_sent / MEETING_TRANSLATION_PCM_BYTES_PER_SECOND, 1),
            "audio_append_count": session.audio_append_count,
            "translated_deltas": session.translated_delta_count,
            "original_deltas": session.original_delta_count,
            **payload,
        }
        self._log_debug(event, **data)

    def _log_debug(self, event_name: str, **payload: Any) -> None:
        LOGGER.warning("meeting_translation_debug %s", json.dumps({"event": event_name, **payload}, sort_keys=True, default=str))

    def _hash_for_log(self, value: str) -> str:
        return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()[:12]

    def _safe_error(self, error: BaseException) -> str:
        message = str(error or "").strip() or error.__class__.__name__
        return message[:500]

    def _public_openai_error(self, event: dict[str, Any]) -> str:
        error = event.get("error") if isinstance(event.get("error"), dict) else {}
        message = str(error.get("message") or event.get("message") or "OpenAI realtime translation failed.").strip()
        return message[:500]

    def _public_error_message(self, error: Exception) -> str:
        message = str(error or "").strip() or "Meeting Translation failed."
        if "api key" in message.lower() or "authorization" in message.lower():
            return "OpenAI realtime translation is not configured or authorized. Check MEETING_TRANSLATION_OPENAI_API_KEY."
        if "screencapturekit" in message.lower() or "screen recording" in message.lower() or "microphone" in message.lower():
            return f"{message} Check macOS Screen Recording and Microphone permissions, then start a new translation."
        return message[:500]
