from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import queue
import re
import shutil
import subprocess
import tempfile
import threading
import time
from typing import Any

import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa_embeddings import (
    OPENAI_COMPATIBLE_API_BASE_URL,
    VERTEX_AI_GLOBAL_API_BASE_URL,
)
from bpmis_jira_tool.source_code_qa_runtime_policy import (
    CODEX_SESSION_MODE_EPHEMERAL,
    CODEX_SESSION_MODE_RESUME,
    DEFAULT_LLM_BACKOFF_SECONDS,
    DEFAULT_LLM_MAX_BACKOFF_SECONDS,
    DEFAULT_LLM_MAX_RETRIES,
    DEFAULT_LLM_TIMEOUT_SECONDS,
    GEMINI_API_BASE_URL,
)
from bpmis_jira_tool.source_code_qa_types import (
    LLMGenerateResult,
    SourceCodeQALLMError,
)


LOGGER = logging.getLogger(__name__)

LLM_PROVIDER_GEMINI = "gemini"
LLM_PROVIDER_OPENAI_COMPATIBLE = "openai_compatible"
LLM_PROVIDER_CODEX_CLI_BRIDGE = "codex_cli_bridge"
LLM_PROVIDER_VERTEX_AI = "vertex_ai"
LLM_PROVIDER_ALLOWED_QUERY_CHOICES = {LLM_PROVIDER_GEMINI, LLM_PROVIDER_CODEX_CLI_BRIDGE, LLM_PROVIDER_VERTEX_AI}


class SourceCodeQALLMProvider:
    name = "unknown"

    def ready(self) -> bool:
        return False

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        raise ToolError("The configured Source Code Q&A LLM provider is not supported yet.")

    def extract_text(self, payload: dict[str, Any]) -> str:
        raise ToolError("The configured Source Code Q&A LLM provider returned an unreadable answer.")

    def public_config(self) -> dict[str, Any]:
        return {"provider": self.name, "ready": self.ready()}


class CodexCliBridgeSourceCodeQALLMProvider(SourceCodeQALLMProvider):
    name = LLM_PROVIDER_CODEX_CLI_BRIDGE
    _semaphore_lock = threading.Lock()
    _run_semaphore = threading.BoundedSemaphore(1)
    _semaphore_limit = 1

    def __init__(
        self,
        *,
        workspace_root: Path,
        timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        concurrency_limit: int = 1,
        session_mode: str = CODEX_SESSION_MODE_EPHEMERAL,
        codex_binary: str | None = None,
    ) -> None:
        self.workspace_root = Path(workspace_root)
        self.timeout_seconds = max(10, int(timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.concurrency_limit = max(1, min(int(concurrency_limit or 1), 4))
        normalized_session_mode = str(session_mode or CODEX_SESSION_MODE_EPHEMERAL).strip().lower()
        self.session_mode = normalized_session_mode if normalized_session_mode in {CODEX_SESSION_MODE_EPHEMERAL, CODEX_SESSION_MODE_RESUME} else CODEX_SESSION_MODE_EPHEMERAL
        self.codex_binary = str(codex_binary or os.getenv("SOURCE_CODE_QA_CODEX_BINARY") or "codex").strip() or "codex"

    @classmethod
    def _semaphore_for_limit(cls, limit: int) -> threading.BoundedSemaphore:
        normalized_limit = max(1, min(int(limit or 1), 4))
        with cls._semaphore_lock:
            if cls._semaphore_limit != normalized_limit:
                cls._run_semaphore = threading.BoundedSemaphore(normalized_limit)
                cls._semaphore_limit = normalized_limit
            return cls._run_semaphore

    def ready(self) -> bool:
        if not shutil.which(self.codex_binary):
            return False
        try:
            result = subprocess.run(
                [self.codex_binary, "login", "status"],
                cwd=str(self.workspace_root),
                text=True,
                capture_output=True,
                env=self._codex_env(),
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        output = f"{result.stdout}\n{result.stderr}"
        return result.returncode == 0 and "Logged in using ChatGPT" in output

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        del fallback_model
        if not self.ready():
            raise ToolError("Codex is unavailable. Run `codex login` with ChatGPT on this server before using Codex mode.")
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        progress_callback = payload.get("_progress_callback") if callable(payload.get("_progress_callback")) else None
        prompt = self._prompt_from_gemini_payload(payload)
        failure_context = self._codex_failure_context_from_payload(payload)
        image_paths = [
            str(path or "").strip()
            for path in (payload.get("_codex_image_paths") or [])
            if str(path or "").strip()
        ]
        for image_path in image_paths:
            path = Path(image_path)
            if not path.exists() or not path.is_file():
                raise ToolError(f"Codex image attachment is missing or unreadable: {image_path}")
        started_at = time.time()
        attempt_started = time.time()
        model = str(primary_model or "codex-cli").strip() or "codex-cli"
        timeout_seconds = max(10, int(payload.get("_timeout_seconds") or self.timeout_seconds))
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as output_file:
            prompt_mode = str(payload.get("codex_prompt_mode") or "").strip()
            codex_cli_session_id = str(payload.get("codex_cli_session_id") or "").strip()
            command, command_mode = self._build_codex_command(
                output_file=output_file.name,
                model=model,
                session_id=codex_cli_session_id,
                image_paths=image_paths,
            )
            queue_started = time.time()
            queue_wait_ms = 0
            try:
                semaphore = self._semaphore_for_limit(self.concurrency_limit)
                if progress_callback:
                    progress_callback(
                        "codex_queue",
                        f"Waiting for Codex slot ({self.concurrency_limit} max concurrent).",
                        0,
                        1,
                    )
                semaphore.acquire()
                queue_wait_ms = int((time.time() - queue_started) * 1000)
                try:
                    if progress_callback and queue_wait_ms > 250:
                        progress_callback(
                            "codex_queue",
                            f"Codex slot acquired after {queue_wait_ms / 1000:.1f}s.",
                            1,
                            1,
                        )
                    if progress_callback:
                        result = self._run_codex_streaming(
                            command=command,
                            prompt=prompt,
                            progress_callback=progress_callback,
                            timeout_seconds=timeout_seconds,
                        )
                    else:
                        result = subprocess.run(
                            command,
                            input=prompt,
                            cwd=str(self.workspace_root),
                            text=True,
                            capture_output=True,
                            env=self._codex_env(),
                            timeout=timeout_seconds,
                            check=False,
                    )
                finally:
                    semaphore.release()
            except subprocess.TimeoutExpired as error:
                self._log_codex_failure(
                    reason="timeout",
                    command=command,
                    command_mode=command_mode,
                    model=model,
                    prompt_mode=prompt_mode,
                    queue_wait_ms=queue_wait_ms,
                    started_at=started_at,
                    attempt_started=attempt_started,
                    timeout=True,
                    error=str(error),
                    context=failure_context,
                )
                raise ToolError(f"Codex unavailable; used code search fallback. Codex CLI timed out after {timeout_seconds}s.") from error
            except OSError as error:
                self._log_codex_failure(
                    reason="os_error",
                    command=command,
                    command_mode=command_mode,
                    model=model,
                    prompt_mode=prompt_mode,
                    queue_wait_ms=queue_wait_ms,
                    started_at=started_at,
                    attempt_started=attempt_started,
                    error=str(error),
                    context=failure_context,
                )
                raise ToolError(f"Codex unavailable; used code search fallback. {error}") from error
            output_file.seek(0)
            answer = output_file.read().strip()
        if result.returncode != 0:
            detail = self._sanitize_cli_output(f"{result.stderr}\n{result.stdout}")
            self._log_codex_failure(
                reason="nonzero_exit",
                command=command,
                command_mode=command_mode,
                model=model,
                prompt_mode=prompt_mode,
                queue_wait_ms=queue_wait_ms,
                started_at=started_at,
                attempt_started=attempt_started,
                result=result,
                answer=answer,
                error=detail[:500],
                context=failure_context,
            )
            raise ToolError(f"Codex unavailable; used code search fallback. Codex CLI exited with {result.returncode}. {detail[:500]}")
        if not answer:
            answer = self._extract_last_json_event_message(result.stdout)
        error_answer = self._codex_error_answer_detail(answer)
        if error_answer:
            self._log_codex_failure(
                reason="api_error_payload",
                command=command,
                command_mode=command_mode,
                model=model,
                prompt_mode=prompt_mode,
                queue_wait_ms=queue_wait_ms,
                started_at=started_at,
                attempt_started=attempt_started,
                result=result,
                answer=answer,
                error=error_answer,
                context=failure_context,
            )
            raise ToolError(f"Codex unavailable; used code search fallback. Codex CLI returned API error: {error_answer[:500]}")
        if not answer:
            self._log_codex_failure(
                reason="empty_answer",
                command=command,
                command_mode=command_mode,
                model=model,
                prompt_mode=prompt_mode,
                queue_wait_ms=queue_wait_ms,
                started_at=started_at,
                attempt_started=attempt_started,
                result=result,
                context=failure_context,
            )
            raise ToolError("Codex unavailable; used code search fallback. Codex CLI returned no readable answer.")
        latency_ms = int((time.time() - started_at) * 1000)
        trace = self._extract_codex_trace(result.stdout, result.stderr)
        trace.update(
            {
                "session_mode": self.session_mode,
                "command_mode": command_mode,
                "session_id": trace.get("session_id") or codex_cli_session_id,
                "exit_code": result.returncode,
                "latency_ms": latency_ms,
                "timeout": False,
            }
        )
        return LLMGenerateResult(
            payload={
                "text": answer,
                "finish_reason": "codex_cli_completed",
                "codex_cli_trace": trace,
            },
            usage={},
            model=model,
            attempts=1,
            latency_ms=latency_ms,
            attempt_log=(
                {
                    "model": model,
                    "attempt": 1,
                    "status": "ok",
                    "retryable": False,
                    "latency_ms": int((time.time() - attempt_started) * 1000),
                    "provider": self.name,
                    "exit_code": result.returncode,
                    "timeout": False,
                    "workspace_root": str(self.workspace_root),
                    "prompt_mode": prompt_mode,
                    "concurrency_limit": self.concurrency_limit,
                    "queue_wait_ms": queue_wait_ms,
                    "session_mode": self.session_mode,
                    "command_mode": command_mode,
                    "codex_cli_session_id": trace.get("session_id") or "",
                    "command": self._command_summary(command),
                },
            ),
        )

    def _build_codex_command(
        self,
        *,
        output_file: str,
        model: str,
        session_id: str = "",
        image_paths: list[str] | None = None,
    ) -> tuple[list[str], str]:
        image_args: list[str] = []
        for image_path in image_paths or []:
            image_args.extend(["--image", str(image_path)])
        if self.session_mode == CODEX_SESSION_MODE_RESUME:
            command = [self.codex_binary, "exec"]
            if model not in {"codex-cli", "codex"}:
                command.extend(["--model", model])
            command.extend(image_args)
            if session_id:
                command.extend(
                    [
                        "resume",
                        "--skip-git-repo-check",
                        "--json",
                        "--output-last-message",
                        output_file,
                        session_id,
                        "-",
                    ]
                )
                return command, "resume"
            command.extend(
                [
                    "--cd",
                    str(self.workspace_root),
                    "--skip-git-repo-check",
                    "--sandbox",
                    "read-only",
                    "--json",
                    "--output-last-message",
                    output_file,
                    "-",
                ]
            )
            return command, "new_persistent"
        command = [
            self.codex_binary,
            "exec",
            "--cd",
            str(self.workspace_root),
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--ephemeral",
            "--json",
            "--output-last-message",
            output_file,
            "-",
        ]
        if model not in {"codex-cli", "codex"}:
            command[2:2] = ["--model", model]
        if image_args:
            insert_at = 2
            if model not in {"codex-cli", "codex"}:
                insert_at = 4
            command[insert_at:insert_at] = image_args
        return command, "ephemeral"

    def _run_codex_streaming(self, *, command: list[str], prompt: str, progress_callback: Any, timeout_seconds: int | None = None) -> Any:
        process = subprocess.Popen(
            command,
            cwd=str(self.workspace_root),
            env=self._codex_env(),
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
        )
        output_queue: queue.Queue[tuple[str, str]] = queue.Queue()

        def read_pipe(name: str, pipe: Any) -> None:
            try:
                for line in iter(pipe.readline, ""):
                    output_queue.put((name, line))
            finally:
                try:
                    pipe.close()
                except Exception:
                    pass

        stdout_thread = threading.Thread(target=read_pipe, args=("stdout", process.stdout), daemon=True)
        stderr_thread = threading.Thread(target=read_pipe, args=("stderr", process.stderr), daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        if process.stdin is not None:
            process.stdin.write(prompt)
            process.stdin.close()

        started_at = time.time()
        timeout_seconds = max(10, int(timeout_seconds or self.timeout_seconds))
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []
        last_message = ""
        while process.poll() is None or not output_queue.empty():
            if time.time() - started_at > timeout_seconds:
                process.kill()
                raise subprocess.TimeoutExpired(command, timeout_seconds)
            try:
                stream_name, line = output_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if stream_name == "stdout":
                stdout_lines.append(line)
                message = self._extract_progress_json_event_message(line)
                if message and message != last_message:
                    last_message = message
                    try:
                        progress_callback("codex_stream", message[-900:], 0, 0)
                    except Exception:
                        pass
            else:
                stderr_lines.append(line)
        stdout_thread.join(timeout=0.2)
        stderr_thread.join(timeout=0.2)
        return subprocess.CompletedProcess(
            command,
            process.returncode,
            stdout="".join(stdout_lines),
            stderr="".join(stderr_lines),
        )

    def _codex_env(self) -> dict[str, str]:
        env = dict(os.environ)
        path_parts = [part for part in str(env.get("PATH") or "").split(os.pathsep) if part]
        for tool_dir in reversed(self._codex_tool_path_dirs()):
            if tool_dir not in path_parts:
                path_parts.insert(0, tool_dir)
        if path_parts:
            env["PATH"] = os.pathsep.join(path_parts)
        return env

    @staticmethod
    def _codex_tool_path_dirs() -> list[str]:
        dirs: list[str] = []
        detected = shutil.which("rg")
        if detected:
            dirs.append(str(Path(detected).parent))
        for candidate in (
            "/Applications/Codex.app/Contents/Resources/rg",
            "/opt/homebrew/bin/rg",
            "/usr/local/bin/rg",
            "/usr/bin/rg",
        ):
            if Path(candidate).exists():
                tool_dir = str(Path(candidate).parent)
                if tool_dir not in dirs:
                    dirs.append(tool_dir)
        return dirs

    @staticmethod
    def _codex_rg_hint() -> str:
        detected = shutil.which("rg")
        if detected:
            return str(detected)
        for candidate in (
            "/Applications/Codex.app/Contents/Resources/rg",
            "/opt/homebrew/bin/rg",
            "/usr/local/bin/rg",
            "/usr/bin/rg",
        ):
            if Path(candidate).exists():
                return candidate
        return ""

    def extract_text(self, payload: dict[str, Any]) -> str:
        text = str(payload.get("text") or "").strip()
        if text:
            return text
        raise ToolError("Codex CLI returned no readable answer.")

    @staticmethod
    def _codex_error_answer_detail(answer: str) -> str:
        text = str(answer or "").strip()
        if not text:
            return ""
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return ""
        if not isinstance(payload, dict):
            return ""
        fields = [
            payload.get("detail"),
            payload.get("error"),
            payload.get("message"),
            (payload.get("error") or {}).get("message") if isinstance(payload.get("error"), dict) else "",
        ]
        detail = " ".join(str(item or "").strip() for item in fields if str(item or "").strip())
        if not detail:
            return ""
        lowered = detail.lower()
        if "bad request" in lowered or "invalid request" in lowered or "api error" in lowered:
            return detail
        return ""

    @staticmethod
    def _codex_failure_context_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
        fields = (
            "_codex_trace_id",
            "_codex_phase",
            "_codex_prompt_chars",
            "_codex_prompt_bytes",
            "_codex_estimated_prompt_tokens",
            "_codex_candidate_path_count",
            "_codex_candidate_repo_count",
            "_codex_repair_issue_count",
        )
        return {
            key.removeprefix("_codex_"): payload.get(key)
            for key in fields
            if isinstance(payload.get(key), (str, int, float, bool))
        }

    @classmethod
    def _tail_for_log(cls, text: str, limit: int = 1200) -> str:
        return cls._sanitize_cli_output(text)[-max(100, int(limit)):]

    @classmethod
    def _log_codex_failure(
        cls,
        *,
        reason: str,
        command: list[str],
        command_mode: str,
        model: str,
        prompt_mode: str,
        queue_wait_ms: int,
        started_at: float,
        attempt_started: float,
        context: dict[str, Any],
        result: Any | None = None,
        answer: str = "",
        error: str = "",
        timeout: bool = False,
    ) -> None:
        payload: dict[str, Any] = {
            "event": "source_code_qa_codex_failure",
            "reason": str(reason or "unknown"),
            "provider": LLM_PROVIDER_CODEX_CLI_BRIDGE,
            "model": str(model or ""),
            "prompt_mode": str(prompt_mode or ""),
            "command_mode": str(command_mode or ""),
            "command": cls._command_summary(command),
            "queue_wait_ms": max(0, int(queue_wait_ms or 0)),
            "latency_ms": max(0, int((time.time() - started_at) * 1000)),
            "attempt_latency_ms": max(0, int((time.time() - attempt_started) * 1000)),
            "timeout": bool(timeout),
        }
        payload.update({key: value for key, value in (context or {}).items() if isinstance(value, (str, int, float, bool))})
        if result is not None:
            payload["exit_code"] = getattr(result, "returncode", None)
            payload["stdout_tail"] = cls._tail_for_log(str(getattr(result, "stdout", "") or ""))
            payload["stderr_tail"] = cls._tail_for_log(str(getattr(result, "stderr", "") or ""))
        if answer:
            payload["answer_tail"] = cls._tail_for_log(answer)
        if error:
            payload["error"] = cls._tail_for_log(error, limit=500)
        LOGGER.warning("source_code_qa_codex_failure %s", json.dumps(payload, ensure_ascii=False, sort_keys=True))

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "workspace_root": str(self.workspace_root),
            "runtime": {
                "timeout_seconds": self.timeout_seconds,
                "concurrency": self.concurrency_limit,
                "sandbox": "read-only",
                "session_mode": self.session_mode,
            },
        }

    @staticmethod
    def _prompt_from_gemini_payload(payload: dict[str, Any]) -> str:
        system_text = "\n".join(
            str(part.get("text") or "").strip()
            for part in (payload.get("systemInstruction") or {}).get("parts") or []
            if str(part.get("text") or "").strip()
        )
        user_parts: list[str] = []
        for content in payload.get("contents") or []:
            for part in content.get("parts") or []:
                text = str(part.get("text") or "").strip()
                if text:
                    user_parts.append(text)
        user_text = "\n\n".join(user_parts)
        return (
            f"{system_text}\n\n"
            "Codex CLI bridge policy:\n"
            "- Read only from the provided repository workspace and retrieval evidence.\n"
            "- Do not modify files, create commits, deploy, install dependencies, or run write commands.\n"
            f"- Tool availability: `rg` is expected on PATH. If not, call it by absolute path: {CodexCliBridgeSourceCodeQALLMProvider._codex_rg_hint() or 'not detected; use grep -R/find fallback'}.\n"
            "- Return the answer in the requested JSON shape when possible.\n\n"
            f"{user_text}"
        ).strip()

    @staticmethod
    def _extract_last_json_event_message(output: str) -> str:
        answer = ""
        for raw_line in str(output or "").splitlines():
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            for key in ("message", "text", "output_text"):
                value = event.get(key)
                if isinstance(value, str) and value.strip():
                    answer = value.strip()
            item = event.get("item")
            if isinstance(item, dict):
                content = item.get("content")
                if isinstance(content, str) and content.strip():
                    answer = content.strip()
        return answer

    @staticmethod
    def _extract_progress_json_event_message(output: str) -> str:
        try:
            event = json.loads(str(output or ""))
        except json.JSONDecodeError:
            return ""
        if not isinstance(event, dict):
            return ""
        candidates: list[str] = []
        for key in ("message", "text", "output_text", "delta"):
            value = event.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())
        item = event.get("item")
        if isinstance(item, dict):
            for key in ("text", "message", "output_text", "delta", "content"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append(value.strip())
            content = item.get("content")
            if isinstance(content, list):
                for part in content:
                    if isinstance(part, dict):
                        value = part.get("text") or part.get("output_text")
                        if isinstance(value, str) and value.strip():
                            candidates.append(value.strip())
        return candidates[-1] if candidates else ""

    @classmethod
    def _extract_codex_trace(cls, stdout: str, stderr: str) -> dict[str, Any]:
        stream_messages: list[str] = []
        command_summaries: list[str] = []
        inspected_paths: list[str] = []
        session_id = ""
        path_pattern = re.compile(
            r"([A-Za-z0-9_.@/$-]+/(?:src|config|spec|app|web|test|tests|resources|pages|components|mapper)/"
            r"[A-Za-z0-9_./$@-]+\.(?:java|xml|kt|groovy|md|sql|yml|yaml|properties|json|ts|tsx|js|py))"
        )
        command_pattern = re.compile(r"\b(rg|grep|find|sed|nl|cat|ls)\b(?:\s+[^`'\n]{0,220})?")
        seen_messages: set[str] = set()
        seen_commands: set[str] = set()
        seen_paths: set[str] = set()
        for raw_line in f"{stdout or ''}\n{stderr or ''}".splitlines():
            message = cls._extract_progress_json_event_message(raw_line)
            if message and message not in seen_messages:
                seen_messages.add(message)
                stream_messages.append(message[:1200])
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError:
                event = {}
            if isinstance(event, dict):
                for key in ("session_id", "conversation_id"):
                    value = str(event.get(key) or "").strip()
                    if value:
                        session_id = value
                item = event.get("item")
                if isinstance(item, dict):
                    for key in ("session_id", "conversation_id", "id"):
                        value = str(item.get(key) or "").strip()
                        if value and ("session" in key or str(item.get("type") or "").lower().find("session") >= 0):
                            session_id = value
                    for key in ("command", "cmd"):
                        value = item.get(key)
                        if isinstance(value, str) and value.strip() and value not in seen_commands:
                            seen_commands.add(value)
                            command_summaries.append(value[:240])
                for key in ("command", "cmd"):
                    value = event.get(key)
                    if isinstance(value, str) and value.strip() and value not in seen_commands:
                        seen_commands.add(value)
                        command_summaries.append(value[:240])
            for match in command_pattern.finditer(raw_line):
                command = match.group(0).strip()
                if command and command not in seen_commands:
                    seen_commands.add(command)
                    command_summaries.append(command[:240])
            for match in path_pattern.finditer(raw_line):
                path = match.group(1).strip()
                if path and path not in seen_paths:
                    seen_paths.add(path)
                    inspected_paths.append(path[:300])
        return {
            "stream_messages": stream_messages[-40:],
            "command_summaries": command_summaries[-30:],
            "probable_inspected_files": inspected_paths[-40:],
            "session_id": session_id,
        }

    @staticmethod
    def _sanitize_cli_output(output: str) -> str:
        return re.sub(r"\s+", " ", str(output or "").strip())

    @staticmethod
    def _command_summary(command: list[str]) -> list[str]:
        summarized = list(command)
        if "--output-last-message" in summarized:
            index = summarized.index("--output-last-message")
            if index + 1 < len(summarized):
                summarized[index + 1] = "<output-file>"
        return summarized


class UnsupportedSourceCodeQALLMProvider(SourceCodeQALLMProvider):
    def __init__(self, name: str) -> None:
        self.name = str(name or "unknown")

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        raise ToolError(f"Source Code Q&A LLM provider {self.name!r} is not supported yet.")


class GeminiSourceCodeQALLMProvider(SourceCodeQALLMProvider):
    name = LLM_PROVIDER_GEMINI

    def __init__(
        self,
        *,
        api_key: str,
        api_base_url: str = GEMINI_API_BASE_URL,
        timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_LLM_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_LLM_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_LLM_MAX_BACKOFF_SECONDS,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.api_base_url = str(api_base_url or GEMINI_API_BASE_URL).rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.max_retries = max(0, int(max_retries or 0))
        self.backoff_seconds = max(0.0, float(backoff_seconds or 0.0))
        self.max_backoff_seconds = max(self.backoff_seconds, float(max_backoff_seconds or self.backoff_seconds or DEFAULT_LLM_MAX_BACKOFF_SECONDS))

    def ready(self) -> bool:
        return bool(self.api_key)

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        if not self.ready():
            raise ToolError("LLM mode is not configured yet. Set SOURCE_CODE_QA_GEMINI_API_KEY or GEMINI_API_KEY on the server first.")
        models = [primary_model]
        if fallback_model and fallback_model not in models:
            models.append(fallback_model)
        retryable_statuses = {429, 500, 502, 503, 504}
        last_error: str | None = None
        last_status_code: int | None = None
        last_provider_status = ""
        last_retry_after: float | None = None
        attempts = 0
        attempt_log: list[dict[str, Any]] = []
        started_at = time.time()
        delays = self._retry_delays()
        timeout_seconds = max(5, int(payload.get("_timeout_seconds") or self.timeout_seconds))
        for model in models:
            model_delays = list(delays)
            for attempt_index, delay in enumerate(model_delays):
                attempts += 1
                if delay:
                    time.sleep(delay)
                attempt_started = time.time()
                try:
                    response = requests.post(
                        f"{self.api_base_url}/models/{model}:generateContent",
                        params={"key": self.api_key},
                        headers={"Content-Type": "application/json"},
                        json=payload,
                        timeout=timeout_seconds,
                    )
                except requests.Timeout as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "timeout",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                except requests.RequestException as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "request_error",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                response_ok = getattr(response, "ok", None)
                if response_ok is None:
                    try:
                        response.raise_for_status()
                        response_ok = True
                    except requests.HTTPError:
                        response_ok = False
                if response_ok:
                    result = response.json()
                    usage = result.get("usageMetadata") or {}
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "ok",
                            "retryable": False,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    return LLMGenerateResult(
                        payload=result,
                        usage=usage,
                        model=model,
                        attempts=attempts,
                        latency_ms=int((time.time() - started_at) * 1000),
                        attempt_log=tuple(attempt_log),
                    )
                status = int(getattr(response, "status_code", 500) or 500)
                detail = self._sanitize_error_detail(response.text)
                last_error = detail
                last_status_code = status
                last_provider_status = self._provider_error_status(detail)
                retryable = status in retryable_statuses
                attempt_log.append(
                    {
                        "model": model,
                        "attempt": attempt_index + 1,
                        "status": status,
                        "retryable": retryable,
                        "latency_ms": int((time.time() - attempt_started) * 1000),
                    }
                )
                if not retryable:
                    raise ToolError(f"Gemini answer generation failed. {detail[:500]}")
                if attempt_index + 1 < len(model_delays):
                    retry_after = self._retry_after_seconds(response)
                    if retry_after is not None:
                        last_retry_after = retry_after
                        model_delays[attempt_index + 1] = retry_after
        raise SourceCodeQALLMError(
            f"Gemini answer generation failed. {str(last_error or 'Model unavailable.')[:500]}",
            status_code=last_status_code,
            provider_status=last_provider_status,
            retryable=bool(last_status_code in retryable_statuses),
            retry_after_seconds=last_retry_after,
        )

    def extract_text(self, payload: dict[str, Any]) -> str:
        candidates = payload.get("candidates") or []
        for candidate in candidates:
            content = candidate.get("content") or {}
            parts = content.get("parts") or []
            texts = [str(part.get("text") or "").strip() for part in parts if str(part.get("text") or "").strip()]
            if texts:
                return "\n".join(texts).strip()
        raise ToolError("Gemini returned no readable answer.")

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "api_base_url": self.api_base_url,
            "runtime": self.runtime_config(),
        }

    def _sanitize_error_detail(self, detail: str) -> str:
        sanitized = str(detail or "")
        if self.api_key:
            sanitized = sanitized.replace(self.api_key, "***")
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", sanitized)

    def runtime_config(self) -> dict[str, Any]:
        return {
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "backoff_seconds": self.backoff_seconds,
            "max_backoff_seconds": self.max_backoff_seconds,
            "retryable_statuses": [429, 500, 502, 503, 504],
        }

    def _retry_delays(self) -> list[float]:
        delays = [0.0]
        for index in range(self.max_retries):
            delay = self.backoff_seconds * (2**index)
            delays.append(min(self.max_backoff_seconds, delay))
        return delays

    def _retry_after_seconds(self, response: Any) -> float | None:
        headers = getattr(response, "headers", {}) or {}
        raw_value = headers.get("Retry-After") if hasattr(headers, "get") else None
        if raw_value is None:
            return None
        try:
            delay = float(str(raw_value).strip())
        except ValueError:
            return None
        return max(0.0, min(self.max_backoff_seconds, delay))

    @staticmethod
    def _provider_error_status(detail: str) -> str:
        try:
            payload = json.loads(str(detail or ""))
        except json.JSONDecodeError:
            return ""
        error = payload.get("error") if isinstance(payload, dict) else {}
        if isinstance(error, dict):
            return str(error.get("status") or "").strip()
        return ""


class VertexAISourceCodeQALLMProvider(GeminiSourceCodeQALLMProvider):
    name = LLM_PROVIDER_VERTEX_AI
    _SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)

    def __init__(
        self,
        *,
        credentials_file: str | None = None,
        project_id: str | None = None,
        location: str = "global",
        timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_LLM_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_LLM_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_LLM_MAX_BACKOFF_SECONDS,
    ) -> None:
        self.credentials_file = str(credentials_file or "").strip()
        self.project_id = str(project_id or "").strip()
        self.location = str(location or "global").strip() or "global"
        self.timeout_seconds = max(5, int(timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.max_retries = max(0, int(max_retries or 0))
        self.backoff_seconds = max(0.0, float(backoff_seconds or 0.0))
        self.max_backoff_seconds = max(self.backoff_seconds, float(max_backoff_seconds or self.backoff_seconds or DEFAULT_LLM_MAX_BACKOFF_SECONDS))

    def ready(self) -> bool:
        return bool(self._credentials_path() and self._resolved_project_id() and self.location)

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        if not self.ready():
            raise ToolError(
                "Vertex AI mode is not configured yet. Set SOURCE_CODE_QA_VERTEX_CREDENTIALS_FILE "
                "or GOOGLE_APPLICATION_CREDENTIALS, plus SOURCE_CODE_QA_VERTEX_PROJECT_ID when the JSON has no project_id."
            )
        access_token = self._access_token()
        models = [primary_model]
        if fallback_model and fallback_model not in models:
            models.append(fallback_model)
        retryable_statuses = {429, 500, 502, 503, 504}
        last_error: str | None = None
        last_status_code: int | None = None
        last_provider_status = ""
        last_retry_after: float | None = None
        attempts = 0
        attempt_log: list[dict[str, Any]] = []
        started_at = time.time()
        delays = self._retry_delays()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        for model in models:
            model_delays = list(delays)
            for attempt_index, delay in enumerate(model_delays):
                attempts += 1
                if delay:
                    time.sleep(delay)
                attempt_started = time.time()
                try:
                    response = requests.post(
                        self._generate_content_url(model),
                        headers=headers,
                        json=self._payload_for_generate_content(payload),
                        timeout=self.timeout_seconds,
                    )
                except requests.Timeout as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "timeout",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                except requests.RequestException as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "request_error",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                response_ok = getattr(response, "ok", None)
                if response_ok is None:
                    try:
                        response.raise_for_status()
                        response_ok = True
                    except requests.HTTPError:
                        response_ok = False
                if response_ok:
                    result = response.json()
                    usage = result.get("usageMetadata") or {}
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "ok",
                            "retryable": False,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    return LLMGenerateResult(
                        payload=result,
                        usage=usage,
                        model=model,
                        attempts=attempts,
                        latency_ms=int((time.time() - started_at) * 1000),
                        attempt_log=tuple(attempt_log),
                    )
                status = int(getattr(response, "status_code", 500) or 500)
                detail = self._sanitize_error_detail(response.text)
                last_error = detail
                last_status_code = status
                last_provider_status = self._provider_error_status(detail)
                retryable = status in retryable_statuses
                attempt_log.append(
                    {
                        "model": model,
                        "attempt": attempt_index + 1,
                        "status": status,
                        "retryable": retryable,
                        "latency_ms": int((time.time() - attempt_started) * 1000),
                    }
                )
                if not retryable:
                    raise ToolError(f"Vertex AI answer generation failed. {detail[:500]}")
                if attempt_index + 1 < len(model_delays):
                    retry_after = self._retry_after_seconds(response)
                    if retry_after is not None:
                        last_retry_after = retry_after
                        model_delays[attempt_index + 1] = retry_after
        raise SourceCodeQALLMError(
            f"Vertex AI answer generation failed. {str(last_error or 'Model unavailable.')[:500]}",
            status_code=last_status_code,
            provider_status=last_provider_status,
            retryable=bool(last_status_code in retryable_statuses),
            retry_after_seconds=last_retry_after,
        )

    @staticmethod
    def _payload_for_generate_content(payload: dict[str, Any]) -> dict[str, Any]:
        normalized = json.loads(json.dumps(payload or {}, ensure_ascii=False))
        for content in normalized.get("contents") or []:
            if isinstance(content, dict) and not str(content.get("role") or "").strip():
                content["role"] = "user"
        return normalized

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "project_id": self._resolved_project_id(),
            "location": self.location,
            "credentials_configured": bool(self._credentials_path()),
            "runtime": self.runtime_config(),
        }

    def _credentials_path(self) -> Path | None:
        if not self.credentials_file:
            return None
        path = Path(self.credentials_file).expanduser()
        return path if path.exists() and path.is_file() else None

    def _resolved_project_id(self) -> str:
        if self.project_id:
            return self.project_id
        path = self._credentials_path()
        if path is None:
            return ""
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""
        return str(payload.get("project_id") or "").strip()

    def _access_token(self) -> str:
        path = self._credentials_path()
        if path is None:
            raise ToolError("Vertex AI credentials file is missing or unreadable.")
        credentials = service_account.Credentials.from_service_account_file(
            str(path),
            scopes=list(self._SCOPES),
        )
        credentials.refresh(GoogleAuthRequest())
        token = str(getattr(credentials, "token", "") or "").strip()
        if not token:
            raise ToolError("Vertex AI service account did not return an OAuth access token.")
        return token

    def _generate_content_url(self, model: str) -> str:
        location = self.location
        base_url = VERTEX_AI_GLOBAL_API_BASE_URL if location == "global" else f"https://{location}-aiplatform.googleapis.com/v1"
        project_id = self._resolved_project_id()
        return (
            f"{base_url}/projects/{project_id}/locations/{location}"
            f"/publishers/google/models/{model}:generateContent"
        )

    def _sanitize_error_detail(self, detail: str) -> str:
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", str(detail or ""))


class OpenAICompatibleSourceCodeQALLMProvider(SourceCodeQALLMProvider):
    name = LLM_PROVIDER_OPENAI_COMPATIBLE

    def __init__(
        self,
        *,
        api_key: str,
        api_base_url: str = OPENAI_COMPATIBLE_API_BASE_URL,
        timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_LLM_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_LLM_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_LLM_MAX_BACKOFF_SECONDS,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.api_base_url = str(api_base_url or OPENAI_COMPATIBLE_API_BASE_URL).rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.max_retries = max(0, int(max_retries or 0))
        self.backoff_seconds = max(0.0, float(backoff_seconds or 0.0))
        self.max_backoff_seconds = max(self.backoff_seconds, float(max_backoff_seconds or self.backoff_seconds or DEFAULT_LLM_MAX_BACKOFF_SECONDS))

    def ready(self) -> bool:
        return bool(self.api_key)

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        if not self.ready():
            raise ToolError("LLM mode is not configured yet. Set SOURCE_CODE_QA_OPENAI_API_KEY or OPENAI_API_KEY on the server first.")
        if self._has_inline_image_part(payload):
            raise ToolError("Current Source Code Q&A provider does not support image attachments. Use Codex or Vertex for image-based questions.")
        messages = self._messages_from_gemini_payload(payload)
        generation_config = payload.get("generationConfig") or {}
        models = [primary_model]
        if fallback_model and fallback_model not in models:
            models.append(fallback_model)
        retryable_statuses = {429, 500, 502, 503, 504}
        last_error: str | None = None
        last_status_code: int | None = None
        last_provider_status = ""
        last_retry_after: float | None = None
        attempts = 0
        attempt_log: list[dict[str, Any]] = []
        started_at = time.time()
        delays = self._retry_delays()
        for model in models:
            model_delays = list(delays)
            for attempt_index, delay in enumerate(model_delays):
                attempts += 1
                if delay:
                    time.sleep(delay)
                attempt_started = time.time()
                request_payload = {
                    "model": model,
                    "messages": messages,
                    "temperature": generation_config.get("temperature", 0.2),
                    "max_tokens": generation_config.get("maxOutputTokens", 900),
                    "response_format": {"type": "json_object"},
                }
                try:
                    response = requests.post(
                        f"{self.api_base_url}/chat/completions",
                        headers={
                            "Authorization": f"Bearer {self.api_key}",
                            "Content-Type": "application/json",
                        },
                        json=request_payload,
                        timeout=self.timeout_seconds,
                    )
                except requests.Timeout as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "timeout",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                except requests.RequestException as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "request_error",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                if response.ok:
                    result = response.json()
                    usage = result.get("usage") or {}
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "ok",
                            "retryable": False,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    return LLMGenerateResult(
                        payload=result,
                        usage=usage,
                        model=model,
                        attempts=attempts,
                        latency_ms=int((time.time() - started_at) * 1000),
                        attempt_log=tuple(attempt_log),
                    )
                detail = self._sanitize_error_detail(response.text)
                last_error = detail
                status = int(getattr(response, "status_code", 500) or 500)
                last_status_code = status
                last_provider_status = GeminiSourceCodeQALLMProvider._provider_error_status(detail)
                retryable = status in retryable_statuses
                attempt_log.append(
                    {
                        "model": model,
                        "attempt": attempt_index + 1,
                        "status": status,
                        "retryable": retryable,
                        "latency_ms": int((time.time() - attempt_started) * 1000),
                    }
                )
                if not retryable:
                    raise ToolError(f"OpenAI-compatible answer generation failed. {detail[:500]}")
                if attempt_index + 1 < len(model_delays):
                    retry_after = self._retry_after_seconds(response)
                    if retry_after is not None:
                        last_retry_after = retry_after
                        model_delays[attempt_index + 1] = retry_after
        raise SourceCodeQALLMError(
            f"OpenAI-compatible answer generation failed. {str(last_error or 'Model unavailable.')[:500]}",
            status_code=last_status_code,
            provider_status=last_provider_status,
            retryable=bool(last_status_code in retryable_statuses),
            retry_after_seconds=last_retry_after,
        )

    def extract_text(self, payload: dict[str, Any]) -> str:
        choices = payload.get("choices") or []
        for choice in choices:
            message = choice.get("message") or {}
            content = message.get("content")
            if isinstance(content, list):
                texts = [str(item.get("text") or "").strip() for item in content if isinstance(item, dict)]
                text = "\n".join(item for item in texts if item).strip()
            else:
                text = str(content or "").strip()
            if text:
                return text
        raise ToolError("OpenAI-compatible provider returned no readable answer.")

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "api_base_url": self.api_base_url,
            "runtime": self.runtime_config(),
        }

    @staticmethod
    def _has_inline_image_part(payload: dict[str, Any]) -> bool:
        for content in payload.get("contents") or []:
            for part in content.get("parts") or []:
                if isinstance(part, dict) and (part.get("inlineData") or part.get("inline_data")):
                    return True
        return False

    @staticmethod
    def _messages_from_gemini_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
        system_text = "\n".join(
            str(part.get("text") or "").strip()
            for part in (payload.get("systemInstruction") or {}).get("parts") or []
            if str(part.get("text") or "").strip()
        )
        user_parts: list[str] = []
        for content in payload.get("contents") or []:
            for part in content.get("parts") or []:
                text = str(part.get("text") or "").strip()
                if text:
                    user_parts.append(text)
        messages = []
        if system_text:
            messages.append({"role": "system", "content": system_text})
        messages.append({"role": "user", "content": "\n\n".join(user_parts)})
        return messages

    def _sanitize_error_detail(self, detail: str) -> str:
        sanitized = str(detail or "")
        if self.api_key:
            sanitized = sanitized.replace(self.api_key, "***")
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", sanitized)

    def runtime_config(self) -> dict[str, Any]:
        return {
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "backoff_seconds": self.backoff_seconds,
            "max_backoff_seconds": self.max_backoff_seconds,
            "retryable_statuses": [429, 500, 502, 503, 504],
        }

    def _retry_delays(self) -> list[float]:
        delays = [0.0]
        for index in range(self.max_retries):
            delay = self.backoff_seconds * (2**index)
            delays.append(min(self.max_backoff_seconds, delay))
        return delays

    def _retry_after_seconds(self, response: Any) -> float | None:
        headers = getattr(response, "headers", {}) or {}
        raw_value = headers.get("Retry-After") if hasattr(headers, "get") else None
        if raw_value is None:
            return None
        try:
            delay = float(str(raw_value).strip())
        except ValueError:
            return None
        return max(0.0, min(self.max_backoff_seconds, delay))
