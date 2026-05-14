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

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.llm_call_ledger import (
    estimate_prompt_tokens,
    infer_llm_flow,
    prompt_sha256,
    record_llm_call,
)
from bpmis_jira_tool.source_code_qa_runtime_policy import (
    CODEX_SESSION_MODE_EPHEMERAL,
    CODEX_SESSION_MODE_RESUME,
    DEFAULT_LLM_TIMEOUT_SECONDS,
)
from bpmis_jira_tool.source_code_qa_types import (
    LLMGenerateResult,
)


LOGGER = logging.getLogger(__name__)

LLM_PROVIDER_CODEX_CLI_BRIDGE = "codex_cli_bridge"
LLM_PROVIDER_ALLOWED_QUERY_CHOICES = {LLM_PROVIDER_CODEX_CLI_BRIDGE}


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
        prompt = self._prompt_from_llm_payload(payload)
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
        reasoning_effort = self._reasoning_effort_from_payload(payload)
        timeout_seconds = max(10, int(payload.get("_timeout_seconds") or self.timeout_seconds))
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as output_file:
            prompt_mode = str(payload.get("codex_prompt_mode") or "").strip()
            codex_cli_session_id = str(payload.get("codex_cli_session_id") or "").strip()
            command, command_mode = self._build_codex_command(
                output_file=output_file.name,
                model=model,
                reasoning_effort=reasoning_effort,
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
                self._record_llm_call_ledger(
                    payload=payload,
                    prompt=prompt,
                    prompt_mode=prompt_mode,
                    model=model,
                    reasoning_effort=reasoning_effort,
                    status="timeout",
                    error_category="timeout",
                    error=str(error),
                    started_at=started_at,
                    command_mode=command_mode,
                    queue_wait_ms=queue_wait_ms,
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
                self._record_llm_call_ledger(
                    payload=payload,
                    prompt=prompt,
                    prompt_mode=prompt_mode,
                    model=model,
                    reasoning_effort=reasoning_effort,
                    status="error",
                    error_category="os_error",
                    error=str(error),
                    started_at=started_at,
                    command_mode=command_mode,
                    queue_wait_ms=queue_wait_ms,
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
            self._record_llm_call_ledger(
                payload=payload,
                prompt=prompt,
                prompt_mode=prompt_mode,
                model=model,
                reasoning_effort=reasoning_effort,
                status="error",
                error_category="nonzero_exit",
                error=detail[:500],
                started_at=started_at,
                command_mode=command_mode,
                queue_wait_ms=queue_wait_ms,
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
            self._record_llm_call_ledger(
                payload=payload,
                prompt=prompt,
                prompt_mode=prompt_mode,
                model=model,
                reasoning_effort=reasoning_effort,
                status="error",
                error_category="api_error_payload",
                error=error_answer,
                started_at=started_at,
                command_mode=command_mode,
                queue_wait_ms=queue_wait_ms,
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
            self._record_llm_call_ledger(
                payload=payload,
                prompt=prompt,
                prompt_mode=prompt_mode,
                model=model,
                reasoning_effort=reasoning_effort,
                status="error",
                error_category="empty_answer",
                error="Codex CLI returned no readable answer.",
                started_at=started_at,
                command_mode=command_mode,
                queue_wait_ms=queue_wait_ms,
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
        self._record_llm_call_ledger(
            payload=payload,
            prompt=prompt,
            prompt_mode=prompt_mode,
            model=model,
            reasoning_effort=reasoning_effort,
            status="ok",
            started_at=started_at,
            command_mode=command_mode,
            queue_wait_ms=queue_wait_ms,
            latency_ms=latency_ms,
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
                    "reasoning_effort": reasoning_effort,
                    "concurrency_limit": self.concurrency_limit,
                    "queue_wait_ms": queue_wait_ms,
                    "session_mode": self.session_mode,
                    "command_mode": command_mode,
                    "codex_cli_session_id": trace.get("session_id") or "",
                    "command": self._command_summary(command),
                },
            ),
        )

    def _record_llm_call_ledger(
        self,
        *,
        payload: dict[str, Any],
        prompt: str,
        prompt_mode: str,
        model: str,
        reasoning_effort: str,
        status: str,
        started_at: float,
        command_mode: str,
        queue_wait_ms: int,
        latency_ms: int | None = None,
        error_category: str = "",
        error: str = "",
    ) -> None:
        prompt_chars = int(payload.get("_codex_prompt_chars") or len(prompt))
        prompt_bytes = int(payload.get("_codex_prompt_bytes") or len(prompt.encode("utf-8")))
        estimated_tokens = estimate_prompt_tokens(prompt, payload.get("_codex_estimated_prompt_tokens"))
        flow = str(payload.get("_llm_ledger_flow") or infer_llm_flow(prompt_mode))
        route = str(payload.get("_llm_ledger_route") or "")
        record_llm_call(
            provider=self.name,
            flow=flow,
            prompt_mode=prompt_mode,
            route=route,
            model_id=model,
            reasoning_effort=reasoning_effort,
            status=status,
            latency_ms=int(latency_ms if latency_ms is not None else (time.time() - started_at) * 1000),
            estimated_prompt_tokens=estimated_tokens,
            prompt_chars=prompt_chars,
            prompt_bytes=prompt_bytes,
            prompt_sha256=prompt_sha256(prompt),
            cache_hit=bool(payload.get("_llm_cache_hit")),
            repair_attempted=bool(payload.get("_llm_repair_attempted") or payload.get("_codex_phase") == "repair"),
            error_category=error_category,
            error=error,
            trace_id=str(payload.get("_codex_trace_id") or payload.get("_llm_trace_id") or ""),
            session_mode=self.session_mode,
            command_mode=command_mode,
            queue_wait_ms=queue_wait_ms,
            attempt_count=1,
            extra={
                "codex_phase": str(payload.get("_codex_phase") or ""),
                "candidate_path_count": int(payload.get("_codex_candidate_path_count") or 0),
                "candidate_repo_count": int(payload.get("_codex_candidate_repo_count") or 0),
                "repair_issue_count": int(payload.get("_codex_repair_issue_count") or 0),
            },
        )

    def _build_codex_command(
        self,
        *,
        output_file: str,
        model: str,
        reasoning_effort: str = "",
        session_id: str = "",
        image_paths: list[str] | None = None,
    ) -> tuple[list[str], str]:
        image_args: list[str] = []
        for image_path in image_paths or []:
            image_args.extend(["--image", str(image_path)])
        reasoning_args = self._reasoning_config_args(reasoning_effort)
        if self.session_mode == CODEX_SESSION_MODE_RESUME:
            command = [self.codex_binary, "exec"]
            command.extend(reasoning_args)
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
        if reasoning_args:
            command[2:2] = reasoning_args
        if model not in {"codex-cli", "codex"}:
            command[2:2] = ["--model", model]
        if image_args:
            insert_at = 2
            if model not in {"codex-cli", "codex"}:
                insert_at = 4
            if reasoning_args:
                insert_at += len(reasoning_args)
            command[insert_at:insert_at] = image_args
        return command, "ephemeral"

    @staticmethod
    def _reasoning_effort_from_payload(payload: dict[str, Any]) -> str:
        effort = str(payload.get("_codex_reasoning_effort") or "").strip().lower()
        return effort if effort in {"low", "medium", "high", "xhigh"} else ""

    @classmethod
    def _reasoning_config_args(cls, reasoning_effort: str) -> list[str]:
        effort = cls._reasoning_effort_from_payload({"_codex_reasoning_effort": reasoning_effort})
        if not effort:
            return []
        return ["-c", f'model_reasoning_effort="{effort}"']

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
    def _prompt_from_llm_payload(payload: dict[str, Any]) -> str:
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
