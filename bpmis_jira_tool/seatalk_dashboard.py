from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
from pathlib import Path
import re
from threading import Lock
from typing import Any, Callable
from zoneinfo import ZoneInfo

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.source_code_qa import (
    CodexCliBridgeSourceCodeQALLMProvider,
    DEFAULT_CODEX_CLI_MODEL,
    DEFAULT_CODEX_TIMEOUT_SECONDS,
)


SEATALK_DASHBOARD_DEFAULT_DAYS = 7
SEATALK_DASHBOARD_CACHE_TTL_SECONDS = 300
SEATALK_DEFAULT_APP_PATH = "/Applications/SeaTalk.app"
SEATALK_DEFAULT_DATA_DIR = "~/Library/Application Support/SeaTalk"
SEATALK_INSIGHTS_PROMPT_MODE = "seatalk_7_day_insights_v4"
SEATALK_NAME_MAPPINGS_CANDIDATE_VERSION = "v2_daily_brief_sources"
SEATALK_INSIGHTS_TIMEZONE = ZoneInfo("Asia/Singapore")
SEATALK_INSIGHTS_HISTORY_MAX_CHARS = 520_000
SEATALK_INSIGHTS_TODO_HISTORY_MAX_CHARS = 260_000
SEATALK_INSIGHTS_SIGNAL_MAX_CHARS = 300_000
SEATALK_INSIGHTS_RECENT_MAX_CHARS = 190_000
SEATALK_INSIGHTS_TODO_SIGNAL_MAX_CHARS = 160_000
SEATALK_INSIGHTS_TODO_RECENT_MAX_CHARS = 80_000
SEATALK_PROJECT_UPDATES_HISTORY_MAX_CHARS = 760_000
SEATALK_PROJECT_UPDATES_SIGNAL_MAX_CHARS = 440_000
SEATALK_PROJECT_UPDATES_RECENT_MAX_CHARS = 280_000
SEATALK_TODO_HISTORY_MAX_CHARS = 620_000
SEATALK_TODO_SIGNAL_MAX_CHARS = 400_000
SEATALK_TODO_RECENT_MAX_CHARS = 180_000
UNAVAILABLE_REASON = "Not available from local SeaTalk desktop data for this scope."


@dataclass
class _SeaTalkDashboardCacheEntry:
    payload: dict[str, Any]
    expires_at: datetime


@dataclass
class _SeaTalkInsightsCacheEntry:
    payload: dict[str, Any]
    expires_at: datetime


def _run_subprocess(command: list[str], *, env: dict[str, str], timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


class SeaTalkDashboardService:
    _dashboard_cache: dict[tuple[str, str, int], _SeaTalkDashboardCacheEntry] = {}
    _dashboard_cache_lock = Lock()
    _insights_cache: dict[tuple[str, str, int, str, str], _SeaTalkInsightsCacheEntry] = {}
    _insights_cache_lock = Lock()

    def __init__(
        self,
        *,
        owner_email: str,
        seatalk_app_path: str = SEATALK_DEFAULT_APP_PATH,
        seatalk_data_dir: str = SEATALK_DEFAULT_DATA_DIR,
        codex_workspace_root: str | Path | None = None,
        codex_model: str | None = None,
        codex_timeout_seconds: int = DEFAULT_CODEX_TIMEOUT_SECONDS,
        codex_concurrency: int = 1,
        codex_binary: str | None = None,
        name_overrides_path: str | Path | None = None,
        daily_cache_dir: str | Path | None = None,
        command_runner: Callable[[list[str]], subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.owner_email = str(owner_email or "").strip().lower()
        self.seatalk_app_path = Path(str(seatalk_app_path or SEATALK_DEFAULT_APP_PATH)).expanduser()
        self.seatalk_data_dir = Path(str(seatalk_data_dir or SEATALK_DEFAULT_DATA_DIR)).expanduser()
        self.codex_workspace_root = Path(codex_workspace_root or Path.cwd()).expanduser()
        self.codex_model = str(codex_model or os.getenv("SOURCE_CODE_QA_CODEX_MODEL") or DEFAULT_CODEX_CLI_MODEL).strip() or DEFAULT_CODEX_CLI_MODEL
        self.codex_timeout_seconds = max(10, int(codex_timeout_seconds or DEFAULT_CODEX_TIMEOUT_SECONDS))
        self.codex_concurrency = max(1, min(int(codex_concurrency or 1), 4))
        self.codex_binary = str(codex_binary or os.getenv("SOURCE_CODE_QA_CODEX_BINARY") or "codex").strip() or "codex"
        self.name_overrides_path = Path(name_overrides_path).expanduser() if name_overrides_path else None
        self.daily_cache_dir = Path(daily_cache_dir).expanduser() if daily_cache_dir else None
        self._command_runner = command_runner
        if not self.owner_email:
            raise ConfigError("SeaTalk owner email is missing. Set SEATALK_OWNER_EMAIL first.")

    def build_overview(
        self,
        *,
        days: int = SEATALK_DASHBOARD_DEFAULT_DAYS,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = now or datetime.now().astimezone()
        cached = self._get_cached_dashboard(days=days, now=now)
        if cached is not None:
            return cached
        self._validate_local_environment()
        payload = self._load_local_payload(days=days, now=now)
        self._store_cached_dashboard(days=days, now=now, payload=payload)
        return payload

    def export_history_text(
        self,
        *,
        days: int = SEATALK_DASHBOARD_DEFAULT_DAYS,
        now: datetime | None = None,
    ) -> tuple[str, str]:
        now = now or datetime.now().astimezone()
        self._validate_local_environment()
        content = self._load_local_history_export(days=days, now=now)
        filename = f"seatalk-history-last-{days}-days.txt"
        return content, filename

    def export_history_since(
        self,
        *,
        since: datetime,
        now: datetime | None = None,
        days: int = SEATALK_DASHBOARD_DEFAULT_DAYS,
    ) -> str:
        now = now or datetime.now().astimezone()
        self._validate_local_environment()
        return self._load_local_history_export(days=days, now=now, since=since)

    def build_insights(
        self,
        *,
        days: int = SEATALK_DASHBOARD_DEFAULT_DAYS,
        now: datetime | None = None,
        todo_since: str | datetime | None = None,
    ) -> dict[str, Any]:
        now = now or datetime.now(SEATALK_INSIGHTS_TIMEZONE)
        todo_since_dt = self._parse_todo_since(todo_since, now=now, days=days)
        cached = self._get_cached_insights(days=days, now=now)
        if cached is not None:
            return cached
        daily_cached = self._load_daily_cache(kind="insights", days=days, now=now)
        if daily_cached is not None:
            self._store_cached_insights(days=days, now=now, payload=daily_cached)
            return self._mark_cache_hit(daily_cached, expires_at=self._insights_cache_expiry(now))
        self._validate_local_environment()
        history_text = self._load_local_history_export(days=days, now=now)
        if not history_text.strip():
            payload = self._empty_insights_payload(days=days, now=now, cache_hit=False)
            self._store_cached_insights(days=days, now=now, payload=payload)
            self._store_daily_cache(kind="insights", days=days, now=now, payload=payload)
            return payload
        history_text = self._filter_system_generated_history(history_text)
        history_text = self._compact_history_for_insights(
            history_text,
            max_chars=SEATALK_INSIGHTS_HISTORY_MAX_CHARS,
            signal_max_chars=SEATALK_INSIGHTS_SIGNAL_MAX_CHARS,
            recent_max_chars=SEATALK_INSIGHTS_RECENT_MAX_CHARS,
        )
        if todo_since_dt is None:
            todo_history_text = ""
        else:
            todo_history_text = self._load_local_history_export(days=days, now=now, since=todo_since_dt)
            todo_history_text = self._filter_system_generated_history(todo_history_text)
            todo_history_text = self._compact_history_for_insights(
                todo_history_text,
                max_chars=SEATALK_INSIGHTS_TODO_HISTORY_MAX_CHARS,
                signal_max_chars=SEATALK_INSIGHTS_TODO_SIGNAL_MAX_CHARS,
                recent_max_chars=SEATALK_INSIGHTS_TODO_RECENT_MAX_CHARS,
            )
        provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=self.codex_workspace_root,
            timeout_seconds=self.codex_timeout_seconds,
            concurrency_limit=self.codex_concurrency,
            session_mode="ephemeral",
            codex_binary=self.codex_binary,
        )
        prompt_payload = {
            "codex_prompt_mode": SEATALK_INSIGHTS_PROMPT_MODE,
            "systemInstruction": {"parts": [{"text": self._insights_system_prompt()}]},
            "contents": [
                {
                    "parts": [
                        {
                            "text": self._insights_user_prompt(
                                history_text=history_text,
                                todo_history_text=todo_history_text,
                                days=days,
                                now=now,
                                todo_since=todo_since_dt,
                            )
                        }
                    ]
                }
            ],
        }
        result = provider.generate(
            payload=prompt_payload,
            primary_model=self.codex_model,
            fallback_model=self.codex_model,
        )
        text = provider.extract_text(result.payload)
        parsed = self._parse_insights_response(text)
        trace = result.payload.get("codex_cli_trace") if isinstance(result.payload.get("codex_cli_trace"), dict) else {}
        payload = {
            "project_updates": parsed["project_updates"],
            "my_todos": self._sort_todos(parsed["my_todos"]),
            "team_todos": [],
            "generated_at": now.isoformat(),
            "period_days": days,
            "todo_processed_from": todo_since_dt.isoformat() if todo_since_dt else "",
            "todo_processed_until": now.astimezone(SEATALK_INSIGHTS_TIMEZONE).isoformat(),
            "model_id": f"codex:{result.model}",
            "cache": {
                "hit": False,
                "expires_at": self._insights_cache_expiry(now).isoformat(),
            },
            "codex": {
                "latency_ms": int(result.latency_ms or trace.get("latency_ms") or 0),
                "session_mode": str(trace.get("session_mode") or "ephemeral"),
            },
        }
        self._store_cached_insights(days=days, now=now, payload=payload)
        self._store_daily_cache(kind="insights", days=days, now=now, payload=payload)
        return payload

    def build_project_updates(
        self,
        *,
        days: int = SEATALK_DASHBOARD_DEFAULT_DAYS,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = now or datetime.now(SEATALK_INSIGHTS_TIMEZONE)
        cached = self._load_daily_cache(kind="project_updates", days=days, now=now)
        if cached is not None:
            return self._mark_cache_hit(cached, expires_at=self._insights_cache_expiry(now))
        self._validate_local_environment()
        history_text = self._filter_system_generated_history(self._load_local_history_export(days=days, now=now))
        history_text = self._compact_history_for_insights(
            history_text,
            max_chars=SEATALK_PROJECT_UPDATES_HISTORY_MAX_CHARS,
            signal_max_chars=SEATALK_PROJECT_UPDATES_SIGNAL_MAX_CHARS,
            recent_max_chars=SEATALK_PROJECT_UPDATES_RECENT_MAX_CHARS,
        )
        result, parsed = self._run_codex_insights_prompt(
            prompt=self._project_updates_user_prompt(history_text=history_text, days=days, now=now),
            system_prompt=self._project_updates_system_prompt(),
        )
        trace = result.payload.get("codex_cli_trace") if isinstance(result.payload.get("codex_cli_trace"), dict) else {}
        payload = {
            "project_updates": parsed["project_updates"],
            "generated_at": now.isoformat(),
            "period_days": days,
            "model_id": f"codex:{result.model}",
            "cache": {"hit": False, "expires_at": self._insights_cache_expiry(now).isoformat()},
            "codex": {
                "latency_ms": int(result.latency_ms or trace.get("latency_ms") or 0),
                "session_mode": str(trace.get("session_mode") or "ephemeral"),
            },
        }
        self._store_daily_cache(kind="project_updates", days=days, now=now, payload=payload)
        return payload

    def build_todos(
        self,
        *,
        days: int = SEATALK_DASHBOARD_DEFAULT_DAYS,
        now: datetime | None = None,
        todo_since: str | datetime | None = None,
    ) -> dict[str, Any]:
        now = now or datetime.now(SEATALK_INSIGHTS_TIMEZONE)
        todo_since_dt = self._parse_todo_since(todo_since, now=now, days=days)
        cached = self._load_daily_cache(kind=self._todo_cache_kind(todo_since_dt), days=days, now=now)
        if cached is not None:
            return self._mark_cache_hit(cached, expires_at=self._insights_cache_expiry(now))
        self._validate_local_environment()
        history_text = self._load_local_history_export(days=days, now=now, since=todo_since_dt)
        history_text = self._filter_system_generated_history(history_text)
        history_text = self._compact_history_for_insights(
            history_text,
            max_chars=SEATALK_TODO_HISTORY_MAX_CHARS,
            signal_max_chars=SEATALK_TODO_SIGNAL_MAX_CHARS,
            recent_max_chars=SEATALK_TODO_RECENT_MAX_CHARS,
        )
        result, parsed = self._run_codex_insights_prompt(
            prompt=self._todos_user_prompt(history_text=history_text, days=days, now=now, todo_since=todo_since_dt),
            system_prompt=self._todos_system_prompt(),
        )
        trace = result.payload.get("codex_cli_trace") if isinstance(result.payload.get("codex_cli_trace"), dict) else {}
        payload = {
            "my_todos": self._sort_todos(parsed["my_todos"]),
            "team_todos": [],
            "generated_at": now.isoformat(),
            "period_days": days,
            "todo_processed_from": todo_since_dt.isoformat() if todo_since_dt else "",
            "todo_processed_until": now.astimezone(SEATALK_INSIGHTS_TIMEZONE).isoformat(),
            "model_id": f"codex:{result.model}",
            "cache": {"hit": False, "expires_at": self._insights_cache_expiry(now).isoformat()},
            "codex": {
                "latency_ms": int(result.latency_ms or trace.get("latency_ms") or 0),
                "session_mode": str(trace.get("session_mode") or "ephemeral"),
            },
        }
        self._store_daily_cache(kind=self._todo_cache_kind(todo_since_dt), days=days, now=now, payload=payload)
        return payload

    def build_name_mappings(
        self,
        *,
        days: int = SEATALK_DASHBOARD_DEFAULT_DAYS,
        now: datetime | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        now = now or datetime.now().astimezone()
        daily_cached = None if force_refresh else self._load_daily_cache(kind="name_mappings", days=days, now=now)
        if daily_cached is not None:
            payload = json.loads(json.dumps(daily_cached))
            payload["cache"] = {"hit": True, "expires_at": self._insights_cache_expiry(now).isoformat()}
            return payload
        self._validate_local_environment()
        result = self._run_local_helper(
            "seatalk_local_export.js",
            days=days,
            now=now,
            timeout=45,
            extra_args=["--unknown-ids-json"],
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            if not message:
                message = "SeaTalk name mapping candidates could not be loaded right now."
            raise ToolError(message)
        try:
            payload = json.loads(result.stdout or "{}")
        except json.JSONDecodeError as error:
            raise ToolError("SeaTalk name mapping candidates returned an invalid response.") from error
        if not isinstance(payload, dict):
            raise ToolError("SeaTalk name mapping candidates returned an invalid payload.")
        unknown_ids = payload.get("unknown_ids") if isinstance(payload.get("unknown_ids"), list) else []
        payload = {
            "unknown_ids": [self._normalize_unknown_id(row) for row in unknown_ids if isinstance(row, dict)],
            "generated_at": self._clean_text(payload.get("generated_at"), now.isoformat()),
            "period_days": int(payload.get("period_days") or days),
            "cache": {"hit": False, "expires_at": self._insights_cache_expiry(now).isoformat()},
        }
        self._store_daily_cache(kind="name_mappings", days=days, now=now, payload=payload)
        return payload

    def _validate_local_environment(self) -> None:
        if not self.seatalk_app_path.exists():
            raise ConfigError(
                f"SeaTalk desktop app was not found at {self.seatalk_app_path}. Update SEATALK_LOCAL_APP_PATH first."
            )
        if not self.seatalk_data_dir.exists():
            raise ConfigError(
                f"SeaTalk desktop data was not found at {self.seatalk_data_dir}. Update SEATALK_LOCAL_DATA_DIR first."
            )
        config_path = self.seatalk_data_dir / "config.json"
        if not config_path.exists():
            raise ConfigError(
                f"SeaTalk desktop config was not found at {config_path}. Open SeaTalk on this Mac first."
            )

    def _load_local_payload(self, *, days: int, now: datetime) -> dict[str, Any]:
        result = self._run_local_helper("seatalk_local_metrics.js", days=days, now=now)
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            if not message:
                message = "SeaTalk desktop metrics could not be loaded right now."
            raise ToolError(message)
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise ToolError("SeaTalk desktop metrics returned an invalid response.") from error
        if not isinstance(payload, dict):
            raise ToolError("SeaTalk desktop metrics returned an invalid payload.")
        return payload

    def _load_local_history_export(self, *, days: int, now: datetime, since: datetime | None = None) -> str:
        extra_args = ["--since", since.isoformat()] if since is not None else None
        result = self._run_local_helper("seatalk_local_export.js", days=days, now=now, timeout=45, extra_args=extra_args)
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            if not message:
                message = "SeaTalk chat history could not be exported right now."
            raise ToolError(message)
        return result.stdout

    def _run_codex_insights_prompt(self, *, prompt: str, system_prompt: str | None = None):
        provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=self.codex_workspace_root,
            timeout_seconds=self.codex_timeout_seconds,
            concurrency_limit=self.codex_concurrency,
            session_mode="ephemeral",
            codex_binary=self.codex_binary,
        )
        prompt_payload = {
            "codex_prompt_mode": SEATALK_INSIGHTS_PROMPT_MODE,
            "systemInstruction": {"parts": [{"text": system_prompt or self._insights_system_prompt()}]},
            "contents": [{"parts": [{"text": prompt}]}],
        }
        result = provider.generate(
            payload=prompt_payload,
            primary_model=self.codex_model,
            fallback_model=self.codex_model,
        )
        text = provider.extract_text(result.payload)
        return result, self._parse_insights_response(text)

    def _run_local_helper(
        self,
        helper_name: str,
        *,
        days: int,
        now: datetime,
        timeout: int = 25,
        extra_args: list[str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        helper_path = Path(__file__).with_name(helper_name)
        command = [
            str(self.seatalk_app_path / "Contents/MacOS/SeaTalk"),
            str(helper_path),
            "--data-dir",
            str(self.seatalk_data_dir),
            "--days",
            str(days),
            "--now",
            now.isoformat(),
        ]
        if helper_name == "seatalk_local_export.js" and self.name_overrides_path is not None:
            command.extend(["--name-overrides", str(self.name_overrides_path)])
        if extra_args:
            command.extend(extra_args)
        env = os.environ.copy()
        env["ELECTRON_RUN_AS_NODE"] = "1"
        runner = self._command_runner or (lambda args: _run_subprocess(args, env=env, timeout=timeout))
        try:
            return runner(command)
        except subprocess.TimeoutExpired as error:
            raise ToolError("SeaTalk desktop data could not be loaded before the timeout. Please try again shortly.") from error
        except OSError as error:
            raise ToolError("SeaTalk desktop metrics could not be launched on this Mac.") from error

    def _get_cached_dashboard(self, *, days: int, now: datetime) -> dict[str, Any] | None:
        cache_key = (str(self.seatalk_app_path), str(self.seatalk_data_dir), days)
        with self._dashboard_cache_lock:
            cached = self._dashboard_cache.get(cache_key)
            if cached is None or cached.expires_at <= now:
                return None
            payload = dict(cached.payload)
            payload["data_quality"] = dict(cached.payload.get("data_quality") or {})
            payload["data_quality"]["used_fallback_cache"] = True
            return payload

    def _store_cached_dashboard(self, *, days: int, now: datetime, payload: dict[str, Any]) -> None:
        cache_key = (str(self.seatalk_app_path), str(self.seatalk_data_dir), days)
        with self._dashboard_cache_lock:
            self._dashboard_cache[cache_key] = _SeaTalkDashboardCacheEntry(
                payload=payload,
                expires_at=now + timedelta(seconds=SEATALK_DASHBOARD_CACHE_TTL_SECONDS),
            )

    def _get_cached_insights(self, *, days: int, now: datetime) -> dict[str, Any] | None:
        cache_key = self._insights_cache_key(days)
        with self._insights_cache_lock:
            cached = self._insights_cache.get(cache_key)
            if cached is None or cached.expires_at <= now:
                return None
            payload = json.loads(json.dumps(cached.payload))
            payload["cache"] = dict(payload.get("cache") or {})
            payload["cache"]["hit"] = True
            payload["cache"]["expires_at"] = cached.expires_at.isoformat()
            return payload

    def _store_cached_insights(self, *, days: int, now: datetime, payload: dict[str, Any]) -> None:
        cache_key = self._insights_cache_key(days)
        with self._insights_cache_lock:
            self._insights_cache[cache_key] = _SeaTalkInsightsCacheEntry(
                payload=json.loads(json.dumps(payload)),
                expires_at=self._insights_cache_expiry(now),
            )

    @staticmethod
    def _mark_cache_hit(payload: dict[str, Any], *, expires_at: datetime) -> dict[str, Any]:
        cached_payload = json.loads(json.dumps(payload))
        cached_payload["cache"] = dict(cached_payload.get("cache") or {})
        cached_payload["cache"]["hit"] = True
        cached_payload["cache"]["expires_at"] = expires_at.isoformat()
        return cached_payload

    def _load_daily_cache(self, *, kind: str, days: int, now: datetime) -> dict[str, Any] | None:
        cache_path = self._daily_cache_path(kind=kind, days=days, now=now)
        if cache_path is None or not cache_path.exists():
            return None
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        expires_at_raw = str(payload.get("expires_at") or "")
        try:
            expires_at = datetime.fromisoformat(expires_at_raw)
        except ValueError:
            return None
        if expires_at <= now.astimezone(SEATALK_INSIGHTS_TIMEZONE):
            return None
        data = payload.get("payload")
        return data if isinstance(data, dict) else None

    def _store_daily_cache(self, *, kind: str, days: int, now: datetime, payload: dict[str, Any]) -> None:
        cache_path = self._daily_cache_path(kind=kind, days=days, now=now)
        if cache_path is None:
            return
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            wrapper = {
                "kind": kind,
                "cache_date": now.astimezone(SEATALK_INSIGHTS_TIMEZONE).date().isoformat(),
                "expires_at": self._insights_cache_expiry(now).isoformat(),
                "payload": payload,
            }
            temp_path = cache_path.with_name(f".{cache_path.name}.{os.getpid()}.tmp")
            temp_path.write_text(json.dumps(wrapper, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, cache_path)
        except OSError:
            return

    def _daily_cache_path(self, *, kind: str, days: int, now: datetime) -> Path | None:
        if self.daily_cache_dir is None:
            return None
        safe_kind = re.sub(r"[^a-z0-9_]+", "_", kind.lower()).strip("_")
        if safe_kind == "insights" or safe_kind == "project_updates" or safe_kind.startswith("todos"):
            safe_mode = re.sub(r"[^a-z0-9_]+", "_", SEATALK_INSIGHTS_PROMPT_MODE.lower()).strip("_")
            mapping_token = self._name_overrides_cache_token()
            mapping_digest = hashlib.sha1(mapping_token.encode("utf-8")).hexdigest()[:12] if mapping_token else "nomap"
            safe_kind = f"{safe_kind}_{safe_mode}_{mapping_digest}"
        elif safe_kind == "name_mappings":
            safe_version = re.sub(r"[^a-z0-9_]+", "_", SEATALK_NAME_MAPPINGS_CANDIDATE_VERSION.lower()).strip("_")
            safe_kind = f"{safe_kind}_{safe_version}"
        cache_date = now.astimezone(SEATALK_INSIGHTS_TIMEZONE).date().isoformat()
        return self.daily_cache_dir / f"{safe_kind}_last_{int(days)}_days_{cache_date}.json"

    @staticmethod
    def _todo_cache_kind(todo_since: datetime | None) -> str:
        if todo_since is None:
            return "todos_initial"
        cursor = todo_since.astimezone(SEATALK_INSIGHTS_TIMEZONE).isoformat()
        digest = hashlib.sha1(cursor.encode("utf-8")).hexdigest()[:12]
        return f"todos_since_{digest}"

    def _insights_cache_key(self, days: int) -> tuple[str, str, int, str, str]:
        return (
            str(self.seatalk_app_path),
            str(self.seatalk_data_dir),
            int(days),
            self.codex_model,
            f"{SEATALK_INSIGHTS_PROMPT_MODE}:{self._name_overrides_cache_token()}",
        )

    @staticmethod
    def _parse_todo_since(value: str | datetime | None, *, now: datetime, days: int) -> datetime | None:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value or "").strip()
            if not text:
                return None
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return None
        local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
        parsed = parsed.astimezone(SEATALK_INSIGHTS_TIMEZONE) if parsed.tzinfo else parsed.replace(tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        earliest = local_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=max(0, int(days) - 1))
        if parsed < earliest:
            return None
        if parsed >= local_now:
            return local_now
        return parsed

    def _name_overrides_cache_token(self) -> str:
        if self.name_overrides_path is None:
            return ""
        try:
            stat = self.name_overrides_path.stat()
        except OSError:
            return str(self.name_overrides_path)
        return f"{self.name_overrides_path}:{stat.st_mtime_ns}:{stat.st_size}"

    @staticmethod
    def _insights_cache_expiry(now: datetime) -> datetime:
        local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
        tomorrow = local_now.date() + timedelta(days=1)
        return datetime.combine(tomorrow, datetime.min.time(), tzinfo=SEATALK_INSIGHTS_TIMEZONE)

    def _empty_insights_payload(self, *, days: int, now: datetime, cache_hit: bool) -> dict[str, Any]:
        return {
            "project_updates": [],
            "my_todos": [],
            "team_todos": [],
            "generated_at": now.isoformat(),
            "period_days": days,
            "todo_processed_from": "",
            "todo_processed_until": now.astimezone(SEATALK_INSIGHTS_TIMEZONE).isoformat(),
            "model_id": f"codex:{self.codex_model}",
            "cache": {"hit": cache_hit, "expires_at": self._insights_cache_expiry(now).isoformat()},
            "codex": {"latency_ms": 0, "session_mode": "ephemeral"},
        }

    @staticmethod
    def _insights_system_prompt() -> str:
        return (
            "You are Codex helping Xiaodong Zheng review SeaTalk chat history. "
            "You must not modify files or run commands. Produce only valid JSON. "
            "Analyze the last 7 days of SeaTalk messages and write concise English work summaries. "
            "Use sender and conversation labels exactly as evidence. Do not replace unresolved UID, buddy, or group IDs with guessed names. "
            "Prioritize Anti-fraud, Credit Risk / Collection, and Ops Risk / GRC topics, but first consider all messages. "
            "Classify project update domains as exactly one of Anti-fraud, Credit Risk, Ops Risk, General. "
            "Use General as a broad awareness radar: include any noteworthy update Xiaodong should know from the whole chat history that does not fit the three owned product lines. "
            "General includes non-owned product lines, cross-product banking updates, leadership or boss updates, AI sharing, planning, slides, reporting, Key Project table work, org/process changes, incidents, launches, dependencies, and important discussions even when Xiaodong has no action item. "
            "Run a separate General-awareness pass before finalizing project_updates; if the chat history contains any General signals, include 3 to 6 General project_updates and put them before owned-product updates. "
            "Classify action items into my_todos when Xiaodong, Zheng Xiaodong, xiaodong.zheng@npt.sg, or direct second-person requests indicate Xiaodong should act. "
            "Include Xiaodong-owned General tasks such as AI sharing, Key Project table updates, slide updates, reporting, meeting prep, and leadership follow-ups. "
            "Do not include action items owned by other people."
        )

    @staticmethod
    def _project_updates_system_prompt() -> str:
        return (
            "You are Codex helping Xiaodong Zheng review SeaTalk chat history. "
            "You must not modify files or run commands. Produce only valid JSON. "
            "This call is only for project updates and awareness summaries from recent SeaTalk messages. "
            "Do not extract action items, do not infer to-dos, and keep my_todos and team_todos empty. "
            "Use sender and conversation labels exactly as evidence. Do not replace unresolved UID, buddy, or group IDs with guessed names. "
            "Classify project update domains as exactly one of Anti-fraud, Credit Risk, Ops Risk, General. "
            "Use General as a broad awareness radar for leadership, AI sharing, Key Project table, slides, reporting, org/process changes, incidents, launches, dependencies, and important non-owned product lines."
        )

    @staticmethod
    def _todos_system_prompt() -> str:
        return (
            "You are Codex helping Xiaodong Zheng extract his own action items from SeaTalk chat history. "
            "You must not modify files or run commands. Produce only valid JSON. "
            "This call is only for Xiaodong-owned to-dos from the provided incremental history. "
            "Do not summarize project updates and keep project_updates and team_todos empty. "
            "Classify my_todos only when Xiaodong, Zheng Xiaodong, xiaodong.zheng@npt.sg, @Xiaodong, or direct second-person wording indicates Xiaodong should act. "
            "Do not include action items owned by other people. Use sender and conversation labels exactly as evidence and keep evidence short."
        )

    @staticmethod
    def _insights_user_prompt(
        *,
        history_text: str,
        todo_history_text: str | None = None,
        days: int,
        now: datetime,
        todo_since: datetime | None = None,
    ) -> str:
        todo_window = (
            f"Only generate my_todos from messages after {todo_since.isoformat()}."
            if todo_since is not None
            else f"Generate my_todos from the same last {days} days because there is no previous to-do cursor yet."
        )
        todo_history = (
            todo_history_text
            if todo_history_text
            else "[Initial run: use the Project update history above as the New to-do history source.]"
        )
        return (
            "Return a JSON object with exactly these top-level keys: project_updates, my_todos, team_todos.\n"
            "project_updates must be an array of objects with keys: domain, title, summary, status, evidence.\n"
            "For every project_updates item, domain must be exactly one of: Anti-fraud, Credit Risk, Ops Risk, General.\n"
            "For General project_updates, scan the entire chat history for anything worth Xiaodong's awareness, not only traditional product projects. Include other banking product lines, cross-product updates, leadership or boss updates, AI sharing, Key Project, slide, reporting, planning, org/process changes, incidents, launches, dependencies, and important discussions. It is OK for a General update to have no Xiaodong-owned todo.\n"
            "Do a separate General pass and put 3 to 6 General project_updates first whenever the history has any such signal. Do not leave General empty merely because the item is not Xiaodong's owned product line or is not an action item.\n"
            f"my_todos must contain only Xiaodong's own action items from the New to-do history section. {todo_window} When the New to-do history section says this is an initial run, use the Project update history above as the to-do source too. Otherwise, do not create my_todos from the Project update history section unless the same message appears in New to-do history. team_todos must always be an empty array.\n"
            "my_todos objects must have keys: task, domain, priority, due, evidence.\n"
            "For every my_todos item, domain must be exactly one of: Anti-fraud, Credit Risk, Ops Risk, General.\n"
            "Do include Xiaodong-owned General tasks such as AI sharing, updating the Key Project table, updating slides, preparing reports, meeting prep, and boss or leadership follow-ups.\n"
            "Allowed status values: done, in_progress, blocked, unknown. Allowed priority values: high, medium, low, unknown.\n"
            "For due, extract an explicit deadline if present; otherwise use unknown.\n"
            "Keep each evidence value short: include date/time or conversation if visible, plus a brief snippet. Do not include long raw chat content.\n"
            "If there are no confident items for a section, return an empty array.\n"
            "The chat export may be compacted to fit the Codex CLI input limit; treat it as the available source of truth.\n"
            f"Window: last {days} days. Generated at: {now.isoformat()}.\n\n"
            "[Project update history - use for project_updates only]\n"
            f"{history_text}\n\n"
            "[New to-do history - use for my_todos only]\n"
            f"{todo_history}"
        )

    @staticmethod
    def _project_updates_user_prompt(*, history_text: str, days: int, now: datetime) -> str:
        return (
            "Return a JSON object with exactly these top-level keys: project_updates, my_todos, team_todos.\n"
            "project_updates must be an array of objects with keys: domain, title, summary, status, evidence.\n"
            "my_todos and team_todos must both be empty arrays.\n"
            "Use the full available SeaTalk history below only for project updates and awareness summaries, not action extraction.\n"
            "For every project_updates item, domain must be exactly one of: Anti-fraud, Credit Risk, Ops Risk, General.\n"
            "Allowed status values: done, in_progress, blocked, unknown.\n"
            "Prioritize General awareness first when present: include 3 to 6 General updates for leadership, AI sharing, Key Project table, slides, reporting, org/process changes, incidents, launches, dependencies, or important non-owned product lines.\n"
            "Then include concise Anti-fraud, Credit Risk, and Ops Risk updates. Keep evidence short with date/time or conversation plus a brief snippet.\n"
            f"Window: last {days} days. Generated at: {now.isoformat()}.\n\n"
            "SeaTalk project-update history:\n"
            f"{history_text}"
        )

    @staticmethod
    def _todos_user_prompt(*, history_text: str, days: int, now: datetime, todo_since: datetime | None = None) -> str:
        todo_window = (
            f"Only generate my_todos from messages after {todo_since.isoformat()}."
            if todo_since is not None
            else f"Generate my_todos from the last {days} days because there is no previous to-do cursor yet."
        )
        return (
            "Return a JSON object with exactly these top-level keys: project_updates, my_todos, team_todos.\n"
            "project_updates and team_todos must both be empty arrays.\n"
            f"my_todos must contain only Xiaodong's own action items. {todo_window}\n"
            "Classify action items into my_todos only when Xiaodong, Zheng Xiaodong, xiaodong.zheng@npt.sg, @Xiaodong, or direct second-person wording indicates Xiaodong should act.\n"
            "Do not include action items owned by other people. Do include Xiaodong-owned General tasks such as AI sharing, Key Project table updates, slide updates, reporting, meeting prep, and leadership follow-ups.\n"
            "my_todos objects must have keys: task, domain, priority, due, evidence.\n"
            "For every my_todos item, domain must be exactly one of: Anti-fraud, Credit Risk, Ops Risk, General.\n"
            "Allowed priority values: high, medium, low, unknown. For due, extract an explicit deadline if present; otherwise use unknown.\n"
            "Keep evidence short with date/time or conversation plus a brief snippet. If there are no confident Xiaodong-owned items, return an empty my_todos array.\n"
            f"Window: last {days} days. Generated at: {now.isoformat()}.\n\n"
            "SeaTalk to-do history:\n"
            f"{history_text}"
        )

    @classmethod
    def _compact_history_for_insights(
        cls,
        history_text: str,
        *,
        max_chars: int = SEATALK_INSIGHTS_HISTORY_MAX_CHARS,
        signal_max_chars: int = SEATALK_INSIGHTS_SIGNAL_MAX_CHARS,
        recent_max_chars: int = SEATALK_INSIGHTS_RECENT_MAX_CHARS,
    ) -> str:
        text = str(history_text or "")
        if len(text) <= max_chars:
            return text
        lines = text.splitlines()
        header = "\n".join(lines[:8]).strip()
        signal_terms = (
            "@xiaodong",
            "xiaodong",
            "zheng xiaodong",
            "please",
            "pls",
            "todo",
            "to-do",
            "follow up",
            "follow-up",
            "action item",
            "need",
            "deadline",
            "by ",
            "eta",
            "block",
            "issue",
            "risk",
            "af",
            "anti-fraud",
            "anti fraud",
            "credit risk",
            "crms",
            "collection",
            "grc",
            "ops risk",
            "incident",
            "approval",
            "prd",
            "release",
            "rollout",
            "ai sharing",
            "key project",
            "slides",
            "slide",
            "reporting",
            "leadership",
            "boss",
        )
        signal_lines: list[str] = []
        signal_chars = 0
        for line in lines:
            lowered = line.lower()
            if not any(term in lowered for term in signal_terms):
                continue
            signal_lines.append(line)
            signal_chars += len(line) + 1
            if signal_chars >= signal_max_chars:
                break

        recent_lines: list[str] = []
        recent_chars = 0
        for line in reversed(lines):
            recent_lines.append(line)
            recent_chars += len(line) + 1
            if recent_chars >= recent_max_chars:
                break
        recent_lines.reverse()

        compacted = "\n".join(
            part for part in (
                header,
                "",
                "[Compacted high-signal lines]",
                "\n".join(signal_lines),
                "",
                "[Most recent lines]",
                "\n".join(recent_lines),
            )
            if part is not None
        ).strip()
        if len(compacted) > max_chars:
            compacted = compacted[-max_chars:]
        return compacted

    @classmethod
    def _filter_system_generated_history(cls, history_text: str) -> str:
        kept_lines: list[str] = []
        skipped_count = 0
        for line in str(history_text or "").splitlines():
            if cls._is_system_generated_history_line(line):
                skipped_count += 1
                continue
            kept_lines.append(line)
        if skipped_count:
            insert_at = min(len(kept_lines), 5)
            kept_lines.insert(insert_at, f"System-generated alarm/reminder messages removed: {skipped_count}.")
        return "\n".join(kept_lines) + ("\n" if kept_lines else "")

    @staticmethod
    def _is_system_generated_history_line(line: str) -> bool:
        text = str(line or "").strip()
        if not text.startswith("[") or "] " not in text or ": " not in text:
            return False
        try:
            sender_and_message = text.split("] ", 1)[1]
            sender, message = sender_and_message.split(": ", 1)
        except ValueError:
            return False
        sender_l = sender.strip().lower()
        message_l = message.strip().lower()
        sender_system_markers = (
            "system",
            "system account",
            "bot",
            "robot",
            "monitor",
            "notification",
            "noreply",
            "no-reply",
            "workflow",
            "scheduler",
            "jira",
            "gitlab",
            "jenkins",
            "grafana",
            "prometheus",
            "系统",
            "机器人",
        )
        sender_alert_markers = ("reminder", "alert", "alarm", "提醒", "告警", "报警")
        message_auto_markers = (
            "automated reminder",
            "auto reminder",
            "scheduled reminder",
            "system reminder",
            "this is an automated",
            "do not reply",
            "no-reply",
            "alarm notification",
            "alert notification",
            "monitor alert",
            "service alert",
            "incident alert",
            "告警通知",
            "报警通知",
            "自动提醒",
            "系统提醒",
            "系统通知",
        )
        if any(marker in sender_l for marker in sender_system_markers):
            return True
        if any(marker in sender_l for marker in sender_alert_markers):
            return any(marker in message_l for marker in message_auto_markers)
        return any(marker in message_l for marker in message_auto_markers)

    @classmethod
    def _parse_insights_response(cls, text: str) -> dict[str, list[dict[str, str]]]:
        cleaned = cls._extract_json_text(text)
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError as error:
            raise ToolError("Codex returned an invalid SeaTalk insights JSON response.") from error
        if not isinstance(payload, dict):
            raise ToolError("Codex returned an invalid SeaTalk insights payload.")
        return {
            "project_updates": cls._normalize_project_updates(payload.get("project_updates")),
            "my_todos": cls._normalize_todos(payload.get("my_todos")),
            "other_updates": cls._normalize_project_updates(payload.get("other_updates")),
            "team_member_reminders": cls._normalize_team_member_reminders(payload.get("team_member_reminders")),
            "team_todos": [],
        }

    @staticmethod
    def _extract_json_text(text: str) -> str:
        value = str(text or "").strip()
        if value.startswith("```"):
            value = value.strip("`").strip()
            if value.lower().startswith("json"):
                value = value[4:].strip()
        if value.startswith("{") and value.endswith("}"):
            return value
        start = value.find("{")
        end = value.rfind("}")
        if start >= 0 and end > start:
            return value[start : end + 1]
        return value

    @classmethod
    def _normalize_project_updates(cls, value: Any) -> list[dict[str, str]]:
        rows = value if isinstance(value, list) else []
        normalized: list[dict[str, str]] = []
        for row in rows[:20]:
            if not isinstance(row, dict):
                continue
            normalized.append(
                {
                    "domain": cls._normalize_insight_domain(row.get("domain")),
                    "title": cls._clean_text(row.get("title"), "Untitled update"),
                    "summary": cls._clean_text(row.get("summary"), ""),
                    "status": cls._clean_choice(row.get("status"), {"done", "in_progress", "blocked", "unknown"}, "unknown"),
                    "evidence": cls._clean_text(row.get("evidence"), ""),
                }
            )
        return normalized

    @classmethod
    def _normalize_todos(cls, value: Any) -> list[dict[str, str]]:
        rows = value if isinstance(value, list) else []
        normalized: list[dict[str, str]] = []
        for row in rows[:20]:
            if not isinstance(row, dict):
                continue
            normalized.append(
                cls._todo_with_id(
                    {
                        "task": cls._clean_text(row.get("task"), "Untitled task"),
                        "domain": cls._normalize_insight_domain(row.get("domain")),
                        "priority": cls._clean_choice(row.get("priority"), {"high", "medium", "low", "unknown"}, "unknown"),
                        "due": cls._clean_text(row.get("due"), "unknown"),
                        "evidence": cls._clean_text(row.get("evidence"), ""),
                    }
                )
            )
        return cls._sort_todos(normalized)

    @classmethod
    def _normalize_team_member_reminders(cls, value: Any) -> list[dict[str, str]]:
        rows = value if isinstance(value, list) else []
        normalized: list[dict[str, str]] = []
        for row in rows[:20]:
            if not isinstance(row, dict):
                continue
            normalized.append(
                {
                    "domain": cls._normalize_insight_domain(row.get("domain")),
                    "person": cls._clean_text(row.get("person"), "Unknown"),
                    "reminder": cls._clean_text(row.get("reminder"), "Untitled reminder"),
                    "evidence": cls._clean_text(row.get("evidence"), ""),
                }
            )
        return normalized

    @classmethod
    def _todo_with_id(cls, todo: dict[str, str]) -> dict[str, str]:
        stable_text = "|".join(
            (
                cls._fingerprint_text(todo.get("domain")),
                cls._fingerprint_text(todo.get("task")),
            )
        )
        digest = hashlib.sha256(stable_text.encode("utf-8")).hexdigest()[:16]
        return {**todo, "id": digest}

    @staticmethod
    def _fingerprint_text(value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()

    @classmethod
    def _normalize_insight_domain(cls, value: Any) -> str:
        text = cls._fingerprint_text(value)
        if any(term in text for term in ("anti fraud", "antifraud")) or text == "af" or text.startswith("af "):
            return "Anti-fraud"
        if (
            any(term in text for term in ("credit risk", "collection", "collections", "consumer lending", "crms"))
            or text == "cr"
            or text.startswith("cr ")
        ):
            return "Credit Risk"
        if any(term in text for term in ("ops risk", "operational risk", "grc", "governance risk compliance")):
            return "Ops Risk"
        return "General"

    @classmethod
    def _sort_todos(cls, todos: list[dict[str, str]]) -> list[dict[str, str]]:
        priority_rank = {"high": 0, "medium": 1, "low": 2, "unknown": 3}

        def due_rank(todo: dict[str, str]) -> tuple[int, str]:
            due = str(todo.get("due") or "unknown").strip()
            if not due or due.lower() == "unknown":
                return (1, "")
            return (0, due)

        return sorted(
            todos,
            key=lambda todo: (
                priority_rank.get(str(todo.get("priority") or "unknown"), 3),
                due_rank(todo),
                str(todo.get("task") or "").lower(),
            ),
        )

    @staticmethod
    def _clean_choice(value: Any, allowed: set[str], fallback: str) -> str:
        normalized = str(value or "").strip().lower().replace(" ", "_")
        return normalized if normalized in allowed else fallback

    @staticmethod
    def _clean_text(value: Any, fallback: str) -> str:
        text = " ".join(str(value or "").split())
        return (text or fallback)[:900]

    @classmethod
    def _normalize_unknown_id(cls, row: dict[str, Any]) -> dict[str, Any]:
        count = row.get("count")
        try:
            count_value = max(0, int(count))
        except (TypeError, ValueError):
            count_value = 0
        return {
            "id": cls._clean_text(row.get("id"), ""),
            "type": cls._clean_choice(row.get("type"), {"group", "buddy", "uid"}, "uid"),
            "count": count_value,
            "example": cls._clean_text(row.get("example"), ""),
            "first_seen": cls._clean_text(row.get("first_seen"), ""),
            "priority_reason": cls._clean_text(row.get("priority_reason"), "Frequent unknown ID"),
        }

    @classmethod
    def clear_cache(cls) -> None:
        with cls._dashboard_cache_lock:
            cls._dashboard_cache.clear()
        with cls._insights_cache_lock:
            cls._insights_cache.clear()
