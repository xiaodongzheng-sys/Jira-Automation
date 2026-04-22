from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Callable

from bpmis_jira_tool.errors import ConfigError, ToolError


SEATALK_DASHBOARD_DEFAULT_DAYS = 7
SEATALK_DASHBOARD_CACHE_TTL_SECONDS = 300
SEATALK_DEFAULT_APP_PATH = "/Applications/SeaTalk.app"
SEATALK_DEFAULT_DATA_DIR = "~/Library/Application Support/SeaTalk"
UNAVAILABLE_REASON = "Not available from local SeaTalk desktop data for this scope."


@dataclass
class _SeaTalkDashboardCacheEntry:
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

    def __init__(
        self,
        *,
        owner_email: str,
        seatalk_app_path: str = SEATALK_DEFAULT_APP_PATH,
        seatalk_data_dir: str = SEATALK_DEFAULT_DATA_DIR,
        command_runner: Callable[[list[str]], subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.owner_email = str(owner_email or "").strip().lower()
        self.seatalk_app_path = Path(str(seatalk_app_path or SEATALK_DEFAULT_APP_PATH)).expanduser()
        self.seatalk_data_dir = Path(str(seatalk_data_dir or SEATALK_DEFAULT_DATA_DIR)).expanduser()
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

    def _load_local_history_export(self, *, days: int, now: datetime) -> str:
        result = self._run_local_helper("seatalk_local_export.js", days=days, now=now, timeout=45)
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            if not message:
                message = "SeaTalk chat history could not be exported right now."
            raise ToolError(message)
        return result.stdout

    def _run_local_helper(
        self,
        helper_name: str,
        *,
        days: int,
        now: datetime,
        timeout: int = 25,
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

    @classmethod
    def clear_cache(cls) -> None:
        with cls._dashboard_cache_lock:
            cls._dashboard_cache.clear()
