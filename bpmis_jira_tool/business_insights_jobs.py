"""On-demand generation jobs for Business Insights reports.

The portal page exposes a "Refresh data" button per generator-backed report.
Clicking it launches the existing operational CLI
(`scripts/generate_business_insights_live_reports.py`) as a detached
subprocess on the Mac host, which reuses the live Chrome Data Admin session to
re-run the Data Workbench SQL and rewrite the Excel + visualization artifacts.

Generation can take minutes, so jobs run detached (not a child we must reap)
and are tracked through small per-report state + log files under the store's
``generation_jobs`` directory. Job status is resolved from a log exit marker
plus pid liveness, which survives portal restarts and works across workers.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from typing import Any

JOBS_DIRNAME = "generation_jobs"
# Appended to the log by the launch wrapper once the CLI exits, so a detached
# (non-child) process's success/failure is recoverable from the log alone.
EXIT_MARKER = "__BI_EXIT__:"
_SCRIPT_RELATIVE = ("scripts", "generate_business_insights_live_reports.py")
# Substrings that point at an expired / missing Chrome Data Admin session so the
# page can tell the user to re-login instead of showing a generic failure.
_SESSION_ERROR_HINTS = (
    "session is not valid",
    "token cookie was not found",
    "is not a JWT",
    "session became invalid",
    "Chrome cookie DB not found",
)


def _jobs_dir(root_dir: Path) -> Path:
    return Path(root_dir) / JOBS_DIRNAME


def _safe_report_id(report_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "", str(report_id or "")).strip("-._")
    if not cleaned:
        raise ValueError("report_id is empty or unsafe")
    return cleaned


def _state_path(root_dir: Path, report_id: str) -> Path:
    return _jobs_dir(root_dir) / f"{_safe_report_id(report_id)}.json"


def _log_path(root_dir: Path, report_id: str) -> Path:
    return _jobs_dir(root_dir) / f"{_safe_report_id(report_id)}.log"


def _default_script_path() -> Path:
    return Path(__file__).resolve().parent.parent.joinpath(*_SCRIPT_RELATIVE)


def _pid_alive(pid: Any) -> bool:
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    # A finished generation job is one of our own detached children. Because we
    # never wait() on it, it lingers as a zombie and os.kill(pid, 0) keeps
    # succeeding even though it is done. Reap it here (we are its parent): if
    # waitpid collects it, it had already exited, so report it as not alive so
    # the job status can resolve to completed instead of freezing at "running".
    try:
        reaped_pid, _ = os.waitpid(pid_int, os.WNOHANG)
    except ChildProcessError:
        return True  # not our child (can't be a zombie we own) -> assume alive
    except OSError:
        return True
    if reaped_pid == pid_int:
        return False  # was a zombie; now reaped
    return True


def _read_log_tail(log_path: Path, *, limit: int = 6000) -> str:
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-limit:]


def _exit_code_from_log(log_text: str) -> int | None:
    matches = re.findall(rf"{re.escape(EXIT_MARKER)}(-?\d+)", log_text)
    if not matches:
        return None
    try:
        return int(matches[-1])
    except ValueError:
        return None


def _last_progress_line(log_text: str) -> str:
    for line in reversed(log_text.splitlines()):
        stripped = line.strip()
        if stripped and not stripped.startswith(EXIT_MARKER):
            return stripped[:300]
    return ""


def _error_hint(log_text: str) -> str:
    lowered = log_text.lower()
    for hint in _SESSION_ERROR_HINTS:
        if hint.lower() in lowered:
            return (
                "Data Admin session is missing or expired. Open "
                "https://data-admin.ph.seabank.io in Chrome, sign in, then retry."
            )
    progress = _last_progress_line(log_text)
    return progress or "Report generation failed. Check the generation log."


def _read_state(root_dir: Path, report_id: str) -> dict[str, Any]:
    try:
        payload = json.loads(_state_path(root_dir, report_id).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_state(root_dir: Path, report_id: str, state: dict[str, Any]) -> None:
    jobs_dir = _jobs_dir(root_dir)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    path = _state_path(root_dir, report_id)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, path)


def start_generation_job(
    *,
    root_dir: Path,
    report_id: str,
    chrome_profile: str | None = None,
    python_bin: str | None = None,
    script_path: str | Path | None = None,
    snapshot: str = "latest",
) -> dict[str, Any]:
    """Launch a detached generation run for ``report_id`` and return its state.

    Idempotent while a run is in flight: if a job is already running it is
    returned unchanged rather than starting a second concurrent run against the
    same report (both would write the same artifacts).
    """
    safe_id = _safe_report_id(report_id)
    existing = _read_state(root_dir, safe_id)
    if existing.get("status") == "running" and _pid_alive(existing.get("pid")):
        return {**existing, "already_running": True}

    python_executable = str(python_bin or sys.executable)
    script = Path(script_path) if script_path else _default_script_path()
    log_path = _log_path(root_dir, safe_id)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    profile = str(chrome_profile or os.getenv("BUSINESS_INSIGHTS_CHROME_PROFILE") or "Default")

    inner = " ".join(
        shlex.quote(part)
        for part in (
            python_executable,
            str(script),
            "--report-id",
            safe_id,
            "--snapshot-pt-date",
            str(snapshot or "latest"),
            "--chrome-profile",
            profile,
        )
    )
    # Redirect all output to the log, then record the exit code so a detached
    # (non-child) process's outcome can be read back later.
    command = f"{inner} > {shlex.quote(str(log_path))} 2>&1; echo \"{EXIT_MARKER}$?\" >> {shlex.quote(str(log_path))}"
    proc = subprocess.Popen(  # noqa: S603 - fixed command shape, report id sanitized
        ["bash", "-c", command],
        cwd=str(script.parent.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    state = {
        "report_id": safe_id,
        "pid": proc.pid,
        "status": "running",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "snapshot": str(snapshot or "latest"),
    }
    _write_state(root_dir, safe_id, state)
    return state


def generation_job_status(*, root_dir: Path, report_id: str) -> dict[str, Any]:
    """Resolve the current job state for ``report_id`` without blocking."""
    safe_id = _safe_report_id(report_id)
    state = _read_state(root_dir, safe_id)
    if not state:
        return {"report_id": safe_id, "status": "idle"}
    if state.get("status") in {"completed", "failed"}:
        return state

    log_text = _read_log_tail(_log_path(root_dir, safe_id))
    exit_code = _exit_code_from_log(log_text)
    if exit_code is not None:
        resolved = "completed" if exit_code == 0 else "failed"
        state["status"] = resolved
        state["exit_code"] = exit_code
        state["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        state["progress"] = _last_progress_line(log_text)
        if resolved == "failed":
            state["error"] = _error_hint(log_text)
        _write_state(root_dir, safe_id, state)
        return state

    if _pid_alive(state.get("pid")):
        return {**state, "status": "running", "progress": _last_progress_line(log_text)}

    # No exit marker and the process is gone: it was killed (e.g. host restart).
    state["status"] = "failed"
    state["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    state["error"] = "Generation process stopped unexpectedly (no completion recorded). Please retry."
    _write_state(root_dir, safe_id, state)
    return state
