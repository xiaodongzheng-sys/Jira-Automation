from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Any, Mapping


def sanitize_error_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return " ".join(text.split())[:240]


def json_command(command: list[str], *, env: Mapping[str, str], runner: Any) -> tuple[dict[str, Any] | None, str]:
    completed = runner(command, env=env)
    if completed.returncode != 0:
        return None, sanitize_error_text(completed.stderr or completed.stdout or f"exit {completed.returncode}")
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as error:
        return None, f"invalid JSON: {error}"
    if not isinstance(payload, dict):
        return None, "JSON payload was not an object"
    return payload, ""


def text_command(command: list[str], *, env: Mapping[str, str], runner: Any) -> tuple[str, str]:
    completed: subprocess.CompletedProcess[str] = runner(command, env=env)
    if completed.returncode != 0:
        return "", sanitize_error_text(completed.stderr or completed.stdout or f"exit {completed.returncode}")
    return (completed.stdout or "").strip(), ""


def health_probe(url: str, *, env: Mapping[str, str], runner: Any) -> str:
    if not url:
        return "url=<missing> status=missing"
    payload, error = json_command(["curl", "-fsS", "--max-time", "10", url], env=env, runner=runner)
    if payload is None:
        return f"url={url} status=unavailable error={error}"
    details = [f"url={url}", f"status={payload.get('status') or 'unknown'}"]
    if payload.get("revision"):
        details.append(f"revision={payload.get('revision')}")
    if payload.get("release_manifest_id"):
        details.append(f"release_manifest_id={payload.get('release_manifest_id')}")
    if payload.get("live_surface"):
        details.append(f"live_surface={payload.get('live_surface')}")
    capabilities = payload.get("capabilities")
    if isinstance(capabilities, dict):
        if "source_code_qa" in capabilities:
            details.append(f"source_code_qa={capabilities.get('source_code_qa')}")
        if "codex_ready" in capabilities:
            details.append(f"codex_ready={capabilities.get('codex_ready')}")
    return " ".join(details)


def detail_value(details: str, key: str) -> str:
    prefix = f"{key}="
    for raw_part in str(details or "").replace("(", " ").replace(")", " ").split():
        if raw_part.startswith(prefix):
            return raw_part[len(prefix) :]
    return ""


def health_status(details: str) -> str:
    return detail_value(details, "status")


def health_revision(details: str) -> str:
    return detail_value(details, "revision")


def cloud_run_role(env_value: str) -> str:
    return str(env_value or "").strip().lower() or "standby"


def cloud_run_is_release_gate(role: str) -> bool:
    return role in {"active", "authoritative", "release_gate", "required"}


def cloud_run_mismatch_message(role: str, message: str) -> str:
    if cloud_run_is_release_gate(role):
        return message
    return f"{message} Cloud Run role is {role}; Mac public Live is authoritative."


def manifest_path(data_dir: str | Path, explicit: str = "") -> Path:
    if explicit:
        return Path(explicit)
    return Path(data_dir).expanduser() / "run" / "team_portal_release_manifest.json"
