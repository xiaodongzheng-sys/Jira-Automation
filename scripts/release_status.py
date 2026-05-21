#!/usr/bin/env python3
"""Print separated release status for Cloud Run, Mac Live, and local-agent."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any, Mapping


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SESSION_SECRET_VALUES = {"", "dev-secret-key", "local-dev-secret-change-me"}
TEAM_PORTAL_FLASK_SECRET_GCP_SECRET = "team-portal-flask-secret"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _env_value(key: str, env: Mapping[str, str]) -> str:
    if env.get(key):
        return str(env[key])
    env_file = Path(env.get("ENV_FILE") or ROOT_DIR / ".env")
    if not env_file.exists():
        return ""
    prefix = f"{key}="
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or not line.startswith(prefix):
            continue
        return line[len(prefix) :].strip().strip('"').strip("'")
    return ""


def _run(command: list[str], *, env: Mapping[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT_DIR,
        env=dict(env or os.environ),
        capture_output=True,
        text=True,
        check=False,
    )


def _json_command(command: list[str], *, env: Mapping[str, str], runner: Any = _run) -> tuple[dict[str, Any] | None, str]:
    completed = runner(command, env=env)
    if completed.returncode != 0:
        return None, (completed.stderr or completed.stdout or f"exit {completed.returncode}").strip()
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as error:
        return None, f"invalid JSON: {error}"
    if not isinstance(payload, dict):
        return None, "JSON payload was not an object"
    return payload, ""


def _text_command(command: list[str], *, env: Mapping[str, str], runner: Any = _run) -> tuple[str, str]:
    completed = runner(command, env=env)
    if completed.returncode != 0:
        return "", (completed.stderr or completed.stdout or f"exit {completed.returncode}").strip()
    return (completed.stdout or "").strip(), ""


def _health_probe(url: str, *, env: Mapping[str, str], runner: Any = _run) -> str:
    if not url:
        return "url=<missing> status=missing"
    payload, error = _json_command(["curl", "-fsS", "--max-time", "10", url], env=env, runner=runner)
    if payload is None:
        return f"url={url} status=unavailable error={error}"
    details = [f"url={url}", f"status={payload.get('status') or 'unknown'}"]
    if payload.get("revision"):
        details.append(f"revision={payload.get('revision')}")
    capabilities = payload.get("capabilities")
    if isinstance(capabilities, dict):
        if "source_code_qa" in capabilities:
            details.append(f"source_code_qa={capabilities.get('source_code_qa')}")
        if "codex_ready" in capabilities:
            details.append(f"codex_ready={capabilities.get('codex_ready')}")
    return " ".join(details)


def _sanitize_error_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return " ".join(text.split())[:240]


def _gcloud_binary(*, env: Mapping[str, str]) -> str:
    configured = env.get("GCLOUD_BIN") or "gcloud"
    resolved = shutil.which(configured) or configured
    if Path(resolved).exists():
        return resolved
    fallback = shutil.which("gcloud")
    if fallback:
        return fallback
    bundled = Path.home() / "google-cloud-sdk/bin/gcloud"
    return str(bundled)


def _gcloud_account_args(*, env: Mapping[str, str]) -> list[str]:
    account = _env_value("CLOUD_RUN_DEPLOY_ACCOUNT", env)
    return ["--account", account] if account else []


def _gcloud_secret_value(secret_name: str, *, env: Mapping[str, str], runner: Any = _run) -> tuple[str, str]:
    project = _env_value("GOOGLE_CLOUD_PROJECT", env)
    gcloud_bin = _gcloud_binary(env=env)
    if not gcloud_bin or not Path(gcloud_bin).exists():
        return "", "gcloud unavailable"
    command = [gcloud_bin, "secrets", "versions", "access", "latest", f"--secret={secret_name}"]
    if project:
        command.extend(["--project", project])
    command.extend(_gcloud_account_args(env=env))
    completed = runner(command, env=env)
    if completed.returncode != 0:
        return "", _sanitize_error_text(completed.stderr or completed.stdout or f"exit {completed.returncode}")
    return str(completed.stdout or "").strip(), ""


def _is_default_flask_secret(value: str) -> bool:
    return str(value or "").strip() in DEFAULT_SESSION_SECRET_VALUES


def _shared_session_status(*, env: Mapping[str, str], runner: Any = _run) -> str:
    cloud_home_enabled = str(_env_value("TEAM_PORTAL_CLOUD_HOME_ENABLED", env) or "").strip().lower() in {"1", "true", "yes", "on"}
    mac_full_portal_url = str(_env_value("TEAM_PORTAL_MAC_FULL_PORTAL_URL", env) or "").strip()
    public_base_url = str(_env_value("TEAM_PORTAL_BASE_URL", env) or "").strip()
    if not (cloud_home_enabled or mac_full_portal_url or public_base_url):
        return "status=not_applicable cloud_home_enabled=false"
    local_secret = str(_env_value("FLASK_SECRET_KEY", env) or "").strip()
    if not local_secret:
        return "status=fail cloud_home_enabled=true reason=local_flask_secret_missing"
    if _is_default_flask_secret(local_secret):
        return "status=fail cloud_home_enabled=true reason=local_flask_secret_default"
    remote_secret, remote_error = _gcloud_secret_value(TEAM_PORTAL_FLASK_SECRET_GCP_SECRET, env=env, runner=runner)
    if remote_error:
        return (
            "status=warn cloud_home_enabled=true "
            f"local_secret=configured gcp_secret=unverified error={remote_error}"
        )
    if not remote_secret:
        return "status=warn cloud_home_enabled=true local_secret=configured gcp_secret=empty"
    if remote_secret != local_secret:
        return "status=fail cloud_home_enabled=true local_secret=configured gcp_secret=configured match=no"
    return "status=ok cloud_home_enabled=true local_secret=configured gcp_secret=configured match=yes"


def _mac_portal_availability_status(*, env: Mapping[str, str], runner: Any = _run) -> str:
    local_port = _env_value("TEAM_PORTAL_PORT", env) or "5000"
    public_url = (_env_value("TEAM_PORTAL_BASE_URL", env) or "").rstrip("/")
    local_probe = _health_probe(f"http://127.0.0.1:{local_port}/healthz", env=env, runner=runner)
    public_probe = _health_probe(f"{public_url}/healthz" if public_url else "", env=env, runner=runner)
    local_ok = " status=ok" in local_probe
    public_ok = " status=ok" in public_probe
    if local_ok and public_ok:
        status = "online"
    elif local_ok or public_ok:
        status = "degraded"
    else:
        status = "offline"
    return f"status={status} local_probe=({local_probe}) public_probe=({public_probe})"


def _version_plan_firestore_status(*, env: Mapping[str, str]) -> str:
    backend = _env_value("VERSION_PLAN_STORE_BACKEND", env).strip().lower()
    stage = (_env_value("VERSION_PLAN_FIRESTORE_ENVIRONMENT", env) or _env_value("TEAM_PORTAL_STAGE", env) or "live").strip().lower()
    project = _env_value("VERSION_PLAN_FIRESTORE_PROJECT", env) or _env_value("GOOGLE_CLOUD_PROJECT", env)
    document = _env_value("VERSION_PLAN_FIRESTORE_DOCUMENT", env) or f"version_plan_{'uat' if stage == 'uat' else 'live'}"
    if backend not in {"firestore", "cloud_firestore"} and not project:
        return "status=not_configured"
    def _load_payload() -> tuple[dict[str, Any] | None, str]:
        try:
            from google.cloud import firestore  # type: ignore

            snapshot = firestore.Client(project=project or None).collection("portal").document(document).get()
            if not getattr(snapshot, "exists", False):
                return None, ""
            return snapshot.to_dict() or {}, ""
        except Exception as sdk_error:
            try:
                from bpmis_jira_tool.team_dashboard_version_plan_store import _FirestoreRestDocument

                snapshot = _FirestoreRestDocument(project=project, document_id=document).get()
                if not getattr(snapshot, "exists", False):
                    return None, ""
                return snapshot.to_dict() or {}, ""
            except Exception as rest_error:
                return None, f"{type(sdk_error).__name__}; REST fallback: {type(rest_error).__name__}"

    try:
        payload, error = _load_payload()
        if error:
            return f"status=unavailable document=portal/{document} error={error}"
        if payload is None:
            return f"status=missing document=portal/{document} environment={stage}"
    except Exception as error:
        return f"status=unavailable document=portal/{document} error={type(error).__name__}"
    return (
        f"status=ok document=portal/{document} "
        f"environment={payload.get('environment') or stage} "
        f"updated_at_sgt={payload.get('updated_at_sgt') or '<missing>'} "
        f"source_hash={payload.get('source_hash') or '<missing>'}"
    )


def _revision_release_value(
    revision_name: str,
    *,
    gcloud_bin: str,
    project_args: list[str],
    account_args: list[str],
    region: str,
    env: Mapping[str, str],
    runner: Any = _run,
) -> str:
    if not revision_name:
        return ""
    payload, _ = _json_command(
        [
            gcloud_bin,
            "run",
            "revisions",
            "describe",
            revision_name,
            *project_args,
            *account_args,
            "--region",
            region,
            "--format=json",
        ],
        env=env,
        runner=runner,
    )
    if payload is None:
        return ""
    containers = payload.get("spec", {}).get("containers", [])
    revision_env = containers[0].get("env", []) if containers else []
    values = {item.get("name"): item.get("value") for item in revision_env if isinstance(item, dict)}
    return str(values.get("TEAM_PORTAL_RELEASE_REVISION") or "")


def build_status_lines(*, env: Mapping[str, str] | None = None, runner: Any = _run) -> list[str]:
    env = dict(env or os.environ)
    lines = ["== Release Status =="]

    expected_revision, error = _text_command(["git", "-C", str(ROOT_DIR), "rev-parse", "HEAD"], env=env, runner=runner)
    lines.append(f"Expected source revision: {expected_revision or f'unavailable ({error})'}")

    service = _env_value("CLOUD_RUN_SERVICE", env) or "team-portal"
    region = _env_value("CLOUD_RUN_REGION", env) or "asia-southeast1"
    uat_tag = _env_value("CLOUD_RUN_UAT_TAG", env) or "uat"
    project = _env_value("GOOGLE_CLOUD_PROJECT", env)
    gcloud_bin = _gcloud_binary(env=env)
    project_args = ["--project", project] if project else []
    account_args = _gcloud_account_args(env=env)

    lines.append(f"Cloud Run service: {service} region={region}")
    if not gcloud_bin or not Path(gcloud_bin).exists():
        lines.append("Cloud Run status: unavailable (gcloud not found)")
    else:
        service_payload, service_error = _json_command(
            [
                gcloud_bin,
                "run",
                "services",
                "describe",
                service,
                *project_args,
                *account_args,
                "--region",
                region,
                "--format=json",
            ],
            env=env,
            runner=runner,
        )
        if service_payload is None:
            lines.append(f"Cloud Run status: unavailable ({service_error})")
        else:
            traffic = service_payload.get("status", {}).get("traffic", [])
            uat_matches = [item for item in traffic if item.get("tag") == uat_tag]
            if uat_matches:
                uat = uat_matches[0]
                uat_revision = str(uat.get("revisionName") or "")
                uat_release = _revision_release_value(
                    uat_revision,
                    gcloud_bin=gcloud_bin,
                    project_args=project_args,
                    account_args=account_args,
                    region=region,
                    env=env,
                    runner=runner,
                )
                lines.append(
                    "Cloud Run UAT tag: "
                    f"tag={uat_tag} revision={uat_revision or '<missing>'} "
                    f"git_revision={uat_release or '<missing>'} url={uat.get('url') or '<missing>'}"
                )
            else:
                lines.append(f"Cloud Run UAT tag: tag={uat_tag} revision=<missing>")

            live_traffic = [item for item in traffic if item.get("percent")]
            if live_traffic:
                for item in live_traffic:
                    revision = str(item.get("revisionName") or "")
                    release_revision = _revision_release_value(
                        revision,
                        gcloud_bin=gcloud_bin,
                        project_args=project_args,
                        account_args=account_args,
                        region=region,
                        env=env,
                        runner=runner,
                    )
                    lines.append(
                        "Cloud Run service live traffic: "
                        f"revision={revision or '<missing>'} percent={item.get('percent')} "
                        f"git_revision={release_revision or '<missing>'} "
                        "(Cloud Run traffic, not Mac public Live)"
                    )
            else:
                lines.append("Cloud Run service live traffic: <none>")

    local_port = _env_value("TEAM_PORTAL_PORT", env) or "5000"
    public_url = (_env_value("TEAM_PORTAL_BASE_URL", env) or "").rstrip("/")
    local_agent_base = (_env_value("LOCAL_AGENT_BASE_URL", env) or "").rstrip("/")
    if not local_agent_base:
        local_agent_host = _env_value("LOCAL_AGENT_HOST", env) or "127.0.0.1"
        local_agent_port = _env_value("LOCAL_AGENT_PORT", env) or "7007"
        local_agent_base = f"http://{local_agent_host}:{local_agent_port}"

    lines.append(f"Mac portal availability: {_mac_portal_availability_status(env=env, runner=runner)}")
    lines.append(f"Shared session configuration: {_shared_session_status(env=env, runner=runner)}")
    lines.append(f"Local portal: {_health_probe(f'http://127.0.0.1:{local_port}/healthz', env=env, runner=runner)}")
    lines.append(
        "Public Live URL (Mac/Cloudflare): "
        f"{_health_probe(f'{public_url}/healthz' if public_url else '', env=env, runner=runner)}"
    )
    lines.append(f"Direct local-agent: {_health_probe(f'{local_agent_base}/healthz', env=env, runner=runner)}")
    lines.append(
        "Public local-agent proxy: "
        f"{_health_probe(f'{public_url}/api/local-agent/healthz' if public_url else '', env=env, runner=runner)}"
    )
    lines.append(f"Version Plan Firestore: {_version_plan_firestore_status(env=env)}")
    return lines


def main() -> int:
    print("\n".join(build_status_lines()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
