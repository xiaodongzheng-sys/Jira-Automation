#!/usr/bin/env python3
"""Print separated release status for Cloud Run, Mac Live, and local-agent."""
from __future__ import annotations

import json
import os
from pathlib import Path
import hashlib
import shutil
import subprocess
import sys
from typing import Any, Mapping

from scripts.release_probes import (
    cloud_run_is_release_gate as _cloud_run_is_release_gate,
    cloud_run_mismatch_message as _cloud_run_mismatch_message,
    cloud_run_role as _cloud_run_role_value,
    detail_value as _detail_value,
    health_probe as _release_health_probe,
    health_revision as _health_revision,
    health_status as _health_status,
    json_command as _release_json_command,
    manifest_path as _manifest_path,
    sanitize_error_text as _sanitize_error_text,
    text_command as _release_text_command,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SESSION_SECRET_VALUES = {"", "dev-secret-key", "local-dev-secret-change-me"}
TEAM_PORTAL_FLASK_SECRET_GCP_SECRET = "team-portal-flask-secret"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _cloud_run_role(env: Mapping[str, str]) -> str:
    return _cloud_run_role_value(_env_value("TEAM_PORTAL_CLOUD_RUN_ROLE", env))


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
    return _release_json_command(command, env=env, runner=runner)


def _text_command(command: list[str], *, env: Mapping[str, str], runner: Any = _run) -> tuple[str, str]:
    return _release_text_command(command, env=env, runner=runner)


def _health_probe(url: str, *, env: Mapping[str, str], runner: Any = _run) -> str:
    return _release_health_probe(url, env=env, runner=runner)


def _issue(severity: str, code: str, message: str) -> dict[str, str]:
    return {"severity": severity, "code": code, "message": message}


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


def _gcloud_firestore_access_token(*, env: Mapping[str, str], runner: Any = _run) -> str:
    explicit = _env_value("FIRESTORE_ACCESS_TOKEN", env)
    if explicit:
        return explicit
    project = _env_value("VERSION_PLAN_FIRESTORE_PROJECT", env) or _env_value("GOOGLE_CLOUD_PROJECT", env)
    gcloud_bin = _gcloud_binary(env=env)
    if not gcloud_bin or not Path(gcloud_bin).exists():
        raise RuntimeError("gcloud unavailable")
    command = [gcloud_bin, "auth", "print-access-token"]
    command.extend(_gcloud_account_args(env=env))
    if project:
        command.extend(["--project", project])
    token, error = _text_command(command, env=env, runner=runner)
    if error:
        raise RuntimeError(_sanitize_error_text(error))
    if not token:
        raise RuntimeError("gcloud auth print-access-token returned an empty token")
    return token


def _prefer_firestore_rest_auth(env: Mapping[str, str]) -> bool:
    return bool(_env_value("FIRESTORE_ACCESS_TOKEN", env) or _env_value("CLOUD_RUN_DEPLOY_ACCOUNT", env))


def _load_version_plan_firestore_payload(
    *,
    project: str,
    document: str,
    env: Mapping[str, str],
) -> tuple[dict[str, Any] | None, str]:
    def _load_via_sdk() -> tuple[dict[str, Any] | None, str]:
        from google.cloud import firestore  # type: ignore

        snapshot = firestore.Client(project=project or None).collection("portal").document(document).get()
        if not getattr(snapshot, "exists", False):
            return None, ""
        return snapshot.to_dict() or {}, ""

    def _load_via_rest() -> tuple[dict[str, Any] | None, str]:
        from bpmis_jira_tool.team_dashboard_version_plan_store import _FirestoreRestDocument

        snapshot = _FirestoreRestDocument(
            project=project,
            document_id=document,
            token_provider=lambda: _gcloud_firestore_access_token(env=env),
        ).get()
        if not getattr(snapshot, "exists", False):
            return None, ""
        return snapshot.to_dict() or {}, ""

    if _prefer_firestore_rest_auth(env):
        try:
            return _load_via_rest()
        except Exception as rest_error:
            return None, f"REST service-account: {type(rest_error).__name__}"

    try:
        return _load_via_sdk()
    except Exception as sdk_error:
        try:
            return _load_via_rest()
        except Exception as rest_error:
            return None, f"{type(sdk_error).__name__}; REST fallback: {type(rest_error).__name__}"


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
    stage = "live"
    project = _env_value("VERSION_PLAN_FIRESTORE_PROJECT", env) or _env_value("GOOGLE_CLOUD_PROJECT", env)
    document = _env_value("VERSION_PLAN_FIRESTORE_DOCUMENT", env) or "version_plan_live"
    if backend not in {"firestore", "cloud_firestore"} and not project:
        return "status=not_configured"
    try:
        payload, error = _load_version_plan_firestore_payload(project=project, document=document, env=env)
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


def _expected_source_revision(*, env: Mapping[str, str], runner: Any = _run) -> tuple[str, str]:
    from bpmis_jira_tool.web_runtime_status import filtered_untracked_paths

    head, error = _text_command(["git", "-C", str(ROOT_DIR), "rev-parse", "HEAD"], env=env, runner=runner)
    if not head:
        return "", error
    diff_command = ["git", "-C", str(ROOT_DIR), "diff", "--no-ext-diff", "--full-index", "--binary", "HEAD", "--", "."]
    diff_completed = runner(diff_command, env=env)
    if diff_completed.returncode != 0:
        return head, _sanitize_error_text(diff_completed.stderr or diff_completed.stdout or f"exit {diff_completed.returncode}")
    untracked_command = ["git", "-C", str(ROOT_DIR), "ls-files", "--others", "--exclude-standard"]
    untracked_completed = runner(untracked_command, env=env)
    if untracked_completed.returncode != 0:
        return head, _sanitize_error_text(
            untracked_completed.stderr or untracked_completed.stdout or f"exit {untracked_completed.returncode}"
        )
    diff_text = diff_completed.stdout or ""
    untracked = untracked_completed.stdout or ""
    dirty_material = diff_text
    untracked_paths = filtered_untracked_paths(untracked)
    if untracked_paths:
        dirty_material += "\n--UNTRACKED--\n" + "\n".join(untracked_paths) + "\n"
    if dirty_material.strip():
        fingerprint = hashlib.sha1(dirty_material.encode("utf-8")).hexdigest()[:12]
        return f"{head}-dirty-{fingerprint}", ""
    return head, ""


def _release_manifest_path(env: Mapping[str, str]) -> Path:
    data_dir = _env_value("TEAM_PORTAL_DATA_DIR", env) or str(ROOT_DIR / ".team-portal")
    return _manifest_path(data_dir, _env_value("TEAM_PORTAL_RELEASE_MANIFEST_PATH", env))


def _release_manifest_status(expected_revision: str, *, env: Mapping[str, str]) -> str:
    from bpmis_jira_tool.release_manifest import load_release_manifest, manifest_file_sha256

    path = _release_manifest_path(env)
    if not path.exists():
        return f"status=missing path={path}"
    payload = load_release_manifest(path)
    if payload is None:
        return f"status=unavailable path={path} reason=invalid_json"
    revision = str(payload.get("release_revision") or "")
    manifest_id = str(payload.get("manifest_id") or "")
    surface = str(payload.get("surface") or "")
    python_version = str(payload.get("python_version") or "")
    status = "ok"
    reason = ""
    if expected_revision and revision != expected_revision:
        status = "fail"
        reason = " revision_mismatch"
    return (
        f"status={status} path={path} manifest_id={manifest_id or '<missing>'} "
        f"surface={surface or '<missing>'} release_revision={revision or '<missing>'} "
        f"python_version={python_version or '<missing>'} file_sha256={manifest_file_sha256(path) or '<missing>'}{reason}"
    )


def build_status_lines(*, env: Mapping[str, str] | None = None, runner: Any = _run) -> list[str]:
    return build_status_report(env=env, runner=runner)["lines"]


def build_status_report(*, env: Mapping[str, str] | None = None, runner: Any = _run) -> dict[str, Any]:
    env = dict(env or os.environ)
    lines = ["== Release Status =="]
    issues: list[dict[str, str]] = []

    expected_revision, error = _expected_source_revision(env=env, runner=runner)
    lines.append(f"Expected source revision: {expected_revision or f'unavailable ({error})'}")
    if not expected_revision:
        issues.append(_issue("warn", "source_revision_unavailable", f"Could not resolve expected source revision: {_sanitize_error_text(error)}"))

    service = _env_value("CLOUD_RUN_SERVICE", env) or "team-portal"
    region = _env_value("CLOUD_RUN_REGION", env) or "asia-southeast1"
    cloud_role = _cloud_run_role(env)
    project = _env_value("GOOGLE_CLOUD_PROJECT", env)
    gcloud_bin = _gcloud_binary(env=env)
    project_args = ["--project", project] if project else []
    account_args = _gcloud_account_args(env=env)

    lines.append(f"Cloud Run role: {cloud_role}")
    lines.append(f"Cloud Run service: {service} region={region}")

    def cloud_run_readiness_or_info(code: str, message: str) -> None:
        if _cloud_run_is_release_gate(cloud_role):
            issues.append(_issue("warn", code, message))
        else:
            lines.append(f"Cloud Run standby info: {code} {message}")

    if not gcloud_bin or not Path(gcloud_bin).exists():
        lines.append("Cloud Run status: unavailable (gcloud not found)")
        cloud_run_readiness_or_info("cloud_run_status_unavailable", "gcloud was not found; Cloud Run revision readiness is unverified.")
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
            cloud_run_readiness_or_info(
                "cloud_run_status_unavailable",
                f"Cloud Run service status unavailable: {_sanitize_error_text(service_error)}",
            )
        else:
            traffic = service_payload.get("status", {}).get("traffic", [])
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
                    if expected_revision and release_revision and release_revision != expected_revision:
                        message = _cloud_run_mismatch_message(
                            cloud_role,
                            f"Cloud Run live traffic serves {release_revision}, expected {expected_revision}.",
                        )
                        cloud_run_readiness_or_info("cloud_run_live_revision_mismatch", message)
                    elif expected_revision and not release_revision:
                        message = "Cloud Run live traffic did not expose TEAM_PORTAL_RELEASE_REVISION."
                        cloud_run_readiness_or_info("cloud_run_live_revision_unknown", message)
            else:
                lines.append("Cloud Run service live traffic: <none>")
                cloud_run_readiness_or_info("cloud_run_live_traffic_missing", "Cloud Run service has no live traffic allocation.")

    local_port = _env_value("TEAM_PORTAL_PORT", env) or "5000"
    public_url = (_env_value("TEAM_PORTAL_BASE_URL", env) or "").rstrip("/")
    local_agent_base = (_env_value("LOCAL_AGENT_BASE_URL", env) or "").rstrip("/")
    if not local_agent_base:
        local_agent_host = _env_value("LOCAL_AGENT_HOST", env) or "127.0.0.1"
        local_agent_port = _env_value("LOCAL_AGENT_PORT", env) or "7007"
        local_agent_base = f"http://{local_agent_host}:{local_agent_port}"

    mac_portal = _mac_portal_availability_status(env=env, runner=runner)
    shared_session = _shared_session_status(env=env, runner=runner)
    local_portal = _health_probe(f"http://127.0.0.1:{local_port}/healthz", env=env, runner=runner)
    public_live = _health_probe(f"{public_url}/healthz" if public_url else "", env=env, runner=runner)
    direct_local_agent = _health_probe(f"{local_agent_base}/healthz", env=env, runner=runner)
    public_local_agent = _health_probe(f"{public_url}/api/local-agent/healthz" if public_url else "", env=env, runner=runner)
    version_plan = _version_plan_firestore_status(env=env)
    release_manifest = _release_manifest_status(expected_revision, env=env)

    lines.append(f"Mac portal availability: {mac_portal}")
    lines.append(f"Shared session configuration: {shared_session}")
    lines.append(f"Local portal: {local_portal}")
    lines.append(
        "Public Live URL (Mac/Cloudflare): "
        f"{public_live}"
    )
    lines.append(f"Direct local-agent: {direct_local_agent}")
    lines.append(
        "Public local-agent proxy: "
        f"{public_local_agent}"
    )
    lines.append(f"Version Plan Firestore: {version_plan}")
    lines.append(f"Release manifest: {release_manifest}")

    mac_status = _detail_value(mac_portal, "status")
    if mac_status == "offline":
        issues.append(_issue("fail", "mac_portal_offline", "Neither local nor public Mac portal health probes are online."))
    elif mac_status == "degraded":
        issues.append(_issue("fail", "mac_portal_degraded", "Only one Mac portal health probe is online; local/public readiness is split."))

    shared_status = _detail_value(shared_session, "status")
    if shared_status == "fail":
        issues.append(_issue("fail", "shared_session_misconfigured", shared_session))
    elif shared_status == "warn":
        issues.append(_issue("warn", "shared_session_unverified", shared_session))

    for label, details, code in (
        ("Local portal", local_portal, "local_portal"),
        ("Public Live URL", public_live, "public_live"),
    ):
        if _health_status(details) != "ok":
            issues.append(_issue("fail", f"{code}_unavailable", f"{label} health probe is not ok: {details}"))
        elif expected_revision:
            revision = _health_revision(details)
            if not revision:
                issues.append(_issue("fail", f"{code}_revision_missing", f"{label} health probe did not expose revision."))
            elif revision != expected_revision:
                issues.append(_issue("fail", f"{code}_revision_mismatch", f"{label} serves {revision}, expected {expected_revision}."))

    for label, details, code in (
        ("Direct local-agent", direct_local_agent, "direct_local_agent_unavailable"),
        ("Public local-agent proxy", public_local_agent, "public_local_agent_unavailable"),
    ):
        if _health_status(details) != "ok":
            issues.append(_issue("warn", code, f"{label} health probe is not ok: {details}"))

    version_plan_status = _detail_value(version_plan, "status")
    if version_plan_status in {"unavailable", "missing"}:
        issues.append(_issue("warn", f"version_plan_firestore_{version_plan_status}", version_plan))

    release_manifest_status = _detail_value(release_manifest, "status")
    if release_manifest_status == "fail":
        issues.append(_issue("fail", "release_manifest_revision_mismatch", release_manifest))
    elif release_manifest_status in {"missing", "unavailable"}:
        issues.append(_issue("warn", f"release_manifest_{release_manifest_status}", release_manifest))

    if any(issue["severity"] == "fail" for issue in issues):
        status = "fail"
    elif issues:
        status = "warn"
    else:
        status = "pass"
    lines.append(f"Readiness: status={status}")
    for issue in issues:
        lines.append(f"Readiness issue: {issue['severity']}:{issue['code']} {issue['message']}")
    return {"status": status, "lines": lines, "issues": issues}


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print machine-readable readiness JSON.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when readiness status is fail.")
    args = parser.parse_args(argv)

    report = build_status_report()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print("\n".join(report["lines"]))
    if args.strict and report["status"] == "fail":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
