#!/usr/bin/env python3
"""Read-only portal-wide runtime doctor for local Team Portal data."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

SGT = ZoneInfo("Asia/Singapore")
HIGH_TOKEN_THRESHOLD = 30_000
SEATALK_QUALITY_TOKEN_THRESHOLD = 60_000
SLOW_LLM_THRESHOLD_MS = 180_000
DEFAULT_RECENT_HOURS = 24.0


def _resolve_data_root(raw: str | None) -> Path:
    if raw:
        return Path(raw).expanduser().resolve()
    env_value = (
        str(os.getenv("LOCAL_AGENT_TEAM_PORTAL_DATA_DIR") or "").strip()
        or str(os.getenv("TEAM_PORTAL_DATA_DIR") or "").strip()
    )
    if env_value:
        return Path(env_value).expanduser().resolve()
    return (Path.cwd() / ".team-portal").resolve()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _read_jsonl_tail(path: Path, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows[-limit:]


def _counter_dict(counter: Counter[str], *, limit: int = 8) -> dict[str, int]:
    return {key: value for key, value in counter.most_common(limit)}


def _counter_text(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in counts.items())


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    return int(statistics.quantiles(values, n=20, method="inclusive")[18])


def _sgt_timestamp(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and value > 0:
        return datetime.fromtimestamp(value, tz=SGT).strftime("%Y-%m-%d %H:%M:%S SGT")
    return ""


def _event_timestamp(row: dict[str, Any], *fields: str) -> float:
    for field in fields:
        value = row.get(field)
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
        if not isinstance(value, str) or not value.strip():
            continue
        text = value.strip()
        if text.endswith(" SGT"):
            try:
                return datetime.strptime(text, "%Y-%m-%d %H:%M:%S SGT").replace(tzinfo=SGT).timestamp()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
    return 0.0


def _issue(severity: str, code: str, message: str) -> dict[str, str]:
    return {"severity": severity, "code": code, "message": message}


def _top_rows(rows: list[dict[str, Any]], field: str, *, limit: int = 5) -> list[dict[str, Any]]:
    def sort_value(row: dict[str, Any]) -> int:
        return _safe_int(row.get(field))

    top: list[dict[str, Any]] = []
    for row in sorted(rows, key=sort_value, reverse=True)[:limit]:
        value = sort_value(row)
        if value <= 0:
            continue
        extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
        top.append(
            {
                "value": value,
                "timestamp_sgt": str(row.get("timestamp_sgt") or row.get("timestamp") or ""),
                "flow": str(row.get("flow") or "unknown"),
                "route": str(row.get("route") or "unknown"),
                "model_id": str(row.get("model_id") or "unknown"),
                "prompt_mode": str(row.get("prompt_mode") or ""),
                "status": str(row.get("status") or "unknown"),
                "error_category": str(row.get("error_category") or ""),
                "trace_id": str(row.get("trace_id") or ""),
                "codex_phase": str(extra.get("codex_phase") or ""),
                "repair_issue_count": _safe_int(extra.get("repair_issue_count")),
                "queue_wait_ms": _safe_int(row.get("queue_wait_ms")),
                "prompt_compaction_reason": str(extra.get("prompt_compaction_reason") or ""),
                "quality_preserving_over_budget": bool(extra.get("quality_preserving_over_budget")),
            }
        )
    return top


def _quality_preserving_over_budget(row: dict[str, Any]) -> bool:
    extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
    return bool(row.get("quality_preserving_over_budget") or extra.get("quality_preserving_over_budget"))


def _high_token_issue_threshold(row: dict[str, Any]) -> int:
    if str(row.get("flow") or "").strip().lower() == "seatalk":
        return SEATALK_QUALITY_TOKEN_THRESHOLD
    return HIGH_TOKEN_THRESHOLD


def _is_high_token_issue_row(row: dict[str, Any]) -> bool:
    tokens = _safe_int(row.get("estimated_prompt_tokens"))
    if tokens <= 0:
        return False
    return tokens >= _high_token_issue_threshold(row)


def _is_test_fixture_llm_row(row: dict[str, Any]) -> bool:
    prompt_mode = str(row.get("prompt_mode") or "").strip()
    latency_ms = _safe_int(row.get("latency_ms"))
    error = str(row.get("error") or "")
    trace_id = str(row.get("trace_id") or "")
    if "attempt to write a readonly database" in error and latency_ms <= 100:
        return True
    if latency_ms != 0:
        return False
    if not prompt_mode:
        if _safe_int(row.get("estimated_prompt_tokens")) > 250:
            return False
        return trace_id in {"", "trace-bad"} or error in {"quota exhausted", "Bad Request"}
    return (
        str(row.get("route") or "") == "repair"
        and error == "Bad Request"
        and not trace_id
        and _safe_int(row.get("estimated_prompt_tokens")) <= 2_000
    )


def _summarize_llm_ledger(data_root: Path, limit: int, *, recent_hours: float) -> tuple[dict[str, Any], list[dict[str, str]]]:
    ledger_path = Path(os.getenv("LLM_CALL_LEDGER_PATH") or data_root / "llm_call_ledger.jsonl")
    rows = _read_jsonl_tail(ledger_path, limit)
    test_fixture_rows = [row for row in rows if _is_test_fixture_llm_row(row)]
    actionable_rows = [row for row in rows if not _is_test_fixture_llm_row(row)]
    recent_cutoff = datetime.now(SGT).timestamp() - max(1.0, float(recent_hours)) * 3600
    recent_actionable_rows = [
        row
        for row in actionable_rows
        if _event_timestamp(row, "timestamp_sgt", "timestamp") >= recent_cutoff
    ]
    issues: list[dict[str, str]] = []
    flow_counts = Counter(str(row.get("flow") or "unknown") for row in actionable_rows)
    status_counts = Counter(str(row.get("status") or "unknown") for row in actionable_rows)
    model_counts = Counter(str(row.get("model_id") or "unknown") for row in actionable_rows)
    route_counts = Counter(str(row.get("route") or "unknown") for row in actionable_rows)
    provider_counts = Counter(str(row.get("provider") or "unknown") for row in actionable_rows)
    error_counts = Counter(str(row.get("error_category") or "uncategorized") for row in actionable_rows if str(row.get("status") or "") != "ok")
    latencies = [_safe_int(row.get("latency_ms")) for row in actionable_rows if _safe_int(row.get("latency_ms")) > 0]
    prompt_tokens = [_safe_int(row.get("estimated_prompt_tokens")) for row in actionable_rows if _safe_int(row.get("estimated_prompt_tokens")) > 0]
    recent_status_counts = Counter(str(row.get("status") or "unknown") for row in recent_actionable_rows)
    recent_flow_counts = Counter(str(row.get("flow") or "unknown") for row in recent_actionable_rows)
    error_flow_counts = Counter(
        str(row.get("flow") or "unknown")
        for row in recent_actionable_rows
        if str(row.get("status") or "") not in {"ok", "cached"}
    )
    high_token_flow_counts = Counter(
        str(row.get("flow") or "unknown")
        for row in recent_actionable_rows
        if _is_high_token_issue_row(row)
    )
    quality_preserving_high_token_flow_counts = Counter(
        str(row.get("flow") or "unknown")
        for row in recent_actionable_rows
        if _quality_preserving_over_budget(row)
        and _safe_int(row.get("estimated_prompt_tokens")) >= HIGH_TOKEN_THRESHOLD
        and not _is_high_token_issue_row(row)
    )
    slow_flow_counts = Counter(
        str(row.get("flow") or "unknown")
        for row in recent_actionable_rows
        if _safe_int(row.get("latency_ms")) >= SLOW_LLM_THRESHOLD_MS
    )
    unknown_flow = sum(1 for row in recent_actionable_rows if str(row.get("flow") or "unknown") == "unknown")
    error_total = sum(value for key, value in recent_status_counts.items() if key not in {"ok", "cached"})
    timeout_total = recent_status_counts.get("timeout", 0)
    high_token_rows = sum(1 for row in recent_actionable_rows if _is_high_token_issue_row(row))
    quality_preserving_high_token_rows = sum(
        1
        for row in recent_actionable_rows
        if _quality_preserving_over_budget(row)
        and _safe_int(row.get("estimated_prompt_tokens")) >= HIGH_TOKEN_THRESHOLD
        and not _is_high_token_issue_row(row)
    )
    slow_rows = sum(1 for row in recent_actionable_rows if _safe_int(row.get("latency_ms")) >= SLOW_LLM_THRESHOLD_MS)

    if not ledger_path.exists():
        issues.append(_issue("warn", "llm_ledger_missing", f"LLM call ledger missing: {ledger_path}"))
    if unknown_flow:
        issues.append(_issue("warn", "llm_unknown_flow", f"{unknown_flow} LLM ledger rows have unknown flow."))
    if error_total:
        issues.append(_issue("warn", "llm_errors", f"{error_total} LLM ledger rows are non-ok in the sampled window by_flow={_counter_text(_counter_dict(error_flow_counts))}."))
    if timeout_total:
        issues.append(_issue("warn", "llm_timeouts", f"{timeout_total} LLM calls timed out in the sampled window."))
    if high_token_rows:
        issues.append(_issue("warn", "llm_high_prompt_tokens", f"{high_token_rows} LLM calls exceeded flow-aware prompt token thresholds by_flow={_counter_text(_counter_dict(high_token_flow_counts))}."))
    if slow_rows:
        issues.append(_issue("warn", "llm_slow_calls", f"{slow_rows} LLM calls took at least {SLOW_LLM_THRESHOLD_MS} ms by_flow={_counter_text(_counter_dict(slow_flow_counts))}."))

    return (
        {
            "path": str(ledger_path),
            "sample_size": len(rows),
            "actionable_sample_size": len(actionable_rows),
            "recent_actionable_sample_size": len(recent_actionable_rows),
            "test_fixture_rows": len(test_fixture_rows),
            "flows": _counter_dict(flow_counts),
            "statuses": _counter_dict(status_counts),
            "models": _counter_dict(model_counts),
            "routes": _counter_dict(route_counts),
            "providers": _counter_dict(provider_counts),
            "error_categories": _counter_dict(error_counts),
            "recent_flows": _counter_dict(recent_flow_counts),
            "recent_statuses": _counter_dict(recent_status_counts),
            "latency_ms": {
                "p50": int(statistics.median(latencies)) if latencies else 0,
                "p95": _p95(latencies),
                "max": max(latencies) if latencies else 0,
            },
            "estimated_prompt_tokens": {
                "p50": int(statistics.median(prompt_tokens)) if prompt_tokens else 0,
                "p95": _p95(prompt_tokens),
                "max": max(prompt_tokens) if prompt_tokens else 0,
            },
            "quality_preserving_high_prompt_tokens": {
                "count": quality_preserving_high_token_rows,
                "flows": _counter_dict(quality_preserving_high_token_flow_counts),
                "threshold": HIGH_TOKEN_THRESHOLD,
                "seatalk_issue_threshold": SEATALK_QUALITY_TOKEN_THRESHOLD,
            },
            "top_prompt_tokens": _top_rows(actionable_rows, "estimated_prompt_tokens"),
            "top_latency_ms": _top_rows(actionable_rows, "latency_ms"),
        },
        issues,
    )


def _job_stores(data_root: Path) -> list[Path]:
    from bpmis_jira_tool.job_store_registry import job_store_specs

    return [spec.path for spec in job_store_specs(data_root)]


def _summarize_jobs(data_root: Path, limit: int, *, recent_hours: float) -> tuple[dict[str, Any], list[dict[str, str]]]:
    from bpmis_jira_tool.job_store_registry import load_all_job_snapshots

    jobs = load_all_job_snapshots(data_root)
    store_counts: Counter[str] = Counter()
    for job in jobs:
        store_counts[str(job.get("_store") or "unknown")] += 1

    jobs.sort(key=lambda row: _safe_int(row.get("updated_at") or row.get("completed_at") or row.get("created_at")), reverse=True)
    sampled = jobs[:limit]
    state_counts = Counter(str(row.get("state") or row.get("status") or "unknown") for row in sampled)
    stage_counts = Counter(str(row.get("stage") or "unknown") for row in sampled)
    action_counts = Counter(str(row.get("action") or "unknown") for row in sampled)
    def is_problem_job(row: dict[str, Any]) -> bool:
        state = str(row.get("state") or row.get("status") or "").lower()
        stage = str(row.get("stage") or "").lower()
        if state in {"failed", "error"} or stage in {"failed", "error", "interrupted"}:
            return True
        return bool(row.get("error")) and state != "completed"

    problem_jobs = [row for row in sampled if is_problem_job(row)]
    completed_with_stale_error = [
        row
        for row in sampled
        if str(row.get("state") or row.get("status") or "").lower() == "completed" and bool(row.get("error"))
    ]
    active = [
        row
        for row in sampled
        if str(row.get("state") or row.get("status") or "").lower() in {"running", "queued"}
        or str(row.get("stage") or "").lower() in {"running", "queued"}
    ]
    issues: list[dict[str, str]] = []
    if not jobs:
        issues.append(_issue("warn", "job_store_empty", "No runtime job records found."))
    now_ts = datetime.now(SGT).timestamp()
    recent_cutoff = now_ts - max(1.0, float(recent_hours)) * 3600
    recent_failed = [
        row
        for row in problem_jobs
        if _safe_int(row.get("updated_at") or row.get("completed_at") or row.get("created_at")) >= recent_cutoff
    ]
    historical_failed = [row for row in problem_jobs if row not in recent_failed]
    if recent_failed:
        issues.append(_issue("warn", "job_failures", f"{len(recent_failed)} recent sampled jobs are failed, interrupted, or carry an error."))
    if active:
        issues.append(_issue("warn", "active_jobs", f"{len(active)} sampled jobs are still active."))

    def compact(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "job_id": str(row.get("job_id") or row.get("id") or ""),
            "store": str(row.get("_store") or ""),
            "action": str(row.get("action") or "unknown"),
            "state": str(row.get("state") or row.get("status") or "unknown"),
            "stage": str(row.get("stage") or "unknown"),
            "updated_at_sgt": _sgt_timestamp(row.get("updated_at") or row.get("completed_at") or row.get("created_at")),
            "error_category": str(row.get("error_category") or row.get("error_code") or ""),
            "retryable": bool(row.get("error_retryable") or row.get("stalled_retryable")),
            "message": str(row.get("message") or row.get("error") or "")[:180],
        }

    return (
        {
            "sample_size": len(sampled),
            "total_known": len(jobs),
            "stores": _counter_dict(store_counts),
            "states": _counter_dict(state_counts),
            "stages": _counter_dict(stage_counts),
            "actions": _counter_dict(action_counts),
            "recent_problem_count": len(recent_failed),
            "historical_problem_count": len(historical_failed),
            "completed_with_stale_error_count": len(completed_with_stale_error),
            "active_count": len(active),
            "recent_problem_jobs": [compact(row) for row in recent_failed[:8]],
            "historical_problem_jobs": [compact(row) for row in historical_failed[:8]],
            "completed_with_stale_error": [compact(row) for row in completed_with_stale_error[:8]],
            "active_jobs": [compact(row) for row in active[:8]],
        },
        issues,
    )


def _summarize_meeting_records(data_root: Path, limit: int, *, recent_hours: float) -> tuple[dict[str, Any], list[dict[str, str]]]:
    records_root = data_root / "meeting_records" / "records"
    metadata_paths = sorted(records_root.glob("*/metadata.json"))
    rows: list[dict[str, Any]] = []
    for path in metadata_paths:
        payload = _read_json(path)
        if isinstance(payload, dict):
            payload["_path"] = str(path)
            rows.append(payload)
    rows.sort(key=lambda row: str(row.get("updated_at") or row.get("created_at") or ""), reverse=True)
    sampled = rows[:limit]
    statuses = Counter(str(row.get("status") or row.get("state") or "unknown") for row in sampled)
    recent_cutoff = datetime.now(SGT).timestamp() - max(1.0, float(recent_hours)) * 3600
    recent_rows = [
        row
        for row in sampled
        if _event_timestamp(row, "updated_at", "created_at") >= recent_cutoff
    ]
    recent_statuses = Counter(str(row.get("status") or row.get("state") or "unknown") for row in recent_rows)
    issues: list[dict[str, str]] = []
    failed = recent_statuses.get("failed", 0) + recent_statuses.get("error", 0)
    in_progress = recent_statuses.get("recorded", 0) + recent_statuses.get("processing", 0)
    if failed:
        issues.append(_issue("warn", "meeting_record_failures", f"{failed} sampled meeting records are failed or errored."))
    if in_progress:
        issues.append(_issue("warn", "meeting_records_in_progress", f"{in_progress} sampled meeting records are not completed or deleted."))
    recent = [
        {
            "record_id": str(row.get("record_id") or row.get("id") or ""),
            "status": str(row.get("status") or row.get("state") or "unknown"),
            "updated_at": str(row.get("updated_at") or row.get("created_at") or ""),
            "title": str(row.get("title") or row.get("meeting_title") or "")[:120],
        }
        for row in sampled[:5]
    ]
    return (
        {
            "records_root": str(records_root),
            "sample_size": len(sampled),
            "recent_sample_size": len(recent_rows),
            "total_known": len(rows),
            "statuses": _counter_dict(statuses),
            "recent_statuses": _counter_dict(recent_statuses),
            "recent": recent,
        },
        issues,
    )


def _source_code_qa_summary(data_root: Path, limit: int) -> tuple[list[str], list[dict[str, str]]]:
    try:
        from scripts.source_code_qa_ops_summary import build_summary
    except Exception as error:  # pragma: no cover - defensive ops diagnostics
        return [f"source_code_qa_ops_summary=unavailable error={type(error).__name__}: {error}"], [
            _issue("warn", "source_code_qa_ops_unavailable", "Source Code QA ops summary import failed.")
        ]
    try:
        lines = build_summary(data_root, limit=limit, strict=True, prefer_local_agent=True)
    except Exception as error:  # pragma: no cover - defensive ops diagnostics
        return [f"source_code_qa_ops_summary=unavailable error={type(error).__name__}: {error}"], [
            _issue("warn", "source_code_qa_ops_unavailable", "Source Code QA ops summary failed.")
        ]
    issues: list[dict[str, str]] = []
    for line in lines:
        if line.startswith("ops_summary_status=fail"):
            issues.append(_issue("fail", "source_code_qa_ops_fail", "Source Code QA ops guard reports fail."))
        elif line.startswith("ops_summary_issues="):
            issues.append(_issue("fail", "source_code_qa_ops_issues", line.split("=", 1)[1]))
    return lines, issues


def _permission_matrix() -> list[dict[str, str]]:
    return [
        {"surface": "Source Code QA", "visibility": "signed-in portal users", "note": "repo access remains constrained by configured mappings and backend gates"},
        {"surface": "PRD Briefing Tool", "visibility": "admin only", "note": "non-admin users are denied at route level"},
        {"surface": "PRD Self-Assessment", "visibility": "signed-in portal users", "note": "Generate PRD Summary is admin only"},
        {"surface": "SeaTalk Management", "visibility": "admin only", "note": "dashboard/admin operations are restricted"},
        {"surface": "Monthly Report / Team Dashboard", "visibility": "admin only", "note": "report generation paths are privileged"},
        {"surface": "Meeting Recorder", "visibility": "admin only", "note": "recording and processing require admin access"},
        {"surface": "VPN Connection", "visibility": "admin only", "note": "network operations are privileged"},
    ]


def _release_status_lines() -> tuple[list[str], list[dict[str, str]]]:
    try:
        from scripts.release_status import build_status_lines

        return build_status_lines(env=os.environ), []
    except Exception as error:  # pragma: no cover - defensive ops diagnostics
        return [f"release_status=unavailable error={type(error).__name__}: {error}"], [
            _issue("warn", "release_status_unavailable", "Release status probes failed.")
        ]


def _version_plan_firestore_summary() -> tuple[dict[str, str], list[dict[str, str]]]:
    from scripts.release_status import _env_value, _load_version_plan_firestore_payload

    env = os.environ
    stage = "live"
    document = str(_env_value("VERSION_PLAN_FIRESTORE_DOCUMENT", env) or "version_plan_live").strip()
    project = str(_env_value("VERSION_PLAN_FIRESTORE_PROJECT", env) or _env_value("GOOGLE_CLOUD_PROJECT", env) or "").strip()
    backend = str(_env_value("VERSION_PLAN_STORE_BACKEND", env) or "").strip().lower()
    summary = {
        "backend": backend or "auto",
        "document": f"portal/{document}",
        "environment": stage or "live",
        "status": "not_configured",
        "updated_at_sgt": "",
        "source_hash": "",
        "error": "",
    }
    if backend not in {"firestore", "cloud_firestore"} and not project:
        return summary, []
    try:
        payload, error = _load_version_plan_firestore_payload(project=project, document=document, env=env)
        if error:
            summary["status"] = "unavailable"
            summary["error"] = error[:500]
            return summary, [
                _issue(
                    "warn",
                    "version_plan_firestore_unavailable",
                    "Version Plan Firestore document check failed; verify the configured service account has Firestore read permission.",
                )
            ]
        if payload is None:
            summary["status"] = "missing"
            return summary, [_issue("warn", "version_plan_firestore_missing", f"Version Plan Firestore document is missing: portal/{document}")]
    except Exception as error:
        summary["status"] = f"unavailable:{type(error).__name__}"
        summary["error"] = f"{type(error).__name__}: {error}"[:500]
        return summary, [
            _issue(
                "warn",
                "version_plan_firestore_unavailable",
                "Version Plan Firestore document check failed; verify the configured service account has Firestore read permission.",
            )
        ]
    summary["status"] = "ok"
    summary["environment"] = str(payload.get("environment") or summary["environment"])
    summary["updated_at_sgt"] = str(payload.get("updated_at_sgt") or "")
    summary["source_hash"] = str(payload.get("source_hash") or "")
    return summary, []


def _mac_portal_runtime_summary() -> tuple[dict[str, str], list[dict[str, str]]]:
    from scripts.release_status import _mac_portal_availability_status

    summary = {
        "status": "unknown",
        "details": _mac_portal_availability_status(env=os.environ),
    }
    details = summary["details"]
    if "status=online" in details:
        summary["status"] = "online"
        return summary, []
    if "status=degraded" in details:
        summary["status"] = "degraded"
        return summary, [_issue("warn", "mac_portal_degraded", "Mac portal availability is degraded.")]
    summary["status"] = "offline"
    return summary, [_issue("warn", "mac_portal_offline", "Mac portal availability is offline.")]


def _shared_session_summary() -> tuple[dict[str, str], list[dict[str, str]]]:
    from scripts.release_status import _shared_session_status

    details = _shared_session_status(env=os.environ)
    summary = {"status": "unknown", "details": details}
    if "status=not_applicable" in details:
        summary["status"] = "not_applicable"
        return summary, []
    if "status=ok" in details:
        summary["status"] = "ok"
        return summary, []
    if "status=warn" in details:
        summary["status"] = "warn"
        return summary, [_issue("warn", "shared_session_unverified", "Shared Cloud/Mac session configuration is not fully verified.")]
    summary["status"] = "fail"
    return summary, [_issue("fail", "shared_session_misconfigured", "Shared Cloud/Mac session configuration is misconfigured.")]


def build_report(
    data_root: Path,
    *,
    limit: int = 200,
    recent_hours: float = DEFAULT_RECENT_HOURS,
    include_release_status: bool = False,
) -> dict[str, Any]:
    data_root = data_root.expanduser().resolve()
    issues: list[dict[str, str]] = []
    if not data_root.exists():
        issues.append(_issue("fail", "data_root_missing", f"Data root missing: {data_root}"))

    llm, llm_issues = _summarize_llm_ledger(data_root, limit, recent_hours=recent_hours)
    jobs, job_issues = _summarize_jobs(data_root, limit, recent_hours=recent_hours)
    meetings, meeting_issues = _summarize_meeting_records(data_root, limit, recent_hours=recent_hours)
    scqa_lines, scqa_issues = _source_code_qa_summary(data_root, limit)
    version_plan, version_plan_issues = _version_plan_firestore_summary()
    mac_portal, mac_portal_issues = _mac_portal_runtime_summary()
    shared_session, shared_session_issues = _shared_session_summary()
    issues.extend(llm_issues)
    issues.extend(job_issues)
    issues.extend(meeting_issues)
    issues.extend(scqa_issues)
    issues.extend(version_plan_issues)
    issues.extend(mac_portal_issues)
    issues.extend(shared_session_issues)

    release_lines: list[str] = []
    if include_release_status:
        release_lines, release_issues = _release_status_lines()
        issues.extend(release_issues)

    if any(issue["severity"] == "fail" for issue in issues):
        status = "fail"
    elif issues:
        status = "warn"
    else:
        status = "pass"

    return {
        "generated_at_sgt": datetime.now(SGT).strftime("%Y-%m-%d %H:%M:%S SGT"),
        "status": status,
        "data_root": str(data_root),
        "limit": limit,
        "recent_hours": recent_hours,
        "llm": llm,
        "jobs": jobs,
        "meeting_records": meetings,
        "source_code_qa": {"lines": scqa_lines},
        "version_plan_firestore": version_plan,
        "mac_portal": mac_portal,
        "shared_session": shared_session,
        "permissions": _permission_matrix(),
        "release_status": {"included": include_release_status, "lines": release_lines},
        "issues": issues,
    }


def format_report(report: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    lines.append("== Portal Runtime Doctor ==")
    lines.append(f"generated_at_sgt={report['generated_at_sgt']}")
    lines.append(f"data_root={report['data_root']}")
    lines.append(f"overall_status={report['status']}")

    llm = report["llm"]
    lines.append("")
    lines.append("== LLM Ledger ==")
    lines.append(f"ledger_path={llm['path']}")
    lines.append(f"ledger_sample_size={llm['sample_size']}")
    lines.append(
        f"llm_actionable_sample_size={llm['actionable_sample_size']} "
        f"recent_actionable_sample_size={llm['recent_actionable_sample_size']} "
        f"test_fixture_rows={llm['test_fixture_rows']}"
    )
    lines.append(f"llm_flows={_counter_text(llm['flows'])}")
    lines.append(f"llm_statuses={_counter_text(llm['statuses'])}")
    lines.append(f"llm_models={_counter_text(llm['models'])}")
    lines.append(f"llm_routes={_counter_text(llm['routes'])}")
    lines.append(f"llm_providers={_counter_text(llm['providers'])}")
    lines.append(f"llm_error_categories={_counter_text(llm['error_categories'])}")
    lines.append(f"llm_recent_flows={_counter_text(llm['recent_flows'])}")
    lines.append(f"llm_recent_statuses={_counter_text(llm['recent_statuses'])}")
    lines.append(
        "llm_latency_ms="
        f"p50={llm['latency_ms']['p50']} p95={llm['latency_ms']['p95']} max={llm['latency_ms']['max']}"
    )
    lines.append(
        "llm_prompt_tokens="
        f"p50={llm['estimated_prompt_tokens']['p50']} "
        f"p95={llm['estimated_prompt_tokens']['p95']} "
        f"max={llm['estimated_prompt_tokens']['max']}"
    )
    quality_prompt = llm.get("quality_preserving_high_prompt_tokens") or {}
    if quality_prompt.get("count"):
        lines.append(
            "llm_quality_preserving_high_prompt_tokens="
            f"{quality_prompt['count']} threshold={quality_prompt['threshold']} "
            f"seatalk_issue_threshold={quality_prompt['seatalk_issue_threshold']} "
            f"flows={_counter_text(quality_prompt.get('flows') or {})}"
        )
    for row in llm["top_prompt_tokens"]:
        lines.append(
            "llm_top_prompt_tokens="
            f"{row['value']} flow={row['flow']} route={row['route']} model={row['model_id']} "
            f"status={row['status']} at={row['timestamp_sgt']} mode={row['prompt_mode']} "
            f"quality_preserving={row['quality_preserving_over_budget']} reason={row['prompt_compaction_reason']}"
        )
    for row in llm["top_latency_ms"]:
        lines.append(
            "llm_top_latency_ms="
            f"{row['value']} flow={row['flow']} route={row['route']} model={row['model_id']} "
            f"status={row['status']} at={row['timestamp_sgt']} mode={row['prompt_mode']} "
            f"trace_id={row['trace_id']} phase={row['codex_phase']} repair_issues={row['repair_issue_count']} "
            f"queue_wait_ms={row['queue_wait_ms']}"
        )

    jobs = report["jobs"]
    lines.append("")
    lines.append("== Jobs ==")
    lines.append(f"job_sample_size={jobs['sample_size']} total_known={jobs['total_known']}")
    lines.append(f"job_stores={_counter_text(jobs['stores'])}")
    lines.append(f"job_states={_counter_text(jobs['states'])}")
    lines.append(f"job_stages={_counter_text(jobs['stages'])}")
    lines.append(f"job_actions={_counter_text(jobs['actions'])}")
    lines.append(
        "job_current_problem_count="
        f"{jobs['recent_problem_count']} active_count={jobs['active_count']} "
        f"historical_problem_count={jobs['historical_problem_count']} "
        f"stale_completed_error_count={jobs['completed_with_stale_error_count']}"
    )
    for row in jobs["recent_problem_jobs"]:
        lines.append(
            "job_problem="
            f"{row['updated_at_sgt']} store={row['store']} action={row['action']} "
            f"state={row['state']} stage={row['stage']} retryable={row['retryable']} message={row['message']}"
        )
    for row in jobs["historical_problem_jobs"]:
        lines.append(
            "job_historical_problem_info="
            f"{row['updated_at_sgt']} store={row['store']} action={row['action']} "
            f"state={row['state']} stage={row['stage']} retryable={row['retryable']} message={row['message']}"
        )
    for row in jobs["completed_with_stale_error"]:
        lines.append(
            "job_stale_completed_error_info="
            f"{row['updated_at_sgt']} store={row['store']} action={row['action']} message={row['message']}"
        )
    for row in jobs["active_jobs"]:
        lines.append(
            "job_active="
            f"{row['updated_at_sgt']} store={row['store']} action={row['action']} "
            f"state={row['state']} stage={row['stage']} retryable={row['retryable']} message={row['message']}"
        )

    meetings = report["meeting_records"]
    lines.append("")
    lines.append("== Meeting Records ==")
    lines.append(f"meeting_records_root={meetings['records_root']}")
    lines.append(
        f"meeting_record_sample_size={meetings['sample_size']} "
        f"recent_sample_size={meetings['recent_sample_size']} total_known={meetings['total_known']}"
    )
    lines.append(f"meeting_record_statuses={_counter_text(meetings['statuses'])}")
    lines.append(f"meeting_record_recent_statuses={_counter_text(meetings['recent_statuses'])}")

    lines.append("")
    lines.append("== Source Code QA Ops ==")
    lines.extend(report["source_code_qa"]["lines"])

    version_plan = report["version_plan_firestore"]
    lines.append("")
    lines.append("== Version Plan Firestore ==")
    lines.append(
        f"version_plan_firestore_status={version_plan['status']} "
        f"backend={version_plan['backend']} document={version_plan['document']} "
        f"environment={version_plan['environment']} updated_at_sgt={version_plan['updated_at_sgt'] or '<missing>'} "
        f"source_hash={version_plan['source_hash'] or '<missing>'}"
    )
    if version_plan.get("error"):
        lines.append(f"version_plan_firestore_error={version_plan['error']}")

    mac_portal = report["mac_portal"]
    lines.append("")
    lines.append("== Mac Portal Availability ==")
    lines.append(f"mac_portal_status={mac_portal['status']} details={mac_portal['details']}")

    shared_session = report["shared_session"]
    lines.append("")
    lines.append("== Shared Session Configuration ==")
    lines.append(f"shared_session_status={shared_session['status']} details={shared_session['details']}")

    if report["release_status"]["included"]:
        lines.append("")
        lines.append("== Release Status ==")
        lines.extend(report["release_status"]["lines"])

    lines.append("")
    lines.append("== Permission Snapshot ==")
    for row in report["permissions"]:
        lines.append(f"permission={row['surface']} visibility={row['visibility']} note={row['note']}")

    lines.append("")
    lines.append("== Doctor Issues ==")
    if report["issues"]:
        for issue in report["issues"]:
            lines.append(f"{issue['severity']}:{issue['code']} {issue['message']}")
    else:
        lines.append("none")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=os.environ.get("TEAM_PORTAL_DATA_DIR"))
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--recent-hours", type=float, default=DEFAULT_RECENT_HOURS, help="Only warn on runtime issues updated within this window.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of text.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero only when the doctor status is fail.")
    parser.add_argument("--include-release-status", action="store_true", help="Include release status probes; may call gcloud/curl.")
    args = parser.parse_args(argv)

    report = build_report(
        _resolve_data_root(args.data_root),
        limit=max(1, args.limit),
        recent_hours=max(1.0, float(args.recent_hours)),
        include_release_status=bool(args.include_release_status),
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        for line in format_report(report):
            print(line)
    return 1 if args.strict and report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
