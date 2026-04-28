#!/usr/bin/env python3
"""Summarize recent Source Code QA operating signals from local JSONL files."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DEMO_REPO_MARKERS = ("git.example.com",)


def _resolve_data_root(raw: str | None) -> Path:
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path.cwd() / "data").resolve()


def _read_tail_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
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


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _counter_text(counter: Counter[str], *, limit: int = 4) -> str:
    if not counter:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in counter.most_common(limit))


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    return int(statistics.quantiles(values, n=20, method="inclusive")[18])


def _source_config_summary(source_root: Path) -> tuple[list[str], list[str]]:
    config = _read_json(source_root / "config.json")
    mappings = config.get("mappings") if isinstance(config.get("mappings"), dict) else {}
    repo_count = 0
    demo_repos: list[str] = []
    keys = sorted(str(key) for key in mappings)
    for key, repos in mappings.items():
        if not isinstance(repos, list):
            continue
        for repo in repos:
            if not isinstance(repo, dict):
                continue
            repo_count += 1
            url = str(repo.get("url") or "")
            if any(marker in url for marker in DEMO_REPO_MARKERS):
                demo_repos.append(f"{key}:{repo.get('display_name') or url}")

    lines = [f"active_config_repos={repo_count} keys={','.join(keys) if keys else 'none'}"]
    issues: list[str] = []
    if demo_repos:
        preview = ", ".join(demo_repos[:5])
        suffix = f", ...(+{len(demo_repos) - 5})" if len(demo_repos) > 5 else ""
        lines.append(f"active_config_demo_repos={len(demo_repos)} {preview}{suffix}")
        issues.append("active config contains fixture/demo repositories")
    else:
        lines.append("active_config_demo_repos=0")
    return lines, issues


def _index_health_summary(data_root: Path) -> tuple[list[str], list[str]]:
    try:
        from bpmis_jira_tool.source_code_qa import SourceCodeQAService
        from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS

        service = SourceCodeQAService(data_root=data_root, team_profiles=TEAM_PROFILE_DEFAULTS)
        health = service.index_health_payload()
    except Exception as error:  # pragma: no cover - defensive ops diagnostics
        return [f"index_health=unavailable error={type(error).__name__}: {error}"], ["index health unavailable"]

    totals = health.get("totals") or {}
    repo_count = int(totals.get("repos") or 0)
    ready = int(totals.get("ready") or 0)
    stale_or_missing = int(totals.get("stale_or_missing") or 0)
    status = str(health.get("status") or "unknown")
    lines = [f"index_health={status} ready={ready}/{repo_count} stale_or_missing={stale_or_missing}"]
    issues: list[str] = []
    if repo_count and status != "ready":
        stale: list[str] = []
        for key, payload in (health.get("keys") or {}).items():
            for repo in payload.get("repos") or []:
                index = repo.get("index") or {}
                if index.get("state") != "ready":
                    stale.append(f"{key}:{repo.get('display_name') or repo.get('url')}")
        if stale:
            preview = ", ".join(stale[:5])
            suffix = f", ...(+{len(stale) - 5})" if len(stale) > 5 else ""
            lines.append(f"stale_index_repos={len(stale)} {preview}{suffix}")
        issues.append("configured repositories do not all have ready indexes")
    return lines, issues


def build_summary(data_root: Path, *, limit: int = 200, strict: bool = False) -> list[str]:
    source_root = data_root / "source_code_qa"
    telemetry_rows = _read_tail_jsonl(source_root / "telemetry.jsonl", limit)
    feedback_rows = _read_tail_jsonl(source_root / "feedback.jsonl", limit)
    review_rows = _read_tail_jsonl(source_root / "review_queue.jsonl", limit)
    eval_status = _read_json(data_root / "run" / "source_code_qa_eval_status.json")
    issues: list[str] = []

    lines: list[str] = []
    lines.append(f"data_root={data_root}")
    config_lines, config_issues = _source_config_summary(source_root)
    lines.extend(config_lines)
    issues.extend(config_issues)
    health_lines, health_issues = _index_health_summary(data_root)
    lines.extend(health_lines)
    issues.extend(health_issues)

    if telemetry_rows:
        status_counts = Counter(str(row.get("status") or "unknown") for row in telemetry_rows)
        route_counts = Counter(str((row.get("llm_route") or {}).get("mode") or row.get("answer_mode") or "unknown") for row in telemetry_rows)
        policy_counts = Counter(str((row.get("answer_contract") or {}).get("status") or "unknown") for row in telemetry_rows)
        latencies = [
            int(row.get("latency_ms") or row.get("total_latency_ms") or 0)
            for row in telemetry_rows
            if isinstance(row.get("latency_ms") or row.get("total_latency_ms") or 0, (int, float))
        ]
        no_match = status_counts.get("no_match", 0)
        stale = sum(1 for row in telemetry_rows if str((row.get("index_freshness") or {}).get("status") or "").startswith("stale"))
        newest = telemetry_rows[-1].get("created_at") or telemetry_rows[-1].get("timestamp") or "unknown"
        lines.append(f"telemetry_window={len(telemetry_rows)} newest={newest}")
        lines.append(f"query_status={_counter_text(status_counts)}")
        lines.append(f"routes={_counter_text(route_counts)}")
        lines.append(f"answer_contract={_counter_text(policy_counts)}")
        lines.append(f"latency_ms_p50={int(statistics.median(latencies)) if latencies else 0} p95={_p95(latencies)}")
        lines.append(f"no_match_rate={no_match}/{len(telemetry_rows)} stale_index_hits={stale}")
    else:
        lines.append("telemetry_window=0")

    if feedback_rows:
        feedback_counts = Counter(str(row.get("rating") or "unknown") for row in feedback_rows)
        newest_feedback = feedback_rows[-1].get("created_at") or "unknown"
        lines.append(f"feedback_window={len(feedback_rows)} newest={newest_feedback} ratings={_counter_text(feedback_counts)}")
    else:
        lines.append("feedback_window=0")

    if review_rows:
        priorities = Counter(str(row.get("priority") or "unknown") for row in review_rows)
        lines.append(f"review_queue={len(review_rows)} priorities={_counter_text(priorities)}")
    else:
        lines.append("review_queue=0")

    state = eval_status.get("state") or "missing"
    updated_unix = eval_status.get("updated_unix")
    if isinstance(updated_unix, int):
        age = int(time.time()) - updated_unix
        lines.append(f"latest_eval_state={state} age_seconds={age}")
    else:
        lines.append(f"latest_eval_state={state}")
    if eval_status.get("failed_cases"):
        lines.append(f"latest_eval_failed_cases={eval_status.get('failed_cases')}")
    if strict:
        lines.append(f"ops_summary_status={'fail' if issues else 'pass'}")
        if issues:
            lines.append("ops_summary_issues=" + "; ".join(issues))
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=os.environ.get("TEAM_PORTAL_DATA_DIR"))
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when active config or index health is unsafe.")
    args = parser.parse_args()

    data_root = _resolve_data_root(args.data_root)
    lines = build_summary(data_root, limit=max(1, args.limit), strict=bool(args.strict))
    for line in lines:
        print(line)
    return 1 if args.strict and any(line.startswith("ops_summary_status=fail") for line in lines) else 0


if __name__ == "__main__":
    raise SystemExit(main())
