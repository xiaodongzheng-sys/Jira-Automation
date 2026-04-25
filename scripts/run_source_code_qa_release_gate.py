#!/usr/bin/env python3
"""Run the Source Code Q&A release gate before publishing retrieval or prompt changes."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings
from scripts.run_source_code_qa_nightly_eval import DEFAULT_CASES, OPTIONAL_CASES, run_nightly_eval


DEFAULT_THRESHOLDS = {
    "max_eval_failed": 0,
    "max_eval_failed_per_team": 0,
    "max_llm_smoke_failed": 0,
    "min_eval_cases": 20,
    "min_eval_cases_per_team": 4,
    "required_eval_teams": ["AF", "CRMS", "GRC"],
    "require_review_queue": True,
}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.{id(payload)}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def evaluate_release_gate(report: dict[str, Any], thresholds: dict[str, Any] | None = None) -> dict[str, Any]:
    rules = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    eval_summary = report.get("eval") or {}
    smoke_summary = report.get("llm_smoke") or {}
    review_summary = report.get("review_queue") or {}
    eval_total = int(eval_summary.get("total") or 0)
    eval_failed = int(eval_summary.get("failed") or 0)
    smoke_failed = int(smoke_summary.get("failed") or 0)
    team_buckets = eval_summary.get("team_buckets") or {}
    required_teams = [str(team).upper() for team in (rules.get("required_eval_teams") or [])]
    min_cases_per_team = int(rules["min_eval_cases_per_team"])
    max_failed_per_team = int(rules["max_eval_failed_per_team"])
    missing_or_thin_teams = [
        team
        for team in required_teams
        if int((team_buckets.get(team) or {}).get("total") or 0) < min_cases_per_team
    ]
    failing_teams = [
        team
        for team in required_teams
        if int((team_buckets.get(team) or {}).get("failed") or 0) > max_failed_per_team
    ]
    checks = {
        "nightly_status": report.get("status") == "pass",
        "eval_failed": eval_failed <= int(rules["max_eval_failed"]),
        "eval_team_coverage": not missing_or_thin_teams,
        "eval_failed_per_team": not failing_teams,
        "llm_smoke_failed": smoke_failed <= int(rules["max_llm_smoke_failed"]),
        "min_eval_cases": eval_total >= int(rules["min_eval_cases"]),
        "review_queue_generated": (not rules["require_review_queue"]) or int(review_summary.get("returncode") or 0) == 0,
    }
    passed = all(checks.values())
    failed_checks = [name for name, ok in checks.items() if not ok]
    return {
        "status": "pass" if passed else "fail",
        "summary": "Release gate passed." if passed else f"Release gate failed: {', '.join(failed_checks)}.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "thresholds": rules,
        "checks": checks,
        "failed_checks": failed_checks,
        "report_path": report.get("report_path"),
        "eval": eval_summary,
        "team_buckets": team_buckets,
        "missing_or_thin_teams": missing_or_thin_teams,
        "failing_teams": failing_teams,
        "llm_smoke": smoke_summary,
    }


def run_release_gate(
    *,
    data_root: Path,
    cases: list[str],
    fixture: bool,
    include_useful_feedback: bool,
    thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output_dir = data_root / "source_code_qa" / "eval_runs"
    report = run_nightly_eval(
        output_dir=output_dir,
        cases=cases,
        fixture=fixture,
        include_useful_feedback=include_useful_feedback,
    )
    gate = evaluate_release_gate(report, thresholds=thresholds)
    run_root = data_root / "run"
    _atomic_write_json(run_root / "source_code_qa_release_gate.json", gate)
    _atomic_write_json(
        run_root / "source_code_qa_eval_status.json",
        {
            "state": "passed" if gate["status"] == "pass" else "failed",
            "message": gate["summary"],
            "updated_at": gate["timestamp"],
            "updated_unix": int(datetime.now(timezone.utc).timestamp()),
            "failed_cases": report.get("eval", {}).get("failed_cases") or [],
            "release_gate": gate["status"],
        },
    )
    return gate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", action="append", default=None, help="JSONL eval case file. Can be passed multiple times.")
    parser.add_argument("--data-root", default=None, help="Defaults to TEAM_PORTAL_DATA_DIR.")
    parser.add_argument("--no-fixture", action="store_true", help="Run against synced repos instead of deterministic fixtures.")
    parser.add_argument("--include-useful-feedback", action="store_true", help="Include useful feedback as positive smoke-test candidates.")
    parser.add_argument("--min-eval-cases", type=int, default=DEFAULT_THRESHOLDS["min_eval_cases"])
    parser.add_argument("--min-eval-cases-per-team", type=int, default=DEFAULT_THRESHOLDS["min_eval_cases_per_team"])
    parser.add_argument(
        "--required-eval-team",
        action="append",
        default=None,
        help="Required PM team code for deterministic eval coverage. Can be passed multiple times.",
    )
    parser.add_argument("--json", action="store_true", help="Print gate JSON.")
    args = parser.parse_args()

    settings = Settings.from_env()
    data_root = Path(args.data_root).expanduser().resolve() if args.data_root else settings.team_portal_data_dir
    case_paths = list(args.cases or DEFAULT_CASES)
    if args.cases is None:
        case_paths.extend(path for path in OPTIONAL_CASES if (ROOT_DIR / path).exists())
    gate = run_release_gate(
        data_root=data_root,
        cases=case_paths,
        fixture=not args.no_fixture,
        include_useful_feedback=bool(args.include_useful_feedback),
        thresholds={
            "min_eval_cases": int(args.min_eval_cases),
            "min_eval_cases_per_team": int(args.min_eval_cases_per_team),
            "required_eval_teams": args.required_eval_team or DEFAULT_THRESHOLDS["required_eval_teams"],
        },
    )
    if args.json:
        print(json.dumps(gate, indent=2, ensure_ascii=False))
    else:
        print(f"Source Code Q&A release gate: {gate['status']} - {gate['summary']}")
        print(f"Report: {gate.get('report_path')}")
    return 0 if gate["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
