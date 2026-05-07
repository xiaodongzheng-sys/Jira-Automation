#!/usr/bin/env python3
"""Run the Source Code Q&A release gate before publishing retrieval or prompt changes."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings


RELEASE_GATE_CASES = ["evals/source_code_qa/release_gate.jsonl"]


DEFAULT_THRESHOLDS = {
    "max_eval_failed": 0,
    "max_eval_failed_per_segment": 0,
    "max_eval_failed_per_team": 0,
    "max_llm_smoke_failed": 0,
    "min_eval_cases": 20,
    "min_eval_cases_per_segment": 2,
    "min_eval_cases_per_team": 4,
    "required_eval_segments": ["AF:ALL", "CRMS:ID", "CRMS:SG", "CRMS:PH", "GRC:ALL"],
    "required_eval_teams": ["AF", "CRMS", "GRC"],
    "require_review_queue": True,
}


def _run_json_command(args: list[str]) -> tuple[dict[str, Any], str, str, int]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT_DIR) if not existing_pythonpath else f"{ROOT_DIR}{os.pathsep}{existing_pythonpath}"
    completed = subprocess.run(args, cwd=ROOT_DIR, env=env, capture_output=True, text=True, check=False)
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        payload = {"status": "error", "message": "command did not return JSON"}
    return payload, stdout, stderr, int(completed.returncode)


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
    segment_buckets = {str(key).upper(): value for key, value in (eval_summary.get("segment_buckets") or {}).items()}
    required_teams = [str(team).upper() for team in (rules.get("required_eval_teams") or [])]
    required_segments = [str(segment).upper() for segment in (rules.get("required_eval_segments") or [])]
    min_cases_per_team = int(rules["min_eval_cases_per_team"])
    min_cases_per_segment = int(rules["min_eval_cases_per_segment"])
    max_failed_per_team = int(rules["max_eval_failed_per_team"])
    max_failed_per_segment = int(rules["max_eval_failed_per_segment"])
    missing_or_thin_teams = [
        team
        for team in required_teams
        if int((team_buckets.get(team) or {}).get("total") or 0) < min_cases_per_team
    ]
    missing_or_thin_segments = [
        segment
        for segment in required_segments
        if int((segment_buckets.get(segment) or {}).get("total") or 0) < min_cases_per_segment
    ]
    failing_teams = [
        team
        for team in required_teams
        if int((team_buckets.get(team) or {}).get("failed") or 0) > max_failed_per_team
    ]
    failing_segments = [
        segment
        for segment in required_segments
        if int((segment_buckets.get(segment) or {}).get("failed") or 0) > max_failed_per_segment
    ]
    checks = {
        "eval_report_status": report.get("status") == "pass",
        "eval_failed": eval_failed <= int(rules["max_eval_failed"]),
        "eval_team_coverage": not missing_or_thin_teams,
        "eval_failed_per_team": not failing_teams,
        "eval_segment_coverage": not missing_or_thin_segments,
        "eval_failed_per_segment": not failing_segments,
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
        "segment_buckets": segment_buckets,
        "missing_or_thin_teams": missing_or_thin_teams,
        "missing_or_thin_segments": missing_or_thin_segments,
        "failing_teams": failing_teams,
        "failing_segments": failing_segments,
        "llm_smoke": smoke_summary,
    }


def run_release_eval_report(
    *,
    output_dir: Path,
    cases: list[str],
    fixture: bool,
    include_useful_feedback: bool,
    mock_llm: bool = True,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fixture_data_root = output_dir / "fixture_data"
    if fixture:
        shutil.rmtree(fixture_data_root, ignore_errors=True)
    eval_args = [
        sys.executable,
        "scripts/run_source_code_qa_evals.py",
        "--json",
    ]
    if fixture:
        eval_args.extend(["--fixture", "--data-root", str(fixture_data_root)])
    if mock_llm:
        eval_args.append("--mock-llm")
    for case_path in cases:
        eval_args.extend(["--cases", case_path])
    eval_payload, eval_stdout, eval_stderr, eval_returncode = _run_json_command(eval_args)

    llm_smoke_data_root = output_dir / "llm_smoke_data"
    shutil.rmtree(llm_smoke_data_root, ignore_errors=True)
    llm_smoke_args = [
        sys.executable,
        "scripts/run_source_code_qa_evals.py",
        "--json",
        "--fixture",
        "--mock-llm",
        "--data-root",
        str(llm_smoke_data_root),
        "--cases",
        "evals/source_code_qa/llm_smoke.jsonl",
    ]
    llm_smoke_payload, llm_smoke_stdout, llm_smoke_stderr, llm_smoke_returncode = _run_json_command(llm_smoke_args)

    feedback_output = output_dir / f"feedback_candidates_{timestamp}.jsonl"
    feedback_args = [
        sys.executable,
        "scripts/source_code_qa_feedback_to_eval.py",
        "--json",
        "--output",
        str(feedback_output),
    ]
    if include_useful_feedback:
        feedback_args.append("--include-useful")
    feedback_payload, feedback_stdout, feedback_stderr, feedback_returncode = _run_json_command(feedback_args)

    review_output = output_dir / f"review_queue_{timestamp}.jsonl"
    review_args = [
        sys.executable,
        "scripts/source_code_qa_review_queue.py",
        "--json",
        "--output",
        str(review_output),
    ]
    review_payload, review_stdout, review_stderr, review_returncode = _run_json_command(review_args)

    report = {
        "status": "pass"
        if (
            eval_returncode == 0
            and eval_payload.get("status") == "pass"
            and llm_smoke_returncode == 0
            and llm_smoke_payload.get("status") == "pass"
            and review_returncode == 0
        )
        else "fail",
        "timestamp": timestamp,
        "fixture": fixture,
        "mock_llm": bool(mock_llm),
        "cases": cases,
        "eval": {
            "returncode": eval_returncode,
            "status": eval_payload.get("status"),
            "total": eval_payload.get("total"),
            "failed": eval_payload.get("failed"),
            "failure_buckets": eval_payload.get("failure_buckets") or {},
            "coverage_buckets": eval_payload.get("coverage_buckets") or {},
            "team_buckets": eval_payload.get("team_buckets") or {},
            "segment_buckets": eval_payload.get("segment_buckets") or {},
            "route_buckets": eval_payload.get("route_buckets") or {},
        },
        "feedback_candidates": {
            "returncode": feedback_returncode,
            "status": feedback_payload.get("status"),
            "feedback_records": feedback_payload.get("feedback_records"),
            "candidates": feedback_payload.get("candidates"),
            "draft_statuses": feedback_payload.get("draft_statuses") or {},
            "output": str(feedback_output),
        },
        "llm_smoke": {
            "returncode": llm_smoke_returncode,
            "status": llm_smoke_payload.get("status"),
            "total": llm_smoke_payload.get("total"),
            "failed": llm_smoke_payload.get("failed"),
            "route_buckets": llm_smoke_payload.get("route_buckets") or {},
        },
        "review_queue": {
            "returncode": review_returncode,
            "status": review_payload.get("status"),
            "review_items": review_payload.get("review_items"),
            "high_priority": review_payload.get("high_priority"),
            "output": str(review_output),
        },
        "raw": {
            "eval_stdout": eval_stdout[-20000:],
            "eval_stderr": eval_stderr[-8000:],
            "llm_smoke_stdout": llm_smoke_stdout[-12000:],
            "llm_smoke_stderr": llm_smoke_stderr[-4000:],
            "feedback_stdout": feedback_stdout[-12000:],
            "feedback_stderr": feedback_stderr[-4000:],
            "review_stdout": review_stdout[-12000:],
            "review_stderr": review_stderr[-4000:],
        },
    }
    report_path = output_dir / f"source_code_qa_eval_{timestamp}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    (output_dir / "latest.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    report["report_path"] = str(report_path)
    return report


def run_release_gate(
    *,
    data_root: Path,
    cases: list[str],
    fixture: bool,
    include_useful_feedback: bool,
    mock_llm: bool = True,
    thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output_dir = data_root / "source_code_qa" / "eval_runs"
    report = run_release_eval_report(
        output_dir=output_dir,
        cases=cases,
        fixture=fixture,
        include_useful_feedback=include_useful_feedback,
        mock_llm=mock_llm,
    )
    gate = evaluate_release_gate(report, thresholds=thresholds)
    run_root = data_root / "run"
    _atomic_write_json(run_root / "source_code_qa_release_gate.json", gate)
    return gate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", action="append", default=None, help="JSONL eval case file. Can be passed multiple times.")
    parser.add_argument("--data-root", default=None, help="Defaults to TEAM_PORTAL_DATA_DIR.")
    parser.add_argument("--no-fixture", action="store_true", help="Run against synced repos instead of deterministic fixtures.")
    parser.add_argument(
        "--live-llm",
        action="store_true",
        help="Use the configured live LLM provider for the main eval. By default the release gate uses deterministic mock LLM to avoid Codex CLI environment drift.",
    )
    parser.add_argument("--include-useful-feedback", action="store_true", help="Include useful feedback as positive smoke-test candidates.")
    parser.add_argument("--min-eval-cases", type=int, default=DEFAULT_THRESHOLDS["min_eval_cases"])
    parser.add_argument("--min-eval-cases-per-segment", type=int, default=DEFAULT_THRESHOLDS["min_eval_cases_per_segment"])
    parser.add_argument("--min-eval-cases-per-team", type=int, default=DEFAULT_THRESHOLDS["min_eval_cases_per_team"])
    parser.add_argument(
        "--required-eval-team",
        action="append",
        default=None,
        help="Required PM team code for deterministic eval coverage. Can be passed multiple times.",
    )
    parser.add_argument(
        "--required-eval-segment",
        action="append",
        default=None,
        help="Required PM team/country segment, for example CRMS:SG. Can be passed multiple times.",
    )
    parser.add_argument("--json", action="store_true", help="Print gate JSON.")
    args = parser.parse_args()

    settings = Settings.from_env()
    data_root = Path(args.data_root).expanduser().resolve() if args.data_root else settings.team_portal_data_dir
    case_paths = list(args.cases or RELEASE_GATE_CASES)
    gate = run_release_gate(
        data_root=data_root,
        cases=case_paths,
        fixture=not args.no_fixture,
        include_useful_feedback=bool(args.include_useful_feedback),
        mock_llm=not bool(args.live_llm),
        thresholds={
            "min_eval_cases": int(args.min_eval_cases),
            "min_eval_cases_per_segment": int(args.min_eval_cases_per_segment),
            "min_eval_cases_per_team": int(args.min_eval_cases_per_team),
            "required_eval_segments": args.required_eval_segment or DEFAULT_THRESHOLDS["required_eval_segments"],
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
