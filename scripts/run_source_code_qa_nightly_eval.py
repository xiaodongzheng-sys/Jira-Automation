from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings


DEFAULT_CASES = ["evals/source_code_qa/golden.jsonl", "evals/source_code_qa/scenario_matrix.jsonl"]
OPTIONAL_CASES = ["evals/source_code_qa/golden_real.jsonl"]


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


def run_nightly_eval(*, output_dir: Path, cases: list[str], fixture: bool, include_useful_feedback: bool) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    eval_args = [
        sys.executable,
        "scripts/run_source_code_qa_evals.py",
        "--json",
    ]
    if fixture:
        eval_args.append("--fixture")
    for case_path in cases:
        eval_args.extend(["--cases", case_path])
    eval_payload, eval_stdout, eval_stderr, eval_returncode = _run_json_command(eval_args)

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

    report = {
        "status": "pass" if eval_returncode == 0 and eval_payload.get("status") == "pass" else "fail",
        "timestamp": timestamp,
        "fixture": fixture,
        "cases": cases,
        "eval": {
            "returncode": eval_returncode,
            "status": eval_payload.get("status"),
            "total": eval_payload.get("total"),
            "failed": eval_payload.get("failed"),
            "failure_buckets": eval_payload.get("failure_buckets") or {},
            "coverage_buckets": eval_payload.get("coverage_buckets") or {},
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
        "raw": {
            "eval_stdout": eval_stdout[-20000:],
            "eval_stderr": eval_stderr[-8000:],
            "feedback_stdout": feedback_stdout[-12000:],
            "feedback_stderr": feedback_stderr[-4000:],
        },
    }
    report_path = output_dir / f"source_code_qa_eval_{timestamp}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    (output_dir / "latest.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    report["report_path"] = str(report_path)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local Source Code Q&A quality check and store a timestamped report.")
    parser.add_argument("--cases", action="append", default=None, help="JSONL eval case file. Can be passed multiple times.")
    parser.add_argument("--output-dir", default=None, help="Directory for eval reports. Defaults to TEAM_PORTAL_DATA_DIR/source_code_qa/eval_runs.")
    parser.add_argument("--no-fixture", action="store_true", help="Run against the currently synced repos instead of deterministic fixtures.")
    parser.add_argument("--include-useful-feedback", action="store_true", help="Include useful feedback as positive smoke-test candidates.")
    parser.add_argument("--json", action="store_true", help="Print the report JSON.")
    args = parser.parse_args()

    settings = Settings.from_env()
    output_dir = Path(args.output_dir) if args.output_dir else settings.team_portal_data_dir / "source_code_qa" / "eval_runs"
    case_paths = list(args.cases or DEFAULT_CASES)
    if args.cases is None:
        case_paths.extend(path for path in OPTIONAL_CASES if (ROOT_DIR / path).exists())
    report = run_nightly_eval(
        output_dir=output_dir,
        cases=case_paths,
        fixture=not args.no_fixture,
        include_useful_feedback=bool(args.include_useful_feedback),
    )
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        eval_summary = report.get("eval") or {}
        feedback_summary = report.get("feedback_candidates") or {}
        print(
            "Source Code Q&A nightly eval: "
            f"{report['status']} ({int(eval_summary.get('total') or 0) - int(eval_summary.get('failed') or 0)}/{eval_summary.get('total')} passed)"
        )
        print(f"Feedback candidates: {feedback_summary.get('candidates')} -> {feedback_summary.get('output')}")
        print(f"Report: {report['report_path']}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
