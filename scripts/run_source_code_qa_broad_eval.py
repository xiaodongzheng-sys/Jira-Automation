#!/usr/bin/env python3
"""Run broad, non-blocking Source Code QA quality evals."""

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
from scripts.source_code_qa_auto_eval_candidates import build_auto_eval_candidates, _write_jsonl


BROAD_QUALITY_CASES = [
    "evals/source_code_qa/release_gate.jsonl",
    "evals/source_code_qa/scenario_matrix.jsonl",
]
LLM_SMOKE_CASES = ["evals/source_code_qa/llm_smoke.jsonl"]


def _run_json_command(args: list[str]) -> tuple[dict[str, Any], str, str, int]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT_DIR) if not existing_pythonpath else f"{ROOT_DIR}{os.pathsep}{existing_pythonpath}"
    completed = subprocess.run(args, cwd=ROOT_DIR, env=env, capture_output=True, text=True, check=False)
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
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


def _eval_command(
    *,
    data_root: Path,
    cases: list[str],
    fixture: bool,
    mock_llm: bool,
) -> list[str]:
    args = [sys.executable, "scripts/run_source_code_qa_evals.py", "--json"]
    if fixture:
        args.extend(["--fixture", "--data-root", str(data_root)])
    else:
        args.extend(["--data-root", str(data_root)])
    if mock_llm:
        args.append("--mock-llm")
    for case_path in cases:
        args.extend(["--cases", case_path])
    return args


def _compact_eval_summary(payload: dict[str, Any], *, returncode: int) -> dict[str, Any]:
    return {
        "returncode": returncode,
        "status": payload.get("status"),
        "total": payload.get("total") or 0,
        "failed": payload.get("failed") or 0,
        "failure_buckets": payload.get("failure_buckets") or {},
        "coverage_buckets": payload.get("coverage_buckets") or {},
        "team_buckets": payload.get("team_buckets") or {},
        "segment_buckets": payload.get("segment_buckets") or {},
        "route_buckets": payload.get("route_buckets") or {},
        "answer_mode_buckets": payload.get("answer_mode_buckets") or {},
        "fallback_buckets": payload.get("fallback_buckets") or {},
        "cache_buckets": payload.get("cache_buckets") or {},
        "slow_query_buckets": payload.get("slow_query_buckets") or {},
    }


def run_broad_eval(
    *,
    data_root: Path,
    output_dir: Path,
    broad_cases: list[str] | None = None,
    auto_candidate_limit: int = 120,
    telemetry_limit: int = 1000,
    min_latency_ms: int = 30_000,
    run_auto_eval: bool = True,
    mock_llm: bool = True,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fixture_data_root = output_dir / "broad_fixture_data"
    llm_smoke_data_root = output_dir / "broad_llm_smoke_data"
    shutil.rmtree(fixture_data_root, ignore_errors=True)
    shutil.rmtree(llm_smoke_data_root, ignore_errors=True)

    stable_cases = list(broad_cases or BROAD_QUALITY_CASES)
    stable_payload, stable_stdout, stable_stderr, stable_returncode = _run_json_command(
        _eval_command(data_root=fixture_data_root, cases=stable_cases, fixture=True, mock_llm=mock_llm)
    )
    smoke_payload, smoke_stdout, smoke_stderr, smoke_returncode = _run_json_command(
        _eval_command(data_root=llm_smoke_data_root, cases=LLM_SMOKE_CASES, fixture=True, mock_llm=True)
    )

    auto_candidates, auto_summary = build_auto_eval_candidates(
        data_root,
        limit=max(1, int(auto_candidate_limit or 1)),
        telemetry_limit=max(1, int(telemetry_limit or 1)),
        min_latency_ms=max(0, int(min_latency_ms or 0)),
        runnable_only=True,
    )
    auto_candidates_path = output_dir / f"auto_eval_candidates_{timestamp}.jsonl"
    _write_jsonl(auto_candidates_path, auto_candidates)

    auto_payload: dict[str, Any] = {"status": "skipped", "total": 0, "failed": 0}
    auto_stdout = ""
    auto_stderr = ""
    auto_returncode = 0
    if run_auto_eval and auto_candidates:
        auto_payload, auto_stdout, auto_stderr, auto_returncode = _run_json_command(
            _eval_command(data_root=data_root, cases=[str(auto_candidates_path)], fixture=False, mock_llm=mock_llm)
        )

    stable_ok = stable_returncode == 0 and stable_payload.get("status") == "pass"
    smoke_ok = smoke_returncode == 0 and smoke_payload.get("status") == "pass"
    auto_ok = auto_returncode == 0 and auto_payload.get("status") in {"pass", "skipped"}
    broad_quality_status = "pass" if stable_ok and smoke_ok and auto_ok else "warn"
    report = {
        "status": "ok",
        "broad_quality_status": broad_quality_status,
        "summary": (
            "Broad quality evals passed."
            if broad_quality_status == "pass"
            else "Broad quality evals produced non-blocking warnings."
        ),
        "timestamp": timestamp,
        "mock_llm": bool(mock_llm),
        "cases": stable_cases,
        "stable_eval": _compact_eval_summary(stable_payload, returncode=stable_returncode),
        "llm_smoke": _compact_eval_summary(smoke_payload, returncode=smoke_returncode),
        "auto_candidates": {**auto_summary, "output": str(auto_candidates_path)},
        "auto_eval": _compact_eval_summary(auto_payload, returncode=auto_returncode),
        "raw": {
            "stable_stdout": stable_stdout[-12000:],
            "stable_stderr": stable_stderr[-4000:],
            "llm_smoke_stdout": smoke_stdout[-8000:],
            "llm_smoke_stderr": smoke_stderr[-4000:],
            "auto_stdout": auto_stdout[-12000:],
            "auto_stderr": auto_stderr[-4000:],
        },
    }
    report_path = output_dir / f"source_code_qa_broad_eval_{timestamp}.json"
    _atomic_write_json(report_path, report)
    _atomic_write_json(output_dir / "broad_latest.json", {**report, "report_path": str(report_path)})
    run_root = data_root / "run"
    _atomic_write_json(run_root / "source_code_qa_broad_eval.json", {**report, "report_path": str(report_path)})
    report["report_path"] = str(report_path)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=None, help="Defaults to TEAM_PORTAL_DATA_DIR.")
    parser.add_argument("--output-dir", default=None, help="Defaults to TEAM_PORTAL_DATA_DIR/source_code_qa/eval_runs.")
    parser.add_argument("--cases", action="append", default=None, help="Broad fixture eval case file. Can be passed multiple times.")
    parser.add_argument("--auto-candidate-limit", type=int, default=120)
    parser.add_argument("--telemetry-limit", type=int, default=1000)
    parser.add_argument("--min-latency-ms", type=int, default=30_000)
    parser.add_argument("--skip-auto-eval", action="store_true")
    parser.add_argument("--live-llm", action="store_true")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on any broad-quality warning or failure.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    settings = Settings.from_env()
    data_root = Path(args.data_root).expanduser().resolve() if args.data_root else settings.team_portal_data_dir
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else data_root / "source_code_qa" / "eval_runs"
    report = run_broad_eval(
        data_root=data_root,
        output_dir=output_dir,
        broad_cases=args.cases or BROAD_QUALITY_CASES,
        auto_candidate_limit=max(1, int(args.auto_candidate_limit or 1)),
        telemetry_limit=max(1, int(args.telemetry_limit or 1)),
        min_latency_ms=max(0, int(args.min_latency_ms or 0)),
        run_auto_eval=not bool(args.skip_auto_eval),
        mock_llm=not bool(args.live_llm),
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "Source Code Q&A broad eval: "
            f"{report['broad_quality_status']} stable={report['stable_eval']['status']} "
            f"auto={report['auto_eval']['status']} report={report['report_path']}"
        )
    if args.strict:
        return 0 if report["broad_quality_status"] == "pass" else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
