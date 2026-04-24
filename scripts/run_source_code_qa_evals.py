from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.source_code_qa import ANSWER_MODE, SourceCodeQAService
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS


def _load_cases(path: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise SystemExit(f"{path}:{line_no}: invalid JSON: {error}") from error
        payload.setdefault("id", f"{path.stem}:{line_no}")
        cases.append(payload)
    return cases


def _case_text(payload: dict[str, Any]) -> str:
    parts = [str(payload.get("summary") or ""), str(payload.get("llm_answer") or "")]
    for match in payload.get("matches") or []:
        parts.extend(
            [
                str(match.get("path") or ""),
                str(match.get("reason") or ""),
                str(match.get("snippet") or ""),
            ]
        )
    return "\n".join(parts).lower()


def _evaluate_case(service: SourceCodeQAService, case: dict[str, Any]) -> dict[str, Any]:
    payload = service.query(
        pm_team=str(case.get("pm_team") or ""),
        country=str(case.get("country") or "All"),
        question=str(case.get("question") or ""),
        answer_mode=str(case.get("answer_mode") or ANSWER_MODE),
        llm_budget_mode=str(case.get("llm_budget_mode") or "cheap"),
    )
    matched_paths = {str(match.get("path") or "") for match in payload.get("matches") or []}
    retrievals = {str(match.get("retrieval") or "") for match in payload.get("matches") or []}
    trace_stages = {str(match.get("trace_stage") or "") for match in payload.get("matches") or []}
    text = _case_text(payload)
    failures: list[str] = []

    if case.get("expected_status") and payload.get("status") != case["expected_status"]:
        failures.append(f"status expected {case['expected_status']!r}, got {payload.get('status')!r}")
    elif not case.get("expected_status") and payload.get("status") != "ok":
        failures.append(f"status expected 'ok', got {payload.get('status')!r}")

    for expected_path in case.get("expected_paths") or []:
        if expected_path not in matched_paths:
            failures.append(f"missing expected path {expected_path!r}")
    for term in case.get("required_terms") or []:
        if str(term).lower() not in text:
            failures.append(f"missing required term {term!r}")
    for term in case.get("forbidden_terms") or []:
        if str(term).lower() in text:
            failures.append(f"found forbidden term {term!r}")
    for retrieval in case.get("expected_retrieval") or []:
        if retrieval not in retrievals:
            failures.append(f"missing retrieval type {retrieval!r}")
    for trace_stage in case.get("expected_trace_stage") or []:
        if trace_stage not in trace_stages:
            failures.append(f"missing trace stage {trace_stage!r}")
    expected_quality = case.get("expected_quality_status")
    if expected_quality and (payload.get("answer_quality") or {}).get("status") != expected_quality:
        failures.append(
            f"quality expected {expected_quality!r}, got {(payload.get('answer_quality') or {}).get('status')!r}"
        )

    return {
        "id": case["id"],
        "status": "pass" if not failures else "fail",
        "failures": failures,
        "matched_paths": sorted(matched_paths),
        "retrievals": sorted(retrievals),
        "trace_stages": sorted(trace_stages),
        "answer_quality": payload.get("answer_quality") or {},
        "citations": payload.get("citations") or [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Source Code Q&A golden-answer retrieval evals.")
    parser.add_argument("--cases", default="evals/source_code_qa/golden.jsonl", help="JSONL eval case file.")
    parser.add_argument("--data-root", default=None, help="Override TEAM_PORTAL_DATA_DIR for the indexed repositories.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()

    settings = Settings.from_env()
    service = SourceCodeQAService(
        data_root=Path(args.data_root) if args.data_root else settings.team_portal_data_dir,
        team_profiles=TEAM_PROFILE_DEFAULTS,
        gitlab_token=settings.source_code_qa_gitlab_token,
        gitlab_username=settings.source_code_qa_gitlab_username,
        gemini_api_key=settings.source_code_qa_gemini_api_key,
        gemini_model=settings.source_code_qa_gemini_model,
        gemini_fallback_model=settings.source_code_qa_gemini_fallback_model,
        llm_cache_ttl_seconds=settings.source_code_qa_llm_cache_ttl_seconds,
        git_timeout_seconds=settings.source_code_qa_git_timeout_seconds,
        max_file_bytes=settings.source_code_qa_max_file_bytes,
    )
    cases = _load_cases(Path(args.cases))
    results = [_evaluate_case(service, case) for case in cases]
    failed = [result for result in results if result["status"] != "pass"]
    summary = {"status": "pass" if not failed else "fail", "total": len(results), "failed": len(failed), "results": results}

    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(f"Source Code Q&A evals: {summary['status']} ({len(results) - len(failed)}/{len(results)} passed)")
        for result in results:
            marker = "PASS" if result["status"] == "pass" else "FAIL"
            print(f"{marker} {result['id']}")
            for failure in result["failures"]:
                print(f"  - {failure}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
