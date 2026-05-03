#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bpmis_jira_tool.web import (
    _build_source_code_qa_effort_business_plan,
    _build_source_code_qa_effort_estimation_rubric,
    _build_source_code_qa_effort_structured_assessment,
    _build_source_code_qa_effort_technical_candidates,
)


DEFAULT_CASES = PROJECT_ROOT / "evals" / "effort_assessment" / "crms_golden.jsonl"


def _load_cases(path: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            cases.append(payload)
    return cases


def _contains_term(candidates: dict[str, Any], term: str) -> bool:
    term_lower = str(term or "").lower()
    if not term_lower:
        return True
    values: list[str] = []
    for key in ("search_terms", "configs_or_tables", "product_terms", "limit_terms", "evidence_hints"):
        values.extend(str(item) for item in (candidates.get(key) or []) if item)
    typed = candidates.get("typed_candidates") if isinstance(candidates.get("typed_candidates"), dict) else {}
    for items in typed.values():
        values.extend(str(item) for item in (items or []) if item)
    return any(term_lower in value.lower() for value in values)


def _surface_has_terms(candidates: dict[str, Any], surface: str) -> bool:
    typed = candidates.get("typed_candidates") if isinstance(candidates.get("typed_candidates"), dict) else {}
    return bool(typed.get(surface))


def _evaluate_case(case: dict[str, Any]) -> dict[str, Any]:
    pm_team = str(case.get("pm_team") or "CRMS")
    country = str(case.get("country") or "SG")
    language = str(case.get("language") or "zh")
    requirement = str(case.get("requirement") or "")
    business_plan = _build_source_code_qa_effort_business_plan(
        pm_team=pm_team,
        country=country,
        language=language,
        requirement=requirement,
    )
    candidates = _build_source_code_qa_effort_technical_candidates(
        pm_team=pm_team,
        country=country,
        business_plan=business_plan,
        requirement=requirement,
    )
    rubric = _build_source_code_qa_effort_estimation_rubric(
        business_plan=business_plan,
        technical_candidates=candidates,
    )
    missing_evidence = ["No confirmed source-code references were found for this mapper-only eval."]
    structured = _build_source_code_qa_effort_structured_assessment(
        result={"matches": []},
        language=language,
        business_plan=business_plan,
        technical_candidates=candidates,
        estimation_rubric=rubric,
        missing_evidence=missing_evidence,
        confidence="low",
    )

    failures: list[str] = []
    if not business_plan.get("business_goals") or not business_plan.get("options"):
        failures.append("business_parse_failed")
    if not structured.get("be_estimate") or not structured.get("fe_estimate"):
        failures.append("person_day_missing")
    if not structured.get("missing_evidence"):
        failures.append("missing_evidence_not_declared")
    if not structured.get("inferred_impact"):
        failures.append("inferred_impact_missing")
    for term in case.get("expected_terms") or []:
        if not _contains_term(candidates, str(term)):
            failures.append(f"missing_term:{term}")
    for surface in case.get("expected_surfaces") or []:
        if not _surface_has_terms(candidates, str(surface)):
            failures.append(f"missing_surface:{surface}")

    return {
        "id": case.get("id") or "",
        "pm_team": pm_team,
        "country": country,
        "status": "pass" if not failures else "fail",
        "failures": failures,
        "business_plan": business_plan,
        "technical_candidates": candidates,
        "structured_assessment": structured,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Effort Assessment mapper/golden-case evals.")
    parser.add_argument("--team", default="CRMS")
    parser.add_argument("--country", default="")
    parser.add_argument("--cases", default=str(DEFAULT_CASES))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    cases = _load_cases(Path(args.cases))
    team = str(args.team or "").upper()
    country = str(args.country or "").upper()
    filtered = [
        case for case in cases
        if (not team or str(case.get("pm_team") or "").upper() == team)
        and (not country or str(case.get("country") or "").upper() == country)
    ]
    results = [_evaluate_case(case) for case in filtered]
    failed = [result for result in results if result["status"] != "pass"]
    buckets: dict[str, int] = {}
    for result in failed:
        for failure in result.get("failures") or []:
            bucket = str(failure).split(":", 1)[0]
            buckets[bucket] = buckets.get(bucket, 0) + 1
    summary = {
        "status": "pass" if not failed else "fail",
        "total": len(results),
        "passed": len(results) - len(failed),
        "failed": len(failed),
        "pass_rate": (len(results) - len(failed)) / len(results) if results else 0,
        "failure_buckets": buckets,
        "results": results,
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"Effort Assessment evals: {summary['status']} ({summary['passed']}/{summary['total']} passed)")
        if buckets:
            print("Failure buckets: " + ", ".join(f"{key}={value}" for key, value in sorted(buckets.items())))
    return 0 if summary["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
