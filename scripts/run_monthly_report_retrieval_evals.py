#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bpmis_jira_tool.monthly_report import (  # noqa: E402
    build_monthly_highlight_deep_evidence,
    match_monthly_report_highlight_topics,
    monthly_report_business_glossary_summary,
    resolve_monthly_report_period_from_user_range,
)


CASES_PATH = REPO_ROOT / "evals" / "monthly_report" / "retrieval_cases.json"


def _case_text(result: dict) -> str:
    parts = []
    parts.extend(str(item) for item in result.get("seatalk_evidence") or [])
    parts.extend(str(item) for item in result.get("gmail_evidence") or [])
    parts.extend(str(item.get("text") or "") for item in result.get("google_sheet_evidence") or [] if isinstance(item, dict))
    return "\n".join(parts)


def main() -> int:
    cases = json.loads(CASES_PATH.read_text(encoding="utf-8"))
    summary = monthly_report_business_glossary_summary()
    if summary.get("entry_count", 0) < 3:
        raise AssertionError(f"Monthly Report glossary looks empty: {summary}")
    period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
    failures: list[str] = []
    for case in cases:
        topic = str(case["topic"])
        key_projects = case.get("key_projects") or []
        matches = match_monthly_report_highlight_topics([topic], key_projects)
        result = build_monthly_highlight_deep_evidence(
            highlight_topics=[topic],
            key_projects=key_projects,
            topic_project_matches=matches or [{"topic": topic, "project_ids": []}],
            seatalk_history_text=str(case.get("seatalk_history_text") or ""),
            topic_gmail_evidence=[],
            monthly_requirements_targets=[],
            prd_scope_summaries=[],
            report_period=period,
            highlight_topic_sources={topic: case.get("selected_sources") or ["seatalk"]},
        )[0]
        text = _case_text(result)
        for expected in case.get("expected_substrings") or []:
            if expected not in text:
                failures.append(f"{case['id']}: missing expected substring {expected!r}")
        for forbidden in case.get("forbidden_substrings") or []:
            if forbidden in text:
                failures.append(f"{case['id']}: included forbidden substring {forbidden!r}")
        expected_project_ids = case.get("expected_project_ids")
        if expected_project_ids is not None and result.get("matched_project_ids") != expected_project_ids:
            failures.append(
                f"{case['id']}: project ids {result.get('matched_project_ids')!r} != {expected_project_ids!r}"
            )
        debug = result.get("evidence_debug") or {}
        if not isinstance(debug, dict) or not debug.get("source_counts"):
            failures.append(f"{case['id']}: missing evidence debug payload")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"monthly_report_retrieval_evals: {len(cases)} cases passed; glossary entries={summary['entry_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
