from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


REVIEW_FIELDS = {
    "comment",
    "draft_status",
    "observed_paths",
    "review_context",
    "review_note",
    "source_feedback_rating",
    "source_feedback_timestamp",
}
PROMOTABLE_STATUSES = {"approved", "ready_positive_smoke"}
EVAL_ASSERTION_FIELDS = {
    "expected_paths",
    "required_terms",
    "forbidden_terms",
    "expected_retrieval",
    "expected_trace_stage",
    "expected_quality_status",
    "expected_trace_path_terms",
    "min_trace_paths",
    "expected_answer_claim_terms",
    "expected_claim_check_status",
    "expected_answer_contract_status",
    "expected_answer_policy_statuses",
    "expected_evidence_pack_terms",
    "expected_parser_backend",
    "min_tree_sitter_files",
    "expected_repo_graph_edges",
    "expected_symbol_edges",
}


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise SystemExit(f"{path}:{line_no}: invalid JSON: {error}") from error
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def _has_eval_assertion(candidate: dict[str, Any]) -> bool:
    for field in EVAL_ASSERTION_FIELDS:
        value = candidate.get(field)
        if isinstance(value, list) and value:
            return True
        if isinstance(value, dict) and value:
            return True
        if value not in (None, "", [], {}):
            return True
    return False


def _promotion_rejection(candidate: dict[str, Any], *, allow_positive_smoke: bool) -> str:
    status = str(candidate.get("draft_status") or "").strip()
    if status == "ready_positive_smoke" and allow_positive_smoke:
        return ""
    if status != "approved":
        return f"draft_status is {status or 'missing'}, expected approved"
    if not _has_eval_assertion(candidate):
        return "approved candidate has no objective eval assertion"
    return ""


def _eval_case_from_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    promoted = {
        key: value
        for key, value in candidate.items()
        if key not in REVIEW_FIELDS and value not in (None, "", [], {})
    }
    promoted["id"] = str(promoted.get("id") or "").strip()
    promoted["pm_team"] = str(promoted.get("pm_team") or "").strip().upper()
    promoted["country"] = str(promoted.get("country") or "All").strip() or "All"
    promoted["question"] = str(promoted.get("question") or "").strip()
    promoted["answer_mode"] = str(promoted.get("answer_mode") or "retrieval_only").strip() or "retrieval_only"
    return promoted


def promote_candidates(
    candidates: list[dict[str, Any]],
    existing_cases: list[dict[str, Any]],
    *,
    allow_positive_smoke: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    existing_ids = {str(row.get("id") or "").strip() for row in existing_cases if str(row.get("id") or "").strip()}
    existing_questions = {
        (
            str(row.get("pm_team") or "").strip().upper(),
            str(row.get("country") or "All").strip() or "All",
            str(row.get("question") or "").strip(),
        )
        for row in existing_cases
        if str(row.get("question") or "").strip()
    }
    promoted: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []
    skipped_duplicates = 0
    for candidate in candidates:
        rejection = _promotion_rejection(candidate, allow_positive_smoke=allow_positive_smoke)
        candidate_id = str(candidate.get("id") or "").strip()
        if rejection:
            rejected.append({"id": candidate_id, "reason": rejection})
            continue
        eval_case = _eval_case_from_candidate(candidate)
        question_key = (eval_case["pm_team"], eval_case["country"], eval_case["question"])
        if not eval_case["id"] or not eval_case["question"]:
            rejected.append({"id": candidate_id, "reason": "missing id or question"})
            continue
        if eval_case["id"] in existing_ids or question_key in existing_questions:
            skipped_duplicates += 1
            continue
        promoted.append(eval_case)
        existing_ids.add(eval_case["id"])
        existing_questions.add(question_key)
    summary = {
        "status": "ok",
        "input_candidates": len(candidates),
        "existing_cases": len(existing_cases),
        "promoted": len(promoted),
        "rejected": len(rejected),
        "skipped_duplicates": skipped_duplicates,
        "rejections": rejected[:20],
    }
    return [*existing_cases, *promoted], summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote reviewed Source Code Q&A feedback candidates into golden eval cases.")
    parser.add_argument("--input", default="evals/source_code_qa/feedback_candidates.jsonl", help="Reviewed candidate JSONL path.")
    parser.add_argument("--output", default="evals/source_code_qa/golden_real.jsonl", help="Golden eval JSONL output path.")
    parser.add_argument("--allow-positive-smoke", action="store_true", help="Promote ready_positive_smoke candidates without manual approval.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print summary without writing output.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any candidate is rejected.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable summary.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    candidates = load_jsonl(input_path)
    existing = load_jsonl(output_path)
    merged, summary = promote_candidates(candidates, existing, allow_positive_smoke=bool(args.allow_positive_smoke))
    summary["input"] = str(input_path)
    summary["output"] = str(output_path)
    summary["dry_run"] = bool(args.dry_run)
    if not args.dry_run:
        write_jsonl(output_path, merged)
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(
            "Source Code Q&A eval promotion: "
            f"{summary['promoted']} promoted, {summary['rejected']} rejected, "
            f"{summary['skipped_duplicates']} duplicates skipped -> {output_path}"
        )
    return 1 if args.strict and summary["rejected"] else 0


if __name__ == "__main__":
    sys.exit(main())
