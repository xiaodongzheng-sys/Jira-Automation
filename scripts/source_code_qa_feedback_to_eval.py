from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

from bpmis_jira_tool.config import Settings


NEGATIVE_FEEDBACK_RATINGS = {"not_useful", "wrong_file", "too_vague", "hallucinated", "missing_repo", "needs_deeper_trace"}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise SystemExit(f"{path}:{line_no}: invalid JSON: {error}") from error
        if isinstance(payload, dict):
            records.append(payload)
    return records


def build_eval_candidates(records: list[dict[str, Any]], *, include_useful: bool = False) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_questions: set[tuple[str, str, str, str]] = set()
    for record in records:
        rating = str(record.get("rating") or "").strip().lower()
        if rating == "useful" and not include_useful:
            continue
        if rating not in NEGATIVE_FEEDBACK_RATINGS and rating != "useful":
            continue
        question = str(record.get("question") or record.get("question_preview") or "").strip()
        if not question:
            continue
        pm_team = str(record.get("pm_team") or "").strip().upper()
        country = str(record.get("country") or "All").strip() or "All"
        key = (pm_team, country, question, rating)
        if key in seen_questions:
            continue
        seen_questions.add(key)
        digest = str(record.get("question_sha1") or hashlib.sha1(question.encode("utf-8")).hexdigest())[:10]
        top_paths = [str(path) for path in record.get("top_paths") or [] if str(path).strip()]
        candidates.append(
            {
                "id": f"feedback-{rating}-{digest}",
                "pm_team": pm_team,
                "country": country,
                "question": question,
                "answer_mode": "retrieval_only",
                "expected_paths": top_paths[:5],
                "required_terms": [],
                "source_feedback_rating": rating,
                "comment": str(record.get("comment") or "").strip()[:500],
            }
        )
    return candidates


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert Source Code Q&A user feedback into draft eval cases.")
    parser.add_argument("--feedback", default=None, help="Feedback JSONL path. Defaults to TEAM_PORTAL_DATA_DIR/source_code_qa/feedback.jsonl.")
    parser.add_argument("--output", default="evals/source_code_qa/feedback_candidates.jsonl", help="Output JSONL path.")
    parser.add_argument("--include-useful", action="store_true", help="Also include positive feedback as smoke-test candidates.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable summary.")
    args = parser.parse_args()

    settings = Settings.from_env()
    feedback_path = Path(args.feedback) if args.feedback else settings.team_portal_data_dir / "source_code_qa" / "feedback.jsonl"
    output_path = Path(args.output)
    records = _load_jsonl(feedback_path)
    candidates = build_eval_candidates(records, include_useful=args.include_useful)
    write_jsonl(output_path, candidates)
    summary = {"status": "ok", "feedback_records": len(records), "candidates": len(candidates), "output": str(output_path)}
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(f"Source Code Q&A feedback candidates: {len(candidates)} written to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
