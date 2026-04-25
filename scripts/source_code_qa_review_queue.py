#!/usr/bin/env python3
"""Build a local review queue from Source Code QA feedback and telemetry."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from bpmis_jira_tool.config import Settings


NEGATIVE_RATINGS = {"not_useful", "wrong_file", "too_vague", "hallucinated", "missing_repo", "needs_deeper_trace"}
TELEMETRY_REVIEW_STATUSES = {"no_match", "weak_question", "empty_config", "error"}


def _load_jsonl(path: Path, *, limit: int = 500) -> list[dict[str, Any]]:
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
    return rows[-max(1, limit) :]


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha1(json.dumps(parts, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{digest}"


def _observed_paths_from_feedback(record: dict[str, Any]) -> list[str]:
    replay = record.get("replay_context") if isinstance(record.get("replay_context"), dict) else {}
    matches = replay.get("matches_snapshot") if isinstance(replay.get("matches_snapshot"), list) else []
    paths = [
        str(match.get("path") or "").strip()
        for match in matches
        if isinstance(match, dict) and str(match.get("path") or "").strip()
    ]
    if not paths:
        paths = [str(path).strip() for path in record.get("top_paths") or [] if str(path).strip()]
    return list(dict.fromkeys(paths))[:8]


def build_review_queue(data_root: Path, *, limit: int = 500) -> list[dict[str, Any]]:
    source_root = data_root / "source_code_qa"
    feedback_records = _load_jsonl(source_root / "feedback.jsonl", limit=limit)
    telemetry_records = _load_jsonl(source_root / "telemetry.jsonl", limit=limit)

    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for record in feedback_records:
        rating = str(record.get("rating") or "").strip().lower()
        if rating not in NEGATIVE_RATINGS:
            continue
        trace_id = str(record.get("trace_id") or "").strip()
        key = ("feedback", trace_id or str(record.get("question_sha1") or ""))
        if key in seen:
            continue
        seen.add(key)
        replay = record.get("replay_context") if isinstance(record.get("replay_context"), dict) else {}
        item = {
            "id": _stable_id("feedback", trace_id, record.get("question_sha1"), rating),
            "source": "feedback",
            "priority": "high" if rating in {"wrong_file", "hallucinated", "missing_repo"} else "medium",
            "rating": rating,
            "pm_team": str(record.get("pm_team") or "").strip(),
            "country": str(record.get("country") or "All").strip() or "All",
            "question": str(record.get("question_preview") or "").strip(),
            "trace_id": trace_id,
            "observed_paths": _observed_paths_from_feedback(record),
            "answer_contract_status": str((replay.get("answer_contract") or {}).get("status") or "").strip(),
            "comment": str(record.get("comment") or "").strip(),
            "recommended_action": "Add expected_paths/required_terms, mark approved, then promote into golden_real.jsonl.",
            "draft_eval": {
                "pm_team": str(record.get("pm_team") or "").strip().upper(),
                "country": str(record.get("country") or "All").strip() or "All",
                "question": str(record.get("question_preview") or "").strip(),
                "expected_paths": [],
                "required_terms": [],
                "draft_status": "needs_human_expected_evidence",
            },
        }
        items.append(item)

    for record in telemetry_records:
        status = str(record.get("status") or "").strip().lower()
        contract_status = str((record.get("answer_contract") or {}).get("status") or "").strip().lower()
        stale = str((record.get("index_freshness") or {}).get("status") or "").startswith("stale")
        fallback = bool(record.get("fallback"))
        needs_review = status in TELEMETRY_REVIEW_STATUSES or contract_status in {"blocked_missing_source", "needs_more_trace"} or stale or fallback
        if not needs_review:
            continue
        trace_id = str(record.get("trace_id") or "").strip()
        key = ("telemetry", trace_id or str(record.get("question_sha1") or ""))
        if key in seen:
            continue
        seen.add(key)
        reasons = []
        if status in TELEMETRY_REVIEW_STATUSES:
            reasons.append(f"status={status}")
        if contract_status in {"blocked_missing_source", "needs_more_trace"}:
            reasons.append(f"answer_contract={contract_status}")
        if stale:
            reasons.append("stale_index")
        if fallback:
            reasons.append("llm_fallback")
        items.append(
            {
                "id": _stable_id("telemetry", trace_id, record.get("question_sha1"), reasons),
                "source": "telemetry",
                "priority": "high" if status == "no_match" or contract_status == "blocked_missing_source" else "medium",
                "reason": ", ".join(reasons),
                "pm_team_country": str(record.get("key") or "").strip(),
                "question_preview": str(record.get("question_preview") or "").strip(),
                "trace_id": trace_id,
                "top_paths": [str(path) for path in record.get("top_paths") or []][:8],
                "recommended_action": "Inspect trace/evidence, add a reviewed eval case if the answer should have succeeded.",
            }
        )

    priority_order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda item: (priority_order.get(str(item.get("priority")), 9), str(item.get("source")), str(item.get("id"))))
    return items


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=None, help="Defaults to TEAM_PORTAL_DATA_DIR.")
    parser.add_argument("--output", default=None, help="Defaults to TEAM_PORTAL_DATA_DIR/source_code_qa/review_queue.jsonl.")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    settings = Settings.from_env()
    data_root = Path(args.data_root or os.environ.get("TEAM_PORTAL_DATA_DIR") or settings.team_portal_data_dir).expanduser().resolve()
    output_path = Path(args.output) if args.output else data_root / "source_code_qa" / "review_queue.jsonl"
    rows = build_review_queue(data_root, limit=max(1, int(args.limit or 500)))
    write_jsonl(output_path, rows)
    summary = {
        "status": "ok",
        "review_items": len(rows),
        "high_priority": sum(1 for row in rows if row.get("priority") == "high"),
        "output": str(output_path),
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"Source Code Q&A review queue: {summary['review_items']} items -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
