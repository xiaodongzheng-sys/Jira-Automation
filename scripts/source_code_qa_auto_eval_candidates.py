#!/usr/bin/env python3
"""Build Source Code QA eval candidates from telemetry and feedback."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
import sys
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings


NEGATIVE_FEEDBACK_RATINGS = {
    "not_useful",
    "wrong_file",
    "too_vague",
    "hallucinated",
    "missing_repo",
    "needs_deeper_trace",
    "incorrect",
    "missing_evidence",
    "stale_code",
}
TELEMETRY_REVIEW_STATUSES = {"no_match", "weak_question", "empty_config", "error"}
ANSWER_CONTRACT_RISK_STATUSES = {"blocked_missing_source", "needs_more_trace", "unreliable_llm_answer"}
IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{2,}")
LOW_VALUE_TERMS = {
    "where",
    "which",
    "what",
    "does",
    "from",
    "with",
    "this",
    "that",
    "code",
    "source",
    "repo",
    "service",
    "class",
    "file",
    "implemented",
}


def _load_jsonl(path: Path, *, limit: int = 1000) -> list[dict[str, Any]]:
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
    return rows[-max(1, int(limit or 1)) :]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha1(json.dumps(parts, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{digest}"


def _split_key(key: str) -> tuple[str, str]:
    pm_team, _, country = str(key or "").partition(":")
    return pm_team.strip().upper(), (country.strip() or "All")


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


def _observed_paths_from_telemetry(record: dict[str, Any]) -> list[str]:
    paths = [str(path).strip() for path in record.get("top_paths") or [] if str(path).strip()]
    if paths:
        return list(dict.fromkeys(paths))[:8]
    return [
        str(citation.get("path") or "").strip()
        for citation in record.get("citations") or []
        if isinstance(citation, dict) and str(citation.get("path") or "").strip()
    ][:8]


def _required_terms(question: str, paths: list[str]) -> list[str]:
    candidates: list[str] = []
    for path in paths[:3]:
        candidates.extend(IDENTIFIER_RE.findall(Path(path).stem))
    candidates.extend(IDENTIFIER_RE.findall(question))
    terms: list[str] = []
    for term in candidates:
        normalized = str(term or "").strip()
        if len(normalized) < 4 or normalized.lower() in LOW_VALUE_TERMS:
            continue
        if normalized not in terms:
            terms.append(normalized)
    return terms[:4]


def _telemetry_category(record: dict[str, Any], *, repeated: bool, min_latency_ms: int) -> str:
    status = str(record.get("status") or "").strip().lower()
    attribution = record.get("slow_query_attribution") if isinstance(record.get("slow_query_attribution"), dict) else {}
    contract_status = str((record.get("answer_contract") or {}).get("status") or "").strip().lower()
    try:
        latency_ms = int(record.get("latency_ms") or 0)
    except (TypeError, ValueError):
        latency_ms = 0
    if status in TELEMETRY_REVIEW_STATUSES:
        return f"telemetry_{status}"
    if bool(record.get("deadline_hit")) or bool(record.get("fallback_used")) or str(attribution.get("reason") or "").startswith("retrieval_exceeded_deadline"):
        return "telemetry_deadline_fallback"
    if str(attribution.get("status") or "").lower() == "slow" or latency_ms >= min_latency_ms:
        return "telemetry_slow_query"
    if contract_status in ANSWER_CONTRACT_RISK_STATUSES:
        return "telemetry_answer_contract_risk"
    if repeated:
        return "telemetry_repeated_question"
    return "telemetry_smoke"


def _candidate_is_runnable(candidate: dict[str, Any]) -> bool:
    return bool(
        candidate.get("expected_paths")
        or candidate.get("required_terms")
        or candidate.get("expected_status")
        or candidate.get("expected_answer_contract_status")
    )


def _feedback_candidates(feedback_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for record in reversed(feedback_records):
        rating = str(record.get("rating") or "").strip().lower()
        question = str(record.get("question") or record.get("question_preview") or "").strip()
        pm_team = str(record.get("pm_team") or "").strip().upper()
        country = str(record.get("country") or "All").strip() or "All"
        if not question or not pm_team:
            continue
        paths = _observed_paths_from_feedback(record)
        base = {
            "pm_team": pm_team,
            "country": country,
            "question": question,
            "answer_mode": "retrieval_only",
            "source": "feedback",
            "source_feedback_rating": rating,
            "source_feedback_timestamp": str(record.get("timestamp") or ""),
            "trace_id": str(record.get("trace_id") or ""),
            "observed_paths": paths,
        }
        digest = str(record.get("question_sha1") or hashlib.sha1(question.encode("utf-8")).hexdigest())[:10]
        if rating == "useful" and paths:
            candidates.append(
                {
                    **base,
                    "id": f"auto-feedback-useful-{digest}",
                    "category": "feedback_useful",
                    "expected_paths": paths[:5],
                    "required_terms": _required_terms(question, paths),
                    "draft_status": "ready_positive_smoke",
                }
            )
        elif rating in NEGATIVE_FEEDBACK_RATINGS:
            candidates.append(
                {
                    **base,
                    "id": f"auto-feedback-review-{digest}",
                    "category": f"feedback_{rating}",
                    "expected_paths": [],
                    "required_terms": [],
                    "draft_status": "needs_human_expected_evidence",
                    "review_note": "Negative feedback is review-only until objective expected evidence is known.",
                }
            )
    return candidates


def _telemetry_candidates(
    telemetry_records: list[dict[str, Any]],
    *,
    min_latency_ms: int,
    repeat_min_count: int,
) -> list[dict[str, Any]]:
    question_counts: dict[tuple[str, str], int] = {}
    for record in telemetry_records:
        key = str(record.get("key") or "")
        question_sha1 = str(record.get("question_sha1") or record.get("question_preview") or "").strip()
        if key and question_sha1:
            bucket = (key, question_sha1)
            question_counts[bucket] = question_counts.get(bucket, 0) + 1

    candidates: list[dict[str, Any]] = []
    for record in reversed(telemetry_records):
        question = str(record.get("question_preview") or "").strip()
        pm_team, country = _split_key(str(record.get("key") or ""))
        if not question or not pm_team:
            continue
        key = str(record.get("key") or "")
        question_sha1 = str(record.get("question_sha1") or question).strip()
        repeated = question_counts.get((key, question_sha1), 0) >= max(2, int(repeat_min_count or 2))
        category = _telemetry_category(record, repeated=repeated, min_latency_ms=min_latency_ms)
        if category == "telemetry_smoke" and not repeated:
            continue
        paths = _observed_paths_from_telemetry(record)
        answer_mode = str(record.get("answer_mode") or record.get("requested_answer_mode") or "retrieval_only").strip() or "retrieval_only"
        candidate: dict[str, Any] = {
            "id": _stable_id("auto-telemetry", category, key, question_sha1),
            "category": category,
            "pm_team": pm_team,
            "country": country,
            "question": question,
            "answer_mode": answer_mode,
            "source": "telemetry",
            "trace_id": str(record.get("trace_id") or ""),
            "observed_paths": paths,
            "expected_paths": paths[:5] if paths and str(record.get("status") or "").lower() == "ok" else [],
            "required_terms": _required_terms(question, paths) if paths and str(record.get("status") or "").lower() == "ok" else [],
            "draft_status": "auto_smoke",
            "telemetry_context": {
                "status": record.get("status"),
                "latency_ms": record.get("latency_ms"),
                "deadline_hit": bool(record.get("deadline_hit")),
                "fallback_used": bool(record.get("fallback_used")),
                "llm_cached": bool(record.get("llm_cached")),
                "answer_contract_status": str((record.get("answer_contract") or {}).get("status") or ""),
                "slow_query_attribution": record.get("slow_query_attribution") or {},
                "repeat_count": question_counts.get((key, question_sha1), 0),
            },
        }
        status = str(record.get("status") or "").strip().lower()
        if status in TELEMETRY_REVIEW_STATUSES and not paths:
            candidate["expected_status"] = status
            candidate["draft_status"] = "auto_observed_status"
        if category == "telemetry_answer_contract_risk" and not paths:
            candidate["expected_answer_contract_status"] = str((record.get("answer_contract") or {}).get("status") or "")
            candidate["draft_status"] = "auto_observed_contract"
        candidates.append(candidate)
    return candidates


def build_auto_eval_candidates(
    data_root: Path,
    *,
    limit: int = 120,
    telemetry_limit: int = 1000,
    min_latency_ms: int = 30_000,
    repeat_min_count: int = 2,
    runnable_only: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_root = data_root / "source_code_qa"
    feedback_records = _load_jsonl(source_root / "feedback.jsonl", limit=telemetry_limit)
    telemetry_records = _load_jsonl(source_root / "telemetry.jsonl", limit=telemetry_limit)
    raw_candidates = [
        *_feedback_candidates(feedback_records),
        *_telemetry_candidates(telemetry_records, min_latency_ms=min_latency_ms, repeat_min_count=repeat_min_count),
    ]

    selected: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for candidate in raw_candidates:
        if runnable_only and not _candidate_is_runnable(candidate):
            continue
        key = (
            str(candidate.get("pm_team") or "").upper(),
            str(candidate.get("country") or "All"),
            str(candidate.get("question") or ""),
            str(candidate.get("answer_mode") or "retrieval_only"),
        )
        if key in seen:
            continue
        seen.add(key)
        selected.append(candidate)
        if len(selected) >= max(1, int(limit or 1)):
            break

    category_counts: dict[str, int] = {}
    runnable_count = 0
    review_only_count = 0
    for candidate in selected:
        category = str(candidate.get("category") or "uncategorized")
        category_counts[category] = category_counts.get(category, 0) + 1
        if _candidate_is_runnable(candidate):
            runnable_count += 1
        else:
            review_only_count += 1
    summary = {
        "status": "ok",
        "feedback_records": len(feedback_records),
        "telemetry_records": len(telemetry_records),
        "candidates": len(selected),
        "runnable_candidates": runnable_count,
        "review_only_candidates": review_only_count,
        "category_counts": category_counts,
        "runnable_only": bool(runnable_only),
    }
    return list(reversed(selected)), summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=None, help="Defaults to TEAM_PORTAL_DATA_DIR.")
    parser.add_argument("--output", default=None, help="Defaults to TEAM_PORTAL_DATA_DIR/source_code_qa/auto_eval_candidates.jsonl.")
    parser.add_argument("--limit", type=int, default=120)
    parser.add_argument("--telemetry-limit", type=int, default=1000)
    parser.add_argument("--min-latency-ms", type=int, default=30_000)
    parser.add_argument("--repeat-min-count", type=int, default=2)
    parser.add_argument("--runnable-only", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    settings = Settings.from_env()
    data_root = Path(args.data_root).expanduser().resolve() if args.data_root else settings.team_portal_data_dir
    output_path = Path(args.output) if args.output else data_root / "source_code_qa" / "auto_eval_candidates.jsonl"
    candidates, summary = build_auto_eval_candidates(
        data_root,
        limit=max(1, int(args.limit or 1)),
        telemetry_limit=max(1, int(args.telemetry_limit or 1)),
        min_latency_ms=max(0, int(args.min_latency_ms or 0)),
        repeat_min_count=max(2, int(args.repeat_min_count or 2)),
        runnable_only=bool(args.runnable_only),
    )
    _write_jsonl(output_path, candidates)
    summary["output"] = str(output_path)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "Source Code Q&A auto eval candidates: "
            f"{summary['candidates']} total, {summary['runnable_candidates']} runnable -> {output_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
