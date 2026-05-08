#!/usr/bin/env python3
"""Warm Source Code QA answer cache with full-quality queries."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.source_code_qa_factory import build_source_code_qa_service_from_settings


def _read_jsonl(path: Path, *, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as handle:
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
    return rows[-limit:]


def _load_questions(path: Path) -> list[dict[str, str]]:
    rows = _read_jsonl(path, limit=10_000)
    questions: list[dict[str, str]] = []
    for row in rows:
        question = str(row.get("question") or row.get("question_preview") or "").strip()
        pm_team = str(row.get("pm_team") or row.get("team") or row.get("key") or "").split(":", 1)[0].strip()
        country = str(row.get("country") or "").strip()
        if not country and ":" in str(row.get("key") or ""):
            country = str(row.get("key") or "").split(":", 1)[1].strip()
        if question and pm_team:
            questions.append({"pm_team": pm_team, "country": country or "All", "question": question})
    return questions


def _recent_slow_questions(data_root: Path, *, limit: int, min_latency_ms: int) -> list[dict[str, str]]:
    telemetry_path = data_root / "source_code_qa" / "telemetry.jsonl"
    rows = _read_jsonl(telemetry_path, limit=max(limit * 10, 200))
    selected: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in reversed(rows):
        try:
            latency = int(row.get("latency_ms") or row.get("total_latency_ms") or 0)
        except (TypeError, ValueError):
            latency = 0
        if latency < min_latency_ms or bool(row.get("llm_cached")):
            continue
        key = str(row.get("key") or "")
        pm_team, _, country = key.partition(":")
        question = str(row.get("question_preview") or "").strip()
        item_key = (pm_team, country or "All", question)
        if not pm_team or not question or item_key in seen:
            continue
        seen.add(item_key)
        selected.append({"pm_team": pm_team, "country": country or "All", "question": question})
        if len(selected) >= limit:
            break
    return list(reversed(selected))


def warm_cache(settings: Settings, questions: list[dict[str, str]], *, limit: int, dry_run: bool) -> dict[str, Any]:
    service = build_source_code_qa_service_from_settings(settings)
    results: list[dict[str, Any]] = []
    for item in questions[:limit]:
        pm_team = str(item["pm_team"])
        country = str(item.get("country") or "All")
        question = str(item["question"])
        if dry_run:
            results.append({"pm_team": pm_team, "country": country, "question": question, "status": "dry_run"})
            continue
        result = service.query(
            pm_team=pm_team,
            country=country,
            question=question,
            answer_mode="auto",
            llm_budget_mode="auto",
            query_mode="deep",
            limit=12,
        )
        results.append(
            {
                "pm_team": pm_team,
                "country": country,
                "question": question,
                "status": result.get("status") or "ok",
                "llm_cached": bool(result.get("llm_cached")),
                "trace_id": result.get("trace_id") or "",
            }
        )
    return {"status": "ok", "dry_run": dry_run, "warmed": len(results), "results": results}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", help="Override TEAM_PORTAL_DATA_DIR.")
    parser.add_argument("--questions-jsonl", type=Path, help="JSONL with pm_team, country, and question fields.")
    parser.add_argument("--from-recent-slow", action="store_true", help="Warm recent slow non-cached telemetry questions.")
    parser.add_argument("--min-latency-ms", type=int, default=30_000)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    settings = Settings.from_env()
    if args.data_root:
        settings = replace(settings, team_portal_data_dir=Path(args.data_root).expanduser().resolve())
    data_root = Path(settings.team_portal_data_dir).expanduser().resolve()
    if args.questions_jsonl:
        questions = _load_questions(args.questions_jsonl.expanduser())
    elif args.from_recent_slow:
        questions = _recent_slow_questions(data_root, limit=max(1, args.limit), min_latency_ms=max(0, args.min_latency_ms))
    else:
        raise SystemExit("--questions-jsonl or --from-recent-slow is required")
    result = warm_cache(settings, questions, limit=max(1, args.limit), dry_run=bool(args.dry_run))
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Source Code QA warm answer cache: {result['status']} warmed={result['warmed']} dry_run={result['dry_run']}")
        for item in result["results"]:
            print(f"{item['pm_team']}:{item['country']} {item['status']} cached={item.get('llm_cached', False)} {item['question'][:120]}")
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
