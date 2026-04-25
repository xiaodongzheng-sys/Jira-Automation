#!/usr/bin/env python3
"""Summarize recent Source Code QA operating signals from local JSONL files."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from collections import Counter
from pathlib import Path
from typing import Any


def _resolve_data_root(raw: str | None) -> Path:
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path.cwd() / "data").resolve()


def _read_tail_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
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
    return rows[-limit:]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _counter_text(counter: Counter[str], *, limit: int = 4) -> str:
    if not counter:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in counter.most_common(limit))


def _p95(values: list[int]) -> int:
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    return int(statistics.quantiles(values, n=20, method="inclusive")[18])


def build_summary(data_root: Path, *, limit: int = 200) -> list[str]:
    source_root = data_root / "source_code_qa"
    telemetry_rows = _read_tail_jsonl(source_root / "telemetry.jsonl", limit)
    feedback_rows = _read_tail_jsonl(source_root / "feedback.jsonl", limit)
    review_rows = _read_tail_jsonl(source_root / "review_queue.jsonl", limit)
    eval_status = _read_json(data_root / "run" / "source_code_qa_eval_status.json")

    lines: list[str] = []
    lines.append(f"data_root={data_root}")

    if telemetry_rows:
        status_counts = Counter(str(row.get("status") or "unknown") for row in telemetry_rows)
        route_counts = Counter(str((row.get("llm_route") or {}).get("mode") or row.get("answer_mode") or "unknown") for row in telemetry_rows)
        policy_counts = Counter(str((row.get("answer_contract") or {}).get("status") or "unknown") for row in telemetry_rows)
        latencies = [
            int(row.get("latency_ms") or row.get("total_latency_ms") or 0)
            for row in telemetry_rows
            if isinstance(row.get("latency_ms") or row.get("total_latency_ms") or 0, (int, float))
        ]
        no_match = status_counts.get("no_match", 0)
        stale = sum(1 for row in telemetry_rows if str((row.get("index_freshness") or {}).get("status") or "").startswith("stale"))
        newest = telemetry_rows[-1].get("created_at") or telemetry_rows[-1].get("timestamp") or "unknown"
        lines.append(f"telemetry_window={len(telemetry_rows)} newest={newest}")
        lines.append(f"query_status={_counter_text(status_counts)}")
        lines.append(f"routes={_counter_text(route_counts)}")
        lines.append(f"answer_contract={_counter_text(policy_counts)}")
        lines.append(f"latency_ms_p50={int(statistics.median(latencies)) if latencies else 0} p95={_p95(latencies)}")
        lines.append(f"no_match_rate={no_match}/{len(telemetry_rows)} stale_index_hits={stale}")
    else:
        lines.append("telemetry_window=0")

    if feedback_rows:
        feedback_counts = Counter(str(row.get("rating") or "unknown") for row in feedback_rows)
        newest_feedback = feedback_rows[-1].get("created_at") or "unknown"
        lines.append(f"feedback_window={len(feedback_rows)} newest={newest_feedback} ratings={_counter_text(feedback_counts)}")
    else:
        lines.append("feedback_window=0")

    if review_rows:
        priorities = Counter(str(row.get("priority") or "unknown") for row in review_rows)
        lines.append(f"review_queue={len(review_rows)} priorities={_counter_text(priorities)}")
    else:
        lines.append("review_queue=0")

    state = eval_status.get("state") or "missing"
    updated_unix = eval_status.get("updated_unix")
    if isinstance(updated_unix, int):
        age = int(time.time()) - updated_unix
        lines.append(f"latest_eval_state={state} age_seconds={age}")
    else:
        lines.append(f"latest_eval_state={state}")
    if eval_status.get("failed_cases"):
        lines.append(f"latest_eval_failed_cases={eval_status.get('failed_cases')}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=os.environ.get("TEAM_PORTAL_DATA_DIR"))
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()

    data_root = _resolve_data_root(args.data_root)
    for line in build_summary(data_root, limit=max(1, args.limit)):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
