#!/usr/bin/env python3
"""Summarize recent Live deployment timing records."""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path
from statistics import mean


def _default_timing_file() -> Path:
    explicit = os.environ.get("TEAM_DEPLOY_TIMING_FILE")
    if explicit:
        return Path(explicit).expanduser()
    data_dir = os.environ.get("TEAM_DEPLOY_TIMING_DATA_DIR") or os.environ.get("TEAM_PORTAL_DATA_DIR")
    if data_dir:
        return Path(data_dir).expanduser() / "run" / "deploy_timings.jsonl"
    return Path(".team-portal") / "run" / "deploy_timings.jsonl"


def _load_records(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    records: list[dict[str, object]] = []
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
                records.append(payload)
    return records


def _fmt_seconds(value: object) -> str:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return "-"
    return f"{seconds}s"


def _print_recent(records: list[dict[str, object]], limit: int) -> None:
    print(f"Recent deploy timings ({min(limit, len(records))}/{len(records)} records):")
    print("script                       phase                 status  duration  details")
    print("---------------------------  --------------------  ------  --------  -------")
    for record in records[-limit:]:
        script = str(record.get("script") or "-")[:27]
        phase = str(record.get("phase") or "-")[:20]
        status = str(record.get("status") if record.get("status") is not None else "-")
        duration = _fmt_seconds(record.get("duration_seconds"))
        details = str(record.get("details") or "")
        print(f"{script:<27}  {phase:<20}  {status:<6}  {duration:<8}  {details}")


def _print_summary(records: list[dict[str, object]], limit: int) -> None:
    grouped: dict[tuple[str, str], list[int]] = defaultdict(list)
    for record in records[-limit:]:
        try:
            duration = int(record.get("duration_seconds") or 0)
        except (TypeError, ValueError):
            continue
        grouped[(str(record.get("script") or "-"), str(record.get("phase") or "-"))].append(duration)

    if not grouped:
        return

    print()
    print(f"Averages by script/phase over latest {min(limit, len(records))} records:")
    print("script                       phase                 runs  avg")
    print("---------------------------  --------------------  ----  ----")
    for (script, phase), durations in sorted(grouped.items()):
        print(f"{script[:27]:<27}  {phase[:20]:<20}  {len(durations):<4}  {int(mean(durations))}s")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize recent Live deployment timing records.")
    parser.add_argument("--file", type=Path, default=_default_timing_file(), help="Path to deploy_timings.jsonl.")
    parser.add_argument("--limit", type=int, default=20, help="Number of recent records to show.")
    parser.add_argument("--no-summary", action="store_true", help="Only print raw recent records.")
    args = parser.parse_args()

    records = _load_records(args.file.expanduser())
    if not records:
        print(f"No deploy timing records found at {args.file}")
        return 0

    limit = max(1, args.limit)
    _print_recent(records, limit)
    if not args.no_summary:
        _print_summary(records, limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
