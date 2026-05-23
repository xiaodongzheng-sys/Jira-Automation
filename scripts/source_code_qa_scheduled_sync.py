#!/usr/bin/env python3
"""Run scheduled full Source Code QA repository syncs.

This script is intentionally separate from query-time freshness checks so repo
pull/index work can run on the cadence instead of surprising users during Q&A.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.source_code_qa_factory import build_source_code_qa_service_from_settings, source_code_qa_data_root
from bpmis_jira_tool.source_code_qa_runtime_policy import (
    DEFAULT_AUTO_SYNC_INTERVAL_DAYS,
    DEFAULT_AUTO_SYNC_START_DATE,
)


DEFAULT_TIMEZONE = "Asia/Singapore"


def _parse_date(value: str, fallback: date) -> date:
    value = str(value or "").strip()
    if not value:
        return fallback
    try:
        return date.fromisoformat(value)
    except ValueError:
        return fallback


def _interval_days() -> int:
    try:
        return max(1, int(os.getenv("SOURCE_CODE_QA_AUTO_SYNC_INTERVAL_DAYS", str(DEFAULT_AUTO_SYNC_INTERVAL_DAYS))))
    except ValueError:
        return DEFAULT_AUTO_SYNC_INTERVAL_DAYS


def _start_date() -> date:
    return _parse_date(os.getenv("SOURCE_CODE_QA_AUTO_SYNC_START_DATE", ""), DEFAULT_AUTO_SYNC_START_DATE)


def _latest_scheduled_date(today: date) -> date:
    start = _start_date()
    if today < start:
        return start
    interval = _interval_days()
    periods = (today - start).days // interval
    return start + timedelta(days=periods * interval)


def _run_store_path(data_root: Path) -> Path:
    return data_root / "source_code_qa" / "scheduled_sync_runs.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f".{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, path)


def _mapping_scope(key: str) -> tuple[str, str]:
    pm_team, _, country = str(key or "").partition(":")
    return pm_team.strip(), country.strip() or "All"


def run_scheduled_sync(*, force: bool = False, dry_run: bool = False, now: datetime | None = None) -> dict[str, Any]:
    timezone = ZoneInfo(os.getenv("SOURCE_CODE_QA_SCHEDULED_SYNC_TIMEZONE", DEFAULT_TIMEZONE))
    current = (now or datetime.now(timezone)).astimezone(timezone)
    scheduled_date = _latest_scheduled_date(current.date())
    settings = Settings.from_env()
    data_root = source_code_qa_data_root(settings)
    store_path = _run_store_path(data_root)
    store = _read_json(store_path)
    run_key = scheduled_date.isoformat()
    previous = store.get(run_key) if isinstance(store.get(run_key), dict) else {}

    service = build_source_code_qa_service_from_settings(settings)
    mappings = service.load_config().get("mappings") or {}
    keys = sorted(str(key) for key, repos in mappings.items() if isinstance(repos, list) and repos)

    due = current.date() >= scheduled_date
    already_completed = str(previous.get("status") or "") == "ok"
    should_run = bool(force or (due and not already_completed))
    result: dict[str, Any] = {
        "status": "skipped",
        "timezone": str(timezone),
        "now": current.isoformat(),
        "scheduled_date": scheduled_date.isoformat(),
        "next_scheduled_date": (scheduled_date + timedelta(days=_interval_days())).isoformat(),
        "sync_interval_days": _interval_days(),
        "force": bool(force),
        "dry_run": bool(dry_run),
        "mapping_count": len(keys),
        "mappings": keys,
        "store_path": str(store_path),
        "reason": "",
        "results": [],
    }
    if not due and not force:
        result["reason"] = f"next scheduled repository sync is {scheduled_date.isoformat()}"
        return result
    if already_completed and not force:
        result["reason"] = f"scheduled sync already completed for {scheduled_date.isoformat()}"
        result["previous"] = previous
        return result
    if not keys:
        result["reason"] = "no Source Code Q&A mappings are configured"
        return result
    if dry_run:
        result["status"] = "would_run"
        result["reason"] = "dry run only"
        return result
    if not should_run:
        result["reason"] = "not due"
        return result

    started_at = datetime.now(timezone).isoformat()
    sync_results: list[dict[str, Any]] = []
    overall_status = "ok"
    for key in keys:
        pm_team, country = _mapping_scope(key)
        try:
            sync_payload = service.sync(pm_team=pm_team, country=country)
            sync_results.append(
                {
                    "key": key,
                    "status": sync_payload.get("status") or "ok",
                    "repo_count": len(sync_payload.get("results") or []),
                    "job_id": ((sync_payload.get("job") or {}).get("job_id") or ""),
                }
            )
            if str(sync_payload.get("status") or "ok") != "ok":
                overall_status = "partial"
        except Exception as error:  # pragma: no cover - operational guardrail.
            overall_status = "partial"
            sync_results.append(
                {
                    "key": key,
                    "status": "error",
                    "error_type": type(error).__name__,
                    "error": str(error),
                }
            )

    finished_at = datetime.now(timezone).isoformat()
    record = {
        "status": overall_status,
        "scheduled_date": scheduled_date.isoformat(),
        "started_at": started_at,
        "finished_at": finished_at,
        "mapping_count": len(keys),
        "results": sync_results,
    }
    store[run_key] = record
    _write_json(store_path, store)
    result.update(record)
    result["reason"] = f"full scheduled sync completed for {scheduled_date.isoformat()}"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="run even if this scheduled date was already completed")
    parser.add_argument("--dry-run", action="store_true", help="show what would run without syncing repositories")
    args = parser.parse_args()
    payload = run_scheduled_sync(force=args.force, dry_run=args.dry_run)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload.get("status") in {"ok", "partial", "skipped", "would_run"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
