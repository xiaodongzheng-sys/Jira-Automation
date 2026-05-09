#!/usr/bin/env python3
"""Run low-risk Team Portal storage maintenance."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings
from scripts.meeting_recorder_compress_audio import compress_old_meeting_audio
from scripts.source_code_qa_rebuild_indexes import cleanup_orphan_indexes


def run_storage_maintenance(
    settings: Settings,
    *,
    apply: bool = False,
    meeting_min_age_days: float = 3.0,
    index_temp_max_age_hours: float = 6.0,
    cleanup_index_backups: bool = False,
    keep_index_backups: int = 2,
) -> dict[str, Any]:
    source_code_qa = cleanup_orphan_indexes(
        settings,
        delete=apply,
        include_stale_temp=True,
        max_temp_age_hours=index_temp_max_age_hours,
        include_old_backups=cleanup_index_backups,
        keep_backups=keep_index_backups,
    )
    meeting_records = compress_old_meeting_audio(
        data_root=settings.team_portal_data_dir,
        ffmpeg_bin=settings.meeting_recorder_ffmpeg_bin,
        min_age_days=meeting_min_age_days,
        dry_run=not apply,
        delete_original=True,
    )
    status = "ok" if source_code_qa.get("status") == "ok" and meeting_records.get("status") == "ok" else "failed"
    return {
        "status": status,
        "mode": "apply" if apply else "dry-run",
        "data_root": str(settings.team_portal_data_dir),
        "source_code_qa": source_code_qa,
        "meeting_records": meeting_records,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Team Portal storage maintenance.")
    parser.add_argument("--data-root", help="Override TEAM_PORTAL_DATA_DIR.")
    parser.add_argument("--apply", action="store_true", help="Delete stale index files and compress old CAF files. Default is dry-run.")
    parser.add_argument("--meeting-min-age-days", type=float, default=3.0, help="Compress Meeting Recorder CAF files older than this many days. Default: 3.")
    parser.add_argument("--index-temp-max-age-hours", type=float, default=6.0, help="Delete Source Code QA index temp files older than this many hours. Default: 6.")
    parser.add_argument("--cleanup-index-backups", action="store_true", help="Delete old indexes.backup.* dirs beyond --keep-index-backups.")
    parser.add_argument("--keep-index-backups", type=int, default=2, help="Number of newest Source Code QA index backup dirs to keep.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()

    settings = Settings.from_env()
    if args.data_root:
        settings = replace(settings, team_portal_data_dir=Path(args.data_root).expanduser().resolve())
    result = run_storage_maintenance(
        settings,
        apply=args.apply,
        meeting_min_age_days=args.meeting_min_age_days,
        index_temp_max_age_hours=args.index_temp_max_age_hours,
        cleanup_index_backups=args.cleanup_index_backups,
        keep_index_backups=args.keep_index_backups,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Team Portal storage maintenance: {result['status']} mode={result['mode']}")
        source = result["source_code_qa"]
        meeting = result["meeting_records"]
        print(
            "source_code_qa: "
            f"orphans={source['orphan_count']} stale_temp={source['stale_temp_count']} "
            f"bytes_reclaimable={source['orphan_bytes'] + source['stale_temp_bytes'] + source['old_backup_bytes']} "
            f"deleted_bytes={source['deleted_bytes']}"
        )
        print(
            "meeting_records: "
            f"candidates={meeting['candidate_count']} candidate_bytes={meeting['candidate_bytes']} "
            f"compressed={meeting['compressed_count']} saved_bytes={meeting['saved_bytes']}"
        )
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
