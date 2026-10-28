"""Preview the Pipeline migration; applying it requires a new private backup file."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.team_dashboard_config import TeamDashboardConfigStore
from bpmis_jira_tool.user_config import DB_FILE
from bpmis_jira_tool.team_dashboard_version_plan_store import (
    FirestoreVersionPlanStore,
    build_version_plan_store,
)


def migrate(store, *, apply: bool = False, backup_file: Path | None = None) -> dict:
    if apply and backup_file is None:
        raise ValueError("--backup-file is required with --apply.")
    if isinstance(store, FirestoreVersionPlanStore):
        document = store.document_ref.get()
        source = document.to_dict() if document.exists else {}
        snapshot = store._snapshot_from_doc(source)
        raw_plan = source.get("version_plan") or {}
    else:
        config_store = store.config_store
        with sqlite3.connect(f"file:{config_store.db_path}?mode=ro", uri=True) as connection:
            row = connection.execute(
                "SELECT config_json FROM team_dashboard_configs WHERE config_key = ?", (config_store.CONFIG_KEY,),
            ).fetchone()
        source = json.loads(row[0]) if row else {}
        snapshot = store._snapshot(source)
        raw_plan = source.get("version_plan") or {}
    plan = snapshot.config["version_plan"]
    af = plan["af"]
    old_history = (raw_plan.get("af") or {}).get("pipeline_history") or []
    report = {
        "status": "preview", "backend": snapshot.metadata["backend"],
        "active_pipeline_rows": len(af["pipeline_rows"]),
        "focus_area_relationships": len(af["pipeline_focus_areas"]),
        "newly_archived_duplicates": len(af["pipeline_history"]) - len(old_history),
        "changes_required": raw_plan != plan,
    }
    if apply:
        # Never overwrite a backup. Complete and flush it before any database write.
        fd = os.open(backup_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump({"backend": report["backend"], "revision": snapshot.revision, "source": source}, stream, ensure_ascii=False, indent=2, default=str)
            stream.flush()
            os.fsync(stream.fileno())
        store.save_config(snapshot.config, expected_revision=snapshot.revision)
        report["status"] = "applied"
        report["backup_file"] = str(backup_file)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-file", type=Path)
    args = parser.parse_args()
    if args.apply and args.backup_file is None:
        parser.error("--backup-file is required with --apply")
    settings = Settings.from_env()
    config_store = TeamDashboardConfigStore(settings.team_portal_data_dir / DB_FILE)
    store = build_version_plan_store(settings, config_store)
    print(json.dumps(migrate(store, apply=args.apply, backup_file=args.backup_file), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
