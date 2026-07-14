#!/usr/bin/env python3
"""Migrate Team Dashboard Version Plan from SQLite config to Firestore once."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings  # noqa: E402
from bpmis_jira_tool.team_dashboard_config import TeamDashboardConfigStore  # noqa: E402
from bpmis_jira_tool.team_dashboard_version_plan_store import FirestoreVersionPlanStore  # noqa: E402


def _git_revision() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT_DIR), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return (completed.stdout or "").strip() if completed.returncode == 0 else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=os.environ.get("TEAM_PORTAL_DATA_DIR"))
    parser.add_argument("--project", default=os.environ.get("VERSION_PLAN_FIRESTORE_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT"))
    args = parser.parse_args(argv)

    data_root = Path(args.data_root or ".team-portal").expanduser().resolve()
    db_path = data_root / "team_dashboard.db"
    store = TeamDashboardConfigStore(db_path)
    config = store.load()

    backup_dir = data_root / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / "version_plan_sqlite_backup_live.json"
    if not backup_path.exists():
        backup_path.write_text(json.dumps(config, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    env = dict(os.environ)
    env["VERSION_PLAN_STORE_BACKEND"] = "firestore"
    if args.project:
        env["VERSION_PLAN_FIRESTORE_PROJECT"] = args.project
        env["GOOGLE_CLOUD_PROJECT"] = args.project
    env["TEAM_PORTAL_STAGE"] = "live"
    with _patched_environ(env):
        settings = Settings.from_env()
        firestore_store = FirestoreVersionPlanStore(settings=settings, config_store=store)
        snapshot, migrated = firestore_store.migrate_from_config(
            source_revision=_git_revision(),
            backup_payload=config,
        )

    print(
        json.dumps(
            {
                "status": "ok",
                "migrated": migrated,
                "backup_path": str(backup_path),
                "document_path": snapshot.metadata.get("document_path"),
                "environment": snapshot.metadata.get("environment"),
                "source_hash": snapshot.metadata.get("source_hash"),
                "updated_at_sgt": snapshot.metadata.get("updated_at_sgt"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


class _patched_environ:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.original: dict[str, str | None] = {}

    def __enter__(self) -> None:
        for key, value in self.values.items():
            self.original[key] = os.environ.get(key)
            os.environ[key] = value

    def __exit__(self, exc_type, exc, tb) -> None:
        for key, value in self.original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


if __name__ == "__main__":
    raise SystemExit(main())
