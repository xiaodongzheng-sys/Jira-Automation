#!/usr/bin/env python3
"""Force a Source Code Q&A repository sync and current index rebuild."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.source_code_qa import CODE_INDEX_VERSION
from bpmis_jira_tool.source_code_qa_factory import build_source_code_qa_service_from_settings, source_code_qa_data_root


def _backup_indexes(source_root: Path) -> Path | None:
    index_root = source_root / "indexes"
    if not index_root.exists():
        return None
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_root = source_root / f"indexes.backup.v{CODE_INDEX_VERSION}.{timestamp}"
    shutil.copytree(index_root, backup_root)
    return backup_root


def _configured_keys(service: Any) -> list[str]:
    config = service.load_config()
    mappings = config.get("mappings") if isinstance(config.get("mappings"), dict) else {}
    return sorted(str(key) for key, repos in mappings.items() if isinstance(repos, list) and repos)


def _verify_health(service: Any) -> tuple[list[str], dict[str, Any]]:
    issues: list[str] = []
    health = service.index_health_payload()
    if health.get("status") != "ready":
        issues.append(f"index_health={health.get('status') or 'unknown'}")
    for key, payload in (health.get("keys") or {}).items():
        for repo in payload.get("repos") or []:
            label = repo.get("display_name") or repo.get("url") or repo.get("path") or "repository"
            index = repo.get("index") or {}
            if index.get("state") != "ready":
                issues.append(f"{key}:{label}: state={index.get('state') or 'unknown'}")
                continue
            try:
                index_version = int(index.get("index_version") or 0)
            except (TypeError, ValueError):
                index_version = 0
            if index_version != CODE_INDEX_VERSION:
                issues.append(f"{key}:{label}: index_version={index.get('index_version') or 'unknown'} expected={CODE_INDEX_VERSION}")
    return issues, health


def rebuild(settings: Settings, *, backup: bool = True) -> dict[str, Any]:
    data_root = source_code_qa_data_root(settings)
    source_root = data_root / "source_code_qa"
    service = build_source_code_qa_service_from_settings(settings)
    backup_path = _backup_indexes(source_root) if backup else None
    keys = _configured_keys(service)
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for key in keys:
        pm_team, _, country = key.partition(":")
        try:
            result = service.sync(pm_team=pm_team, country=country or "All")
            results.append({"key": key, "status": result.get("status"), "repo_count": len(result.get("results") or [])})
            if result.get("status") not in {"ok", "empty_config"}:
                errors.append(f"{key}: sync status={result.get('status') or 'unknown'}")
        except Exception as error:  # noqa: BLE001 - ops command should report the failed repo key.
            results.append({"key": key, "status": "error", "error": str(error)})
            errors.append(f"{key}: {type(error).__name__}: {error}")
    health_errors, health = _verify_health(service)
    errors.extend(health_errors)
    return {
        "status": "ok" if not errors else "failed",
        "data_root": str(data_root),
        "index_version": CODE_INDEX_VERSION,
        "backup_path": str(backup_path) if backup_path else "",
        "synced_keys": results,
        "health": health,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Force Source Code Q&A active repo sync and rebuild indexes.")
    parser.add_argument("--data-root", help="Override TEAM_PORTAL_DATA_DIR for this rebuild.")
    parser.add_argument("--no-backup", action="store_true", help="Skip copying the current source_code_qa/indexes directory first.")
    parser.add_argument("--json", action="store_true", help="Print the full machine-readable result.")
    args = parser.parse_args()

    settings = Settings.from_env()
    if args.data_root:
        settings = replace(settings, team_portal_data_dir=Path(args.data_root).expanduser().resolve())
    result = rebuild(settings, backup=not args.no_backup)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Source Code Q&A rebuild: {result['status']} index_version=v{result['index_version']}")
        print(f"data_root={result['data_root']}")
        print(f"backup_path={result['backup_path'] or 'none'}")
        for item in result["synced_keys"]:
            detail = f" repos={item.get('repo_count', 0)}" if item.get("status") != "error" else f" error={item.get('error')}"
            print(f"{item['key']}: {item['status']}{detail}")
        for error in result["errors"]:
            print(f"ERROR: {error}", file=sys.stderr)
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
