from __future__ import annotations

import json
from pathlib import Path
import re
import sqlite3
from typing import Any

from bpmis_jira_tool.monthly_report import (
    DEFAULT_MONTHLY_REPORT_TEMPLATE,
    normalize_monthly_report_template,
    normalize_report_intelligence_config,
)
from bpmis_jira_tool.team_dashboard_version_plan import normalize_version_plan_state


TEAM_DASHBOARD_LEGACY_DEFAULT_MEMBER_EMAILS = (
    "huixian.nah@npt.sg",
    "jireh.tanyx@npt.sg",
    "keryin.lim@npt.sg",
    "liye.ng@npt.sg",
    "mingming.yeo@npt.sg",
    "chongzj@npt.sg",
    "sabrina.chan@npt.sg",
    "sophia.wangzj@npt.sg",
    "chang.wang@npt.sg",
    "zoey.luxy@npt.sg",
)
TEAM_DASHBOARD_DEFAULT_MEMBER_EMAILS_BY_TEAM = {
    "AF": (
        "jireh.tanyx@npt.sg",
        "keryin.lim@npt.sg",
        "chongzj@npt.sg",
        "chang.wang@npt.sg",
        "zoey.luxy@npt.sg",
        "xiaodong.zheng@npt.sg",
    ),
    "CRMS": (
        "huixian.nah@npt.sg",
        "liye.ng@npt.sg",
        "mingming.yeo@npt.sg",
        "sophia.wangzj@npt.sg",
    ),
    "GRC": (
        "sabrina.chan@npt.sg",
    ),
}
TEAM_DASHBOARD_TEAMS = {
    "AF": "Anti-fraud",
    "CRMS": "Credit Risk",
    "GRC": "Ops Risk",
}
TEAM_DASHBOARD_UNDER_PRD_STATUSES = {"waiting", "prd in progress", "prd reviewed"}
TEAM_DASHBOARD_EXCLUDED_PENDING_STATUSES = {"icebox", "closed", "done"}
TEAM_DASHBOARD_UNDER_PRD_BIZ_PROJECT_STATUSES = {"pending review", "confirmed"}
TEAM_DASHBOARD_PENDING_LIVE_BIZ_PROJECT_STATUSES = {"developing", "testing", "uat"}
TEAM_DASHBOARD_TASK_CACHE_VERSION = 3

def normalize_team_dashboard_emails(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_values = re.split(r"[\s,;]+", value)
    elif isinstance(value, list):
        raw_values = value
    else:
        raw_values = []
    normalized: list[str] = []
    for raw_email in raw_values:
        email = str(raw_email or "").strip().lower()
        if email and email not in normalized:
            normalized.append(email)
    return normalized


_normalize_team_dashboard_emails = normalize_team_dashboard_emails


class TeamDashboardConfigStore:
    CONFIG_KEY = "team_dashboard"

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._ensure_db()

    def load(self) -> dict[str, Any]:
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT config_json FROM team_dashboard_configs WHERE config_key = ?",
                (self.CONFIG_KEY,),
            ).fetchone()
        if not row:
            return self.default_config()
        try:
            payload = json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            return self.default_config()
        return self.normalize_config(payload)

    def save(self, config: dict[str, Any]) -> dict[str, Any]:
        normalized = self.normalize_config(config)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO team_dashboard_configs (config_key, config_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(config_key) DO UPDATE SET
                    config_json = excluded.config_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (self.CONFIG_KEY, json.dumps(normalized, ensure_ascii=False)),
            )
            connection.commit()
        return normalized

    def default_config(self) -> dict[str, Any]:
        return {
            "teams": {
                team_key: {
                    "label": label,
                    "member_emails": list(TEAM_DASHBOARD_DEFAULT_MEMBER_EMAILS_BY_TEAM.get(team_key, ())),
                }
                for team_key, label in TEAM_DASHBOARD_TEAMS.items()
            },
            "key_project_overrides": {},
            "monthly_report_template": DEFAULT_MONTHLY_REPORT_TEMPLATE,
            "report_intelligence_config": normalize_report_intelligence_config({}),
            "actual_mandays_cache": self._normalize_actual_mandays_cache({}),
            "version_plan": normalize_version_plan_state({}),
        }

    def normalize_config(self, config: dict[str, Any]) -> dict[str, Any]:
        raw_teams = config.get("teams") if isinstance(config, dict) else {}
        raw_teams = raw_teams if isinstance(raw_teams, dict) else {}
        raw_key_project_overrides = config.get("key_project_overrides") if isinstance(config, dict) else {}
        raw_key_project_overrides = raw_key_project_overrides if isinstance(raw_key_project_overrides, dict) else {}
        raw_monthly_report_template = config.get("monthly_report_template") if isinstance(config, dict) else ""
        raw_report_intelligence_config = config.get("report_intelligence_config") if isinstance(config, dict) else {}
        raw_task_cache = config.get("task_cache") if isinstance(config, dict) else {}
        raw_task_cache = raw_task_cache if isinstance(raw_task_cache, dict) else {}
        raw_actual_mandays_cache = config.get("actual_mandays_cache") if isinstance(config, dict) else {}
        raw_actual_mandays_cache = raw_actual_mandays_cache if isinstance(raw_actual_mandays_cache, dict) else {}
        raw_version_plan = config.get("version_plan") if isinstance(config, dict) else {}
        default = self.default_config()
        normalized_teams: dict[str, dict[str, Any]] = {}
        for team_key, label in TEAM_DASHBOARD_TEAMS.items():
            raw_team = raw_teams.get(team_key) if isinstance(raw_teams.get(team_key), dict) else {}
            raw_emails = raw_team.get("member_emails") if isinstance(raw_team, dict) else None
            if raw_emails is None:
                raw_emails = default["teams"][team_key]["member_emails"]
            normalized_emails = _normalize_team_dashboard_emails(raw_emails)
            if set(normalized_emails) == set(TEAM_DASHBOARD_LEGACY_DEFAULT_MEMBER_EMAILS):
                normalized_emails = list(default["teams"][team_key]["member_emails"])
            normalized_teams[team_key] = {
                "label": label,
                "member_emails": normalized_emails,
            }
        normalized_key_project_overrides: dict[str, dict[str, Any]] = {}
        for raw_bpmis_id, raw_override in raw_key_project_overrides.items():
            bpmis_id = str(raw_bpmis_id or "").strip()
            if not bpmis_id or not isinstance(raw_override, dict) or "is_key_project" not in raw_override:
                continue
            normalized_key_project_overrides[bpmis_id] = {
                "is_key_project": bool(raw_override.get("is_key_project")),
                "updated_by": str(raw_override.get("updated_by") or "").strip().lower(),
                "updated_at": str(raw_override.get("updated_at") or "").strip(),
            }
        return {
            "teams": normalized_teams,
            "key_project_overrides": normalized_key_project_overrides,
            "monthly_report_template": normalize_monthly_report_template(raw_monthly_report_template),
            "report_intelligence_config": normalize_report_intelligence_config(raw_report_intelligence_config),
            "task_cache": self._normalize_task_cache(raw_task_cache),
            "actual_mandays_cache": self._normalize_actual_mandays_cache(raw_actual_mandays_cache),
            "version_plan": normalize_version_plan_state(raw_version_plan),
        }

    def _normalize_task_cache(self, task_cache: dict[str, Any]) -> dict[str, Any]:
        version = int(task_cache.get("version") or 1)
        if version != TEAM_DASHBOARD_TASK_CACHE_VERSION:
            return {
                "version": TEAM_DASHBOARD_TASK_CACHE_VERSION,
                "updated_at": "",
                "teams": {},
            }
        raw_teams = task_cache.get("teams") if isinstance(task_cache.get("teams"), dict) else {}
        teams: dict[str, dict[str, Any]] = {}
        for team_key in TEAM_DASHBOARD_TEAMS:
            raw_team = raw_teams.get(team_key)
            if not isinstance(raw_team, dict):
                continue
            teams[team_key] = {
                **raw_team,
                "team_key": team_key,
                "email_signature": str(raw_team.get("email_signature") or "").strip(),
                "cached_at": str(raw_team.get("cached_at") or "").strip(),
                "loaded": bool(raw_team.get("loaded", True)),
                "loading": False,
                "error": "",
                "progress_text": "",
                "under_prd": raw_team.get("under_prd") if isinstance(raw_team.get("under_prd"), list) else [],
                "pending_live": raw_team.get("pending_live") if isinstance(raw_team.get("pending_live"), list) else [],
            }
        return {
            "version": TEAM_DASHBOARD_TASK_CACHE_VERSION,
            "updated_at": str(task_cache.get("updated_at") or "").strip(),
            "teams": teams,
        }

    def _normalize_actual_mandays_cache(self, actual_mandays_cache: dict[str, Any]) -> dict[str, Any]:
        raw_projects = actual_mandays_cache.get("projects") if isinstance(actual_mandays_cache.get("projects"), dict) else {}
        projects: dict[str, dict[str, Any]] = {}
        for raw_project_id, raw_entry in raw_projects.items():
            project_id = str(raw_project_id or "").strip()
            if not project_id or not isinstance(raw_entry, dict):
                continue
            value = raw_entry.get("value")
            try:
                normalized_value: float | int | str = float(value)
            except (TypeError, ValueError):
                normalized_value = ""
            else:
                if float(normalized_value).is_integer():
                    normalized_value = int(normalized_value)
            projects[project_id] = {
                "value": normalized_value,
                "cached_at": str(raw_entry.get("cached_at") or "").strip(),
            }
        return {
            "version": 1,
            "updated_at": str(actual_mandays_cache.get("updated_at") or "").strip(),
            "projects": projects,
        }

    def _ensure_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS team_dashboard_configs (
                    config_key TEXT PRIMARY KEY,
                    config_json TEXT NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.commit()
