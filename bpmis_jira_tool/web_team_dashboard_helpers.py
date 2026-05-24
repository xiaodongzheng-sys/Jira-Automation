from __future__ import annotations

from datetime import datetime, timezone
import os
import re
import time
from typing import Any

from bpmis_jira_tool.team_dashboard_config import (
    TEAM_DASHBOARD_PENDING_LIVE_BIZ_PROJECT_STATUSES,
    TEAM_DASHBOARD_UNDER_PRD_BIZ_PROJECT_STATUSES,
    normalize_team_dashboard_emails,
)
from bpmis_jira_tool.web_productization_helpers import jira_browse_base_url


def team_dashboard_jira_max_pages() -> int:
    raw_value = str(os.getenv("TEAM_DASHBOARD_JIRA_MAX_PAGES") or "5").strip()
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 5


def team_dashboard_jira_release_after() -> str:
    configured = str(os.getenv("TEAM_DASHBOARD_JIRA_RELEASE_AFTER") or "").strip()
    if configured:
        return configured
    return time.strftime("%Y-%m-%d", time.localtime())


def team_dashboard_monthly_report_jira_release_after() -> str:
    configured = str(os.getenv("TEAM_DASHBOARD_MONTHLY_REPORT_JIRA_RELEASE_AFTER") or "").strip()
    if configured:
        return configured
    return time.strftime("%Y-%m-%d", time.localtime(time.time() - 60 * 60 * 24 * 45))


def normalize_team_dashboard_task(task: dict[str, Any]) -> dict[str, Any]:
    jira_id = str(task.get("jira_id") or task.get("ticket_key") or "").strip()
    issue_id = str(task.get("issue_id") or "").strip()
    jira_link = str(task.get("jira_link") or task.get("ticket_link") or "").strip()
    if not jira_link and jira_id:
        jira_link = f"{jira_browse_base_url()}{jira_id}"
    raw_prd_links = task.get("prd_links")
    if not raw_prd_links and task.get("prd_link"):
        raw_prd_links = str(task.get("prd_link") or "").splitlines()
    prd_links = team_dashboard_link_items(raw_prd_links)
    return {
        "issue_id": issue_id,
        "jira_id": jira_id or issue_id,
        "jira_link": jira_link,
        "jira_title": str(task.get("jira_title") or "").strip(),
        "pm_email": str(task.get("pm_email") or "").strip().lower(),
        "jira_status": str(task.get("jira_status") or task.get("status") or "").strip(),
        "created_at": str(task.get("created_at") or task.get("created") or "").strip(),
        "release_date": format_team_dashboard_release_date(task.get("release_date") or task.get("release")),
        "version": str(task.get("version") or task.get("fix_version_name") or "").strip(),
        "description": str(task.get("description") or task.get("desc") or task.get("jiraDescription") or "").strip(),
        "prd_links": prd_links,
        "parent_project": normalize_team_dashboard_project(
            task.get("parent_project") if isinstance(task.get("parent_project"), dict) else {}
        ),
    }


def normalize_team_dashboard_project(project: dict[str, Any]) -> dict[str, Any]:
    bpmis_id = str(project.get("bpmis_id") or project.get("issue_id") or "").strip()
    matched_pm_emails = normalize_team_dashboard_emails(project.get("matched_pm_emails") or [])
    normalized: dict[str, Any] = {
        "bpmis_id": bpmis_id,
        "project_name": str(project.get("project_name") or "").strip(),
        "market": str(project.get("market") or "").strip(),
        "priority": str(project.get("priority") or "").strip(),
        "regional_pm_pic": str(project.get("regional_pm_pic") or "").strip(),
        "status": str(project.get("status") or project.get("biz_project_status") or "").strip(),
        "actual_mandays": project.get("actual_mandays", ""),
    }
    if matched_pm_emails:
        normalized["matched_pm_emails"] = matched_pm_emails
    return normalized


def split_team_dashboard_biz_projects_by_status(
    biz_projects: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    under_prd: list[dict[str, Any]] = []
    pending_live: list[dict[str, Any]] = []
    for raw_project in biz_projects:
        project = normalize_team_dashboard_project(raw_project if isinstance(raw_project, dict) else {})
        status_key = str(project.get("status") or "").strip().casefold()
        if status_key in TEAM_DASHBOARD_UNDER_PRD_BIZ_PROJECT_STATUSES:
            under_prd.append(project)
        elif status_key in TEAM_DASHBOARD_PENDING_LIVE_BIZ_PROJECT_STATUSES:
            pending_live.append(project)
    return under_prd, pending_live


def group_team_dashboard_tasks_by_project(
    tasks: list[dict[str, Any]],
    *,
    sort_by_release: bool = False,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for task in tasks:
        project = task.get("parent_project") if isinstance(task.get("parent_project"), dict) else {}
        project = normalize_team_dashboard_project(project)
        key = project.get("bpmis_id") or "unknown"
        if key not in grouped:
            if key == "unknown":
                project = {
                    "bpmis_id": "",
                    "project_name": "BPMIS unavailable",
                    "market": "",
                    "priority": "",
                    "regional_pm_pic": "",
                }
            grouped[key] = {
                **project,
                "jira_tickets": [],
                "task_count": 0,
                "release_date": "-",
                "release_date_sort": "",
            }
        grouped[key]["jira_tickets"].append(task)
        grouped[key]["task_count"] = len(grouped[key]["jira_tickets"])

    projects = list(grouped.values())
    for project in projects:
        project["jira_tickets"].sort(key=team_dashboard_sort_key)
        apply_team_dashboard_project_release_date(project)
    if sort_by_release:
        projects.sort(key=team_dashboard_project_release_sort_key)
    else:
        projects.sort(key=team_dashboard_project_name_sort_key)
    for project in projects:
        project.pop("release_date_sort", None)
    return projects


def merge_team_dashboard_biz_projects(
    projects: list[dict[str, Any]],
    biz_projects: list[dict[str, Any]],
    *,
    sort_by_release: bool = False,
) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {
        str(project.get("bpmis_id") or "").strip(): project
        for project in projects
        if str(project.get("bpmis_id") or "").strip()
    }
    merged = list(projects)
    for raw_project in biz_projects:
        project = normalize_team_dashboard_project(raw_project if isinstance(raw_project, dict) else {})
        bpmis_id = project.get("bpmis_id")
        if not bpmis_id:
            continue
        existing = by_id.get(bpmis_id)
        if existing:
            for key in ("project_name", "market", "priority", "regional_pm_pic", "status", "actual_mandays"):
                if project.get(key) and not existing.get(key):
                    existing[key] = project[key]
            merge_team_dashboard_project_pm_emails(existing, project.get("matched_pm_emails") or [])
            continue
        project.update(
            {
                "jira_tickets": [],
                "task_count": 0,
                "release_date": "-",
                "release_date_sort": "",
            }
        )
        merged.append(project)
        by_id[bpmis_id] = project
    if sort_by_release:
        merged.sort(key=team_dashboard_project_release_sort_key)
    else:
        merged.sort(key=team_dashboard_project_name_sort_key)
    for project in merged:
        project.pop("release_date_sort", None)
    return merged


def merge_team_dashboard_project_pm_emails(project: dict[str, Any], emails: list[str]) -> None:
    existing = normalize_team_dashboard_emails(project.get("matched_pm_emails") or [])
    for email in normalize_team_dashboard_emails(emails):
        if email not in existing:
            existing.append(email)
    if existing:
        project["matched_pm_emails"] = existing


def apply_team_dashboard_key_project_states(projects: list[dict[str, Any]], overrides: dict[str, Any]) -> None:
    for project in projects:
        apply_team_dashboard_key_project_state(project, overrides)


def apply_team_dashboard_key_project_state(project: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    bpmis_id = str(project.get("bpmis_id") or "").strip()
    override = overrides.get(bpmis_id) if bpmis_id and isinstance(overrides, dict) else None
    if isinstance(override, dict) and "is_key_project" in override:
        is_key_project = bool(override.get("is_key_project"))
        project["is_key_project"] = is_key_project
        project["key_project_source"] = "manual_on" if is_key_project else "manual_off"
        project["key_project_override"] = {
            "is_key_project": is_key_project,
            "updated_by": str(override.get("updated_by") or "").strip().lower(),
            "updated_at": str(override.get("updated_at") or "").strip(),
        }
        return project
    priority = str(project.get("priority") or "").strip().casefold()
    is_priority_default = priority in {"sp", "p0"}
    project["is_key_project"] = is_priority_default
    project["key_project_source"] = "priority_default" if is_priority_default else "none"
    project.pop("key_project_override", None)
    return project


def apply_team_dashboard_project_release_date(project: dict[str, Any]) -> None:
    latest = None
    for task in project.get("jira_tickets") or []:
        parsed, _text = parse_team_dashboard_release_date(task.get("release_date"))
        if parsed and (latest is None or parsed > latest):
            latest = parsed
    if latest:
        project["release_date"] = time.strftime("%Y-%m-%d", latest)
        project["release_date_sort"] = time.strftime("%Y-%m-%d", latest)
    else:
        project["release_date"] = "-"
        project["release_date_sort"] = ""


def format_team_dashboard_release_date(value: Any) -> str:
    parsed, text = parse_team_dashboard_release_date(value)
    if parsed:
        return time.strftime("%Y-%m-%d", parsed)
    return text


def parse_team_dashboard_release_date(value: Any) -> tuple[time.struct_time | None, str]:
    text = str(value or "").strip()
    if not text:
        return None, ""
    for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return time.strptime(text[:10], pattern), text
        except ValueError:
            continue
    return None, text


def team_dashboard_project_name_sort_key(project: dict[str, Any]) -> tuple[str, str]:
    return (
        str(project.get("project_name") or "").casefold(),
        str(project.get("bpmis_id") or "").casefold(),
    )


def sort_team_dashboard_under_prd_projects(projects: list[dict[str, Any]]) -> None:
    projects.sort(key=team_dashboard_under_prd_project_sort_key)


def team_dashboard_under_prd_project_sort_key(project: dict[str, Any]) -> tuple[int, str, str, str]:
    release_sort = str(project.get("release_date_sort") or "").strip()
    if not release_sort:
        parsed, _text = parse_team_dashboard_release_date(project.get("release_date"))
        if parsed:
            release_sort = time.strftime("%Y-%m-%d", parsed)
    jira_count = len(project.get("jira_tickets") or [])
    if release_sort:
        bucket = 0
    elif jira_count > 0:
        bucket = 1
    else:
        bucket = 2
    return (
        bucket,
        release_sort,
        str(project.get("project_name") or "").casefold(),
        str(project.get("bpmis_id") or "").casefold(),
    )


def team_dashboard_project_release_sort_key(project: dict[str, Any]) -> tuple[int, str, str, str]:
    release_sort = str(project.get("release_date_sort") or "").strip()
    if not release_sort:
        parsed, _text = parse_team_dashboard_release_date(project.get("release_date"))
        if parsed:
            release_sort = time.strftime("%Y-%m-%d", parsed)
    return (
        0 if release_sort else 1,
        release_sort,
        str(project.get("project_name") or "").casefold(),
        str(project.get("bpmis_id") or "").casefold(),
    )


def team_dashboard_link_items(value: Any) -> list[dict[str, str]]:
    raw_links: list[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                raw_links.append(str(item.get("url") or item.get("label") or "").strip())
            else:
                raw_links.append(str(item or "").strip())
    elif isinstance(value, str):
        raw_links.extend(item.strip() for item in re.split(r"[\n,]+", value) if item.strip())
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for link in raw_links:
        if not link or link in seen:
            continue
        seen.add(link)
        deduped.append({"label": link, "url": link})
    return deduped


def team_dashboard_sort_key(task: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(task.get("pm_email") or "").casefold(),
        str(task.get("version") or "").casefold(),
        str(task.get("jira_id") or "").casefold(),
    )


def team_dashboard_actual_mandays_cache_ttl_seconds() -> int:
    raw_value = str(os.getenv("TEAM_DASHBOARD_ACTUAL_MANDAYS_CACHE_TTL_SECONDS") or "86400").strip()
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 86400


def team_dashboard_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def team_dashboard_parse_timestamp(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def team_dashboard_manday_value(value: Any) -> float | int | str:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return ""
    if normalized.is_integer():
        return int(normalized)
    return normalized


def team_dashboard_actual_mandays_entry_is_fresh(entry: dict[str, Any], *, now: float | None = None) -> bool:
    if not isinstance(entry, dict) or entry.get("value") in {None, ""}:
        return False
    ttl_seconds = team_dashboard_actual_mandays_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return False
    cached_at = team_dashboard_parse_timestamp(entry.get("cached_at"))
    if cached_at is None:
        return False
    return ((now if now is not None else time.time()) - cached_at) <= ttl_seconds


def team_dashboard_project_entries(team_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    for team_payload in team_payloads:
        if not isinstance(team_payload, dict):
            continue
        for section_key in ("under_prd", "pending_live"):
            section_projects = team_payload.get(section_key)
            if isinstance(section_projects, list):
                projects.extend(project for project in section_projects if isinstance(project, dict))
    return projects


def apply_team_dashboard_actual_mandays_cache(config: dict[str, Any], team_payloads: list[dict[str, Any]]) -> list[str]:
    cache = config.get("actual_mandays_cache") if isinstance(config.get("actual_mandays_cache"), dict) else {}
    cached_projects = cache.get("projects") if isinstance(cache.get("projects"), dict) else {}
    now = time.time()
    pending_project_ids: list[str] = []
    for project in team_dashboard_project_entries(team_payloads):
        bpmis_id = str(project.get("bpmis_id") or "").strip()
        if not bpmis_id:
            continue
        entry = cached_projects.get(bpmis_id) if isinstance(cached_projects.get(bpmis_id), dict) else {}
        cached_value = team_dashboard_manday_value(entry.get("value")) if entry else ""
        is_fresh = team_dashboard_actual_mandays_entry_is_fresh(entry, now=now)
        if cached_value != "":
            project["actual_mandays"] = cached_value
            project["actual_mandays_cached_at"] = str(entry.get("cached_at") or "")
            project["actual_mandays_stale"] = not is_fresh
        elif "actual_mandays" not in project:
            project["actual_mandays"] = ""
        if not is_fresh:
            project["actual_mandays_pending"] = True
            if bpmis_id not in pending_project_ids:
                pending_project_ids.append(bpmis_id)
        else:
            project["actual_mandays_pending"] = False
    for team_payload in team_payloads:
        team_payload["actual_mandays_status"] = team_dashboard_actual_mandays_status(team_payload)
    return pending_project_ids


def team_dashboard_actual_mandays_status(team_payload: dict[str, Any]) -> dict[str, Any]:
    projects = team_dashboard_project_entries([team_payload])
    pending_count = sum(1 for project in projects if project.get("actual_mandays_pending"))
    stale_count = sum(1 for project in projects if project.get("actual_mandays_stale"))
    cached_count = sum(1 for project in projects if str(project.get("actual_mandays_cached_at") or "").strip())
    return {
        "pending_count": pending_count,
        "stale_count": stale_count,
        "cached_count": cached_count,
        "project_count": len([project for project in projects if str(project.get("bpmis_id") or "").strip()]),
    }
