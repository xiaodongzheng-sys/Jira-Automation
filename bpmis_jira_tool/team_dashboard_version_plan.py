from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta
import re
import uuid
from typing import Any
from zoneinfo import ZoneInfo


VERSION_PLAN_PRIORITY_ORDER = ("SP", "P0", "P1", "P2", "P3")
VERSION_PLAN_PM_OPTIONS = ("Wang Chang", "Zoey", "Jireh", "Ker Yin", "Rene", "Jun Wei", "TBC")
VERSION_PLAN_PM_ALIASES = {
    "chang.wang@npt.sg": "Wang Chang",
    "wang chang": "Wang Chang",
    "zoey.luxy@npt.sg": "Zoey",
    "zoey": "Zoey",
    "jireh.tanyx@npt.sg": "Jireh",
    "jireh": "Jireh",
    "keryin.lim@npt.sg": "Ker Yin",
    "ker yin": "Ker Yin",
    "keryin": "Ker Yin",
    "chongzj@npt.sg": "Rene",
    "rene": "Rene",
    "jun wei": "Jun Wei",
    "tbc": "TBC",
}
VERSION_PLAN_TIMEZONE = ZoneInfo("Asia/Singapore")
VERSION_PLAN_SYNC_OPERATION = "af_version_plan"


PIPELINE_SEED_ROWS: tuple[tuple[str, str, str], ...] = (
    ("[ID] Corporate Internet Banking Phase 2", "SP", "Wang Chang"),
    ("[SG] [ID] Child Account", "SP", "Wang Chang"),
    ("[SG] Scam cognitive break questions", "P0", "Zoey"),
    ("[SG] Money Lock - Fixed Deposit", "P0", "Jireh"),
    ("[PH] New Channeling Loan HappyCash/Akulaku", "P1", "Ker Yin"),
    ("[PH] Card N0 Unblock", "P1", "Ker Yin"),
    ("[ID] Foreigner Onboarding", "P1", "Rene"),
    ("[ID] Anti Fraud Enhancement to Support In App Appeal Journey", "P1", "Rene"),
    ("[PH] Change of Authentication Outcome for Login Not Soft Token Activated Device Scenarios", "P2", "Ker Yin"),
    ("[ID] Blacklist/Whitelist Edit Record", "P2", "Rene"),
    ("[PH] Case Review Center 2.0", "P2", "Wang Chang / Ker Yin"),
    ("[ID] Security Enhancement - TSS Fields", "P2", "Rene"),
    ("[ID][PH] FV sampling configuration", "P2", "Jun Wei"),
    ("[SG] MCC Temp Credit Limit Increase", "P2", "TBC"),
    ("[SG] SME RCF Limit Increase", "P2", "TBC"),
    ("[SG] CS block and CS blacklist query page", "P2", "Jireh"),
    ("[SG] Reverse Authentication - Notifications to Customers Enhancements", "P2", "Zoey"),
    ("[ID] Fraud Multi-Blacklist Enhancements v1.0", "P2", "Wang Chang"),
    ("[PH] Malware Detection and Screen Sharing / Remote Screen Control", "P2", "Ker Yin"),
    ("[PH] Viber as Primary OTP Channel", "P2", "Wang Chang & Ker Yin"),
    ("[ID] Challenge 3 for Login Scenario", "P2", "Rene"),
    ("[ID] Greylist Operational Enhancement", "P2", "Rene"),
    ("[SG] Prelogin Mobile Update", "P2", "Jireh"),
    ("[SG] Hold and release", "P2", "Zoey"),
    ("[SG] Real time info enquiry", "P2", "Jireh"),
    ("[SG] New card response code for soft declines", "P2", "TBC"),
    ("[SG] Sub expression in AF rule configuration", "P2", "TBC"),
    ("[SG] Enhancements on 3DS binding check", "P2", "TBC"),
    ("[SG] AF enhancement on Function and Corporate Tagging", "P2", "TBC"),
    ("[Reg] Productised enhancements to NLA FE and Data - 3", "P3", "Zoey"),
    ("[SG] Multicurrency Account - Money Lock", "P3", "TBC"),
    ("[SG] Request Center", "P3", "TBC"),
    ("[SG] Pop up and 2 way protection", "P3", "TBC"),
    ("[ID] Operations & Data structure enhancements related to Fraud Case Review Center (CRC) Part 2", "P3", "TBC"),
    ("[All] Migration to APC", "P3", "TBC"),
    ("[SG] Standalone Cash Loan", "SP", "Zoey"),
    ("[SG] Corporate Internet Banking", "SP", "Wang Chang"),
    ("[ID] PaaS - Add originatorInfos in Payin Service", "P0", "Rene"),
    ("[PH] [AFASA] FMS Initiated Trigger and Holding for Incoming Transactions - Phase 2", "P0", "Wang Chang / Ker Yin"),
    ("[ID] Kredivo Channeling Loan", "P1", "Rene"),
    ("[ID] SeaBank Direct Debit as Source of Fund for ShopeePay QRIS CrossBorder", "P1", "Rene"),
    ("[PH] SLoans Disbursement", "P1", "Ker Yin"),
    ("[SG] Reset PIN/Password flow standardisation", "P1", "Jireh"),
    ("[SG] Mari Trade", "SP", "Zoey"),
    ("[ID] Credit Card - Txn Scenario / In-App Auth / Rules Changes", "SP", "Ker Yin"),
)


def singapore_today(now: datetime | None = None) -> date:
    current = now or datetime.now(VERSION_PLAN_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=VERSION_PLAN_TIMEZONE)
    return current.astimezone(VERSION_PLAN_TIMEZONE).date()


def singapore_date_text(now: datetime | None = None) -> str:
    return singapore_today(now).isoformat()


def normalize_version_plan_state(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    raw_af = raw.get("af") if isinstance(raw.get("af"), dict) else {}
    bundles = raw_af.get("bundles") if isinstance(raw_af.get("bundles"), dict) else {}
    normalized_bundles: dict[str, dict[str, Any]] = {}
    for version_id, bundle in bundles.items():
        normalized_version_id = str(version_id or "").strip()
        if not normalized_version_id or not isinstance(bundle, dict):
            continue
        normalized_bundles[normalized_version_id] = {
            **{key: deepcopy(value) for key, value in bundle.items() if key not in {"manual_rows", "synced_rows"}},
            "manual_rows": _normalize_manual_rows(bundle.get("manual_rows")),
            "synced_rows": _normalize_synced_rows(bundle.get("synced_rows")),
        }

    raw_pipeline_rows = raw_af.get("pipeline_rows")
    pipeline_rows = _normalize_manual_rows(raw_pipeline_rows)
    if not isinstance(raw_pipeline_rows, list):
        pipeline_rows = _pipeline_seed_rows()

    seen_versions = raw_af.get("seen_versions") if isinstance(raw_af.get("seen_versions"), dict) else {}
    normalized_seen: dict[str, dict[str, Any]] = {}
    for version_id, version in seen_versions.items():
        normalized_version_id = str(version_id or "").strip()
        if normalized_version_id and isinstance(version, dict):
            normalized_seen[normalized_version_id] = _version_record(version)

    sync_state = raw_af.get("sync_state") if isinstance(raw_af.get("sync_state"), dict) else {}
    normalized_sync_state = {
        "state": str(sync_state.get("state") or "idle").strip() or "idle",
        "last_synced_date_sgt": str(sync_state.get("last_synced_date_sgt") or "").strip(),
        "started_at": str(sync_state.get("started_at") or "").strip(),
        "finished_at": str(sync_state.get("finished_at") or "").strip(),
        "message": str(sync_state.get("message") or "").strip(),
        "error": str(sync_state.get("error") or "").strip(),
    }
    return {
        "af": {
            "bundles": normalized_bundles,
            "pipeline_rows": pipeline_rows,
            "seen_versions": normalized_seen,
            "sync_state": normalized_sync_state,
        }
    }


def version_plan_payload(config: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    af = plan["af"]
    today = singapore_today(now)
    bundles = []
    archived = []
    for version_id, bundle in af["bundles"].items():
        item = _bundle_payload(bundle, today=today)
        release_date = _parse_date(item.get("af_release_date"))
        if release_date and release_date < today:
            archived.append(_archived_bundle_payload(item))
        else:
            bundles.append(item)
    bundles.sort(key=lambda item: (str(item.get("af_release_date") or "9999-12-31"), str(item.get("af_version_name") or "")))
    archived.sort(key=lambda item: (str(item.get("af_release_date") or "0000-00-00"), str(item.get("af_version_name") or "")), reverse=True)
    return {
        "status": "ok",
        "track": "Anti-fraud",
        "priority_order": list(VERSION_PLAN_PRIORITY_ORDER),
        "pm_options": list(VERSION_PLAN_PM_OPTIONS),
        "bundles": bundles,
        "pipeline_rows": _sort_manual_rows(af["pipeline_rows"]),
        "archived_bundles": archived,
        "sync_state": af["sync_state"],
        "today_sgt": today.isoformat(),
    }


def version_plan_sync(config: dict[str, Any], bpmis_client: Any, *, now: datetime | None = None) -> dict[str, Any]:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    af = plan["af"]
    today = singapore_today(now)
    current_dt = now or datetime.now(VERSION_PLAN_TIMEZONE)
    if current_dt.tzinfo is None:
        current_dt = current_dt.replace(tzinfo=VERSION_PLAN_TIMEZONE)

    af_versions = [_version_record(row) for row in _safe_search_versions(bpmis_client, "AF_")]
    af_versions = [row for row in af_versions if row["version_id"] and row["version_name"].startswith("AF_") and row["release_date"]]
    dbp_versions_by_prefix = {
        "DBPSG": [_version_record(row) for row in _safe_search_versions(bpmis_client, "DBPSG_")],
        "DBPID": [_version_record(row) for row in _safe_search_versions(bpmis_client, "DBPID_")],
        "DBPPH": [_version_record(row) for row in _safe_search_versions(bpmis_client, "DBPPH_")],
    }
    range_end = _next_quarter_end(today)
    upcoming = [row for row in af_versions if _date_in_range(_parse_date(row["release_date"]), today, range_end)]
    upcoming.sort(key=lambda row: (row["release_date"], row["version_name"]))

    for version in upcoming:
        af["seen_versions"][version["version_id"]] = version

    next_bundles: dict[str, dict[str, Any]] = {}
    all_versions_by_id = {row["version_id"]: row for row in af_versions}
    all_versions_by_id.update(af["seen_versions"])
    candidate_versions = list(upcoming)
    for seen_version in af["seen_versions"].values():
        release_date = _parse_date(seen_version.get("release_date"))
        if release_date and release_date < today:
            candidate_versions.append(all_versions_by_id.get(str(seen_version.get("version_id") or ""), seen_version))

    for version in _dedupe_versions(candidate_versions):
        version_id = version["version_id"]
        release_date = _parse_date(version["release_date"])
        existing = af["bundles"].get(version_id, {})
        mapped_dbp_versions = _mapped_dbp_versions(version, dbp_versions_by_prefix)
        in_dev = _is_in_dev(version, current_dt)
        synced_rows = []
        if in_dev or (release_date and release_date < today):
            synced_rows = _sync_rows_for_bundle(bpmis_client, mapped_dbp_versions)
        elif isinstance(existing, dict):
            synced_rows = _normalize_synced_rows(existing.get("synced_rows"))
        next_bundles[version_id] = {
            **version,
            "mapped_versions": mapped_dbp_versions,
            "prd_initial_date": _offset_date_text(_prd_schedule_base_date(version), -4),
            "prd_final_date": _offset_date_text(_prd_schedule_base_date(version), -2),
            "in_dev": in_dev,
            "manual_rows": _normalize_manual_rows(existing.get("manual_rows") if isinstance(existing, dict) else []),
            "synced_rows": synced_rows,
            "synced_at": _now_text(current_dt) if synced_rows else str(existing.get("synced_at") or ""),
        }
    af["bundles"] = next_bundles
    af["sync_state"] = {
        "state": "fresh_today",
        "last_synced_date_sgt": today.isoformat(),
        "started_at": str(af["sync_state"].get("started_at") or ""),
        "finished_at": _now_text(current_dt),
        "message": "Version Plan synced.",
        "error": "",
    }
    config = dict(config)
    config["version_plan"] = plan
    return config


def mark_version_plan_sync_running(config: dict[str, Any], *, message: str = "Syncing Jira information.") -> dict[str, Any]:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    now_text = _now_text()
    plan["af"]["sync_state"] = {
        **plan["af"]["sync_state"],
        "state": "running",
        "started_at": now_text,
        "finished_at": "",
        "message": message,
        "error": "",
    }
    config = dict(config)
    config["version_plan"] = plan
    return config


def mark_version_plan_sync_error(config: dict[str, Any], error: str) -> dict[str, Any]:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    plan["af"]["sync_state"] = {
        **plan["af"]["sync_state"],
        "state": "error",
        "finished_at": _now_text(),
        "message": "Version Plan sync failed.",
        "error": str(error or "Sync failed."),
    }
    config = dict(config)
    config["version_plan"] = plan
    return config


def version_plan_synced_today(config: dict[str, Any], *, now: datetime | None = None) -> bool:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    return str(plan["af"]["sync_state"].get("last_synced_date_sgt") or "") == singapore_date_text(now)


def update_version_plan_cell(config: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    row = _find_manual_row(plan, payload)
    field = str(payload.get("field") or "").strip()
    if field not in {"feature", "priority", "pm", "remarks", "productization_efforts"}:
        raise ValueError("Unsupported Version Plan field.")
    if field == "priority":
        row[field] = _normalize_priority(payload.get("value"))
    elif field == "pm":
        row[field] = _normalize_pm_values(payload.get("value"))
    else:
        row[field] = str(payload.get("value") or "").strip()
    row["updated_at"] = _now_text()
    config = dict(config)
    config["version_plan"] = plan
    return config


def update_version_plan_rows(config: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    action = str(payload.get("action") or "").strip().lower()
    if action == "move":
        _move_version_plan_manual_row(plan, payload)
        config = dict(config)
        config["version_plan"] = plan
        return config

    rows = _manual_rows_for_scope(plan, payload)
    if action == "add":
        rows.append(_manual_row({"sort_order": _next_sort_order(rows)}))
    elif action == "delete":
        row_id = str(payload.get("row_id") or "").strip()
        rows[:] = [row for row in rows if row.get("row_id") != row_id]
    elif action == "reorder":
        order = [str(item or "").strip() for item in payload.get("row_ids") or [] if str(item or "").strip()]
        index = {row_id: position for position, row_id in enumerate(order)}
        for row in rows:
            if row.get("row_id") in index:
                row["sort_order"] = index[row["row_id"]]
    else:
        raise ValueError("Unsupported Version Plan row action.")
    config = dict(config)
    config["version_plan"] = plan
    return config


def _move_version_plan_manual_row(plan: dict[str, Any], payload: dict[str, Any]) -> None:
    row_id = str(payload.get("row_id") or "").strip()
    if not row_id:
        raise ValueError("row_id is required.")
    source_payload = {
        "scope": str(payload.get("source_scope") or payload.get("scope") or "").strip(),
        "version_id": str(payload.get("source_version_id") or payload.get("version_id") or "").strip(),
    }
    target_payload = {
        "scope": str(payload.get("target_scope") or "").strip(),
        "version_id": str(payload.get("target_version_id") or "").strip(),
    }
    source_rows = _manual_rows_for_scope(plan, source_payload)
    target_rows = _manual_rows_for_scope(plan, target_payload)
    if source_rows is target_rows:
        return
    moving_row = None
    remaining_rows = []
    for row in source_rows:
        if row.get("row_id") == row_id:
            moving_row = row
        else:
            remaining_rows.append(row)
    if moving_row is None:
        raise ValueError("Version Plan manual row was not found.")
    source_rows[:] = remaining_rows
    moving_row["sort_order"] = _next_sort_order(target_rows)
    moving_row["updated_at"] = _now_text()
    target_rows.append(moving_row)


def _pipeline_seed_rows() -> list[dict[str, Any]]:
    return [
        _manual_row({"feature": feature, "priority": priority, "pm": _split_pm_values(pm), "sort_order": index})
        for index, (feature, priority, pm) in enumerate(PIPELINE_SEED_ROWS)
    ]


def _manual_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_id": str(row.get("row_id") or f"manual-{uuid.uuid4().hex}"),
        "row_type": "manual",
        "feature": str(row.get("feature") or "").strip(),
        "priority": _normalize_priority(row.get("priority")),
        "pm": _normalize_pm_values(row.get("pm")),
        "remarks": str(row.get("remarks") or "").strip(),
        "productization_efforts": str(row.get("productization_efforts") or "").strip().upper() if str(row.get("productization_efforts") or "").strip().upper() in {"Y", "N"} else "",
        "sort_order": _safe_int(row.get("sort_order"), 0),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def _normalize_manual_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [_manual_row(row) for row in value if isinstance(row, dict)]


def _normalize_synced_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [_synced_row(row) for row in value if isinstance(row, dict)]


def _synced_row(row: dict[str, Any]) -> dict[str, Any]:
    jira_id = str(row.get("jira_id") or row.get("jira_ticket_number") or "").strip()
    return {
        "row_id": str(row.get("row_id") or f"sync-{jira_id or uuid.uuid4().hex}"),
        "row_type": "synced",
        "jira_id": jira_id,
        "jira_link": str(row.get("jira_link") or "").strip(),
        "market": _normalize_market(row.get("market")),
        "jira_summary": str(row.get("jira_summary") or row.get("feature") or "").strip(),
        "priority": _normalize_priority(row.get("priority")),
        "pm": _normalize_pm_values(row.get("pm")),
        "remarks": str(row.get("remarks") or "").strip(),
        "productization_efforts": "Y" if jira_id.upper().startswith("SPDBP") else "N",
        "sort_order": _safe_int(row.get("sort_order"), 0),
    }


def _version_record(row: dict[str, Any]) -> dict[str, Any]:
    version_name = str(
        row.get("fullName") or row.get("name") or row.get("versionName") or row.get("version_name") or row.get("label") or ""
    ).strip()
    return {
        "version_id": str(row.get("id") or row.get("versionId") or row.get("version_id") or "").strip(),
        "version_name": version_name,
        "release_date": _date_text(row.get("timelineEnd") or row.get("release_date") or row.get("release") or ""),
        "timeline_start": _datetime_text(row.get("timelineStart") or row.get("timeline_start") or ""),
        "prd_deadline_date": _version_prd_deadline_date(row),
        "market": _normalize_market(row.get("market") or row.get("marketId") or _market_from_version_name(version_name)),
    }


def _version_prd_deadline_date(row: dict[str, Any]) -> str:
    timeline = row.get("timeline") if isinstance(row.get("timeline"), dict) else {}
    for value in (
        timeline.get("prdDueDate"),
        timeline.get("prdDeadline"),
        timeline.get("prdEndDate"),
        timeline.get("prdFinalDate"),
        row.get("prdDueDate"),
        row.get("prdDeadline"),
        row.get("prdEndDate"),
        row.get("prdFinalDate"),
        row.get("prd_deadline_date"),
    ):
        text = _date_text(value)
        if text:
            return text
    return ""


def _prd_schedule_base_date(version: dict[str, Any]) -> str:
    return str(version.get("prd_deadline_date") or version.get("release_date") or "").strip()


def _bundle_payload(bundle: dict[str, Any], *, today: date) -> dict[str, Any]:
    item = {
        "version_id": str(bundle.get("version_id") or "").strip(),
        "af_version_name": str(bundle.get("version_name") or "").strip(),
        "af_release_date": str(bundle.get("release_date") or "").strip(),
        "timeline_start": str(bundle.get("timeline_start") or "").strip(),
        "prd_deadline_date": str(bundle.get("prd_deadline_date") or "").strip(),
        "prd_initial_date": str(bundle.get("prd_initial_date") or "").strip(),
        "prd_final_date": str(bundle.get("prd_final_date") or "").strip(),
        "in_dev": bool(bundle.get("in_dev")),
        "mapped_versions": bundle.get("mapped_versions") if isinstance(bundle.get("mapped_versions"), dict) else {},
        "synced_at": str(bundle.get("synced_at") or "").strip(),
        "synced_rows": _sort_synced_rows(bundle.get("synced_rows")),
        "manual_rows": _sort_manual_rows(bundle.get("manual_rows")),
    }
    release_date = _parse_date(item["af_release_date"])
    item["is_archived"] = bool(release_date and release_date < today)
    return item


def _archived_bundle_payload(item: dict[str, Any]) -> dict[str, Any]:
    archived = dict(item)
    archived["manual_rows"] = []
    archived["is_archived"] = True
    return archived


def _sync_rows_for_bundle(bpmis_client: Any, mapped_versions: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for prefix, version in mapped_versions.items():
        version_id = str((version or {}).get("version_id") or "").strip() if isinstance(version, dict) else ""
        if not version_id:
            continue
        for raw in _safe_list_issues_for_version(bpmis_client, version_id):
            if not _is_af_reporter(raw):
                continue
            if _is_closed_or_icebox(raw):
                continue
            jira_id = _extract_jira_id(raw)
            if not jira_id or jira_id in seen:
                continue
            seen.add(jira_id)
            parent = _parent_project_detail(bpmis_client, raw)
            rows.append(
                _synced_row(
                    {
                        "row_id": f"sync-{version_id}-{jira_id}",
                        "jira_id": jira_id,
                        "jira_link": _extract_jira_link(raw, jira_id),
                        "market": _extract_market(parent) or _extract_market(raw) or _market_from_version_prefix(prefix),
                        "jira_summary": _extract_first_text(raw, "summary", "title", "jiraSummary"),
                        "priority": _extract_first_text(parent, "bizPriorityId", "bizPriority", "priority", "priorityId"),
                        "pm": _extract_pm(raw),
                        "sort_order": len(rows),
                    }
                )
            )
    return rows


def _parent_project_detail(bpmis_client: Any, row: dict[str, Any]) -> dict[str, Any]:
    parent_ids = row.get("parentIds") if isinstance(row.get("parentIds"), list) else []
    if not parent_ids:
        return {}
    parent_id = str(parent_ids[0] or "").strip()
    if not parent_id or not hasattr(bpmis_client, "get_issue_detail"):
        return {}
    try:
        detail = bpmis_client.get_issue_detail(parent_id) or {}
        return detail if isinstance(detail, dict) else {}
    except Exception:
        return {}


def _mapped_dbp_versions(af_version: dict[str, Any], dbp_versions_by_prefix: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    release_date = _parse_date(af_version.get("release_date"))
    minimum_date = release_date + timedelta(days=7) if release_date else None
    mapped: dict[str, dict[str, Any]] = {}
    for prefix, versions in dbp_versions_by_prefix.items():
        candidates = []
        for version in versions:
            if not str(version.get("version_name") or "").startswith(f"{prefix}_"):
                continue
            candidate_date = _parse_date(version.get("release_date"))
            if minimum_date and candidate_date and candidate_date >= minimum_date:
                candidates.append(version)
        candidates.sort(key=lambda row: (str(row.get("release_date") or "9999-12-31"), str(row.get("version_name") or "")))
        mapped[prefix] = candidates[0] if candidates else {"version_id": "", "version_name": "-", "release_date": "", "market": _market_from_version_prefix(prefix)}
    return mapped


def _safe_search_versions(bpmis_client: Any, query: str) -> list[dict[str, Any]]:
    try:
        rows = bpmis_client.search_versions(query) if hasattr(bpmis_client, "search_versions") else []
        return [row for row in rows or [] if isinstance(row, dict)]
    except Exception:
        return []


def _safe_list_issues_for_version(bpmis_client: Any, version_id: str) -> list[dict[str, Any]]:
    try:
        rows = bpmis_client.list_issues_for_version(version_id) if hasattr(bpmis_client, "list_issues_for_version") else []
        return [row for row in rows or [] if isinstance(row, dict)]
    except Exception:
        return []


def _find_manual_row(plan: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    row_id = str(payload.get("row_id") or "").strip()
    for row in _manual_rows_for_scope(plan, payload):
        if row.get("row_id") == row_id:
            return row
    raise ValueError("Version Plan manual row was not found.")


def _manual_rows_for_scope(plan: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    scope = str(payload.get("scope") or "").strip().lower()
    af = plan["af"]
    if scope == "pipeline":
        return af["pipeline_rows"]
    if scope == "bundle":
        version_id = str(payload.get("version_id") or "").strip()
        if not version_id:
            raise ValueError("version_id is required.")
        bundle = af["bundles"].setdefault(version_id, {"manual_rows": [], "synced_rows": []})
        bundle["manual_rows"] = _normalize_manual_rows(bundle.get("manual_rows"))
        return bundle["manual_rows"]
    raise ValueError("Unsupported Version Plan row scope.")


def _sort_manual_rows(rows: Any) -> list[dict[str, Any]]:
    normalized = _normalize_manual_rows(rows)
    return sorted(normalized, key=lambda row: (_priority_rank(row.get("priority")), _safe_int(row.get("sort_order"), 0), str(row.get("feature") or "")))


def _sort_synced_rows(rows: Any) -> list[dict[str, Any]]:
    normalized = _normalize_synced_rows(rows)
    return sorted(normalized, key=lambda row: (_priority_rank(row.get("priority")), _safe_int(row.get("sort_order"), 0), str(row.get("jira_id") or "")))


def _priority_rank(priority: Any) -> int:
    normalized = _normalize_priority(priority)
    try:
        return VERSION_PLAN_PRIORITY_ORDER.index(normalized)
    except ValueError:
        return len(VERSION_PLAN_PRIORITY_ORDER)


def _normalize_priority(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if text in VERSION_PLAN_PRIORITY_ORDER else ""


def _normalize_pm_values(value: Any) -> list[str]:
    if isinstance(value, list):
        raw_values = value
    else:
        raw_values = _split_pm_values(str(value or ""))
    normalized: list[str] = []
    allowed = {item.casefold(): item for item in VERSION_PLAN_PM_OPTIONS}
    for raw in raw_values:
        text = str(raw or "").strip()
        if not text:
            continue
        text_key = text.casefold()
        canonical = VERSION_PLAN_PM_ALIASES.get(text_key) or allowed.get(text_key, text)
        if text_key == "xiaodong.zheng@npt.sg":
            continue
        if canonical not in normalized:
            normalized.append(canonical)
    return normalized


def _split_pm_values(value: str) -> list[str]:
    return [part.strip() for part in re.split(r"\s*(?:/|&|,|;|\band\b)\s*", str(value or ""), flags=re.IGNORECASE) if part.strip()]


def _is_af_reporter(row: dict[str, Any]) -> bool:
    reporter = row.get("reporter")
    email = ""
    if isinstance(reporter, dict):
        email = str(reporter.get("email") or reporter.get("name") or "").strip().lower()
    if not email:
        email = _extract_first_text(row, "reporterEmail", "reporter_email", "pm_email").lower()
    return email in {
        "jireh.tanyx@npt.sg",
        "keryin.lim@npt.sg",
        "chongzj@npt.sg",
        "chang.wang@npt.sg",
        "zoey.luxy@npt.sg",
    }


def _is_closed_or_icebox(row: dict[str, Any]) -> bool:
    status = _extract_first_text(row, "statusId", "status", "jiraStatus").casefold()
    return status in {"closed", "icebox"}


def _extract_jira_id(row: dict[str, Any]) -> str:
    value = _extract_first_text(row, "jiraKey", "ticketKey", "jiraIssueKey", "issueKey", "key")
    if value:
        return value
    link = _extract_first_text(row, "jiraLink", "ticketLink", "jiraUrl", "url", "link")
    match = re.search(r"([A-Z][A-Z0-9]+-\d+)", link)
    return match.group(1) if match else ""


def _extract_jira_link(row: dict[str, Any], jira_id: str) -> str:
    link = _extract_first_text(row, "jiraLink", "ticketLink", "jiraUrl", "url", "link")
    if link:
        return link
    return f"https://jira.shopee.io/browse/{jira_id}" if jira_id else ""


def _extract_pm(row: dict[str, Any]) -> list[str]:
    value = row.get("jiraRegionalPmPicId") or row.get("regionalPmPic") or row.get("productManager") or row.get("pm")
    people = _flatten_people(value)
    return _normalize_pm_values(people)


def _flatten_people(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [str(value.get(key) or "").strip() for key in ("displayName", "email", "name", "label") if str(value.get(key) or "").strip()][:1]
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(_flatten_people(item))
        return result
    if value:
        return [str(value).strip()]
    return []


def _extract_market(row: dict[str, Any]) -> str:
    return _normalize_market(row.get("market") or row.get("marketId") or row.get("country") or row.get("region"))


def _normalize_market(value: Any) -> str:
    text = str(value or "").strip()
    lookup = {
        "1": "ID",
        "2": "PH",
        "3": "SG",
        "4": "Regional",
        "id": "ID",
        "ph": "PH",
        "sg": "SG",
        "regional": "Regional",
        "reg": "Regional",
        "all": "Regional",
    }
    return lookup.get(text.casefold(), text if text in {"ID", "PH", "SG", "Regional"} else "")


def _market_from_version_prefix(prefix: str) -> str:
    return {"DBPSG": "SG", "DBPID": "ID", "DBPPH": "PH"}.get(str(prefix or "").upper(), "Regional")


def _market_from_version_name(name: str) -> str:
    upper = str(name or "").upper()
    for prefix in ("DBPSG", "DBPID", "DBPPH"):
        if upper.startswith(prefix):
            return _market_from_version_prefix(prefix)
    if upper.startswith("AF_"):
        return "Regional"
    return ""


def _extract_first_text(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if isinstance(value, dict):
            for child_key in ("label", "name", "displayName", "email", "value"):
                child = str(value.get(child_key) or "").strip()
                if child:
                    return child
        if isinstance(value, list):
            flattened = _flatten_people(value)
            if flattened:
                return flattened[0]
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _dedupe_versions(versions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    seen = set()
    for version in versions:
        version_id = str(version.get("version_id") or "").strip()
        if not version_id or version_id in seen:
            continue
        seen.add(version_id)
        result.append(version)
    return result


def _is_in_dev(version: dict[str, Any], now: datetime) -> bool:
    start = _parse_datetime(version.get("timeline_start"))
    return bool(start and start <= now.astimezone(VERSION_PLAN_TIMEZONE))


def _next_quarter_end(today: date) -> date:
    quarter = (today.month - 1) // 3 + 1
    next_quarter = quarter + 1
    year = today.year + (1 if next_quarter > 4 else 0)
    next_quarter = 1 if next_quarter > 4 else next_quarter
    end_month = next_quarter * 3
    if end_month == 12:
        return date(year, 12, 31)
    return date(year, end_month + 1, 1) - timedelta(days=1)


def _date_in_range(value: date | None, start: date, end: date) -> bool:
    return bool(value and start <= value <= end)


def _date_text(value: Any) -> str:
    parsed = _parse_datetime(value)
    if parsed:
        return parsed.date().isoformat()
    parsed_date = _parse_date(value)
    return parsed_date.isoformat() if parsed_date else ""


def _datetime_text(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "").strip()


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=VERSION_PLAN_TIMEZONE)
        return parsed.astimezone(VERSION_PLAN_TIMEZONE)
    except ValueError:
        return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    parsed = _parse_datetime(value)
    if parsed:
        return parsed.date()
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _offset_date_text(value: Any, days: int) -> str:
    parsed = _parse_date(value)
    return (parsed + timedelta(days=days)).isoformat() if parsed else ""


def _now_text(now: datetime | None = None) -> str:
    current = now or datetime.now(VERSION_PLAN_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=VERSION_PLAN_TIMEZONE)
    return current.astimezone(VERSION_PLAN_TIMEZONE).isoformat()


def _next_sort_order(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    return max(_safe_int(row.get("sort_order"), 0) for row in rows) + 1


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
