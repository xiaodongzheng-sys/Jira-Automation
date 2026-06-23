from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta
import re
import uuid
from typing import Any
from zoneinfo import ZoneInfo


VERSION_PLAN_PRIORITY_ORDER = ("SP", "P0", "P1", "P2", "P3")
VERSION_PLAN_PM_OPTIONS = ("Wang Chang", "Zoey", "Jireh", "Ker Yin", "Rene", "Jun Wei", "Xiaodong")
VERSION_PLAN_AF_PM_EMAILS = (
    "jireh.tanyx@npt.sg",
    "keryin.lim@npt.sg",
    "chongzj@npt.sg",
    "chang.wang@npt.sg",
    "zoey.luxy@npt.sg",
)
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
    "junwei": "Jun Wei",
    "junwei.ong@npt.sg": "Jun Wei",
    "xiaodong.zheng@npt.sg": "Xiaodong",
    "xiaodong": "Xiaodong",
}
VERSION_PLAN_TIMEZONE = ZoneInfo("Asia/Singapore")
VERSION_PLAN_SYNC_OPERATION = "af_version_plan"
VERSION_PLAN_AUDIT_LIMIT = 500


class VersionPlanSyncUpstreamError(RuntimeError):
    """Raised when a critical BPMIS upstream call fails during Version Plan sync."""


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
        "last_synced_at_sgt": str(sync_state.get("last_synced_at_sgt") or "").strip(),
        "started_at": str(sync_state.get("started_at") or "").strip(),
        "finished_at": str(sync_state.get("finished_at") or "").strip(),
        "message": str(sync_state.get("message") or "").strip(),
        "error": str(sync_state.get("error") or "").strip(),
    }
    audit_log = _normalize_audit_log(raw_af.get("audit_log"))
    return {
        "af": {
            "bundles": normalized_bundles,
            "pipeline_rows": pipeline_rows,
            "seen_versions": normalized_seen,
            "sync_state": normalized_sync_state,
            "audit_log": audit_log,
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

    af_versions = [_version_record(row) for row in _search_versions_or_raise(bpmis_client, "AF_")]
    af_versions = [row for row in af_versions if row["version_id"] and row["version_name"].startswith("AF_") and row["release_date"]]
    if not af_versions:
        af["sync_state"] = {
            **af["sync_state"],
            "state": "error",
            "finished_at": _now_text(current_dt),
            "message": "Version Plan sync skipped.",
            "error": "No AF versions were returned from BPMIS; cached Version Plan data was preserved.",
        }
        config = dict(config)
        config["version_plan"] = plan
        return config
    dbp_versions_by_prefix = {
        "DBPSG": [_version_record(row) for row in _search_versions_or_raise(bpmis_client, "DBPSG_")],
        "DBPID": [_version_record(row) for row in _search_versions_or_raise(bpmis_client, "DBPID_")],
        "DBPPH": [_version_record(row) for row in _search_versions_or_raise(bpmis_client, "DBPPH_")],
    }
    range_end = _next_quarter_end(today)
    upcoming = [row for row in af_versions if _date_in_range(_parse_date(row["release_date"]), today, range_end)]
    upcoming.sort(key=lambda row: (row["release_date"], row["version_name"]))

    for version in upcoming:
        af["seen_versions"][version["version_id"]] = version

    next_bundles: dict[str, dict[str, Any]] = {}
    all_versions_by_id = dict(af["seen_versions"])
    all_versions_by_id.update({row["version_id"]: row for row in af_versions})
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
        prd_final_date = _offset_date_text(_prd_schedule_base_date(version), -2)
        if _version_plan_should_sync_jira(version, today):
            synced_rows = _sync_rows_for_bundle(
                bpmis_client,
                version,
                mapped_dbp_versions,
                existing.get("synced_rows") if isinstance(existing, dict) else [],
            )
        next_bundles[version_id] = {
            **version,
            "mapped_versions": mapped_dbp_versions,
            "prd_initial_date": _offset_date_text(_prd_schedule_base_date(version), -4),
            "prd_final_date": prd_final_date,
            "in_dev": in_dev,
            "manual_rows": _normalize_manual_rows(existing.get("manual_rows") if isinstance(existing, dict) else []),
            "synced_rows": synced_rows,
            "synced_at": _now_text(current_dt) if synced_rows else str(existing.get("synced_at") or ""),
        }
    af["bundles"] = next_bundles
    af["sync_state"] = {
        "state": "fresh_today",
        "last_synced_date_sgt": today.isoformat(),
        "last_synced_at_sgt": _display_text(current_dt),
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


def merge_version_plan_editable_state(synced_config: dict[str, Any], current_config: dict[str, Any]) -> dict[str, Any]:
    synced_plan = normalize_version_plan_state(synced_config.get("version_plan") if isinstance(synced_config, dict) else {})
    current_plan = normalize_version_plan_state(current_config.get("version_plan") if isinstance(current_config, dict) else {})
    synced_af = synced_plan["af"]
    current_af = current_plan["af"]
    synced_af["pipeline_rows"] = _normalize_manual_rows(current_af.get("pipeline_rows"))
    synced_af["audit_log"] = _normalize_audit_log(current_af.get("audit_log"))
    for version_id, current_bundle in current_af["bundles"].items():
        synced_bundle = synced_af["bundles"].get(version_id)
        if not isinstance(synced_bundle, dict):
            if current_bundle.get("manual_rows"):
                synced_af["bundles"][version_id] = {
                    **{key: deepcopy(value) for key, value in current_bundle.items() if key not in {"manual_rows", "synced_rows"}},
                    "manual_rows": _normalize_manual_rows(current_bundle.get("manual_rows")),
                    "synced_rows": [],
                }
            continue
        synced_bundle["manual_rows"] = _normalize_manual_rows(current_bundle.get("manual_rows"))
    merged = dict(synced_config)
    merged["version_plan"] = synced_plan
    return merged


def append_version_plan_audit(
    config: dict[str, Any],
    *,
    action: str,
    actor: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    entry = _audit_entry(action=action, actor=actor or {}, details=details or {})
    audit_log = _normalize_audit_log(plan["af"].get("audit_log"))
    audit_log.append(entry)
    plan["af"]["audit_log"] = audit_log[-VERSION_PLAN_AUDIT_LIMIT:]
    updated = dict(config)
    updated["version_plan"] = plan
    return updated


def version_plan_synced_today(config: dict[str, Any], *, now: datetime | None = None) -> bool:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    return str(plan["af"]["sync_state"].get("last_synced_date_sgt") or "") == singapore_date_text(now)


def version_plan_auto_sync_attempted_today(config: dict[str, Any], *, now: datetime | None = None) -> bool:
    if version_plan_synced_today(config, now=now):
        return True
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    sync_state = plan["af"]["sync_state"]
    if str(sync_state.get("state") or "").strip() != "error":
        return False
    today = singapore_today(now)
    for key in ("finished_at", "started_at"):
        sync_date = _parse_date(sync_state.get(key))
        if sync_date == today:
            return True
    return False


def update_version_plan_cell(config: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
    field = str(payload.get("field") or "").strip()
    if field not in {"feature", "priority", "pm", "productization_efforts"}:
        raise ValueError("Unsupported Version Plan field.")
    row = _find_version_plan_cell_row(plan, payload, field)
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


def _find_version_plan_cell_row(plan: dict[str, Any], payload: dict[str, Any], field: str) -> dict[str, Any]:
    try:
        return _find_manual_row(plan, payload)
    except ValueError:
        pass
    raise ValueError("Version Plan manual row was not found.")


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
        rows.append(
            _manual_row(
                {
                    "row_id": str(payload.get("row_id") or "").strip(),
                    "feature": str(payload.get("feature") or "").strip(),
                    "priority": payload.get("priority"),
                    "pm": payload.get("pm"),
                    "remarks": str(payload.get("remarks") or "").strip(),
                    "productization_efforts": str(payload.get("productization_efforts") or "").strip(),
                    "sort_order": _next_sort_order(rows),
                    "updated_at": _now_text(),
                }
            )
        )
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
    moving_row["updated_at"] = _now_text()
    target_before_row_id = str(payload.get("target_before_row_id") or "").strip()
    target_index = next(
        (index for index, row in enumerate(target_rows) if row.get("row_id") == target_before_row_id),
        len(target_rows),
    )
    target_rows.insert(target_index, moving_row)
    for index, row in enumerate(source_rows):
        row["sort_order"] = index
    for index, row in enumerate(target_rows):
        row["sort_order"] = index


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
        "component": "",
        "release_version": "",
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
    jira_board = _extract_jira_board(row) or jira_id
    productization_efforts = str(row.get("productization_efforts") or "").strip().upper()
    if productization_efforts not in {"Y", "N"}:
        productization_efforts = "Y" if _jira_board_is_productization(jira_board) else "N"
    return {
        "row_id": str(row.get("row_id") or f"sync-{jira_id or uuid.uuid4().hex}"),
        "row_type": "synced",
        "jira_id": jira_id,
        "jira_link": _canonical_jira_link(row.get("jira_link"), jira_id),
        "market": _market_from_jira_board(jira_board) or _normalize_market(row.get("market")),
        "jira_summary": str(row.get("jira_summary") or row.get("feature") or "").strip(),
        "priority": _normalize_priority(row.get("priority")),
        "pm": _normalize_pm_values(row.get("pm")),
        "remarks": str(row.get("remarks") or "").strip(),
        "productization_efforts": productization_efforts,
        "component": str(row.get("component") or "").strip(),
        "release_version": str(row.get("release_version") or "").strip(),
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
    release_date = _parse_date(str(bundle.get("release_date") or ""))
    should_show_synced_rows = _bundle_should_show_synced_rows(bundle, today=today)
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
        "synced_at": str(bundle.get("synced_at") or "").strip() if should_show_synced_rows else "",
        "synced_rows": _sort_synced_rows(bundle.get("synced_rows")) if should_show_synced_rows else [],
        "manual_rows": _sort_manual_rows(bundle.get("manual_rows")),
    }
    item["is_archived"] = bool(release_date and release_date < today)
    return item


def _archived_bundle_payload(item: dict[str, Any]) -> dict[str, Any]:
    archived = dict(item)
    archived["manual_rows"] = []
    archived["is_archived"] = True
    return archived


def _version_plan_should_sync_jira(version: dict[str, Any], today: date) -> bool:
    prd_final_date = _parse_date(_offset_date_text(_prd_schedule_base_date(version), -2))
    return bool(prd_final_date and prd_final_date < today)


def _bundle_should_show_synced_rows(bundle: dict[str, Any], *, today: date) -> bool:
    release_date = _parse_date(str(bundle.get("release_date") or ""))
    if release_date and release_date < today:
        return True
    prd_final_date = _parse_date(str(bundle.get("prd_final_date") or ""))
    if prd_final_date:
        return prd_final_date < today
    if not str(bundle.get("prd_deadline_date") or "").strip():
        return True
    return _version_plan_should_sync_jira(bundle, today)


def _sync_rows_for_bundle(
    bpmis_client: Any,
    af_version: dict[str, Any],
    mapped_versions: dict[str, Any],
    existing_synced_rows: Any = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    existing_by_jira = {
        str(row.get("jira_id") or "").strip(): row
        for row in _normalize_synced_rows(existing_synced_rows)
        if str(row.get("jira_id") or "").strip()
    }
    release_after = str(af_version.get("release_date") or "").strip()
    release_before = _latest_mapped_release_date(mapped_versions)
    af_version_id = str(af_version.get("version_id") or "").strip()
    candidates: list[dict[str, Any]] = []
    if af_version_id:
        candidates.extend(_list_productization_issues_for_version_or_raise(bpmis_client, af_version_id))
    candidates.extend(
        _list_jira_tasks_for_release_window_or_raise(
            bpmis_client,
            VERSION_PLAN_AF_PM_EMAILS,
            release_after=release_after,
            release_before=release_before,
        )
    )
    jira_live_details = _enrich_candidates_with_jira_live_details(bpmis_client, candidates)
    for raw in candidates:
        if _is_closed_or_icebox(raw):
            continue
        if _row_has_excluded_task_type(raw):
            continue
        if _row_has_planning_version(raw):
            continue
        jira_id = _extract_jira_id(raw)
        if not jira_id or jira_id in seen:
            continue
        seen.add(jira_id)
        _apply_jira_live_detail(raw, jira_id, jira_live_details)
        parent = _task_parent_project(raw)
        priority = _extract_sync_row_priority(raw, parent)
        if not priority:
            parent_detail = _parent_project_detail(bpmis_client, raw)
            if parent_detail:
                parent = parent or parent_detail
                priority = _extract_sync_row_priority(raw, parent_detail)
        existing = existing_by_jira.get(jira_id) or {}
        rows.append(
            _synced_row(
                {
                    "row_id": f"sync-{af_version.get('version_id') or af_version.get('version_name')}-{jira_id}",
                    "jira_id": jira_id,
                    "jira_link": _extract_jira_link(raw, jira_id),
                    "market": _extract_market_from_jira_board(raw, jira_id),
                    "jira_summary": _extract_first_text(raw, "jira_title", "summary", "title", "jiraSummary"),
                    "priority": priority,
                    "pm": _extract_pm(raw),
                    "remarks": existing.get("remarks") or "",
                    "productization_efforts": _extract_productization_efforts(raw, jira_id),
                    "component": _extract_component(raw),
                    "release_version": _extract_release_version(raw),
                    "sort_order": len(rows),
                }
            )
        )
    return rows


def _enrich_candidates_with_jira_live_details(
    bpmis_client: Any,
    candidates: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    jira_ids: list[str] = []
    seen_ids: set[str] = set()
    for raw in candidates:
        jira_id = _extract_jira_id(raw)
        if jira_id and jira_id not in seen_ids:
            seen_ids.add(jira_id)
            jira_ids.append(jira_id)
    if not jira_ids or not hasattr(bpmis_client, "get_jira_ticket_details"):
        return {}
    try:
        return bpmis_client.get_jira_ticket_details(jira_ids) or {}
    except Exception:
        return {}


def _apply_jira_live_detail(
    row: dict[str, Any],
    jira_id: str,
    live_details: dict[str, dict[str, Any]],
) -> None:
    if not live_details:
        return
    detail = live_details.get(jira_id) or live_details.get(jira_id.upper())
    if not detail or not isinstance(detail, dict):
        return
    live_components = detail.get("components")
    if isinstance(live_components, list) and live_components:
        row["components"] = live_components
    live_fix_versions = detail.get("fixVersions")
    if isinstance(live_fix_versions, list) and live_fix_versions:
        row["fixVersions"] = live_fix_versions


def _safe_list_productization_issues_for_version(bpmis_client: Any, version_id: str) -> list[dict[str, Any]]:
    rows = _safe_list_issues_for_version(bpmis_client, version_id)
    return [row for row in rows if _row_is_productization_ticket(row)]


def _list_productization_issues_for_version_or_raise(bpmis_client: Any, version_id: str) -> list[dict[str, Any]]:
    rows = _list_issues_for_version_or_raise(bpmis_client, version_id)
    return [row for row in rows if _row_is_productization_ticket(row)]


def _row_is_productization_ticket(row: dict[str, Any]) -> bool:
    jira_id = _extract_jira_id(row)
    jira_board = _extract_jira_board(row) or jira_id
    return _jira_board_is_productization(jira_board)


def _row_has_excluded_task_type(row: dict[str, Any]) -> bool:
    task_type = _extract_task_type(row).casefold()
    return task_type in {"tech", "support"}


def _row_has_planning_version(row: dict[str, Any]) -> bool:
    return any(name.startswith("planning") for name in _row_jira_version_names(row))


def _row_jira_version_names(row: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for key in (
        "version",
        "versions",
        "fix_version_name",
        "fixVersionName",
        "fixVersion",
        "fixVersions",
        "fixVersionId",
    ):
        _collect_version_names(row.get(key), names)
    raw = row.get("raw_response")
    if isinstance(raw, dict):
        for key in ("version", "versions", "fixVersion", "fixVersions", "fixVersionId"):
            _collect_version_names(raw.get(key), names)
    raw_jira = row.get("raw_jira")
    if isinstance(raw_jira, dict):
        fields = raw_jira.get("fields")
        if isinstance(fields, dict):
            _collect_version_names(fields.get("fixVersions"), names)
    return names


def _collect_version_names(value: Any, names: set[str]) -> None:
    if value is None:
        return
    if isinstance(value, dict):
        for key in ("fullName", "name", "versionName", "label", "value"):
            text = _normalize_version_match_text(value.get(key))
            if text:
                names.add(text)
        for key in ("version", "versions", "fixVersion", "fixVersions", "fixVersionId"):
            nested = value.get(key)
            if nested is not value:
                _collect_version_names(nested, names)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_version_names(item, names)
        return
    text = _normalize_version_match_text(value)
    if text:
        names.add(text)


def _normalize_version_match_text(value: Any) -> str:
    return str(value or "").strip().casefold()


def _latest_mapped_release_date(mapped_versions: dict[str, Any]) -> str:
    dates = [
        parsed
        for version in mapped_versions.values()
        for parsed in [_parse_date((version or {}).get("release_date") if isinstance(version, dict) else "")]
        if parsed
    ]
    return max(dates).isoformat() if dates else ""


def _safe_list_jira_tasks_for_release_window(
    bpmis_client: Any,
    emails: tuple[str, ...],
    *,
    release_after: str,
    release_before: str,
) -> list[dict[str, Any]]:
    if not hasattr(bpmis_client, "list_jira_tasks_created_by_emails"):
        return []
    try:
        rows = bpmis_client.list_jira_tasks_created_by_emails(
            list(emails),
            release_after=release_after,
            release_before=release_before,
        )
        return [row for row in rows or [] if isinstance(row, dict)]
    except Exception:
        return []


def _list_jira_tasks_for_release_window_or_raise(
    bpmis_client: Any,
    emails: tuple[str, ...],
    *,
    release_after: str,
    release_before: str,
) -> list[dict[str, Any]]:
    if not hasattr(bpmis_client, "list_jira_tasks_created_by_emails"):
        return []
    try:
        rows = bpmis_client.list_jira_tasks_created_by_emails(
            list(emails),
            release_after=release_after,
            release_before=release_before,
        )
    except Exception as error:  # noqa: BLE001
        raise VersionPlanSyncUpstreamError(
            "Version Plan sync aborted because BPMIS release-window Jira lookup failed."
        ) from error
    return [row for row in rows or [] if isinstance(row, dict)]


def _task_parent_project(row: dict[str, Any]) -> dict[str, Any]:
    parent = row.get("parent_project")
    return parent if isinstance(parent, dict) else {}


def _extract_sync_row_priority(row: dict[str, Any], parent: dict[str, Any] | None = None) -> str:
    parent_priority = _extract_parent_priority(parent or {})
    if parent_priority:
        return parent_priority
    return _extract_first_text(row, "priority", "bizPriorityId", "bizPriority", "priorityId")


def _extract_parent_priority(parent: dict[str, Any]) -> str:
    return _extract_first_text(parent, "priority", "bizPriorityId", "bizPriority", "priorityId")


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
    minimum_date = release_date + timedelta(days=6) if release_date else None
    mapped: dict[str, dict[str, Any]] = {}
    for prefix, versions in dbp_versions_by_prefix.items():
        candidates = []
        for version in versions:
            if not _is_formal_dbp_version_name(prefix, str(version.get("version_name") or "")):
                continue
            candidate_date = _parse_date(version.get("release_date"))
            if minimum_date and candidate_date and candidate_date >= minimum_date:
                candidates.append(version)
        candidates.sort(key=lambda row: (str(row.get("release_date") or "9999-12-31"), str(row.get("version_name") or "")))
        mapped[prefix] = candidates[0] if candidates else {"version_id": "", "version_name": "-", "release_date": "", "market": _market_from_version_prefix(prefix)}
    return mapped


def _is_formal_dbp_version_name(prefix: str, version_name: str) -> bool:
    normalized_prefix = re.escape(str(prefix or "").strip())
    normalized_name = str(version_name or "").strip()
    return bool(re.fullmatch(rf"{normalized_prefix}_v\d+(?:\.\d+)*_\d{{4}}", normalized_name))


def _safe_search_versions(bpmis_client: Any, query: str) -> list[dict[str, Any]]:
    try:
        rows = bpmis_client.search_versions(query) if hasattr(bpmis_client, "search_versions") else []
        return [row for row in rows or [] if isinstance(row, dict)]
    except Exception:
        return []


def _search_versions_or_raise(bpmis_client: Any, query: str) -> list[dict[str, Any]]:
    try:
        rows = bpmis_client.search_versions(query) if hasattr(bpmis_client, "search_versions") else []
    except Exception as error:  # noqa: BLE001
        raise VersionPlanSyncUpstreamError(
            f"Version Plan sync aborted because BPMIS version lookup failed for '{query}'."
        ) from error
    return [row for row in rows or [] if isinstance(row, dict)]


def _safe_list_issues_for_version(bpmis_client: Any, version_id: str) -> list[dict[str, Any]]:
    try:
        rows = bpmis_client.list_issues_for_version(version_id) if hasattr(bpmis_client, "list_issues_for_version") else []
        return [row for row in rows or [] if isinstance(row, dict)]
    except Exception:
        return []


def _list_issues_for_version_or_raise(bpmis_client: Any, version_id: str) -> list[dict[str, Any]]:
    try:
        rows = bpmis_client.list_issues_for_version(version_id) if hasattr(bpmis_client, "list_issues_for_version") else []
    except Exception as error:  # noqa: BLE001
        raise VersionPlanSyncUpstreamError(
            f"Version Plan sync aborted because BPMIS version issue lookup failed for '{version_id}'."
        ) from error
    return [row for row in rows or [] if isinstance(row, dict)]


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
    return sorted(
        normalized,
        key=lambda row: (
            _priority_rank(row.get("priority")),
            _safe_int(row.get("sort_order"), 0),
            _pm_sort_key(row.get("pm")),
            str(row.get("feature") or ""),
        ),
    )


def _sort_synced_rows(rows: Any) -> list[dict[str, Any]]:
    normalized = _normalize_synced_rows(rows)
    return sorted(
        normalized,
        key=lambda row: (
            _priority_rank(row.get("priority")),
            _pm_sort_key(row.get("pm")),
            _safe_int(row.get("sort_order"), 0),
            str(row.get("jira_id") or ""),
        ),
    )


def _pm_sort_key(value: Any) -> str:
    values = _normalize_pm_values(value)
    return " / ".join(values).casefold()


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
        if text_key in {"tbc"}:
            continue
        canonical = VERSION_PLAN_PM_ALIASES.get(text_key) or allowed.get(text_key, text)
        if canonical not in VERSION_PLAN_PM_OPTIONS:
            continue
        if canonical not in normalized:
            normalized.append(canonical)
            break
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
    value = _extract_first_text(row, "jira_id", "ticket_key", "jiraKey", "ticketKey", "jiraIssueKey", "issueKey", "key")
    if value:
        return value
    link = _extract_first_text(row, "jiraLink", "ticketLink", "jiraUrl", "url", "link")
    match = re.search(r"([A-Z][A-Z0-9]+-\d+)", link)
    return match.group(1) if match else ""


def _extract_jira_link(row: dict[str, Any], jira_id: str) -> str:
    link = _extract_first_text(row, "jira_link", "ticket_link", "jiraLink", "ticketLink", "jiraUrl", "url", "link")
    return _canonical_jira_link(link, jira_id)


def _canonical_jira_link(link: Any, jira_id: str) -> str:
    jira_key = str(jira_id or "").strip()
    raw_link = str(link or "").strip()
    if jira_key:
        preferred = f"https://jira.shopee.io/browse/{jira_key}"
        if not raw_link:
            return preferred
        normalized = raw_link.lower()
        if "jira.shopee.io/browse/" in normalized:
            return preferred
        if re.search(rf"(^|[^A-Z0-9]){re.escape(jira_key)}($|[^A-Z0-9])", raw_link, flags=re.IGNORECASE):
            return preferred
    return raw_link


def _extract_pm(row: dict[str, Any]) -> list[str]:
    value = (
        row.get("pm_email")
        or row.get("jiraRegionalPmPicId")
        or row.get("regionalPmPic")
        or row.get("productManager")
        or row.get("pm")
        or row.get("reporter")
        or row.get("reporterEmail")
        or row.get("reporter_email")
    )
    people = _flatten_people(value)
    return _normalize_pm_values(people)


def _extract_task_type(row: dict[str, Any]) -> str:
    return _extract_first_text(
        row,
        "task_type",
        "taskType",
        "taskTypeId",
        "task_type_label",
        "taskTypeLabel",
        "issueTaskType",
    )


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


def _extract_market_from_jira_board(row: dict[str, Any], jira_id: str = "") -> str:
    return _market_from_jira_board(_extract_jira_board(row) or jira_id)


def _extract_productization_efforts(row: dict[str, Any], jira_id: str = "") -> str:
    return "Y" if _jira_board_is_productization(_extract_jira_board(row) or jira_id) else "N"


def _extract_component(row: dict[str, Any]) -> str:
    components = _extract_all_components(row)
    return ", ".join(components) if components else ""


def _extract_all_components(row: dict[str, Any]) -> list[str]:
    for key in ("components", "component", "componentId"):
        if key not in row:
            continue
        value = row.get(key)
        names = _stringify_component_value(value)
        if names:
            return names
    return []


def _stringify_component_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, dict):
        for child_key in ("label", "name", "displayName"):
            child = str(value.get(child_key) or "").strip()
            if child:
                return [child]
        return []
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(_stringify_component_value(item))
        return result
    text = str(value or "").strip()
    return [text] if text else []


def _extract_release_version(row: dict[str, Any]) -> str:
    names = _collect_all_release_version_names(row)
    normalized = _normalize_and_dedu_release_version_names(names)
    return ", ".join(normalized) if normalized else ""


def _collect_all_release_version_names(row: dict[str, Any]) -> list[str]:
    names: list[str] = []
    text = _extract_first_text(
        row,
        "fix_version_name",
        "fixVersionName",
        "fixVersion",
        "fixVersions",
        "version",
        "versions",
    )
    if text:
        names.extend(part.strip() for part in text.split(",") if part.strip())
    for name in _row_jira_version_names(row):
        if name and name not in names:
            names.append(name)
    return names


def _normalize_and_dedu_release_version_names(names: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw_name in names:
        for part in str(raw_name or "").split(","):
            normalized = _normalize_release_version_name(part.strip())
            if not normalized:
                continue
            key = normalized.casefold()
            if key not in seen:
                seen.add(key)
                result.append(normalized)
    return sorted(result)


def _normalize_release_version_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    if text.lower().startswith("af_"):
        return "AF_" + text[3:]
    if text.startswith("v1.0."):
        return "AF_" + text
    return text


def _extract_jira_board(row: dict[str, Any]) -> str:
    value = _extract_first_text(
        row,
        "jira_board",
        "jiraBoard",
        "jiraBoardName",
        "board",
        "boardName",
        "jira_project_key",
        "jiraProjectKey",
        "projectKey",
        "project_key",
        "jira_project",
        "jiraProject",
        "project",
        "projectName",
    )
    if value:
        return value
    raw = row.get("raw_response")
    if isinstance(raw, dict):
        return _extract_jira_board(raw)
    return ""


def _market_from_jira_board(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    board_key = text.split("-", 1)[0].strip().upper()
    normalized = re.sub(r"[^A-Z0-9]+", " ", text.upper())
    if _jira_board_is_productization(text):
        return "Regional"
    if board_key in {"SGDB", "SG"} or re.search(r"\b(SG|SINGAPORE)\b", normalized):
        return "SG"
    if board_key in {"SPPHDB", "PHDB", "PH"} or re.search(r"\b(PH|PHILIPPINES)\b", normalized):
        return "PH"
    if board_key in {"SPDBK", "IDDB", "ID"} or re.search(r"\b(ID|INDONESIA)\b", normalized):
        return "ID"
    if board_key in {"REG", "REGIONAL"} or re.search(r"\b(REG|REGIONAL)\b", normalized):
        return "Regional"
    return ""


def _jira_board_is_productization(value: Any) -> bool:
    text = str(value or "").strip()
    board_key = text.split("-", 1)[0].strip().upper()
    normalized = re.sub(r"[^A-Z0-9]+", " ", text.upper())
    return board_key in {"SPDBP", "DBP"} or "PRODUCTIZATION" in normalized


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


def _normalize_audit_log(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    entries = [_normalize_audit_entry(item) for item in value if isinstance(item, dict)]
    return entries[-VERSION_PLAN_AUDIT_LIMIT:]


def _normalize_audit_entry(entry: dict[str, Any]) -> dict[str, Any]:
    actor = entry.get("actor") if isinstance(entry.get("actor"), dict) else {}
    details = entry.get("details") if isinstance(entry.get("details"), dict) else {}
    return {
        "audit_id": str(entry.get("audit_id") or f"audit-{uuid.uuid4().hex}").strip(),
        "timestamp": str(entry.get("timestamp") or "").strip(),
        "action": str(entry.get("action") or "").strip(),
        "actor": {
            "email": str(actor.get("email") or "").strip().lower(),
            "name": str(actor.get("name") or "").strip(),
        },
        "details": _sanitize_audit_value(details),
    }


def _audit_entry(*, action: str, actor: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
    normalized_actor = {
        "email": str(actor.get("email") or "").strip().lower(),
        "name": str(actor.get("name") or "").strip(),
    }
    return {
        "audit_id": f"audit-{uuid.uuid4().hex}",
        "timestamp": _now_text(),
        "action": str(action or "").strip(),
        "actor": normalized_actor,
        "details": _sanitize_audit_value(details),
    }


def _sanitize_audit_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_audit_value(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_sanitize_audit_value(item) for item in value[:50]]
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    text = str(value)
    return text[:500]


def _now_text(now: datetime | None = None) -> str:
    current = now or datetime.now(VERSION_PLAN_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=VERSION_PLAN_TIMEZONE)
    return current.astimezone(VERSION_PLAN_TIMEZONE).isoformat()


def _display_text(now: datetime | None = None) -> str:
    # Unified portal display format: YYYY-MM-DD HH:MM:SS (GMT+8).
    current = now or datetime.now(VERSION_PLAN_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=VERSION_PLAN_TIMEZONE)
    return current.astimezone(VERSION_PLAN_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S (GMT+8)")


def _next_sort_order(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    return max(_safe_int(row.get("sort_order"), 0) for row in rows) + 1


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
