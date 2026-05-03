from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from bpmis_jira_tool.user_config import DB_FILE


DEFAULT_PRIORITY_KEYWORDS = (
    "上线",
    "紧急",
    "延期",
    "复盘",
    "BSP",
    "OJK",
    "blocked",
    "approval",
    "risk",
    "MAS",
    "ITC",
    "launch",
    "delay",
    "incident",
)
REPORT_INTELLIGENCE_CONFIG_KEY = "report_intelligence_config"
TEAM_DASHBOARD_CONFIG_KEY = "team_dashboard"
MAX_MATCHED_VIPS = 12
MAX_MATCHED_KEYWORDS = 18
MAX_MATCHED_KEY_PROJECTS = 16
MAX_MONTHLY_SIDECAR_ITEMS = 80


def default_report_intelligence_config() -> dict[str, Any]:
    return {
        "vip_people": [],
        "priority_keywords": list(DEFAULT_PRIORITY_KEYWORDS),
        "noise": {
            "seatalk_group_blacklist": [],
            "gmail_sender_blacklist": [],
            "gmail_subject_hints": [],
        },
    }


def normalize_report_intelligence_config(config: Any) -> dict[str, Any]:
    raw = config if isinstance(config, dict) else {}
    default = default_report_intelligence_config()
    return {
        "vip_people": _normalize_vip_people(raw.get("vip_people")),
        "priority_keywords": _dedupe_strings(raw.get("priority_keywords"), default["priority_keywords"]),
        "noise": _normalize_noise_config(raw.get("noise")),
    }


def report_intelligence_from_team_config(config: Any) -> dict[str, Any]:
    raw = config.get(REPORT_INTELLIGENCE_CONFIG_KEY) if isinstance(config, dict) else {}
    return normalize_report_intelligence_config(raw)


def load_team_dashboard_config_from_data_root(data_root: Path) -> dict[str, Any]:
    root = Path(data_root)
    db_path = root / DB_FILE
    if not db_path.exists():
        return {}
    try:
        with sqlite3.connect(db_path) as connection:
            row = connection.execute(
                "SELECT config_json FROM team_dashboard_configs WHERE config_key = ?",
                (TEAM_DASHBOARD_CONFIG_KEY,),
            ).fetchone()
    except sqlite3.Error:
        return {}
    if not row:
        return {}
    try:
        payload = json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def load_report_intelligence_config_from_data_root(data_root: Path) -> dict[str, Any]:
    return report_intelligence_from_team_config(load_team_dashboard_config_from_data_root(data_root))


def key_project_candidates_from_team_config(config: Any) -> list[dict[str, Any]]:
    raw_config = config if isinstance(config, dict) else {}
    task_cache = raw_config.get("task_cache") if isinstance(raw_config.get("task_cache"), dict) else {}
    teams = task_cache.get("teams") if isinstance(task_cache.get("teams"), dict) else {}
    candidates: dict[str, dict[str, Any]] = {}
    for team in teams.values():
        if not isinstance(team, dict):
            continue
        for section_key in ("under_prd", "pending_live"):
            for project in team.get(section_key) or []:
                if not isinstance(project, dict) or not project.get("is_key_project"):
                    continue
                bpmis_id = str(project.get("bpmis_id") or "").strip()
                project_name = str(project.get("project_name") or "").strip()
                key = bpmis_id or project_name.casefold()
                if not key:
                    continue
                candidate = candidates.setdefault(
                    key,
                    {
                        "bpmis_id": bpmis_id,
                        "project_name": project_name,
                        "aliases": [],
                        "jira_ids": [],
                    },
                )
                _append_unique(candidate["aliases"], project.get("market"))
                _append_unique(candidate["aliases"], project.get("priority"))
                for ticket in project.get("jira_tickets") or []:
                    if not isinstance(ticket, dict):
                        continue
                    _append_unique(candidate["jira_ids"], ticket.get("jira_id") or ticket.get("issue_id"))
                    _append_unique(candidate["aliases"], ticket.get("jira_title"))
    return list(candidates.values())


def match_report_intelligence(
    text: str,
    *,
    config: Any,
    key_projects: list[dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]] | list[str]]:
    normalized = normalize_report_intelligence_config(config)
    source_text = str(text or "")
    lowered = source_text.casefold()
    matched_vips: list[dict[str, Any]] = []
    for vip in normalized["vip_people"]:
        terms = [
            vip.get("display_name"),
            *(vip.get("emails") or []),
            *(vip.get("seatalk_ids") or []),
            *(vip.get("aliases") or []),
        ]
        if _any_term_matches(lowered, terms):
            matched_vips.append(
                {
                    "display_name": vip.get("display_name") or "",
                    "role_tags": vip.get("role_tags") or [],
                }
            )
        if len(matched_vips) >= MAX_MATCHED_VIPS:
            break

    matched_keywords = [
        keyword
        for keyword in normalized["priority_keywords"]
        if _term_matches(lowered, keyword)
    ][:MAX_MATCHED_KEYWORDS]

    matched_key_projects: list[dict[str, Any]] = []
    for project in key_projects or []:
        if not isinstance(project, dict):
            continue
        terms = [
            project.get("bpmis_id"),
            project.get("project_name"),
            *(project.get("jira_ids") or []),
            *(project.get("aliases") or []),
        ]
        if _any_term_matches(lowered, terms):
            matched_key_projects.append(
                {
                    "bpmis_id": str(project.get("bpmis_id") or "").strip(),
                    "project_name": str(project.get("project_name") or "").strip(),
                    "jira_ids": _dedupe_strings(project.get("jira_ids"), []),
                }
            )
        if len(matched_key_projects) >= MAX_MATCHED_KEY_PROJECTS:
            break

    return {
        "matched_vips": matched_vips,
        "matched_keywords": matched_keywords,
        "matched_key_projects": matched_key_projects,
    }


def build_daily_match_summary(matches: dict[str, Any]) -> str:
    lines: list[str] = []
    vips = matches.get("matched_vips") if isinstance(matches, dict) else []
    keywords = matches.get("matched_keywords") if isinstance(matches, dict) else []
    key_projects = matches.get("matched_key_projects") if isinstance(matches, dict) else []
    if vips:
        labels = []
        for vip in vips[:MAX_MATCHED_VIPS]:
            name = str((vip or {}).get("display_name") or "").strip()
            roles = [str(item).strip() for item in ((vip or {}).get("role_tags") or []) if str(item).strip()]
            labels.append(f"{name} ({', '.join(roles)})" if roles else name)
        lines.append(f"Today's matched VIPs: {', '.join(item for item in labels if item)}.")
    if keywords:
        lines.append(f"Today's matched priority keywords: {', '.join(str(item) for item in keywords[:MAX_MATCHED_KEYWORDS])}.")
    if key_projects:
        labels = []
        for project in key_projects[:MAX_MATCHED_KEY_PROJECTS]:
            name = str((project or {}).get("project_name") or "").strip()
            bpmis_id = str((project or {}).get("bpmis_id") or "").strip()
            jira_ids = [str(item).strip() for item in ((project or {}).get("jira_ids") or []) if str(item).strip()]
            labels.append(" / ".join(item for item in [bpmis_id, name, ", ".join(jira_ids[:4])] if item))
        lines.append(f"Today's matched key projects: {', '.join(item for item in labels if item)}.")
    return "\n".join(lines)


def filter_text_by_noise(text: str, *, config: Any, source: str) -> str:
    normalized = normalize_report_intelligence_config(config)
    noise = normalized.get("noise") or {}
    if source == "seatalk":
        terms = noise.get("seatalk_group_blacklist") or []
    else:
        terms = []
    clean_terms = [str(item).strip().casefold() for item in terms if str(item).strip()]
    if not clean_terms:
        return str(text or "")
    kept = []
    for line in str(text or "").splitlines():
        lowered = line.casefold()
        if any(term in lowered for term in clean_terms):
            continue
        kept.append(line)
    return "\n".join(kept)


def is_gmail_noise(headers: dict[str, str], *, config: Any) -> bool:
    normalized = normalize_report_intelligence_config(config)
    noise = normalized.get("noise") or {}
    sender = _first_contact_address(headers.get("from", ""))
    subject = str(headers.get("subject") or "").strip().casefold()
    if sender and sender in {item.casefold() for item in noise.get("gmail_sender_blacklist") or []}:
        return True
    return any(str(hint or "").strip().casefold() in subject for hint in noise.get("gmail_subject_hints") or [] if str(hint or "").strip())


def build_monthly_evidence_sidecar(
    *,
    seatalk_history_text: str,
    key_projects: list[dict[str, Any]],
    prd_sources: list[dict[str, Any]],
    config: Any,
) -> list[dict[str, Any]]:
    candidates = _monthly_key_project_candidates(key_projects)
    items: list[dict[str, Any]] = []
    for line in str(seatalk_history_text or "").splitlines():
        if len(items) >= MAX_MONTHLY_SIDECAR_ITEMS:
            break
        matches = match_report_intelligence(line, config=config, key_projects=candidates)
        if _has_matches(matches):
            items.append(_sidecar_item(source="seatalk", text=line, matches=matches))
    for project in key_projects:
        if len(items) >= MAX_MONTHLY_SIDECAR_ITEMS:
            break
        text = json.dumps(project, ensure_ascii=False)
        matches = match_report_intelligence(text, config=config, key_projects=candidates)
        if _has_matches(matches):
            items.append(_sidecar_item(source="key_project_jira", text=_project_label(project), matches=matches))
    for source in prd_sources:
        if len(items) >= MAX_MONTHLY_SIDECAR_ITEMS:
            break
        text = json.dumps(source, ensure_ascii=False)
        matches = match_report_intelligence(text, config=config, key_projects=candidates)
        if _has_matches(matches):
            items.append(_sidecar_item(source="prd", text=str(source.get("title") or source.get("url") or ""), matches=matches))
    return items


def _normalize_vip_people(value: Any) -> list[dict[str, Any]]:
    people = value if isinstance(value, list) else []
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in people:
        if not isinstance(item, dict):
            continue
        display_name = str(item.get("display_name") or item.get("name") or "").strip()
        emails = _dedupe_strings(item.get("emails"), [])
        seatalk_ids = _dedupe_strings(item.get("seatalk_ids"), [])
        aliases = _dedupe_strings(item.get("aliases"), [])
        role_tags = _dedupe_strings(item.get("role_tags") or item.get("roles"), [])
        key = (display_name or ",".join(emails) or ",".join(seatalk_ids) or ",".join(aliases)).casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "display_name": display_name,
                "role_tags": role_tags,
                "emails": [email.lower() for email in emails],
                "seatalk_ids": seatalk_ids,
                "aliases": aliases,
            }
        )
    return normalized[:200]


def _normalize_noise_config(value: Any) -> dict[str, list[str]]:
    raw = value if isinstance(value, dict) else {}
    return {
        "seatalk_group_blacklist": _dedupe_strings(raw.get("seatalk_group_blacklist"), []),
        "gmail_sender_blacklist": [item.lower() for item in _dedupe_strings(raw.get("gmail_sender_blacklist"), [])],
        "gmail_subject_hints": _dedupe_strings(raw.get("gmail_subject_hints"), []),
    }


def _dedupe_strings(value: Any, fallback: list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(value, str):
        raw_items = re.split(r"[\n,;]+", value)
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = list(fallback)
    seen: set[str] = set()
    result: list[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result[:500]


def _append_unique(items: list[str], value: Any) -> None:
    text = str(value or "").strip()
    if text and text not in items:
        items.append(text)


def _term_matches(lowered_text: str, term: Any) -> bool:
    clean = str(term or "").strip()
    if not clean:
        return False
    return clean.casefold() in lowered_text


def _any_term_matches(lowered_text: str, terms: list[Any]) -> bool:
    return any(_term_matches(lowered_text, term) for term in terms)


def _first_contact_address(header_value: str) -> str:
    match = re.search(r"<([^>]+)>", header_value or "")
    if match:
        return match.group(1).strip().lower()
    text = str(header_value or "").strip().lower()
    if "@" in text:
        return text.split()[0].strip(",;")
    return ""


def _monthly_key_project_candidates(key_projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for project in key_projects:
        jira_ids = []
        aliases = []
        for ticket in project.get("jira_tickets") or []:
            if not isinstance(ticket, dict):
                continue
            _append_unique(jira_ids, ticket.get("jira_id") or ticket.get("issue_id"))
            _append_unique(aliases, ticket.get("jira_title"))
        candidates.append(
            {
                "bpmis_id": str(project.get("bpmis_id") or "").strip(),
                "project_name": str(project.get("project_name") or "").strip(),
                "jira_ids": jira_ids,
                "aliases": aliases,
            }
        )
    return candidates


def _has_matches(matches: dict[str, Any]) -> bool:
    return bool(matches.get("matched_vips") or matches.get("matched_keywords") or matches.get("matched_key_projects"))


def _sidecar_item(*, source: str, text: str, matches: dict[str, Any]) -> dict[str, Any]:
    keywords = [str(item).casefold() for item in matches.get("matched_keywords") or []]
    risk_level = "high" if any(item in {"bsp", "ojk", "mas", "risk", "blocked", "延期", "delay", "incident"} for item in keywords) else "medium"
    return {
        "source": source,
        "matched_vips": matches.get("matched_vips") or [],
        "matched_keywords": matches.get("matched_keywords") or [],
        "matched_key_projects": matches.get("matched_key_projects") or [],
        "risk_level": risk_level,
        "evidence": str(text or "").strip()[:500],
    }


def _project_label(project: dict[str, Any]) -> str:
    return " / ".join(
        item
        for item in [
            str(project.get("bpmis_id") or "").strip(),
            str(project.get("project_name") or "").strip(),
            str(project.get("market") or "").strip(),
        ]
        if item
    )
