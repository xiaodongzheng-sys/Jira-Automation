from __future__ import annotations

import re
from typing import Any, Callable


def serialize_productization_version_candidate(row: dict[str, Any]) -> dict[str, str]:
    version_id = str(row.get("id") or row.get("versionId") or "").strip()
    version_name = (
        str(row.get("fullName") or row.get("name") or row.get("versionName") or row.get("label") or "").strip()
    )
    market = coerce_display_text(row.get("marketId") or row.get("market") or row.get("country"))
    label = version_name
    if market:
        label = f"{version_name} · {market}"
    return {
        "version_id": version_id,
        "version_name": version_name,
        "market": market,
        "label": label,
    }


def normalize_productization_issue_row(
    row: dict[str, Any],
    *,
    description_formatter: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    ticket_key = extract_first_text(
        row,
        "jiraKey",
        "ticketKey",
        "jiraIssueKey",
        "issueKey",
        "key",
    )
    ticket_link = normalize_productization_ticket_url(
        extract_first_text(row, "jiraLink", "ticketLink", "jiraUrl", "url", "link")
    )
    if not ticket_key:
        ticket_key = extract_issue_key_from_text(ticket_link)
    if not ticket_link and ticket_key:
        ticket_link = f"{jira_browse_base_url()}{ticket_key}"

    formatter = description_formatter or (lambda value: str(value or "").strip())
    return {
        "jira_ticket_number": ticket_key or "-",
        "jira_ticket_url": ticket_link or "",
        "feature_summary": extract_first_text(row, "summary", "title", "jiraSummary") or "-",
        "detailed_feature": formatter(extract_first_text(row, "desc", "description", "jiraDescription")),
        "detailed_feature_source": "jira_description",
        "pm": extract_person_display(
            extract_first_value(row, "jiraRegionalPmPicId", "regionalPmPic", "productManager", "pm", "regionalPm")
        )
        or "-",
        "prd_links": extract_link_values(
            extract_first_value(row, "jiraPrdLink", "prdLink", "prdLinks", "prd", "brdLink")
        ),
    }


def filter_productization_issue_rows_for_pm_team(
    rows: list[dict[str, Any]],
    config_data: dict[str, Any],
    *,
    show_all_before_team_filtering: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    allowed_components = productization_allowed_components_for_pm_team(config_data)
    if show_all_before_team_filtering:
        return rows, {"team_filter_applied": False, "show_all_before_team_filtering": True}
    if not allowed_components:
        return rows, {"team_filter_applied": False, "show_all_before_team_filtering": False}
    filtered_rows = [row for row in rows if productization_issue_matches_components(row, allowed_components)]
    return filtered_rows, {"team_filter_applied": True, "show_all_before_team_filtering": False}


def productization_allowed_components_for_pm_team(config_data: dict[str, Any]) -> set[str]:
    pm_team = str(config_data.get("pm_team", "") or "").strip().upper()
    if pm_team == "AF":
        return {"dbp-anti-fraud", "anti-fraud"}
    return set()


def productization_issue_matches_components(row: dict[str, Any], allowed_components: set[str]) -> bool:
    issue_components = extract_productization_issue_components(row)
    return bool(issue_components and issue_components.intersection(allowed_components))


def extract_productization_issue_components(row: dict[str, Any]) -> set[str]:
    raw_value = extract_first_value(
        row,
        "componentId",
        "component",
        "components",
        "jiraComponent",
        "jiraComponentId",
    )
    flattened = flatten_productization_component_values(raw_value)
    return {component.lower() for component in flattened if component}


def flatten_productization_component_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        parts = [part.strip() for part in re.split(r"[;,/|]", text) if part.strip()]
        return parts or [text]
    if isinstance(value, dict):
        parts: list[str] = []
        for key in ("displayName", "name", "label", "value", "fullName", "id"):
            text = str(value.get(key) or "").strip()
            if text:
                parts.append(text)
        return parts
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            parts.extend(flatten_productization_component_values(item))
        return parts
    return [str(value).strip()]


def normalize_productization_ticket_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    issue_key = extract_issue_key_from_text(text)
    if issue_key:
        return f"{jira_browse_base_url()}{issue_key}"
    return text


def extract_first_value(row: dict[str, Any], *keys: str) -> Any:
    containers = [row]
    for nested_key in ("fields", "mapping", "data", "detail", "row"):
        nested = row.get(nested_key)
        if isinstance(nested, dict):
            containers.append(nested)

    for key in keys:
        lowered_key = key.lower()
        for container in containers:
            for candidate_key, value in container.items():
                if str(candidate_key).lower() == lowered_key:
                    return value
    return None


def extract_first_text(row: dict[str, Any], *keys: str) -> str:
    value = extract_first_value(row, *keys)
    return coerce_display_text(value)


def coerce_display_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("displayName", "name", "emailAddress", "label", "value", "fullName", "id"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [coerce_display_text(item) for item in value]
        parts = [part for part in parts if part]
        return ", ".join(parts)
    return str(value).strip()


def extract_person_display(value: Any) -> str:
    if isinstance(value, list):
        people = [extract_person_display(item) for item in value]
        people = [person for person in people if person]
        return ", ".join(people)
    if isinstance(value, dict):
        for key in ("displayName", "name", "emailAddress", "label", "username", "value"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""
    return coerce_display_text(value)


def extract_link_values(value: Any) -> list[dict[str, str]]:
    links = flatten_links(value)
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append({"label": link, "url": link})
    return deduped


def flatten_links(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        links: list[str] = []
        for item in value:
            links.extend(flatten_links(item))
        return links
    if isinstance(value, dict):
        links: list[str] = []
        for key in ("url", "link", "href", "value"):
            links.extend(flatten_links(value.get(key)))
        return links
    text = str(value).strip()
    if not text:
        return []
    matches = re.findall(r"https?://[^\s,]+", text)
    if matches:
        return matches
    return [text] if text.startswith("http://") or text.startswith("https://") else []


def extract_issue_key_from_text(value: str) -> str:
    match = re.search(r"\b([A-Z][A-Z0-9]+-\d+)\b", value or "")
    return match.group(1) if match else ""


def jira_browse_base_url() -> str:
    return "https://jira.shopee.io/browse/"
