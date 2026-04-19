from __future__ import annotations

import json
import re

from bpmis_jira_tool.errors import FieldResolutionError
from bpmis_jira_tool.models import FieldMapping, InputRow


TEMPLATE_PATTERN = re.compile(r"{{\s*(?P<header>[^}]+?)\s*}}")
INPUT_COLUMN_PATTERN = re.compile(r'^follow input tab column (?P<column>[a-z]+)$', re.I)
QUOTED_OPTION_PATTERN = re.compile(r'"([^"]+)"')
OPTIONAL_FIELDS = {"PRD Link/s", "Biz PIC", "Description"}
COMPONENT_DEFAULT_FIELD_KEYS = {
    "Fix Version": "fix_version",
    "Assignee": "assignee",
    "Dev PIC": "dev_pic",
    "QA PIC": "qa_pic",
}


def _resolve_template(template: str, row: InputRow) -> str:
    def replace(match: re.Match[str]) -> str:
        header = match.group("header")
        return row._get_first(header)

    return TEMPLATE_PATTERN.sub(replace, template)


def _normalize_source_text(source: str) -> str:
    return " ".join(source.strip().split())


def _extract_follow_input_column(source: str) -> str | None:
    match = re.search(
        r'follow\s+"?input"?\s+(?:tab|sheet)\s+column\s+([a-z]+)',
        source,
        re.I,
    )
    if match:
        return match.group(1).upper()
    return None


def _extract_select_fallbacks(source: str) -> str | None:
    quoted = [item.strip() for item in QUOTED_OPTION_PATTERN.findall(source) if item.strip()]
    if len(quoted) >= 2 and re.search(r"if\s+this\s+option\s+is\s+not\s+available", source, re.I):
        return "|".join(quoted)
    return None


def _resolve_special_mapping(mapping: FieldMapping, row: InputRow) -> str | None:
    source = _normalize_source_text(mapping.source)
    lowered = source.lower()

    input_column_match = INPUT_COLUMN_PATTERN.match(source)
    if input_column_match:
        return row.get_by_column_letter(input_column_match.group("column"))

    input_column = _extract_follow_input_column(source)
    if input_column:
        return row.get_by_column_letter(input_column)

    if lowered in {'follow "input" sheet column b', 'follow "input" tab column b'}:
        return row.get_by_column_letter("B")

    if lowered in {'follow "input" sheet column d', 'follow "input" tab column d'}:
        return row.get_by_column_letter("D")

    if lowered == 'follow input tab column b, start with "[feature]" in front':
        summary = row.get_by_column_letter("B")
        return f"[Feature] {summary}".strip() if summary else None

    select_fallbacks = _extract_select_fallbacks(source)
    if select_fallbacks:
        return select_fallbacks

    return None


def resolve_fields(
    mappings: list[FieldMapping],
    row: InputRow,
    optional_fields: set[str] | None = None,
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    effective_optional_fields = OPTIONAL_FIELDS | {field.strip() for field in (optional_fields or set()) if field.strip()}

    for mapping in mappings:
        source = mapping.source.strip()
        special_value = _resolve_special_mapping(mapping, row)

        if source.startswith("market_choices:"):
            raw = source.partition(":")[2].strip()
            try:
                market_map = json.loads(raw) if raw else {}
            except json.JSONDecodeError as error:
                raise FieldResolutionError(
                    f"Invalid market mapping for Jira field '{mapping.jira_field}'."
                ) from error

            market_value = resolved.get("Market", "").strip()
            value = str(market_map.get(market_value, "")).strip()
        elif source.startswith("component_routes:"):
            raw = source.partition(":")[2].strip()
            try:
                route_rules = json.loads(raw) if raw else []
            except json.JSONDecodeError as error:
                raise FieldResolutionError(
                    f"Invalid component routing rules for Jira field '{mapping.jira_field}'."
                ) from error

            market_value = resolved.get("Market", "").strip().lower()
            system_value = resolved.get("System", "").strip().lower()
            matched_rule = next(
                (
                    rule
                    for rule in route_rules
                    if str(rule.get("market", "")).strip().lower() == market_value
                    and str(rule.get("system", "")).strip().lower() == system_value
                ),
                None,
            )
            value = str((matched_rule or {}).get("component", "")).strip()
        elif source.startswith("component_defaults:"):
            raw = source.partition(":")[2].strip()
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError as error:
                raise FieldResolutionError(
                    f"Invalid component default rules for Jira field '{mapping.jira_field}'."
                ) from error

            component_value = resolved.get("Component", "").strip().lower()
            field_key = str(payload.get("field") or COMPONENT_DEFAULT_FIELD_KEYS.get(mapping.jira_field, "")).strip()
            matched_rule = next(
                (
                    rule
                    for rule in payload.get("rules", [])
                    if str(rule.get("component", "")).strip().lower() == component_value
                ),
                None,
            )
            value = str((matched_rule or {}).get(field_key, "")).strip()
        elif special_value is not None:
            value = special_value
        elif source.startswith("column:"):
            header = source.partition(":")[2].strip()
            value = row._get_first(header)
        elif source.startswith("literal:"):
            value = source.partition(":")[2]
        elif source.startswith("choices:"):
            value = source.partition(":")[2]
        elif source.startswith("template:"):
            value = _resolve_template(source.partition(":")[2], row).strip()
        elif source:
            value = row._get_first(source) or source.strip()
        else:
            value = row._get_first(mapping.jira_field)

        if not value:
            if mapping.jira_field.strip() in effective_optional_fields:
                continue
            raise FieldResolutionError(
                f"Could not resolve Jira field '{mapping.jira_field}' for sheet row {row.row_number}."
            )

        resolved[mapping.jira_field] = value

    return resolved
