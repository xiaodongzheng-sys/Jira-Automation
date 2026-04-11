from __future__ import annotations

import json
import re
from pathlib import Path

from bpmis_jira_tool.models import FieldMapping


CONFIG_FILE = "jira_web_config.json"
CONFIGURED_FIELDS = [
    "Market",
    "Task Type",
    "Summary",
    "PRD Link/s",
    "Fix Version",
    "Component",
    "Priority",
    "Assignee",
    "Product Manager",
    "Dev PIC",
    "QA PIC",
    "Reporter",
    "Biz PIC",
    "Need UAT",
]
MARKET_KEYS = ["ID", "SG", "PH", "Regional"]
SOURCE_FIELDS = {
    "spreadsheet_link": "",
    "input_tab_name": "Input",
    "issue_id_header": "Issue ID",
    "jira_ticket_link_header": "Jira Ticket Link",
}
HEADER_FIELDS = {
    "Market": "market_header",
    "Summary": "summary_header",
    "PRD Link/s": "prd_links_header",
}
MARKET_CHOICE_FIELDS = {
    "Component": "component_by_market",
    "Need UAT": "need_uat_by_market",
}
DIRECT_FIELDS = {
    "Task Type": "task_type_value",
    "Fix Version": "fix_version_value",
    "Priority": "priority_value",
    "Assignee": "assignee_value",
    "Product Manager": "product_manager_value",
    "Dev PIC": "dev_pic_value",
    "QA PIC": "qa_pic_value",
    "Reporter": "reporter_value",
    "Biz PIC": "biz_pic_value",
}
DEFAULT_DIRECT_VALUES = {
    "task_type_value": "Feature",
}
QUOTED_OPTION_PATTERN = re.compile(r'"([^"]+)"')


class WebConfigStore:
    def __init__(self, project_root: Path):
        self.path = project_root / CONFIG_FILE

    def load(self) -> dict[str, object] | None:
        if not self.path.exists():
            return None
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return self._normalize(data)

    def save(self, data: dict[str, object]) -> dict[str, object]:
        normalized = self._normalize(data)
        self.path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
        return normalized

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()

    def build_field_mappings(self, data: dict[str, object]) -> list[FieldMapping]:
        mappings: list[FieldMapping] = []

        for jira_field, key in HEADER_FIELDS.items():
            header = str(data.get(key, "")).strip()
            if header:
                mappings.append(FieldMapping(jira_field=jira_field, source=f"column:{header}"))

        for jira_field, key in MARKET_CHOICE_FIELDS.items():
            market_choices = data.get(key, {})
            normalized_market_choices: dict[str, str] = {}
            for market in MARKET_KEYS:
                raw_value = market_choices.get(market, "") if isinstance(market_choices, dict) else ""
                value = str(raw_value).strip()
                if value:
                    normalized_market_choices[market] = value
            if normalized_market_choices:
                mappings.append(
                    FieldMapping(
                        jira_field=jira_field,
                        source=f"market_choices:{json.dumps(normalized_market_choices, ensure_ascii=False)}",
                    )
                )

        for jira_field, key in DIRECT_FIELDS.items():
            value = str(data.get(key, "")).strip()
            if value:
                mappings.append(FieldMapping(jira_field=jira_field, source=f"literal:{value}"))

        return mappings

    def derive_from_sheet(
        self,
        mappings: list[FieldMapping],
        headers: list[str],
    ) -> dict[str, object]:
        header_lookup = {self._column_letter(index + 1): header for index, header in enumerate(headers)}
        result = self._normalize({})

        for mapping in mappings:
            field = mapping.jira_field.strip()
            source = mapping.source.strip()
            lowered = source.lower()

            if field in HEADER_FIELDS:
                header = ""
                if source.startswith("column:"):
                    header = source.partition(":")[2].strip()
                else:
                    column_match = re.search(r'column\s+([a-z]+)', source, re.I)
                    if column_match:
                        header = header_lookup.get(column_match.group(1).upper(), "")
                if header:
                    result[HEADER_FIELDS[field]] = header
                continue

            if field in MARKET_CHOICE_FIELDS:
                if source.startswith("market_choices:"):
                    raw = source.partition(":")[2].strip()
                    try:
                        parsed = json.loads(raw) if raw else {}
                    except json.JSONDecodeError:
                        parsed = {}
                    result[MARKET_CHOICE_FIELDS[field]] = self._normalize_market_choice_map(parsed)
                continue

            if field in DIRECT_FIELDS:
                if source.startswith("literal:"):
                    result[DIRECT_FIELDS[field]] = source.partition(":")[2]
                else:
                    result[DIRECT_FIELDS[field]] = source

        return result

    def _normalize(self, data: dict[str, object]) -> dict[str, object]:
        normalized: dict[str, object] = {}

        for key, default in SOURCE_FIELDS.items():
            normalized[key] = str(data.get(key, default)).strip()

        for key in HEADER_FIELDS.values():
            normalized[key] = str(data.get(key, "")).strip()

        for key in DIRECT_FIELDS.values():
            normalized[key] = str(data.get(key, DEFAULT_DIRECT_VALUES.get(key, ""))).strip()

        for key in MARKET_CHOICE_FIELDS.values():
            normalized[key] = self._normalize_market_choice_map(data.get(key, {}))

        return normalized

    @staticmethod
    def _normalize_market_choice_map(data: object) -> dict[str, list[str]]:
        normalized: dict[str, str] = {}
        raw_map = data if isinstance(data, dict) else {}
        for market in MARKET_KEYS:
            raw = raw_map.get(market, "")
            normalized[market] = str(raw).strip()
        return normalized

    @staticmethod
    def _column_letter(index: int) -> str:
        result = ""
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            result = chr(65 + remainder) + result
        return result
