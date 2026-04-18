from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.models import FieldMapping


CONFIG_FILE = "jira_web_config.json"
DB_FILE = "team_portal.db"
DEFAULT_SHEET_HEADERS = [
    "BPMIS ID",
    "Project Name",
    "Market",
    "BRD Link",
    "System",
    "Jira Title",
    "PRD Link",
    "Description",
    "Jira Ticket Link",
]
CONFIGURED_FIELDS = [
    "Market",
    "Task Type",
    "Summary",
    "PRD Link/s",
    "Description",
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
    "input_tab_name": "Projects",
    "bpmis_api_access_token": "",
    "issue_id_header": DEFAULT_SHEET_HEADERS[0],
    "jira_ticket_link_header": "Jira Ticket Link",
    "sync_pm_email": "",
    "sync_project_name_header": DEFAULT_SHEET_HEADERS[1],
    "sync_market_header": DEFAULT_SHEET_HEADERS[2],
    "sync_brd_link_header": DEFAULT_SHEET_HEADERS[3],
    "component_route_rules_text": "",
    "component_default_rules_text": "",
}
HEADER_FIELDS = {
    "Market": ("market_header", DEFAULT_SHEET_HEADERS[2]),
    "System": ("system_header", DEFAULT_SHEET_HEADERS[4]),
    "Summary": ("summary_header", DEFAULT_SHEET_HEADERS[5]),
    "PRD Link/s": ("prd_links_header", DEFAULT_SHEET_HEADERS[6]),
    "Description": ("description_header", DEFAULT_SHEET_HEADERS[7]),
}
MARKET_CHOICE_FIELDS = {
    "Need UAT": "need_uat_by_market",
}
LEGACY_MARKET_CHOICE_FIELDS = {
    "Component": "component_by_market",
}
DIRECT_FIELDS = {
    "Task Type": "task_type_value",
    "Priority": "priority_value",
    "Product Manager": "product_manager_value",
    "Reporter": "reporter_value",
    "Biz PIC": "biz_pic_value",
}
DEFAULT_DIRECT_VALUES = {
    "task_type_value": "Feature",
}
COMPONENT_ROUTED_DIRECT_FIELDS = {
    "Fix Version": "fix_version",
    "Assignee": "assignee",
    "Dev PIC": "dev_pic",
    "QA PIC": "qa_pic",
}


class WebConfigStore:
    ENCRYPTED_PREFIX = "enc:"
    ENCRYPTED_FIELDS = ("bpmis_api_access_token",)

    def __init__(self, data_root: Path, legacy_root: Path | None = None, encryption_key: str | None = None):
        self.root = data_root
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / CONFIG_FILE
        self.db_path = self.root / DB_FILE
        self.legacy_path = (legacy_root / CONFIG_FILE) if legacy_root else None
        self.encryption_key = (encryption_key or "").strip() or None
        self._fernet = Fernet(self.encryption_key.encode("utf-8")) if self.encryption_key else None
        self._ensure_db()

    def load(self, user_key: str | None = None) -> dict[str, object] | None:
        if user_key:
            row = self._fetch_row(user_key)
            if row is not None:
                return self._normalize(self._deserialize_config(json.loads(row)))
        if not self.path.exists():
            if self.legacy_path and self.legacy_path.exists():
                data = self._deserialize_config(json.loads(self.legacy_path.read_text(encoding="utf-8")))
                return self._normalize(data)
            return None
        data = self._deserialize_config(json.loads(self.path.read_text(encoding="utf-8")))
        return self._normalize(data)

    def save(self, data: dict[str, object], user_key: str | None = None) -> dict[str, object]:
        normalized = self._normalize(data)
        serialized = self._serialize_config(normalized)
        if user_key:
            self._upsert_row(user_key, serialized)
        else:
            self.path.write_text(json.dumps(serialized, indent=2), encoding="utf-8")
        return normalized

    def migrate(self, from_user_key: str, to_user_key: str) -> None:
        if from_user_key == to_user_key:
            return
        source = self._fetch_row(from_user_key)
        if source is None:
            return
        if self._fetch_row(to_user_key) is None:
            self._upsert_row(to_user_key, self._serialize_config(self._normalize(self._deserialize_config(json.loads(source)))))

    def clear(self, user_key: str | None = None) -> None:
        if user_key:
            with sqlite3.connect(self.db_path) as connection:
                connection.execute("DELETE FROM user_configs WHERE user_key = ?", (user_key,))
                connection.commit()
            return
        if self.path.exists():
            self.path.unlink()

    def build_field_mappings(self, data: dict[str, object]) -> list[FieldMapping]:
        mappings: list[FieldMapping] = []

        for jira_field, key in HEADER_FIELDS.items():
            if isinstance(key, tuple):
                key = key[0]
            header = str(data.get(key, "")).strip()
            if header:
                mappings.append(FieldMapping(jira_field=jira_field, source=f"column:{header}"))

        component_route_rules = self._parse_component_route_rules(str(data.get("component_route_rules_text", "")))
        if component_route_rules:
            system_header = str(data.get("system_header", "")).strip()
            if not system_header:
                raise ToolError("System Header is required when System + Market -> Component rules are configured.")
            mappings.append(
                FieldMapping(
                    jira_field="Component",
                    source=f"component_routes:{json.dumps(component_route_rules, ensure_ascii=False)}",
                )
            )
        else:
            market_choices = data.get(LEGACY_MARKET_CHOICE_FIELDS["Component"], {})
            normalized_market_choices: dict[str, str] = {}
            for market in MARKET_KEYS:
                raw_value = market_choices.get(market, "") if isinstance(market_choices, dict) else ""
                value = str(raw_value).strip()
                if value:
                    normalized_market_choices[market] = value
            if normalized_market_choices:
                mappings.append(
                    FieldMapping(
                        jira_field="Component",
                        source=f"market_choices:{json.dumps(normalized_market_choices, ensure_ascii=False)}",
                    )
                )

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

        component_default_rules = self._parse_component_default_rules(str(data.get("component_default_rules_text", "")))
        if component_route_rules and not component_default_rules:
            raise ToolError(
                "Component Defaults are required when System + Market -> Component routing is configured."
            )
        if component_route_rules and component_default_rules:
            routed_components = {rule["component"].strip().lower() for rule in component_route_rules if rule["component"].strip()}
            default_components = {rule["component"].strip().lower() for rule in component_default_rules if rule["component"].strip()}
            missing_components = sorted(component for component in routed_components if component not in default_components)
            if missing_components:
                raise ToolError(
                    "Component Defaults are missing these routed components: "
                    + ", ".join(missing_components)
                    + "."
                )
        for jira_field, field_key in COMPONENT_ROUTED_DIRECT_FIELDS.items():
            if component_default_rules:
                mappings.append(
                    FieldMapping(
                        jira_field=jira_field,
                        source=(
                            "component_defaults:"
                            + json.dumps(
                                {
                                    "field": field_key,
                                    "rules": component_default_rules,
                                },
                                ensure_ascii=False,
                            )
                        ),
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

            if field in HEADER_FIELDS:
                config_key = HEADER_FIELDS[field][0] if isinstance(HEADER_FIELDS[field], tuple) else HEADER_FIELDS[field]
                header = ""
                if source.startswith("column:"):
                    header = source.partition(":")[2].strip()
                else:
                    column_match = re.search(r'column\s+([a-z]+)', source, re.I)
                    if column_match:
                        header = header_lookup.get(column_match.group(1).upper(), "")
                if header:
                    result[config_key] = header
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

        if not normalized.get("sync_project_name_header"):
            normalized["sync_project_name_header"] = "Project Name"
        if not normalized.get("sync_market_header"):
            normalized["sync_market_header"] = "Market"
        if not normalized.get("sync_brd_link_header"):
            normalized["sync_brd_link_header"] = "BRD Link"

        for field_name, config_meta in HEADER_FIELDS.items():
            config_key, default_value = config_meta if isinstance(config_meta, tuple) else (config_meta, "")
            normalized[config_key] = str(data.get(config_key, default_value)).strip()

        if not normalized.get("market_header"):
            normalized["market_header"] = normalized.get("sync_market_header", "").strip()

        for key in ("component_route_rules_text", "component_default_rules_text"):
            normalized[key] = self._normalize_multiline_text(data.get(key, ""))

        for key in DIRECT_FIELDS.values():
            normalized[key] = str(data.get(key, DEFAULT_DIRECT_VALUES.get(key, ""))).strip()

        for key in MARKET_CHOICE_FIELDS.values():
            normalized[key] = self._normalize_market_choice_map(data.get(key, {}))

        for key in LEGACY_MARKET_CHOICE_FIELDS.values():
            normalized[key] = self._normalize_market_choice_map(data.get(key, {}))

        return normalized

    def _ensure_db(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS user_configs (
                    user_key TEXT PRIMARY KEY,
                    config_json TEXT NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.commit()

    def _fetch_row(self, user_key: str) -> str | None:
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT config_json FROM user_configs WHERE user_key = ?",
                (user_key,),
            ).fetchone()
        return row[0] if row else None

    def _upsert_row(self, user_key: str, config: dict[str, object]) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO user_configs (user_key, config_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_key) DO UPDATE SET
                    config_json = excluded.config_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (user_key, json.dumps(config, ensure_ascii=False)),
            )
            connection.commit()

    def _serialize_config(self, config: dict[str, object]) -> dict[str, object]:
        serialized = dict(config)
        for field in self.ENCRYPTED_FIELDS:
            raw_value = str(serialized.get(field, "") or "").strip()
            if not raw_value or raw_value.startswith(self.ENCRYPTED_PREFIX):
                continue
            if self._fernet is None:
                continue
            encrypted = self._fernet.encrypt(raw_value.encode("utf-8")).decode("utf-8")
            serialized[field] = f"{self.ENCRYPTED_PREFIX}{encrypted}"
        return serialized

    def _deserialize_config(self, config: dict[str, object]) -> dict[str, object]:
        deserialized = dict(config)
        for field in self.ENCRYPTED_FIELDS:
            raw_value = str(deserialized.get(field, "") or "").strip()
            if not raw_value.startswith(self.ENCRYPTED_PREFIX):
                continue
            if self._fernet is None:
                raise ToolError(
                    "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY is required to read saved BPMIS tokens in shared mode."
                )
            token = raw_value[len(self.ENCRYPTED_PREFIX) :]
            try:
                deserialized[field] = self._fernet.decrypt(token.encode("utf-8")).decode("utf-8")
            except InvalidToken as error:
                raise ToolError("Could not decrypt the saved BPMIS token. Check TEAM_PORTAL_CONFIG_ENCRYPTION_KEY.") from error
        return deserialized

    @staticmethod
    def _normalize_market_choice_map(data: object) -> dict[str, str]:
        normalized: dict[str, str] = {}
        raw_map = data if isinstance(data, dict) else {}
        for market in MARKET_KEYS:
            raw = raw_map.get(market, "")
            normalized[market] = str(raw).strip()
        return normalized

    @staticmethod
    def _normalize_multiline_text(data: object) -> str:
        text = str(data or "").replace("\r\n", "\n").replace("\r", "\n")
        lines = [line.rstrip() for line in text.split("\n")]
        return "\n".join(lines).strip()

    @staticmethod
    def _parse_component_route_rules(text: str) -> list[dict[str, str]]:
        rules: list[dict[str, str]] = []
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [part.strip() for part in line.split("|")]
            if len(parts) != 3 or any(not part for part in parts):
                raise ToolError(
                    f"Invalid System + Market -> Component rule on line {line_number}. "
                    "Use: System | Market | Component"
                )
            rules.append({"system": parts[0], "market": parts[1], "component": parts[2]})
        return rules

    @staticmethod
    def _parse_component_default_rules(text: str) -> list[dict[str, str]]:
        rules: list[dict[str, str]] = []
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [part.strip() for part in line.split("|")]
            if len(parts) != 5 or any(not part for part in parts):
                raise ToolError(
                    f"Invalid Component default rule on line {line_number}. "
                    "Use: Component | Assignee | Dev PIC | QA PIC | Fix Version"
                )
            rules.append(
                {
                    "component": parts[0],
                    "assignee": parts[1],
                    "dev_pic": parts[2],
                    "qa_pic": parts[3],
                    "fix_version": parts[4],
                }
            )
        return rules

    @staticmethod
    def _column_letter(index: int) -> str:
        result = ""
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            result = chr(65 + remainder) + result
        return result
