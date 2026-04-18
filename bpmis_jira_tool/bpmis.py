from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import requests

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError, BPMISNotConfiguredError
from bpmis_jira_tool.models import CreatedTicket, ProjectMatch


ISSUE_KEY_PATTERN = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")


class BPMISClient(ABC):
    @abstractmethod
    def ping(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def find_project(self, issue_id: str) -> ProjectMatch:
        raise NotImplementedError

    @abstractmethod
    def create_jira_ticket(self, project: ProjectMatch, fields: dict[str, str]) -> CreatedTicket:
        raise NotImplementedError

    @abstractmethod
    def list_biz_projects_for_pm_email(self, email: str) -> list[dict[str, str]]:
        raise NotImplementedError

    @abstractmethod
    def get_single_brd_doc_link_for_project(self, project_issue_id: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_single_brd_doc_links_for_projects(self, project_issue_ids: list[str]) -> dict[str, str]:
        raise NotImplementedError


class BPMISDirectApiClient(BPMISClient):
    BIZ_PROJECT_TYPE_ID = 1
    BRD_TYPE_ID = 2
    TASK_TYPE_ID = 4
    SUPPORTED_COUNTRIES_ALL_VALUE = 49007
    JIRA_BROWSE_BASE_URL = "https://jira.shopee.io/browse/"
    SYNC_BIZ_PROJECT_STATUS_IDS = [22, 4, 23, 10, 11, 12]
    TASK_TYPE_PREFIX = {
        "feature": "[Feature]",
        "tech": "[Tech]",
        "support": "[Support]",
    }

    def __init__(self, settings: Settings, access_token: str | None = None):
        self.settings = settings
        self.access_token = access_token or self._resolve_access_token()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self._field_defs_cache: dict[str, Any] | None = None
        self._group_options_cache: dict[str, list[dict[str, Any]]] = {}

    def ping(self) -> None:
        self._get_issue_fields()

    def find_project(self, issue_id: str) -> ProjectMatch:
        return ProjectMatch(
            project_id=issue_id,
            raw={
                "issueId": issue_id,
                "url": self._resolve_project_url(),
            },
        )

    def create_jira_ticket(self, project: ProjectMatch, fields: dict[str, str]) -> CreatedTicket:
        payload = self._build_create_payload(project, fields)
        response = self._api_request(
            "/api/v1/issues/batchCreateJiraIssue",
            method="POST",
            body=[payload],
        )
        self._write_debug_capture(payload, response)
        data = response.get("data") or {}
        created = (data.get("created") or [{}])[0]
        add = (data.get("add") or [{}])[0]
        update = (data.get("update") or [{}])[0]
        create_errors = created.get("errors") or {}
        if create_errors:
            error_text = "; ".join(f"{key}: {value}" for key, value in create_errors.items())
            raise BPMISError(f"BPMIS API validation failed: {error_text}")

        ticket_key = (
            created.get("key")
            or add.get("jiraLink")
            or update.get("jiraLink")
            or self._extract_issue_key(created.get("self"))
        )
        ticket_link = self._normalize_ticket_link(
            add.get("jiraLink")
            or update.get("jiraLink")
            or created.get("self")
            or ticket_key
        )
        if not ticket_key and not ticket_link:
            raise BPMISError(
                "BPMIS API did not return a Jira ticket key. "
                "Debug saved to tmp/last_bpmis_api_result.json."
            )
        return CreatedTicket(ticket_key=ticket_key, ticket_link=ticket_link, raw=response)

    def list_biz_projects_for_pm_email(self, email: str) -> list[dict[str, str]]:
        normalized_email = email.strip().lower()
        if not normalized_email:
            raise BPMISError("PM email is required before syncing BPMIS projects.")

        user_ids = self._resolve_bpmis_user_ids_by_email(normalized_email)
        if not user_ids:
            return []

        rows: list[dict[str, Any]] = []
        page = 1
        page_size = 200
        while True:
            response = self._api_request(
                "/api/v1/issues/list",
                params={
                    "search": json.dumps(
                        {
                            "joinType": "and",
                            "subQueries": [
                                {"typeId": [self.BIZ_PROJECT_TYPE_ID]},
                                {"statusId": self.SYNC_BIZ_PROJECT_STATUS_IDS},
                                {
                                    "joinType": "or",
                                    "subQueries": [
                                        {"regionalPmPicId": user_ids},
                                        {"involvedPM": user_ids},
                                    ],
                                },
                            ],
                            "page": page,
                            "pageSize": page_size,
                            "mapping": True,
                        }
                    )
                },
            )
            data = response.get("data") or {}
            page_rows = data.get("rows") or []
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            page += 1

        deduped: dict[str, dict[str, str]] = {}
        for row in rows:
            issue_id = str(row.get("id") or "").strip()
            if not issue_id or issue_id in deduped:
                continue
            summary = str(row.get("summary") or "").strip()
            market = self._extract_market_label(row.get("marketId"))
            deduped[issue_id] = {
                "issue_id": issue_id,
                "project_name": summary,
                "market": market,
            }
        return list(deduped.values())

    def get_single_brd_doc_link_for_project(self, project_issue_id: str) -> str:
        return self.get_single_brd_doc_links_for_projects([project_issue_id]).get(str(project_issue_id).strip(), "")

    def get_single_brd_doc_links_for_projects(self, project_issue_ids: list[str]) -> dict[str, str]:
        normalized_issue_ids = []
        for issue_id in project_issue_ids:
            cleaned = str(issue_id).strip()
            if cleaned and cleaned not in normalized_issue_ids:
                normalized_issue_ids.append(cleaned)
        if not normalized_issue_ids:
            return {}

        parent_issue_ids = [int(issue_id) for issue_id in normalized_issue_ids]
        rows: list[dict[str, Any]] = []
        page = 1
        page_size = 500
        while True:
            response = self._api_request(
                "/api/v1/issues/list",
                params={
                    "search": json.dumps(
                        {
                            "joinType": "and",
                            "subQueries": [
                                {"typeId": [self.BRD_TYPE_ID]},
                                {"parentIds": parent_issue_ids},
                            ],
                            "page": page,
                            "pageSize": page_size,
                            "mapping": True,
                        }
                    )
                },
            )
            page_rows = (response.get("data") or {}).get("rows") or []
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            page += 1

        grouped_rows: dict[str, list[dict[str, Any]]] = {issue_id: [] for issue_id in normalized_issue_ids}
        seen_brd_ids: set[str] = set()
        for row in rows:
            brd_id = str(row.get("id") or "").strip()
            if brd_id and brd_id in seen_brd_ids:
                continue
            if brd_id:
                seen_brd_ids.add(brd_id)
            parent_ids = [str(parent_id).strip() for parent_id in (row.get("parentIds") or []) if str(parent_id).strip()]
            for parent_id in parent_ids:
                if parent_id in grouped_rows:
                    grouped_rows[parent_id].append(row)

        resolved_links: dict[str, str] = {}
        for issue_id, brd_rows in grouped_rows.items():
            if len(brd_rows) == 1:
                resolved_links[issue_id] = str(brd_rows[0].get("link") or "").strip()
            else:
                resolved_links[issue_id] = ""
        return resolved_links

    def _build_create_payload(
        self,
        project: ProjectMatch,
        fields: dict[str, str],
    ) -> dict[str, Any]:
        field_defs = self._get_issue_fields()
        market_value = self._required_field(fields, "Market")
        market_id = self._resolve_option_value(field_defs["marketId"], market_value)
        task_type_label = fields.get("Task Type", "Feature").strip() or "Feature"
        task_type = self._resolve_option_value(field_defs["taskType"], task_type_label)
        summary = self._prefix_summary(task_type_label, self._required_field(fields, "Summary"))

        payload: dict[str, Any] = {
            "typeId": self.TASK_TYPE_ID,
            "marketId": market_id,
            "taskType": task_type,
            "summary": summary,
            "parentIssueId": int(project.project_id),
        }

        if fields.get("PRD Link/s"):
            payload["jiraPrdLink"] = fields["PRD Link/s"].strip()
        if fields.get("Description"):
            payload["desc"] = fields["Description"].strip()
        if fields.get("TD Link/s"):
            payload["jiraTdLink"] = fields["TD Link/s"].strip()

        fix_version_name = fields.get("Fix Version") or fields.get("Fix Version/s")
        if fix_version_name:
            payload["fixVersionId"] = self._resolve_fix_versions(market_id, fix_version_name)

        if fields.get("Component"):
            payload["componentId"] = [
                self._resolve_option_value(field_defs["componentId"], fields["Component"], match_value=market_id)
            ]

        if fields.get("Priority"):
            payload["bizPriorityId"] = self._resolve_option_value(
                field_defs["bizPriorityId"],
                fields["Priority"],
                match_value=self.TASK_TYPE_ID,
            )

        user_field_names = {
            "Assignee": ("assignee", False),
            "Product Manager": ("jiraRegionalPmPicId", True),
            "Dev PIC": ("jiraDevPicId", True),
            "QA PIC": ("jiraQaPicId", True),
            "Reporter": ("reporter", False),
            "Biz PIC": ("jiraBizPicId", True),
        }
        for source_name, (target_name, is_array) in user_field_names.items():
            raw_value = fields.get(source_name, "").strip()
            if not raw_value:
                continue
            user_id = self._resolve_jira_user_id(raw_value)
            payload[target_name] = [user_id] if is_array else user_id

        if fields.get("Need UAT"):
            payload["uatRequired"] = self._resolve_option_value(
                field_defs["uatRequired"],
                fields["Need UAT"],
                match_value=market_id,
            )

        if fields.get("Involved Tracks"):
            payload["involvedProductTrackId"] = [
                self._resolve_option_value(field_defs["involvedProductTrackId"], fields["Involved Tracks"])
            ]

        payload["supportedCountries"] = [self.SUPPORTED_COUNTRIES_ALL_VALUE]
        return payload

    def _write_debug_capture(self, payload: dict[str, Any], response: dict[str, Any]) -> None:
        debug_path = Path(__file__).resolve().parent.parent / "tmp" / "last_bpmis_api_result.json"
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        debug_path.write_text(
            json.dumps({"payload": payload, "response": response}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _required_field(self, fields: dict[str, str], name: str) -> str:
        value = fields.get(name, "").strip()
        if not value:
            raise BPMISError(f"Missing required Jira mapping value for '{name}'.")
        return value

    def _resolve_fix_versions(self, market_id: int, raw_value: str) -> list[int]:
        values = [item.strip() for item in raw_value.split("|") if item.strip()] or [raw_value.strip()]
        resolved_ids: list[int] = []
        for value in values:
            response = self._api_request(
                "/api/v1/versions/list",
                params={
                    "search": json.dumps(
                        {
                            "marketId": market_id,
                            "name": value,
                            "archived": 0,
                            "pageSize": 10,
                        }
                    )
                },
            )
            rows = (response.get("data") or {}).get("rows") or []
            match = next(
                (
                    row
                    for row in rows
                    if str(row.get("marketId")) == str(market_id)
                    and str(row.get("fullName", "")).strip().lower() == value.lower()
                ),
                None,
            )
            if match is None:
                match = next(
                    (
                        row
                        for row in rows
                        if str(row.get("marketId")) == str(market_id)
                        and value.lower() in str(row.get("fullName", "")).lower()
                    ),
                    None,
                )
            if match is None and rows:
                match = rows[0]
            if match is None or "id" not in match:
                raise BPMISError(f"Could not resolve Fix Version '{value}' for market {market_id}.")
            resolved_ids.append(int(match["id"]))
        return resolved_ids

    def _resolve_jira_user_id(self, query: str) -> int:
        response = self._api_request(
            "/api/v1/jira/user",
            params={"query": query, "local": "true"},
        )
        options = response.get("data") or []
        if not options:
            raise BPMISError(f"Could not resolve BPMIS Jira user '{query}'.")

        normalized_query = query.strip().lower()
        for option in options:
            candidates = [
                str(option.get("emailAddress", "")).lower(),
                str(option.get("displayName", "")).lower(),
                str(option.get("name", "")).lower(),
            ]
            if normalized_query in candidates:
                return int(option["id"])

        return int(options[0]["id"])

    def _resolve_option_value(
        self,
        field_def: dict[str, Any],
        raw_value: str,
        match_value: int | None = None,
    ) -> int:
        candidates = [item.strip() for item in raw_value.split("|") if item.strip()] or [raw_value.strip()]
        group_names = self._select_option_groups(field_def, match_value)
        options = self._get_group_options(group_names)

        for candidate in candidates:
            exact = next(
                (
                    option
                    for option in options
                    if str(option.get("label", "")).strip().lower() == candidate.lower()
                ),
                None,
            )
            if exact is not None:
                return int(exact["value"])

        for candidate in candidates:
            partial = next(
                (
                    option
                    for option in options
                    if candidate.lower() in str(option.get("label", "")).strip().lower()
                ),
                None,
            )
            if partial is not None:
                return int(partial["value"])

        raise BPMISError(
            f"Could not resolve BPMIS option for field '{field_def.get('name', field_def.get('key', 'unknown'))}' "
            f"with value '{raw_value}'."
        )

    def _select_option_groups(self, field_def: dict[str, Any], match_value: int | None) -> list[str]:
        option_group = field_def.get("optionGroup")
        if isinstance(option_group, str):
            return [option_group]
        if not isinstance(option_group, list):
            raise BPMISError(f"Field '{field_def.get('key')}' does not expose BPMIS option groups.")

        option_filter = field_def.get("optionGroupFilter") or {}
        match = option_filter.get("match") or {}
        values = match.get("value") or []
        if match_value is not None and values:
            for index, bucket in enumerate(values):
                if isinstance(bucket, list):
                    if match_value in bucket:
                        return [option_group[index]]
                elif match_value == bucket:
                    return [option_group[index]]
        return [group for group in option_group if group]

    def _get_issue_fields(self) -> dict[str, Any]:
        if self._field_defs_cache is None:
            response = self._api_request("/api/v1/issueField/list")
            self._field_defs_cache = response.get("data") or {}
        return self._field_defs_cache

    def _get_group_options(self, group_names: list[str]) -> list[dict[str, Any]]:
        missing = [group for group in group_names if group not in self._group_options_cache]
        if missing:
            response = self._api_request(
                "/api/v1/options/getGroupOptions",
                params={"search": json.dumps({"group": missing})},
            )
            data = response.get("data") or {}
            for group in missing:
                self._group_options_cache[group] = data.get(group) or []

        options: list[dict[str, Any]] = []
        for group in group_names:
            options.extend(self._group_options_cache.get(group, []))
        return options

    def _api_request(
        self,
        path: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        body: Any | None = None,
    ) -> dict[str, Any]:
        url = f"{self.settings.bpmis_base_url.rstrip('/')}/{path.lstrip('/')}"
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=body if body is not None else None,
                timeout=60,
            )
        except requests.RequestException as error:
            raise BPMISError(f"BPMIS API request failed for '{path}'.") from error

        if response.status_code >= 400:
            raise BPMISError(f"BPMIS API request failed for '{path}' with status {response.status_code}.")

        try:
            payload = response.json()
        except ValueError as error:
            raise BPMISError(f"BPMIS API returned non-JSON data for '{path}'.") from error

        if payload.get("code") not in {0, None}:
            raise BPMISError(payload.get("message") or f"BPMIS API error for '{path}'.")
        return payload

    def _prefix_summary(self, task_type_label: str, summary: str) -> str:
        prefix = self.TASK_TYPE_PREFIX.get(task_type_label.strip().lower())
        clean_summary = summary.strip()
        if prefix and not clean_summary.lower().startswith(prefix.lower()):
            return f"{prefix} {clean_summary}".strip()
        return clean_summary

    def _extract_issue_key(self, value: str | None) -> str | None:
        if not value:
            return None
        match = ISSUE_KEY_PATTERN.search(value)
        return match.group(1) if match else None

    def _normalize_ticket_link(self, value: str | None) -> str | None:
        if not value:
            return None
        if value.startswith("http://") or value.startswith("https://"):
            return value
        issue_key = self._extract_issue_key(value) or value.strip()
        if not issue_key:
            return None
        return f"{self.JIRA_BROWSE_BASE_URL}{issue_key}"

    def _resolve_bpmis_user_ids_by_email(self, email: str) -> list[int]:
        response = self._api_request(
            "/api/v1/users/listByEmail",
            params={"search": json.dumps([email])},
        )
        users = response.get("data") or []
        return [int(user["id"]) for user in users if user.get("id") is not None]

    @staticmethod
    def _extract_market_label(value: Any) -> str:
        if isinstance(value, dict):
            return str(value.get("label") or value.get("name") or value.get("id") or "").strip()
        return str(value or "").strip()

    def _resolve_project_url(self) -> str:
        return f"{self.settings.bpmis_base_url.rstrip('/')}/me"

    def _resolve_access_token(self) -> str:
        token = self.settings.bpmis_api_access_token
        if token:
            return token
        raise BPMISNotConfiguredError(
            "BPMIS API access token is not configured. "
            "Please set BPMIS_API_ACCESS_TOKEN in .env with a token generated from BPMIS."
        )
