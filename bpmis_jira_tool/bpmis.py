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
    def create_jira_ticket(
        self,
        project: ProjectMatch,
        fields: dict[str, str],
        *,
        preformatted_summary: bool = False,
    ) -> CreatedTicket:
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

    @abstractmethod
    def get_brd_doc_links_for_projects(self, project_issue_ids: list[str]) -> dict[str, list[str]]:
        raise NotImplementedError

    @abstractmethod
    def search_versions(self, query: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_issues_for_version(self, version_id: str | int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_issue_detail(self, issue_id: str | int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_jira_ticket_detail(self, ticket_key: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def update_jira_ticket_status(self, ticket_key: str, status: str) -> dict[str, Any]:
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
    JIRA_STATUS_OPTIONS = [
        "Waiting",
        "PRD in Progress",
        "PRD Reviewed",
        "Developing",
        "Testing",
        "UAT",
        "Regression",
        "Done",
        "Closed",
        "IceBox",
    ]

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

    def create_jira_ticket(
        self,
        project: ProjectMatch,
        fields: dict[str, str],
        *,
        preformatted_summary: bool = False,
    ) -> CreatedTicket:
        payload = self._build_create_payload(project, fields, preformatted_summary=preformatted_summary)
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
        all_links = self.get_brd_doc_links_for_projects(project_issue_ids)
        resolved_links: dict[str, str] = {}
        for issue_id, links in all_links.items():
            resolved_links[issue_id] = links[0] if len(links) == 1 else ""
        return resolved_links

    def get_brd_doc_links_for_projects(self, project_issue_ids: list[str]) -> dict[str, list[str]]:
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
            parent_ids: list[str] = []
            for parent_ref in row.get("parentIds") or []:
                if isinstance(parent_ref, dict):
                    parent_value = str(parent_ref.get("id") or "").strip()
                else:
                    parent_value = str(parent_ref).strip()
                if parent_value:
                    parent_ids.append(parent_value)
            for parent_id in parent_ids:
                if parent_id in grouped_rows:
                    grouped_rows[parent_id].append(row)

        resolved_links: dict[str, list[str]] = {}
        for issue_id, brd_rows in grouped_rows.items():
            resolved_links[issue_id] = [
                str(brd_row.get("link") or "").strip()
                for brd_row in brd_rows
                if str(brd_row.get("link") or "").strip()
            ]
        return resolved_links

    def search_versions(self, query: str) -> list[dict[str, Any]]:
        normalized_query = query.strip()
        if not normalized_query:
            return []

        rows: list[dict[str, Any]] = []
        page = 1
        page_size = 100
        while True:
            response = self._api_request(
                "/api/v1/versions/list",
                params={
                    "search": json.dumps(
                        {
                            "name": normalized_query,
                            "archived": 0,
                            "page": page,
                            "pageSize": page_size,
                        }
                    )
                },
            )
            page_rows = (response.get("data") or {}).get("rows") or []
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            page += 1

        deduped: dict[str, dict[str, Any]] = {}
        query_lower = normalized_query.lower()
        for row in rows:
            version_id = str(row.get("id") or "").strip()
            version_name = self._extract_version_name(row)
            if not version_id or not version_name:
                continue
            if query_lower not in version_name.lower():
                continue
            if version_id in deduped:
                continue
            deduped[version_id] = row

        return sorted(
            deduped.values(),
            key=lambda row: self._version_sort_key(normalized_query, self._extract_version_name(row)),
        )

    def list_issues_for_version(self, version_id: str | int) -> list[dict[str, Any]]:
        normalized_version_id = str(version_id).strip()
        if not normalized_version_id:
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
                                {"typeId": [self.TASK_TYPE_ID]},
                                {"fixVersionId": [int(normalized_version_id)]},
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

        enriched_rows: list[dict[str, Any]] = []
        seen_issue_ids: set[str] = set()
        for row in rows:
            issue_id = self._extract_issue_identifier(row)
            issue_key = self._extract_issue_key_from_row(row)
            dedupe_key = issue_id or issue_key
            if dedupe_key and dedupe_key in seen_issue_ids:
                continue
            if dedupe_key:
                seen_issue_ids.add(dedupe_key)

            if self._issue_requires_enrichment(row) and issue_id:
                detail = self.get_issue_detail(issue_id)
                if detail:
                    row = self._merge_issue_payloads(row, detail)
            enriched_rows.append(row)
        return enriched_rows

    def get_issue_detail(self, issue_id: str | int) -> dict[str, Any]:
        normalized_issue_id = str(issue_id).strip()
        if not normalized_issue_id:
            return {}

        attempts = [
            ("GET", "/api/v1/issues/detail", {"id": normalized_issue_id}, None),
            ("GET", "/api/v1/issues/detail", {"issueId": normalized_issue_id}, None),
            ("GET", "/api/v1/issue/detail", {"id": normalized_issue_id}, None),
            ("GET", "/api/v1/issue/detail", {"issueId": normalized_issue_id}, None),
            ("GET", f"/api/v1/issues/{normalized_issue_id}", None, None),
            ("GET", f"/api/v1/issue/{normalized_issue_id}", None, None),
        ]
        for method, path, params, body in attempts:
            payload = self._safe_api_request(path, method=method, params=params, body=body)
            detail = self._extract_issue_detail_payload(payload)
            if detail:
                return detail
        return {}

    def get_jira_ticket_detail(self, ticket_key: str) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        if not normalized_ticket_key:
            return {}

        detail_attempts = [
            ("GET", "/api/v1/issues/detail", {"jiraKey": normalized_ticket_key}, None),
            ("GET", "/api/v1/issues/detail", {"jiraIssueKey": normalized_ticket_key}, None),
            ("GET", "/api/v1/issues/detail", {"key": normalized_ticket_key}, None),
            ("GET", "/api/v1/issue/detail", {"jiraKey": normalized_ticket_key}, None),
            ("GET", "/api/v1/issue/detail", {"jiraIssueKey": normalized_ticket_key}, None),
            ("GET", "/api/v1/issue/detail", {"key": normalized_ticket_key}, None),
        ]
        for method, path, params, body in detail_attempts:
            payload = self._safe_api_request(path, method=method, params=params, body=body)
            detail = self._extract_issue_detail_payload(payload)
            if detail and self._row_matches_jira_key(detail, normalized_ticket_key):
                return detail

        for search_payload in self._jira_ticket_search_payloads(normalized_ticket_key):
            payload = self._safe_api_request(
                "/api/v1/issues/list",
                params={"search": json.dumps(search_payload)},
            )
            rows = ((payload or {}).get("data") or {}).get("rows") or []
            match = next((row for row in rows if self._row_matches_jira_key(row, normalized_ticket_key)), None)
            if match:
                issue_id = self._extract_issue_identifier(match)
                if issue_id:
                    detail = self.get_issue_detail(issue_id)
                    if detail:
                        return self._merge_issue_payloads(match, detail)
                return match
        return {}

    def update_jira_ticket_status(self, ticket_key: str, status: str) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        normalized_status = self._normalize_jira_status(status)
        if not normalized_ticket_key:
            raise BPMISError("Jira ticket key is required.")
        if not normalized_status:
            raise BPMISError("Jira status is required.")

        detail = self.get_jira_ticket_detail(normalized_ticket_key)
        issue_id = self._extract_issue_identifier(detail)
        status_id = self._resolve_jira_status_id(normalized_status)
        bodies = self._jira_status_update_bodies(
            ticket_key=normalized_ticket_key,
            issue_id=issue_id,
            status=normalized_status,
            status_id=status_id,
        )
        attempts = [
            ("POST", "/api/v1/issues/updateStatus"),
            ("POST", "/api/v1/issues/status/update"),
            ("POST", "/api/v1/issues/transition"),
            ("POST", "/api/v1/issues/workflow"),
            ("POST", "/api/v1/issues/update"),
            ("PUT", "/api/v1/issues/update"),
            ("POST", "/api/v1/issue/updateStatus"),
            ("POST", "/api/v1/issue/status/update"),
        ]
        last_error: BPMISError | None = None
        for body in bodies:
            for method, path in attempts:
                try:
                    self._api_request(path, method=method, body=body)
                    return self.get_jira_ticket_detail(normalized_ticket_key)
                except BPMISError as error:
                    last_error = error
        if last_error is not None:
            raise BPMISError(f"Could not update Jira status through BPMIS: {last_error}") from last_error
        raise BPMISError("Could not update Jira status through BPMIS.")

    def _build_create_payload(
        self,
        project: ProjectMatch,
        fields: dict[str, str],
        *,
        preformatted_summary: bool = False,
    ) -> dict[str, Any]:
        field_defs = self._get_issue_fields()
        market_value = self._required_field(fields, "Market")
        market_id = self._resolve_option_value(field_defs["marketId"], market_value)
        task_type_label = fields.get("Task Type", "Feature").strip() or "Feature"
        task_type = self._resolve_option_value(field_defs["taskType"], task_type_label)
        raw_summary = self._required_field(fields, "Summary")
        summary = raw_summary.strip() if preformatted_summary else self._prefix_summary(
            task_type_label,
            market_value,
            fields.get("System", ""),
            raw_summary,
        )

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
                self._resolve_option_value(field_defs["componentId"], component_value, match_value=market_id)
                for component_value in self._split_component_values(fields["Component"])
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

    @staticmethod
    def _split_component_values(raw_value: str) -> list[str]:
        return [value.strip() for value in str(raw_value or "").split(",") if value.strip()]

    def _extract_issue_detail_payload(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        if not payload:
            return {}
        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("detail", "row", "issue", "item", "info"):
                if isinstance(data.get(key), dict):
                    return data[key]
            if any(key in data for key in ("id", "issueId", "summary", "desc", "description", "jiraPrdLink")):
                return data
        return {}

    def _safe_api_request(
        self,
        path: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        body: Any | None = None,
    ) -> dict[str, Any] | None:
        try:
            return self._api_request(path, method=method, params=params, body=body)
        except BPMISError:
            return None

    def _issue_requires_enrichment(self, row: dict[str, Any]) -> bool:
        return not (
            self._extract_issue_description(row)
            and self._extract_issue_pm(row)
            and self._extract_issue_prd_links(row)
        )

    def _merge_issue_payloads(self, primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        merged = dict(fallback)
        for key, value in primary.items():
            if value not in (None, "", [], {}):
                merged[key] = value
        return merged

    def _extract_issue_identifier(self, row: dict[str, Any]) -> str:
        for key in ("id", "issueId", "issue_id"):
            value = self._find_first_value(row, key)
            if value is not None:
                text = str(value).strip()
                if text:
                    return text
        return ""

    def _extract_issue_key_from_row(self, row: dict[str, Any]) -> str:
        for key in ("jiraKey", "ticketKey", "jiraIssueKey", "issueKey", "key"):
            value = self._find_first_value(row, key)
            if value is not None:
                text = str(value).strip()
                if text:
                    return text
        return self._extract_issue_key(str(self._find_first_value(row, "jiraLink") or ""))

    def _row_matches_jira_key(self, row: dict[str, Any], ticket_key: str) -> bool:
        row_key = self._extract_issue_key_from_row(row)
        normalized_ticket_key = str(ticket_key or "").strip()
        return bool(row_key and normalized_ticket_key and row_key.lower() == normalized_ticket_key.lower())

    def _jira_ticket_search_payloads(self, ticket_key: str) -> list[dict[str, Any]]:
        base = {"page": 1, "pageSize": 10, "mapping": True}
        return [
            {**base, "jiraKey": ticket_key},
            {**base, "issueKey": ticket_key},
            {**base, "key": ticket_key},
            {**base, "keyword": ticket_key},
            {
                **base,
                "joinType": "and",
                "subQueries": [
                    {"typeId": [self.TASK_TYPE_ID]},
                    {"jiraKey": [ticket_key]},
                ],
            },
        ]

    def _normalize_jira_status(self, status: str) -> str:
        normalized = str(status or "").strip()
        allowed = {value.lower(): value for value in self.JIRA_STATUS_OPTIONS}
        return allowed.get(normalized.lower(), "")

    def _resolve_jira_status_id(self, status: str) -> int | None:
        field_defs = self._get_issue_fields()
        status_field = field_defs.get("statusId") or field_defs.get("jiraStatusId") or field_defs.get("status") or {}
        if not isinstance(status_field, dict):
            return None
        try:
            return int(self._resolve_option_value(status_field, status))
        except (BPMISError, TypeError, ValueError):
            return None

    def _jira_status_update_bodies(
        self,
        *,
        ticket_key: str,
        issue_id: str,
        status: str,
        status_id: int | None,
    ) -> list[dict[str, Any]]:
        identifiers: list[dict[str, Any]] = [{"jiraKey": ticket_key}, {"jiraIssueKey": ticket_key}, {"key": ticket_key}]
        if issue_id:
            identifiers.extend([{"id": issue_id}, {"issueId": issue_id}])
        status_values: list[dict[str, Any]] = []
        if status_id is not None:
            status_values.extend([{"statusId": status_id}, {"jiraStatusId": status_id}])
        status_values.extend([{"status": status}, {"jiraStatus": status}, {"statusName": status}])

        bodies: list[dict[str, Any]] = []
        seen: set[str] = set()
        for identifier in identifiers:
            for status_value in status_values:
                body = {**identifier, **status_value}
                key = json.dumps(body, sort_keys=True)
                if key not in seen:
                    bodies.append(body)
                    seen.add(key)
        return bodies

    def _extract_issue_description(self, row: dict[str, Any]) -> str:
        for key in ("desc", "description", "jiraDescription"):
            value = self._find_first_value(row, key)
            if value is None:
                continue
            text = self._stringify_value(value)
            if text:
                return text
        return ""

    def _extract_issue_pm(self, row: dict[str, Any]) -> str:
        for key in ("jiraRegionalPmPicId", "regionalPmPic", "productManager", "pm", "regionalPm"):
            value = self._find_first_value(row, key)
            text = self._stringify_person(value)
            if text:
                return text
        return ""

    def _extract_issue_prd_links(self, row: dict[str, Any]) -> list[str]:
        candidates: list[str] = []
        for key in ("jiraPrdLink", "prdLink", "prdLinks", "prd", "brdLink"):
            value = self._find_first_value(row, key)
            candidates.extend(self._extract_links(value))
        deduped: list[str] = []
        for item in candidates:
            if item not in deduped:
                deduped.append(item)
        return deduped

    def _extract_version_name(self, row: dict[str, Any]) -> str:
        for key in ("fullName", "name", "versionName", "label"):
            value = row.get(key)
            text = str(value or "").strip()
            if text:
                return text
        return ""

    def _version_sort_key(self, query: str, value: str) -> tuple[int, int, str]:
        lowered_query = query.lower()
        lowered_value = value.lower()
        if lowered_value == lowered_query:
            return (0, len(value), lowered_value)
        if lowered_value.startswith(lowered_query):
            return (1, len(value), lowered_value)
        return (2, len(value), lowered_value)

    def _find_first_value(self, row: dict[str, Any], key: str) -> Any:
        containers = [row]
        for nested_key in ("fields", "mapping", "data", "detail", "row"):
            nested = row.get(nested_key)
            if isinstance(nested, dict):
                containers.append(nested)
        lowered_key = key.lower()
        for container in containers:
            for candidate_key, value in container.items():
                if str(candidate_key).lower() == lowered_key:
                    return value
        return None

    def _stringify_value(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            for key in ("label", "name", "displayName", "emailAddress", "value"):
                text = str(value.get(key) or "").strip()
                if text:
                    return text
            return ""
        if isinstance(value, list):
            rendered = [self._stringify_value(item) for item in value]
            rendered = [item for item in rendered if item]
            return ", ".join(rendered)
        return str(value).strip()

    def _stringify_person(self, value: Any) -> str:
        if isinstance(value, list):
            people = [self._stringify_person(item) for item in value]
            people = [item for item in people if item]
            return ", ".join(people)
        if isinstance(value, dict):
            for key in ("displayName", "name", "emailAddress", "label", "username", "value"):
                text = str(value.get(key) or "").strip()
                if text:
                    return text
            return ""
        return self._stringify_value(value)

    def _extract_links(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            links: list[str] = []
            for item in value:
                links.extend(self._extract_links(item))
            return links
        if isinstance(value, dict):
            links: list[str] = []
            for key in ("url", "link", "href", "value"):
                links.extend(self._extract_links(value.get(key)))
            return links
        text = str(value).strip()
        if not text:
            return []
        matches = re.findall(r"https?://[^\s,]+", text)
        return matches or ([text] if text.startswith("http://") or text.startswith("https://") else [])

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

    def _prefix_summary(self, task_type_label: str, market_value: str, system_value: str, summary: str) -> str:
        task_prefix = self.TASK_TYPE_PREFIX.get(task_type_label.strip().lower())
        clean_summary = summary.strip()
        if not task_prefix:
            return clean_summary
        is_regional = market_value.strip().lower() == "regional"
        scope_value = "Productization" if is_regional else system_value.strip()
        scope_prefix = f"[{scope_value}]" if scope_value else ""
        full_prefix = f"{task_prefix}{scope_prefix}"
        if scope_prefix:
            core_summary = self._strip_known_summary_prefixes(clean_summary, [task_prefix, scope_prefix])
            if not core_summary:
                return full_prefix
            return f"{full_prefix} {core_summary}".strip()
        return clean_summary

    @staticmethod
    def _strip_known_summary_prefixes(summary: str, prefixes: list[str]) -> str:
        text = summary.strip()
        active_prefixes = [prefix for prefix in prefixes if prefix]
        while text:
            matched = False
            for prefix in active_prefixes:
                if text.lower().startswith(prefix.lower()):
                    text = text[len(prefix):]
                    text = re.sub(r"^\s*[-_:：|]*\s*", "", text)
                    matched = True
                    break
            if not matched:
                break
        return text.strip()

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
            "Get a token from https://bpmis-uat1.uat.npt.seabank.io/me/access-token "
            "and save it in the portal or set BPMIS_API_ACCESS_TOKEN in .env."
        )
