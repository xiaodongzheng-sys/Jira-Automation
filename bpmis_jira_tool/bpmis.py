from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import json
import logging
import os
import re
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import requests

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError, BPMISNotConfiguredError
from bpmis_jira_tool.models import CreatedTicket, ProjectMatch


ISSUE_KEY_PATTERN = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")
BPMIS_LOGGER = logging.getLogger(__name__)
BPMIS_SLOW_REQUEST_SECONDS = 5.0
JIRA_LIVE_BULK_DETAIL_CHUNK_SIZE = 100


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
    def list_biz_projects_for_pm_emails(self, emails: list[str]) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def search_biz_projects_by_title_keywords(self, keywords: str, *, max_pages: int | None = None) -> list[dict[str, str]]:
        raise NotImplementedError

    @abstractmethod
    def list_jira_tasks_for_project_created_by_email(self, project_issue_id: str, email: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_jira_tasks_for_projects_created_by_emails(
        self,
        project_issue_ids: list[str],
        emails: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        raise NotImplementedError

    @abstractmethod
    def list_jira_tasks_created_by_emails(
        self,
        emails: list[str],
        *,
        max_pages: int | None = None,
        enrich_missing_parent: bool = True,
        created_after: str | date | datetime | None = None,
        release_after: str | date | datetime | None = None,
    ) -> list[dict[str, Any]]:
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
    def get_jira_ticket_details(self, ticket_keys: list[str]) -> dict[str, dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def update_jira_ticket_status(self, ticket_key: str, status: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def update_jira_ticket_fix_version(self, ticket_key: str, version_name: str, version_id: str | None = None) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def link_jira_ticket_to_project(self, ticket_key: str, project_issue_id: str | int) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def delink_jira_ticket_from_project(self, ticket_key: str, project_issue_id: str | int) -> dict[str, Any]:
        raise NotImplementedError


class BPMISDirectApiClient(BPMISClient):
    BIZ_PROJECT_TYPE_ID = 1
    BRD_TYPE_ID = 2
    TASK_TYPE_ID = 4
    SUPPORTED_COUNTRIES_ALL_VALUE = 49007
    JIRA_BROWSE_BASE_URL = "https://jira.shopee.io/browse/"
    SYNC_BIZ_PROJECT_STATUS_IDS = [4, 23, 10, 11, 12]
    TEAM_DASHBOARD_BIZ_PROJECT_STATUS_NAMES = {
        "pending review",
        "confirmed",
        "developing",
        "testing",
        "uat",
    }
    TEAM_DASHBOARD_BIZ_PROJECT_STATUS_ID_VALUES = {str(status_id) for status_id in SYNC_BIZ_PROJECT_STATUS_IDS}
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
        self._main_thread_id = threading.get_ident()
        self._thread_local = threading.local()
        self._stats_lock = threading.Lock()
        self._cache_lock = threading.RLock()
        self._field_defs_cache: dict[str, Any] | None = None
        self._group_options_cache: dict[str, list[dict[str, Any]]] = {}
        self._bpmis_user_ids_by_email_cache: dict[str, list[int]] = {}
        self._issue_detail_cache: dict[str, dict[str, Any]] = {}
        self._team_dashboard_release_versions_by_id: dict[int, dict[str, Any]] = {}
        self.event_logger = BPMIS_LOGGER
        self.request_timings: dict[str, float] = {}
        self.request_stats: dict[str, int] = {
            "api_call_count": 0,
            "issue_detail_lookup_count": 0,
            "issue_detail_bulk_lookup_count": 0,
            "issue_detail_bulk_issue_count": 0,
            "issue_detail_single_fallback_count": 0,
            "jira_live_bulk_lookup_count": 0,
            "jira_live_bulk_issue_count": 0,
            "jira_live_detail_lookup_count": 0,
            "jira_live_status_override_count": 0,
            "bpmis_release_query_filter_probe_count": 0,
            "bpmis_release_query_filter_enabled_count": 0,
            "bpmis_release_query_filter_disabled_count": 0,
            "bpmis_release_query_filter_probe_failed_count": 0,
            "bpmis_release_query_filter_used_count": 0,
            "issue_detail_enrichment_skipped_count": 0,
            "issue_created_before_cutoff_count": 0,
            "issue_release_before_cutoff_count": 0,
            "issue_release_missing_included_count": 0,
            "issue_list_created_cutoff_hit": 0,
            "issue_list_page_cap_hit": 0,
            "issue_list_page_count": 0,
            "issue_rows_scanned": 0,
            "issue_tree_page_count": 0,
            "issue_tree_rows_scanned": 0,
            "issue_tree_fallback_count": 0,
            "release_version_lookup_count": 0,
            "release_version_count": 0,
            "release_version_lookup_failed_count": 0,
            "user_lookup_count": 0,
        }

    def ping(self) -> None:
        self._get_issue_fields()

    def _worker_count(self, env_name: str, default: int, hard_cap: int) -> int:
        raw_value = str(os.getenv(env_name) or "").strip()
        try:
            configured = int(raw_value) if raw_value else default
        except ValueError:
            configured = default
        return max(1, min(max(1, hard_cap), configured))

    def _bpmis_session_for_current_thread(self) -> requests.Session:
        if threading.get_ident() == self._main_thread_id:
            return self.session
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(self.session.headers)
            self._thread_local.session = session
        return session

    def _increment_stat(self, key: str, amount: int = 1) -> int:
        with self._stats_lock:
            self.request_stats[key] = int(self.request_stats.get(key) or 0) + int(amount or 0)
            return self.request_stats[key]

    def _add_request_timing(self, key: str, started_at: float) -> None:
        elapsed = time.monotonic() - started_at
        with self._stats_lock:
            self.request_timings[key] = round(float(self.request_timings.get(key) or 0.0) + elapsed, 3)

    def _snapshot_issue_detail(self, issue_id: str) -> dict[str, Any] | None:
        with self._cache_lock:
            cached = self._issue_detail_cache.get(issue_id)
            return dict(cached) if isinstance(cached, dict) else None

    def _store_issue_detail(self, issue_id: str, detail: dict[str, Any]) -> None:
        if not issue_id:
            return
        with self._cache_lock:
            self._issue_detail_cache[issue_id] = dict(detail)

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

        return self._normalize_team_dashboard_biz_project_rows(rows)

    def list_biz_projects_for_pm_emails(self, emails: list[str]) -> list[dict[str, Any]]:
        normalized_emails = self._normalize_email_list(emails)
        if not normalized_emails:
            return []

        user_ids_by_email = self._resolve_bpmis_user_ids_by_emails(normalized_emails)
        user_ids = sorted({user_id for ids in user_ids_by_email.values() for user_id in ids})
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
            page_rows = (response.get("data") or {}).get("rows") or []
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            page += 1

        normalized = self._normalize_team_dashboard_biz_project_rows(rows)
        rows_by_id = {
            str(row.get("id") or row.get("issue_id") or row.get("bpmis_id") or "").strip(): row
            for row in rows
            if isinstance(row, dict)
        }
        user_id_texts_by_email = {email: {str(user_id) for user_id in ids} for email, ids in user_ids_by_email.items()}
        for project in normalized:
            raw_row = rows_by_id.get(str(project.get("issue_id") or project.get("bpmis_id") or "").strip()) or {}
            matched = self._biz_project_matched_pm_emails(raw_row, normalized_emails, user_id_texts_by_email)
            project["matched_pm_emails"] = matched
        return normalized

    def search_biz_projects_by_title_keywords(self, keywords: str, *, max_pages: int | None = None) -> list[dict[str, str]]:
        normalized_keywords = re.sub(r"\s+", " ", str(keywords or "").strip())
        if not normalized_keywords:
            return []

        rows: list[dict[str, Any]] = []
        page = 1
        page_size = 50
        page_cap = max(1, int(max_pages or 2))
        while page <= page_cap:
            response = self._api_request(
                "/api/v1/issues/list",
                params={
                    "search": json.dumps(
                        {
                            "joinType": "and",
                            "subQueries": [
                                {"typeId": [self.BIZ_PROJECT_TYPE_ID]},
                                {"statusId": self.SYNC_BIZ_PROJECT_STATUS_IDS},
                            ],
                            "keyword": normalized_keywords,
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
        return self._normalize_team_dashboard_biz_project_rows(rows)

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

    def list_jira_tasks_for_project_created_by_email(self, project_issue_id: str, email: str) -> list[dict[str, Any]]:
        normalized_issue_id = str(project_issue_id or "").strip()
        normalized_email = str(email or "").strip().lower()
        if not normalized_issue_id or not normalized_email:
            return []

        try:
            parent_issue_id = int(normalized_issue_id)
        except ValueError:
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
                                {"parentIds": [parent_issue_id]},
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

        tasks: list[dict[str, Any]] = []
        seen_issue_ids: set[str] = set()
        for row in rows:
            issue_id = self._extract_issue_identifier(row)
            issue_key = self._extract_issue_key_from_row(row)
            dedupe_key = issue_key or issue_id
            if dedupe_key and dedupe_key in seen_issue_ids:
                continue
            if dedupe_key:
                seen_issue_ids.add(dedupe_key)

            if self._issue_requires_user_enrichment(row, normalized_email) and issue_id:
                detail = self.get_issue_detail(issue_id)
                if detail:
                    row = self._merge_issue_payloads(row, detail)
            if not self._issue_reported_by(row, normalized_email):
                continue
            row = self._with_live_jira_fields(row, issue_key)
            tasks.append(self._normalize_project_jira_task(row))
        return tasks

    def list_jira_tasks_for_projects_created_by_emails(
        self,
        project_issue_ids: list[str],
        emails: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        normalized_emails = self._normalize_email_list(emails)
        parent_issue_ids: list[int] = []
        parent_id_texts: set[str] = set()
        for project_issue_id in project_issue_ids:
            text = str(project_issue_id or "").strip()
            if not text or text in parent_id_texts:
                continue
            try:
                parent_issue_ids.append(int(text))
            except ValueError:
                continue
            parent_id_texts.add(text)
        if not parent_issue_ids or not normalized_emails:
            return {}

        started_at = time.monotonic()
        rows: list[dict[str, Any]] = []
        page_size = 200
        parent_chunks = self._chunks(parent_issue_ids, 50)
        worker_count = min(len(parent_chunks), self._worker_count("TEAM_DASHBOARD_BPMIS_BULK_WORKERS", 4, 4))
        try:
            if worker_count > 1:
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    future_by_index = {
                        executor.submit(self._list_jira_task_rows_for_parent_chunk, parent_chunk, page_size): index
                        for index, parent_chunk in enumerate(parent_chunks)
                    }
                    rows_by_index: dict[int, list[dict[str, Any]]] = {}
                    for future in as_completed(future_by_index):
                        rows_by_index[future_by_index[future]] = future.result()
                    for index in range(len(parent_chunks)):
                        rows.extend(rows_by_index.get(index) or [])
            else:
                for parent_chunk in parent_chunks:
                    rows.extend(self._list_jira_task_rows_for_parent_chunk(parent_chunk, page_size))
        finally:
            self._add_request_timing("zero_jira_bulk", started_at)

        ticket_rows: list[tuple[dict[str, Any], str, str, list[str]]] = []
        seen_by_parent: set[tuple[str, str]] = set()
        self._prime_issue_detail_cache(
            [
                self._extract_issue_identifier(row)
                for row in rows
                if isinstance(row, dict)
                and any(self._issue_requires_user_enrichment(row, email) for email in normalized_emails)
            ]
        )
        for row in rows:
            if not isinstance(row, dict):
                continue
            issue_id = self._extract_issue_identifier(row)
            issue_key = self._extract_issue_key_from_row(row)
            matched_email = ""
            for email in normalized_emails:
                if self._issue_requires_user_enrichment(row, email) and issue_id:
                    detail = self.get_issue_detail(issue_id)
                    if detail:
                        row = self._merge_issue_payloads(row, detail)
                if self._issue_reported_by(row, email):
                    matched_email = email
                    break
            if not matched_email:
                continue
            parent_ids = [parent_id for parent_id in self._extract_parent_issue_ids(row) if parent_id in parent_id_texts]
            if not parent_ids:
                continue
            dedupe_key = issue_key or issue_id
            if not dedupe_key:
                continue
            for parent_id in parent_ids:
                key = (parent_id, dedupe_key)
                if key in seen_by_parent:
                    continue
                seen_by_parent.add(key)
                ticket_rows.append((row, issue_key, matched_email, [parent_id]))

        live_details = self._get_jira_ticket_details_via_jira_bulk([issue_key for _row, issue_key, _email, _parents in ticket_rows])
        grouped: dict[str, list[dict[str, Any]]] = {str(parent_id): [] for parent_id in parent_issue_ids}
        for row, issue_key, matched_email, parent_ids in ticket_rows:
            if live_details is None:
                row = self._with_live_jira_fields(row, issue_key)
            else:
                detail = live_details.get(self._normalize_jira_issue_key(issue_key))
                if detail:
                    row = self._merge_live_jira_fields(row, detail)
            for parent_id in parent_ids:
                grouped.setdefault(parent_id, []).append(
                    self._normalize_team_dashboard_jira_task(row, pm_email=matched_email, parent_project={})
                )
        for tasks in grouped.values():
            tasks.sort(key=lambda task: (str(task.get("release_date") or ""), str(task.get("jira_id") or "")))
        return grouped

    def _list_jira_task_rows_for_parent_chunk(self, parent_chunk: list[int], page_size: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            response = self._api_request(
                "/api/v1/issues/list",
                params={
                    "search": json.dumps(
                        {
                            "joinType": "and",
                            "subQueries": [
                                {"typeId": [self.TASK_TYPE_ID]},
                                {"parentIds": parent_chunk},
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
        return rows

    def list_jira_tasks_created_by_emails(
        self,
        emails: list[str],
        *,
        max_pages: int | None = None,
        enrich_missing_parent: bool = True,
        created_after: str | date | datetime | None = None,
        release_after: str | date | datetime | None = None,
    ) -> list[dict[str, Any]]:
        normalized_emails = self._normalize_email_list(emails)
        if not normalized_emails:
            return []

        created_cutoff = self._parse_issue_datetime(created_after) if created_after else None
        release_cutoff = self._parse_issue_datetime(release_after) if release_after else None
        with ThreadPoolExecutor(max_workers=2) as executor:
            user_future = executor.submit(self._resolve_team_dashboard_user_ids_timed, normalized_emails)
            release_future = executor.submit(self._team_dashboard_release_version_ids, release_cutoff)
            user_ids_by_email = user_future.result()
            release_version_ids = release_future.result()
        user_ids = sorted({user_id for ids in user_ids_by_email.values() for user_id in ids})
        if not user_ids:
            return []

        rows: list[dict[str, Any]] = []
        if release_cutoff and not release_version_ids:
            rows = self._list_team_dashboard_jira_task_rows_via_list(
                user_ids,
                max_pages=max_pages,
                created_cutoff=created_cutoff,
            )
        else:
            rows = self._list_team_dashboard_jira_task_rows_via_tree(
                user_ids,
                fix_version_ids=release_version_ids,
                max_pages=max_pages,
                created_cutoff=created_cutoff,
            )
            if rows is None:
                self._increment_stat("issue_tree_fallback_count")
                rows = self._list_team_dashboard_jira_task_rows_via_list(
                    user_ids,
                    max_pages=max_pages,
                    created_cutoff=created_cutoff,
                )
        if release_version_ids:
            rows = [self._with_team_dashboard_release_version_detail(row) for row in rows]

        candidate_task_rows: list[tuple[dict[str, Any], str, str]] = []
        seen_issue_ids: set[str] = set()
        parent_project_cache: dict[str, dict[str, Any]] = {}
        user_id_texts_by_email = {email: {str(user_id) for user_id in ids} for email, ids in user_ids_by_email.items()}
        for row in rows:
            issue_id = self._extract_issue_identifier(row)
            issue_key = self._extract_issue_key_from_row(row)
            dedupe_key = issue_key or issue_id
            if dedupe_key and dedupe_key in seen_issue_ids:
                continue
            if dedupe_key:
                seen_issue_ids.add(dedupe_key)

            matched_email = self._creator_email_for_row(row, normalized_emails, user_id_texts_by_email)
            if enrich_missing_parent and not matched_email and issue_id and not self._issue_has_creator_value(row):
                detail = self.get_issue_detail(issue_id)
                if detail:
                    row = self._merge_issue_payloads(row, detail)
                    matched_email = self._creator_email_for_row(row, normalized_emails, user_id_texts_by_email)
            if not matched_email:
                continue
            if created_cutoff and not self._issue_created_on_or_after(row, created_cutoff):
                self._increment_stat("issue_created_before_cutoff_count")
                continue
            if release_cutoff:
                release_text = self._extract_issue_release_date_text(row)
                release_at = self._parse_issue_datetime(release_text)
                if not release_text or not release_at:
                    self._increment_stat("issue_release_missing_included_count")
                elif release_at < release_cutoff:
                    self._increment_stat("issue_release_before_cutoff_count")
                    continue
            if enrich_missing_parent and issue_id and not self._extract_parent_issue_ids(row):
                detail = self.get_issue_detail(issue_id)
                if detail:
                    row = self._merge_issue_payloads(row, detail)
            elif issue_id and not self._extract_parent_issue_ids(row):
                self._increment_stat("issue_detail_enrichment_skipped_count")
            candidate_task_rows.append((row, issue_key, matched_email))

        self._prime_biz_project_parent_details([row for row, _issue_key, _email in candidate_task_rows])
        task_rows: list[tuple[dict[str, Any], str, str, dict[str, Any]]] = []
        for row, issue_key, matched_email in candidate_task_rows:
            parent_project = self._parent_project_for_task(row, parent_project_cache)
            task_rows.append((row, issue_key, matched_email, parent_project))
        live_details = self._get_jira_ticket_details_via_jira_bulk([issue_key for _row, issue_key, _email, _parent in task_rows])
        tasks: list[dict[str, Any]] = []
        for row, issue_key, matched_email, parent_project in task_rows:
            if live_details is None:
                row = self._with_live_jira_fields(row, issue_key)
            else:
                detail = live_details.get(self._normalize_jira_issue_key(issue_key))
                if detail:
                    row = self._merge_live_jira_fields(row, detail)
            tasks.append(self._normalize_team_dashboard_jira_task(row, pm_email=matched_email, parent_project=parent_project))
        return tasks

    def _resolve_team_dashboard_user_ids_timed(self, emails: list[str]) -> dict[str, list[int]]:
        started_at = time.monotonic()
        try:
            return self._resolve_bpmis_user_ids_by_emails(emails)
        finally:
            self._add_request_timing("bpmis_user_lookup", started_at)

    def _list_team_dashboard_jira_task_rows_via_list(
        self,
        user_ids: list[int],
        *,
        max_pages: int | None,
        created_cutoff: datetime | None,
        field_name: str | None = None,
        fix_version_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        page_size = 200
        while True:
            if max_pages is not None and page > max(0, int(max_pages)):
                self._increment_stat("issue_list_page_cap_hit")
                break
            response = self._api_request(
                "/api/v1/issues/list",
                params={
                    "search": json.dumps(
                        self._team_dashboard_jira_issue_list_search_payload(
                            user_ids,
                            page=page,
                            page_size=page_size,
                            field_name=field_name,
                            fix_version_ids=fix_version_ids,
                        )
                    )
                },
            )
            self._increment_stat("issue_list_page_count")
            page_rows = self._extract_issue_rows_from_response(response)
            self._increment_stat("issue_rows_scanned", len(page_rows))
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            if created_cutoff and page_rows and self._all_rows_before_created_cutoff(page_rows, created_cutoff):
                self._increment_stat("issue_list_created_cutoff_hit")
                break
            page += 1
        return rows

    def _list_team_dashboard_jira_task_rows_via_tree(
        self,
        user_ids: list[int],
        *,
        fix_version_ids: list[int],
        max_pages: int | None,
        created_cutoff: datetime | None,
    ) -> list[dict[str, Any]] | None:
        rows_by_key: dict[str, dict[str, Any]] = {}
        field_names = ("reporter", "jiraRegionalPmPicId")
        worker_count = self._worker_count("TEAM_DASHBOARD_TREE_WORKERS", 2, 2)
        if worker_count > 1:
            field_results: dict[str, list[dict[str, Any]] | None] = {}
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = {
                    executor.submit(
                        self._list_team_dashboard_jira_task_rows_via_tree_or_fallback_field,
                        user_ids,
                        field_name=field_name,
                        fix_version_ids=fix_version_ids,
                        max_pages=max_pages,
                        created_cutoff=created_cutoff,
                    ): field_name
                    for field_name in field_names
                }
                for future in as_completed(futures):
                    field_results[futures[future]] = future.result()
            for field_name in field_names:
                field_rows = field_results.get(field_name)
                if field_rows is None:
                    return None
                for row in field_rows:
                    dedupe_key = self._extract_issue_key_from_row(row) or self._extract_issue_identifier(row)
                    if not dedupe_key:
                        dedupe_key = f"row:{len(rows_by_key)}"
                    rows_by_key.setdefault(dedupe_key, row)
            return list(rows_by_key.values())

        for field_name in field_names:
            field_rows = self._list_team_dashboard_jira_task_rows_via_tree_or_fallback_field(
                user_ids,
                field_name=field_name,
                fix_version_ids=fix_version_ids,
                max_pages=max_pages,
                created_cutoff=created_cutoff,
            )
            if field_rows is None:
                return None
            for row in field_rows:
                dedupe_key = self._extract_issue_key_from_row(row) or self._extract_issue_identifier(row)
                if not dedupe_key:
                    dedupe_key = f"row:{len(rows_by_key)}"
                rows_by_key.setdefault(dedupe_key, row)
        return list(rows_by_key.values())

    def _list_team_dashboard_jira_task_rows_via_tree_or_fallback_field(
        self,
        user_ids: list[int],
        *,
        field_name: str,
        fix_version_ids: list[int],
        max_pages: int | None,
        created_cutoff: datetime | None,
    ) -> list[dict[str, Any]] | None:
        started_at = time.monotonic()
        try:
            return self._list_team_dashboard_jira_task_rows_via_tree_field(
                user_ids,
                field_name=field_name,
                fix_version_ids=fix_version_ids,
                max_pages=max_pages,
                created_cutoff=created_cutoff,
            )
        except (BPMISError, ValueError, TypeError) as error:
            self.event_logger.warning("Could not load Team Dashboard tasks via BPMIS issues/tree %s: %s", field_name, error)
            self._increment_stat("issue_tree_fallback_count")
            try:
                return self._list_team_dashboard_jira_task_rows_via_list(
                    user_ids,
                    max_pages=max_pages,
                    created_cutoff=created_cutoff,
                    field_name=field_name,
                    fix_version_ids=fix_version_ids or None,
                )
            except (BPMISError, ValueError, TypeError):
                return None
        finally:
            self._add_request_timing(f"issue_tree_{field_name}", started_at)

    def _list_team_dashboard_jira_task_rows_via_tree_field(
        self,
        user_ids: list[int],
        *,
        field_name: str,
        fix_version_ids: list[int],
        max_pages: int | None,
        created_cutoff: datetime | None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        page_size = 200
        while True:
            if max_pages is not None and page > max(0, int(max_pages)):
                self._increment_stat("issue_list_page_cap_hit")
                break
            response = self._api_request(
                "/api/v1/issues/tree",
                params={
                    "search": json.dumps(
                        self._team_dashboard_jira_issue_tree_search_payload(
                            user_ids,
                            field_name=field_name,
                            page=page,
                            page_size=page_size,
                            fix_version_ids=fix_version_ids,
                        )
                    )
                },
            )
            self._increment_stat("issue_tree_page_count")
            page_rows = self._extract_issue_rows_from_response(response)
            self._increment_stat("issue_tree_rows_scanned", len(page_rows))
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            if created_cutoff and page_rows and self._all_rows_before_created_cutoff(page_rows, created_cutoff):
                self._increment_stat("issue_list_created_cutoff_hit")
                break
            page += 1
        return rows

    def _team_dashboard_jira_issue_tree_search_payload(
        self,
        user_ids: list[int],
        *,
        field_name: str,
        page: int,
        page_size: int,
        fix_version_ids: list[int],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            field_name: user_ids,
            "typeId": self.TASK_TYPE_ID,
            "taskType": 1,
            "page": page,
            "pageSize": page_size,
        }
        if fix_version_ids:
            payload["fixVersionId"] = fix_version_ids
        return payload

    def _team_dashboard_jira_issue_list_search_payload(
        self,
        user_ids: list[int],
        *,
        page: int,
        page_size: int,
        field_name: str | None = None,
        fix_version_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        if field_name:
            creator_query: dict[str, Any] = {field_name: user_ids}
        else:
            creator_query = {
                "joinType": "or",
                "subQueries": [
                    {"reporter": user_ids},
                    {"jiraRegionalPmPicId": user_ids},
                ],
            }
        sub_queries: list[dict[str, Any]] = [
            {"typeId": [self.TASK_TYPE_ID]},
            creator_query,
        ]
        if fix_version_ids:
            sub_queries.append({"fixVersionId": fix_version_ids})
        return {
            "joinType": "and",
            "subQueries": sub_queries,
            "page": page,
            "pageSize": page_size,
            "mapping": True,
        }

    def _team_dashboard_release_version_ids(self, release_cutoff: datetime | None) -> list[int]:
        started_at = time.monotonic()
        with self._cache_lock:
            self._team_dashboard_release_versions_by_id = {}
        if not release_cutoff:
            self._add_request_timing("release_versions", started_at)
            return []
        rows: list[dict[str, Any]] = []
        page = 1
        page_size = 1000
        end_date = (release_cutoff + timedelta(days=730)).date().isoformat()
        start_date = release_cutoff.date().isoformat()
        try:
            while True:
                self._increment_stat("release_version_lookup_count")
                response = self._api_request(
                    "/api/v1/versions/list",
                    params={
                        "search": json.dumps(
                            {
                                "timelineEndBefore": end_date,
                                "timelineEndAfter": start_date,
                                "page": page,
                                "pageSize": page_size,
                            }
                        )
                    },
                )
                page_rows = self._extract_issue_rows_from_response(response)
                rows.extend(page_rows)
                if len(page_rows) < page_size:
                    break
                page += 1
        except (BPMISError, ValueError, TypeError) as error:
            self._increment_stat("release_version_lookup_failed_count")
            self.event_logger.warning("Could not load BPMIS release versions: %s", error)
            self._add_request_timing("release_versions", started_at)
            return []

        version_ids: list[int] = []
        seen_ids: set[int] = set()
        versions_by_id: dict[int, dict[str, Any]] = {}
        for row in rows:
            try:
                version_id = int(str(row.get("id") or "").strip())
            except (TypeError, ValueError):
                continue
            if version_id in seen_ids:
                continue
            seen_ids.add(version_id)
            versions_by_id[version_id] = row
            version_ids.append(version_id)
        with self._cache_lock:
            self._team_dashboard_release_versions_by_id = versions_by_id
        self._increment_stat("release_version_count", len(version_ids))
        self._add_request_timing("release_versions", started_at)
        return version_ids

    def _with_team_dashboard_release_version_detail(self, row: dict[str, Any]) -> dict[str, Any]:
        if not self._team_dashboard_release_versions_by_id:
            return row
        fix_version_value = self._find_first_value(row, "fixVersionId")
        enriched = self._enrich_team_dashboard_fix_version_value(fix_version_value)
        if enriched is fix_version_value:
            return row
        merged = dict(row)
        merged["fixVersionId"] = enriched
        return merged

    def _enrich_team_dashboard_fix_version_value(self, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, list):
            enriched_items = [self._enrich_team_dashboard_fix_version_value(item) for item in value]
            return enriched_items
        version_id = self._extract_team_dashboard_version_id(value)
        if version_id is None:
            return value
        version_detail = self._team_dashboard_release_versions_by_id.get(version_id)
        if not version_detail:
            return value
        if isinstance(value, dict):
            enriched = dict(version_detail)
            enriched.update(value)
            return enriched
        return dict(version_detail)

    @staticmethod
    def _extract_team_dashboard_version_id(value: Any) -> int | None:
        if isinstance(value, dict):
            for key in ("id", "value", "versionId", "fixVersionId"):
                parsed = BPMISDirectApiClient._extract_team_dashboard_version_id(value.get(key))
                if parsed is not None:
                    return parsed
            return None
        try:
            text = str(value or "").strip()
            return int(text) if text.isdigit() else None
        except (TypeError, ValueError):
            return None

    def _extract_issue_rows_from_response(self, response: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(response, dict):
            return []
        data = response.get("data")
        if isinstance(data, dict) and isinstance(data.get("rows"), list):
            return [row for row in data.get("rows") or [] if isinstance(row, dict)]
        return self._flatten_issue_tree_rows(data)

    def _flatten_issue_tree_rows(self, value: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if isinstance(value, list):
            for item in value:
                rows.extend(self._flatten_issue_tree_rows(item))
            return rows
        if not isinstance(value, dict):
            return rows
        if any(key in value for key in ("id", "jiraKey", "issueKey", "key", "parentIds", "summary", "fixVersionId")):
            rows.append(value)
        for child_key in ("children", "childIssues", "issues", "tasks", "list"):
            child_value = value.get(child_key)
            if isinstance(child_value, (dict, list)):
                rows.extend(self._flatten_issue_tree_rows(child_value))
        return rows

    def _bpmis_release_cutoff_subquery(self, release_cutoff: datetime) -> dict[str, Any]:
        cutoff_text = release_cutoff.date().isoformat()
        return {
            "joinType": "or",
            "subQueries": [
                {"releaseDate": {"gte": cutoff_text}},
                {"fixVersionId.timeline.release": {"gte": cutoff_text}},
                {"fixVersions.timeline.release": {"gte": cutoff_text}},
            ],
        }

    def _bpmis_release_query_filter_enabled(self, user_ids: list[int], release_cutoff: datetime | None) -> bool:
        del user_ids
        if not release_cutoff:
            return False
        # Deprecated: Team Dashboard now mirrors BPMIS Feature Pool by resolving
        # release windows through /versions/list and filtering issues by fixVersionId.
        self._increment_stat("bpmis_release_query_filter_disabled_count")
        return False

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

        deduped_rows: list[dict[str, Any]] = []
        enrichment_issue_ids: list[str] = []
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
                enrichment_issue_ids.append(issue_id)
            deduped_rows.append(row)

        bulk_details: dict[str, dict[str, Any]] | None = {}
        if enrichment_issue_ids:
            bulk_details = self._get_issue_details_via_list_bulk(enrichment_issue_ids)

        enriched_rows: list[dict[str, Any]] = []
        for row in deduped_rows:
            issue_id = self._extract_issue_identifier(row)
            if self._issue_requires_enrichment(row) and issue_id:
                detail = None
                if bulk_details is not None:
                    detail = bulk_details.get(issue_id)
                if detail is None:
                    detail = self.get_issue_detail(issue_id)
                if detail:
                    row = self._merge_issue_payloads(row, detail)
            enriched_rows.append(row)
        return enriched_rows

    def get_issue_detail(self, issue_id: str | int) -> dict[str, Any]:
        normalized_issue_id = str(issue_id).strip()
        if not normalized_issue_id:
            return {}
        cached = self._snapshot_issue_detail(normalized_issue_id)
        if cached is not None:
            return cached

        self._increment_stat("issue_detail_lookup_count")
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
                self._store_issue_detail(normalized_issue_id, detail)
                return detail
        detail = self._get_issue_detail_via_list(normalized_issue_id)
        if detail:
            self._store_issue_detail(normalized_issue_id, detail)
            return detail
        self._store_issue_detail(normalized_issue_id, {})
        return {}

    def _get_issue_detail_via_list(self, issue_id: str) -> dict[str, Any]:
        try:
            response = self._api_request(
                "/api/v1/issues/list",
                params={
                    "search": json.dumps(
                        {
                            "joinType": "and",
                            "subQueries": [{"id": [int(issue_id)]}],
                            "page": 1,
                            "pageSize": 1,
                            "mapping": True,
                        }
                    )
                },
            )
        except (BPMISError, ValueError):
            return {}
        rows = ((response.get("data") or {}).get("rows") or []) if isinstance(response, dict) else []
        for row in rows:
            if isinstance(row, dict) and self._extract_issue_identifier(row) == str(issue_id):
                return row
        return {}

    def _get_parent_issue_detail(self, issue_id: str) -> dict[str, Any]:
        normalized_issue_id = str(issue_id or "").strip()
        if not normalized_issue_id:
            return {}
        cached = self._snapshot_issue_detail(normalized_issue_id)
        if cached is not None:
            return cached
        self._increment_stat("issue_detail_single_fallback_count")
        detail = self._get_issue_detail_via_list(normalized_issue_id)
        if detail:
            self._store_issue_detail(normalized_issue_id, detail)
            return detail
        return self.get_issue_detail(normalized_issue_id)

    def _get_issue_details_via_list_bulk(self, issue_ids: list[str], *, chunk_size: int = 50) -> dict[str, dict[str, Any]] | None:
        normalized_issue_ids: list[int] = []
        seen_ids: set[str] = set()
        for issue_id in issue_ids:
            text = str(issue_id or "").strip()
            if not text or text in seen_ids:
                continue
            try:
                normalized_issue_ids.append(int(text))
            except ValueError:
                continue
            seen_ids.add(text)
        if not normalized_issue_ids:
            return {}

        started_at = time.monotonic()
        details: dict[str, dict[str, Any]] = {}
        chunks = self._chunks(normalized_issue_ids, chunk_size)
        worker_count = min(len(chunks), self._worker_count("TEAM_DASHBOARD_BPMIS_BULK_WORKERS", 4, 4))
        try:
            if worker_count > 1:
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    future_by_chunk = {
                        executor.submit(self._get_issue_details_via_list_bulk_chunk, issue_id_chunk): issue_id_chunk
                        for issue_id_chunk in chunks
                    }
                    for future in as_completed(future_by_chunk):
                        chunk_details = future.result()
                        details.update(chunk_details)
            else:
                for issue_id_chunk in chunks:
                    details.update(self._get_issue_details_via_list_bulk_chunk(issue_id_chunk))
        except (BPMISError, ValueError, TypeError) as error:
            self.event_logger.warning("Could not bulk load BPMIS issue details: %s", error)
            return None
        finally:
            self._add_request_timing("parent_detail_bulk", started_at)
        return details

    def _get_issue_details_via_list_bulk_chunk(self, issue_id_chunk: list[int]) -> dict[str, dict[str, Any]]:
        self._increment_stat("issue_detail_bulk_lookup_count")
        response = self._api_request(
            "/api/v1/issues/list",
            params={
                "search": json.dumps(
                    {
                        "joinType": "and",
                        "subQueries": [{"id": issue_id_chunk}],
                        "page": 1,
                        "pageSize": max(1, len(issue_id_chunk)),
                        "mapping": True,
                    }
                )
            },
        )
        rows = ((response.get("data") or {}).get("rows") or []) if isinstance(response, dict) else []
        chunk_ids = {str(issue_id) for issue_id in issue_id_chunk}
        details: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            detail_id = self._extract_issue_identifier(row)
            if not detail_id or detail_id not in chunk_ids:
                continue
            details[detail_id] = row
            self._store_issue_detail(detail_id, row)
        self._increment_stat("issue_detail_bulk_issue_count", len([issue_id for issue_id in chunk_ids if issue_id in details]))
        return details

    def _prime_issue_detail_cache(self, issue_ids: list[str]) -> None:
        with self._cache_lock:
            missing_issue_ids = [
                str(issue_id or "").strip()
                for issue_id in issue_ids
                if str(issue_id or "").strip() and str(issue_id or "").strip() not in self._issue_detail_cache
            ]
        if missing_issue_ids:
            self._get_issue_details_via_list_bulk(missing_issue_ids)

    def _prime_biz_project_parent_details(self, rows: list[dict[str, Any]], *, max_depth: int = 5) -> None:
        pending = {
            parent_id
            for row in rows
            if isinstance(row, dict)
            for parent_id in self._extract_parent_issue_ids(row)
        }
        visited: set[str] = set()
        for _depth in range(max_depth):
            current_ids = sorted(issue_id for issue_id in pending if issue_id and issue_id not in visited)
            if not current_ids:
                break
            visited.update(current_ids)
            self._prime_issue_detail_cache(current_ids)
            next_pending: set[str] = set()
            for issue_id in current_ids:
                detail = self._snapshot_issue_detail(issue_id) or {}
                if not detail or self._is_biz_project_issue(detail):
                    continue
                next_pending.update(self._extract_parent_issue_ids(detail))
            pending = next_pending

    def get_jira_ticket_detail(self, ticket_key: str) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        if not normalized_ticket_key:
            return {}

        jira_detail = self._get_jira_ticket_detail_via_jira(normalized_ticket_key)
        if jira_detail is not None:
            return jira_detail

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

    def get_jira_ticket_details(self, ticket_keys: list[str]) -> dict[str, dict[str, Any]]:
        normalized_keys: list[str] = []
        for ticket_key in ticket_keys:
            normalized_key = self._normalize_jira_issue_key(ticket_key)
            if normalized_key and normalized_key not in normalized_keys:
                normalized_keys.append(normalized_key)
        if not normalized_keys:
            return {}

        bulk_details = self._get_jira_ticket_details_via_jira_bulk(normalized_keys)
        if bulk_details is not None and (bulk_details or self._jira_token()):
            return bulk_details

        details: dict[str, dict[str, Any]] = {}
        for ticket_key in normalized_keys:
            detail = self.get_jira_ticket_detail(ticket_key)
            detail_key = self._normalize_jira_issue_key(str(detail.get("jiraKey") or detail.get("key") or ticket_key))
            if detail_key and detail:
                details[detail_key] = detail
        return details

    def update_jira_ticket_status(self, ticket_key: str, status: str) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        normalized_status = self._normalize_jira_status(status)
        if not normalized_ticket_key:
            raise BPMISError("Jira ticket key is required.")
        if not normalized_status:
            raise BPMISError("Jira status is required.")

        direct_jira_detail = self._update_jira_ticket_status_via_jira(normalized_ticket_key, normalized_status)
        if direct_jira_detail is not None:
            return direct_jira_detail

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
        accepted_statuses: list[str] = []
        for body in bodies:
            for method, path in attempts:
                try:
                    self._api_request(path, method=method, body=body)
                    updated_detail = self.get_jira_ticket_detail(normalized_ticket_key)
                    updated_status = self._extract_jira_status_label(updated_detail)
                    if self._status_labels_match(updated_status, normalized_status):
                        return updated_detail
                    if updated_status:
                        accepted_statuses.append(updated_status)
                except BPMISError as error:
                    last_error = error
        if accepted_statuses:
            current_status = accepted_statuses[-1]
            raise BPMISError(
                f"BPMIS accepted the status update request, but Jira is still '{current_status}'. "
                "The current BPMIS API token does not appear to expose a working Jira workflow transition endpoint."
            )
        if last_error is not None:
            raise BPMISError(
                "Could not update Jira status through BPMIS. "
                "The current BPMIS API token does not expose a working Jira workflow transition endpoint. "
                f"Last error: {last_error}"
            ) from last_error
        raise BPMISError(
            "Could not update Jira status through BPMIS. "
            "The current BPMIS API token does not expose a working Jira workflow transition endpoint."
        )

    def update_jira_ticket_fix_version(self, ticket_key: str, version_name: str, version_id: str | None = None) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        normalized_version_name = str(version_name or "").strip()
        normalized_version_id = str(version_id or "").strip()
        if not normalized_ticket_key:
            raise BPMISError("Jira ticket key is required.")
        if not normalized_version_name and not normalized_version_id:
            raise BPMISError("Jira fix version is required.")

        version_payload = {"name": normalized_version_name} if normalized_version_name else {"id": normalized_version_id}
        direct_jira_detail = self._update_jira_ticket_fix_version_via_jira(normalized_ticket_key, version_payload)
        if direct_jira_detail is not None:
            return direct_jira_detail

        raise BPMISError(
            "Could not update Jira fix version. "
            "A direct Jira API token is required for editing an existing Jira ticket's Fix Version."
        )

    def link_jira_ticket_to_project(self, ticket_key: str, project_issue_id: str | int) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        normalized_project_id = str(project_issue_id or "").strip()
        if not normalized_ticket_key:
            raise BPMISError("Jira ticket key is required.")
        if not normalized_project_id:
            raise BPMISError("BPMIS project issue ID is required.")

        if self._issue_is_linked_to_parent(normalized_ticket_key, normalized_project_id):
            return self._verified_linked_jira_detail(normalized_ticket_key, normalized_project_id)

        issue_row = self._find_bpmis_task_row_for_jira_key(normalized_ticket_key)
        issue_id = self._extract_issue_identifier(issue_row)
        if not issue_id:
            self._add_existing_jira_ticket_to_project(normalized_ticket_key, normalized_project_id)
            if not self._issue_is_linked_to_parent(normalized_ticket_key, normalized_project_id):
                raise BPMISError(
                    "BPMIS accepted the Add Existing Jira request, but the Jira task is still not linked to this Biz Project."
                )
            return self._verified_linked_jira_detail(normalized_ticket_key, normalized_project_id)

        existing_parent_ids = self._extract_parent_issue_ids(issue_row)
        parent_ids = [int(parent_id) for parent_id in existing_parent_ids if str(parent_id).strip().isdigit()]
        project_parent_id = int(normalized_project_id)
        if project_parent_id not in parent_ids:
            parent_ids.append(project_parent_id)
        link_payload = {
            "id": [int(issue_id)],
            "parentIds": parent_ids,
            "parentIssueId": project_parent_id,
        }
        try:
            response = self._api_request(
                "/api/v1/issues/list",
                method="PUT",
                body=link_payload,
            )
            self._write_debug_capture(link_payload, response)
        except (BPMISError, ValueError) as error:
            raise BPMISError(
                "Could not link Jira task to BPMIS Biz Project through BPMIS. "
                f"Last error: {error}"
            ) from error

        if not self._issue_is_linked_to_parent(normalized_ticket_key, normalized_project_id):
            raise BPMISError("BPMIS accepted the link request, but the Jira task is still not linked to this Biz Project.")
        return self._verified_linked_jira_detail(normalized_ticket_key, normalized_project_id)

    def _add_existing_jira_ticket_to_project(self, ticket_key: str, project_issue_id: str | int) -> None:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        normalized_project_id = str(project_issue_id or "").strip()
        if not normalized_ticket_key:
            raise BPMISError("Jira ticket key is required.")
        if not normalized_project_id or not normalized_project_id.isdigit():
            raise BPMISError("BPMIS project issue ID must be numeric before adding an existing Jira ticket.")

        project_parent_id = int(normalized_project_id)
        ticket_link = self._normalize_ticket_link(normalized_ticket_key)
        payloads = [
            {
                "typeId": self.TASK_TYPE_ID,
                "parentIssueId": project_parent_id,
                "jiraLink": ticket_link,
            },
            {
                "typeId": self.TASK_TYPE_ID,
                "parentIssueId": project_parent_id,
                "jiraLink": normalized_ticket_key,
            },
            {
                "typeId": self.TASK_TYPE_ID,
                "parentIssueId": project_parent_id,
                "jiraKey": normalized_ticket_key,
            },
            {
                "typeId": self.TASK_TYPE_ID,
                "parentIssueId": project_parent_id,
                "key": normalized_ticket_key,
            },
        ]
        last_error: Exception | None = None
        for body in self._add_existing_jira_request_bodies(payloads):
            try:
                response = self._api_request(
                    "/api/v1/issues/batchCreateJiraIssue",
                    method="POST",
                    body=body,
                )
                self._write_debug_capture(body, response)
                batch_error = self._extract_batch_jira_issue_error(response)
                if batch_error:
                    last_error = BPMISError(batch_error)
                    continue
                if self._wait_until_jira_ticket_is_linked(normalized_ticket_key, normalized_project_id):
                    return
                last_error = BPMISError(
                    "BPMIS accepted the Add Existing Jira request, but verification did not find the Jira task under this Biz Project."
                )
            except (BPMISError, ValueError) as error:
                last_error = error
        detail = f" Last error: {last_error}" if last_error else ""
        raise BPMISError(
            "Could not add existing Jira task to BPMIS Biz Project through BPMIS."
            f"{detail}"
        )

    def _add_existing_jira_request_bodies(self, payloads: list[dict[str, Any]]) -> list[Any]:
        bodies: list[Any] = []
        for payload in payloads:
            bodies.append({"values": [payload]})
        for payload in payloads:
            bodies.append([payload])
        return bodies

    def _extract_batch_jira_issue_error(self, response: dict[str, Any]) -> str:
        data = response.get("data") or {}
        errors: list[str] = []
        for bucket_name in ("created", "add", "update", "failed", "errors"):
            bucket = data.get(bucket_name)
            if isinstance(bucket, dict):
                bucket = [bucket]
            if not isinstance(bucket, list):
                continue
            for item in bucket:
                if not isinstance(item, dict):
                    continue
                item_errors = item.get("errors") or item.get("error")
                if isinstance(item_errors, dict):
                    errors.extend(f"{key}: {value}" for key, value in item_errors.items())
                elif item_errors:
                    errors.append(str(item_errors))
        return "; ".join(error for error in errors if error)

    def _wait_until_jira_ticket_is_linked(self, ticket_key: str, project_issue_id: str | int) -> bool:
        for attempt in range(3):
            if self._issue_is_linked_to_parent(ticket_key, project_issue_id):
                return True
            if attempt < 2:
                time.sleep(0.5)
        return False

    def delink_jira_ticket_from_project(self, ticket_key: str, project_issue_id: str | int) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        normalized_project_id = str(project_issue_id or "").strip()
        if not normalized_ticket_key:
            raise BPMISError("Jira ticket key is required.")
        if not normalized_project_id:
            raise BPMISError("BPMIS project issue ID is required.")

        if not self._issue_is_linked_to_parent(normalized_ticket_key, normalized_project_id):
            return self.get_jira_ticket_detail(normalized_ticket_key)

        issue_id = self._find_linked_bpmis_task_id(normalized_ticket_key, normalized_project_id)
        if not issue_id:
            raise BPMISError(
                "Could not delink Jira task from BPMIS Biz Project. "
                "BPMIS did not return the linked task issue ID."
            )
        try:
            self._api_request(f"/api/v1/issues/removeTask/{issue_id}", method="DELETE")
        except BPMISError as error:
            raise BPMISError(
                "Could not delink Jira task from BPMIS Biz Project. "
                f"Last error: {error}"
            ) from error
        if self._issue_is_linked_to_parent(normalized_ticket_key, normalized_project_id):
            raise BPMISError("BPMIS accepted the delink request, but the Jira task is still linked to this Biz Project.")
        return self.get_jira_ticket_detail(normalized_ticket_key)

    def _find_bpmis_task_row_for_jira_key(self, ticket_key: str) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        if not normalized_ticket_key:
            return {}
        for search_payload in self._jira_ticket_search_payloads(normalized_ticket_key):
            payload = self._safe_api_request(
                "/api/v1/issues/list",
                params={"search": json.dumps(search_payload)},
            )
            rows = ((payload or {}).get("data") or {}).get("rows") or []
            match = next((row for row in rows if self._row_matches_jira_key(row, normalized_ticket_key)), None)
            if not isinstance(match, dict):
                continue
            issue_id = self._extract_issue_identifier(match)
            if not issue_id:
                return match
            detail = self.get_issue_detail(issue_id)
            return self._merge_issue_payloads(match, detail) if detail else match
        return {}

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

    def _extract_parent_issue_ids(self, row: dict[str, Any]) -> list[str]:
        parent_values = self._find_first_value(row, "parentIds")
        if parent_values is None:
            parent_values = self._find_first_value(row, "parentIssueId")
        if parent_values is None:
            parent_values = self._find_first_value(row, "parentId")
        if parent_values is None:
            return []
        if not isinstance(parent_values, list):
            parent_values = [parent_values]
        parent_ids: list[str] = []
        for parent_ref in parent_values:
            if isinstance(parent_ref, dict):
                value = parent_ref.get("id") or parent_ref.get("issueId") or parent_ref.get("value")
            else:
                value = parent_ref
            text = str(value or "").strip()
            if text:
                parent_ids.append(text)
        return parent_ids

    def _extract_parent_issue_payload(self, row: dict[str, Any], parent_id: str) -> dict[str, Any]:
        parent_values = self._find_first_value(row, "parentIds")
        if parent_values is None:
            parent_values = self._find_first_value(row, "parentIssueId")
        if parent_values is None:
            parent_values = self._find_first_value(row, "parentId")
        if parent_values is None:
            return {}
        if not isinstance(parent_values, list):
            parent_values = [parent_values]
        normalized_parent_id = str(parent_id or "").strip()
        for parent_ref in parent_values:
            if not isinstance(parent_ref, dict):
                continue
            candidate_id = str(parent_ref.get("id") or parent_ref.get("issueId") or parent_ref.get("value") or "").strip()
            if candidate_id and candidate_id == normalized_parent_id:
                return parent_ref
        return {}

    def _issue_is_linked_to_parent(self, ticket_key: str, project_issue_id: str | int) -> bool:
        normalized_project_id = str(project_issue_id or "").strip()
        detail = self.get_jira_ticket_detail(ticket_key)
        parent_ids = self._extract_parent_issue_ids(detail)
        if normalized_project_id in parent_ids:
            return True
        try:
            response = self._api_request(
                "/api/v1/issues/list",
                params={
                    "search": json.dumps(
                        {
                            "joinType": "and",
                            "subQueries": [
                                {"typeId": [self.TASK_TYPE_ID]},
                                {"parentIds": [int(normalized_project_id)]},
                            ],
                            "page": 1,
                            "pageSize": 50,
                            "mapping": True,
                        }
                    )
                },
            )
        except (BPMISError, ValueError):
            if parent_ids:
                return normalized_project_id in parent_ids
            raise
        rows = ((response.get("data") or {}).get("rows") or []) if isinstance(response, dict) else []
        return any(isinstance(row, dict) and self._row_matches_jira_key(row, ticket_key) for row in rows)

    def _verified_linked_jira_detail(self, ticket_key: str, project_issue_id: str | int) -> dict[str, Any]:
        normalized_project_id = str(project_issue_id or "").strip()
        linked_row = self._find_bpmis_task_row_for_jira_key(ticket_key)
        if linked_row and normalized_project_id in self._extract_parent_issue_ids(linked_row):
            return linked_row

        # The link was already verified through BPMIS. If the live Jira API is configured,
        # its native issue detail does not carry BPMIS parent fields, so preserve the verified
        # BPMIS parent in the returned payload for the portal-level cache/update step.
        detail = self.get_jira_ticket_detail(ticket_key)
        if normalized_project_id and normalized_project_id not in self._extract_parent_issue_ids(detail):
            detail = dict(detail)
            parent_ids = self._extract_parent_issue_ids(detail)
            parent_ids.append(normalized_project_id)
            detail["parentIds"] = parent_ids
        return detail

    def _find_linked_bpmis_task_id(self, ticket_key: str, project_issue_id: str | int) -> str:
        normalized_project_id = str(project_issue_id or "").strip()
        try:
            response = self._api_request(
                "/api/v1/issues/list",
                params={
                    "search": json.dumps(
                        {
                            "joinType": "and",
                            "subQueries": [
                                {"typeId": [self.TASK_TYPE_ID]},
                                {"parentIds": [int(normalized_project_id)]},
                            ],
                            "page": 1,
                            "pageSize": 50,
                            "mapping": True,
                        }
                    )
                },
            )
        except (BPMISError, ValueError):
            response = {}
        rows = ((response.get("data") or {}).get("rows") or []) if isinstance(response, dict) else []
        for row in rows:
            if isinstance(row, dict) and self._row_matches_jira_key(row, ticket_key):
                issue_id = self._extract_issue_identifier(row)
                if issue_id:
                    return issue_id

        detail = self.get_jira_ticket_detail(ticket_key)
        if "raw_jira" not in detail and normalized_project_id in self._extract_parent_issue_ids(detail):
            return self._extract_issue_identifier(detail)
        return ""

    def _row_matches_jira_key(self, row: dict[str, Any], ticket_key: str) -> bool:
        row_key = self._extract_issue_key_from_row(row)
        normalized_ticket_key = str(ticket_key or "").strip()
        return bool(row_key and normalized_ticket_key and row_key.lower() == normalized_ticket_key.lower())

    def _jira_ticket_search_payloads(self, ticket_key: str) -> list[dict[str, Any]]:
        base = {"page": 1, "pageSize": 10, "mapping": True}
        return [
            {**base, "typeId": self.TASK_TYPE_ID, "jiraLink": ticket_key},
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

    def _get_jira_ticket_detail_via_jira(self, ticket_key: str) -> dict[str, Any] | None:
        try:
            payload = self._jira_api_request(
                "GET",
                f"/rest/api/2/issue/{ticket_key}",
                params={"fields": "summary,status,fixVersions,components"},
            )
        except BPMISError as error:
            if self._jira_api_error_is_fallbackable(error):
                return None
            raise
        if payload is None:
            return None

        return self._normalize_live_jira_issue_payload(payload, fallback_ticket_key=ticket_key)

    def _get_jira_ticket_details_via_jira_bulk(self, ticket_keys: list[str]) -> dict[str, dict[str, Any]] | None:
        normalized_keys: list[str] = []
        for ticket_key in ticket_keys:
            normalized_key = self._normalize_jira_issue_key(ticket_key)
            if normalized_key and normalized_key not in normalized_keys:
                normalized_keys.append(normalized_key)
        if not normalized_keys or not self._jira_token():
            return {}

        started_at = time.monotonic()
        details: dict[str, dict[str, Any]] = {}
        chunks = [
            normalized_keys[index : index + JIRA_LIVE_BULK_DETAIL_CHUNK_SIZE]
            for index in range(0, len(normalized_keys), JIRA_LIVE_BULK_DETAIL_CHUNK_SIZE)
        ]
        worker_count = min(len(chunks), self._worker_count("TEAM_DASHBOARD_JIRA_BULK_WORKERS", 4, 4))
        try:
            if worker_count > 1:
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    future_by_chunk = {
                        executor.submit(self._get_jira_ticket_details_via_jira_bulk_chunk, chunk): chunk
                        for chunk in chunks
                    }
                    for future in as_completed(future_by_chunk):
                        chunk_details = future.result()
                        if chunk_details is None:
                            return None
                        details.update(chunk_details)
            else:
                for chunk in chunks:
                    chunk_details = self._get_jira_ticket_details_via_jira_bulk_chunk(chunk)
                    if chunk_details is None:
                        return None
                    details.update(chunk_details)
        finally:
            self._add_request_timing("jira_live_bulk", started_at)
        return details

    def _get_jira_ticket_details_via_jira_bulk_chunk(self, chunk: list[str]) -> dict[str, dict[str, Any]] | None:
        self._increment_stat("jira_live_bulk_lookup_count")
        try:
            payload = self._jira_api_request(
                "POST",
                "/rest/api/2/search",
                body={
                    "jql": f"key in ({', '.join(self._jira_jql_literal(key) for key in chunk)})",
                    "fields": ["summary", "status", "fixVersions", "components"],
                    "maxResults": len(chunk),
                },
            )
        except BPMISError as error:
            if self._jira_bulk_error_is_fallbackable(error):
                self.event_logger.warning("Could not bulk refresh live Jira fields: %s", error)
                return None
            raise
        if payload is None:
            return None
        issues = payload.get("issues") if isinstance(payload.get("issues"), list) else []
        self._increment_stat("jira_live_bulk_issue_count", len(issues))
        details: dict[str, dict[str, Any]] = {}
        for issue in issues:
            if not isinstance(issue, dict):
                continue
            detail = self._normalize_live_jira_issue_payload(issue)
            detail_key = self._normalize_jira_issue_key(str(detail.get("jiraKey") or detail.get("key") or ""))
            if detail_key:
                details[detail_key] = detail
        return details

    def _normalize_live_jira_issue_payload(self, payload: dict[str, Any], *, fallback_ticket_key: str = "") -> dict[str, Any]:
        fields = payload.get("fields") if isinstance(payload.get("fields"), dict) else {}
        status = fields.get("status") if isinstance(fields.get("status"), dict) else {}
        fix_versions = fields.get("fixVersions") if isinstance(fields.get("fixVersions"), list) else []
        components = fields.get("components") if isinstance(fields.get("components"), list) else []
        ticket_key = str(payload.get("key") or fallback_ticket_key).strip()
        return {
            "id": str(payload.get("id") or "").strip(),
            "jiraKey": ticket_key,
            "key": ticket_key,
            "summary": str(fields.get("summary") or "").strip(),
            "status": {"label": str(status.get("name") or "").strip()},
            "fixVersions": [
                str(item.get("name") or "").strip()
                for item in fix_versions
                if isinstance(item, dict) and str(item.get("name") or "").strip()
            ],
            "components": [
                str(item.get("name") or "").strip()
                for item in components
                if isinstance(item, dict) and str(item.get("name") or "").strip()
            ],
            "raw_jira": payload,
        }

    def _normalize_jira_issue_key(self, ticket_key: str) -> str:
        return (self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()).upper()

    @staticmethod
    def _jira_jql_literal(value: str) -> str:
        escaped = str(value or "").replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    @staticmethod
    def _jira_bulk_error_is_fallbackable(error: BPMISError) -> bool:
        message = str(error)
        return any(f"status {status}" in message for status in (400, 401, 403, 404))

    def _update_jira_ticket_status_via_jira(self, ticket_key: str, status: str) -> dict[str, Any] | None:
        try:
            transitions_payload = self._jira_api_request(
                "GET",
                f"/rest/api/2/issue/{ticket_key}/transitions",
            )
        except BPMISError as error:
            if self._jira_api_error_is_fallbackable(error):
                return None
            raise
        if transitions_payload is None:
            return None

        transitions = transitions_payload.get("transitions") or []
        transition = next(
            (
                item
                for item in transitions
                if self._status_labels_match(str(((item.get("to") or {}).get("name")) or item.get("name") or ""), status)
            ),
            None,
        )
        if not transition:
            available = ", ".join(
                str(((item.get("to") or {}).get("name")) or item.get("name") or "").strip()
                for item in transitions
                if str(((item.get("to") or {}).get("name")) or item.get("name") or "").strip()
            )
            raise BPMISError(
                f"Jira does not expose a transition from the current status to '{status}'."
                + (f" Available transitions: {available}." if available else "")
            )

        transition_id = str(transition.get("id") or "").strip()
        self._jira_api_request(
            "POST",
            f"/rest/api/2/issue/{ticket_key}/transitions",
            body={"transition": {"id": transition_id}},
            expected_statuses={200, 204},
            allow_empty=True,
        )

        detail = self._get_jira_ticket_detail_via_jira(ticket_key)
        if detail is None:
            return {"jiraKey": ticket_key, "status": {"label": status}}
        updated_status = self._extract_jira_status_label(detail)
        if updated_status and not self._status_labels_match(updated_status, status):
            raise BPMISError(f"Jira transition request completed, but Jira is still '{updated_status}'.")
        return detail

    def _update_jira_ticket_fix_version_via_jira(self, ticket_key: str, version_payload: dict[str, str]) -> dict[str, Any] | None:
        if not self._jira_token():
            return None
        try:
            self._jira_api_request(
                "PUT",
                f"/rest/api/2/issue/{ticket_key}",
                body={"fields": {"fixVersions": [version_payload]}},
                expected_statuses={200, 204},
                allow_empty=True,
            )
        except BPMISError as error:
            if self._jira_api_error_is_fallbackable(error):
                return None
            raise
        detail = self._get_jira_ticket_detail_via_jira(ticket_key)
        return detail or {"jiraKey": ticket_key, "fixVersions": [version_payload.get("name") or version_payload.get("id") or ""]}

    @staticmethod
    def _jira_api_error_is_fallbackable(error: BPMISError) -> bool:
        message = str(error)
        return "status 401" in message or "status 403" in message or "status 404" in message

    def _jira_api_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: Any | None = None,
        expected_statuses: set[int] | None = None,
        allow_empty: bool = False,
    ) -> dict[str, Any] | None:
        token = self._jira_token()
        if not token:
            return None

        expected = expected_statuses or {200}
        jira_base_url = self._jira_base_url()
        url = f"{jira_base_url}/{path.lstrip('/')}"
        candidates = self._jira_auth_candidates(token)
        last_status = 0
        last_text = ""
        for headers, auth in candidates:
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    params=params,
                    json=body if body is not None else None,
                    headers=headers,
                    auth=auth,
                    timeout=30,
                )
            except requests.RequestException as error:
                raise BPMISError(f"Jira API request failed for '{path}'.") from error

            last_status = int(response.status_code)
            last_text = response.text[:240]
            if response.status_code in {401, 403} and len(candidates) > 1:
                continue
            if response.status_code not in expected:
                detail = f": {last_text}" if last_text else "."
                raise BPMISError(f"Jira API request failed for '{path}' with status {response.status_code}{detail}")
            if response.status_code == 204 or not response.text.strip():
                return {} if allow_empty else {}
            try:
                payload = response.json()
            except ValueError as error:
                if allow_empty:
                    return {}
                raise BPMISError(f"Jira API returned non-JSON data for '{path}'.") from error
            return payload if isinstance(payload, dict) else {}

        raise BPMISError(f"Jira API request failed for '{path}' with status {last_status}: {last_text}")

    @staticmethod
    def _jira_token() -> str:
        return (
            os.getenv("JIRA_API_TOKEN")
            or os.getenv("JIRA_PAT")
            or os.getenv("JIRA_PERSONAL_ACCESS_TOKEN")
            or ""
        ).strip()

    def _jira_base_url(self) -> str:
        jira_base_url = (os.getenv("JIRA_BASE_URL") or self.JIRA_BROWSE_BASE_URL).strip().rstrip("/")
        if jira_base_url.endswith("/browse"):
            jira_base_url = jira_base_url[: -len("/browse")]
        return jira_base_url

    def _jira_auth_candidates(self, token: str) -> list[tuple[dict[str, str], tuple[str, str] | None]]:
        base_headers = {"Accept": "application/json", "Content-Type": "application/json"}
        username = (os.getenv("JIRA_USERNAME") or os.getenv("JIRA_EMAIL") or "").strip()
        if username:
            return [(base_headers, (username, token))]

        requested_scheme = str(os.getenv("JIRA_AUTH_SCHEME") or "").strip().lower()
        bearer = ({**base_headers, "Authorization": f"Bearer {token}"}, None)
        basic = ({**base_headers, "Authorization": f"Basic {token}"}, None)
        if requested_scheme == "bearer":
            return [bearer]
        if requested_scheme == "basic":
            return [basic]
        return [basic, bearer] if self._looks_like_basic_auth_blob(token) else [bearer, basic]

    @staticmethod
    def _looks_like_basic_auth_blob(token: str) -> bool:
        if not token:
            return False
        try:
            padded = token + "=" * (-len(token) % 4)
            decoded = base64.b64decode(padded, validate=True)
        except Exception:
            return False
        return b":" in decoded[:128]

    def _extract_jira_status_label(self, detail: dict[str, Any]) -> str:
        for key in ("status", "statusId", "jiraStatus", "jiraStatusId"):
            text = self._stringify_value(self._find_first_value(detail, key))
            if text:
                return text
        return ""

    def _with_live_jira_fields(self, row: dict[str, Any], ticket_key: str) -> dict[str, Any]:
        normalized_ticket_key = self._extract_issue_key(str(ticket_key or "")) or str(ticket_key or "").strip()
        if not normalized_ticket_key or not self._jira_token():
            return row
        self._increment_stat("jira_live_detail_lookup_count")
        try:
            detail = self._get_jira_ticket_detail_via_jira(normalized_ticket_key)
        except BPMISError as error:
            self.event_logger.warning("Could not refresh live Jira status for %s: %s", normalized_ticket_key, error)
            return row
        if not detail:
            return row

        return self._merge_live_jira_fields(row, detail)

    def _merge_live_jira_fields(self, row: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
        merged = dict(row)
        live_status = self._extract_jira_status_label(detail)
        current_status = self._extract_jira_status_label(row)
        if live_status:
            merged["status"] = {"label": live_status}
            merged["jiraStatus"] = {"label": live_status}
            if current_status and not self._status_labels_match(current_status, live_status):
                self._increment_stat("jira_live_status_override_count")

        live_summary = str(detail.get("summary") or "").strip()
        if live_summary:
            merged["summary"] = live_summary
        live_fix_versions = detail.get("fixVersions")
        if isinstance(live_fix_versions, list) and live_fix_versions:
            merged["fixVersions"] = live_fix_versions
        live_components = detail.get("components")
        if isinstance(live_components, list) and live_components:
            merged["components"] = live_components
        return merged

    @staticmethod
    def _status_labels_match(left: str, right: str) -> bool:
        return str(left or "").strip().casefold() == str(right or "").strip().casefold()

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

    def _parent_project_for_task(self, row: dict[str, Any], cache: dict[str, dict[str, Any]]) -> dict[str, Any]:
        parent_ids = self._extract_parent_issue_ids(row)
        parent_id = parent_ids[0] if parent_ids else ""
        if not parent_id:
            return self._normalize_team_dashboard_parent_project({})
        if parent_id not in cache:
            cache[parent_id] = self._resolve_biz_project_parent(row, parent_id, cache)
        return cache[parent_id]

    def _resolve_biz_project_parent(
        self,
        row: dict[str, Any],
        parent_id: str,
        cache: dict[str, dict[str, Any]],
        *,
        max_depth: int = 5,
    ) -> dict[str, Any]:
        current_id = str(parent_id or "").strip()
        current_payload = self._extract_parent_issue_payload(row, current_id)
        visited: set[str] = set()
        for _depth in range(max_depth):
            if not current_id or current_id in visited:
                break
            visited.add(current_id)
            detail = self._get_parent_issue_detail(current_id)
            payload = self._merge_issue_payloads(detail, current_payload) if detail else current_payload
            if self._is_biz_project_issue(payload):
                return self._normalize_team_dashboard_parent_project(payload, fallback_id=current_id)
            next_ids = self._extract_parent_issue_ids(payload)
            next_id = next_ids[0] if next_ids else ""
            if not next_id or next_id == current_id:
                break
            if next_id in cache and cache[next_id].get("bpmis_id"):
                return cache[next_id]
            current_id = next_id
            current_payload = self._extract_parent_issue_payload(payload, current_id)
        return self._normalize_team_dashboard_parent_project({})

    def _is_biz_project_issue(self, row: dict[str, Any]) -> bool:
        value = self._find_first_value(row, "typeId")
        if value is None:
            return False
        if isinstance(value, dict):
            candidates = [value.get("id"), value.get("value"), value.get("label"), value.get("name"), value.get("fullName")]
        else:
            candidates = [value]
        for candidate in candidates:
            text = str(candidate or "").strip()
            if not text:
                continue
            if text == str(self.BIZ_PROJECT_TYPE_ID) or text.casefold() == "biz project":
                return True
        return False

    def _creator_email_for_row(
        self,
        row: dict[str, Any],
        emails: list[str],
        user_ids_by_email: dict[str, set[str]],
    ) -> str:
        for email in emails:
            user_ids = user_ids_by_email.get(email) or set()
            for key in self._issue_creator_field_names():
                value = self._find_first_value(row, key)
                if self._value_matches_user(value, email, user_ids):
                    return email
        return ""

    def _biz_project_matched_pm_emails(
        self,
        row: dict[str, Any],
        emails: list[str],
        user_ids_by_email: dict[str, set[str]],
    ) -> list[str]:
        matched: list[str] = []
        for email in emails:
            user_ids = user_ids_by_email.get(email) or set()
            for key in (
                "regionalPmPicId",
                "regional_pm_pic",
                "regionalPmPic",
                "regionalPm",
                "involvedPM",
                "involvedPm",
                "pm",
                "pmEmail",
            ):
                value = self._find_first_value(row, key)
                if self._value_matches_user(value, email, user_ids):
                    matched.append(email)
                    break
        return list(dict.fromkeys(matched))

    def _issue_has_creator_value(self, row: dict[str, Any]) -> bool:
        return any(self._find_first_value(row, key) is not None for key in self._issue_creator_field_names())

    @staticmethod
    def _issue_creator_field_names() -> tuple[str, ...]:
        return (
            "reporter",
            "reporter.email",
            "reporter.emailAddress",
            "reporter.mail",
            "reporterEmail",
            "jiraRegionalPmPicId",
            "jiraRegionalPmPicId.email",
            "jiraRegionalPmPicId.emailAddress",
            "jiraRegionalPmPicId.mail",
        )

    def _issue_requires_user_enrichment(self, row: dict[str, Any], email: str) -> bool:
        if self._issue_reported_by(row, email):
            return False
        return not any(self._find_first_value(row, key) is not None for key in self._issue_reporter_field_names())

    def _issue_reported_by(self, row: dict[str, Any], email: str) -> bool:
        normalized_email = str(email or "").strip().lower()
        for key in self._issue_reporter_field_names():
            value = self._find_first_value(row, key)
            if self._value_matches_email(value, normalized_email):
                return True
        return False

    @staticmethod
    def _issue_reporter_field_names() -> tuple[str, ...]:
        return (
            "reporter.email",
            "reporter.emailAddress",
            "reporter.mail",
            "reporter",
            "reporterEmail",
        )

    def _value_matches_email(self, value: Any, email: str) -> bool:
        if value is None:
            return False
        if isinstance(value, list):
            return any(self._value_matches_email(item, email) for item in value)
        if isinstance(value, dict):
            for key in ("email", "emailAddress", "mail"):
                if self._value_matches_email(value.get(key), email):
                    return True
            return False
        return bool(email and str(value).strip().lower() == email)

    def _value_matches_user(self, value: Any, email: str, user_ids: set[str]) -> bool:
        if value is None:
            return False
        if isinstance(value, list):
            return any(self._value_matches_user(item, email, user_ids) for item in value)
        if isinstance(value, dict):
            for key in ("emailAddress", "email", "mail", "username", "name", "displayName", "label", "value"):
                if self._value_matches_user(value.get(key), email, user_ids):
                    return True
            for key in ("id", "userId", "jiraUserId", "value"):
                raw_id = value.get(key)
                if raw_id is not None and str(raw_id).strip() in user_ids:
                    return True
            return False
        text = str(value or "").strip()
        if not text:
            return False
        lowered = text.lower()
        if email and (lowered == email or email in lowered):
            return True
        return text in user_ids

    def _normalize_project_jira_task(self, row: dict[str, Any]) -> dict[str, Any]:
        ticket_key = self._extract_issue_key_from_row(row)
        ticket_link = self._normalize_ticket_link(self._issue_first_text(row, "jiraLink", "ticketLink", "link", "self") or ticket_key)
        prd_links = self._extract_issue_prd_links(row)
        fix_version = self._issue_first_text(row, "fixVersionId", "fixVersion", "fixVersions", "version", "versions")
        return {
            "component": self._issue_first_text(row, "componentId", "component", "components"),
            "market": self._issue_first_text(row, "marketId", "market", "country"),
            "system": self._issue_first_text(row, "system", "systemId", "track", "scope"),
            "jira_title": self._issue_first_text(row, "summary", "title", "jiraSummary"),
            "prd_link": "\n".join(prd_links),
            "description": self._extract_issue_description(row),
            "fix_version_name": fix_version,
            "ticket_key": ticket_key,
            "ticket_link": ticket_link or "",
            "status": self._extract_jira_status_label(row),
            "message": "Imported from BPMIS project sync.",
            "raw_response": row,
        }

    def _normalize_team_dashboard_jira_task(
        self,
        row: dict[str, Any],
        *,
        pm_email: str,
        parent_project: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ticket_key = self._extract_issue_key_from_row(row)
        ticket_link = self._normalize_ticket_link(self._issue_first_text(row, "jiraLink", "ticketLink", "link", "self") or ticket_key)
        prd_links = self._extract_issue_prd_links(row)
        return {
            "issue_id": self._extract_issue_identifier(row),
            "jira_id": ticket_key,
            "ticket_key": ticket_key,
            "jira_link": ticket_link or "",
            "ticket_link": ticket_link or "",
            "jira_title": self._issue_first_text(row, "summary", "title", "jiraSummary"),
            "component": self._issue_first_text(row, "componentId", "component", "components"),
            "market": self._issue_first_text(row, "marketId", "market", "country"),
            "system": self._issue_first_text(row, "system", "systemId", "track", "scope"),
            "pm_email": pm_email,
            "jira_status": self._extract_jira_status_label(row),
            "status": self._extract_jira_status_label(row),
            "created_at": self._extract_issue_created_at_text(row),
            "release_date": self._extract_issue_release_date_text(row),
            "version": self._extract_issue_version_text(row),
            "fix_version_name": self._extract_issue_version_text(row),
            "prd_links": prd_links,
            "prd_link": "\n".join(prd_links),
            "parent_project": parent_project or self._normalize_team_dashboard_parent_project({}),
            "raw_response": row,
        }

    def _issue_created_on_or_after(self, row: dict[str, Any], cutoff: datetime) -> bool:
        created_at = self._parse_issue_datetime(self._extract_issue_created_at_text(row))
        return bool(created_at and created_at >= cutoff)

    def _issue_release_on_or_after(self, row: dict[str, Any], cutoff: datetime) -> bool:
        release_at = self._parse_issue_datetime(self._extract_issue_release_date_text(row))
        return bool(release_at and release_at >= cutoff)

    def _all_rows_before_created_cutoff(self, rows: list[dict[str, Any]], cutoff: datetime) -> bool:
        parsed_dates = [
            parsed
            for row in rows
            if (parsed := self._parse_issue_datetime(self._extract_issue_created_at_text(row))) is not None
        ]
        return bool(parsed_dates) and len(parsed_dates) == len(rows) and all(parsed < cutoff for parsed in parsed_dates)

    def _extract_issue_created_at_text(self, row: dict[str, Any]) -> str:
        for key in (
            "created",
            "createdAt",
            "created_at",
            "createTime",
            "create_time",
            "createdTime",
            "createdDate",
            "createDate",
            "gmtCreate",
            "gmtCreated",
            "jiraCreated",
            "issueCreatedAt",
            "jiraCreatedAt",
        ):
            text = self._stringify_value(self._find_first_value(row, key))
            if text:
                return text
        return ""

    def _extract_issue_release_date_text(self, row: dict[str, Any]) -> str:
        for key in ("releaseDate", "release_date", "release", "goliveDate", "goLiveDate", "golive"):
            text = self._stringify_value(self._find_first_value(row, key))
            if text:
                return text
        for key in ("fixVersionId", "fixVersion", "fixVersions", "version", "versions"):
            text = self._extract_release_date_from_version_value(self._find_first_value(row, key))
            if text:
                return text
        return ""

    def _extract_issue_version_text(self, row: dict[str, Any]) -> str:
        fallback = ""
        for key in ("fixVersions", "fixVersion", "version", "versions", "fixVersionId"):
            text = self._issue_first_text(row, key)
            if not text:
                continue
            if not text.isdigit():
                return text
            fallback = fallback or text
        return fallback

    def _extract_release_date_from_version_value(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            for item in value:
                text = self._extract_release_date_from_version_value(item)
                if text:
                    return text
            return ""
        if not isinstance(value, dict):
            return ""
        for key in (
            "release",
            "releaseDate",
            "release_date",
            "goliveDate",
            "goLiveDate",
            "golive",
            "timelineEnd",
            "timelineEndDate",
            "timeline_end",
            "endDate",
            "end",
        ):
            text = self._stringify_value(value.get(key))
            if text:
                return text
        timeline = value.get("timeline")
        if isinstance(timeline, dict):
            for key in ("release", "golive", "goLive", "releaseDate", "goliveDate", "timelineEnd", "endDate", "end"):
                text = self._stringify_value(timeline.get(key))
                if text:
                    return text
        if isinstance(timeline, list):
            for item in timeline:
                if not isinstance(item, dict):
                    continue
                label = str(item.get("label") or item.get("name") or "").strip().casefold()
                if label not in {"release", "golive", "go live", "go-live"}:
                    continue
                text = self._stringify_value(item.get("value"))
                if text:
                    return text
        return ""

    def _parse_issue_datetime(self, value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, date):
            parsed = datetime.combine(value, datetime.min.time())
        elif isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            try:
                parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
        else:
            text = str(value or "").strip()
            if not text:
                return None
            if text.isdigit():
                return self._parse_issue_datetime(int(text))
            normalized = text.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                parsed = None
                for pattern in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
                    try:
                        parsed = datetime.strptime(text[:19] if "%H" in pattern else text[:10], pattern)
                        break
                    except ValueError:
                        continue
                if parsed is None:
                    return None
        if parsed.tzinfo is not None:
            parsed = parsed.replace(tzinfo=None)
        return parsed

    def _normalize_team_dashboard_parent_project(self, row: dict[str, Any], *, fallback_id: str = "") -> dict[str, Any]:
        bpmis_id = self._extract_issue_identifier(row) or str(fallback_id or "").strip()
        return {
            "bpmis_id": bpmis_id,
            "project_name": self._issue_first_text(row, "summary", "title", "projectName", "name"),
            "market": self._issue_first_text(row, "marketId", "market", "country"),
            "priority": self._issue_first_text(row, "bizPriorityId", "bizPriority", "priority", "priorityId"),
            "regional_pm_pic": self._issue_first_person(
                row,
                "regionalPmPicId",
                "jiraRegionalPmPicId",
                "regionalPmPic",
                "pmPic",
                "involvedPM",
                "involvedPm",
                "involvedPMId",
            ),
        }

    def _team_dashboard_project_requires_enrichment(self, project: dict[str, Any]) -> bool:
        return not (
            str(project.get("project_name") or "").strip()
            and str(project.get("market") or "").strip()
            and str(project.get("priority") or "").strip()
            and str(project.get("regional_pm_pic") or "").strip()
        )

    def _normalize_team_dashboard_biz_project_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, str]]:
        deduped: dict[str, dict[str, str]] = {}
        self._prime_issue_detail_cache(
            [
                str(row.get("id") or row.get("issue_id") or row.get("bpmis_id") or "").strip()
                for row in rows
                if isinstance(row, dict)
                and self._team_dashboard_project_requires_enrichment(
                    self._normalize_team_dashboard_parent_project(
                        row,
                        fallback_id=str(row.get("id") or row.get("issue_id") or row.get("bpmis_id") or "").strip(),
                    )
                )
            ]
        )
        for row in rows:
            issue_id = str(row.get("id") or row.get("issue_id") or row.get("bpmis_id") or "").strip()
            if not issue_id or issue_id in deduped:
                continue
            if not self._is_team_dashboard_biz_project_status_allowed(row):
                continue
            project = self._normalize_team_dashboard_parent_project(row, fallback_id=issue_id)
            if self._team_dashboard_project_requires_enrichment(project):
                detail = self._get_parent_issue_detail(issue_id)
                if detail:
                    row = self._merge_issue_payloads(row, detail)
                    if not self._is_team_dashboard_biz_project_status_allowed(row):
                        continue
                    project = self._normalize_team_dashboard_parent_project(row, fallback_id=issue_id)
            deduped[issue_id] = {
                "issue_id": issue_id,
                "bpmis_id": str(project.get("bpmis_id") or issue_id),
                "project_name": str(project.get("project_name") or ""),
                "market": str(project.get("market") or ""),
                "priority": str(project.get("priority") or ""),
                "regional_pm_pic": str(project.get("regional_pm_pic") or ""),
                "status": self._team_dashboard_biz_project_status_label(row),
            }
        return list(deduped.values())

    def _team_dashboard_biz_project_status_label(self, row: dict[str, Any]) -> str:
        return self._issue_first_text(row, "statusId", "status", "statusName", "issueStatus")

    def _is_team_dashboard_biz_project_status_allowed(self, row: dict[str, Any]) -> bool:
        status = self._team_dashboard_biz_project_status_label(row)
        if not status:
            return True
        normalized_status = status.strip().lower()
        return (
            normalized_status in self.TEAM_DASHBOARD_BIZ_PROJECT_STATUS_NAMES
            or normalized_status in self.TEAM_DASHBOARD_BIZ_PROJECT_STATUS_ID_VALUES
        )

    def _issue_first_text(self, row: dict[str, Any], *keys: str) -> str:
        for key in keys:
            text = self._stringify_value(self._find_first_value(row, key))
            if text:
                return text
        return ""

    def _issue_first_person(self, row: dict[str, Any], *keys: str) -> str:
        for key in keys:
            text = self._stringify_person(self._find_first_value(row, key))
            if text:
                return text
        return ""

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
            for key in ("fullName", "label", "name", "displayName", "emailAddress", "email", "value"):
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
            for key in ("displayName", "name", "emailAddress", "email", "label", "username", "value"):
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
        request_index = self._increment_stat("api_call_count")
        started_at = time.monotonic()
        log_context = {
            "event": "bpmis_api_request",
            "path": path,
            "method": method,
            "request_index": request_index,
            "params": self._summarize_api_params(params),
            "has_body": body is not None,
        }
        self.event_logger.warning(
            "bpmis_event %s",
            json.dumps(
                {**log_context, "event": "bpmis_api_request_start"},
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        try:
            response = self._bpmis_session_for_current_thread().request(
                method=method,
                url=url,
                params=params,
                json=body if body is not None else None,
                timeout=60,
            )
        except requests.RequestException as error:
            elapsed_seconds = round(time.monotonic() - started_at, 3)
            self.event_logger.warning(
                "bpmis_event %s",
                json.dumps(
                    {
                        **log_context,
                        "event": "bpmis_api_request_error",
                        "elapsed_seconds": elapsed_seconds,
                        "error_type": type(error).__name__,
                        "error_message": str(error),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            raise BPMISError(f"BPMIS API request failed for '{path}': {error}") from error

        if response.status_code >= 400:
            self._log_bpmis_api_result(
                log_context,
                started_at=started_at,
                status_code=response.status_code,
                payload=None,
                level=logging.WARNING,
            )
            raise BPMISError(f"BPMIS API request failed for '{path}' with status {response.status_code}.")

        try:
            payload = response.json()
        except ValueError as error:
            self._log_bpmis_api_result(
                log_context,
                started_at=started_at,
                status_code=response.status_code,
                payload=None,
                level=logging.WARNING,
                extra={"json_error": type(error).__name__},
            )
            raise BPMISError(f"BPMIS API returned non-JSON data for '{path}'.") from error

        if payload.get("code") not in {0, None}:
            self._log_bpmis_api_result(
                log_context,
                started_at=started_at,
                status_code=response.status_code,
                payload=payload,
                level=logging.WARNING,
            )
            raise BPMISError(payload.get("message") or f"BPMIS API error for '{path}'.")
        self._log_bpmis_api_result(log_context, started_at=started_at, status_code=response.status_code, payload=payload)
        return payload

    def _log_bpmis_api_result(
        self,
        log_context: dict[str, Any],
        *,
        started_at: float,
        status_code: int,
        payload: dict[str, Any] | None,
        level: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        elapsed_seconds = round(time.monotonic() - started_at, 3)
        inferred_level = level if level is not None else (
            logging.WARNING if elapsed_seconds >= BPMIS_SLOW_REQUEST_SECONDS else logging.INFO
        )
        self.event_logger.log(
            inferred_level,
            "bpmis_event %s",
            json.dumps(
                {
                    **log_context,
                    "event": "bpmis_api_request_done",
                    "elapsed_seconds": elapsed_seconds,
                    "status_code": status_code,
                    **self._summarize_api_payload(payload),
                    **(extra or {}),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        )

    @staticmethod
    def _summarize_api_params(params: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(params, dict) or not params:
            return {}
        summary: dict[str, Any] = {"keys": sorted(str(key) for key in params)}
        search = params.get("search")
        if isinstance(search, str):
            try:
                parsed = json.loads(search)
            except json.JSONDecodeError:
                summary["search_type"] = "raw"
            else:
                summary["search_type"] = type(parsed).__name__
                if isinstance(parsed, dict):
                    for key in ("page", "pageSize", "mapping", "joinType"):
                        if key in parsed:
                            summary[key] = parsed.get(key)
                    sub_queries = parsed.get("subQueries")
                    if isinstance(sub_queries, list):
                        summary["subquery_count"] = len(sub_queries)
                        summary["subquery_keys"] = [
                            sorted(str(sub_key) for sub_key in item)
                            for item in sub_queries
                            if isinstance(item, dict)
                        ]
                elif isinstance(parsed, list):
                    summary["search_item_count"] = len(parsed)
        return summary

    @staticmethod
    def _summarize_api_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {"payload_type": type(payload).__name__}
        data = payload.get("data")
        summary: dict[str, Any] = {
            "payload_code": payload.get("code"),
            "data_type": type(data).__name__,
        }
        if isinstance(data, dict):
            rows = data.get("rows")
            if isinstance(rows, list):
                summary["row_count"] = len(rows)
            if "total" in data:
                summary["total"] = data.get("total")
        elif isinstance(data, list):
            summary["data_count"] = len(data)
        return summary

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
        return self._resolve_bpmis_user_ids_by_emails([email]).get(str(email or "").strip().lower(), [])

    def _resolve_bpmis_user_ids_by_emails(self, emails: list[str]) -> dict[str, list[int]]:
        normalized_emails = self._normalize_email_list(emails)
        resolved: dict[str, list[int]] = {}
        with self._cache_lock:
            missing = [email for email in normalized_emails if email not in self._bpmis_user_ids_by_email_cache]
        if missing:
            self._increment_stat("user_lookup_count")
            response = self._api_request(
                "/api/v1/users/listByEmail",
                params={"search": json.dumps(missing)},
            )
            users = response.get("data") or []
            grouped: dict[str, list[int]] = {email: [] for email in missing}
            if isinstance(users, list):
                for user in users:
                    if not isinstance(user, dict) or user.get("id") is None:
                        continue
                    user_id = int(user["id"])
                    matched_emails = self._emails_for_bpmis_user(user, missing)
                    if not matched_emails and len(missing) == 1:
                        matched_emails = missing
                    for matched_email in matched_emails:
                        if user_id not in grouped[matched_email]:
                            grouped[matched_email].append(user_id)
            with self._cache_lock:
                for email in missing:
                    self._bpmis_user_ids_by_email_cache[email] = grouped.get(email) or []
        with self._cache_lock:
            for email in normalized_emails:
                resolved[email] = list(self._bpmis_user_ids_by_email_cache.get(email) or [])
        return resolved

    @staticmethod
    def _chunks(items: list[Any], size: int) -> list[list[Any]]:
        chunk_size = max(1, int(size or 1))
        return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]

    def _emails_for_bpmis_user(self, user: dict[str, Any], candidate_emails: list[str]) -> list[str]:
        matched: list[str] = []
        for key in ("email", "emailAddress", "mail", "username", "name", "displayName", "label", "value"):
            value = user.get(key)
            if value is None:
                continue
            text = str(value).strip().lower()
            for email in candidate_emails:
                if email and (text == email or email in text):
                    matched.append(email)
        return list(dict.fromkeys(matched))

    @staticmethod
    def _normalize_email_list(emails: list[str]) -> list[str]:
        normalized: list[str] = []
        for raw_email in emails:
            email = str(raw_email or "").strip().lower()
            if email and email not in normalized:
                normalized.append(email)
        return normalized

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
