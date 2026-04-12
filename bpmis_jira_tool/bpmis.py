from __future__ import annotations

import json
import re
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import requests

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError, BPMISNotConfiguredError
from bpmis_jira_tool.models import CreatedTicket, ProjectMatch


ISSUE_KEY_PATTERN = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")


def _lookup_path(payload: Any, path: str | None) -> Any:
    if path is None or path == "":
        return payload

    current = payload
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        raise BPMISError(f"Could not find response path '{path}'.")
    return current


class BPMISClient(ABC):
    @abstractmethod
    def find_project(self, issue_id: str) -> ProjectMatch:
        raise NotImplementedError

    @abstractmethod
    def create_jira_ticket(self, project: ProjectMatch, fields: dict[str, str]) -> CreatedTicket:
        raise NotImplementedError

    @abstractmethod
    def submit_sdlc_approval(self, approval: dict[str, str]) -> dict[str, Any]:
        raise NotImplementedError


class BPMISHelperClient(BPMISClient):
    def __init__(self, helper_base_url: str):
        self.helper_base_url = helper_base_url.rstrip("/")

    def find_project(self, issue_id: str) -> ProjectMatch:
        return ProjectMatch(project_id=issue_id, raw={"issueId": issue_id, "source": "team-helper"})

    def create_jira_ticket(self, project: ProjectMatch, fields: dict[str, str]) -> CreatedTicket:
        try:
            response = requests.post(
                f"{self.helper_base_url}/bpmis/create-jira",
                json={"issue_id": project.project_id, "fields": fields},
                timeout=120,
            )
        except requests.RequestException as error:
            raise BPMISError(
                f"Could not reach local helper at {self.helper_base_url}. Please start the helper and try again."
            ) from error

        try:
            payload = response.json()
        except ValueError as error:
            raise BPMISError("Local helper returned a non-JSON response.") from error

        if response.status_code >= 400 or payload.get("status") == "error":
            raise BPMISError(payload.get("message") or "Local helper could not create the Jira ticket.")

        ticket_key = payload.get("ticket_key")
        ticket_link = payload.get("ticket_link")
        if not ticket_key and not ticket_link:
            raise BPMISError("Local helper did not return a Jira ticket key.")

        return CreatedTicket(ticket_key=ticket_key, ticket_link=ticket_link, raw=payload)

    def submit_sdlc_approval(self, approval: dict[str, str]) -> dict[str, Any]:
        try:
            response = requests.post(
                f"{self.helper_base_url}/sdlc/submit-approval",
                json=approval,
                timeout=120,
            )
        except requests.RequestException as error:
            raise BPMISError(
                f"Could not reach local helper at {self.helper_base_url}. Please start the helper and try again."
            ) from error

        try:
            payload = response.json()
        except ValueError as error:
            raise BPMISError("Local helper returned a non-JSON response.") from error

        if response.status_code >= 400 or payload.get("status") == "error":
            raise BPMISError(payload.get("message") or "Local helper could not submit SDLC approval.")

        return payload


class BPMISPageApiClient(BPMISClient):
    TASK_TYPE_ID = 4
    SUPPORTED_COUNTRIES_ALL_VALUE = 49007
    JIRA_BROWSE_BASE_URL = "https://jira.shopee.io/browse/"
    SDLC_MANAGEMENT_URLS = {
        "ID": "https://sdlc.npt.seabank.io/workflow/id/management",
        "SG": "https://sdlc.npt.seabank.io/workflow/sg/management",
        "PH": "https://sdlc.npt.seabank.io/workflow/ph/management",
        "REGIONAL": "https://sdlc.npt.seabank.io/workflow/prod/management",
    }
    TASK_TYPE_PREFIX = {
        "feature": "[Feature]",
        "tech": "[Tech]",
        "support": "[Support]",
    }

    def __init__(self, settings: Settings):
        self.settings = settings
        self._field_defs_cache: dict[str, Any] | None = None
        self._group_options_cache: dict[str, list[dict[str, Any]]] = {}

    def find_project(self, issue_id: str) -> ProjectMatch:
        self._require_cdp()
        return ProjectMatch(
            project_id=issue_id,
            raw={
                "issueId": issue_id,
                "url": self._resolve_project_url(issue_id),
            },
        )

    def create_jira_ticket(self, project: ProjectMatch, fields: dict[str, str]) -> CreatedTicket:
        self._require_cdp()
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            browser = self._connect_browser(playwright)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = self._pick_existing_bpmis_page(context)
            try:
                payload = self._build_create_payload(page, project, fields)
                response = self._api_request(
                    page,
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
                return CreatedTicket(
                    ticket_key=ticket_key,
                    ticket_link=ticket_link,
                    raw=response,
                )
            finally:
                pass

    def submit_sdlc_approval(self, approval: dict[str, str]) -> dict[str, Any]:
        self._require_cdp()
        market = str(approval.get("market") or "").strip().upper()
        if not market:
            raise BPMISError("SDLC approval requires Market.")

        capture_template_path = Path(__file__).resolve().parent.parent / "tmp" / "last_sdlc_api_template.json"
        if not capture_template_path.exists():
            raise BPMISError(
                "SDLC API template has not been captured yet. "
                "Please run one manual SDLC submission capture first."
            )

        template = json.loads(capture_template_path.read_text(encoding="utf-8"))
        endpoint = str(template.get("url") or "").strip()
        method = str(template.get("method") or "POST").strip().upper()
        request_headers = dict(template.get("headers") or {})
        body = template.get("body")
        if not endpoint or body is None:
            raise BPMISError("Captured SDLC API template is incomplete. Please capture again.")

        body_text = json.dumps(body, ensure_ascii=False)
        replacements = {
            "__TITLE__": approval.get("title", ""),
            "__CONTENT__": approval.get("content", ""),
            "__BUSINESS_LEAD__": approval.get("business_lead", ""),
            "__JIRA_TICKET_LINK__": approval.get("jira_ticket_link", ""),
            "__JIRA_TICKET_KEY__": approval.get("jira_ticket_key", ""),
            "__ISSUE_ID__": approval.get("issue_id", ""),
            "__MARKET__": market,
        }
        for placeholder, value in replacements.items():
            body_text = body_text.replace(placeholder, str(value))

        try:
            request_body = json.loads(body_text)
        except json.JSONDecodeError as error:
            raise BPMISError("Captured SDLC API template could not be rendered into valid JSON.") from error

        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            browser = self._connect_browser(playwright)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = self._pick_existing_sdlc_page(context, market)
            approval_name = str(request_body.get("name") or "").strip()
            if not approval_name:
                raise BPMISError("Captured SDLC API template is missing approval name.")

            business_lead_id = self._resolve_sdlc_business_lead_id(
                page,
                market=market,
                approval_name=approval_name,
                lead_value=str(approval.get("business_lead") or "").strip(),
                request_headers=request_headers,
            )
            node_array = request_body.get("nodeArray")
            if isinstance(node_array, dict):
                for key, value in node_array.items():
                    if isinstance(value, list):
                        node_array[key] = [business_lead_id]

            response = self._api_request(
                page,
                endpoint,
                method=method,
                body=request_body,
                headers=request_headers,
            )
            self._write_sdlc_debug_capture(request_body, response)
            return response

    @staticmethod
    def _coerce_numeric_id(value):
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return value

    def _build_create_payload(
        self,
        page,
        project: ProjectMatch,
        fields: dict[str, str],
    ) -> dict[str, Any]:
        field_defs = self._get_issue_fields(page)
        market_value = self._required_field(fields, "Market")
        market_id = self._resolve_option_value(page, field_defs["marketId"], market_value)
        task_type_label = fields.get("Task Type", "Feature").strip() or "Feature"
        task_type = self._resolve_option_value(page, field_defs["taskType"], task_type_label)
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
            payload["fixVersionId"] = self._resolve_fix_versions(page, market_id, fix_version_name)

        if fields.get("Component"):
            payload["componentId"] = [
                self._resolve_option_value(page, field_defs["componentId"], fields["Component"], match_value=market_id)
            ]

        if fields.get("Priority"):
            payload["bizPriorityId"] = self._resolve_option_value(
                page,
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
            user_id = self._resolve_jira_user_id(page, raw_value)
            payload[target_name] = [user_id] if is_array else user_id

        if fields.get("Need UAT"):
            payload["uatRequired"] = self._resolve_option_value(
                page,
                field_defs["uatRequired"],
                fields["Need UAT"],
                match_value=market_id,
            )

        if fields.get("Involved Tracks"):
            payload["involvedProductTrackId"] = [
                self._resolve_option_value(page, field_defs["involvedProductTrackId"], fields["Involved Tracks"])
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

    def _write_sdlc_debug_capture(self, payload: dict[str, Any], response: dict[str, Any]) -> None:
        debug_path = Path(__file__).resolve().parent.parent / "tmp" / "last_sdlc_api_result.json"
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

    def _resolve_fix_versions(self, page, market_id: int, raw_value: str) -> list[int]:
        values = [item.strip() for item in raw_value.split("|") if item.strip()] or [raw_value.strip()]
        resolved_ids: list[int] = []
        for value in values:
            response = self._api_request(
                page,
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

    def _resolve_jira_user_id(self, page, query: str) -> int:
        response = self._api_request(
            page,
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
        page,
        field_def: dict[str, Any],
        raw_value: str,
        match_value: int | None = None,
    ) -> int:
        candidates = [item.strip() for item in raw_value.split("|") if item.strip()] or [raw_value.strip()]
        group_names = self._select_option_groups(field_def, match_value)
        options = self._get_group_options(page, group_names)

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

    def _get_issue_fields(self, page) -> dict[str, Any]:
        if self._field_defs_cache is None:
            response = self._api_request(page, "/api/v1/issueField/list")
            self._field_defs_cache = response.get("data") or {}
        return self._field_defs_cache

    def _get_group_options(self, page, group_names: list[str]) -> list[dict[str, Any]]:
        missing = [group for group in group_names if group not in self._group_options_cache]
        if missing:
            response = self._api_request(
                page,
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

    def _resolve_sdlc_business_lead_id(
        self,
        page,
        market: str,
        approval_name: str,
        lead_value: str,
        request_headers: dict[str, Any] | None = None,
    ) -> int:
        normalized_lead = lead_value.strip()
        if not normalized_lead:
            raise BPMISError("Business Lead is required for SDLC approval.")
        if normalized_lead.isdigit():
            return int(normalized_lead)

        request_headers = request_headers or {}
        role_ids = self._fetch_sdlc_role_ids(page, approval_name, request_headers)
        if not role_ids:
            raise BPMISError("Could not discover SDLC approval roles for Business Lead mapping.")

        users: list[dict[str, Any]] = []
        for role_id in role_ids:
            users.extend(self._fetch_sdlc_role_users(page, role_id, request_headers))

        matched_user_id = self._match_sdlc_user(users, normalized_lead)
        if matched_user_id is None:
            raise BPMISError(
                f"Could not resolve SDLC Business Lead '{normalized_lead}' for market {market}."
            )
        return matched_user_id

    def _fetch_sdlc_role_ids(self, page, approval_name: str, request_headers: dict[str, Any]) -> list[int]:
        paths = (
            "/api/v1/sdlc/sdlc-manage/get-node-info",
            "/api/sdlc-manage/get-node-info",
        )
        bodies = (
            {"name": approval_name},
            {"approvalName": approval_name},
            {"approvalType": approval_name},
            {"key": approval_name},
            {"workflowName": approval_name},
        )
        for path in paths:
            for body in bodies:
                try:
                    response = self._api_request(
                        page,
                        path,
                        method="POST",
                        body=body,
                        headers=request_headers,
                    )
                except BPMISError:
                    continue
                role_ids = self._extract_sdlc_role_ids(response)
                if role_ids:
                    return role_ids
        return []

    def _extract_sdlc_role_ids(self, payload: Any) -> list[int]:
        role_ids: set[int] = set()

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                role_type = str(node.get("type") or "").strip().lower()
                role_id = node.get("roleId")
                if role_type == "role" and role_id is not None:
                    try:
                        role_ids.add(int(role_id))
                    except (TypeError, ValueError):
                        pass
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(payload)
        return sorted(role_ids)

    def _fetch_sdlc_role_users(self, page, role_id: int, request_headers: dict[str, Any]) -> list[dict[str, Any]]:
        users: list[dict[str, Any]] = []
        for path in (
            f"/api/v1/sdlc/rule/getUsersByRoleId/{role_id}",
            f"/api/v1/sdlc/rule/get-role-master-list/{role_id}",
        ):
            try:
                response = self._api_request(page, path, headers=request_headers)
            except BPMISError:
                continue
            users.extend(self._extract_candidate_users(response))
        return users

    def _extract_candidate_users(self, payload: Any) -> list[dict[str, Any]]:
        users: list[dict[str, Any]] = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                normalized_keys = {str(key).lower() for key in node.keys()}
                if "id" in normalized_keys and (
                    "email" in normalized_keys
                    or "emailaddress" in normalized_keys
                    or "displayname" in normalized_keys
                    or "name" in normalized_keys
                    or "label" in normalized_keys
                ):
                    users.append(node)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(payload)
        return users

    def _match_sdlc_user(self, users: list[dict[str, Any]], lead_value: str) -> int | None:
        normalized = lead_value.strip().lower()
        if not normalized:
            return None

        def candidate_strings(user: dict[str, Any]) -> list[str]:
            values = (
                user.get("email"),
                user.get("emailAddress"),
                user.get("displayName"),
                user.get("name"),
                user.get("label"),
                user.get("userName"),
                user.get("username"),
                user.get("loginName"),
            )
            return [str(value).strip().lower() for value in values if str(value).strip()]

        for user in users:
            if normalized in candidate_strings(user):
                return int(user["id"])
        for user in users:
            if any(normalized in value for value in candidate_strings(user)):
                return int(user["id"])
        if users:
            return int(users[0]["id"])
        return None

    def _api_request(
        self,
        page,
        path: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        body: Any | None = None,
        headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = page.evaluate(
            """
            async ({path, method, params, body, headers}) => {
              const url = new URL(path, window.location.origin);
              for (const [key, value] of Object.entries(params || {})) {
                url.searchParams.set(key, String(value));
              }
              const options = {
                method,
                credentials: 'include',
                headers: { 'Accept': 'application/json', ...(headers || {}) }
              };
              if (body !== null && body !== undefined) {
                if (!options.headers['Content-Type']) {
                  options.headers['Content-Type'] = 'application/json';
                }
                options.body = JSON.stringify(body);
              }
              const resp = await fetch(url.toString(), options);
              const text = await resp.text();
              return { status: resp.status, text };
            }
            """,
            {
                "path": path,
                "method": method,
                "params": params or {},
                "body": body,
                "headers": headers or {},
            },
        )

        if int(response["status"]) >= 400:
            raise BPMISError(f"BPMIS API request failed for '{path}' with status {response['status']}.")

        try:
            payload = json.loads(response["text"])
        except json.JSONDecodeError as error:
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

    def _resolve_project_url(self, issue_id: str) -> str:
        if self.settings.bpmis_browser_project_url_template:
            return self.settings.bpmis_browser_project_url_template.format(issue_id=issue_id, project_id=issue_id)
        return self.settings.bpmis_browser_base_url

    def _connect_browser(self, playwright):
        last_error: Exception | None = None
        for timeout_ms in (30000, 60000, 120000):
            try:
                return playwright.chromium.connect_over_cdp(
                    self.settings.bpmis_browser_cdp_url,
                    timeout=timeout_ms,
                )
            except Exception as error:  # noqa: BLE001
                last_error = error
        raise BPMISError(
            "Could not connect to the BPMIS Chrome session on port 9222. "
            "Please make sure the remote-debug Chrome window is still open."
        ) from last_error

    def _pick_existing_bpmis_page(self, context):
        for existing in context.pages:
            try:
                if "bpmis-uat1.uat.npt.seabank.io" in existing.url:
                    return existing
            except Exception:  # noqa: BLE001
                continue
        page = context.new_page()
        try:
            page.goto(self.settings.bpmis_browser_base_url, wait_until="domcontentloaded", timeout=30000)
            return page
        except Exception as error:  # noqa: BLE001
            try:
                page.close()
            except Exception:  # noqa: BLE001
                pass
            raise BPMISError(
                "Could not open a BPMIS tab in Chrome automatically. "
                "Please make sure your Chrome session is still logged in."
            ) from error

    def _pick_existing_sdlc_page(self, context, market: str):
        for existing in context.pages:
            try:
                if "sdlc.npt.seabank.io/workflow/" in (existing.url or ""):
                    return existing
            except Exception:  # noqa: BLE001
                continue
        page = context.new_page()
        try:
            url = self.SDLC_MANAGEMENT_URLS.get(market.upper())
            if not url:
                raise BPMISError(f"Unsupported SDLC market '{market}'.")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            return page
        except Exception as error:  # noqa: BLE001
            try:
                page.close()
            except Exception:  # noqa: BLE001
                pass
            raise BPMISError(
                "Could not open an SDLC tab in Chrome automatically. "
                "Please make sure your Chrome session is still logged in."
            ) from error

    def _require_cdp(self) -> None:
        if not self.settings.bpmis_browser_cdp_url:
            raise BPMISNotConfiguredError("BPMIS Chrome session is not configured.")


class BPMISApiClient(BPMISClient):
    def __init__(self, settings: Settings, access_token: str):
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def find_project(self, issue_id: str) -> ProjectMatch:
        if not self.settings.bpmis_api_search_url_template:
            raise BPMISNotConfiguredError("BPMIS API search endpoint is not configured.")

        url = self.settings.bpmis_api_search_url_template.format(issue_id=issue_id, project_id=issue_id)
        response = self.session.request(self.settings.bpmis_api_search_method, url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        project_payload = _lookup_path(payload, self.settings.bpmis_api_search_response_path)

        if isinstance(project_payload, list):
            if not project_payload:
                raise BPMISError(f"No BPMIS project found for Issue ID '{issue_id}'.")
            project_payload = project_payload[0]

        project_id = (
            project_payload.get("id")
            or project_payload.get("projectId")
            or project_payload.get("issueId")
            or issue_id
        )
        return ProjectMatch(project_id=str(project_id), raw=project_payload)

    def create_jira_ticket(self, project: ProjectMatch, fields: dict[str, str]) -> CreatedTicket:
        if not self.settings.bpmis_api_create_url_template:
            raise BPMISNotConfiguredError("BPMIS API create endpoint is not configured.")

        url = self.settings.bpmis_api_create_url_template.format(
            issue_id=project.project_id,
            project_id=project.project_id,
        )
        response = self.session.request(
            self.settings.bpmis_api_create_method,
            url,
            json={"project": project.raw, "fields": fields},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        ticket_payload = _lookup_path(payload, self.settings.bpmis_api_created_ticket_path)

        if isinstance(ticket_payload, str):
            ticket_link = ticket_payload
            ticket_key_match = ISSUE_KEY_PATTERN.search(ticket_payload)
            ticket_key = ticket_key_match.group(1) if ticket_key_match else None
            return CreatedTicket(ticket_key=ticket_key, ticket_link=ticket_link, raw=payload)

        if not isinstance(ticket_payload, dict):
            raise BPMISError("BPMIS API ticket response is in an unsupported format.")

        ticket_key = ticket_payload.get("key") or ticket_payload.get("ticketKey")
        ticket_link = ticket_payload.get("url") or ticket_payload.get("ticketUrl") or ticket_payload.get("link")
        return CreatedTicket(ticket_key=ticket_key, ticket_link=ticket_link, raw=ticket_payload)

    def submit_sdlc_approval(self, approval: dict[str, str]) -> dict[str, Any]:
        raise BPMISError("Direct SDLC API client is not implemented for this transport.")


class BPMISBrowserClient(BPMISClient):
    def __init__(self, settings: Settings, access_token: str):
        self.settings = settings
        self.access_token = access_token

    def _pause_after_step(self, seconds: float = 0.5) -> None:
        if not self.settings.bpmis_browser_headless:
            time.sleep(seconds)

    def submit_sdlc_approval(self, approval: dict[str, str]) -> dict[str, Any]:
        raise BPMISError("Legacy browser SDLC submission is not implemented in this transport.")

    def _open_browser_session(self, playwright):
        if self.settings.bpmis_browser_cdp_url:
            browser = playwright.chromium.connect_over_cdp(self.settings.bpmis_browser_cdp_url)
            if browser.contexts:
                context = browser.contexts[0]
            else:
                context = browser.new_context()
            page = context.new_page()
            return browser, context, page, False

        browser = self._launch_browser(playwright)
        context = self._new_context(browser)
        page = context.new_page()
        return browser, context, page, True

    def _launch_browser(self, playwright):
        launch_kwargs = {"headless": self.settings.bpmis_browser_headless}
        if self.settings.bpmis_browser_executable_path:
            launch_kwargs["executable_path"] = self.settings.bpmis_browser_executable_path
            return playwright.chromium.launch(**launch_kwargs)

        try:
            return playwright.chromium.launch(channel="chrome", **launch_kwargs)
        except Exception:  # noqa: BLE001
            return playwright.chromium.launch(**launch_kwargs)

    def _new_context(self, browser):
        context = browser.new_context(
            extra_http_headers={"Authorization": f"Bearer {self.access_token}"}
        )
        context.add_init_script(
            script=(
                "window.localStorage.setItem("
                f"{json.dumps(self.settings.bpmis_browser_token_storage_key)}, "
                f"{json.dumps(self.access_token)}"
                ");"
            )
        )
        return context

    def find_project(self, issue_id: str) -> ProjectMatch:
        if self.settings.bpmis_browser_project_url_template:
            project_url = self._resolve_project_url(issue_id)
            return ProjectMatch(project_id=issue_id, raw={"issueId": issue_id, "url": project_url})

        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            browser, _context, page, owns_browser = self._open_browser_session(playwright)
            try:
                page.set_default_timeout(30000)
                project_url = self._resolve_project_url(issue_id)
                page.goto(project_url, wait_until="domcontentloaded")

                row_data = self._search_for_project(page, issue_id)

                project_id = issue_id
                return ProjectMatch(
                    project_id=project_id,
                    raw={"issueId": issue_id, "url": project_url, **row_data},
                )
            finally:
                try:
                    page.close()
                except Exception:  # noqa: BLE001
                    pass
                if owns_browser:
                    browser.close()

    def create_jira_ticket(self, project: ProjectMatch, fields: dict[str, str]) -> CreatedTicket:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            browser, _context, page, owns_browser = self._open_browser_session(playwright)
            try:
                page.set_default_timeout(30000)
                page.goto(self._resolve_project_url(project.project_id), wait_until="domcontentloaded")

                self._click_create_jira(page, project.project_id)
                modal = self._wait_for_jira_modal(page)
                form_scope = self._wait_for_jira_form_scope(modal)

                for field_name, value in fields.items():
                    self._fill_field(page, form_scope, field_name, value)

                submit_in_modal = modal.get_by_role("button", name=re.compile(r"^submit$", re.I))
                if submit_in_modal.count() > 0:
                    submit_in_modal.first.click(force=True)
                elif self.settings.bpmis_browser_submit_selector:
                    page.locator(self.settings.bpmis_browser_submit_selector).first.click(force=True)
                else:
                    modal.get_by_role("button", name=re.compile("create|submit", re.I)).click()

                try:
                    page.wait_for_load_state("networkidle")
                except PlaywrightTimeoutError:
                    pass

                ticket_key, ticket_link = self._extract_ticket(page)
                return CreatedTicket(ticket_key=ticket_key, ticket_link=ticket_link, raw={"project": project.raw})
            finally:
                try:
                    page.close()
                except Exception:  # noqa: BLE001
                    pass
                if owns_browser:
                    browser.close()

    def _wait_for_jira_modal(self, page):
        modal_candidates = [
            page.locator(".ant-modal-wrap:visible"),
            page.locator(".ant-modal:visible"),
            page.locator("text=Create New Jira Tickets").locator("xpath=ancestor::*[contains(@class, 'ant-modal') or contains(@class, 'ant-modal-wrap')][1]"),
        ]

        for locator in modal_candidates:
            try:
                if locator.count() == 0:
                    continue
                locator.first.wait_for(state="visible", timeout=10000)
                self._pause_after_step()
                return locator.first
            except Exception:  # noqa: BLE001
                continue

        raise BPMISError("Jira modal opened by BPMIS was not detected.")

    def _wait_for_jira_form_scope(self, modal):
        scope_candidates = [
            modal.locator("tr.ant-table-expanded-row form"),
            modal.locator("form"),
            modal.locator(".ant-table-expanded-row"),
            modal.locator(".ant-modal-body"),
        ]

        for locator in scope_candidates:
            try:
                if locator.count() == 0:
                    continue
                locator.first.wait_for(state="visible", timeout=5000)
                return locator.first
            except Exception:  # noqa: BLE001
                continue

        raise BPMISError("Jira modal form area was not detected.")

    def _click_create_jira(self, page, issue_id: str) -> None:
        row_locator = page.locator(f"tr[data-row-key='{issue_id}']").first
        row_scoped_candidates = [
            row_locator.locator(
                "td.ant-table-cell.ant-table-cell-fix-right.ant-table-cell-fix-right-first "
                "span:nth-child(3) > button"
            ),
            row_locator.locator(
                "td.ant-table-cell.ant-table-cell-fix-right.ant-table-cell-fix-right-first > div > span:nth-child(3) > button"
            ),
            row_locator.locator(
                "td.ant-table-cell.ant-table-cell-fix-right.ant-table-cell-fix-right-first button[aria-describedby]"
            ).nth(2),
        ]

        for locator in row_scoped_candidates:
            try:
                if locator.count() == 0:
                    continue
                locator.first.wait_for(state="visible")
                locator.first.click(force=True)
                self._pause_after_step()
                self._click_task_item(page)
                return
            except Exception:  # noqa: BLE001
                continue

        try:
            clicked = page.evaluate(
                """
                (issueId) => {
                  const row = document.querySelector(`tr[data-row-key="${issueId}"]`);
                  if (!row) return false;
                  const actionCell = row.querySelector(
                    'td.ant-table-cell.ant-table-cell-fix-right.ant-table-cell-fix-right-first'
                  );
                  if (!actionCell) return false;
                  const buttons = actionCell.querySelectorAll('button');
                  if (buttons.length < 3) return false;
                  buttons[2].click();
                  return true;
                }
                """,
                issue_id,
            )
            if clicked:
                self._pause_after_step()
                self._click_task_item(page)
                return
        except Exception:  # noqa: BLE001
            pass

        if self.settings.bpmis_browser_create_button_selector:
            try:
                page.locator(self.settings.bpmis_browser_create_button_selector).first.click(force=True)
                self._pause_after_step()
                self._click_task_item(page)
                return
            except Exception as error:  # noqa: BLE001
                raise BPMISError(
                    "Configured BPMIS create button selector did not work."
                ) from error

        candidates = [
            page.get_by_role("button", name=re.compile("create jira|jira", re.I)),
            page.get_by_text(re.compile("create jira|jira", re.I)),
            page.locator("button, a, span").filter(has_text=re.compile("create jira|jira", re.I)),
        ]

        last_error: Exception | None = None
        for locator in candidates:
            try:
                if locator.count() == 0:
                    continue
                locator.first.click()
                self._pause_after_step()
                self._click_task_item(page)
                return
            except Exception as error:  # noqa: BLE001
                last_error = error

        raise BPMISError(
            "Could not find or click the BPMIS Jira creation action. "
            "Please share a screenshot of the project page action area or the selector."
        ) from last_error

    def _click_task_item(self, page) -> None:
        try:
            page.wait_for_selector(".ant-popover:visible, .ant-dropdown:visible", timeout=3000)
        except Exception:  # noqa: BLE001
            pass

        task_candidates = [
            page.locator(".ant-popover:visible .ant-radio-group > label:nth-child(1)"),
            page.locator(".ant-popover:visible .ant-radio-group .ant-radio-button-wrapper").first,
            page.locator(".ant-popover:visible .ant-radio-group .ant-radio-button-wrapper").filter(has_text=re.compile(r"^task$", re.I)),
            page.locator(".ant-dropdown:visible .ant-radio-group > label:nth-child(1)"),
            page.locator(".ant-dropdown:visible .ant-radio-group .ant-radio-button-wrapper").first,
        ]

        if self.settings.bpmis_browser_task_item_selector:
            task_candidates.insert(0, page.locator(f".ant-popover:visible {self.settings.bpmis_browser_task_item_selector}"))

        last_error: Exception | None = None
        for locator in task_candidates:
            try:
                if locator.count() == 0:
                    continue
                locator.first.click(force=True)
                self._pause_after_step()
                return
            except Exception as error:  # noqa: BLE001
                last_error = error

        try:
            clicked = page.evaluate(
                """
                () => {
                  const layers = Array.from(document.querySelectorAll('.ant-popover, .ant-dropdown'));
                  const visibleLayer = layers.find((node) => {
                    const style = window.getComputedStyle(node);
                    return style.display !== 'none' && style.visibility !== 'hidden' && node.offsetParent !== null;
                  });
                  if (!visibleLayer) return false;
                  const firstRadio = visibleLayer.querySelector('.ant-radio-group > label:nth-child(1)');
                  if (firstRadio) {
                    firstRadio.click();
                    return true;
                  }
                  const taskRadio = Array.from(
                    visibleLayer.querySelectorAll('.ant-radio-button-wrapper')
                  ).find((node) => node.textContent.trim().toLowerCase() === 'task');
                  if (!taskRadio) return false;
                  taskRadio.click();
                  return true;
                }
                """
            )
            if clicked:
                self._pause_after_step()
                return
        except Exception as error:  # noqa: BLE001
            last_error = error

        raise BPMISError("Could not click the BPMIS Task item in the popup menu.") from last_error

    def _resolve_project_url(self, issue_id: str) -> str:
        if self.settings.bpmis_browser_project_url_template:
            return self.settings.bpmis_browser_project_url_template.format(issue_id=issue_id, project_id=issue_id)
        return self.settings.bpmis_browser_base_url

    def _search_for_project(self, page, issue_id: str) -> dict[str, str]:
        page.goto(self.settings.bpmis_browser_base_url, wait_until="domcontentloaded")
        self._apply_search_filter(page, issue_id)
        return self._extract_project_row_data(page, issue_id)

    def _apply_search_filter(self, page, issue_id: str) -> None:
        try:
            if self.settings.bpmis_browser_search_input_selector:
                input_locator = page.locator(self.settings.bpmis_browser_search_input_selector).first
            else:
                input_locator = self._find_search_input(page)
                if input_locator is None:
                    self._expand_filters(page)
                    input_locator = self._find_search_input(page)

            if input_locator is None:
                # If no obvious search field exists, continue and try to click the row directly.
                return

            self._fill_issue_id_input(page, input_locator, issue_id)
        except Exception as error:  # noqa: BLE001
            raise BPMISError(
                "Could not use the BPMIS Issue ID filter automatically."
            ) from error

        if self.settings.bpmis_browser_search_submit_selector:
            page.locator(self.settings.bpmis_browser_search_submit_selector).first.click()
        else:
            clicked = False
            query_candidates = [
                page.get_by_role("button", name=re.compile(r"^query$", re.I)),
                page.locator("button").filter(has_text=re.compile(r"^query$", re.I)),
                page.get_by_text(re.compile(r"^query$", re.I)),
            ]
            for locator in query_candidates:
                try:
                    if locator.count() == 0:
                        continue
                    locator.first.click()
                    clicked = True
                    break
                except Exception:  # noqa: BLE001
                    continue

            if not clicked:
                try:
                    input_locator.press("Enter")
                except Exception:  # noqa: BLE001
                    page.keyboard.press("Enter")

        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:  # noqa: BLE001
            pass

    def _expand_filters(self, page) -> None:
        expand_candidates = [
            page.get_by_role("button", name=re.compile(r"^expand$", re.I)),
            page.get_by_text(re.compile(r"^expand$", re.I)),
            page.locator("button, span, a").filter(has_text=re.compile(r"^expand$", re.I)),
        ]
        for locator in expand_candidates:
            try:
                if locator.count() == 0:
                    continue
                locator.first.click()
                try:
                    page.wait_for_timeout(500)
                except Exception:  # noqa: BLE001
                    pass
                return
            except Exception:  # noqa: BLE001
                continue

    def _fill_issue_id_input(self, page, input_locator, issue_id: str) -> None:
        try:
            if input_locator.is_visible():
                input_locator.click()
                input_locator.fill("")
                input_locator.fill(issue_id)
                return
        except Exception:  # noqa: BLE001
            pass

        container_candidates = [
            page.locator("xpath=//*[contains(normalize-space(.), 'Issue ID')]/following::*[contains(@class, 'ant-select')][1]"),
            page.locator("xpath=//*[contains(normalize-space(.), 'Issue ID')]/following::*[contains(@class, 'ant-select-selector')][1]"),
            page.locator("xpath=//*[contains(normalize-space(.), 'Issue ID')]/ancestor::div[1]"),
        ]
        for container in container_candidates:
            try:
                if container.count() == 0:
                    continue
                container.first.click()
                visible_input = page.locator("input#id:visible, input[aria-owns='id_list']:visible, input.ant-select-selection-search-input:visible").first
                visible_input.fill("")
                visible_input.fill(issue_id)
                return
            except Exception:  # noqa: BLE001
                continue

        input_locator.fill("")
        input_locator.fill(issue_id)

    def _find_search_input(self, page):
        selectors = [
            "input#id:visible",
            "input[aria-owns='id_list']:visible",
            "input.ant-select-selection-search-input:visible",
            "input[type='search']",
            "input[placeholder*='Search' i]",
            "input[placeholder*='Issue' i]",
            "input[placeholder*='ID' i]",
            "input[name*='search' i]",
            "input[name*='issue' i]",
            "input[id*='search' i]",
            "input[id*='issue' i]",
        ]

        for selector in selectors:
            locator = page.locator(selector)
            if locator.count() > 0:
                return locator.first

        role_candidates = [
            page.get_by_role("searchbox"),
            page.get_by_label(re.compile("search|issue|id", re.I)),
            page.get_by_placeholder(re.compile("search|issue|id", re.I)),
        ]
        for locator in role_candidates:
            try:
                if locator.count() > 0:
                    return locator.first
            except Exception:  # noqa: BLE001
                continue

        labeled_input_candidates = [
            "xpath=//*[contains(normalize-space(.), 'Issue ID')]/following::input[1]",
            "xpath=//label[contains(normalize-space(.), 'Issue ID')]/following::input[1]",
            "xpath=//span[contains(normalize-space(.), 'Issue ID')]/following::input[1]",
            "xpath=//div[contains(normalize-space(.), 'Issue ID')]/following::input[1]",
        ]
        for selector in labeled_input_candidates:
            try:
                locator = page.locator(selector)
                if locator.count() > 0:
                    return locator.first
            except Exception:  # noqa: BLE001
                continue

        return None

    def _extract_project_row_data(self, page, issue_id: str) -> dict[str, str]:
        exact_text_candidates = [
            page.get_by_role("link", name=re.compile(rf"\b{re.escape(issue_id)}\b")),
            page.get_by_role("cell", name=re.compile(rf"\b{re.escape(issue_id)}\b")),
            page.get_by_text(re.compile(rf"\b{re.escape(issue_id)}\b")),
            page.locator("td").filter(has_text=re.compile(rf"\b{re.escape(issue_id)}\b")),
        ]

        for locator in exact_text_candidates:
            try:
                if locator.count() == 0:
                    continue
                target = locator.first
                tag_name = target.evaluate("(el) => el.tagName.toLowerCase()")
                if tag_name in {"a", "button"}:
                    row = target.locator("xpath=ancestor::tr[1]")
                    if row.count() > 0:
                        return self._row_to_project_data(row.first)
                    return {"summary": "", "market": ""}

                row = target.locator("xpath=ancestor::tr[1]")
                if row.count() > 0:
                    return self._row_to_project_data(row.first)

                return {"summary": target.inner_text().strip(), "market": ""}
            except Exception:  # noqa: BLE001
                continue

        raise BPMISNotConfiguredError(
            "Could not find the BPMIS project row automatically. "
            "Please provide a direct project URL pattern or the page selectors."
        )

    def _row_to_project_data(self, row) -> dict[str, str]:
        try:
            cells = row.locator("td")
            count = cells.count()
            texts = []
            for index in range(count):
                texts.append(cells.nth(index).inner_text().strip())

            summary = texts[3] if len(texts) > 3 else ""
            market = texts[4] if len(texts) > 4 else ""
            return {"summary": summary, "market": market}
        except Exception:  # noqa: BLE001
            return {"summary": "", "market": ""}

    def _fill_field(self, page, modal, field_name: str, value: str) -> None:
        if self._is_optional_field(field_name) and not self._field_exists(modal, field_name):
            return

        if field_name.lower() in {"fix version", "fix version/s"}:
            self._fill_select_field(page, modal, field_name, value, allow_multiple=True)
            return
        if field_name.lower() in {
            "component",
            "assignee",
            "product manager",
            "dev pic",
            "qa pic",
            "reporter",
            "biz pic",
            "need uat",
            "task type",
            "market",
            "priority",
        }:
            self._fill_select_field(page, modal, field_name, value, allow_multiple=False)
            return

        container = self._find_field_container(modal, field_name)
        self._prepare_field_container(container)
        candidates = [
            container.locator("textarea"),
            container.locator("input:not([type='hidden'])"),
            container.locator("[contenteditable='true']"),
            modal.get_by_label(field_name, exact=False),
            modal.get_by_placeholder(field_name, exact=False),
            modal.locator(f"textarea[name='{field_name}'], input[name='{field_name}'], select[name='{field_name}']"),
        ]

        last_error: Exception | None = None

        for locator in candidates:
            try:
                if locator.count() == 0:
                    continue
                target = locator.first
                tag_name = target.evaluate("(el) => el.tagName.toLowerCase()")
                if tag_name == "select":
                    target.select_option(label=value)
                elif tag_name == "textarea":
                    target.click(force=True)
                    target.fill("")
                    target.type(value, delay=40)
                elif tag_name == "input":
                    target.click(force=True)
                    target.fill("")
                    target.type(value, delay=40)
                else:
                    target.click(force=True)
                    page.keyboard.press("Meta+A")
                    target.type(value, delay=40)
                return
            except Exception as error:  # noqa: BLE001
                last_error = error

        raise BPMISError(f"Could not fill BPMIS field '{field_name}'.") from last_error

    def _fill_select_field(self, page, modal, field_name: str, value: str, allow_multiple: bool) -> None:
        fallbacks = [item.strip() for item in value.split("|") if item.strip()] or [value]
        last_error: Exception | None = None
        has_multiple_fallbacks = len(fallbacks) > 1

        for candidate_value in fallbacks:
            try:
                control = self._find_select_control(modal, field_name)
                control.click()
                self._pause_after_step(1.0)
                search_input = self._find_select_search_input(control, modal, page)
                if search_input.count() > 0:
                    search_input.click(force=True)
                    search_input.fill("")
                    search_input.type(candidate_value, delay=80)
                    self._pause_after_step(1.0)
                option = page.locator(
                    ".ant-select-item-option-content, [role='option']"
                ).filter(has_text=re.compile(re.escape(candidate_value), re.I)).first
                if option.count() > 0:
                    option.click(force=True)
                elif search_input.count() > 0:
                    if has_multiple_fallbacks:
                        raise BPMISError(
                            f"Option '{candidate_value}' was not available for BPMIS field '{field_name}'."
                        )
                    search_input.press("Enter")
                else:
                    if has_multiple_fallbacks:
                        raise BPMISError(
                            f"Option '{candidate_value}' was not available for BPMIS field '{field_name}'."
                        )
                    page.keyboard.press("Enter")
                self._pause_after_step(1.0)
                if not allow_multiple:
                    page.keyboard.press("Escape")
                return
            except Exception as error:  # noqa: BLE001
                last_error = error
                try:
                    page.keyboard.press("Escape")
                except Exception:  # noqa: BLE001
                    pass

        raise BPMISError(f"Could not select BPMIS field '{field_name}'.") from last_error

    def _is_optional_field(self, field_name: str) -> bool:
        return field_name.strip().lower() in {"biz pic"}

    def _requires_explicit_option_pick(self, field_name: str) -> bool:
        return field_name.strip().lower() in {
            "fix version",
            "fix version/s",
            "component",
            "priority",
            "assignee",
            "product manager",
            "dev pic",
            "qa pic",
            "reporter",
            "biz pic",
            "need uat",
        }

    def _prepare_field_container(self, container) -> None:
        try:
            container.scroll_into_view_if_needed()
        except Exception:  # noqa: BLE001
            pass
        try:
            container.click(position={"x": 5, "y": 5}, force=False, timeout=1000)
        except Exception:  # noqa: BLE001
            pass

    def _field_exists(self, modal, field_name: str) -> bool:
        try:
            self._find_field_container(modal, field_name)
            return True
        except BPMISError:
            return False

    def _find_field_container(self, modal, field_name: str):
        label_patterns = [field_name]
        if field_name.lower() == "fix version/s":
            label_patterns.append("Fix Version")

        for label in label_patterns:
            selectors = [
                (
                    "xpath=.//*[contains(@class, 'ant-form-item')][.//*[self::label or self::div or "
                    f"self::span][contains(normalize-space(.), '{label}')]][1]"
                ),
                (
                    "xpath=.//*[contains(@class, 'ant-row') and contains(@class, 'ant-form-item-row')]"
                    f"[.//*[self::label or self::div or self::span][contains(normalize-space(.), '{label}')]][1]"
                ),
                (
                    "xpath=.//*[self::label or self::div or self::span][contains(normalize-space(.), "
                    f"'{label}')]/ancestor::*[contains(@class, 'ant-form-item')][1]"
                ),
            ]
            for selector in selectors:
                try:
                    locator = modal.locator(selector)
                    if locator.count() > 0:
                        return locator.first
                except Exception:  # noqa: BLE001
                    continue

        raise BPMISError(f"Could not locate form container for BPMIS field '{field_name}'.")

    def _find_select_search_input(self, control, modal, page):
        candidates = [
            control.locator("input.ant-select-selection-search-input"),
            control.locator("input[role='combobox']"),
            control.locator("xpath=.//input[1]"),
            control.locator("xpath=.//ancestor::*[contains(@class, 'ant-select')][1]//input[contains(@class, 'ant-select-selection-search-input')]"),
            page.locator(".ant-select-dropdown:visible input.ant-select-selection-search-input"),
            page.locator(".ant-select-dropdown:visible input[role='combobox']"),
        ]

        for locator in candidates:
            try:
                if locator.count() == 0:
                    continue
                return locator.first
            except Exception:  # noqa: BLE001
                continue

        return page.locator("__missing__")

    def _find_select_control(self, modal, field_name: str, container=None):
        if field_name.lower() in {"fix version", "fix version/s"} and self.settings.bpmis_browser_fix_version_selector:
            locator = modal.locator(self.settings.bpmis_browser_fix_version_selector)
            if locator.count() > 0:
                return locator.first

        try:
            container = container or self._find_field_container(modal, field_name)
            scoped_candidates = [
                container.locator(".ant-select-selector").first,
                container.locator(".ant-select").first,
                container.locator("input[role='combobox']").first,
            ]
            for locator in scoped_candidates:
                try:
                    if locator.count() > 0:
                        return locator
                except Exception:  # noqa: BLE001
                    continue
        except Exception:  # noqa: BLE001
            pass

        label_patterns = [field_name]
        if field_name.lower() == "fix version/s":
            label_patterns.append("Fix Version")

        for label in label_patterns:
            selectors = [
                f"xpath=.//*[contains(normalize-space(.), '{label}')]/following::*[contains(@class, 'ant-select')][1]",
                f"xpath=.//*[contains(normalize-space(.), '{label}')]/following::input[1]",
                f"xpath=.//*[contains(normalize-space(.), '{label}')]/ancestor::div[1]//*[contains(@class, 'ant-select')][1]",
            ]
            for selector in selectors:
                locator = modal.locator(selector)
                try:
                    if locator.count() > 0:
                        return locator.first
                except Exception:  # noqa: BLE001
                    continue

        raise BPMISError(f"Could not locate control for BPMIS field '{field_name}'.")

    def _extract_ticket(self, page) -> tuple[str | None, str | None]:
        text = page.content()
        if self.settings.bpmis_browser_ticket_url_regex:
            url_match = re.search(self.settings.bpmis_browser_ticket_url_regex, text)
            if url_match:
                ticket_link = url_match.group(0)
                key_match = ISSUE_KEY_PATTERN.search(ticket_link)
                return key_match.group(1) if key_match else None, ticket_link

        key_match = ISSUE_KEY_PATTERN.search(text)
        ticket_key = key_match.group(1) if key_match else None
        ticket_link = None

        for anchor in page.locator("a").all():
            href = anchor.get_attribute("href") or ""
            if ticket_key and ticket_key in href:
                ticket_link = href
                break

        if not ticket_key and not ticket_link:
            raise BPMISError("Could not extract the created Jira ticket from BPMIS.")

        return ticket_key, ticket_link


class FallbackBPMISClient(BPMISClient):
    def __init__(self, primary: BPMISClient, fallback: BPMISClient):
        self.primary = primary
        self.fallback = fallback

    def find_project(self, issue_id: str) -> ProjectMatch:
        try:
            return self.primary.find_project(issue_id)
        except Exception:  # noqa: BLE001
            return self.fallback.find_project(issue_id)

    def create_jira_ticket(self, project: ProjectMatch, fields: dict[str, str]) -> CreatedTicket:
        try:
            return self.primary.create_jira_ticket(project, fields)
        except Exception:  # noqa: BLE001
            return self.fallback.create_jira_ticket(project, fields)
