from __future__ import annotations

import base64
import json
import time
from typing import Any, Callable
from urllib.parse import urljoin, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_protocol import sign_headers
from bpmis_jira_tool.models import CreatedTicket, ProjectMatch


LOCAL_AGENT_TRANSIENT_UNREADABLE_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0)
LOCAL_AGENT_TRANSIENT_UNREADABLE_HINTS = (
    "err_ngrok_3200",
    "endpoint",
    "is offline",
    "ngrok",
)


def _build_local_agent_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=0,
        status=2,
        backoff_factor=0.25,
        status_forcelist=(502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=32)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_LOCAL_AGENT_SESSION = _build_local_agent_session()


def _is_transient_unreadable_local_agent_response(*, status_code: int, body_preview: str) -> bool:
    if int(status_code or 0) not in {404, 502, 503, 504}:
        return False
    normalized = str(body_preview or "").lower()
    return "err_ngrok_3200" in normalized or all(hint in normalized for hint in LOCAL_AGENT_TRANSIENT_UNREADABLE_HINTS)


class LocalAgentClient:
    def __init__(
        self,
        *,
        base_url: str,
        hmac_secret: str,
        timeout_seconds: int = 300,
        connect_timeout_seconds: int = 10,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/") + "/"
        self.hmac_secret = str(hmac_secret or "")
        self.timeout_seconds = max(5, int(timeout_seconds or 300))
        self.connect_timeout_seconds = max(1, min(int(connect_timeout_seconds or 10), self.timeout_seconds))
        self.session = session or _LOCAL_AGENT_SESSION
        if not self.base_url.strip("/"):
            raise ToolError("LOCAL_AGENT_BASE_URL is required before using Mac local-agent capabilities.")
        if not self.hmac_secret:
            raise ToolError("LOCAL_AGENT_HMAC_SECRET is required before using Mac local-agent capabilities.")

    def get_health(self) -> dict[str, Any]:
        proxied_error: ToolError | None = None
        try:
            payload = self._request("GET", "/api/local-agent/healthz", signed=False)
            if isinstance(payload.get("capabilities"), dict):
                return payload
        except ToolError as error:
            proxied_error = error
        try:
            return self._request("GET", "/healthz", signed=False)
        except ToolError:
            if proxied_error is not None:
                raise proxied_error
            raise

    def source_code_qa_config(self, *, llm_provider: str | None = None) -> dict[str, Any]:
        payload = {"llm_provider": llm_provider} if llm_provider else {}
        return self._request("POST", "/api/local-agent/source-code-qa/config", payload)

    def source_code_qa_save_mapping(self, *, pm_team: str, country: str, repositories: list[dict[str, Any]]) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/local-agent/source-code-qa/config/save",
            {"pm_team": pm_team, "country": country, "repositories": repositories},
        )

    def source_code_qa_sync(self, *, pm_team: str, country: str) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/source-code-qa/sync", {"pm_team": pm_team, "country": country})

    def source_code_qa_ensure_synced_today(self, *, pm_team: str, country: str, background: bool = False) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/local-agent/source-code-qa/ensure-synced-today",
            {"pm_team": pm_team, "country": country, "background": background},
        )

    def source_code_qa_query(self, payload: dict[str, Any], *, progress_callback: Callable[[str, str, int, int], None] | None = None) -> dict[str, Any]:
        if progress_callback is None:
            return self._request("POST", "/api/local-agent/source-code-qa/query", payload)
        initial = self._request("POST", "/api/local-agent/source-code-qa/query-async", payload)
        job_id = str(initial.get("job_id") or "").strip()
        if not job_id:
            raise ToolError("Mac local-agent did not return a Source Code Q&A job id.")
        last_progress: tuple[str, str, int, int] | None = None
        while True:
            status = self._request("GET", f"/api/local-agent/source-code-qa/query-jobs/{job_id}", signed=True)
            stage = str(status.get("stage") or "")
            message = str(status.get("message") or "")
            current = int(status.get("current") or 0)
            total = int(status.get("total") or 0)
            progress = (stage, message, current, total)
            if message and progress != last_progress:
                progress_callback(stage, message, current, total)
                last_progress = progress
            state = str(status.get("state") or "")
            if state == "completed":
                result = status.get("result")
                return result if isinstance(result, dict) else {}
            if state == "failed":
                raise ToolError(str(status.get("error") or message or "Mac local-agent Source Code Q&A job failed."))
            time.sleep(0.7)

    def productization_llm_descriptions(self, *, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload = self._request("POST", "/api/local-agent/productization/llm-descriptions", {"items": items})
        generated = payload.get("items")
        return generated if isinstance(generated, list) else []

    def seatalk_overview(self) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/overview", {})

    def seatalk_insights(self, *, name_mappings: dict[str, str] | None = None, todo_since: str | None = None) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/insights", {"name_mappings": name_mappings or {}, "todo_since": todo_since or ""})

    def seatalk_project_updates(self, *, name_mappings: dict[str, str] | None = None) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/project-updates", {"name_mappings": name_mappings or {}})

    def seatalk_todos(self, *, name_mappings: dict[str, str] | None = None, todo_since: str | None = None) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/todos", {"name_mappings": name_mappings or {}, "todo_since": todo_since or ""})

    def seatalk_name_mappings(self) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/name-mappings", {})

    def seatalk_export(self, *, name_mappings: dict[str, str] | None = None) -> tuple[str, str]:
        payload = self._request("POST", "/api/local-agent/seatalk/export", {"name_mappings": name_mappings or {}})
        return str(payload.get("content") or ""), str(payload.get("filename") or "seatalk-history-last-7-days.txt")

    def bpmis_call(self, *, operation: str, access_token: str | None, args: list[Any] | None = None, kwargs: dict[str, Any] | None = None) -> Any:
        payload = self._request(
            "POST",
            "/api/local-agent/bpmis/call",
            {
                "operation": operation,
                "access_token": access_token,
                "args": args or [],
                "kwargs": kwargs or {},
            },
        )
        return payload.get("result")

    def bpmis_config_load(self, *, user_key: str) -> dict[str, Any] | None:
        payload = self._request("POST", "/api/local-agent/bpmis/config/load", {"user_key": user_key})
        config = payload.get("config")
        return config if isinstance(config, dict) else None

    def bpmis_config_save(self, *, user_key: str, config: dict[str, Any]) -> dict[str, Any]:
        payload = self._request("POST", "/api/local-agent/bpmis/config/save", {"user_key": user_key, "config": config})
        saved = payload.get("config")
        return saved if isinstance(saved, dict) else {}

    def bpmis_config_migrate(self, *, from_user_key: str, to_user_key: str) -> None:
        self._request(
            "POST",
            "/api/local-agent/bpmis/config/migrate",
            {"from_user_key": from_user_key, "to_user_key": to_user_key},
        )

    def bpmis_team_profiles_load(self) -> dict[str, Any]:
        payload = self._request("POST", "/api/local-agent/bpmis/team-profiles/load", {})
        profiles = payload.get("profiles")
        return profiles if isinstance(profiles, dict) else {}

    def bpmis_team_profile_save(self, *, team_key: str, profile: dict[str, Any]) -> dict[str, Any]:
        payload = self._request("POST", "/api/local-agent/bpmis/team-profiles/save", {"team_key": team_key, "profile": profile})
        saved = payload.get("profile")
        return saved if isinstance(saved, dict) else {}

    def bpmis_projects_list(self, *, user_key: str) -> list[dict[str, Any]]:
        payload = self._request("POST", "/api/local-agent/bpmis/projects/list", {"user_key": user_key})
        projects = payload.get("projects")
        return projects if isinstance(projects, list) else []

    def bpmis_projects_reorder(self, *, user_key: str, bpmis_ids: list[str]) -> list[dict[str, Any]]:
        payload = self._request("POST", "/api/local-agent/bpmis/projects/reorder", {"user_key": user_key, "bpmis_ids": bpmis_ids})
        projects = payload.get("projects")
        return projects if isinstance(projects, list) else []

    def bpmis_project_upsert(
        self,
        *,
        user_key: str,
        bpmis_id: str,
        project_name: str,
        brd_link: str,
        market: str,
    ) -> str:
        payload = self._request(
            "POST",
            "/api/local-agent/bpmis/projects/upsert",
            {
                "user_key": user_key,
                "bpmis_id": bpmis_id,
                "project_name": project_name,
                "brd_link": brd_link,
                "market": market,
            },
        )
        return str(payload.get("result") or "")

    def bpmis_project_delete(self, *, user_key: str, bpmis_id: str) -> bool:
        payload = self._request("POST", "/api/local-agent/bpmis/projects/delete", {"user_key": user_key, "bpmis_id": bpmis_id})
        return bool(payload.get("deleted"))

    def bpmis_project_comment_update(self, *, user_key: str, bpmis_id: str, pm_comment: str) -> bool:
        payload = self._request(
            "POST",
            "/api/local-agent/bpmis/projects/comment",
            {"user_key": user_key, "bpmis_id": bpmis_id, "pm_comment": pm_comment},
        )
        return bool(payload.get("updated"))

    def bpmis_project_ticket_add(self, **ticket: Any) -> dict[str, Any]:
        payload = self._request("POST", "/api/local-agent/bpmis/projects/jira-tickets/add", ticket)
        stored = payload.get("ticket")
        return stored if isinstance(stored, dict) else {}

    def bpmis_project_ticket_upsert_synced(self, **ticket: Any) -> dict[str, Any] | None:
        payload = self._request("POST", "/api/local-agent/bpmis/projects/jira-tickets/upsert-synced", ticket)
        stored = payload.get("ticket")
        return stored if isinstance(stored, dict) else None

    def bpmis_project_ticket_delete(self, *, user_key: str, bpmis_id: str, ticket_id: str | int) -> bool:
        payload = self._request(
            "POST",
            "/api/local-agent/bpmis/projects/jira-tickets/delete",
            {"user_key": user_key, "bpmis_id": bpmis_id, "ticket_id": ticket_id},
        )
        return bool(payload.get("deleted"))

    def bpmis_project_ticket_status_update(self, *, user_key: str, bpmis_id: str, ticket_id: str | int, status: str) -> bool:
        payload = self._request(
            "POST",
            "/api/local-agent/bpmis/projects/jira-tickets/status",
            {"user_key": user_key, "bpmis_id": bpmis_id, "ticket_id": ticket_id, "status": status},
        )
        return bool(payload.get("updated"))

    def bpmis_project_ticket_version_update(
        self,
        *,
        user_key: str,
        bpmis_id: str,
        ticket_id: str | int,
        version_name: str,
        version_id: str = "",
    ) -> bool:
        payload = self._request(
            "POST",
            "/api/local-agent/bpmis/projects/jira-tickets/version",
            {
                "user_key": user_key,
                "bpmis_id": bpmis_id,
                "ticket_id": ticket_id,
                "version_name": version_name,
                "version_id": version_id,
            },
        )
        return bool(payload.get("updated"))

    def source_code_qa_sessions_list(self, *, owner_email: str, limit: int = 30) -> list[dict[str, Any]]:
        payload = self._request("POST", "/api/local-agent/source-code-qa/sessions/list", {"owner_email": owner_email, "limit": limit})
        sessions = payload.get("sessions")
        return sessions if isinstance(sessions, list) else []

    def source_code_qa_session_create(self, **session_payload: Any) -> dict[str, Any]:
        payload = self._request("POST", "/api/local-agent/source-code-qa/sessions/create", session_payload)
        session = payload.get("session")
        return session if isinstance(session, dict) else {}

    def source_code_qa_session_get(self, *, session_id: str, owner_email: str) -> dict[str, Any] | None:
        payload = self._request("POST", "/api/local-agent/source-code-qa/sessions/get", {"session_id": session_id, "owner_email": owner_email})
        session = payload.get("session")
        return session if isinstance(session, dict) else None

    def source_code_qa_session_archive(self, *, session_id: str, owner_email: str) -> dict[str, Any] | None:
        payload = self._request("POST", "/api/local-agent/source-code-qa/sessions/archive", {"session_id": session_id, "owner_email": owner_email})
        archived = payload.get("archived")
        return archived if isinstance(archived, dict) else None

    def source_code_qa_session_context(self, *, session_id: str, owner_email: str) -> dict[str, Any] | None:
        payload = self._request("POST", "/api/local-agent/source-code-qa/sessions/context", {"session_id": session_id, "owner_email": owner_email})
        context = payload.get("context")
        return context if isinstance(context, dict) else None

    def source_code_qa_session_append(self, **exchange: Any) -> dict[str, Any] | None:
        payload = self._request("POST", "/api/local-agent/source-code-qa/sessions/append", exchange)
        session = payload.get("session")
        return session if isinstance(session, dict) else None

    def source_code_qa_attachment_save(
        self,
        *,
        owner_email: str,
        session_id: str,
        filename: str,
        mime_type: str,
        content: bytes,
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "/api/local-agent/source-code-qa/attachments/save",
            {
                "owner_email": owner_email,
                "session_id": session_id,
                "filename": filename,
                "mime_type": mime_type,
                "content_base64": base64.b64encode(content).decode("ascii"),
            },
        )
        attachment = payload.get("attachment")
        return attachment if isinstance(attachment, dict) else {}

    def source_code_qa_attachments_resolve(self, *, owner_email: str, session_id: str, attachment_ids: list[str]) -> list[dict[str, Any]]:
        payload = self._request(
            "POST",
            "/api/local-agent/source-code-qa/attachments/resolve",
            {"owner_email": owner_email, "session_id": session_id, "attachment_ids": attachment_ids},
        )
        attachments = payload.get("attachments")
        return attachments if isinstance(attachments, list) else []

    def source_code_qa_attachment_get(self, *, owner_email: str, session_id: str, attachment_id: str) -> tuple[dict[str, Any], bytes]:
        payload = self._request(
            "POST",
            "/api/local-agent/source-code-qa/attachments/get",
            {"owner_email": owner_email, "session_id": session_id, "attachment_id": attachment_id},
        )
        metadata = payload.get("attachment") if isinstance(payload.get("attachment"), dict) else {}
        encoded = str(payload.get("content_base64") or "")
        return metadata, base64.b64decode(encoded.encode("ascii")) if encoded else b""

    def source_code_qa_model_availability_get(self) -> dict[str, bool]:
        payload = self._request("POST", "/api/local-agent/source-code-qa/model-availability/get", {})
        availability = payload.get("availability")
        return availability if isinstance(availability, dict) else {}

    def source_code_qa_model_availability_save(self, availability: dict[str, Any]) -> dict[str, bool]:
        payload = self._request("POST", "/api/local-agent/source-code-qa/model-availability/save", {"availability": availability})
        saved = payload.get("availability")
        return saved if isinstance(saved, dict) else {}

    def source_code_qa_runtime_evidence_list(self, *, pm_team: str, country: str) -> list[dict[str, Any]]:
        payload = self._request(
            "POST",
            "/api/local-agent/source-code-qa/runtime-evidence/list",
            {"pm_team": pm_team, "country": country},
        )
        evidence = payload.get("evidence")
        return evidence if isinstance(evidence, list) else []

    def source_code_qa_runtime_evidence_save(
        self,
        *,
        pm_team: str,
        country: str,
        source_type: str,
        uploaded_by: str,
        filename: str,
        mime_type: str,
        content: bytes,
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "/api/local-agent/source-code-qa/runtime-evidence/save",
            {
                "pm_team": pm_team,
                "country": country,
                "source_type": source_type,
                "uploaded_by": uploaded_by,
                "filename": filename,
                "mime_type": mime_type,
                "content_base64": base64.b64encode(content).decode("ascii"),
            },
        )
        evidence = payload.get("evidence")
        return evidence if isinstance(evidence, dict) else {}

    def source_code_qa_runtime_evidence_resolve(self, *, pm_team: str, country: str) -> list[dict[str, Any]]:
        payload = self._request(
            "POST",
            "/api/local-agent/source-code-qa/runtime-evidence/resolve",
            {"pm_team": pm_team, "country": country},
        )
        evidence = payload.get("evidence")
        return evidence if isinstance(evidence, list) else []

    def source_code_qa_runtime_evidence_delete(self, *, pm_team: str, country: str, evidence_id: str) -> bool:
        payload = self._request(
            "POST",
            "/api/local-agent/source-code-qa/runtime-evidence/delete",
            {"pm_team": pm_team, "country": country, "evidence_id": evidence_id},
        )
        return bool(payload.get("deleted"))

    def seatalk_todos_completed_ids(self, *, owner_email: str) -> list[str]:
        payload = self._request("POST", "/api/local-agent/seatalk/todos/completed-ids", {"owner_email": owner_email})
        ids = payload.get("completed_ids")
        return [str(item) for item in ids] if isinstance(ids, list) else []

    def seatalk_todos_open(self, *, owner_email: str) -> list[dict[str, Any]]:
        payload = self._request("POST", "/api/local-agent/seatalk/todos/open", {"owner_email": owner_email})
        items = payload.get("todos")
        return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []

    def seatalk_todos_processed_until(self, *, owner_email: str) -> str:
        payload = self._request("POST", "/api/local-agent/seatalk/todos/processed-until", {"owner_email": owner_email})
        return str(payload.get("processed_until") or "")

    def seatalk_todos_mark_processed_until(self, *, owner_email: str, processed_until: str) -> None:
        self._request(
            "POST",
            "/api/local-agent/seatalk/todos/mark-processed-until",
            {"owner_email": owner_email, "processed_until": processed_until},
        )

    def seatalk_todos_merge_open(self, *, owner_email: str, todos: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload = self._request("POST", "/api/local-agent/seatalk/todos/merge-open", {"owner_email": owner_email, "todos": todos})
        items = payload.get("todos")
        return items if isinstance(items, list) else []

    def seatalk_todo_complete(self, *, owner_email: str, todo: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/todos/complete", {"owner_email": owner_email, "todo": todo})

    def seatalk_name_mappings_get(self) -> dict[str, str]:
        payload = self._request("POST", "/api/local-agent/seatalk/name-mappings/store/get", {})
        mappings = payload.get("mappings")
        return mappings if isinstance(mappings, dict) else {}

    def seatalk_name_mappings_merge(self, mappings: dict[str, str]) -> dict[str, str]:
        payload = self._request("POST", "/api/local-agent/seatalk/name-mappings/store/merge", {"mappings": mappings})
        saved = payload.get("mappings")
        return saved if isinstance(saved, dict) else {}

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None, *, signed: bool = True) -> dict[str, Any]:
        url = urljoin(self.base_url, path.lstrip("/"))
        body = b""
        headers = {
            "Accept": "application/json",
            "ngrok-skip-browser-warning": "true",
        }
        if method.upper() != "GET":
            body = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if signed:
            request_path = urlsplit(url).path or "/"
            headers.update(sign_headers(secret=self.hmac_secret, method=method, path=request_path, body=body))
        max_attempts = len(LOCAL_AGENT_TRANSIENT_UNREADABLE_RETRY_DELAYS_SECONDS) + 1
        for attempt in range(max_attempts):
            try:
                response = self.session.request(
                    method,
                    url,
                    data=body if body else None,
                    headers=headers,
                    timeout=(self.connect_timeout_seconds, self.timeout_seconds),
                )
            except requests.RequestException as error:
                raise ToolError(f"Mac local-agent is unavailable: {error}") from error
            try:
                response_payload = response.json()
                break
            except ValueError as error:
                body_preview = response.text.strip().replace("\n", " ")[:160]
                if (
                    attempt < max_attempts - 1
                    and _is_transient_unreadable_local_agent_response(status_code=response.status_code, body_preview=body_preview)
                ):
                    time.sleep(LOCAL_AGENT_TRANSIENT_UNREADABLE_RETRY_DELAYS_SECONDS[attempt])
                    continue
                host = urlsplit(url).netloc or self.base_url
                detail = f" HTTP {response.status_code} from {host}."
                if body_preview:
                    detail += f" Response starts with: {body_preview}"
                raise ToolError(f"Mac local-agent returned an unreadable response.{detail}") from error
        if response.status_code >= 400 or response_payload.get("status") == "error":
            message = str(response_payload.get("message") or f"Mac local-agent request failed with HTTP {response.status_code}.")
            raise ToolError(message)
        return response_payload


class RemoteSeaTalkDashboardService:
    def __init__(self, client: LocalAgentClient, *, name_mappings_provider: Callable[[], dict[str, str]] | None = None) -> None:
        self.client = client
        self.name_mappings_provider = name_mappings_provider or (lambda: {})

    def build_overview(self) -> dict[str, Any]:
        return _strip_status(self.client.seatalk_overview())

    def build_insights(self, *, todo_since: str | None = None) -> dict[str, Any]:
        return _strip_status(self.client.seatalk_insights(name_mappings=self.name_mappings_provider(), todo_since=todo_since))

    def build_project_updates(self) -> dict[str, Any]:
        return _strip_status(self.client.seatalk_project_updates(name_mappings=self.name_mappings_provider()))

    def build_todos(self, *, todo_since: str | None = None) -> dict[str, Any]:
        return _strip_status(self.client.seatalk_todos(name_mappings=self.name_mappings_provider(), todo_since=todo_since))

    def build_name_mappings(self) -> dict[str, Any]:
        return _strip_status(self.client.seatalk_name_mappings())

    def export_history_text(self) -> tuple[str, str]:
        return self.client.seatalk_export(name_mappings=self.name_mappings_provider())


class RemoteSourceCodeQAService:
    def __init__(self, client: LocalAgentClient, fallback_service: Any, *, llm_provider: str | None = None) -> None:
        self.client = client
        self.fallback_service = fallback_service
        self.llm_provider_name = fallback_service.normalize_query_llm_provider(llm_provider) if llm_provider else fallback_service.llm_provider_name
        self.llm_budgets = fallback_service.llm_budgets
        self._config_payload: dict[str, Any] | None = None

    def with_llm_provider(self, llm_provider: str) -> "RemoteSourceCodeQAService":
        return RemoteSourceCodeQAService(self.client, self.fallback_service.with_llm_provider(llm_provider), llm_provider=llm_provider)

    def _source_code_qa_config_payload(self) -> dict[str, Any]:
        if self._config_payload is None:
            self._config_payload = _strip_status(self.client.source_code_qa_config(llm_provider=self.llm_provider_name))
        return self._config_payload

    def load_config(self) -> dict[str, Any]:
        return self._source_code_qa_config_payload().get("config") or {}

    def llm_ready(self) -> bool:
        return bool(self._source_code_qa_config_payload().get("llm_ready"))

    def git_auth_ready(self) -> bool:
        return bool(self._source_code_qa_config_payload().get("git_auth_ready"))

    def options_payload(self) -> dict[str, Any]:
        return self.fallback_service.options_payload()

    def llm_policy_payload(self) -> dict[str, Any]:
        payload = self._source_code_qa_config_payload()
        return payload.get("llm_policy") or self.fallback_service.llm_policy_payload()

    def index_health_payload(self) -> dict[str, Any]:
        return self._source_code_qa_config_payload().get("index_health") or {}

    def domain_knowledge_payload(self) -> dict[str, Any]:
        return self._source_code_qa_config_payload().get("domain_knowledge") or {}

    def _llm_fallback_model(self) -> str:
        return self.fallback_service._llm_fallback_model()

    def save_mapping(self, *, pm_team: str, country: str, repositories: list[dict[str, Any]]) -> dict[str, Any]:
        result = self.client.source_code_qa_save_mapping(pm_team=pm_team, country=country, repositories=repositories)
        self._config_payload = None
        return result

    def sync(self, *, pm_team: str, country: str) -> dict[str, Any]:
        return self.client.source_code_qa_sync(pm_team=pm_team, country=country)

    def ensure_synced_today(self, *, pm_team: str, country: str) -> dict[str, Any]:
        return self.client.source_code_qa_ensure_synced_today(pm_team=pm_team, country=country)

    def ensure_synced_today_background(self, *, pm_team: str, country: str) -> dict[str, Any]:
        return self.client.source_code_qa_ensure_synced_today(pm_team=pm_team, country=country, background=True)

    def query(self, **kwargs: Any) -> dict[str, Any]:
        payload = dict(kwargs)
        progress_callback = payload.pop("progress_callback", None)
        payload["llm_provider"] = self.llm_provider_name
        return self.client.source_code_qa_query(
            payload,
            progress_callback=progress_callback if callable(progress_callback) else None,
        )


def _strip_status(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "status"}


class RemoteBPMISClient:
    def __init__(self, client: LocalAgentClient, *, access_token: str | None = None) -> None:
        self.client = client
        self.access_token = access_token

    def ping(self) -> None:
        self._call("ping")

    def find_project(self, issue_id: str) -> ProjectMatch:
        payload = self._call("find_project", issue_id)
        return ProjectMatch(project_id=str(payload.get("project_id") or ""), raw=payload.get("raw") or {})

    def create_jira_ticket(
        self,
        project: ProjectMatch,
        fields: dict[str, str],
        *,
        preformatted_summary: bool = False,
    ) -> CreatedTicket:
        payload = self._call(
            "create_jira_ticket",
            {"project_id": project.project_id, "raw": project.raw},
            fields,
            preformatted_summary=preformatted_summary,
        )
        return CreatedTicket(
            ticket_key=payload.get("ticket_key"),
            ticket_link=payload.get("ticket_link"),
            raw=payload.get("raw") or {},
        )

    def list_biz_projects_for_pm_email(self, email: str) -> list[dict[str, str]]:
        return self._call("list_biz_projects_for_pm_email", email) or []

    def list_jira_tasks_for_project_created_by_email(self, project_issue_id: str, email: str) -> list[dict[str, Any]]:
        return self._call("list_jira_tasks_for_project_created_by_email", project_issue_id, email) or []

    def list_jira_tasks_created_by_emails(self, emails: list[str]) -> list[dict[str, Any]]:
        return self._call("list_jira_tasks_created_by_emails", emails) or []

    def get_single_brd_doc_link_for_project(self, project_issue_id: str) -> str:
        return str(self._call("get_single_brd_doc_link_for_project", project_issue_id) or "")

    def get_single_brd_doc_links_for_projects(self, project_issue_ids: list[str]) -> dict[str, str]:
        return self._call("get_single_brd_doc_links_for_projects", project_issue_ids) or {}

    def get_brd_doc_links_for_projects(self, project_issue_ids: list[str]) -> dict[str, list[str]]:
        return self._call("get_brd_doc_links_for_projects", project_issue_ids) or {}

    def search_versions(self, query: str) -> list[dict[str, Any]]:
        return self._call("search_versions", query) or []

    def list_issues_for_version(self, version_id: str | int) -> list[dict[str, Any]]:
        return self._call("list_issues_for_version", version_id) or []

    def get_issue_detail(self, issue_id: str | int) -> dict[str, Any]:
        return self._call("get_issue_detail", issue_id) or {}

    def get_jira_ticket_detail(self, ticket_key: str) -> dict[str, Any]:
        return self._call("get_jira_ticket_detail", ticket_key) or {}

    def update_jira_ticket_status(self, ticket_key: str, status: str) -> dict[str, Any]:
        return self._call("update_jira_ticket_status", ticket_key, status) or {}

    def update_jira_ticket_fix_version(self, ticket_key: str, version_name: str, version_id: str | None = None) -> dict[str, Any]:
        return self._call("update_jira_ticket_fix_version", ticket_key, version_name, version_id) or {}

    def delink_jira_ticket_from_project(self, ticket_key: str, project_issue_id: str | int) -> dict[str, Any]:
        return self._call("delink_jira_ticket_from_project", ticket_key, project_issue_id) or {}

    def _call(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        return self.client.bpmis_call(
            operation=operation,
            access_token=self.access_token,
            args=list(args),
            kwargs=kwargs,
        )


class RemoteBPMISProjectStore:
    def __init__(self, client: LocalAgentClient) -> None:
        self.client = client

    def upsert_project(
        self,
        *,
        user_key: str,
        bpmis_id: str,
        project_name: str,
        brd_link: str,
        market: str,
    ) -> str:
        return self.client.bpmis_project_upsert(
            user_key=user_key,
            bpmis_id=bpmis_id,
            project_name=project_name,
            brd_link=brd_link,
            market=market,
        )

    def list_projects(self, *, user_key: str) -> list[dict[str, Any]]:
        return self.client.bpmis_projects_list(user_key=user_key)

    def reorder_projects(self, *, user_key: str, bpmis_ids: list[str]) -> list[dict[str, Any]]:
        return self.client.bpmis_projects_reorder(user_key=user_key, bpmis_ids=bpmis_ids)

    def get_project(self, *, user_key: str, bpmis_id: str) -> dict[str, Any] | None:
        issue_id = str(bpmis_id or "").strip()
        for project in self.list_projects(user_key=user_key):
            if str(project.get("bpmis_id") or "").strip() == issue_id:
                return project
        return None

    def soft_delete_project(self, *, user_key: str, bpmis_id: str) -> bool:
        return self.client.bpmis_project_delete(user_key=user_key, bpmis_id=bpmis_id)

    def update_project_comment(self, *, user_key: str, bpmis_id: str, pm_comment: str) -> bool:
        return self.client.bpmis_project_comment_update(user_key=user_key, bpmis_id=bpmis_id, pm_comment=pm_comment)

    def add_jira_ticket(self, **ticket: Any) -> dict[str, Any]:
        return self.client.bpmis_project_ticket_add(**ticket)

    def upsert_synced_jira_ticket(self, **ticket: Any) -> dict[str, Any] | None:
        return self.client.bpmis_project_ticket_upsert_synced(**ticket)

    def delete_jira_ticket(self, *, user_key: str, bpmis_id: str, ticket_id: str | int) -> bool:
        return self.client.bpmis_project_ticket_delete(user_key=user_key, bpmis_id=bpmis_id, ticket_id=ticket_id)

    def update_jira_ticket_status(self, *, user_key: str, bpmis_id: str, ticket_id: str | int, status: str) -> bool:
        return self.client.bpmis_project_ticket_status_update(
            user_key=user_key,
            bpmis_id=bpmis_id,
            ticket_id=ticket_id,
            status=status,
        )

    def update_jira_ticket_version(
        self,
        *,
        user_key: str,
        bpmis_id: str,
        ticket_id: str | int,
        version_name: str,
        version_id: str = "",
    ) -> bool:
        return self.client.bpmis_project_ticket_version_update(
            user_key=user_key,
            bpmis_id=bpmis_id,
            ticket_id=ticket_id,
            version_name=version_name,
            version_id=version_id,
        )


class RemoteSourceCodeQASessionStore:
    def __init__(self, client: LocalAgentClient) -> None:
        self.client = client

    def list(self, *, owner_email: str, limit: int = 30) -> list[dict[str, Any]]:
        return self.client.source_code_qa_sessions_list(owner_email=owner_email, limit=limit)

    def create(self, **kwargs: Any) -> dict[str, Any]:
        return self.client.source_code_qa_session_create(**kwargs)

    def get(self, session_id: str, *, owner_email: str) -> dict[str, Any] | None:
        return self.client.source_code_qa_session_get(session_id=session_id, owner_email=owner_email)

    def archive(self, session_id: str, *, owner_email: str) -> dict[str, Any] | None:
        return self.client.source_code_qa_session_archive(session_id=session_id, owner_email=owner_email)

    def get_context(self, session_id: str, *, owner_email: str) -> dict[str, Any] | None:
        return self.client.source_code_qa_session_context(session_id=session_id, owner_email=owner_email)

    def append_exchange(self, session_id: str, **kwargs: Any) -> dict[str, Any] | None:
        return self.client.source_code_qa_session_append(session_id=session_id, **kwargs)


class RemoteSourceCodeQAAttachmentStore:
    def __init__(self, client: LocalAgentClient) -> None:
        self.client = client

    def save_bytes(
        self,
        *,
        owner_email: str,
        session_id: str,
        filename: str,
        content: bytes,
        mime_type: str = "",
    ) -> dict[str, Any]:
        return self.client.source_code_qa_attachment_save(
            owner_email=owner_email,
            session_id=session_id,
            filename=filename,
            mime_type=mime_type,
            content=content,
        )

    def resolve_many(self, *, owner_email: str, session_id: str, attachment_ids: list[str]) -> list[dict[str, Any]]:
        return self.client.source_code_qa_attachments_resolve(
            owner_email=owner_email,
            session_id=session_id,
            attachment_ids=attachment_ids,
        )

    def get_bytes(self, *, owner_email: str, session_id: str, attachment_id: str) -> tuple[dict[str, Any], bytes]:
        return self.client.source_code_qa_attachment_get(
            owner_email=owner_email,
            session_id=session_id,
            attachment_id=attachment_id,
        )


class RemoteSourceCodeQAModelAvailabilityStore:
    def __init__(self, client: LocalAgentClient) -> None:
        self.client = client

    def get(self) -> dict[str, bool]:
        return self.client.source_code_qa_model_availability_get()

    def save(self, availability: dict[str, Any]) -> dict[str, bool]:
        return self.client.source_code_qa_model_availability_save(availability)


class RemoteSourceCodeQARuntimeEvidenceStore:
    def __init__(self, client: LocalAgentClient) -> None:
        self.client = client

    def list(self, *, pm_team: str, country: str) -> list[dict[str, Any]]:
        return self.client.source_code_qa_runtime_evidence_list(pm_team=pm_team, country=country)

    def save_bytes(
        self,
        *,
        pm_team: str,
        country: str,
        source_type: str,
        uploaded_by: str,
        filename: str,
        content: bytes,
        mime_type: str = "",
    ) -> dict[str, Any]:
        return self.client.source_code_qa_runtime_evidence_save(
            pm_team=pm_team,
            country=country,
            source_type=source_type,
            uploaded_by=uploaded_by,
            filename=filename,
            mime_type=mime_type,
            content=content,
        )

    def resolve_scope(self, *, pm_team: str, country: str) -> list[dict[str, Any]]:
        return self.client.source_code_qa_runtime_evidence_resolve(pm_team=pm_team, country=country)

    def delete(self, *, pm_team: str, country: str, evidence_id: str) -> bool:
        return self.client.source_code_qa_runtime_evidence_delete(
            pm_team=pm_team,
            country=country,
            evidence_id=evidence_id,
        )


class RemoteSeaTalkTodoStore:
    def __init__(self, client: LocalAgentClient) -> None:
        self.client = client

    def completed_ids(self, *, owner_email: str) -> set[str]:
        return set(self.client.seatalk_todos_completed_ids(owner_email=owner_email))

    def open_todos(self, *, owner_email: str) -> list[dict[str, Any]]:
        return self.client.seatalk_todos_open(owner_email=owner_email)

    def processed_until(self, *, owner_email: str) -> str:
        return self.client.seatalk_todos_processed_until(owner_email=owner_email)

    def mark_processed_until(self, *, owner_email: str, processed_until: str) -> None:
        self.client.seatalk_todos_mark_processed_until(owner_email=owner_email, processed_until=processed_until)

    def merge_open_todos(self, *, owner_email: str, todos: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self.client.seatalk_todos_merge_open(owner_email=owner_email, todos=todos)

    def mark_completed(self, *, owner_email: str, todo: dict[str, Any]) -> dict[str, Any]:
        return self.client.seatalk_todo_complete(owner_email=owner_email, todo=todo)


class RemoteSeaTalkNameMappingStore:
    def __init__(self, client: LocalAgentClient) -> None:
        self.client = client

    def mappings(self) -> dict[str, str]:
        return self.client.seatalk_name_mappings_get()

    def merge_mappings(self, mappings: dict[str, str]) -> dict[str, str]:
        return self.client.seatalk_name_mappings_merge(mappings)
