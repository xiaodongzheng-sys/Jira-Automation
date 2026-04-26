from __future__ import annotations

import json
from typing import Any, Callable
from urllib.parse import urljoin, urlsplit

import requests

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_protocol import sign_headers


class LocalAgentClient:
    def __init__(self, *, base_url: str, hmac_secret: str, timeout_seconds: int = 300) -> None:
        self.base_url = str(base_url or "").rstrip("/") + "/"
        self.hmac_secret = str(hmac_secret or "")
        self.timeout_seconds = max(5, int(timeout_seconds or 300))
        if not self.base_url.strip("/"):
            raise ToolError("LOCAL_AGENT_BASE_URL is required before using Mac local-agent capabilities.")
        if not self.hmac_secret:
            raise ToolError("LOCAL_AGENT_HMAC_SECRET is required before using Mac local-agent capabilities.")

    def get_health(self) -> dict[str, Any]:
        return self._request("GET", "/healthz", signed=False)

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

    def source_code_qa_ensure_synced_today(self, *, pm_team: str, country: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/local-agent/source-code-qa/ensure-synced-today",
            {"pm_team": pm_team, "country": country},
        )

    def source_code_qa_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/source-code-qa/query", payload)

    def seatalk_overview(self) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/overview", {})

    def seatalk_insights(self, *, name_mappings: dict[str, str] | None = None) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/insights", {"name_mappings": name_mappings or {}})

    def seatalk_name_mappings(self) -> dict[str, Any]:
        return self._request("POST", "/api/local-agent/seatalk/name-mappings", {})

    def seatalk_export(self, *, name_mappings: dict[str, str] | None = None) -> tuple[str, str]:
        payload = self._request("POST", "/api/local-agent/seatalk/export", {"name_mappings": name_mappings or {}})
        return str(payload.get("content") or ""), str(payload.get("filename") or "seatalk-history-last-7-days.txt")

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None, *, signed: bool = True) -> dict[str, Any]:
        url = urljoin(self.base_url, path.lstrip("/"))
        body = b""
        headers = {"Accept": "application/json"}
        if method.upper() != "GET":
            body = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if signed:
            request_path = urlsplit(url).path or "/"
            headers.update(sign_headers(secret=self.hmac_secret, method=method, path=request_path, body=body))
        try:
            response = requests.request(method, url, data=body if body else None, headers=headers, timeout=self.timeout_seconds)
        except requests.RequestException as error:
            raise ToolError(f"Mac local-agent is unavailable: {error}") from error
        try:
            response_payload = response.json()
        except ValueError as error:
            raise ToolError("Mac local-agent returned an unreadable response.") from error
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

    def build_insights(self) -> dict[str, Any]:
        return _strip_status(self.client.seatalk_insights(name_mappings=self.name_mappings_provider()))

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

    def with_llm_provider(self, llm_provider: str) -> "RemoteSourceCodeQAService":
        return RemoteSourceCodeQAService(self.client, self.fallback_service.with_llm_provider(llm_provider), llm_provider=llm_provider)

    def load_config(self) -> dict[str, Any]:
        return _strip_status(self.client.source_code_qa_config(llm_provider=self.llm_provider_name)).get("config") or {}

    def llm_ready(self) -> bool:
        return bool(_strip_status(self.client.source_code_qa_config(llm_provider=self.llm_provider_name)).get("llm_ready"))

    def options_payload(self) -> dict[str, Any]:
        return self.fallback_service.options_payload()

    def llm_policy_payload(self) -> dict[str, Any]:
        payload = _strip_status(self.client.source_code_qa_config(llm_provider=self.llm_provider_name))
        return payload.get("llm_policy") or self.fallback_service.llm_policy_payload()

    def index_health_payload(self) -> dict[str, Any]:
        return _strip_status(self.client.source_code_qa_config(llm_provider=self.llm_provider_name)).get("index_health") or {}

    def domain_knowledge_payload(self) -> dict[str, Any]:
        return _strip_status(self.client.source_code_qa_config(llm_provider=self.llm_provider_name)).get("domain_knowledge") or {}

    def _llm_fallback_model(self) -> str:
        return self.fallback_service._llm_fallback_model()

    def save_mapping(self, *, pm_team: str, country: str, repositories: list[dict[str, Any]]) -> dict[str, Any]:
        return self.client.source_code_qa_save_mapping(pm_team=pm_team, country=country, repositories=repositories)

    def sync(self, *, pm_team: str, country: str) -> dict[str, Any]:
        return self.client.source_code_qa_sync(pm_team=pm_team, country=country)

    def ensure_synced_today(self, *, pm_team: str, country: str) -> dict[str, Any]:
        return self.client.source_code_qa_ensure_synced_today(pm_team=pm_team, country=country)

    def query(self, **kwargs: Any) -> dict[str, Any]:
        payload = dict(kwargs)
        payload["llm_provider"] = self.llm_provider_name
        payload.pop("progress_callback", None)
        return self.client.source_code_qa_query(payload)


def _strip_status(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "status"}
