from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

from bpmis_jira_tool.errors import ConfigError, ToolError


DEFAULT_DAILY_LIST_NAME = "Daily Summary Email"
TRELLO_API_BASE_URL = "https://api.trello.com/1"


@dataclass(frozen=True)
class TrelloCardSpec:
    section: str
    name: str
    description: str
    fingerprint_text: str
    domain: str = "General"


@dataclass(frozen=True)
class TrelloCardResult:
    status: str
    name: str
    url: str = ""
    trello_id: str = ""


@dataclass(frozen=True)
class TrelloSyncResult:
    status: str = "disabled"
    created_count: int = 0
    skipped_count: int = 0
    cards: list[dict[str, str]] = field(default_factory=list)


class TrelloDailySummaryClient:
    def __init__(
        self,
        *,
        api_key: str,
        api_token: str,
        board_id: str,
        list_name: str = DEFAULT_DAILY_LIST_NAME,
        session: requests.Session | None = None,
        base_url: str = TRELLO_API_BASE_URL,
    ) -> None:
        self.api_key = api_key.strip()
        self.api_token = api_token.strip()
        self.board_id = board_id.strip()
        self.list_name = list_name.strip() or DEFAULT_DAILY_LIST_NAME
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")
        if not self.api_key or not self.api_token or not self.board_id:
            raise ConfigError("Trello daily summary requires TRELLO_API_KEY, TRELLO_API_TOKEN, and TRELLO_BOARD_ID.")

    @classmethod
    def from_env(cls, *, session: requests.Session | None = None) -> "TrelloDailySummaryClient":
        return cls(
            api_key=str(os.getenv("TRELLO_API_KEY") or ""),
            api_token=str(os.getenv("TRELLO_API_TOKEN") or ""),
            board_id=str(os.getenv("TRELLO_BOARD_ID") or ""),
            list_name=str(os.getenv("TRELLO_DAILY_LIST_NAME") or DEFAULT_DAILY_LIST_NAME),
            session=session,
        )

    def get_or_create_list_id(self) -> str:
        response = self.session.get(
            f"{self.base_url}/boards/{self.board_id}/lists",
            params={**self._auth_params(), "fields": "name,closed"},
            timeout=20,
        )
        payload = self._json_response(response, action="load Trello board lists")
        if not isinstance(payload, list):
            raise ToolError("Trello returned an invalid board list payload.")
        for item in payload:
            if not isinstance(item, dict) or item.get("closed"):
                continue
            if str(item.get("name") or "").strip().lower() == self.list_name.lower():
                list_id = str(item.get("id") or "").strip()
                if list_id:
                    return list_id
        created = self.session.post(
            f"{self.base_url}/lists",
            params={**self._auth_params(), "idBoard": self.board_id, "name": self.list_name},
            timeout=20,
        )
        created_payload = self._json_response(created, action="create Trello daily summary list")
        if not isinstance(created_payload, dict) or not str(created_payload.get("id") or "").strip():
            raise ToolError("Trello did not return an id for the daily summary list.")
        return str(created_payload["id"])

    def create_card(self, *, list_id: str, name: str, description: str) -> TrelloCardResult:
        response = self.session.post(
            f"{self.base_url}/cards",
            params={**self._auth_params(), "idList": list_id, "name": name, "desc": description},
            timeout=20,
        )
        payload = self._json_response(response, action="create Trello card")
        if not isinstance(payload, dict):
            raise ToolError("Trello returned an invalid card payload.")
        trello_id = str(payload.get("id") or "").strip()
        url = str(payload.get("url") or payload.get("shortUrl") or "").strip()
        if not trello_id and not url:
            raise ToolError("Trello did not return an id or URL for the created card.")
        return TrelloCardResult(status="created", name=str(payload.get("name") or name), url=url, trello_id=trello_id)

    def _auth_params(self) -> dict[str, str]:
        return {"key": self.api_key, "token": self.api_token}

    @staticmethod
    def _json_response(response: requests.Response, *, action: str) -> Any:
        try:
            response.raise_for_status()
        except requests.RequestException as error:
            text = str(getattr(response, "text", "") or "").strip()
            detail = f": {text[:240]}" if text else ""
            raise ToolError(f"Failed to {action}{detail}") from error
        try:
            return response.json()
        except ValueError as error:
            raise ToolError(f"Trello returned invalid JSON while trying to {action}.") from error


class TrelloDailySummaryStore:
    def __init__(self, storage_path: Path) -> None:
        self.storage_path = storage_path

    def has_card(self, fingerprint: str) -> bool:
        return fingerprint in self._load().get("cards", {})

    def mark_card(self, *, fingerprint: str, name: str, url: str, trello_id: str, created_at: str) -> None:
        payload = self._load()
        cards = payload.setdefault("cards", {})
        cards[fingerprint] = {
            "name": name,
            "url": url,
            "trello_id": trello_id,
            "created_at": created_at,
        }
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, self.storage_path)

    def _load(self) -> dict[str, Any]:
        if not self.storage_path.exists():
            return {"cards": {}}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"cards": {}}
        return payload if isinstance(payload, dict) else {"cards": {}}


def fingerprint_daily_card(*, run_date: str, section: str, item_text: str, domain: str) -> str:
    normalized = " ".join(re.findall(r"[a-z0-9]+", f"{run_date} {section} {domain} {item_text}".lower()))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

