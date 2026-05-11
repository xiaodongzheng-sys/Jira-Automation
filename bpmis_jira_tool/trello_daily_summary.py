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
TRELLO_WORKFLOW_LIST_INBOX = "Inbox"
TRELLO_WORKFLOW_LIST_TODAY = "Today"
TRELLO_WORKFLOW_LIST_THIS_WEEK = "This Week"
TRELLO_WORKFLOW_LIST_FOLLOW_UP = "Waiting / Follow-up"
TRELLO_WORKFLOW_LIST_WATCH = "Watch / Risk"
TRELLO_WORKFLOW_LIST_BACKLOG = "Project Backlog"
TRELLO_WORKFLOW_LIST_PERSONAL = "Personal / Sensitive"
TRELLO_WORKFLOW_LIST_DONE = "Done"
TRELLO_DOMAIN_LABELS = {
    "AF-ID": "red",
    "AF-SG": "blue",
    "AF-PH": "sky",
    "GRC": "orange",
    "Credit Risk": "purple",
    "AI": "green",
    "Personal": "yellow",
    "Sensitive": "black",
}


@dataclass(frozen=True)
class TrelloCardSpec:
    section: str
    name: str
    description: str
    fingerprint_text: str
    domain: str = "General"
    target_list: str = TRELLO_WORKFLOW_LIST_INBOX
    labels: tuple[str, ...] = ()
    due: str = ""


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
        self._write_board_id = ""
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

    def board_lists(self) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}/boards/{self.board_id}/lists",
            params={**self._auth_params(), "fields": "name,closed,idBoard"},
            timeout=20,
        )
        payload = self._json_response(response, action="load Trello board lists")
        if not isinstance(payload, list):
            raise ToolError("Trello returned an invalid board list payload.")
        return [item for item in payload if isinstance(item, dict)]

    def get_or_create_list_id(self, list_name: str | None = None) -> str:
        target_name = str(list_name or self.list_name or DEFAULT_DAILY_LIST_NAME).strip() or DEFAULT_DAILY_LIST_NAME
        payload = self.board_lists()
        for item in payload:
            if not isinstance(item, dict) or item.get("closed"):
                continue
            if str(item.get("name") or "").strip().lower() == target_name.lower():
                list_id = str(item.get("id") or "").strip()
                if list_id:
                    return list_id
        created = self.session.post(
            f"{self.base_url}/lists",
            params={**self._auth_params(), "idBoard": self._board_id_for_writes(payload), "name": target_name},
            timeout=20,
        )
        created_payload = self._json_response(created, action="create Trello daily summary list")
        if not isinstance(created_payload, dict) or not str(created_payload.get("id") or "").strip():
            raise ToolError("Trello did not return an id for the daily summary list.")
        return str(created_payload["id"])

    def create_card(
        self,
        *,
        list_id: str,
        name: str,
        description: str,
        label_ids: list[str] | tuple[str, ...] | None = None,
        due: str | None = None,
    ) -> TrelloCardResult:
        params = {**self._auth_params(), "idList": list_id, "name": name, "desc": description}
        clean_label_ids = [str(item).strip() for item in (label_ids or []) if str(item).strip()]
        if clean_label_ids:
            params["idLabels"] = ",".join(clean_label_ids)
        if due:
            params["due"] = str(due)
        response = self.session.post(
            f"{self.base_url}/cards",
            params=params,
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

    def list_cards(self, *, list_id: str) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}/lists/{list_id}/cards",
            params={**self._auth_params(), "fields": "name,desc,url,shortUrl,id,closed"},
            timeout=20,
        )
        payload = self._json_response(response, action="load Trello daily summary cards")
        if not isinstance(payload, list):
            raise ToolError("Trello returned an invalid card list payload.")
        return [item for item in payload if isinstance(item, dict) and not item.get("closed")]

    def list_board_cards(self) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}/boards/{self.board_id}/cards",
            params={
                **self._auth_params(),
                "filter": "open",
                "fields": "name,desc,url,shortUrl,id,closed,idList,due,dueComplete,labels",
            },
            timeout=20,
        )
        payload = self._json_response(response, action="load Trello board cards")
        if not isinstance(payload, list):
            raise ToolError("Trello returned an invalid board card payload.")
        return [item for item in payload if isinstance(item, dict) and not item.get("closed")]

    def board_labels(self) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}/boards/{self.board_id}/labels",
            params={**self._auth_params(), "limit": "1000", "fields": "name,color"},
            timeout=20,
        )
        payload = self._json_response(response, action="load Trello board labels")
        if not isinstance(payload, list):
            raise ToolError("Trello returned an invalid board label payload.")
        return [item for item in payload if isinstance(item, dict)]

    def get_or_create_label_id(self, label_name: str, *, color: str | None = None) -> str:
        target_name = str(label_name or "").strip()
        if not target_name:
            return ""
        for label in self.board_labels():
            if str(label.get("name") or "").strip().lower() == target_name.lower():
                return str(label.get("id") or "").strip()
        response = self.session.post(
            f"{self.base_url}/labels",
            params={
                **self._auth_params(),
                "idBoard": self._board_id_for_writes(),
                "name": target_name,
                "color": str(color or TRELLO_DOMAIN_LABELS.get(target_name) or "blue"),
            },
            timeout=20,
        )
        payload = self._json_response(response, action="create Trello board label")
        if not isinstance(payload, dict) or not str(payload.get("id") or "").strip():
            raise ToolError("Trello did not return an id for the created label.")
        return str(payload["id"])

    def get_or_create_label_ids(self, label_names: list[str] | tuple[str, ...]) -> list[str]:
        label_ids = []
        for label_name in label_names:
            label_id = self.get_or_create_label_id(label_name, color=TRELLO_DOMAIN_LABELS.get(label_name))
            if label_id:
                label_ids.append(label_id)
        return label_ids

    def _board_id_for_writes(self, lists: list[dict[str, Any]] | None = None) -> str:
        if self._write_board_id:
            return self._write_board_id
        payload = lists if lists is not None else self.board_lists()
        for item in payload:
            board_id = str(item.get("idBoard") or "").strip()
            if board_id:
                self._write_board_id = board_id
                return board_id
        return self.board_id

    def move_card(self, *, card_id: str, list_id: str) -> None:
        self._json_response(
            self.session.put(
                f"{self.base_url}/cards/{card_id}",
                params={**self._auth_params(), "idList": list_id},
                timeout=20,
            ),
            action="move Trello card",
        )

    def add_label_to_card(self, *, card_id: str, label_id: str) -> None:
        self._json_response(
            self.session.post(
                f"{self.base_url}/cards/{card_id}/idLabels",
                params={**self._auth_params(), "value": label_id},
                timeout=20,
            ),
            action="add Trello card label",
        )

    def archive_card(self, *, card_id: str) -> None:
        self._json_response(
            self.session.put(
                f"{self.base_url}/cards/{card_id}",
                params={**self._auth_params(), "closed": "true"},
                timeout=20,
            ),
            action="archive Trello card",
        )

    def rename_list(self, *, list_id: str, name: str) -> None:
        self._json_response(
            self.session.put(
                f"{self.base_url}/lists/{list_id}",
                params={**self._auth_params(), "name": name},
                timeout=20,
            ),
            action="rename Trello list",
        )

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


def normalize_daily_card_identity_text(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", str(value or "").lower()))


def daily_card_board_identity(*, run_date: str, name: str, domain: str) -> str:
    return "|".join(
        [
            normalize_daily_card_identity_text(run_date),
            normalize_daily_card_identity_text(domain),
            normalize_daily_card_identity_text(name),
        ]
    )


def daily_card_identity_from_trello_card(card: dict[str, Any]) -> str:
    name = str(card.get("name") or "")
    description = str(card.get("desc") or "")
    report_date = ""
    domain = ""
    for raw_line in description.splitlines():
        line = raw_line.strip()
        if line.lower().startswith("report date:"):
            report_date = line.split(":", 1)[1].strip()
        elif line.lower().startswith("domain:"):
            domain = line.split(":", 1)[1].strip()
    if not report_date:
        return ""
    return daily_card_board_identity(run_date=report_date, name=name, domain=domain)
