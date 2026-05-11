from __future__ import annotations

import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
from typing import Any

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService


class SeaTalkTodoStore:
    def __init__(self, storage_path: Path | None = None) -> None:
        self.storage_path = storage_path
        self._payload = self._load()
        self._lock = threading.Lock()

    def _load(self) -> dict[str, Any]:
        if self.storage_path is None or not self.storage_path.exists():
            return {"owners": {}}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"owners": {}}
        return payload if isinstance(payload, dict) else {"owners": {}}

    def _persist_locked(self) -> None:
        if self.storage_path is None:
            return
        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {**self._payload, "updated_at": time.time()}
            temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.storage_path)
        except OSError:
            return

    @staticmethod
    def _now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    @staticmethod
    def todo_id(todo: dict[str, Any]) -> str:
        explicit = str(todo.get("id") or "").strip()
        if explicit:
            return explicit
        stable = "|".join(
            (
                re.sub(r"[^a-z0-9]+", " ", str(todo.get("domain") or "").lower()).strip(),
                re.sub(r"[^a-z0-9]+", " ", str(todo.get("task") or "").lower()).strip(),
            )
        )
        return hashlib.sha256(stable.encode("utf-8")).hexdigest()[:16]

    def completed_ids(self, *, owner_email: str) -> set[str]:
        owner = str(owner_email or "").strip().lower()
        with self._lock:
            owners = self._payload.get("owners") if isinstance(self._payload.get("owners"), dict) else {}
            owner_payload = owners.get(owner) if isinstance(owners.get(owner), dict) else {}
            completed = owner_payload.get("completed") if isinstance(owner_payload.get("completed"), dict) else {}
            return {str(todo_id) for todo_id in completed if str(todo_id).strip()}

    def processed_until(self, *, owner_email: str) -> str:
        owner = str(owner_email or "").strip().lower()
        with self._lock:
            owners = self._payload.get("owners") if isinstance(self._payload.get("owners"), dict) else {}
            owner_payload = owners.get(owner) if isinstance(owners.get(owner), dict) else {}
            return str(owner_payload.get("todo_processed_until") or "").strip()

    def mark_processed_until(self, *, owner_email: str, processed_until: str) -> None:
        owner = str(owner_email or "").strip().lower()
        value = str(processed_until or "").strip()
        if not owner or not value:
            return
        with self._lock:
            owners = self._payload.setdefault("owners", {})
            owner_payload = owners.setdefault(owner, {})
            current_value = str(owner_payload.get("todo_processed_until") or "").strip()
            if current_value and current_value >= value:
                return
            owner_payload["todo_processed_until"] = value
            self._persist_locked()

    def open_todos(self, *, owner_email: str) -> list[dict[str, Any]]:
        owner = str(owner_email or "").strip().lower()
        with self._lock:
            owners = self._payload.get("owners") if isinstance(self._payload.get("owners"), dict) else {}
            owner_payload = owners.get(owner) if isinstance(owners.get(owner), dict) else {}
            open_items = owner_payload.get("open") if isinstance(owner_payload.get("open"), dict) else {}
            return [dict(todo) for todo in open_items.values() if isinstance(todo, dict)]

    def merge_open_todos(self, *, owner_email: str, todos: list[dict[str, Any]]) -> list[dict[str, Any]]:
        owner = str(owner_email or "").strip().lower()
        if not owner:
            return []
        with self._lock:
            owners = self._payload.setdefault("owners", {})
            owner_payload = owners.setdefault(owner, {})
            open_items = owner_payload.setdefault("open", {})
            completed = owner_payload.get("completed") if isinstance(owner_payload.get("completed"), dict) else {}
            for todo in todos:
                if not isinstance(todo, dict):
                    continue
                todo_id = self.todo_id(todo)
                if not todo_id or todo_id in completed:
                    continue
                similar_id = self._find_similar_open_todo_id(open_items=open_items, todo=todo)
                if similar_id:
                    existing = open_items.get(similar_id) if isinstance(open_items.get(similar_id), dict) else {}
                    open_items[similar_id] = self._merge_similar_open_todo(existing=existing, incoming=todo, todo_id=similar_id)
                    continue
                open_items[todo_id] = {**todo, "id": todo_id, "last_seen_at": self._now()}
            self._persist_locked()
            return [dict(todo) for todo in open_items.values() if isinstance(todo, dict)]

    @classmethod
    def _find_similar_open_todo_id(cls, *, open_items: dict[str, Any], todo: dict[str, Any]) -> str | None:
        for existing_id, existing in open_items.items():
            if not isinstance(existing, dict):
                continue
            if cls._todos_are_similar(existing, todo):
                return str(existing.get("id") or existing_id)
        return None

    @classmethod
    def _todos_are_similar(cls, left: dict[str, Any], right: dict[str, Any]) -> bool:
        left_task = cls._similarity_text(left.get("task"))
        right_task = cls._similarity_text(right.get("task"))
        if not left_task or not right_task:
            return False
        left_domain = SeaTalkDashboardService._normalize_insight_domain(left.get("domain"))
        right_domain = SeaTalkDashboardService._normalize_insight_domain(right.get("domain"))
        same_domain = left_domain == right_domain
        sequence_score = difflib.SequenceMatcher(None, left_task, right_task).ratio()
        token_score = cls._token_overlap_score(left_task, right_task)
        score = max(sequence_score, token_score)
        if score >= (0.78 if same_domain else 0.9):
            return True
        if not same_domain:
            return False
        if not cls._todo_due_compatible(left.get("due"), right.get("due")):
            return False
        left_tokens = cls._informative_todo_tokens(left)
        right_tokens = cls._informative_todo_tokens(right)
        if not left_tokens or not right_tokens:
            return False
        overlap_count = len(left_tokens & right_tokens)
        overlap_ratio = overlap_count / max(1, min(len(left_tokens), len(right_tokens)))
        return overlap_count >= 4 and overlap_ratio >= 0.42

    @staticmethod
    def _similarity_text(value: Any) -> str:
        return re.sub(r"[^\w\u4e00-\u9fff]+", " ", str(value or "").lower()).strip()

    @staticmethod
    def _token_overlap_score(left: str, right: str) -> float:
        left_tokens = {token for token in left.split() if len(token) > 1}
        right_tokens = {token for token in right.split() if len(token) > 1}
        if not left_tokens or not right_tokens:
            return 0.0
        return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)

    @classmethod
    def _informative_todo_tokens(cls, todo: dict[str, Any]) -> set[str]:
        text = cls._similarity_text(f"{todo.get('task') or ''} {todo.get('evidence') or ''}")
        stopwords = {
            "about", "accepted", "after", "aligned", "also", "and", "another", "any", "are", "arrange",
            "asks", "attend", "complete", "discussion", "follow", "for", "from", "help", "if", "invited",
            "join", "keep", "meeting", "needed", "on", "or", "plan", "prepare", "remaining", "says", "session",
            "support", "task", "the", "to", "tool", "up", "with", "xiaodong",
        }
        tokens = {token for token in text.split() if len(token) > 1 and token not in stopwords}
        normalized: set[str] = set()
        for token in tokens:
            normalized.add(token)
            if "-" in token:
                normalized.update(part for part in token.split("-") if len(part) > 1 and part not in stopwords)
        return normalized

    @classmethod
    def _todo_due_compatible(cls, left: Any, right: Any) -> bool:
        left_text = str(left or "").strip().lower()
        right_text = str(right or "").strip().lower()
        if not left_text or left_text == "unknown" or not right_text or right_text == "unknown":
            return True
        left_date = cls._todo_due_date(left_text)
        right_date = cls._todo_due_date(right_text)
        if left_date and right_date:
            return left_date == right_date
        return left_text == right_text

    @staticmethod
    def _todo_due_date(value: str) -> str:
        match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", value)
        return match.group(1) if match else ""

    @classmethod
    def _merge_similar_open_todo(cls, *, existing: dict[str, Any], incoming: dict[str, Any], todo_id: str) -> dict[str, Any]:
        merged = {**existing, "id": todo_id, "last_seen_at": cls._now()}
        priority_rank = {"high": 0, "medium": 1, "low": 2, "unknown": 3}
        incoming_priority = str(incoming.get("priority") or "unknown").strip().lower()
        existing_priority = str(existing.get("priority") or "unknown").strip().lower()
        if priority_rank.get(incoming_priority, 3) < priority_rank.get(existing_priority, 3):
            merged["priority"] = incoming_priority
        existing_due = str(existing.get("due") or "").strip()
        incoming_due = str(incoming.get("due") or "").strip()
        if (not existing_due or existing_due.lower() == "unknown") and incoming_due:
            merged["due"] = incoming_due
        if not str(existing.get("evidence") or "").strip() and str(incoming.get("evidence") or "").strip():
            merged["evidence"] = str(incoming.get("evidence") or "").strip()
        return merged

    def mark_completed(self, *, owner_email: str, todo: dict[str, Any]) -> dict[str, Any]:
        owner = str(owner_email or "").strip().lower()
        todo_id = self.todo_id(todo)
        if not owner or not todo_id:
            raise ToolError("SeaTalk to-do completion requires a signed-in owner and a valid task.")
        with self._lock:
            owners = self._payload.setdefault("owners", {})
            owner_payload = owners.setdefault(owner, {})
            completed = owner_payload.setdefault("completed", {})
            completed[todo_id] = {
                "id": todo_id,
                "task": str(todo.get("task") or "").strip(),
                "domain": str(todo.get("domain") or "").strip(),
                "due": str(todo.get("due") or "").strip(),
                "completed_at": self._now(),
            }
            open_items = owner_payload.get("open") if isinstance(owner_payload.get("open"), dict) else {}
            open_items.pop(todo_id, None)
            self._persist_locked()
            return {"status": "ok", "todo_id": todo_id, "completed_at": completed[todo_id]["completed_at"]}


class SeaTalkNameMappingStore:
    def __init__(self, storage_path: Path | None = None) -> None:
        self.storage_path = storage_path
        self._payload = self._load()
        self._lock = threading.Lock()

    @staticmethod
    def normalize_key(value: Any) -> str:
        key = str(value or "").strip()
        if key.startswith("group-") or key.startswith("buddy-"):
            return key
        uid_match = re.match(r"^UID\s+(.+)$", key, re.IGNORECASE)
        if uid_match and uid_match.group(1).strip():
            return f"UID {uid_match.group(1).strip()}"
        return ""

    @classmethod
    def is_ignored_key(cls, value: Any) -> bool:
        raw = str(value or "").strip().lower()
        key = cls.normalize_key(value)
        return raw == "0" or key in {"UID 0", "buddy-0"}

    @staticmethod
    def person_aliases(key: str) -> set[str]:
        if key.startswith("buddy-"):
            suffix = key.removeprefix("buddy-").strip()
            return {f"UID {suffix}"} if suffix else set()
        uid_match = re.match(r"^UID\s+(.+)$", key, re.IGNORECASE)
        if uid_match and uid_match.group(1).strip():
            return {f"buddy-{uid_match.group(1).strip()}"}
        return set()

    @classmethod
    def equivalent_keys(cls, value: Any) -> set[str]:
        key = cls.normalize_key(value)
        return {key, *cls.person_aliases(key)} if key else set()

    @classmethod
    def canonical_display_key(cls, value: Any) -> str:
        key = cls.normalize_key(value)
        if key.startswith("buddy-"):
            suffix = key.removeprefix("buddy-").strip()
            return f"UID {suffix}" if suffix else key
        return key

    @classmethod
    def normalize_mappings(cls, value: Any) -> dict[str, str]:
        if not isinstance(value, dict):
            return {}
        mappings: dict[str, str] = {}
        for raw_key, raw_name in value.items():
            key = cls.normalize_key(raw_key)
            name = " ".join(str(raw_name or "").split())
            if key and name and not cls.is_ignored_key(key):
                mappings[key] = name[:180]
                for alias in cls.person_aliases(key):
                    mappings[alias] = name[:180]
        return mappings

    @classmethod
    def missing_mappings(cls, current: Any, candidates: Any) -> dict[str, str]:
        normalized_current = cls.normalize_mappings(current)
        normalized_candidates = cls.normalize_mappings(candidates)
        existing_keys = {
            alias
            for key in normalized_current
            for alias in cls.equivalent_keys(key)
        }
        return {
            key: name
            for key, name in normalized_candidates.items()
            if not (cls.equivalent_keys(key) & existing_keys)
        }

    def _load(self) -> dict[str, Any]:
        if self.storage_path is None or not self.storage_path.exists():
            return {"mappings": {}}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"mappings": {}}
        if not isinstance(payload, dict):
            return {"mappings": {}}
        payload["mappings"] = self.normalize_mappings(payload.get("mappings") if "mappings" in payload else payload)
        return payload

    def _persist_locked(self) -> None:
        if self.storage_path is None:
            return
        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {**self._payload, "updated_at": time.time()}
            temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.storage_path)
        except OSError:
            return

    def mappings(self) -> dict[str, str]:
        with self._lock:
            return dict(self._payload.get("mappings") or {})

    def replace_mappings(self, mappings: dict[str, str]) -> dict[str, str]:
        normalized = self.normalize_mappings(mappings)
        with self._lock:
            self._payload["mappings"] = normalized
            self._persist_locked()
            return dict(normalized)

    def merge_mappings(self, mappings: dict[str, str]) -> dict[str, str]:
        normalized = self.normalize_mappings(mappings)
        with self._lock:
            current = dict(self._payload.get("mappings") or {})
            current.update(normalized)
            self._payload["mappings"] = current
            self._persist_locked()
            return dict(current)
