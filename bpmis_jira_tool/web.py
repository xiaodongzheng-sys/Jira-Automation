from __future__ import annotations

from dataclasses import asdict, dataclass, field
import difflib
from functools import lru_cache
import hashlib
import html
import inspect
import io
from http import HTTPStatus
import json
import logging
import os
from pathlib import Path
import re
import secrets
import subprocess
import threading
import time
from typing import Any
import uuid

from flask import Flask, current_app, flash, g, has_app_context, jsonify, redirect, render_template, request, send_file, session, url_for
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from openpyxl import Workbook
from openpyxl.styles import Font
import requests

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.google_auth import (
    create_google_authorization_url,
    finish_google_oauth,
    get_google_credentials,
)
from bpmis_jira_tool.local_agent_client import (
    LocalAgentClient,
    RemoteBPMISProjectStore,
    RemoteSeaTalkDashboardService,
    RemoteSeaTalkNameMappingStore,
    RemoteSeaTalkTodoStore,
    RemoteSourceCodeQAModelAvailabilityStore,
    RemoteSourceCodeQASessionStore,
    RemoteSourceCodeQAService,
    _LOCAL_AGENT_SESSION,
)
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE, GmailDashboardService
from bpmis_jira_tool.gmail_sender import StoredGoogleCredentials
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService
from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.bpmis_projects import BPMISProjectStore, PortalJiraCreationService, PortalProjectSyncService
from bpmis_jira_tool.project_sync import BPMISProjectSyncService
from bpmis_jira_tool.service import JiraCreationService, build_bpmis_client
from bpmis_jira_tool.source_code_qa import CRMS_COUNTRIES, ALL_COUNTRY, CodexCliBridgeSourceCodeQALLMProvider, SourceCodeQAService
from bpmis_jira_tool.user_config import (
    CONFIGURED_FIELDS,
    DEFAULT_DIRECT_VALUES,
    DEFAULT_NEED_UAT_BY_MARKET,
    DEFAULT_SHEET_HEADERS,
    TEAM_DEFAULT_EMAIL_PLACEHOLDER,
    TEAM_PROFILE_DEFAULTS,
    WebConfigStore,
)
from prd_briefing import create_prd_briefing_blueprint
from prd_briefing.storage import BriefingStore


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
MARKET_KEYS = ["ID", "SG", "PH", "Regional"]
TEAM_PROFILE_ADMIN_EMAIL = "xiaodong.zheng@npt.sg"
SYNC_EMAIL_EDIT_ALLOWLIST = {"xiaodong.zheng@npt.sg", "xiaodong.zheng1991@gmail.com"}
SOURCE_CODE_QA_BUILTIN_ADMIN_EMAILS = {"xiaodong.zheng@npt.sg"}
GMAIL_SEATALK_BUILTIN_OWNER_EMAILS = {"xiaodong.zheng@npt.sg"}
_gmail_export_active_users: set[str] = set()
_gmail_export_active_users_lock = threading.Lock()
_source_code_qa_codex_session_locks: dict[str, threading.Lock] = {}
_source_code_qa_codex_session_locks_guard = threading.Lock()


@lru_cache(maxsize=1)
def _current_release_revision() -> str:
    pinned_revision = str(os.environ.get("TEAM_PORTAL_RELEASE_REVISION") or "").strip()
    if pinned_revision:
        return pinned_revision
    try:
        completed = subprocess.run(
            ["git", "-C", str(PROJECT_ROOT), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return "unknown"
    revision = completed.stdout.strip()
    return revision or "unknown"


@dataclass
class JobState:
    job_id: str
    action: str
    state: str = "queued"
    title: str = ""
    message: str = ""
    stage: str = "queued"
    current: int = 0
    total: int = 0
    results: list[dict[str, Any]] = field(default_factory=list)
    notice: dict[str, Any] | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class JobStore:
    def __init__(self, storage_path: Path | None = None) -> None:
        self.storage_path = storage_path
        self._loaded_jobs_interrupted = False
        self._jobs: dict[str, JobState] = self._load(mark_interrupted=True)
        self._lock = threading.Lock()
        if self._loaded_jobs_interrupted:
            with self._lock:
                self._persist_locked()

    def _load(self, *, mark_interrupted: bool = False) -> dict[str, JobState]:
        if self.storage_path is None or not self.storage_path.exists():
            return {}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        jobs: dict[str, JobState] = {}
        for job_id, raw_job in (payload.get("jobs") or {}).items():
            if not isinstance(raw_job, dict):
                continue
            try:
                job = JobState(**raw_job)
            except TypeError:
                continue
            if mark_interrupted and job.state in {"queued", "running"}:
                job.state = "failed"
                job.stage = "failed"
                job.message = "This job was interrupted by a server restart. Please start it again."
                job.error = job.message
                job.updated_at = time.time()
                self._loaded_jobs_interrupted = True
            jobs[str(job_id)] = job
        return jobs

    def _refresh_locked(self) -> None:
        if self.storage_path is None:
            return
        loaded_jobs = self._load(mark_interrupted=False)
        for job_id, loaded_job in loaded_jobs.items():
            current_job = self._jobs.get(job_id)
            if current_job is None or loaded_job.updated_at >= current_job.updated_at:
                self._jobs[job_id] = loaded_job

    def _persist_locked(self) -> None:
        if self.storage_path is None:
            return
        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": time.time(),
                "jobs": {job_id: asdict(job) for job_id, job in self._jobs.items()},
            }
            temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.storage_path)
        except OSError:
            return

    def create(self, action: str, title: str) -> JobState:
        with self._lock:
            job = JobState(
                job_id=uuid.uuid4().hex,
                action=action,
                title=title,
                message="Queued and waiting to start.",
            )
            self._jobs[job.job_id] = job
            self._persist_locked()
            return job

    def update(
        self,
        job_id: str,
        *,
        state: str | None = None,
        stage: str | None = None,
        message: str | None = None,
        current: int | None = None,
        total: int | None = None,
    ) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            if state is not None:
                job.state = state
            if stage is not None:
                job.stage = stage
            if message is not None:
                job.message = message
            if current is not None:
                job.current = current
            if total is not None:
                job.total = total
            job.updated_at = time.time()
            self._persist_locked()

    def complete(self, job_id: str, *, results: list[dict[str, Any]], notice: dict[str, Any]) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            job.state = "completed"
            job.stage = "completed"
            job.message = "Finished."
            job.results = results
            job.notice = notice
            job.updated_at = time.time()
            self._persist_locked()

    def fail(self, job_id: str, error: str) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            job.state = "failed"
            job.stage = "failed"
            job.message = error
            job.error = error
            job.updated_at = time.time()
            self._persist_locked()

    def get(self, job_id: str) -> JobState | None:
        with self._lock:
            self._refresh_locked()
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return JobState(**asdict(job))

    def snapshot(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        return asdict(job) if job else None


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
    def normalize_mappings(cls, value: Any) -> dict[str, str]:
        if not isinstance(value, dict):
            return {}
        mappings: dict[str, str] = {}
        for raw_key, raw_name in value.items():
            key = cls.normalize_key(raw_key)
            name = " ".join(str(raw_name or "").split())
            if key and name:
                mappings[key] = name[:180]
                for alias in cls.person_aliases(key):
                    mappings[alias] = name[:180]
        return mappings

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


class SourceCodeQASessionStore:
    def __init__(self, storage_path: Path | None = None) -> None:
        self.storage_path = storage_path
        self._sessions: dict[str, dict[str, Any]] = self._load()
        self._lock = threading.Lock()

    def _load(self) -> dict[str, dict[str, Any]]:
        if self.storage_path is None or not self.storage_path.exists():
            return {}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        sessions: dict[str, dict[str, Any]] = {}
        for session_id, raw_session in (payload.get("sessions") or {}).items():
            if isinstance(raw_session, dict):
                sessions[str(session_id)] = raw_session
        return sessions

    def _persist_locked(self) -> None:
        if self.storage_path is None:
            return
        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": time.time(),
                "sessions": self._sessions,
            }
            temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.storage_path)
        except OSError:
            return

    @staticmethod
    def _now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    @staticmethod
    def _title_from_question(question: str) -> str:
        normalized = re.sub(r"\s+", " ", str(question or "").strip())
        if not normalized:
            return "New Source Code Chat"
        return normalized[:72] + ("..." if len(normalized) > 72 else "")

    def create(
        self,
        *,
        owner_email: str,
        pm_team: str,
        country: str,
        llm_provider: str,
        title: str = "",
    ) -> dict[str, Any]:
        now = self._now()
        session_payload = {
            "id": uuid.uuid4().hex,
            "owner_email": str(owner_email or "").strip().lower(),
            "title": str(title or "").strip() or "New Source Code Chat",
            "pm_team": str(pm_team or "").strip() or "AF",
            "country": str(country or "").strip() or ALL_COUNTRY,
            "llm_provider": SourceCodeQAService.normalize_query_llm_provider(llm_provider),
            "created_at": now,
            "updated_at": now,
            "messages": [],
            "last_context": None,
            "last_trace_id": "",
            "archived_at": "",
            "archived_by": "",
        }
        with self._lock:
            self._sessions[session_payload["id"]] = session_payload
            self._persist_locked()
            return dict(session_payload)

    def list(self, *, owner_email: str, limit: int = 30) -> list[dict[str, Any]]:
        owner = str(owner_email or "").strip().lower()
        with self._lock:
            sessions = [
                self._public_session(session_payload, include_messages=False)
                for session_payload in self._sessions.values()
                if str(session_payload.get("owner_email") or "").strip().lower() == owner
                and not str(session_payload.get("archived_at") or "").strip()
            ]
        sessions.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return sessions[: max(1, min(int(limit or 30), 100))]

    def get(self, session_id: str, *, owner_email: str) -> dict[str, Any] | None:
        owner = str(owner_email or "").strip().lower()
        with self._lock:
            session_payload = self._sessions.get(str(session_id or ""))
            if not session_payload:
                return None
            if str(session_payload.get("owner_email") or "").strip().lower() != owner:
                return None
            return self._public_session(session_payload, include_messages=True)

    def archive(self, session_id: str, *, owner_email: str) -> dict[str, Any] | None:
        owner = str(owner_email or "").strip().lower()
        now = self._now()
        with self._lock:
            session_payload = self._sessions.get(str(session_id or ""))
            if not session_payload:
                return None
            if str(session_payload.get("owner_email") or "").strip().lower() != owner:
                return None
            session_payload["archived_at"] = now
            session_payload["archived_by"] = owner
            session_payload["updated_at"] = now
            self._persist_locked()
            return {
                "status": "ok",
                "session_id": session_payload.get("id") or "",
                "archived_at": now,
            }

    def get_context(self, session_id: str, *, owner_email: str) -> dict[str, Any] | None:
        session_payload = self.get(session_id, owner_email=owner_email)
        context = session_payload.get("last_context") if session_payload else None
        return context if isinstance(context, dict) else None

    @staticmethod
    def _recent_turn_from_context(context: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(context, dict):
            return None
        question = str(context.get("question") or "").strip()
        answer = str(context.get("answer") or context.get("rendered_answer") or "").strip()
        if not question and not answer:
            return None
        llm_route = context.get("llm_route") if isinstance(context.get("llm_route"), dict) else {}
        return {
            "question": question[:500],
            "answer": answer[:1200],
            "summary": str(context.get("summary") or "")[:500],
            "trace_id": str(context.get("trace_id") or "")[:80],
            "llm_provider": str(context.get("llm_provider") or "")[:80],
            "llm_model": str(context.get("llm_model") or "")[:120],
            "matches_snapshot": [
                item for item in (context.get("matches_snapshot") or context.get("matches") or [])[:8]
                if isinstance(item, dict)
            ],
            "codex_candidate_paths": [
                item for item in (
                    context.get("codex_candidate_paths")
                    or (llm_route.get("candidate_paths") if isinstance(llm_route, dict) else [])
                    or []
                )[:12]
                if isinstance(item, dict)
            ],
            "evidence_pack": (
                context.get("evidence_pack")
                if isinstance(context.get("evidence_pack"), dict)
                else {}
            ),
        }

    @classmethod
    def _extend_recent_turns(cls, context: dict[str, Any], previous_context: dict[str, Any] | None) -> dict[str, Any]:
        enriched = dict(context or {})
        recent_turns = [
            item for item in (
                previous_context.get("recent_turns", []) if isinstance(previous_context, dict) else []
            )
            if isinstance(item, dict)
        ]
        previous_turn = cls._recent_turn_from_context(previous_context)
        if previous_turn:
            recent_turns.append(previous_turn)
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for item in recent_turns:
            key = (str(item.get("question") or ""), str(item.get("trace_id") or ""))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        try:
            max_turns = int(enriched.get("codex_session_max_turns") or 8)
        except (TypeError, ValueError):
            max_turns = 8
        enriched["recent_turns"] = deduped[-max(1, min(max_turns, 30)):]
        return enriched

    def append_exchange(
        self,
        session_id: str,
        *,
        owner_email: str,
        pm_team: str,
        country: str,
        llm_provider: str,
        question: str,
        result: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any] | None:
        owner = str(owner_email or "").strip().lower()
        now = self._now()
        with self._lock:
            session_payload = self._sessions.get(str(session_id or ""))
            if not session_payload:
                return None
            if str(session_payload.get("owner_email") or "").strip().lower() != owner:
                return None
            title = str(session_payload.get("title") or "").strip()
            if not title or title == "New Source Code Chat":
                session_payload["title"] = self._title_from_question(question)
            previous_context = session_payload.get("last_context") if isinstance(session_payload.get("last_context"), dict) else None
            previous_scope = (
                str(session_payload.get("pm_team") or ""),
                str(session_payload.get("country") or ""),
                str(session_payload.get("llm_provider") or ""),
            )
            current_scope = (
                str(pm_team or "").strip() or str(session_payload.get("pm_team") or ""),
                str(country or "").strip() or str(session_payload.get("country") or ALL_COUNTRY),
                SourceCodeQAService.normalize_query_llm_provider(llm_provider),
            )
            session_payload["pm_team"] = current_scope[0] or "AF"
            session_payload["country"] = current_scope[1] or ALL_COUNTRY
            session_payload["llm_provider"] = current_scope[2]
            session_payload["updated_at"] = now
            next_context = self._extend_recent_turns(context, previous_context)
            if previous_scope != current_scope:
                next_context.pop("codex_cli_session", None)
            session_payload["last_context"] = next_context
            session_payload["last_trace_id"] = str(result.get("trace_id") or "")
            messages = list(session_payload.get("messages") or [])
            messages.extend(
                [
                    {
                        "role": "user",
                        "text": str(question or ""),
                        "created_at": now,
                    },
                    {
                        "role": "assistant",
                        "text": str(result.get("llm_answer") or result.get("summary") or ""),
                        "created_at": now,
                        "payload": _compact_source_code_qa_session_payload(result),
                    },
                ]
            )
            session_payload["messages"] = messages[-80:]
            self._persist_locked()
            return self._public_session(session_payload, include_messages=True)

    def _public_session(self, session_payload: dict[str, Any], *, include_messages: bool) -> dict[str, Any]:
        public_payload = {
            "id": session_payload.get("id") or "",
            "title": session_payload.get("title") or "New Source Code Chat",
            "pm_team": session_payload.get("pm_team") or "",
            "country": session_payload.get("country") or ALL_COUNTRY,
            "llm_provider": session_payload.get("llm_provider") or "codex_cli_bridge",
            "created_at": session_payload.get("created_at") or "",
            "updated_at": session_payload.get("updated_at") or "",
            "archived_at": session_payload.get("archived_at") or "",
            "last_context": session_payload.get("last_context") if include_messages else None,
            "last_trace_id": session_payload.get("last_trace_id") or "",
            "message_count": len(session_payload.get("messages") or []),
        }
        if include_messages:
            public_payload["messages"] = list(session_payload.get("messages") or [])
        return public_payload


class SourceCodeQAModelAvailabilityStore:
    DEFAULT_AVAILABILITY = {
        "codex_cli_bridge": True,
        "gemini": False,
        "vertex_ai": True,
    }

    def __init__(self, storage_path: Path | None = None) -> None:
        self.storage_path = storage_path
        self._lock = threading.Lock()
        self._availability = self._load()

    def _load(self) -> dict[str, bool]:
        availability = dict(self.DEFAULT_AVAILABILITY)
        if self.storage_path is None or not self.storage_path.exists():
            return availability
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return availability
        raw_availability = payload.get("availability") if isinstance(payload, dict) else {}
        if isinstance(raw_availability, dict):
            for provider in availability:
                if provider in raw_availability:
                    availability[provider] = bool(raw_availability[provider])
        return availability

    def _persist_locked(self) -> None:
        if self.storage_path is None:
            return
        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": time.time(),
                "availability": self._availability,
            }
            temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.storage_path)
        except OSError:
            return

    def get(self) -> dict[str, bool]:
        with self._lock:
            return dict(self._availability)

    def save(self, availability: dict[str, Any]) -> dict[str, bool]:
        with self._lock:
            next_availability = dict(self.DEFAULT_AVAILABILITY)
            for provider in next_availability:
                if provider in availability:
                    next_availability[provider] = bool(availability[provider])
            self._availability = next_availability
            self._persist_locked()
            return dict(self._availability)


def _safe_email_identity(user_identity: dict[str, str | None] | None = None) -> str:
    if not user_identity:
        return ""
    return str(user_identity.get("email") or user_identity.get("config_key") or "").strip().lower()


def _try_acquire_gmail_export_lock(email: str) -> bool:
    normalized = str(email or "").strip().lower()
    if not normalized:
        return False
    with _gmail_export_active_users_lock:
        if normalized in _gmail_export_active_users:
            return False
        _gmail_export_active_users.add(normalized)
        return True


def _release_gmail_export_lock(email: str) -> None:
    normalized = str(email or "").strip().lower()
    if not normalized:
        return
    with _gmail_export_active_users_lock:
        _gmail_export_active_users.discard(normalized)


def _source_code_qa_codex_session_lock(session_id: str) -> threading.Lock:
    normalized = str(session_id or "").strip()
    if not normalized:
        normalized = "_no_session"
    with _source_code_qa_codex_session_locks_guard:
        lock = _source_code_qa_codex_session_locks.get(normalized)
        if lock is None:
            lock = threading.Lock()
            _source_code_qa_codex_session_locks[normalized] = lock
        return lock


def _count_configured_lines(value: Any) -> int:
    return sum(1 for line in str(value or "").splitlines() if line.strip() and not line.strip().startswith("#"))


def _count_routed_components(value: Any) -> int:
    components = {
        parts[2].strip().lower()
        for raw_line in str(value or "").splitlines()
        if (line := raw_line.strip()) and not line.startswith("#")
        if len(parts := [part.strip() for part in line.split("|")]) == 3 and parts[2].strip()
    }
    return len(components)


def _build_mapping_log_summary(config_data: dict[str, Any], *, save_mode: str = "") -> dict[str, Any]:
    return {
        "save_mode": save_mode or "full",
        "pm_team": str(config_data.get("pm_team", "") or "").strip().upper(),
        "system_header": str(config_data.get("system_header", "") or "").strip(),
        "market_header": str(config_data.get("market_header", "") or "").strip(),
        "input_tab_name": str(config_data.get("input_tab_name", "") or "").strip(),
        "has_spreadsheet_link": bool(str(config_data.get("spreadsheet_link", "") or "").strip()),
        "has_bpmis_token": bool(str(config_data.get("bpmis_api_access_token", "") or "").strip()),
        "route_rule_count": _count_configured_lines(config_data.get("component_route_rules_text", "")),
        "route_component_count": _count_routed_components(config_data.get("component_route_rules_text", "")),
        "default_rule_count": _count_configured_lines(config_data.get("component_default_rules_text", "")),
    }


def _build_request_log_context(
    settings: Settings | None = None,
    *,
    user_identity: dict[str, str | None] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "request_id": getattr(g, "request_id", None),
        "method": request.method,
        "path": request.path,
        "endpoint": request.endpoint,
        "request_origin": "api" if request.path.startswith("/api/") else "page",
        "remote_addr": request.headers.get("X-Forwarded-For", request.remote_addr),
        "user_agent": request.headers.get("User-Agent", ""),
        "user": _safe_email_identity(user_identity or (settings and _get_user_identity(settings))),
        "google_connected": _google_session_is_connected(),
    }
    if extra:
        context.update(extra)
    return context


def _classify_portal_error(error: Exception | str | None) -> dict[str, Any]:
    message = str(error or "").strip()
    normalized = message.lower()
    exception_type = type(error).__name__ if isinstance(error, BaseException) else ""

    details = {
        "error_message": message,
        "error_type": exception_type or "UnknownError",
        "error_category": "unexpected_internal",
        "error_code": "unexpected_error",
        "error_retryable": False,
    }

    if not message and not exception_type:
        return details

    if isinstance(error, ConfigError):
        details.update(
            {
                "error_category": "host_configuration",
                "error_code": "oauth_host_misconfigured",
                "error_retryable": False,
            }
        )
        return details

    if isinstance(error, ToolError):
        details["error_category"] = "tool_error"
        details["error_code"] = "tool_error"
        if "sign in with your npt google account" in normalized:
            details.update({"error_category": "authentication", "error_code": "auth_required", "error_retryable": True})
        elif "not authorized for the team portal" in normalized:
            details.update({"error_category": "authorization", "error_code": "account_not_allowed", "error_retryable": False})
        elif "team_portal_config_encryption_key" in normalized or "could not decrypt the saved bpmis token" in normalized:
            details.update({"error_category": "host_configuration", "error_code": "portal_encryption_config", "error_retryable": False})
        elif "duplicate system + market -> component rule" in normalized:
            details.update({"error_category": "config_validation", "error_code": "duplicate_route_rule", "error_retryable": False})
        elif "invalid system + market -> component rule" in normalized:
            details.update({"error_category": "config_validation", "error_code": "invalid_route_rule_format", "error_retryable": False})
        elif "invalid component default rule" in normalized:
            details.update({"error_category": "config_validation", "error_code": "invalid_component_default_rule", "error_retryable": False})
        elif "duplicate component default rule" in normalized:
            details.update({"error_category": "config_validation", "error_code": "duplicate_component_default_rule", "error_retryable": False})
        elif "component defaults are missing these routed components" in normalized:
            details.update({"error_category": "config_validation", "error_code": "missing_component_defaults", "error_retryable": False})
        elif "pm team is required" in normalized or "unsupported pm team" in normalized:
            details.update({"error_category": "config_validation", "error_code": "invalid_pm_team", "error_retryable": False})
        elif "version keyword is required" in normalized or "version_id is required" in normalized:
            details.update({"error_category": "request_validation", "error_code": "missing_required_parameter", "error_retryable": False})
        elif "google sheets" in normalized or "spreadsheet" in normalized or "sheet" in normalized:
            details.update({"error_category": "google_sheets", "error_code": "google_sheet_access", "error_retryable": True})
        elif "bpmis" in normalized:
            details.update({"error_category": "bpmis_upstream", "error_code": "bpmis_request_failed", "error_retryable": True})
        return details

    if "connection" in normalized or "timeout" in normalized or "temporar" in normalized:
        details["error_retryable"] = True
    return details


def _classify_http_status(status_code: int) -> dict[str, str]:
    if status_code == HTTPStatus.BAD_REQUEST:
        return {"error_category": "request_validation", "error_code": "bad_request"}
    if status_code == HTTPStatus.UNAUTHORIZED:
        return {"error_category": "authentication", "error_code": "auth_required"}
    if status_code == HTTPStatus.FORBIDDEN:
        return {"error_category": "authorization", "error_code": "forbidden"}
    if status_code == HTTPStatus.NOT_FOUND:
        return {"error_category": "routing", "error_code": "not_found"}
    if status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
        return {"error_category": "unexpected_internal", "error_code": "server_error"}
    return {"error_category": "http_error", "error_code": f"http_{status_code}"}


def _log_portal_event(
    event: str,
    *,
    level: int = logging.INFO,
    logger: logging.Logger | None = None,
    **fields: Any,
) -> None:
    active_logger = logger or current_app.logger
    payload = {"event": event, **fields}
    active_logger.log(level, "portal_event %s", json.dumps(payload, ensure_ascii=False, sort_keys=True))


def create_app() -> Flask:
    settings = Settings.from_env()
    package_dir = Path(__file__).resolve().parent
    project_root = package_dir.parent
    data_root = settings.team_portal_data_dir
    if not data_root.is_absolute():
        data_root = (project_root / data_root).resolve()
    config_store = WebConfigStore(
        data_root,
        legacy_root=project_root,
        encryption_key=settings.team_portal_config_encryption_key,
    )
    app = Flask(
        __name__,
        template_folder=str(project_root / "templates"),
        static_folder=str(project_root / "static"),
    )
    app.config["SECRET_KEY"] = settings.flask_secret_key
    app.config["SETTINGS"] = settings
    app.config["CONFIG_STORE"] = config_store
    app.config["BPMIS_PROJECT_STORE"] = BPMISProjectStore(config_store.db_path)
    app.config["JOB_STORE"] = JobStore(data_root / "run" / "jobs.json")
    app.config["SEATALK_TODO_STORE"] = SeaTalkTodoStore(data_root / "seatalk" / "completed_todos.json")
    app.config["SEATALK_NAME_MAPPING_STORE"] = SeaTalkNameMappingStore(data_root / "seatalk" / "name_overrides.json")
    app.config["SEATALK_DAILY_CACHE_DIR"] = data_root / "seatalk" / "cache"
    app.config["GOOGLE_CREDENTIAL_STORE"] = StoredGoogleCredentials(
        data_root / "google" / "credentials.json",
        encryption_key=settings.team_portal_config_encryption_key,
    )
    app.config["SOURCE_CODE_QA_SESSION_STORE"] = SourceCodeQASessionStore(data_root / "source_code_qa" / "sessions.json")
    app.config["SOURCE_CODE_QA_MODEL_AVAILABILITY_STORE"] = SourceCodeQAModelAvailabilityStore(
        data_root / "source_code_qa" / "model_availability.json"
    )
    app.config["PRD_BRIEFING_STORE"] = BriefingStore(data_root / "prd_briefing")
    app.config["SOURCE_CODE_QA_SERVICE"] = SourceCodeQAService(
        data_root=data_root,
        team_profiles=TEAM_PROFILE_DEFAULTS,
        gitlab_token=settings.source_code_qa_gitlab_token,
        gitlab_username=settings.source_code_qa_gitlab_username,
        llm_provider=settings.source_code_qa_llm_provider,
        gemini_api_key=settings.source_code_qa_gemini_api_key,
        gemini_api_base_url=settings.source_code_qa_gemini_api_base_url,
        openai_api_key=settings.source_code_qa_openai_api_key,
        openai_api_base_url=settings.source_code_qa_openai_api_base_url,
        openai_model=settings.source_code_qa_openai_model,
        openai_fast_model=settings.source_code_qa_openai_fast_model,
        openai_deep_model=settings.source_code_qa_openai_deep_model,
        openai_fallback_model=settings.source_code_qa_openai_fallback_model,
        gemini_model=settings.source_code_qa_gemini_model,
        gemini_fast_model=settings.source_code_qa_gemini_fast_model,
        gemini_deep_model=settings.source_code_qa_gemini_deep_model,
        gemini_fallback_model=settings.source_code_qa_gemini_fallback_model,
        vertex_credentials_file=settings.source_code_qa_vertex_credentials_file,
        vertex_project_id=settings.source_code_qa_vertex_project_id,
        vertex_location=settings.source_code_qa_vertex_location,
        vertex_model=settings.source_code_qa_vertex_model,
        vertex_fast_model=settings.source_code_qa_vertex_fast_model,
        vertex_deep_model=settings.source_code_qa_vertex_deep_model,
        vertex_fallback_model=settings.source_code_qa_vertex_fallback_model,
        query_rewrite_model=settings.source_code_qa_query_rewrite_model,
        planner_model=settings.source_code_qa_planner_model,
        answer_model=settings.source_code_qa_answer_model,
        judge_model=settings.source_code_qa_judge_model,
        repair_model=settings.source_code_qa_repair_model,
        llm_judge_enabled=settings.source_code_qa_llm_judge_enabled,
        semantic_index_model=settings.source_code_qa_embedding_model,
        semantic_index_enabled=settings.source_code_qa_semantic_index_enabled,
        embedding_provider=settings.source_code_qa_embedding_provider,
        embedding_api_key=settings.source_code_qa_embedding_api_key,
        embedding_api_base_url=settings.source_code_qa_embedding_api_base_url,
        llm_cache_ttl_seconds=settings.source_code_qa_llm_cache_ttl_seconds,
        llm_timeout_seconds=settings.source_code_qa_llm_timeout_seconds,
        codex_timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        codex_concurrency=settings.source_code_qa_codex_concurrency,
        codex_top_path_limit=settings.source_code_qa_codex_top_path_limit,
        codex_repair_enabled=settings.source_code_qa_codex_repair_enabled,
        codex_session_mode=settings.source_code_qa_codex_session_mode,
        codex_session_max_turns=settings.source_code_qa_codex_session_max_turns,
        codex_fast_path_enabled=settings.source_code_qa_codex_fast_path_enabled,
        codex_cache_followups=settings.source_code_qa_codex_cache_followups,
        llm_max_retries=settings.source_code_qa_llm_max_retries,
        llm_backoff_seconds=settings.source_code_qa_llm_backoff_seconds,
        llm_max_backoff_seconds=settings.source_code_qa_llm_max_backoff_seconds,
        git_timeout_seconds=settings.source_code_qa_git_timeout_seconds,
        max_file_bytes=settings.source_code_qa_max_file_bytes,
    )
    app.config["GET_USER_IDENTITY"] = lambda: _get_user_identity(settings)
    app.config["CAN_ACCESS_PRD_BRIEFING"] = lambda: _can_access_prd_briefing(settings)
    app.config["CAN_ACCESS_GMAIL_SEATALK_DEMO"] = lambda: _can_access_gmail_seatalk_demo(settings)
    app.register_blueprint(create_prd_briefing_blueprint())

    @app.context_processor
    def inject_primary_navigation():
        current_endpoint = request.endpoint or ""
        if _site_requires_google_login(settings) and not _google_session_is_connected():
            return {
                "site_tabs": [],
                "site_requires_google_login": True,
                "can_access_prd_briefing": False,
                "can_access_gmail_seatalk_demo": False,
                "can_access_source_code_qa": False,
                "can_manage_source_code_qa": False,
            }
        site_tabs = [
            {
                "label": "BPMIS Automation Tool",
                "href": url_for("index"),
                "active": current_endpoint == "index",
            }
        ]
        if _can_access_source_code_qa(settings):
            site_tabs.append(
                {
                    "label": "Source Code Q&A",
                    "href": url_for("source_code_qa"),
                    "active": request.path.startswith("/source-code-qa"),
                }
            )
        prd_tab = None
        if _can_access_prd_briefing(settings):
            prd_tab = {
                "label": "PRD Briefing Tool",
                "href": url_for("prd_briefing.portal"),
                "active": current_endpoint.startswith("prd_briefing"),
            }
        if _can_access_gmail_seatalk_demo(settings):
            site_tabs.append(
                {
                    "label": "SeaTalk Summary",
                    "href": url_for("gmail_seatalk_demo"),
                    "active": request.path.startswith("/gmail-sea-talk-demo"),
                }
            )
        if prd_tab:
            site_tabs.append(prd_tab)
        return {
            "site_tabs": site_tabs,
            "site_requires_google_login": _site_requires_google_login(settings),
            "can_access_prd_briefing": _can_access_prd_briefing(settings),
            "can_access_gmail_seatalk_demo": _can_access_gmail_seatalk_demo(settings),
            "can_access_source_code_qa": _can_access_source_code_qa(settings),
            "can_manage_source_code_qa": _can_manage_source_code_qa(settings),
            "asset_revision": _current_release_revision(),
        }

    @app.before_request
    def enforce_team_access():
        g.request_id = uuid.uuid4().hex[:12]
        if request.path.rstrip("/") == "/healthz":
            return jsonify({"status": "ok", "revision": _current_release_revision()}), HTTPStatus.OK
        if request.path.startswith("/api/local-agent/"):
            return None
        if request.endpoint in {
            None,
            "static",
            "index",
            "healthz",
            "google_login",
            "google_callback",
            "google_logout",
            "access_denied",
        }:
            return None
        login_gate = _require_google_login(
            settings,
            api=(request.path.startswith("/api/") or "/api/" in request.path),
        )
        if login_gate is not None:
            return login_gate
        if _current_google_user_is_blocked(settings):
            session.pop("google_credentials", None)
            session.pop("google_profile", None)
            message = "This Google account is not authorized for the team portal. Please contact the maintainer."
            if request.path.startswith("/api/") or "/api/" in request.path:
                return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
            flash(message, "error")
            return redirect(url_for("access_denied"))
        return None

    @app.after_request
    def prevent_authenticated_html_caching(response):
        request_id = getattr(g, "request_id", None)
        if request_id:
            response.headers["X-Request-ID"] = request_id
        if (
            _google_session_is_connected()
            and response.mimetype == "text/html"
            and request.method == "GET"
        ):
            response.headers["Cache-Control"] = "no-store, private, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        if response.status_code >= HTTPStatus.BAD_REQUEST:
            status_details = _classify_http_status(response.status_code)
            _log_portal_event(
                "http_response_error",
                level=logging.WARNING if response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR else logging.ERROR,
                **_build_request_log_context(
                    settings,
                    extra={
                        "status_code": response.status_code,
                        **status_details,
                    },
                ),
            )
        return response

    @app.get("/")
    def index():
        if _current_google_user_is_blocked(settings):
            session.pop("google_credentials", None)
            session.pop("google_profile", None)
            flash("This Google account is not authorized for the team portal. Please contact the maintainer.", "error")
            return redirect(url_for("access_denied"))

        if _site_requires_google_login(settings) and not _google_session_is_connected():
            return render_template("login_gate.html", page_title="Sign In")

        results = _results_for_display(session.pop("last_results", []))
        run_notice = session.pop("run_notice", None)
        user_identity = _get_user_identity(settings)
        config_key = user_identity.get("config_key")
        raw_config_data = _load_user_config_for_identity(settings, user_identity) if config_key else None
        effective_team_profiles = _load_effective_team_profiles(config_store)
        config_data = raw_config_data or config_store._normalize({})
        config_data = _hydrate_setup_defaults(config_data, user_identity, team_profiles=effective_team_profiles)
        _apply_sync_email_policy(config_data, user_identity)
        input_headers: list[str] = []
        has_saved_config = bool(config_key and raw_config_data)

        if (
            "google_credentials" in session
            and _has_explicit_spreadsheet_link(raw_config_data)
            and _google_session_can_call_live_google_apis()
        ):
            try:
                sheets = _build_sheets_service(settings, config_data)
                snapshot = sheets.read_snapshot()
                input_headers = snapshot.headers
            except ToolError as error:
                error_details = _classify_portal_error(error)
                _log_portal_event(
                    "index_sheet_snapshot_tool_error",
                    level=logging.WARNING,
                    **_build_request_log_context(
                        settings,
                        user_identity=user_identity,
                        extra=error_details,
                    ),
                )
            except Exception:
                _log_portal_event(
                    "index_sheet_snapshot_unexpected_error",
                    level=logging.WARNING,
                    **_build_request_log_context(settings, user_identity=user_identity),
                )
                current_app.logger.warning("Google Sheets snapshot read failed on index.", exc_info=True)

        return render_template(
            "index.html",
            settings=settings,
            shared_portal_enabled=_shared_portal_enabled(settings),
            google_connected="google_credentials" in session,
            user_identity=user_identity,
            sync_email_editable=_can_edit_sync_email(user_identity),
            results=results,
            run_notice=run_notice,
            mapping_fields=CONFIGURED_FIELDS,
            mapping_config=config_data,
            team_profiles=_build_team_profiles_for_display(
                config_data,
                user_identity,
                team_profiles=effective_team_profiles,
            ),
            team_profile_admin_configs=effective_team_profiles,
            team_profile_admin_enabled=_is_team_profile_admin(user_identity),
            default_workspace_tab=session.pop("default_workspace_tab", "run" if has_saved_config else "setup"),
            input_headers=input_headers,
            google_authorized=True,
        )

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok", "revision": _current_release_revision()}), HTTPStatus.OK

    @app.get("/access-denied")
    def access_denied():
        return render_template("access_denied.html", page_title="Access Restricted"), HTTPStatus.FORBIDDEN

    @app.get("/source-code-qa")
    def source_code_qa():
        access_gate = _require_source_code_qa_access(settings)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        service = _build_source_code_qa_service()
        return render_template(
            "source_code_qa.html",
            page_title="Source Code Q&A",
            user_identity=user_identity,
            options=_source_code_qa_options_payload(service),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            country_options=list(CRMS_COUNTRIES),
            all_country=ALL_COUNTRY,
            can_manage_source_code_qa=_can_manage_source_code_qa(settings),
            asset_revision=_current_release_revision(),
        )

    @app.get("/api/source-code-qa/config")
    def source_code_qa_config_api():
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            service = _build_source_code_qa_service()
            gemini_service = _build_source_code_qa_service("gemini")
            codex_service = _build_source_code_qa_service("codex_cli_bridge")
            vertex_service = _build_source_code_qa_service("vertex_ai")
            model_availability = _source_code_qa_model_availability()
            return jsonify(
                {
                    "status": "ok",
                    "answer_mode": "auto",
                    "can_manage": _can_manage_source_code_qa(settings),
                    "auth": _source_code_qa_auth_payload(settings),
                    "git_auth_ready": _source_code_qa_git_auth_ready(service, settings),
                    "llm_ready": service.llm_ready(),
                    "llm_provider": settings.source_code_qa_llm_provider,
                    "llm_providers": {
                        "gemini": {"ready": gemini_service.llm_ready(), "label": "Gemini", "available": model_availability["gemini"]},
                        "codex_cli_bridge": {"ready": codex_service.llm_ready(), "label": "Codex", "available": model_availability["codex_cli_bridge"]},
                        "vertex_ai": {"ready": vertex_service.llm_ready(), "label": "Vertex AI", "available": model_availability["vertex_ai"]},
                    },
                    "llm_model": service.llm_budgets["balanced"]["model"],
                    "llm_fast_model": service.llm_budgets["cheap"]["model"],
                    "llm_deep_model": service.llm_budgets["deep"]["model"],
                    "llm_fallback_model": service._llm_fallback_model(),
                    "llm_policy": service.llm_policy_payload(),
                    "index_health": service.index_health_payload(),
                    "release_gate": _source_code_qa_release_gate_payload(settings),
                    "domain_knowledge": service.domain_knowledge_payload(),
                    "model_availability": model_availability,
                    "options": _source_code_qa_options_payload(service),
                    "config": service.load_config(),
                }
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001 - keep API clients on JSON even for unexpected failures.
            request_id = getattr(g, "request_id", "")
            current_app.logger.exception("Source Code Q&A config failed unexpectedly")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Source Code Q&A config failed unexpectedly. Please refresh; if it repeats, share the request ID.",
                        "request_id": request_id,
                        "error_category": "source_code_qa_internal",
                        "error_retryable": True,
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/source-code-qa/config")
    def source_code_qa_save_config_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            result = _build_source_code_qa_service().save_mapping(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
                repositories=payload.get("repositories") or [],
            )
            return jsonify({"status": "ok", **result})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/source-code-qa/model-availability")
    def source_code_qa_model_availability_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        raw_availability = payload.get("availability") if isinstance(payload.get("availability"), dict) else {}
        store = _get_source_code_qa_model_availability_store()
        availability = store.save(raw_availability)
        return jsonify(
            {
                "status": "ok",
                "model_availability": availability,
                "options": _source_code_qa_options_payload(_build_source_code_qa_service()),
            }
        )

    @app.post("/api/source-code-qa/sync")
    def source_code_qa_sync_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        pm_team = str(payload.get("pm_team") or "")
        country = str(payload.get("country") or "")
        job_store: JobStore = current_app.config["JOB_STORE"]
        job = job_store.create("source-code-qa-sync", title="Sync Source Code Repositories")
        app_obj = current_app._get_current_object()
        thread = threading.Thread(
            target=_run_source_code_qa_sync_job,
            args=(app_obj, job.job_id, settings, pm_team, country),
            daemon=True,
        )
        thread.start()
        return jsonify({"status": "queued", "job_id": job.job_id})

    @app.route("/api/source-code-qa/sessions", methods=["GET", "POST"])
    def source_code_qa_sessions_api():
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        store = _get_source_code_qa_session_store()
        owner_email = _current_google_email() or "local"
        if request.method == "GET":
            limit = request.args.get("limit", "30")
            try:
                limit_value = int(limit)
            except ValueError:
                limit_value = 30
            return jsonify({"status": "ok", "sessions": store.list(owner_email=owner_email, limit=limit_value)})

        payload = request.get_json(silent=True) or {}
        session_payload = store.create(
            owner_email=owner_email,
            pm_team=str(payload.get("pm_team") or ""),
            country=str(payload.get("country") or ""),
            llm_provider=str(payload.get("llm_provider") or ""),
            title=str(payload.get("title") or ""),
        )
        return jsonify({"status": "ok", "session": session_payload})

    @app.get("/api/source-code-qa/sessions/<session_id>")
    def source_code_qa_session_api(session_id: str):
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        store = _get_source_code_qa_session_store()
        session_payload = store.get(session_id, owner_email=_current_google_email() or "local")
        if session_payload is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        return jsonify({"status": "ok", "session": session_payload})

    @app.post("/api/source-code-qa/sessions/<session_id>/archive")
    def source_code_qa_session_archive_api(session_id: str):
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        store = _get_source_code_qa_session_store()
        archived = store.archive(session_id, owner_email=_current_google_email() or "local")
        if archived is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        return jsonify(archived)

    @app.post("/api/source-code-qa/query")
    def source_code_qa_query_api():
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        if payload.get("async"):
            if not _source_code_qa_provider_available(payload.get("llm_provider")):
                return jsonify({"status": "error", "message": "Selected Source Code Q&A model is unavailable."}), HTTPStatus.BAD_REQUEST
            job_store: JobStore = current_app.config["JOB_STORE"]
            job = job_store.create("source-code-qa-query", title="Source Code Q&A Query")
            app_obj = current_app._get_current_object()
            async_payload = dict(payload)
            async_payload["_session_owner_email"] = _current_google_email() or "local"
            thread = threading.Thread(
                target=_run_source_code_qa_query_job,
                args=(app_obj, job.job_id, async_payload),
                daemon=True,
            )
            thread.start()
            return jsonify({"status": "queued", "job_id": job.job_id})
        try:
            if not _source_code_qa_provider_available(payload.get("llm_provider")):
                raise ToolError("Selected Source Code Q&A model is unavailable.")
            service = _build_source_code_qa_service(payload.get("llm_provider"))
            session_store = _get_source_code_qa_session_store()
            session_id = str(payload.get("session_id") or "").strip()
            owner_email = _current_google_email() or "local"
            conversation_context = payload.get("conversation_context") if isinstance(payload.get("conversation_context"), dict) else None
            if conversation_context is None and session_id:
                conversation_context = session_store.get_context(session_id, owner_email=owner_email)
            pm_team = str(payload.get("pm_team") or "")
            country = str(payload.get("country") or "")
            auto_sync = _prepare_source_code_qa_auto_sync(service, pm_team=pm_team, country=country)
            def run_query() -> dict[str, Any]:
                return service.query(
                    pm_team=pm_team,
                    country=country,
                    question=str(payload.get("question") or ""),
                    answer_mode=_source_code_qa_public_answer_mode(payload.get("answer_mode")),
                    llm_budget_mode="auto",
                    conversation_context=conversation_context,
                )

            if service.llm_provider_name == "codex_cli_bridge" and session_id:
                with _source_code_qa_codex_session_lock(session_id):
                    result = run_query()
            else:
                result = run_query()
            result["auto_sync"] = auto_sync
            if session_id:
                session_payload = session_store.append_exchange(
                    session_id,
                    owner_email=owner_email,
                    pm_team=str(payload.get("pm_team") or ""),
                    country=str(payload.get("country") or ""),
                    llm_provider=str(payload.get("llm_provider") or ""),
                    question=str(payload.get("question") or ""),
                    result=result,
                    context=_build_source_code_qa_session_context(result, payload),
                )
                if session_payload is not None:
                    result["session"] = session_payload
                    result["session_id"] = session_id
            return jsonify(result)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001 - keep API clients on JSON even for unexpected failures.
            request_id = getattr(g, "request_id", "")
            current_app.logger.exception("Source Code Q&A query failed unexpectedly")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Source Code Q&A failed unexpectedly. Please retry; if it repeats, share the request ID.",
                        "request_id": request_id,
                        "error_category": "source_code_qa_internal",
                        "error_retryable": True,
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/source-code-qa/feedback")
    def source_code_qa_feedback_api():
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            result = _build_source_code_qa_service().save_feedback(
                user_email=_current_google_email() or "",
                payload=payload,
            )
            return jsonify(result)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.get("/auth/google/login")
    def google_login():
        try:
            authorization_url = create_google_authorization_url(settings)
            return redirect(authorization_url)
        except ConfigError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "google_login_config_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, extra=error_details),
            )
            flash(str(error), "error")
            return redirect(url_for("index"))

    @app.get("/auth/google/callback")
    def google_callback():
        try:
            previous_identity = _get_user_identity(settings)
            finish_google_oauth(settings, request.url)
            if _current_google_user_is_blocked(settings):
                session.pop("google_credentials", None)
                session.pop("google_profile", None)
                flash("This Google account is not authorized for the team portal. Please contact the maintainer.", "error")
                return redirect(url_for("access_denied"))
            current_identity = _get_user_identity(settings)
            if previous_identity.get("config_key") and current_identity.get("config_key"):
                _migrate_user_config(settings, previous_identity["config_key"], current_identity["config_key"])
            _persist_owner_google_credentials(settings)
            _log_portal_event(
                "google_callback_success",
                **_build_request_log_context(settings, user_identity=current_identity),
            )
            flash("Google account connected successfully.", "success")
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "google_callback_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, extra=error_details),
            )
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.get("/gmail-sea-talk-demo")
    def gmail_seatalk_demo():
        access_gate = _require_gmail_seatalk_demo_access(settings)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        return render_template(
            "gmail_seatalk_demo.html",
            page_title="SeaTalk Summary",
            user_identity=user_identity,
            google_connected="google_credentials" in session,
            seatalk_configured=_seatalk_dashboard_is_configured(settings),
            seatalk_insights_url=url_for("gmail_seatalk_demo_seatalk_insights_api"),
            seatalk_project_updates_url=url_for("gmail_seatalk_demo_seatalk_project_updates_api"),
            seatalk_todos_url=url_for("gmail_seatalk_demo_seatalk_todos_api"),
            seatalk_open_todos_url=url_for("gmail_seatalk_demo_seatalk_open_todos_api"),
            seatalk_todo_complete_url=url_for("gmail_seatalk_demo_seatalk_todo_complete"),
            seatalk_name_mappings_url=url_for("gmail_seatalk_demo_seatalk_name_mappings"),
            asset_revision=_current_release_revision(),
        )

    @app.get("/api/gmail-sea-talk-demo/dashboard")
    def gmail_seatalk_demo_dashboard_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return jsonify(
                {
                    "status": "error",
                    "message": "Gmail access is not available for this Google session yet. Please sign in with Google again to grant Gmail read access.",
                }
            ), HTTPStatus.BAD_REQUEST
        try:
            dashboard = _build_gmail_dashboard_service().build_overview()
            return jsonify({"status": "ok", **dashboard})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_dashboard_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_dashboard_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Gmail dashboard load failed.")
            return jsonify({"status": "error", "message": "Gmail data could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    @app.get("/api/gmail-sea-talk-demo/network")
    def gmail_seatalk_demo_network_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return jsonify(
                {
                    "status": "error",
                    "message": "Gmail access is not available for this Google session yet. Please sign in with Google again to grant Gmail read access.",
                }
            ), HTTPStatus.BAD_REQUEST
        try:
            network = _build_gmail_dashboard_service().build_network()
            return jsonify({"status": "ok", **network})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_network_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_network_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Gmail network load failed.")
            return jsonify({"status": "error", "message": "Gmail network rankings could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    @app.get("/api/gmail-sea-talk-demo/gmail/export-manifest")
    def gmail_seatalk_demo_gmail_export_manifest():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return jsonify(
                {
                    "status": "error",
                    "message": "Gmail access is not available for this Google session yet. Please sign in with Google again to grant Gmail read access.",
                }
            ), HTTPStatus.BAD_REQUEST
        try:
            manifest = _build_gmail_dashboard_service().build_export_manifest()
            return jsonify({"status": "ok", **manifest})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_gmail_export_manifest_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_gmail_export_manifest_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Gmail export manifest failed.")
            return (
                jsonify({"status": "error", "message": "Gmail export batches could not be prepared right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/gmail-sea-talk-demo/gmail/export")
    def gmail_seatalk_demo_gmail_export():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return jsonify(
                {
                    "status": "error",
                    "message": "Gmail access is not available for this Google session yet. Please sign in with Google again to grant Gmail read access.",
                }
            ), HTTPStatus.BAD_REQUEST
        try:
            batch = max(int(request.args.get("batch", "1")), 1)
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid Gmail export batch. Please refresh and try again."}), HTTPStatus.BAD_REQUEST
        user_email = _safe_email_identity(_get_user_identity(settings))
        service = _build_gmail_dashboard_service()
        if not _try_acquire_gmail_export_lock(user_email):
            cached_payload = service.get_cached_export_history_text(batch=batch)
            if cached_payload is not None:
                content, filename = cached_payload
                return send_file(
                    io.BytesIO(content.encode("utf-8")),
                    mimetype="text/plain; charset=utf-8",
                    as_attachment=True,
                    download_name=filename,
                )
            return (
                jsonify({"status": "error", "message": "A Gmail export is already running for this account. Please wait a few seconds and try again."}),
                HTTPStatus.TOO_MANY_REQUESTS,
            )
        try:
            content, filename = service.export_history_text(batch=batch)
            return send_file(
                io.BytesIO(content.encode("utf-8")),
                mimetype="text/plain; charset=utf-8",
                as_attachment=True,
                download_name=filename,
            )
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_gmail_export_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_gmail_export_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Gmail history export failed.")
            return (
                jsonify({"status": "error", "message": "Gmail mail history could not be exported right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        finally:
            _release_gmail_export_lock(user_email)

    @app.post("/api/gmail-sea-talk-demo/gmail/export-prewarm")
    def gmail_seatalk_demo_gmail_export_prewarm():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return jsonify(
                {
                    "status": "error",
                    "message": "Gmail access is not available for this Google session yet. Please sign in with Google again to grant Gmail read access.",
                }
            ), HTTPStatus.BAD_REQUEST
        try:
            batch = max(int(request.args.get("batch", "1")), 1)
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid Gmail export batch. Please refresh and try again."}), HTTPStatus.BAD_REQUEST
        user_email = _safe_email_identity(_get_user_identity(settings))
        service = _build_gmail_dashboard_service()
        cached_payload = service.get_cached_export_history_text(batch=batch)
        if cached_payload is not None:
            return jsonify({"status": "ok", "cached": True, "batch": batch}), HTTPStatus.OK
        if not _try_acquire_gmail_export_lock(user_email):
            return jsonify({"status": "ok", "cached": False, "in_progress": True, "batch": batch}), HTTPStatus.ACCEPTED
        try:
            service.prewarm_export_history_text(batch=batch)
            return jsonify({"status": "ok", "cached": True, "batch": batch}), HTTPStatus.OK
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_gmail_export_prewarm_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_gmail_export_prewarm_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Gmail export prewarm failed.")
            return (
                jsonify({"status": "error", "message": "Gmail export prewarm could not be completed right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        finally:
            _release_gmail_export_lock(user_email)

    @app.get("/api/gmail-sea-talk-demo/seatalk")
    def gmail_seatalk_demo_seatalk_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            payload = _build_seatalk_dashboard_service(settings).build_overview()
            return jsonify({"status": "ok", **payload})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk dashboard load failed.")
            return (
                jsonify({"status": "error", "message": "SeaTalk data could not be loaded right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/gmail-sea-talk-demo/seatalk/insights")
    def gmail_seatalk_demo_seatalk_insights_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _current_google_email() or settings.gmail_seatalk_demo_owner_email
            todo_store = _get_seatalk_todo_store(settings)
            service = _build_seatalk_dashboard_service(settings)
            todo_since = todo_store.processed_until(owner_email=owner_email)
            if _callable_accepts_keyword(service.build_insights, "todo_since"):
                payload = service.build_insights(todo_since=todo_since)
            else:
                payload = service.build_insights()
            completed_ids = todo_store.completed_ids(owner_email=owner_email)
            payload = dict(payload)
            open_todos = todo_store.merge_open_todos(
                owner_email=owner_email,
                todos=[todo for todo in (payload.get("my_todos") or []) if isinstance(todo, dict)],
            )
            todo_store.mark_processed_until(
                owner_email=owner_email,
                processed_until=str(payload.get("todo_processed_until") or ""),
            )
            payload["my_todos"] = [
                todo for todo in SeaTalkDashboardService._sort_todos(open_todos)
                if SeaTalkTodoStore.todo_id(todo) not in completed_ids
            ]
            payload["team_todos"] = []
            return jsonify({"status": "ok", **payload})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_insights_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_insights_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk insights load failed.")
            return (
                jsonify({"status": "error", "message": "SeaTalk insights could not be loaded right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/gmail-sea-talk-demo/seatalk/project-updates")
    def gmail_seatalk_demo_seatalk_project_updates_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            service = _build_seatalk_dashboard_service(settings)
            if hasattr(service, "build_project_updates"):
                payload = service.build_project_updates()
            else:
                payload = service.build_insights()
            payload = dict(payload)
            payload["my_todos"] = []
            payload["team_todos"] = []
            return jsonify({"status": "ok", **payload})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_project_updates_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_project_updates_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk project updates load failed.")
            return (
                jsonify({"status": "error", "message": "SeaTalk project updates could not be loaded right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/gmail-sea-talk-demo/seatalk/todos/open")
    def gmail_seatalk_demo_seatalk_open_todos_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _current_google_email() or settings.gmail_seatalk_demo_owner_email
            todo_store = _get_seatalk_todo_store(settings)
            completed_ids = todo_store.completed_ids(owner_email=owner_email)
            open_todos = [
                todo for todo in SeaTalkDashboardService._sort_todos(todo_store.open_todos(owner_email=owner_email))
                if SeaTalkTodoStore.todo_id(todo) not in completed_ids
            ]
            return jsonify({"status": "ok", "my_todos": open_todos, "team_todos": [], "project_updates": []})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_open_todos_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_open_todos_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk open to-dos load failed.")
            return (
                jsonify({"status": "error", "message": "Saved SeaTalk to-dos could not be loaded right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/gmail-sea-talk-demo/seatalk/todos")
    def gmail_seatalk_demo_seatalk_todos_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _current_google_email() or settings.gmail_seatalk_demo_owner_email
            todo_store = _get_seatalk_todo_store(settings)
            service = _build_seatalk_dashboard_service(settings)
            todo_since = todo_store.processed_until(owner_email=owner_email)
            if hasattr(service, "build_todos") and _callable_accepts_keyword(service.build_todos, "todo_since"):
                payload = service.build_todos(todo_since=todo_since)
            elif _callable_accepts_keyword(service.build_insights, "todo_since"):
                payload = service.build_insights(todo_since=todo_since)
            else:
                payload = service.build_insights()
            completed_ids = todo_store.completed_ids(owner_email=owner_email)
            payload = dict(payload)
            open_todos = todo_store.merge_open_todos(
                owner_email=owner_email,
                todos=[todo for todo in (payload.get("my_todos") or []) if isinstance(todo, dict)],
            )
            todo_store.mark_processed_until(
                owner_email=owner_email,
                processed_until=str(payload.get("todo_processed_until") or ""),
            )
            payload["project_updates"] = []
            payload["my_todos"] = [
                todo for todo in SeaTalkDashboardService._sort_todos(open_todos)
                if SeaTalkTodoStore.todo_id(todo) not in completed_ids
            ]
            payload["team_todos"] = []
            return jsonify({"status": "ok", **payload})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_todos_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_todos_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk to-do load failed.")
            return (
                jsonify({"status": "error", "message": "SeaTalk to-dos could not be loaded right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/gmail-sea-talk-demo/seatalk/todos/complete")
    def gmail_seatalk_demo_seatalk_todo_complete():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        todo = payload.get("todo") if isinstance(payload.get("todo"), dict) else payload
        try:
            todo_store = _get_seatalk_todo_store(settings)
            result = todo_store.mark_completed(
                owner_email=_current_google_email() or settings.gmail_seatalk_demo_owner_email,
                todo=todo,
            )
            return jsonify(result)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.route("/api/gmail-sea-talk-demo/seatalk/name-mappings", methods=["GET", "POST"])
    def gmail_seatalk_demo_seatalk_name_mappings():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        mapping_store = _get_seatalk_name_mapping_store(settings)
        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            mappings = mapping_store.merge_mappings(payload.get("mappings") if isinstance(payload, dict) else {})
            SeaTalkDashboardService.clear_cache()
            return jsonify({"status": "ok", "mappings": mappings})
        try:
            candidates = _build_seatalk_dashboard_service(settings).build_name_mappings()
            mappings = mapping_store.mappings()
            mapped_keys = {alias for key in mappings for alias in SeaTalkNameMappingStore.equivalent_keys(key)}
            candidates = dict(candidates)
            candidates["unknown_ids"] = [
                row for row in (candidates.get("unknown_ids") or [])
                if isinstance(row, dict) and not (SeaTalkNameMappingStore.equivalent_keys(row.get("id")) & mapped_keys)
            ]
            return jsonify({"status": "ok", "mappings": mappings, **candidates})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_name_mappings_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_name_mappings_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk name mapping load failed.")
            return (
                jsonify({"status": "error", "message": "SeaTalk name mappings could not be loaded right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/gmail-sea-talk-demo/seatalk/export")
    def gmail_seatalk_demo_seatalk_export():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            content, filename = _build_seatalk_dashboard_service(settings).export_history_text()
            return send_file(
                io.BytesIO(content.encode("utf-8")),
                mimetype="text/plain; charset=utf-8",
                as_attachment=True,
                download_name=filename,
            )
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_export_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=_get_user_identity(settings),
                    extra=error_details,
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_export_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk history export failed.")
            return (
                jsonify({"status": "error", "message": "SeaTalk chat history could not be exported right now. Please try again shortly."}),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/auth/google/logout")
    def google_logout():
        session.pop("google_credentials", None)
        session.pop("google_profile", None)
        flash("Google session cleared.", "success")
        return redirect(url_for("index"))

    @app.post("/config/save")
    def save_mapping_config():
        login_gate = _require_google_login(settings)
        if login_gate is not None:
            return login_gate
        try:
            user_identity = _get_user_identity(settings)
            existing_config = _load_user_config_for_identity(settings, user_identity) or config_store._normalize({})
            save_mode = str(request.form.get("save_mode", "") or "").strip()
            config = {
                "spreadsheet_link": request.form.get("spreadsheet_link", ""),
                "input_tab_name": request.form.get("input_tab_name", ""),
                "bpmis_api_access_token": request.form.get("bpmis_api_access_token", ""),
                "pm_team": request.form.get("pm_team", ""),
                "issue_id_header": request.form.get("issue_id_header", ""),
                "jira_ticket_link_header": request.form.get("jira_ticket_link_header", ""),
                "sync_pm_email": request.form.get("sync_pm_email", ""),
                "sync_project_name_header": request.form.get("sync_project_name_header", ""),
                "sync_market_header": request.form.get("sync_market_header", ""),
                "sync_brd_link_header": request.form.get("sync_brd_link_header", ""),
                "component_route_rules_text": request.form.get("component_route_rules_text", ""),
                "component_default_rules_text": request.form.get("component_default_rules_text", ""),
                "market_header": request.form.get("market_header", ""),
                "system_header": request.form.get("system_header", ""),
                "summary_header": request.form.get("summary_header", ""),
                "prd_links_header": request.form.get("prd_links_header", ""),
                "description_header": request.form.get("description_header", ""),
                "task_type_value": request.form.get("task_type_value", ""),
                "priority_value": request.form.get("priority_value", ""),
                "product_manager_value": request.form.get("product_manager_value", ""),
                "reporter_value": request.form.get("reporter_value", ""),
                "biz_pic_value": request.form.get("biz_pic_value", ""),
                "component_by_market": {
                    market: request.form.get(
                        f"component_{market}",
                        str((existing_config.get("component_by_market") or {}).get(market, "")),
                    )
                    for market in MARKET_KEYS
                },
                "need_uat_by_market": {
                    market: request.form.get(f"need_uat_{market}", "")
                    for market in MARKET_KEYS
                },
            }
            _apply_sync_email_policy(config, user_identity)
            config = config_store._normalize(
                _hydrate_setup_defaults(
                    config,
                    user_identity,
                    team_profiles=_load_effective_team_profiles(config_store),
                )
            )
            if save_mode == "route_only":
                _validate_config_security(settings, config)
                _save_route_only_config(existing_config, config, user_identity)
                _log_portal_event(
                    "config_save_success",
                    **_build_request_log_context(
                        settings,
                        user_identity=user_identity,
                        extra=_build_mapping_log_summary(config, save_mode=save_mode),
                    ),
                )
                flash("System + Market to Component was saved. Component owner table refreshed from the latest saved Components.", "success")
                return redirect(url_for("index"))

            _validate_config_security(settings, config)
            _validate_team_profile_setup(config, team_profiles=_load_effective_team_profiles(config_store))

            config_store.build_field_mappings(config)
            _save_user_config_for_identity(settings, user_identity, config)
            _log_portal_event(
                "config_save_success",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra=_build_mapping_log_summary(config, save_mode=save_mode),
                ),
            )
            flash("Your web Jira config was saved for this user and will be used for preview/run.", "success")
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "config_save_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    extra={
                        **error_details,
                        **_build_mapping_log_summary(config, save_mode=save_mode),
                    },
                ),
            )
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.post("/config/save-route")
    def save_mapping_route_only():
        login_gate = _require_google_login(settings)
        if login_gate is not None:
            return login_gate
        try:
            user_identity = _get_user_identity(settings)
            existing_config = _load_user_config_for_identity(settings, user_identity) or config_store._normalize({})
            payload = request.get_json(silent=True) or {}
            config = config_store._normalize(
                _hydrate_setup_defaults(
                    {
                        "pm_team": payload.get("pm_team", ""),
                        "system_header": payload.get("system_header", ""),
                        "market_header": payload.get("market_header", ""),
                        "component_route_rules_text": payload.get("component_route_rules_text", ""),
                        "component_default_rules_text": payload.get("component_default_rules_text", ""),
                    },
                    user_identity,
                    team_profiles=_load_effective_team_profiles(config_store),
                )
            )
            _validate_config_security(settings, config)
            saved = _save_route_only_config(
                existing_config,
                config,
                user_identity,
                default_text_override=str(payload.get("component_default_rules_text", "") or ""),
            )
            _log_portal_event(
                "config_save_route_success",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra=_build_mapping_log_summary(config, save_mode="route_only"),
                ),
            )
            return jsonify(
                {
                    "status": "ok",
                    "message": "System + Market to Component was saved. Component owner table refreshed from the latest saved Components.",
                    "component_route_rules_text": str(saved.get("component_route_rules_text", "") or ""),
                    "component_default_rules_text": str(saved.get("component_default_rules_text", "") or ""),
                }
            )
        except ToolError as error:
            summary_source = config if "config" in locals() else (payload if "payload" in locals() and isinstance(payload, dict) else {})
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "config_save_route_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    extra={
                        **error_details,
                        **_build_mapping_log_summary(summary_source, save_mode="route_only"),
                    },
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    def _save_route_only_config(
        existing_config: dict[str, Any],
        config: dict[str, Any],
        user_identity: dict[str, str | None],
        *,
        default_text_override: str = "",
    ) -> dict[str, Any]:
        route_only_config = dict(existing_config)
        route_only_config["pm_team"] = config.get("pm_team", "")
        route_only_config["system_header"] = config.get("system_header", "")
        route_only_config["market_header"] = config.get("market_header", "")
        route_only_config["component_route_rules_text"] = config.get("component_route_rules_text", "")
        config_store._parse_component_route_rules(str(route_only_config["component_route_rules_text"]))
        default_seed_text = str(default_text_override or "").strip() or str(existing_config.get("component_default_rules_text", "") or "")
        route_only_config["component_default_rules_text"] = config_store.align_component_defaults_to_routes(
            str(route_only_config["component_route_rules_text"]),
            default_seed_text,
        )
        normalized = config_store._normalize(route_only_config)
        _save_user_config_for_identity(settings, user_identity, normalized)
        return normalized

    @app.post("/admin/team-profiles/save")
    def save_team_profile_admin():
        login_gate = _require_google_login(settings)
        if login_gate is not None:
            return login_gate
        user_identity = _get_user_identity(settings)
        if not _is_team_profile_admin(user_identity):
            flash("Only the portal admin can update team default routing.", "error")
            return redirect(url_for("access_denied"))
        try:
            team_key = str(request.form.get("team_key", "") or "").strip().upper()
            team_profiles = _load_effective_team_profiles(config_store)
            if team_key not in team_profiles:
                raise ToolError(f"Unsupported PM Team: {team_key}.")
            saved_profile = _save_team_profile(settings, config_store,
                team_key,
                {
                    "label": str(team_profiles[team_key].get("label", "") or ""),
                    "ready": True,
                    "component_route_rules_text": request.form.get("component_route_rules_text", ""),
                },
            )
            session["default_workspace_tab"] = "team-default-admin"
            _log_portal_event(
                "team_profile_admin_save_success",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={
                        "team_key": team_key,
                        "route_rule_count": _count_configured_lines(str(saved_profile.get("component_route_rules_text", "") or "")),
                        "default_rule_count": _count_configured_lines(str(saved_profile.get("component_default_rules_text", "") or "")),
                    },
                ),
            )
            flash(f"{team_profiles[team_key]['label']} team defaults were saved.", "success")
        except ToolError as error:
            error_details = _classify_portal_error(error)
            session["default_workspace_tab"] = "team-default-admin"
            _log_portal_event(
                "team_profile_admin_save_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={**error_details, "team_key": str(request.form.get("team_key", "") or "").strip().upper()},
                ),
            )
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.get("/download/default-sheet-template.csv")
    def download_default_sheet_template():
        sample_row = [
            "225159",
            "Standalone Cash Loan",
            "https://docs.google.com/document/d/example",
            "SG",
            "AF",
            "Fraud rule improvement",
            "https://confluence/example-prd",
            "Detailed Jira description goes here.",
            "",
        ]
        csv_lines = [
            ",".join(_csv_escape(header) for header in DEFAULT_SHEET_HEADERS),
            ",".join(_csv_escape(value) for value in sample_row),
        ]
        payload = io.BytesIO("\n".join(csv_lines).encode("utf-8"))
        return send_file(
            payload,
            mimetype="text/csv",
            as_attachment=True,
            download_name="bpmis_jira_default_sheet_template.csv",
        )

    @app.get("/download/default-sheet-template.xlsx")
    def download_default_sheet_template_xlsx():
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = "Sheet1"

        sample_row = [
            "225159",
            "Standalone Cash Loan",
            "https://docs.google.com/document/d/example",
            "SG",
            "AF",
            "Fraud rule improvement",
            "https://confluence/example-prd",
            "Detailed Jira description goes here.",
            "",
        ]
        worksheet.append(DEFAULT_SHEET_HEADERS)
        worksheet.append(sample_row)

        header_font = Font(bold=True, color="1F2937")
        for cell in worksheet[1]:
            cell.font = header_font

        worksheet.freeze_panes = "A2"
        for column_letter, width in {
            "A": 16,
            "B": 28,
            "C": 12,
            "D": 30,
            "E": 14,
            "F": 28,
            "G": 34,
            "H": 42,
            "I": 28,
        }.items():
            worksheet.column_dimensions[column_letter].width = width

        payload = io.BytesIO()
        workbook.save(payload)
        payload.seek(0)
        return send_file(
            payload,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="bpmis_jira_default_sheet_template.xlsx",
        )

    @app.post("/preview")
    def preview():
        login_gate = _require_google_login(settings)
        if login_gate is not None:
            return login_gate
        try:
            service = _build_service(settings)
            results, _headers = service.preview()
            session["last_results"] = _serialize_results(results, include_skipped=False)
            _log_portal_event("preview_success", **_build_request_log_context(settings))
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "preview_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, extra=error_details),
            )
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.post("/run")
    def run():
        login_gate = _require_google_login(settings)
        if login_gate is not None:
            return login_gate
        dry_run = request.form.get("dry_run") == "on"
        try:
            service = _build_service(settings)
            results = service.run(dry_run=dry_run)
            session["last_results"] = _serialize_results(results, include_skipped=False)
            session["run_notice"] = _build_run_notice(results, dry_run=dry_run)
            if dry_run:
                flash("Dry run completed without updating the sheet.", "success")
            else:
                flash("Run completed. Check the results table below.", "success")
            _log_portal_event(
                "run_success",
                **_build_request_log_context(settings, extra={"dry_run": dry_run}),
            )
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "run_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, extra={**error_details, "dry_run": dry_run}),
            )
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.post("/api/jobs/preview")
    def create_preview_job():
        return _start_job("preview", dry_run=True)

    @app.post("/api/jobs/run")
    def create_run_job():
        return _start_job("run", dry_run=False)

    @app.post("/api/jobs/sync-bpmis-projects")
    def create_sync_bpmis_projects_job():
        return _start_job("sync-bpmis-projects", dry_run=False)

    @app.route("/api/local-agent/<path:agent_path>", methods=["GET", "POST", "PATCH", "DELETE"])
    def local_agent_public_proxy(agent_path: str):
        return _proxy_local_agent_request(agent_path)

    @app.get("/api/bpmis-projects")
    def bpmis_projects():
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        user_identity = _get_user_identity(settings)
        store = _get_bpmis_project_store()
        return jsonify({"status": "ok", "projects": store.list_projects(user_key=user_identity["config_key"])})

    @app.delete("/api/bpmis-projects/<bpmis_id>")
    def delete_bpmis_project(bpmis_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        user_identity = _get_user_identity(settings)
        deleted = _get_bpmis_project_store().soft_delete_project(user_key=user_identity["config_key"], bpmis_id=bpmis_id)
        return jsonify({"status": "ok", "deleted": deleted, "scope": "portal_only"})

    @app.patch("/api/bpmis-projects/<bpmis_id>/comment")
    def update_bpmis_project_comment(bpmis_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        payload = request.get_json(silent=True) or {}
        user_identity = _get_user_identity(settings)
        updated = _get_bpmis_project_store().update_project_comment(
            user_key=user_identity["config_key"],
            bpmis_id=bpmis_id,
            pm_comment=str(payload.get("pm_comment") or ""),
        )
        return jsonify({"status": "ok", "updated": updated, "scope": "portal_only"})

    @app.get("/api/bpmis-projects/<bpmis_id>/jira-options")
    def bpmis_project_jira_options(bpmis_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        try:
            user_identity = _get_user_identity(settings)
            service = _build_portal_jira_creation_service(settings)
            options = service.jira_options(user_key=user_identity["config_key"], bpmis_id=bpmis_id)
            return jsonify({"status": "ok", **options})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.get("/api/bpmis-projects/<bpmis_id>/jira-tickets")
    def bpmis_project_jira_tickets(bpmis_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        try:
            user_identity = _get_user_identity(settings)
            service = _build_portal_jira_creation_service(settings)
            include_live = str(request.args.get("live") or "").strip().lower() in {"1", "true", "yes"}
            tickets = service.list_tickets(
                user_key=user_identity["config_key"],
                bpmis_id=bpmis_id,
                include_live=include_live,
            )
            return jsonify({"status": "ok", "tickets": tickets})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.delete("/api/bpmis-projects/<bpmis_id>/jira-tickets/<ticket_id>")
    def delete_bpmis_project_jira_ticket(bpmis_id: str, ticket_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        try:
            user_identity = _get_user_identity(settings)
            service = _build_portal_jira_creation_service(settings)
            deleted = service.delete_ticket(
                user_key=user_identity["config_key"],
                bpmis_id=bpmis_id,
                ticket_id=ticket_id,
            )
            return jsonify({"status": "ok", "deleted": deleted, "scope": "bpmis_and_portal"})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.patch("/api/bpmis-projects/<bpmis_id>/jira-tickets/<ticket_id>/status")
    def update_bpmis_project_jira_ticket_status(bpmis_id: str, ticket_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        payload = request.get_json(silent=True) or {}
        status_value = str(payload.get("status") or "").strip() if isinstance(payload, dict) else ""
        if not status_value:
            return jsonify({"status": "error", "message": "Jira status is required."}), HTTPStatus.BAD_REQUEST
        try:
            user_identity = _get_user_identity(settings)
            service = _build_portal_jira_creation_service(settings)
            ticket = service.update_ticket_status(
                user_key=user_identity["config_key"],
                bpmis_id=bpmis_id,
                ticket_id=ticket_id,
                status=status_value,
            )
            return jsonify({"status": "ok", "ticket": ticket})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.patch("/api/bpmis-projects/<bpmis_id>/jira-tickets/<ticket_id>/version")
    def update_bpmis_project_jira_ticket_version(bpmis_id: str, ticket_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        payload = request.get_json(silent=True) or {}
        version_name = str(payload.get("version_name") or "").strip() if isinstance(payload, dict) else ""
        version_id = str(payload.get("version_id") or "").strip() if isinstance(payload, dict) else ""
        if not version_name and not version_id:
            return jsonify({"status": "error", "message": "Jira fix version is required."}), HTTPStatus.BAD_REQUEST
        try:
            user_identity = _get_user_identity(settings)
            service = _build_portal_jira_creation_service(settings)
            ticket = service.update_ticket_version(
                user_key=user_identity["config_key"],
                bpmis_id=bpmis_id,
                ticket_id=ticket_id,
                version_name=version_name,
                version_id=version_id,
            )
            return jsonify({"status": "ok", "ticket": ticket})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/bpmis-projects/<bpmis_id>/jira-tickets")
    def create_bpmis_project_jira_tickets(bpmis_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        payload = request.get_json(silent=True) or {}
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list):
            return jsonify({"status": "error", "message": "items must be a list."}), HTTPStatus.BAD_REQUEST
        try:
            user_identity = _get_user_identity(settings)
            service = _build_portal_jira_creation_service(settings)
            results = service.create_tickets(user_key=user_identity["config_key"], bpmis_id=bpmis_id, items=items)
            status_code = HTTPStatus.OK if any(result.get("status") == "created" for result in results) else HTTPStatus.BAD_REQUEST
            return jsonify({"status": "ok" if status_code == HTTPStatus.OK else "error", "results": results}), status_code
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.get("/api/productization-upgrade-summary/versions")
    def productization_upgrade_summary_versions():
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate

        query = str(request.args.get("q") or "").strip()
        if not query:
            return jsonify({"status": "error", "message": "Version keyword is required."}), HTTPStatus.BAD_REQUEST

        try:
            bpmis_client = _build_bpmis_client_for_current_user(settings)
            versions = bpmis_client.search_versions(query)
            return jsonify(
                {
                    "status": "ok",
                    "items": [_serialize_productization_version_candidate(item) for item in versions],
                }
            )
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "productization_version_search_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, extra={**error_details, "query": query}),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "productization_version_search_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, extra={"query": query}),
            )
            current_app.logger.exception("Productization version search failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Unable to search versions right now. Please try again shortly.",
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/productization-upgrade-summary/issues")
    def productization_upgrade_summary_issues():
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate

        version_id = str(request.args.get("version_id") or "").strip()
        show_all_before_team_filtering = str(request.args.get("show_all_before_team_filtering") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if not version_id:
            return jsonify({"status": "error", "message": "version_id is required."}), HTTPStatus.BAD_REQUEST

        try:
            bpmis_client = _build_bpmis_client_for_current_user(settings)
            rows = bpmis_client.list_issues_for_version(version_id)
            config_data = _load_current_user_config(settings)
            raw_count = len(rows)
            rows, filter_metadata = _filter_productization_issue_rows_for_pm_team(
                rows,
                config_data,
                show_all_before_team_filtering=show_all_before_team_filtering,
            )
            normalized_items = [_normalize_productization_issue_row(item) for item in rows]
            return jsonify(
                {
                    "status": "ok",
                    "items": normalized_items,
                    "raw_count": raw_count,
                    "filtered_count": len(rows),
                    "llm_description_generated": False,
                    "llm_generated_count": 0,
                    "codex_detailed_feature": False,
                    "codex_generated_count": 0,
                    **filter_metadata,
                }
            )
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "productization_issue_lookup_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, extra={**error_details, "version_id": version_id}),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "productization_issue_lookup_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, extra={"version_id": version_id}),
            )
            current_app.logger.exception("Productization issue lookup failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Unable to load upgrade tickets right now. Please try again shortly.",
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/productization-upgrade-summary/llm-descriptions")
    def productization_upgrade_summary_llm_descriptions():
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate

        version_id = str(request.args.get("version_id") or "").strip()
        show_all_before_team_filtering = str(request.args.get("show_all_before_team_filtering") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if not version_id:
            return jsonify({"status": "error", "message": "version_id is required."}), HTTPStatus.BAD_REQUEST

        try:
            bpmis_client = _build_bpmis_client_for_current_user(settings)
            rows = bpmis_client.list_issues_for_version(version_id)
            config_data = _load_current_user_config(settings)
            raw_count = len(rows)
            rows, filter_metadata = _filter_productization_issue_rows_for_pm_team(
                rows,
                config_data,
                show_all_before_team_filtering=show_all_before_team_filtering,
            )
            normalized_items = [_normalize_productization_issue_row(item) for item in rows]
            codex_metadata = _apply_codex_productization_detailed_features(
                normalized_items,
                rows,
                settings=settings,
            )
            return jsonify(
                {
                    "status": "ok",
                    "items": normalized_items,
                    "raw_count": raw_count,
                    "filtered_count": len(rows),
                    **codex_metadata,
                    **filter_metadata,
                }
            )
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "productization_llm_description_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error), **error_details}), HTTPStatus.BAD_REQUEST
        except Exception:
            request_id = getattr(g, "request_id", "")
            _log_portal_event(
                "productization_llm_description_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, extra={"version_id": version_id}),
            )
            current_app.logger.exception("Productization LLM Description generation failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Productization LLM Description generation failed unexpectedly. Please retry or share the request ID.",
                        "request_id": request_id,
                        "error_category": "unexpected_internal",
                        "error_code": "server_error",
                        "error_retryable": True,
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/jobs/<job_id>")
    def get_job(job_id: str):
        snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
        if snapshot is None:
            return jsonify({"status": "error", "message": "Job not found."}), 404
        return jsonify(snapshot)

    @app.post("/api/spreadsheets/create-template")
    def create_template_spreadsheet():
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        if "google_credentials" not in session:
            return jsonify({"status": "error", "message": "Please connect Google Sheets first."}), HTTPStatus.BAD_REQUEST

        config_data = _load_current_user_config(settings)
        input_tab_name = str(config_data.get("input_tab_name", "") or "").strip() or settings.input_tab_name or "Sheet1"
        spreadsheet_title = "BPMIS Automation Tool"
        try:
            created = GoogleSheetsService.create_template_spreadsheet(
                get_google_credentials(),
                spreadsheet_title=spreadsheet_title,
                input_tab=input_tab_name,
                headers=DEFAULT_SHEET_HEADERS,
            )
            _log_portal_event(
                "spreadsheet_template_create_success",
                **_build_request_log_context(
                    settings,
                    extra={"spreadsheet_id": created["spreadsheet_id"], "input_tab_name": input_tab_name},
                ),
            )
            return jsonify(
                {
                    "status": "ok",
                    "message": "A new Google Sheet was created and prefilled with the template header row.",
                    **created,
                }
            )
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "spreadsheet_template_create_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    extra={**error_details, "input_tab_name": input_tab_name},
                ),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "spreadsheet_template_create_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, extra={"input_tab_name": input_tab_name}),
            )
            current_app.logger.exception("Google Sheet template creation failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Unable to create a new Google Sheet right now. Please try again shortly.",
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def _start_job(action: str, *, dry_run: bool):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        if action != "sync-bpmis-projects" and "google_credentials" not in session:
            return jsonify({"status": "error", "message": "Please connect Google Sheets first."}), 400

        user_identity = _get_user_identity(settings)
        config_data = _load_user_config_for_identity(settings, user_identity) or config_store._normalize({})
        config_data = _hydrate_setup_defaults(config_data, user_identity)
        _apply_sync_email_policy(config_data, user_identity)
        config_data["_user_key"] = user_identity["config_key"]
        job_store: JobStore = current_app.config["JOB_STORE"]
        title = {
            "preview": "Preview Eligible Rows",
            "run": "Run Ticket Creation",
            "sync-bpmis-projects": "Sync BPMIS Projects",
        }.get(action, "Background Job")
        job = job_store.create(action, title=title)
        credentials_payload = dict(session.get("google_credentials") or {})

        thread = threading.Thread(
            target=_run_background_job,
            args=(app, job.job_id, action, settings, config_data, credentials_payload, dry_run),
            daemon=True,
        )
        thread.start()
        _log_portal_event(
            "job_queued",
            **_build_request_log_context(
                settings,
                user_identity=user_identity,
                extra={"job_id": job.job_id, "action": action, "dry_run": dry_run},
            ),
        )
        return jsonify({"status": "queued", "job_id": job.job_id})

    return app


def _build_service(settings: Settings) -> JiraCreationService:
    user_identity = _get_user_identity(settings)
    config_store = _get_config_store()
    config_data = _load_user_config_for_identity(settings, user_identity) or config_store._normalize({})
    sheets = _build_sheets_service(settings, config_data)
    field_mappings_override = config_store.build_field_mappings(config_data)
    access_token = _resolve_bpmis_access_token(config_data, settings)
    return JiraCreationService(
        settings,
        sheets,
        access_token=access_token,
        field_mappings_override=field_mappings_override,
    )


def _build_bpmis_client_for_current_user(settings: Settings):
    config_data = _load_current_user_config(settings)
    access_token = _resolve_bpmis_access_token(config_data, settings)
    return build_bpmis_client(settings, access_token=access_token)


def _load_current_user_config(settings: Settings) -> dict[str, Any]:
    user_identity = _get_user_identity(settings)
    config_store = _get_config_store()
    config_data = _load_user_config_for_identity(settings, user_identity) or config_store._normalize({})
    hydrated = _hydrate_setup_defaults(config_data, user_identity)
    _apply_sync_email_policy(hydrated, user_identity)
    return hydrated


def _build_service_from_config(
    settings: Settings,
    config_data: dict[str, Any],
    credentials_payload: dict[str, Any],
) -> JiraCreationService:
    credentials = Credentials(**credentials_payload)
    sheets = _build_sheets_service_with_credentials(settings, config_data, credentials)
    field_mappings_override = _get_config_store().build_field_mappings(config_data)
    access_token = _resolve_bpmis_access_token(config_data, settings)
    return JiraCreationService(
        settings,
        sheets,
        access_token=access_token,
        field_mappings_override=field_mappings_override,
    )


def _build_project_sync_service_from_config(
    settings: Settings,
    config_data: dict[str, Any],
    credentials_payload: dict[str, Any],
) -> BPMISProjectSyncService:
    credentials = Credentials(**credentials_payload)
    sheets = _build_sheets_service_with_credentials(settings, config_data, credentials)
    bpmis_client = build_bpmis_client(settings, access_token=_resolve_bpmis_access_token(config_data, settings))
    return BPMISProjectSyncService(sheets, bpmis_client)


def _build_portal_project_sync_service(settings: Settings, config_data: dict[str, Any]) -> PortalProjectSyncService:
    bpmis_client = build_bpmis_client(settings, access_token=_resolve_bpmis_access_token(config_data, settings))
    return PortalProjectSyncService(_get_bpmis_project_store(), bpmis_client)


def _build_portal_jira_creation_service(settings: Settings) -> PortalJiraCreationService:
    config_data = _load_current_user_config(settings)
    return PortalJiraCreationService(
        store=_get_bpmis_project_store(),
        bpmis_client=build_bpmis_client(settings, access_token=_resolve_bpmis_access_token(config_data, settings)),
        config_store=_get_config_store(),
        config_data=config_data,
    )


def _build_sheets_service(settings: Settings, config_data: dict[str, object] | None = None) -> GoogleSheetsService:
    credentials = get_google_credentials()
    return _build_sheets_service_with_credentials(settings, config_data or {}, credentials)


def _build_gmail_dashboard_service() -> GmailDashboardService:
    credentials = get_google_credentials()
    return GmailDashboardService(credentials=credentials, cache_key=_current_google_email())


def _persist_owner_google_credentials(settings: Settings) -> None:
    owner_email = str(settings.gmail_seatalk_demo_owner_email or settings.seatalk_owner_email or "").strip().lower()
    current_email = _current_google_email()
    if not owner_email or current_email != owner_email:
        return
    store = current_app.config.get("GOOGLE_CREDENTIAL_STORE") if current_app else None
    credentials_payload = dict(session.get("google_credentials") or {})
    if store is None or not credentials_payload:
        return
    store.save(owner_email=owner_email, credentials_payload=credentials_payload)


def _build_seatalk_dashboard_service(settings: Settings) -> SeaTalkDashboardService:
    if _local_agent_seatalk_enabled(settings):
        name_mapping_store = _get_seatalk_name_mapping_store(settings) if current_app else None
        return RemoteSeaTalkDashboardService(
            _build_local_agent_client(settings),
            name_mappings_provider=lambda: name_mapping_store.mappings() if name_mapping_store else {},
        )
    name_mapping_store = current_app.config.get("SEATALK_NAME_MAPPING_STORE") if current_app else None
    name_overrides_path = getattr(name_mapping_store, "storage_path", None)
    daily_cache_dir = current_app.config.get("SEATALK_DAILY_CACHE_DIR") if current_app else None
    return SeaTalkDashboardService(
        owner_email=settings.seatalk_owner_email,
        seatalk_app_path=settings.seatalk_local_app_path,
        seatalk_data_dir=settings.seatalk_local_data_dir,
        codex_workspace_root=PROJECT_ROOT,
        codex_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
        codex_timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        codex_concurrency=settings.source_code_qa_codex_concurrency,
        name_overrides_path=name_overrides_path,
        daily_cache_dir=daily_cache_dir,
    )


def _seatalk_dashboard_is_configured(settings: Settings) -> bool:
    if _local_agent_seatalk_enabled(settings):
        try:
            capabilities = _build_local_agent_client(settings).get_health().get("capabilities") or {}
            return bool(capabilities.get("seatalk_configured"))
        except ToolError:
            return False
    app_path = Path(str(settings.seatalk_local_app_path or "")).expanduser()
    data_dir = Path(str(settings.seatalk_local_data_dir or "")).expanduser()
    return bool(app_path.exists() and data_dir.exists() and (data_dir / "config.json").exists())


def _has_explicit_spreadsheet_link(config_data: dict[str, object] | None) -> bool:
    if not config_data:
        return False
    return bool(str(config_data.get("spreadsheet_link", "")).strip())


def _build_sheets_service_with_credentials(
    settings: Settings,
    config_data: dict[str, object] | None,
    credentials,
) -> GoogleSheetsService:
    config_data = config_data or {}
    spreadsheet_id = _resolve_spreadsheet_id(str(config_data.get("spreadsheet_link", "")).strip()) or settings.spreadsheet_id
    input_tab_name = str(config_data.get("input_tab_name", "")).strip() or settings.input_tab_name
    issue_id_header = str(config_data.get("issue_id_header", "")).strip() or "Issue ID"
    jira_ticket_link_header = str(config_data.get("jira_ticket_link_header", "")).strip() or "Jira Ticket Link"
    return GoogleSheetsService(
        credentials=credentials,
        spreadsheet_id=spreadsheet_id,
        common_tab=settings.common_tab_name,
        input_tab=input_tab_name,
        issue_id_header=issue_id_header,
        jira_ticket_link_header=jira_ticket_link_header,
    )


def _get_config_store() -> WebConfigStore:
    return current_app.config["CONFIG_STORE"]


def _get_bpmis_project_store() -> BPMISProjectStore:
    settings: Settings = current_app.config["SETTINGS"]
    if _remote_bpmis_config_enabled(settings):
        return RemoteBPMISProjectStore(_build_local_agent_client(settings))
    return current_app.config["BPMIS_PROJECT_STORE"]


def _get_source_code_qa_session_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQASessionStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_SESSION_STORE"]


def _get_source_code_qa_model_availability_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQAModelAvailabilityStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_MODEL_AVAILABILITY_STORE"]


def _get_seatalk_todo_store(settings: Settings):
    if _local_agent_seatalk_enabled(settings):
        return RemoteSeaTalkTodoStore(_build_local_agent_client(settings))
    return current_app.config["SEATALK_TODO_STORE"]


def _get_seatalk_name_mapping_store(settings: Settings):
    if _local_agent_seatalk_enabled(settings):
        return RemoteSeaTalkNameMappingStore(_build_local_agent_client(settings))
    return current_app.config["SEATALK_NAME_MAPPING_STORE"]


def _remote_bpmis_config_enabled(settings: Settings) -> bool:
    return bool(
        _local_agent_mode_enabled(settings)
        and settings.local_agent_base_url
        and settings.local_agent_hmac_secret
        and settings.local_agent_bpmis_enabled
    )


def _load_user_config_for_identity(settings: Settings, user_identity: dict[str, str | None]) -> dict[str, Any] | None:
    user_key = str(user_identity.get("config_key") or "").strip()
    if not user_key:
        return None
    if _remote_bpmis_config_enabled(settings):
        return _build_local_agent_client(settings).bpmis_config_load(user_key=user_key)
    return _get_config_store().load(user_key)


def _save_user_config_for_identity(settings: Settings, user_identity: dict[str, str | None], config: dict[str, Any]) -> dict[str, Any]:
    user_key = str(user_identity.get("config_key") or "").strip()
    if not user_key:
        raise ToolError("Sign in before saving Setup.")
    if _remote_bpmis_config_enabled(settings):
        return _build_local_agent_client(settings).bpmis_config_save(user_key=user_key, config=config)
    return _get_config_store().save(config, user_key)


def _migrate_user_config(settings: Settings, from_user_key: str, to_user_key: str) -> None:
    if _remote_bpmis_config_enabled(settings):
        _build_local_agent_client(settings).bpmis_config_migrate(from_user_key=from_user_key, to_user_key=to_user_key)
        return
    _get_config_store().migrate(from_user_key, to_user_key)


def _save_team_profile(settings: Settings, config_store: WebConfigStore, team_key: str, profile: dict[str, Any]) -> dict[str, Any]:
    if _remote_bpmis_config_enabled(settings):
        return _build_local_agent_client(settings).bpmis_team_profile_save(team_key=team_key, profile=profile)
    return config_store.save_team_profile(team_key, profile)


def _current_google_profile() -> dict[str, Any]:
    return session.get("google_profile") or {}


def _current_google_email() -> str:
    return str(_current_google_profile().get("email") or "").strip().lower()


def _current_google_user_is_blocked(settings: Settings) -> bool:
    if not _shared_portal_enabled(settings):
        return False
    if not settings.team_allowed_emails and not settings.team_allowed_email_domains:
        return False
    email = _current_google_email()
    if not email:
        return False
    if email in settings.team_allowed_emails:
        return False
    if "@" in email:
        domain = email.rsplit("@", 1)[1]
        if domain in settings.team_allowed_email_domains:
            return False
    return True


def _shared_portal_enabled(settings: Settings) -> bool:
    return bool(
        settings.team_portal_base_url
        or settings.team_allowed_email_domains
    )


def _site_requires_google_login(settings: Settings) -> bool:
    return _shared_portal_enabled(settings)


def _google_session_is_connected() -> bool:
    return "google_credentials" in session and bool(_current_google_email())


def _can_access_prd_briefing(settings: Settings) -> bool:
    email = _current_google_email()
    return bool(email and email == settings.prd_briefing_owner_email.strip().lower())


def _can_access_gmail_seatalk_demo(settings: Settings) -> bool:
    email = _current_google_email()
    allowed_emails = {
        settings.gmail_seatalk_demo_owner_email.strip().lower(),
        *GMAIL_SEATALK_BUILTIN_OWNER_EMAILS,
    }
    return bool(email and email in {item for item in allowed_emails if item})


def _can_access_source_code_qa(settings: Settings) -> bool:
    email = _current_google_email()
    return bool(
        email
        and (
            email.endswith("@npt.sg")
            or email == "xiaodong.zheng1991@gmail.com"
        )
    )


def _can_manage_source_code_qa(settings: Settings) -> bool:
    email = _current_google_email()
    admin_emails = {
        settings.source_code_qa_owner_email.strip().lower(),
        *settings.source_code_qa_admin_emails,
        *SOURCE_CODE_QA_BUILTIN_ADMIN_EMAILS,
    }
    return bool(email and email in {item for item in admin_emails if item})


def _source_code_qa_auth_payload(settings: Settings) -> dict[str, Any]:
    email = _current_google_email()
    owner_email = settings.source_code_qa_owner_email.strip().lower()
    admin_emails = {owner_email, *settings.source_code_qa_admin_emails, *SOURCE_CODE_QA_BUILTIN_ADMIN_EMAILS}
    normalized_admins = {item for item in admin_emails if item}
    if email == owner_email:
        match_source = "owner"
    elif email and email in SOURCE_CODE_QA_BUILTIN_ADMIN_EMAILS:
        match_source = "builtin_admin"
    elif email and email in normalized_admins:
        match_source = "admin_allowlist"
    else:
        match_source = ""
    return {
        "signed_in_email": email,
        "can_manage": bool(email and email in normalized_admins),
        "owner_email": owner_email,
        "admin_email_count": len(normalized_admins),
        "admin_match_source": match_source,
    }


def _source_code_qa_git_auth_ready(service: Any, settings: Settings) -> bool:
    if hasattr(service, "git_auth_ready"):
        return bool(service.git_auth_ready())
    return bool(settings.source_code_qa_gitlab_token)


def _build_source_code_qa_service(llm_provider: str | None = None) -> SourceCodeQAService:
    service: SourceCodeQAService = current_app.config["SOURCE_CODE_QA_SERVICE"]
    resolved = service if llm_provider is None else service.with_llm_provider(str(llm_provider or ""))
    if _local_agent_source_code_qa_enabled(current_app.config["SETTINGS"]):
        return RemoteSourceCodeQAService(_build_local_agent_client(current_app.config["SETTINGS"]), service, llm_provider=llm_provider or resolved.llm_provider_name)
    return resolved


def _source_code_qa_query_sync_mode(settings: Settings) -> str:
    mode = str(os.getenv("SOURCE_CODE_QA_QUERY_SYNC_MODE") or "").strip().lower()
    if mode in {"blocking", "background", "disabled"}:
        return mode
    return "background" if _local_agent_source_code_qa_enabled(settings) else "blocking"


def _prepare_source_code_qa_auto_sync(
    service: Any,
    *,
    pm_team: str,
    country: str,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    settings: Settings = current_app.config["SETTINGS"]
    mode = _source_code_qa_query_sync_mode(settings)
    key = service.mapping_key(pm_team, country) if hasattr(service, "mapping_key") else f"{pm_team}:{country}"
    if mode == "disabled":
        return {
            "attempted": False,
            "status": "skipped",
            "reason": "query-time repository auto-sync is disabled",
            "key": key,
        }
    if mode == "background":
        if progress_callback:
            progress_callback("auto_sync_queued", "Repository freshness check is running in the background.", 0, 1)
        if hasattr(service, "ensure_synced_today_background"):
            return service.ensure_synced_today_background(pm_team=pm_team, country=country)

        app_obj = current_app._get_current_object()
        logger = current_app.logger

        def run_background_sync() -> None:
            with app_obj.app_context():
                try:
                    service.ensure_synced_today(pm_team=pm_team, country=country)
                except Exception:
                    logger.exception("Source Code Q&A background auto-sync failed for %s.", key)

        threading.Thread(target=run_background_sync, daemon=True).start()
        return {
            "attempted": False,
            "status": "background_queued",
            "reason": "repository freshness check queued in the background",
            "key": key,
        }
    if progress_callback:
        progress_callback("auto_sync_check", "Checking repository sync schedule.", 0, 1)
    result = service.ensure_synced_today(pm_team=pm_team, country=country)
    if progress_callback:
        if result.get("attempted"):
            progress_callback("auto_sync_completed", "Repository auto-sync completed; starting code search.", 1, 1)
        else:
            progress_callback("auto_sync_completed", "Repository indexes do not need scheduled sync; starting code search.", 1, 1)
    return result


def _build_local_agent_client(settings: Settings) -> LocalAgentClient:
    return LocalAgentClient(
        base_url=settings.local_agent_base_url or "",
        hmac_secret=settings.local_agent_hmac_secret or "",
        timeout_seconds=settings.local_agent_timeout_seconds,
        connect_timeout_seconds=settings.local_agent_connect_timeout_seconds,
    )


def _proxy_local_agent_request(agent_path: str):
    settings: Settings = current_app.config["SETTINGS"]
    configured_base_url = (settings.local_agent_base_url or "").strip().rstrip("/")
    local_base_url = configured_base_url or _local_agent_loopback_base_url()
    normalized_agent_path = agent_path.lstrip("/")
    if configured_base_url:
        target_path = f"/api/local-agent/{normalized_agent_path}"
    else:
        target_path = "/healthz" if normalized_agent_path == "healthz" else f"/api/local-agent/{normalized_agent_path}"
    target_url = f"{local_base_url}{target_path}"
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower()
        not in {
            "host",
            "content-length",
            "connection",
            "accept-encoding",
        }
    }
    try:
        connect_timeout_seconds = max(
            1,
            min(
                int(settings.local_agent_connect_timeout_seconds or 10),
                int(settings.local_agent_timeout_seconds or 300),
            ),
        )
        response = _LOCAL_AGENT_SESSION.request(
            request.method,
            target_url,
            params=request.args,
            data=request.get_data() if request.method != "GET" else None,
            headers=headers,
            timeout=(connect_timeout_seconds, settings.local_agent_timeout_seconds),
        )
    except requests.RequestException as error:
        return jsonify({"status": "error", "message": f"Mac local-agent is unavailable: {error}"}), HTTPStatus.BAD_GATEWAY

    excluded_headers = {"content-encoding", "content-length", "connection", "transfer-encoding"}
    proxy_headers = [(key, value) for key, value in response.headers.items() if key.lower() not in excluded_headers]
    return current_app.response_class(response.content, status=response.status_code, headers=proxy_headers)


def _local_agent_loopback_base_url() -> str:
    host = str(os.environ.get("LOCAL_AGENT_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    port = str(os.environ.get("LOCAL_AGENT_PORT") or "7007").strip() or "7007"
    return f"http://{host}:{port}".rstrip("/")


def _local_agent_mode_enabled(settings: Settings) -> bool:
    mode = (settings.local_agent_mode or "").strip().lower()
    return mode in {"sync", "remote", "cloud_run", "enabled"}


def _local_agent_source_code_qa_enabled(settings: Settings) -> bool:
    return bool(
        _local_agent_mode_enabled(settings)
        and settings.local_agent_base_url
        and settings.local_agent_hmac_secret
        and settings.local_agent_source_code_qa_enabled
    )


def _local_agent_seatalk_enabled(settings: Settings) -> bool:
    return bool(
        _local_agent_mode_enabled(settings)
        and settings.local_agent_base_url
        and settings.local_agent_hmac_secret
        and settings.local_agent_seatalk_enabled
    )


def _source_code_qa_model_availability() -> dict[str, bool]:
    return _get_source_code_qa_model_availability_store().get()


def _source_code_qa_options_payload(service: SourceCodeQAService) -> dict[str, Any]:
    options = service.options_payload()
    availability = _source_code_qa_model_availability()
    providers = []
    for provider in options.get("llm_providers") or []:
        provider_payload = dict(provider)
        value = str(provider_payload.get("value") or "")
        available = bool(availability.get(value, False))
        provider_payload["available"] = available
        provider_payload["disabled"] = not available
        base_label = str(provider_payload.get("label") or value).replace(" (Unavailable)", "")
        provider_payload["label"] = base_label if available else f"{base_label} (Unavailable)"
        providers.append(provider_payload)
    options["llm_providers"] = providers
    return options


def _source_code_qa_provider_available(llm_provider: str | None) -> bool:
    provider = SourceCodeQAService.normalize_query_llm_provider(llm_provider)
    return bool(_source_code_qa_model_availability().get(provider, False))


def _source_code_qa_public_answer_mode(answer_mode: str | None) -> str:
    mode = str(answer_mode or "auto").strip()
    return mode if mode in {"auto", "gemini_flash"} else "auto"


def _compact_source_code_qa_session_payload(result: dict[str, Any]) -> dict[str, Any]:
    structured = result.get("structured_answer") if isinstance(result.get("structured_answer"), dict) else {}
    answer_claim_check = result.get("answer_claim_check") if isinstance(result.get("answer_claim_check"), dict) else {}
    llm_route = result.get("llm_route") if isinstance(result.get("llm_route"), dict) else {}
    codex_trace = result.get("codex_cli_trace") if isinstance(result.get("codex_cli_trace"), dict) else {}
    return {
        "status": result.get("status") or "",
        "trace_id": result.get("trace_id") or "",
        "summary": result.get("summary") or "",
        "llm_answer": result.get("llm_answer") or "",
        "llm_provider": result.get("llm_provider") or "",
        "llm_model": result.get("llm_model") or "",
        "llm_route": {
            "mode": llm_route.get("mode") or "",
            "provider": llm_route.get("provider") or "",
            "prompt_mode": llm_route.get("prompt_mode") or "",
            "candidate_paths": (llm_route.get("candidate_paths") or [])[:30],
            "candidate_path_layers": llm_route.get("candidate_path_layers") or {},
            "codex_session_max_turns": llm_route.get("codex_session_max_turns") or 8,
        },
        "structured_answer": {
            "direct_answer": structured.get("direct_answer") or "",
            "claims": (structured.get("claims") or [])[:8],
            "citations": (structured.get("citations") or [])[:12],
            "missing_evidence": (structured.get("missing_evidence") or [])[:8],
            "confidence": structured.get("confidence") or "",
        },
        "answer_contract": result.get("answer_contract") or {},
        "answer_quality": result.get("answer_quality") or {},
        "codex_cli_summary": result.get("codex_cli_summary") or {},
        "codex_cli_trace": {
            "session_mode": codex_trace.get("session_mode") or "",
            "command_mode": codex_trace.get("command_mode") or "",
            "session_id": codex_trace.get("session_id") or "",
            "exit_code": codex_trace.get("exit_code"),
            "latency_ms": codex_trace.get("latency_ms"),
            "timeout": bool(codex_trace.get("timeout")),
            "stream_messages": (codex_trace.get("stream_messages") or [])[-20:],
            "command_summaries": (codex_trace.get("command_summaries") or [])[-12:],
            "probable_inspected_files": (codex_trace.get("probable_inspected_files") or [])[-20:],
        },
        "codex_citation_validation": answer_claim_check.get("codex_citation_validation") or {},
        "matches": [
            {
                "repo": match.get("repo"),
                "path": match.get("path"),
                "line_start": match.get("line_start"),
                "line_end": match.get("line_end"),
                "retrieval": match.get("retrieval"),
                "trace_stage": match.get("trace_stage"),
                "reason": match.get("reason"),
                "score": match.get("score"),
            }
            for match in (result.get("matches") or [])[:10]
            if isinstance(match, dict)
        ],
    }


def _build_source_code_qa_session_context(result: dict[str, Any], request_payload: dict[str, Any]) -> dict[str, Any]:
    compact = _compact_source_code_qa_session_payload(result)
    matches = compact.get("matches") or []
    codex_trace = compact.get("codex_cli_trace") if isinstance(compact.get("codex_cli_trace"), dict) else {}
    candidate_paths = (compact.get("llm_route") or {}).get("candidate_paths") or []
    inspected_paths: list[dict[str, Any]] = []
    for raw_path in codex_trace.get("probable_inspected_files") or []:
        raw_text = str(raw_path or "")
        matched = None
        for candidate in candidate_paths:
            if isinstance(candidate, dict) and str(candidate.get("path") or "") and str(candidate.get("path") or "") in raw_text:
                matched = candidate
                break
        if matched:
            inspected_paths.append({**matched, "source": "codex_cli_trace"})
    if not inspected_paths:
        inspected_paths = [
            item for item in candidate_paths[:5]
            if isinstance(item, dict) and str(item.get("trace_stage") or "") == "followup_memory"
        ]
    session_id = str(codex_trace.get("session_id") or "").strip()
    return {
        "key": f"{request_payload.get('pm_team') or ''}:{request_payload.get('country') or ALL_COUNTRY}",
        "pm_team": request_payload.get("pm_team") or "",
        "country": request_payload.get("country") or ALL_COUNTRY,
        "question": request_payload.get("question") or "",
        "trace_id": compact.get("trace_id") or "",
        "summary": compact.get("summary") or "",
        "answer": compact.get("llm_answer") or "",
        "rendered_answer": compact.get("llm_answer") or "",
        "llm_provider": compact.get("llm_provider") or "",
        "llm_model": compact.get("llm_model") or "",
        "llm_route": compact.get("llm_route") or {},
        "codex_session_max_turns": (compact.get("llm_route") or {}).get("codex_session_max_turns") or 8,
        "codex_cli_summary": compact.get("codex_cli_summary") or {},
        "codex_cli_trace": codex_trace,
        "codex_cli_session": {
            "session_id": session_id,
            "mode": codex_trace.get("session_mode") or "",
            "last_used_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        } if session_id else {},
        "codex_inspected_paths": inspected_paths[:12],
        "codex_citation_validation": compact.get("codex_citation_validation") or {},
        "codex_candidate_paths": candidate_paths,
        "repo_scope": list(dict.fromkeys([match.get("repo") for match in matches if match.get("repo")]))[:8],
        "matches": matches[:8],
        "matches_snapshot": matches[:10],
        "trace_paths": (result.get("trace_paths") or [])[:5],
        "structured_answer": compact.get("structured_answer") or {},
        "answer_contract": compact.get("answer_contract") or {},
        "evidence_pack": result.get("evidence_pack") or {},
        "answer_quality": compact.get("answer_quality") or {},
    }


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _callable_accepts_keyword(func: Any, keyword: str) -> bool:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return True
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or name == keyword
        for name, parameter in signature.parameters.items()
    )


def _source_code_qa_release_gate_payload(settings: Settings) -> dict[str, Any]:
    data_root = settings.team_portal_data_dir
    if not data_root.is_absolute():
        data_root = (PROJECT_ROOT / data_root).resolve()
    gate = _read_json_file(data_root / "run" / "source_code_qa_release_gate.json")
    eval_status = _read_json_file(data_root / "run" / "source_code_qa_eval_status.json")
    latest_eval = _read_json_file(data_root / "source_code_qa" / "eval_runs" / "latest.json")
    status = str(gate.get("status") or eval_status.get("state") or latest_eval.get("status") or "missing")
    updated_at = gate.get("timestamp") or latest_eval.get("timestamp") or eval_status.get("updated_at")
    return {
        "status": status,
        "updated_at": updated_at,
        "summary": gate.get("summary") or eval_status.get("message") or "",
        "thresholds": gate.get("thresholds") or {},
        "checks": gate.get("checks") or {},
        "latest_eval": {
            "status": latest_eval.get("status"),
            "eval": latest_eval.get("eval") or {},
            "llm_smoke": latest_eval.get("llm_smoke") or {},
            "report_path": latest_eval.get("report_path"),
        },
    }


def _google_credentials_have_scopes(*required_scopes: str) -> bool:
    payload = dict(session.get("google_credentials") or {})
    scopes = payload.get("scopes") or []
    normalized_scopes = {str(scope).strip() for scope in scopes if str(scope).strip()}
    return all(scope in normalized_scopes for scope in required_scopes)


def _google_session_can_call_live_google_apis() -> bool:
    payload = dict(session.get("google_credentials") or {})
    token = str(payload.get("token") or "").strip()
    if not token:
        return False
    refresh_fields = (
        "refresh_token",
        "token_uri",
        "client_id",
        "client_secret",
    )
    return all(str(payload.get(field) or "").strip() for field in refresh_fields)


def _require_google_login(settings: Settings, *, api: bool = False):
    if _site_requires_google_login(settings):
        if not _google_session_is_connected():
            message = "Sign in with your NPT Google account before using the shared portal."
            if api:
                return jsonify({"status": "error", "message": message}), HTTPStatus.UNAUTHORIZED
            return redirect(url_for("index"))
        if _current_google_user_is_blocked(settings):
            message = "This Google account is not authorized for the team portal."
            if api:
                return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
            flash(message, "error")
            return redirect(url_for("access_denied"))
    return None


def _require_gmail_seatalk_demo_access(settings: Settings, *, api: bool = False):
    login_gate = _require_google_login(settings, api=api)
    if login_gate is not None:
        return login_gate
    message = f"SeaTalk Summary is restricted to {settings.gmail_seatalk_demo_owner_email.strip().lower()}."
    if not _can_access_gmail_seatalk_demo(settings):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("index"))
    return None


def _require_source_code_qa_access(settings: Settings, *, api: bool = False):
    login_gate = _require_google_login(settings, api=api)
    if login_gate is not None:
        return login_gate
    message = "Source Code Q&A is available to signed-in @npt.sg users only."
    if not _can_access_source_code_qa(settings):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("index"))
    return None


def _require_source_code_qa_manage_access(settings: Settings, *, api: bool = False):
    access_gate = _require_source_code_qa_access(settings, api=api)
    if access_gate is not None:
        return access_gate
    auth_payload = _source_code_qa_auth_payload(settings)
    message = (
        f"Source Code Q&A repository admin is restricted to {auth_payload['owner_email']} "
        f"and configured admin allowlist. Signed in as {auth_payload['signed_in_email'] or 'unknown'}."
    )
    if not _can_manage_source_code_qa(settings):
        if api:
            return jsonify({"status": "error", "message": message, "auth": auth_payload}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("source_code_qa"))
    return None


def _validate_config_security(settings: Settings, config_data: dict[str, Any]) -> None:
    portal_token = str(config_data.get("bpmis_api_access_token", "") or "").strip()
    if _shared_portal_enabled(settings) and portal_token and not settings.team_portal_config_encryption_key:
        raise ToolError(
            "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY must be configured on the host before saving BPMIS tokens in shared mode."
        )


def _load_effective_team_profiles(config_store: WebConfigStore) -> dict[str, dict[str, Any]]:
    profiles = {team_key: dict(profile) for team_key, profile in TEAM_PROFILE_DEFAULTS.items()}
    stored_profiles = config_store.load_team_profiles()
    if has_app_context():
        settings = current_app.config.get("SETTINGS")
        if isinstance(settings, Settings) and _remote_bpmis_config_enabled(settings):
            stored_profiles = _build_local_agent_client(settings).bpmis_team_profiles_load()
    for team_key, stored_profile in stored_profiles.items():
        if team_key not in profiles:
            continue
        profiles[team_key].update(stored_profile)
    return profiles


def _is_team_profile_admin(user_identity: dict[str, str | None]) -> bool:
    return str(user_identity.get("email") or "").strip().lower() == TEAM_PROFILE_ADMIN_EMAIL


def _can_edit_sync_email(user_identity: dict[str, str | None]) -> bool:
    return str(user_identity.get("email") or "").strip().lower() in SYNC_EMAIL_EDIT_ALLOWLIST


def _apply_sync_email_policy(config_data: dict[str, Any], user_identity: dict[str, str | None]) -> None:
    email = str(user_identity.get("email") or "").strip().lower()
    if email and not _can_edit_sync_email(user_identity):
        config_data["sync_pm_email"] = email


def _hydrate_setup_defaults(
    config_data: dict[str, Any],
    user_identity: dict[str, str | None],
    *,
    team_profiles: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    hydrated = dict(config_data)
    email = str(user_identity.get("email") or "").strip().lower()
    pm_team = str(hydrated.get("pm_team", "") or "").strip().upper()
    effective_team_profiles = team_profiles or TEAM_PROFILE_DEFAULTS
    if pm_team:
        hydrated["pm_team"] = pm_team
    for field, default_value in DEFAULT_DIRECT_VALUES.items():
        if not str(hydrated.get(field, "") or "").strip():
            hydrated[field] = default_value
    profile = effective_team_profiles.get(pm_team)
    if profile:
        if not str(hydrated.get("component_route_rules_text", "") or "").strip():
            hydrated["component_route_rules_text"] = str(profile.get("component_route_rules_text", "") or "")
        if not str(hydrated.get("component_default_rules_text", "") or "").strip():
            hydrated["component_default_rules_text"] = str(profile.get("component_default_rules_text", "") or "")
    existing_need_uat = hydrated.get("need_uat_by_market", {})
    hydrated["need_uat_by_market"] = {
        market: str((existing_need_uat or {}).get(market, "") or DEFAULT_NEED_UAT_BY_MARKET.get(market, "")).strip()
        for market in MARKET_KEYS
    }
    if email:
        for field in ("component_route_rules_text", "component_default_rules_text"):
            value = str(hydrated.get(field, "") or "")
            if TEAM_DEFAULT_EMAIL_PLACEHOLDER in value:
                hydrated[field] = value.replace(TEAM_DEFAULT_EMAIL_PLACEHOLDER, email)
        for field in ("sync_pm_email", "product_manager_value", "reporter_value", "biz_pic_value"):
            if not str(hydrated.get(field, "") or "").strip():
                hydrated[field] = email
    return hydrated


def _build_team_profiles_for_display(
    config_data: dict[str, Any],
    user_identity: dict[str, str | None],
    *,
    team_profiles: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    display_email = (
        str(user_identity.get("email") or "").strip().lower()
        or str(config_data.get("sync_pm_email", "") or "").strip().lower()
        or str(config_data.get("product_manager_value", "") or "").strip().lower()
        or str(config_data.get("reporter_value", "") or "").strip().lower()
        or str(config_data.get("biz_pic_value", "") or "").strip().lower()
    )

    profiles: dict[str, dict[str, Any]] = {}
    for team_key, profile in (team_profiles or TEAM_PROFILE_DEFAULTS).items():
        rendered_profile = dict(profile)
        if display_email:
            for field in ("component_route_rules_text", "component_default_rules_text"):
                value = str(rendered_profile.get(field, "") or "")
                if TEAM_DEFAULT_EMAIL_PLACEHOLDER in value:
                    rendered_profile[field] = value.replace(TEAM_DEFAULT_EMAIL_PLACEHOLDER, display_email)
        profiles[team_key] = rendered_profile
    return profiles


def _validate_team_profile_setup(
    config_data: dict[str, Any],
    *,
    team_profiles: dict[str, dict[str, Any]] | None = None,
) -> None:
    effective_team_profiles = team_profiles or TEAM_PROFILE_DEFAULTS
    pm_team = str(config_data.get("pm_team", "") or "").strip().upper()
    valid_team_labels = ", ".join(profile["label"] for profile in effective_team_profiles.values())
    if not pm_team:
        raise ToolError(f"PM Team is required. Choose {valid_team_labels} before saving setup.")
    if pm_team not in effective_team_profiles:
        raise ToolError(f"Unsupported PM Team: {pm_team}. Choose {valid_team_labels}.")
    profile = effective_team_profiles[pm_team]
    has_routing = bool(str(config_data.get("component_route_rules_text", "") or "").strip())
    has_defaults = bool(str(config_data.get("component_default_rules_text", "") or "").strip())
    if not profile.get("ready") and not (has_routing and has_defaults):
        raise ToolError(
            f"{profile['label']} default setup is not available yet. Open Advanced mapping overrides and enter Component Routing and Component Defaults manually for now."
        )


def _resolve_spreadsheet_id(value: str) -> str | None:
    if not value:
        return None
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    if match:
        return match.group(1)
    if re.fullmatch(r"[a-zA-Z0-9-_]{20,}", value):
        return value
    return None


def _build_run_notice(results: list[object], dry_run: bool) -> dict[str, object]:
    created = [result for result in results if getattr(result, "status", "") == "created"]
    errors = [result for result in results if getattr(result, "status", "") == "error"]
    skipped = [result for result in results if getattr(result, "status", "") == "skipped"]
    previews = [result for result in results if getattr(result, "status", "") == "preview"]

    title = "Dry Run Ready" if dry_run else ("Run Completed" if not errors else "Run Completed With Issues")
    tone = "success" if not errors else "warning"
    summary = (
        f"{len(previews)} ready, {len(errors)} error, {len(skipped)} skipped."
        if dry_run
        else f"{len(created)} created, {len(errors)} error, {len(skipped)} skipped."
    )

    details: list[str] = []
    for result in created[:3]:
        ticket = getattr(result, "ticket_key", None) or getattr(result, "ticket_link", None) or "-"
        details.append(f"Created row {result.row_number}: {ticket}")
    for result in errors[:3]:
        details.append(f"Row {result.row_number}: {result.message}")
    if not details:
        for result in skipped[:3]:
            details.append(f"Row {result.row_number}: {result.message}")

    return {
        "title": title,
        "tone": tone,
        "summary": summary,
        "details": details,
    }


def _build_sync_notice(results: list[object]) -> dict[str, object]:
    created = [result for result in results if getattr(result, "status", "") == "created"]
    updated = [result for result in results if getattr(result, "status", "") == "updated"]
    errors = [result for result in results if getattr(result, "status", "") == "error"]
    skipped = [result for result in results if getattr(result, "status", "") == "skipped"]

    details: list[str] = []
    for result in created[:3]:
        details.append(f"Added BPMIS Issue ID {result.issue_id}.")
    for result in updated[:3]:
        details.append(f"Updated BPMIS Issue ID {result.issue_id}.")
    for result in errors[:3]:
        details.append(f"{result.issue_id or 'Unknown Issue'}: {result.message}")
    if not details:
        for result in skipped[:3]:
            details.append(f"{result.issue_id}: {result.message}")

    return {
        "title": "BPMIS Sync Completed" if not errors else "BPMIS Sync Completed With Issues",
        "tone": "success" if not errors else "warning",
        "summary": f"{len(created)} added, {len(updated)} updated, {len(skipped)} skipped, {len(errors)} error.",
        "details": details,
    }


def _results_for_display(results: list[dict[str, Any]] | list[object], *, include_skipped: bool = False) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for result in results:
        payload = result if isinstance(result, dict) else result.__dict__
        if not include_skipped and str(payload.get("status", "") or "").lower() == "skipped":
            continue
        filtered.append(dict(payload))
    return filtered


def _serialize_results(results: list[object], *, include_skipped: bool = True) -> list[dict[str, Any]]:
    return _results_for_display(results, include_skipped=include_skipped)


def _run_source_code_qa_sync_job(
    app: Flask,
    job_id: str,
    settings: Settings,
    pm_team: str,
    country: str,
) -> None:
    del settings
    with app.app_context():
        job_store: JobStore = app.config["JOB_STORE"]
        job_store.update(
            job_id,
            state="running",
            stage="syncing",
            message="Syncing repositories and rebuilding the source-code index.",
            current=0,
            total=1,
        )
        try:
            result = _build_source_code_qa_service().sync(pm_team=pm_team, country=country)
            status = str(result.get("status") or "ok")
            summary = "Source repositories are synced." if status == "ok" else "Source repository sync completed with issues."
            job_store.complete(
                job_id,
                results=[result],
                notice={
                    "title": "Source Code Sync",
                    "tone": "success" if status == "ok" else "warning",
                    "summary": summary,
                    "details": [f"Status: {status}", f"Repositories: {len(result.get('results') or [])}"],
                },
            )
        except ToolError as error:
            job_store.fail(job_id, str(error))
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            app.logger.exception("Source code QA sync job failed unexpectedly.")
            job_store.fail(job_id, f"Unexpected error: {error}")


def _run_source_code_qa_query_job(app: Flask, job_id: str, payload: dict[str, Any]) -> None:
    with app.app_context():
        job_store: JobStore = app.config["JOB_STORE"]

        def progress_callback(stage: str, message: str, current: int, total: int) -> None:
            job_store.update(
                job_id,
                state="running",
                stage=stage,
                message=message,
                current=current,
                total=total,
            )

        try:
            if not _source_code_qa_provider_available(payload.get("llm_provider")):
                raise ToolError("Selected Source Code Q&A model is unavailable.")
            service = _build_source_code_qa_service(payload.get("llm_provider"))
            pm_team = str(payload.get("pm_team") or "")
            country = str(payload.get("country") or "")
            session_store = _get_source_code_qa_session_store()
            session_id = str(payload.get("session_id") or "").strip()
            owner_email = str(payload.get("_session_owner_email") or "").strip().lower() or "local"
            conversation_context = payload.get("conversation_context") if isinstance(payload.get("conversation_context"), dict) else None
            if conversation_context is None and session_id:
                conversation_context = session_store.get_context(session_id, owner_email=owner_email)
            auto_sync = _prepare_source_code_qa_auto_sync(
                service,
                pm_team=pm_team,
                country=country,
                progress_callback=progress_callback,
            )
            def run_query() -> dict[str, Any]:
                return service.query(
                    pm_team=pm_team,
                    country=country,
                    question=str(payload.get("question") or ""),
                    answer_mode=_source_code_qa_public_answer_mode(payload.get("answer_mode")),
                    llm_budget_mode="auto",
                    conversation_context=conversation_context,
                    progress_callback=progress_callback,
                )

            if service.llm_provider_name == "codex_cli_bridge" and session_id:
                progress_callback("codex_session_lock", "Waiting for this chat's Codex session slot.", 0, 1)
                with _source_code_qa_codex_session_lock(session_id):
                    result = run_query()
            else:
                result = run_query()
            result["auto_sync"] = auto_sync
            if session_id:
                session_payload = session_store.append_exchange(
                    session_id,
                    owner_email=owner_email,
                    pm_team=pm_team,
                    country=country,
                    llm_provider=str(payload.get("llm_provider") or ""),
                    question=str(payload.get("question") or ""),
                    result=result,
                    context=_build_source_code_qa_session_context(result, payload),
                )
                if session_payload is not None:
                    result["session"] = session_payload
                    result["session_id"] = session_id
            status = str(result.get("status") or "ok")
            job_store.complete(
                job_id,
                results=[result],
                notice={
                    "title": "Source Code Q&A",
                    "tone": "success" if status == "ok" else "warning",
                    "summary": result.get("summary") or "Source Code Q&A completed.",
                    "details": [f"Status: {status}", f"Trace: {result.get('trace_id') or 'n/a'}"],
                },
            )
        except ToolError as error:
            job_store.fail(job_id, str(error))
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            app.logger.exception("Source code QA query job failed unexpectedly.")
            job_store.fail(job_id, f"Unexpected error: {error}")


def _run_background_job(
    app: Flask,
    job_id: str,
    action: str,
    settings: Settings,
    config_data: dict[str, Any],
    credentials_payload: dict[str, Any],
    dry_run: bool,
) -> None:
    with app.app_context():
        job_store: JobStore = app.config["JOB_STORE"]
        logger = app.logger
        job_store.update(
            job_id,
            state="running",
            stage="starting",
            message="Preparing your request.",
            current=0,
            total=0,
        )
        _log_portal_event(
            "job_started",
            logger=logger,
            job_id=job_id,
            action=action,
            dry_run=dry_run,
            pm_team=str(config_data.get("pm_team", "") or "").strip().upper(),
            user=str(config_data.get("sync_pm_email", "") or "").strip().lower(),
        )

        def progress_callback(stage: str, message: str, current: int, total: int) -> None:
            job_store.update(
                job_id,
                state="running",
                stage=stage,
                message=message,
                current=current,
                total=total,
            )

        try:
            if action == "sync-bpmis-projects":
                service = _build_portal_project_sync_service(settings, config_data)
                results = service.sync_projects(
                    user_key=str(config_data.get("_user_key", "")).strip(),
                    pm_email=str(config_data.get("sync_pm_email", "")).strip(),
                    progress_callback=progress_callback,
                )
                notice = _build_sync_notice(results)
            else:
                service = _build_service_from_config(settings, config_data, credentials_payload)
                if dry_run:
                    results, _headers = service.preview(progress_callback=progress_callback)
                else:
                    results = service.run(dry_run=False, progress_callback=progress_callback)
                notice = _build_run_notice(results, dry_run=dry_run)
            include_skipped = action == "sync-bpmis-projects"
            job_store.complete(
                job_id,
                results=_serialize_results(results, include_skipped=include_skipped),
                notice=notice,
            )
            _log_portal_event(
                "job_completed",
                logger=logger,
                job_id=job_id,
                action=action,
                dry_run=dry_run,
                result_count=len(results),
            )
        except ToolError as error:
            job_store.fail(job_id, str(error))
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "job_tool_error",
                level=logging.WARNING,
                logger=logger,
                job_id=job_id,
                action=action,
                dry_run=dry_run,
                **error_details,
            )
        except Exception as error:  # noqa: BLE001
            job_store.fail(job_id, f"Unexpected error: {error}")
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "job_unexpected_error",
                level=logging.ERROR,
                logger=logger,
                job_id=job_id,
                action=action,
                dry_run=dry_run,
                **error_details,
            )
            logger.exception("Background job failed unexpectedly.")


def _csv_escape(value: str) -> str:
    text = str(value)
    if any(character in text for character in [",", "\"", "\n"]):
        return "\"" + text.replace("\"", "\"\"") + "\""
    return text


def _resolve_bpmis_access_token(config_data: dict[str, Any], settings: Settings) -> str | None:
    configured_token = str(config_data.get("bpmis_api_access_token", "") or "").strip()
    return configured_token or settings.bpmis_api_access_token


def _serialize_productization_version_candidate(row: dict[str, Any]) -> dict[str, str]:
    version_id = str(row.get("id") or row.get("versionId") or "").strip()
    version_name = (
        str(row.get("fullName") or row.get("name") or row.get("versionName") or row.get("label") or "").strip()
    )
    market = _coerce_display_text(row.get("marketId") or row.get("market") or row.get("country"))
    label = version_name
    if market:
        label = f"{version_name} · {market}"
    return {
        "version_id": version_id,
        "version_name": version_name,
        "market": market,
        "label": label,
    }


def _normalize_productization_issue_row(row: dict[str, Any]) -> dict[str, Any]:
    ticket_key = _extract_first_text(
        row,
        "jiraKey",
        "ticketKey",
        "jiraIssueKey",
        "issueKey",
        "key",
    )
    ticket_link = _normalize_productization_ticket_url(
        _extract_first_text(row, "jiraLink", "ticketLink", "jiraUrl", "url", "link")
    )
    if not ticket_key:
        ticket_key = _extract_issue_key_from_text(ticket_link)
    if not ticket_link and ticket_key:
        ticket_link = f"{_jira_browse_base_url()}{ticket_key}"

    return {
        "jira_ticket_number": ticket_key or "-",
        "jira_ticket_url": ticket_link or "",
        "feature_summary": _extract_first_text(row, "summary", "title", "jiraSummary") or "-",
        "detailed_feature": _format_productization_description_text(
            _extract_first_text(row, "desc", "description", "jiraDescription")
        ),
        "detailed_feature_source": "jira_description",
        "pm": _extract_person_display(
            _extract_first_value(row, "jiraRegionalPmPicId", "regionalPmPic", "productManager", "pm", "regionalPm")
        )
        or "-",
        "prd_links": _extract_link_values(
            _extract_first_value(row, "jiraPrdLink", "prdLink", "prdLinks", "prd", "brdLink")
        ),
    }


def _apply_codex_productization_detailed_features(
    normalized_items: list[dict[str, Any]],
    raw_rows: list[dict[str, Any]],
    *,
    settings: Settings,
) -> dict[str, Any]:
    if not normalized_items:
        return {
            "llm_description_generated": True,
            "llm_generated_count": 0,
            "codex_detailed_feature": True,
            "codex_generated_count": 0,
        }

    prompt_items = []
    for normalized, raw in zip(normalized_items, raw_rows):
        prompt_items.append(
            {
                "jira_ticket_number": str(normalized.get("jira_ticket_number") or "-"),
                "feature_summary": str(normalized.get("feature_summary") or "-"),
                "jira_description": _format_productization_description_text(
                    _extract_first_text(raw, "desc", "description", "jiraDescription")
                )[:6000],
            }
        )

    generated = _generate_productization_detailed_features_with_codex(prompt_items, settings=settings)
    generated_by_ticket = {
        str(item.get("jira_ticket_number") or "").strip(): str(item.get("detailed_feature") or "").strip()
        for item in generated
        if isinstance(item, dict)
    }

    generated_count = 0
    for item in normalized_items:
        ticket_number = str(item.get("jira_ticket_number") or "").strip()
        detailed_feature = generated_by_ticket.get(ticket_number, "")
        if detailed_feature:
            item["detailed_feature"] = detailed_feature
            item["detailed_feature_source"] = "codex"
            generated_count += 1
    return {
        "llm_description_generated": True,
        "llm_generated_count": generated_count,
        "codex_detailed_feature": True,
        "codex_generated_count": generated_count,
    }


def _generate_productization_detailed_features_with_codex(
    prompt_items: list[dict[str, str]],
    *,
    settings: Settings,
) -> list[dict[str, str]]:
    if _local_agent_source_code_qa_enabled(settings):
        return [
            {
                "jira_ticket_number": str(item.get("jira_ticket_number") or "").strip(),
                "detailed_feature": _clean_codex_productization_detailed_feature(str(item.get("detailed_feature") or "")),
            }
            for item in _build_local_agent_client(settings).productization_llm_descriptions(items=prompt_items)
            if isinstance(item, dict)
        ]
    return _generate_productization_detailed_features_with_local_codex(prompt_items, settings=settings)


def _generate_productization_detailed_features_with_local_codex(
    prompt_items: list[dict[str, str]],
    *,
    settings: Settings,
) -> list[dict[str, str]]:
    provider = CodexCliBridgeSourceCodeQALLMProvider(
        workspace_root=PROJECT_ROOT,
        timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        concurrency_limit=settings.source_code_qa_codex_concurrency,
        session_mode="ephemeral",
        codex_binary=os.getenv("SOURCE_CODE_QA_CODEX_BINARY") or None,
    )
    prompt = (
        "Generate English Detailed Feature text for each Jira ticket from its Jira Description.\n"
        "Rules:\n"
        "- Output strict JSON only, with shape: {\"items\":[{\"jira_ticket_number\":\"...\",\"detailed_feature\":\"...\"}]}.\n"
        "- Keep one item per input Jira ticket and preserve the jira_ticket_number exactly.\n"
        "- Write in clear product/engineering English.\n"
        "- Summarize the functional change and expected behavior, not implementation chatter.\n"
        "- If the description is empty or not meaningful, use \"-\".\n"
        "- Do not include Markdown fences, citations, explanations, or Chinese text.\n\n"
        f"Input JSON:\n{json.dumps({'items': prompt_items}, ensure_ascii=False)}"
    )
    result = provider.generate(
        payload={
            "systemInstruction": {"parts": [{"text": "You are a concise product feature summarizer."}]},
            "contents": [{"parts": [{"text": prompt}]}],
            "codex_prompt_mode": "productization_detailed_feature_v1",
        },
        primary_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
        fallback_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
    )
    text = provider.extract_text(result.payload)
    payload = _parse_codex_json_object(text)
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise ToolError("Codex returned an invalid Detailed Feature payload.")
    return [
        {
            "jira_ticket_number": str(item.get("jira_ticket_number") or "").strip(),
            "detailed_feature": _clean_codex_productization_detailed_feature(str(item.get("detailed_feature") or "")),
        }
        for item in items
        if isinstance(item, dict)
    ]


def _parse_codex_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end <= start:
            raise ToolError("Codex returned unreadable Detailed Feature JSON.") from error
        try:
            payload = json.loads(raw[start : end + 1])
        except json.JSONDecodeError as nested_error:
            raise ToolError("Codex returned unreadable Detailed Feature JSON.") from nested_error
    if not isinstance(payload, dict):
        raise ToolError("Codex returned an invalid Detailed Feature payload.")
    return payload


def _clean_codex_productization_detailed_feature(value: str) -> str:
    text = _format_productization_description_text(value)
    if not text:
        return "-"
    text = re.sub(r"```(?:json)?|```", "", text, flags=re.I).strip()
    return text or "-"


def _filter_productization_issue_rows_for_pm_team(
    rows: list[dict[str, Any]],
    config_data: dict[str, Any],
    *,
    show_all_before_team_filtering: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    allowed_components = _productization_allowed_components_for_pm_team(config_data)
    if show_all_before_team_filtering:
        return rows, {"team_filter_applied": False, "show_all_before_team_filtering": True}
    if not allowed_components:
        return rows, {"team_filter_applied": False, "show_all_before_team_filtering": False}
    filtered_rows = [row for row in rows if _productization_issue_matches_components(row, allowed_components)]
    return filtered_rows, {"team_filter_applied": True, "show_all_before_team_filtering": False}


def _productization_allowed_components_for_pm_team(config_data: dict[str, Any]) -> set[str]:
    pm_team = str(config_data.get("pm_team", "") or "").strip().upper()
    if pm_team == "AF":
        return {"dbp-anti-fraud", "anti-fraud"}
    return set()


def _productization_issue_matches_components(row: dict[str, Any], allowed_components: set[str]) -> bool:
    issue_components = _extract_productization_issue_components(row)
    return bool(issue_components and issue_components.intersection(allowed_components))


def _extract_productization_issue_components(row: dict[str, Any]) -> set[str]:
    raw_value = _extract_first_value(
        row,
        "componentId",
        "component",
        "components",
        "jiraComponent",
        "jiraComponentId",
    )
    flattened = _flatten_productization_component_values(raw_value)
    return {component.lower() for component in flattened if component}


def _flatten_productization_component_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        parts = [part.strip() for part in re.split(r"[;,/|]", text) if part.strip()]
        return parts or [text]
    if isinstance(value, dict):
        parts: list[str] = []
        for key in ("displayName", "name", "label", "value", "fullName", "id"):
            text = str(value.get(key) or "").strip()
            if text:
                parts.append(text)
        return parts
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            parts.extend(_flatten_productization_component_values(item))
        return parts
    return [str(value).strip()]


def _normalize_productization_ticket_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    issue_key = _extract_issue_key_from_text(text)
    if issue_key:
        return f"{_jira_browse_base_url()}{issue_key}"
    return text


def _format_productization_description_text(value: str) -> str:
    if not str(value or "").strip():
        return "-"

    text = html.unescape(value)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\r", "\n")
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.split("\n")]
    chunks = [line for line in lines if line]
    return "\n".join(chunks).strip() if chunks else "-"


def _extract_first_value(row: dict[str, Any], *keys: str) -> Any:
    containers = [row]
    for nested_key in ("fields", "mapping", "data", "detail", "row"):
        nested = row.get(nested_key)
        if isinstance(nested, dict):
            containers.append(nested)

    for key in keys:
        lowered_key = key.lower()
        for container in containers:
            for candidate_key, value in container.items():
                if str(candidate_key).lower() == lowered_key:
                    return value
    return None


def _extract_first_text(row: dict[str, Any], *keys: str) -> str:
    value = _extract_first_value(row, *keys)
    return _coerce_display_text(value)


def _coerce_display_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("displayName", "name", "emailAddress", "label", "value", "fullName", "id"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_coerce_display_text(item) for item in value]
        parts = [part for part in parts if part]
        return ", ".join(parts)
    return str(value).strip()


def _extract_person_display(value: Any) -> str:
    if isinstance(value, list):
        people = [_extract_person_display(item) for item in value]
        people = [person for person in people if person]
        return ", ".join(people)
    if isinstance(value, dict):
        for key in ("displayName", "name", "emailAddress", "label", "username", "value"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""
    return _coerce_display_text(value)


def _extract_link_values(value: Any) -> list[dict[str, str]]:
    links = _flatten_links(value)
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append({"label": link, "url": link})
    return deduped


def _flatten_links(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        links: list[str] = []
        for item in value:
            links.extend(_flatten_links(item))
        return links
    if isinstance(value, dict):
        links: list[str] = []
        for key in ("url", "link", "href", "value"):
            links.extend(_flatten_links(value.get(key)))
        return links
    text = str(value).strip()
    if not text:
        return []
    matches = re.findall(r"https?://[^\s,]+", text)
    if matches:
        return matches
    return [text] if text.startswith("http://") or text.startswith("https://") else []


def _extract_issue_key_from_text(value: str) -> str:
    match = re.search(r"\b([A-Z][A-Z0-9]+-\d+)\b", value or "")
    return match.group(1) if match else ""


def _jira_browse_base_url() -> str:
    return "https://jira.shopee.io/browse/"


def _get_user_identity(settings: Settings | None = None) -> dict[str, str | None]:
    profile = _current_google_profile()
    email = _current_google_email()
    name = str(profile.get("name") or "").strip()

    if email:
        return {
            "config_key": f"google:{email}",
            "display_name": name or email,
            "email": email,
            "mode": "google",
        }

    if settings and _shared_portal_enabled(settings):
        return {
            "config_key": None,
            "display_name": "Sign in with your NPT Google account",
            "email": None,
            "mode": "guest",
        }

    anonymous_key = session.get("anonymous_user_key")
    if not anonymous_key:
        anonymous_key = secrets.token_hex(8)
        session["anonymous_user_key"] = anonymous_key

    return {
        "config_key": f"anon:{anonymous_key}",
        "display_name": f"Anonymous Session {anonymous_key[:6]}",
        "email": None,
        "mode": "anonymous",
    }
