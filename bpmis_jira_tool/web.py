from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
import difflib
from email.utils import getaddresses
from functools import lru_cache
import hashlib
import inspect
import io
from http import HTTPStatus
import json
import logging
import mimetypes
import os
from pathlib import Path
import re
import secrets
import sqlite3
import subprocess
import threading
import time
from typing import Any
from urllib.parse import parse_qs, urlparse
import uuid
import zipfile

from flask import Flask, Response, current_app, flash, g, has_app_context, jsonify, redirect, render_template, request, send_file, session, stream_with_context, url_for
from dotenv import load_dotenv
import google_auth_httplib2
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build as build_google_api
from googleapiclient.errors import HttpError
import httplib2
import requests
from werkzeug.middleware.proxy_fix import ProxyFix

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.daily_brief_archive import (
    DailyBriefArchiveStore,
    daily_brief_archive_path,
    daily_brief_pdf_bytes,
)
from bpmis_jira_tool.errors import BPMISError, ConfigError, ToolError
from bpmis_jira_tool.google_auth import (
    create_google_authorization_url,
    finish_google_oauth,
    get_google_credentials,
)
from bpmis_jira_tool.job_store import JobState, JobStore
from bpmis_jira_tool.local_agent_client import (
    LocalAgentClient,
    RemoteBPMISProjectStore,
    RemoteSeaTalkDashboardService,
    RemoteSeaTalkNameMappingStore,
    RemoteSeaTalkTodoStore,
    RemoteTeamDashboardConfigStore,
    RemoteSourceCodeQAAttachmentStore,
    RemoteSourceCodeQAGeneratedArtifactStore,
    RemoteSourceCodeQAModelAvailabilityStore,
    RemoteSourceCodeQARuntimeEvidenceStore,
    RemoteSourceCodeQASessionStore,
    RemoteSourceCodeQAService,
    _LOCAL_AGENT_SESSION,
)
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE, GmailDashboardService
from bpmis_jira_tool.gmail_sender import StoredGoogleCredentials
from bpmis_jira_tool.meeting_recorder import (
    CALENDAR_READONLY_SCOPE,
    GoogleCalendarMeetingService,
    MeetingProcessingService,
    MeetingRecorderConfig,
    MeetingRecorderRuntime,
    normalize_meeting_transcript_language,
    MeetingRecordStore,
    _utc_now,
    meeting_platform_from_link,
)
from bpmis_jira_tool.monthly_report import (
    DEFAULT_MONTHLY_REPORT_RECIPIENT,
    DEFAULT_MONTHLY_REPORT_TEMPLATE,
    MONTHLY_REPORT_PRODUCT_SCOPE,
    MonthlyReportService,
    monthly_report_subject,
    normalize_monthly_report_template,
    resolve_monthly_report_period,
    send_monthly_report_email,
)
from bpmis_jira_tool.report_intelligence import (
    normalize_report_intelligence_config,
)
from bpmis_jira_tool.productization_codex import (
    clean_codex_productization_detailed_feature as _clean_codex_productization_detailed_feature,
    format_productization_description_text as _format_productization_description_text,
    generate_productization_detailed_features_with_local_codex as _generate_productization_detailed_features_with_local_codex,
    parse_codex_json_object as _parse_codex_json_object,
)
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService
from bpmis_jira_tool.seatalk_stores import SeaTalkNameMappingStore, SeaTalkTodoStore
from bpmis_jira_tool.bpmis_client import build_bpmis_client
from bpmis_jira_tool.bpmis_projects import BPMISProjectStore, PortalJiraCreationService, PortalProjectSyncService
from bpmis_jira_tool.source_code_qa import CRMS_COUNTRIES, ALL_COUNTRY, IDENTIFIER_PATTERN, CodexCliBridgeSourceCodeQALLMProvider, SourceCodeQAService
from bpmis_jira_tool.source_code_qa_factory import build_source_code_qa_service_from_settings
from bpmis_jira_tool.source_code_qa_jobs import SourceCodeQAQueryScheduler
from bpmis_jira_tool.source_code_qa_sql_artifacts import (
    build_source_code_qa_sql_readme as _build_source_code_qa_sql_readme,
    extract_source_code_qa_sql_blocks as _extract_source_code_qa_sql_blocks,
    format_source_code_qa_sql_text as _format_source_code_qa_sql_text,
)
from bpmis_jira_tool.source_code_qa_stores import (
    SourceCodeQAAttachmentStore,
    SourceCodeQAGeneratedArtifactStore,
    SourceCodeQAModelAvailabilityStore,
    SourceCodeQARuntimeEvidenceStore,
    SourceCodeQASessionStore,
    _compact_source_code_qa_session_payload,
)
from bpmis_jira_tool.team_dashboard_config import (
    TEAM_DASHBOARD_DEFAULT_MEMBER_EMAILS_BY_TEAM,
    TEAM_DASHBOARD_EXCLUDED_PENDING_STATUSES,
    TEAM_DASHBOARD_LEGACY_DEFAULT_MEMBER_EMAILS,
    TEAM_DASHBOARD_PENDING_LIVE_BIZ_PROJECT_STATUSES,
    TEAM_DASHBOARD_TASK_CACHE_VERSION,
    TEAM_DASHBOARD_TEAMS,
    TEAM_DASHBOARD_UNDER_PRD_BIZ_PROJECT_STATUSES,
    TEAM_DASHBOARD_UNDER_PRD_STATUSES,
    TeamDashboardConfigStore,
    normalize_team_dashboard_emails as _normalize_team_dashboard_emails,
)
from bpmis_jira_tool.user_config import (
    CONFIGURED_FIELDS,
    DEFAULT_DIRECT_VALUES,
    DEFAULT_NEED_UAT_BY_MARKET,
    TEAM_DEFAULT_EMAIL_PLACEHOLDER,
    TEAM_PROFILE_DEFAULTS,
    WebConfigStore,
)
from bpmis_jira_tool.work_memory import (
    WorkMemoryStore,
    gmail_attachment_memory_item,
    gmail_drive_link_memory_item,
    gmail_message_memory_item,
    meeting_record_memory_items,
    sent_monthly_report_memory_item_from_gmail_record,
    source_code_qa_memory_item,
    team_dashboard_memory_items,
)
from prd_briefing import create_prd_briefing_blueprint
from prd_briefing.confluence import ConfluenceConnector
from prd_briefing.reviewer import PRDBriefingReviewRequest, PRDReviewRequest, PRDReviewService
from prd_briefing.storage import BriefingStore
from prd_briefing.text_generation import CodexTextGenerationClient


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_CODE_QA_EFFORT_DICTIONARY_PATH = PROJECT_ROOT / "config" / "source_code_qa_effort_dictionaries.json"
SOURCE_CODE_QA_DOMAIN_PROFILES_PATH = PROJECT_ROOT / "config" / "source_code_qa_domain_profiles.json"
SOURCE_CODE_QA_DOMAIN_KNOWLEDGE_PATH = PROJECT_ROOT / "config" / "source_code_qa_domain_knowledge_packs.json"
_configured_env_file = os.getenv("ENV_FILE")
if _configured_env_file is not None:
    _dotenv_path = _configured_env_file.strip()
else:
    _dotenv_path = str(PROJECT_ROOT / ".env")
if _dotenv_path:
    load_dotenv(_dotenv_path)
MARKET_KEYS = ["ID", "SG", "PH", "Regional"]
PORTAL_ADMIN_EMAIL = "xiaodong.zheng@npt.sg"
PORTAL_TEST_USER_EMAIL = "xiaodong.zheng1991@gmail.com"
TEAM_PROFILE_ADMIN_EMAIL = PORTAL_ADMIN_EMAIL
SYNC_EMAIL_EDIT_ALLOWLIST = {PORTAL_ADMIN_EMAIL}
SOURCE_CODE_QA_BUILTIN_ADMIN_EMAILS = {PORTAL_ADMIN_EMAIL}
GMAIL_SEATALK_BUILTIN_OWNER_EMAILS = {PORTAL_ADMIN_EMAIL}
TEAM_DASHBOARD_ACCESS_EMAILS = {PORTAL_ADMIN_EMAIL}
MEETING_RECORDER_PROCESS_ACTION = "meeting-recorder-process"
MEETING_RECORDER_UPCOMING_DISPLAY_LIMIT = 3
WORK_MEMORY_GMAIL_BACKFILL_ACTION = "work-memory-gmail-backfill"
GOOGLE_DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
GMAIL_WORK_MEMORY_MAX_ATTACHMENT_BYTES = 12 * 1024 * 1024
GMAIL_WORK_MEMORY_MAX_VIP_ATTACHMENTS_PER_MESSAGE = 5
GMAIL_WORK_MEMORY_CONTENT_CHARS = 16000
GMAIL_WORK_MEMORY_FETCH_BATCH_SIZE = 25
GMAIL_WORK_MEMORY_FETCH_WORKERS = 4
GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS = 20
TEAM_DASHBOARD_LINK_BIZ_EXCLUDED_TITLE_PHRASES = (
    "sync af productization",
    "productisation upgrade",
    "deployment of productization",
)
_gmail_export_active_users: set[str] = set()
_gmail_export_active_users_lock = threading.Lock()
_source_code_qa_codex_session_locks: dict[str, threading.Lock] = {}
_source_code_qa_codex_session_locks_guard = threading.Lock()


def _bool_env_from_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


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



def _dedupe_seatalk_name_mapping_candidates(rows: Any) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        canonical_id = SeaTalkNameMappingStore.canonical_display_key(row.get("id"))
        if not canonical_id:
            continue
        if canonical_id not in merged:
            order.append(canonical_id)
            merged[canonical_id] = {**row, "id": canonical_id}
        else:
            current = merged[canonical_id]
            current["count"] = _safe_int(current.get("count")) + _safe_int(row.get("count"))
            if not current.get("example") and row.get("example"):
                current["example"] = row.get("example")
            if _seatalk_mapping_reason_rank(row.get("priority_reason")) > _seatalk_mapping_reason_rank(current.get("priority_reason")):
                current["priority_reason"] = row.get("priority_reason")
            if current.get("type") != "group" and row.get("type") == "group":
                current["type"] = "group"
    return [merged[key] for key in order]


def _seatalk_mapping_reason_rank(value: Any) -> int:
    reason = str(value or "").strip().lower()
    if "@mentioned" in reason:
        return 3
    if "direct" in reason or "private" in reason:
        return 2
    return 1 if reason else 0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_email_identity(user_identity: dict[str, str | None] | None = None) -> str:
    if not user_identity:
        return ""
    return str(user_identity.get("email") or user_identity.get("config_key") or "").strip().lower()


def _meeting_record_summary(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "record_id": record.get("record_id"),
        "title": record.get("title"),
        "platform": record.get("platform"),
        "meeting_link": record.get("meeting_link"),
        "calendar_event_id": record.get("calendar_event_id"),
        "scheduled_start": record.get("scheduled_start"),
        "scheduled_end": record.get("scheduled_end"),
        "status": record.get("status"),
        "transcript_language": normalize_meeting_transcript_language(record.get("transcript_language")),
        "transcript_language_label": record.get("transcript_language_label") or "",
        "recording_started_at": record.get("recording_started_at"),
        "recording_stopped_at": record.get("recording_stopped_at"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "media": record.get("media") or {},
        "diagnostics_snapshot": record.get("diagnostics_snapshot") or {},
        "audio_preflight": record.get("audio_preflight") or {},
        "recording_health": record.get("recording_health") or {},
        "transcript_status": (record.get("transcript") or {}).get("status"),
        "transcript_quality": (record.get("transcript") or {}).get("quality") or {},
        "minutes_status": (record.get("minutes") or {}).get("status"),
        "email_status": (record.get("email") or {}).get("status"),
        "error": record.get("error") or "",
    }


def _meeting_recorder_diagnostics_payload(settings: Settings) -> dict[str, Any]:
    if _local_agent_meeting_recorder_enabled(settings):
        return _build_local_agent_client(settings).meeting_recorder_diagnostics()
    return _get_meeting_recorder_runtime().diagnostics()


def _meeting_recorder_record_summaries_for_current_user(settings: Settings) -> list[dict[str, Any]]:
    if _local_agent_meeting_recorder_enabled(settings):
        return _build_local_agent_client(settings).meeting_recorder_records(owner_email=_current_google_email())
    return [_meeting_record_summary(record) for record in _get_meeting_record_store().list_records(owner_email=_current_google_email())]


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
    if status_code == HTTPStatus.SERVICE_UNAVAILABLE:
        return {"error_category": "upstream_unavailable", "error_code": "service_unavailable"}
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


def _team_portal_data_root(settings: Settings) -> Path:
    data_root = settings.team_portal_data_dir
    if data_root.is_absolute():
        return data_root
    return (PROJECT_ROOT / data_root).resolve()


def _get_daily_brief_archive_store(settings: Settings) -> DailyBriefArchiveStore:
    return DailyBriefArchiveStore(daily_brief_archive_path(_team_portal_data_root(settings)))


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
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
    app.config["SECRET_KEY"] = settings.flask_secret_key
    app.config["SETTINGS"] = settings
    app.config["CONFIG_STORE"] = config_store
    app.config["BPMIS_PROJECT_STORE"] = BPMISProjectStore(config_store.db_path)
    app.config["TEAM_DASHBOARD_CONFIG_STORE"] = TeamDashboardConfigStore(config_store.db_path)
    app.config["WORK_MEMORY_STORE"] = WorkMemoryStore(data_root / "work_memory" / "memory.db")
    app.config["JOB_STORE"] = JobStore(data_root / "run" / "jobs.json")
    app.config["SOURCE_CODE_QA_QUERY_SCHEDULER"] = SourceCodeQAQueryScheduler(
        job_store=app.config["JOB_STORE"],
        max_running=settings.source_code_qa_codex_concurrency,
        default_runner=_run_source_code_qa_query_job,
    )
    app.config["SEATALK_TODO_STORE"] = SeaTalkTodoStore(data_root / "seatalk" / "completed_todos.json")
    app.config["SEATALK_NAME_MAPPING_STORE"] = SeaTalkNameMappingStore(data_root / "seatalk" / "name_overrides.json")
    app.config["SEATALK_DAILY_CACHE_DIR"] = data_root / "seatalk" / "cache"
    meeting_store = MeetingRecordStore(data_root / "meeting_records")
    app.config["MEETING_RECORD_STORE"] = meeting_store
    app.config["MEETING_RECORDER_RUNTIME"] = MeetingRecorderRuntime(
        store=meeting_store,
        config=_meeting_recorder_config(settings),
    )
    app.config["GOOGLE_CREDENTIAL_STORE"] = StoredGoogleCredentials(
        data_root / "google" / "credentials.json",
        encryption_key=settings.team_portal_config_encryption_key,
    )
    app.config["SOURCE_CODE_QA_SESSION_STORE"] = SourceCodeQASessionStore(data_root / "source_code_qa" / "sessions.json")
    app.config["SOURCE_CODE_QA_ATTACHMENT_STORE"] = SourceCodeQAAttachmentStore(data_root / "source_code_qa" / "attachments")
    app.config["SOURCE_CODE_QA_GENERATED_ARTIFACT_STORE"] = SourceCodeQAGeneratedArtifactStore(
        data_root / "source_code_qa" / "generated_artifacts"
    )
    app.config["SOURCE_CODE_QA_RUNTIME_EVIDENCE_STORE"] = SourceCodeQARuntimeEvidenceStore(
        data_root / "source_code_qa" / "runtime_evidence"
    )
    app.config["SOURCE_CODE_QA_MODEL_AVAILABILITY_STORE"] = SourceCodeQAModelAvailabilityStore(
        data_root / "source_code_qa" / "model_availability.json"
    )
    app.config["PRD_BRIEFING_STORE"] = BriefingStore(data_root / "prd_briefing")
    app.config["SOURCE_CODE_QA_SERVICE"] = build_source_code_qa_service_from_settings(settings)
    app.config["GET_USER_IDENTITY"] = lambda: _get_user_identity(settings)
    app.config["CAN_ACCESS_PRD_BRIEFING"] = lambda: _can_access_prd_briefing(settings)
    app.config["CAN_ACCESS_PRD_SELF_ASSESSMENT"] = lambda: _can_access_prd_self_assessment(settings)
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
                "can_access_prd_self_assessment": False,
                "can_access_gmail_seatalk_demo": False,
                "can_access_meeting_recorder": False,
                "can_access_source_code_qa": False,
                "can_manage_source_code_qa": False,
                "asset_revision": _current_release_revision(),
                "portal_stage": str(settings.team_portal_stage or "").strip().lower(),
            }
        user_identity = _get_user_identity(settings)
        can_access_team_dashboard = _can_access_team_dashboard(user_identity)
        site_tabs = []
        if _can_access_source_code_qa(settings):
            site_tabs.append(
                {
                    "label": "Source Code Q&A",
                    "href": url_for("source_code_qa"),
                    "active": request.path.startswith("/source-code-qa"),
                }
            )
        prd_tab = None
        prd_self_assessment_tab = None
        if _can_access_prd_self_assessment(settings):
            prd_self_assessment_tab = {
                "label": "PRD Self-Assessment",
                "href": url_for("prd_self_assessment_page"),
                "active": current_endpoint == "prd_self_assessment_page",
            }
        if _can_access_prd_briefing(settings):
            prd_tab = {
                "label": "PRD Briefing Tool",
                "href": url_for("prd_briefing.portal"),
                "active": current_endpoint.startswith("prd_briefing"),
            }
        if _can_access_meeting_recorder(settings):
            site_tabs.append(
                {
                    "label": "Meeting Recorder",
                    "href": url_for("meeting_recorder_page"),
                    "active": request.path.startswith("/meeting-recorder"),
                }
            )
        if prd_self_assessment_tab:
            site_tabs.append(prd_self_assessment_tab)
        if prd_tab:
            site_tabs.append(prd_tab)
        if can_access_team_dashboard:
            site_tabs.append(
                {
                    "label": "Team Dashboard",
                    "href": url_for("team_dashboard_page"),
                    "active": current_endpoint == "team_dashboard_page",
                }
            )
        site_tabs.append(
            {
                "label": "BPMIS Automation Tool",
                "href": url_for("index", workspace="run"),
                "active": current_endpoint == "index",
            }
        )
        if _can_access_work_memory(settings):
            site_tabs.append(
                {
                    "label": "AI Memory",
                    "href": url_for("work_memory_page"),
                    "active": request.path.startswith("/work-memory"),
                }
            )
        return {
            "site_tabs": site_tabs,
            "site_requires_google_login": _site_requires_google_login(settings),
            "can_access_prd_briefing": _can_access_prd_briefing(settings),
            "can_access_prd_self_assessment": _can_access_prd_self_assessment(settings),
            "can_access_gmail_seatalk_demo": _can_access_gmail_seatalk_demo(settings),
            "can_access_meeting_recorder": _can_access_meeting_recorder(settings),
            "can_access_source_code_qa": _can_access_source_code_qa(settings),
            "can_manage_source_code_qa": _can_manage_source_code_qa(settings),
            "asset_revision": _current_release_revision(),
            "portal_stage": str(settings.team_portal_stage or "").strip().lower(),
        }

    @app.before_request
    def enforce_team_access():
        g.request_id = uuid.uuid4().hex[:12]
        if request.path.rstrip("/") == "/healthz":
            return jsonify({"status": "ok", "revision": _current_release_revision()}), HTTPStatus.OK
        if request.path.startswith("/api/local-agent/"):
            return None
        if request.path.startswith("/uat-local-agent/"):
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
            "prd_briefing.image_proxy",
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
                level=(
                    logging.WARNING
                    if response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR
                    or status_details.get("error_category") == "upstream_unavailable"
                    else logging.ERROR
                ),
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

        if str(request.args.get("workspace") or "").strip() == "team-dashboard":
            return redirect(url_for("team_dashboard_page"))
        requested_workspace_tab = str(request.args.get("workspace") or "").strip()
        has_bpmis_return_state = any(key in session for key in ("default_workspace_tab", "last_results", "run_notice"))
        if (
            not requested_workspace_tab
            and not has_bpmis_return_state
            and _can_access_source_code_qa(settings)
        ):
            return redirect(url_for("source_code_qa"))

        results = _results_for_display(session.pop("last_results", []))
        run_notice = session.pop("run_notice", None)
        user_identity = _get_user_identity(settings)
        config_key = user_identity.get("config_key")
        raw_config_data = None
        effective_team_profiles = {team_key: dict(profile) for team_key, profile in TEAM_PROFILE_DEFAULTS.items()}
        try:
            raw_config_data = _load_user_config_for_identity(settings, user_identity) if config_key else None
            effective_team_profiles = _load_effective_team_profiles(config_store)
        except ToolError as error:
            if not _is_local_agent_unavailable_error(error):
                raise
            current_app.logger.warning(
                "Mac local-agent is unavailable while rendering index; falling back to empty setup/default profiles.",
                exc_info=True,
            )
            flash("Mac local-agent is not reachable right now. The portal is open, but local-agent backed data and actions are temporarily unavailable.", "warning")
        config_data = raw_config_data or config_store._normalize({})
        config_data = _hydrate_setup_defaults(config_data, user_identity, team_profiles=effective_team_profiles)
        _apply_sync_email_policy(config_data, user_identity)
        has_saved_config = bool(config_key and raw_config_data)
        default_workspace_tab = session.pop("default_workspace_tab", "run" if has_saved_config else "setup")
        allowed_workspace_tabs = {"setup", "run", "productization-upgrade-summary"}
        if _is_team_profile_admin(user_identity):
            allowed_workspace_tabs.add("team-default-admin")
        if requested_workspace_tab in allowed_workspace_tabs:
            default_workspace_tab = requested_workspace_tab

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
            default_workspace_tab=default_workspace_tab,
            input_headers=[],
            google_authorized=True,
        )

    @app.get("/team-dashboard")
    def team_dashboard_page():
        access_gate = _require_team_dashboard_access(settings)
        if access_gate is not None:
            return access_gate
        return render_template(
            "team_dashboard.html",
            page_title="Team Dashboard",
            user_identity=_get_user_identity(settings),
            team_dashboard_config=_get_team_dashboard_config_store().load(),
            can_manage_team_dashboard=_can_manage_team_dashboard(_get_user_identity(settings)),
            can_view_team_dashboard_monthly_report=_can_access_team_dashboard_monthly_report(
                _get_user_identity(settings)
            ),
            seatalk_configured=_seatalk_dashboard_is_configured(settings),
        )

    @app.get("/prd-self-assessment")
    @app.get("/prd-self-assessment/")
    def prd_self_assessment_page():
        access_gate = _require_prd_self_assessment_access(settings)
        if access_gate is not None:
            return access_gate
        return render_template(
            "prd_self_assessment.html",
            page_title="PRD Self-Assessment",
            user_identity=_get_user_identity(settings),
            review_url=url_for("prd_self_assessment_review_api"),
            summary_url=url_for("prd_self_assessment_summary_api"),
            asset_revision=_current_release_revision(),
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
            codex_service = _build_source_code_qa_service("codex_cli_bridge")
            model_availability = _source_code_qa_model_availability()
            return jsonify(
                {
                    "status": "ok",
                    "answer_mode": "auto",
                    "query_mode": "deep",
                    "can_manage": _can_manage_source_code_qa(settings),
                    "auth": _source_code_qa_auth_payload(settings),
                    "git_auth_ready": _source_code_qa_git_auth_ready(service, settings),
                    "llm_ready": service.llm_ready(),
                    "llm_provider": settings.source_code_qa_llm_provider,
                    "llm_providers": {
                        "codex_cli_bridge": {"ready": codex_service.llm_ready(), "label": "Codex", "available": model_availability["codex_cli_bridge"]},
                    },
                    "llm_model": service.llm_budgets["balanced"]["model"],
                    "llm_cheap_model": service.llm_budgets["cheap"]["model"],
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
            status_code = HTTPStatus.SERVICE_UNAVAILABLE if _is_local_agent_unavailable_error(error) else HTTPStatus.BAD_REQUEST
            return jsonify({"status": "error", "message": str(error), "error_category": _tool_error_category(error)}), status_code
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
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            payload = {}
        try:
            result = _build_source_code_qa_service().save_mapping(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
                repositories=payload.get("repositories") or [],
            )
            return jsonify({"status": "ok", **result})
        except ToolError as error:
            status_code = HTTPStatus.SERVICE_UNAVAILABLE if _is_local_agent_unavailable_error(error) else HTTPStatus.BAD_REQUEST
            return jsonify({"status": "error", "message": str(error), "error_category": _tool_error_category(error)}), status_code

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

    @app.post("/api/source-code-qa/attachments")
    def source_code_qa_attachments_api():
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email() or "local"
        session_id = str(request.form.get("session_id") or "").strip()
        if not session_id:
            return jsonify({"status": "error", "message": "A Source Code Q&A session is required before uploading attachments."}), HTTPStatus.BAD_REQUEST
        if _get_source_code_qa_session_store().get(session_id, owner_email=owner_email) is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        uploaded = request.files.get("file")
        if uploaded is None:
            return jsonify({"status": "error", "message": "Upload a file field named file."}), HTTPStatus.BAD_REQUEST
        try:
            content = uploaded.read()
            attachment = _get_source_code_qa_attachment_store().save_bytes(
                owner_email=owner_email,
                session_id=session_id,
                filename=uploaded.filename or "attachment",
                mime_type=uploaded.mimetype or "",
                content=content,
            )
            return jsonify({"status": "ok", "attachment": attachment})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.get("/api/source-code-qa/attachments/<attachment_id>")
    def source_code_qa_attachment_api(attachment_id: str):
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email() or "local"
        session_id = str(request.args.get("session_id") or "").strip()
        if not session_id:
            return jsonify({"status": "error", "message": "session_id is required."}), HTTPStatus.BAD_REQUEST
        if _get_source_code_qa_session_store().get(session_id, owner_email=owner_email) is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        try:
            metadata, content = _get_source_code_qa_attachment_store().get_bytes(
                owner_email=owner_email,
                session_id=session_id,
                attachment_id=attachment_id,
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.NOT_FOUND
        return send_file(
            io.BytesIO(content),
            mimetype=metadata.get("mime_type") or "application/octet-stream",
            download_name=metadata.get("filename") or "attachment",
            as_attachment=False,
        )

    @app.get("/api/source-code-qa/generated-artifacts/<artifact_id>")
    def source_code_qa_generated_artifact_api(artifact_id: str):
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email() or "local"
        session_id = str(request.args.get("session_id") or "").strip()
        if not session_id:
            return jsonify({"status": "error", "message": "session_id is required."}), HTTPStatus.BAD_REQUEST
        if _get_source_code_qa_session_store().get(session_id, owner_email=owner_email) is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        try:
            metadata, content = _get_source_code_qa_generated_artifact_store().get_bytes(
                owner_email=owner_email,
                session_id=session_id,
                artifact_id=artifact_id,
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.NOT_FOUND
        return send_file(
            io.BytesIO(content),
            mimetype=metadata.get("mime_type") or "application/zip",
            download_name=metadata.get("filename") or "source-code-qa-sql-package.zip",
            as_attachment=True,
        )

    @app.route("/api/source-code-qa/runtime-evidence", methods=["GET", "POST"])
    def source_code_qa_runtime_evidence_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        store = _get_source_code_qa_runtime_evidence_store()
        if request.method == "GET":
            try:
                evidence = store.list(
                    pm_team=str(request.args.get("pm_team") or ""),
                    country=str(request.args.get("country") or ""),
                )
                return jsonify({"status": "ok", "evidence": evidence})
            except ToolError as error:
                return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

        uploaded = request.files.get("file")
        if uploaded is None:
            return jsonify({"status": "error", "message": "Upload a file field named file."}), HTTPStatus.BAD_REQUEST
        try:
            evidence = store.save_bytes(
                pm_team=str(request.form.get("pm_team") or ""),
                country=str(request.form.get("country") or ""),
                source_type=str(request.form.get("source_type") or "other"),
                uploaded_by=_current_google_email() or "local",
                filename=uploaded.filename or "runtime-evidence",
                mime_type=uploaded.mimetype or "",
                content=uploaded.read(),
            )
            return jsonify({"status": "ok", "evidence": evidence, "items": store.list(pm_team=evidence["pm_team"], country=evidence["country"])})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.delete("/api/source-code-qa/runtime-evidence/<evidence_id>")
    def source_code_qa_runtime_evidence_delete_api(evidence_id: str):
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            pm_team = str(request.args.get("pm_team") or "")
            country = str(request.args.get("country") or "")
            deleted = _get_source_code_qa_runtime_evidence_store().delete(
                pm_team=pm_team,
                country=country,
                evidence_id=evidence_id,
            )
            return jsonify(
                {
                    "status": "ok",
                    "deleted": deleted,
                    "evidence": _get_source_code_qa_runtime_evidence_store().list(pm_team=pm_team, country=country),
                }
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

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
            async_payload["query_mode"] = _source_code_qa_query_mode(payload.get("query_mode"))
            owner_email = _current_google_email() or "local"
            async_payload["_session_owner_email"] = owner_email
            session_id = str(payload.get("session_id") or "").strip()
            if session_id:
                try:
                    attachments = _resolve_source_code_qa_query_attachments(payload, owner_email=owner_email, session_id=session_id)
                    session_payload = _get_source_code_qa_session_store().append_pending_question(
                        session_id,
                        owner_email=owner_email,
                        pm_team=str(payload.get("pm_team") or ""),
                        country=str(payload.get("country") or ""),
                        llm_provider=str(payload.get("llm_provider") or ""),
                        question=str(payload.get("question") or ""),
                        job_id=job.job_id,
                        attachments=attachments,
                    )
                    if session_payload is not None:
                        async_payload["_resolved_attachments"] = attachments
                except ToolError:
                    pass
            scheduler: SourceCodeQAQueryScheduler = current_app.config["SOURCE_CODE_QA_QUERY_SCHEDULER"]
            scheduler.submit(app=app_obj, job_id=job.job_id, payload=async_payload, owner_email=owner_email)
            snapshot = _public_source_code_qa_job_snapshot(job_store.snapshot(job.job_id) or {})
            return jsonify({**snapshot, "status": "queued", "job_id": job.job_id, "session_id": session_id})
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
            if isinstance(payload.get("_resolved_attachments"), list):
                attachments = payload.get("_resolved_attachments") or []
            else:
                attachments = _resolve_source_code_qa_query_attachments(payload, owner_email=owner_email, session_id=session_id)
            pm_team = str(payload.get("pm_team") or "")
            country = str(payload.get("country") or "")
            query_mode = _source_code_qa_query_mode(payload.get("query_mode"))
            runtime_evidence = _resolve_source_code_qa_runtime_evidence(pm_team=pm_team, country=country)
            auto_sync = _prepare_source_code_qa_auto_sync(service, pm_team=pm_team, country=country)
            def run_query() -> dict[str, Any]:
                return service.query(
                    pm_team=pm_team,
                    country=country,
                    question=str(payload.get("question") or ""),
                    answer_mode=_source_code_qa_public_answer_mode(payload.get("answer_mode")),
                    llm_budget_mode="auto",
                    query_mode=query_mode,
                    conversation_context=conversation_context,
                    attachments=attachments,
                    runtime_evidence=runtime_evidence,
                )

            if service.llm_provider_name == "codex_cli_bridge" and session_id:
                with _source_code_qa_codex_session_lock(session_id):
                    result = run_query()
            else:
                result = run_query()
            result["auto_sync"] = auto_sync
            result["attachments"] = _source_code_qa_public_attachments(attachments)
            result["runtime_evidence"] = _source_code_qa_public_runtime_evidence(runtime_evidence)
            if session_id:
                result["generated_artifacts"] = _build_source_code_qa_generated_artifacts(
                    owner_email=owner_email,
                    session_id=session_id,
                    pm_team=pm_team,
                    country=country,
                    question=str(payload.get("question") or ""),
                    result=result,
                    runtime_evidence=runtime_evidence,
                )
                session_payload = session_store.append_exchange(
                    session_id,
                    owner_email=owner_email,
                    pm_team=str(payload.get("pm_team") or ""),
                    country=str(payload.get("country") or ""),
                    llm_provider=str(payload.get("llm_provider") or ""),
                    question=str(payload.get("question") or ""),
                    result=result,
                    context=_build_source_code_qa_session_context(result, payload),
                    attachments=attachments,
                )
                if session_payload is not None:
                    result["session"] = session_payload
                    result["session_id"] = session_id
            _record_source_code_qa_work_memory(
                owner_email=owner_email,
                pm_team=pm_team,
                country=country,
                question=str(payload.get("question") or ""),
                result=result,
                session_id=session_id,
            )
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

    @app.post("/api/source-code-qa/effort-assessment")
    def source_code_qa_effort_assessment_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        requirement = str(payload.get("requirement") or "").strip()
        if not requirement:
            return jsonify({"status": "error", "message": "Business requirement is empty."}), HTTPStatus.BAD_REQUEST
        if not _source_code_qa_provider_available(payload.get("llm_provider")):
            return jsonify({"status": "error", "message": "Selected Source Code Q&A model is unavailable."}), HTTPStatus.BAD_REQUEST

        job_store: JobStore = current_app.config["JOB_STORE"]
        job = job_store.create("source-code-qa-effort-assessment", title="Source Code Q&A Effort Assessment")
        app_obj = current_app._get_current_object()
        assessment_payload = {
            "pm_team": str(payload.get("pm_team") or ""),
            "country": str(payload.get("country") or ""),
            "language": _source_code_qa_effort_assessment_language(payload.get("language")),
            "requirement": requirement,
            "llm_provider": str(payload.get("llm_provider") or ""),
            "answer_mode": "auto",
            "query_mode": "deep",
            "_session_owner_email": _current_google_email() or "local",
        }
        scheduler: SourceCodeQAQueryScheduler = current_app.config["SOURCE_CODE_QA_QUERY_SCHEDULER"]
        scheduler.submit(
            app=app_obj,
            job_id=job.job_id,
            payload=assessment_payload,
            owner_email=assessment_payload["_session_owner_email"],
            runner=_run_source_code_qa_effort_assessment_job,
        )
        snapshot = _public_source_code_qa_job_snapshot(job_store.snapshot(job.job_id) or {})
        return jsonify({**snapshot, "status": "queued", "job_id": job.job_id})

    @app.get("/api/source-code-qa/effort-assessment/latest")
    def source_code_qa_effort_assessment_latest_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        result = current_app.config["JOB_STORE"].latest_completed_result("source-code-qa-effort-assessment")
        if not result:
            return jsonify({"status": "empty", "result": {}})
        return jsonify({"status": "ok", "result": result})

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

    @app.get("/work-memory")
    def work_memory_page():
        access_gate = _require_work_memory_access(settings)
        if access_gate is not None:
            return access_gate
        return render_template("work_memory.html", page_title="AI Memory")

    @app.get("/api/work-memory/health")
    def work_memory_health_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_work_memory_enabled(settings):
            return jsonify(_build_local_agent_client(settings).work_memory_health())
        return jsonify(_get_work_memory_store().health())

    @app.get("/api/work-memory/recent")
    def work_memory_recent_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        query_args = {
            "owner_email": _current_google_email(),
            "visibility_scope": str(request.args.get("scope") or "owner").strip().lower() or "owner",
            "query": str(request.args.get("q") or ""),
            "filters": {
                "source_type": str(request.args.get("source_type") or "").strip(),
                "item_type": str(request.args.get("item_type") or "").strip(),
            },
            "limit": int(request.args.get("limit") or 50),
        }
        if _local_agent_work_memory_enabled(settings):
            items = _build_local_agent_client(settings).work_memory_recent(**query_args)
        else:
            items = _get_work_memory_store().query_work_memory(**query_args)
        return jsonify({"status": "ok", "items": items})

    @app.get("/api/work-memory/review-candidates")
    def work_memory_review_candidates_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_work_memory_enabled(settings):
            items = _build_local_agent_client(settings).work_memory_review_candidates(owner_email=_current_google_email(), limit=int(request.args.get("limit") or 50))
        else:
            items = _get_work_memory_store().review_candidates(owner_email=_current_google_email(), limit=int(request.args.get("limit") or 50))
        return jsonify({"status": "ok", "items": items})

    @app.get("/api/work-memory/project-timeline")
    def work_memory_project_timeline_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        project_ref = str(request.args.get("project_ref") or request.args.get("q") or "").strip()
        query_args = {
            "project_ref": project_ref,
            "owner_email": _current_google_email(),
            "visibility_scope": str(request.args.get("scope") or "owner").strip().lower() or "owner",
            "limit": int(request.args.get("limit") or 100),
        }
        if _local_agent_work_memory_enabled(settings):
            items = _build_local_agent_client(settings).work_memory_project_timeline(**query_args)
        else:
            items = _get_work_memory_store().project_timeline(**query_args)
        return jsonify({"status": "ok", "items": items})

    @app.get("/api/work-memory/entity-resolution")
    def work_memory_entity_resolution_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        query_args = {
            "query": str(request.args.get("q") or request.args.get("query") or "").strip(),
            "owner_email": _current_google_email(),
            "entity_type": str(request.args.get("entity_type") or "").strip(),
        }
        if _local_agent_work_memory_enabled(settings):
            result = _build_local_agent_client(settings).work_memory_entity_resolution(**query_args)
        else:
            result = _get_work_memory_store().resolve_work_entity(**query_args)
        return jsonify(result)

    @app.post("/api/work-memory/feedback")
    def work_memory_feedback_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            feedback_args = {
                "item_id": str(payload.get("item_id") or "").strip(),
                "action": str(payload.get("action") or "").strip(),
                "owner_email": _current_google_email(),
                "correction_text": str(payload.get("correction_text") or "").strip(),
                "visibility_override": str(payload.get("visibility_override") or "").strip(),
                "reason": str(payload.get("reason") or "").strip(),
            }
            if _local_agent_work_memory_enabled(settings):
                result = _build_local_agent_client(settings).work_memory_feedback(**feedback_args)
            else:
                result = _get_work_memory_store().record_memory_feedback(**feedback_args)
            return jsonify(result)
        except (KeyError, ValueError) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/work-memory/ingest-sent-monthly-reports")
    def work_memory_ingest_sent_monthly_reports_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            result = _ingest_sent_monthly_reports_from_gmail(settings)
            return jsonify({"status": "ok", **result})
        except (ConfigError, ToolError) as error:
            return jsonify({"status": "error", "message": str(error), **_classify_portal_error(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/work-memory/backfill-existing")
    def work_memory_backfill_existing_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        backfill_args = {
            "owner_email": _current_google_email(),
            "date_range": str(payload.get("date_range") or "90d").strip() or "90d",
            "sources": [str(item or "").strip() for item in payload.get("sources") or [] if str(item or "").strip()] if isinstance(payload.get("sources"), list) else [],
        }
        if _local_agent_work_memory_enabled(settings):
            result = _build_local_agent_client(settings).work_memory_backfill_existing(**backfill_args)
        else:
            result = _ingest_existing_work_memory_sources(settings, date_range=backfill_args["date_range"], sources=backfill_args["sources"])
        return jsonify({"status": "ok", **result})

    @app.post("/api/work-memory/distill")
    def work_memory_distill_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        distill_args = {
            "owner_email": _current_google_email(),
            "date_range": str(payload.get("date_range") or "90d").strip() or "90d",
            "sources": [str(item or "").strip() for item in payload.get("sources") or [] if str(item or "").strip()] if isinstance(payload.get("sources"), list) else [],
            "project_refs": [str(item or "").strip() for item in payload.get("project_refs") or [] if str(item or "").strip()] if isinstance(payload.get("project_refs"), list) else [],
        }
        if _local_agent_work_memory_enabled(settings):
            result = _build_local_agent_client(settings).work_memory_distill(**distill_args)
        else:
            result = _get_work_memory_store().distill_work_memory(**distill_args)
        return jsonify({"status": "ok", **result})

    @app.post("/api/work-memory/ingest-incremental")
    def work_memory_ingest_incremental_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            if _local_agent_work_memory_enabled(settings):
                result = _build_local_agent_client(settings).work_memory_ingest_incremental(
                    owner_email=_current_google_email(),
                    window=str(payload.get("window") or "7d").strip() or "7d",
                    reconciliation=bool(payload.get("reconciliation")),
                )
            else:
                result = _run_incremental_memory_ingestion(
                    settings,
                    window=str(payload.get("window") or "7d").strip() or "7d",
                    reconciliation=bool(payload.get("reconciliation")),
                )
            return jsonify({"status": "ok", **result})
        except (ConfigError, ToolError) as error:
            return jsonify({"status": "error", "message": str(error), **_classify_portal_error(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/work-memory/backfill-gmail")
    def work_memory_backfill_gmail_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return jsonify(
                {
                    "status": "error",
                    "message": "Gmail read permission is missing. Reconnect Google once to grant gmail.readonly.",
                }
            ), HTTPStatus.BAD_REQUEST
        payload = request.get_json(silent=True) or {}
        owner_email = _current_google_email()
        job_store: JobStore = current_app.config["JOB_STORE"]
        active = job_store.active_for_record(WORK_MEMORY_GMAIL_BACKFILL_ACTION, owner_email=owner_email, record_id=owner_email)
        if active:
            return jsonify({**active, "status": "queued" if active.get("state") == "queued" else "running"}), HTTPStatus.ACCEPTED
        try:
            days = max(1, min(int(payload.get("days") or 90), 365))
            max_messages_raw = payload.get("max_messages")
            max_messages = None
            if max_messages_raw is not None and str(max_messages_raw).strip():
                max_messages = max(1, min(int(max_messages_raw), 10000))
        except (TypeError, ValueError):
            return jsonify({"status": "error", "message": "days and max_messages must be numbers."}), HTTPStatus.BAD_REQUEST
        job = job_store.create(WORK_MEMORY_GMAIL_BACKFILL_ACTION, "Gmail Work Memory Backfill", owner_email=owner_email, record_id=owner_email)
        app_obj = current_app._get_current_object()
        runner_payload = {
            "owner_email": owner_email,
            "days": days,
            "max_messages": max_messages,
            "credentials": dict(session.get("google_credentials") or {}),
            "report_intelligence_config": _get_team_dashboard_config_store().load().get("report_intelligence_config") or {},
            "drive_read_enabled": _google_credentials_have_scopes(GOOGLE_DRIVE_READONLY_SCOPE),
        }
        threading.Thread(target=_run_work_memory_gmail_backfill_job, args=(app_obj, job.job_id, runner_payload), daemon=True).start()
        snapshot = job_store.snapshot(job.job_id) or asdict(job)
        return jsonify({**snapshot, "status": "queued", "job_id": job.job_id}), HTTPStatus.ACCEPTED

    @app.get("/api/work-memory/ingestion-jobs")
    def work_memory_ingestion_jobs_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            limit = max(1, min(int(request.args.get("limit") or 20), 100))
        except (TypeError, ValueError):
            return jsonify({"status": "error", "message": "limit must be a number."}), HTTPStatus.BAD_REQUEST
        snapshots = current_app.config["JOB_STORE"].list_snapshots(
            action=WORK_MEMORY_GMAIL_BACKFILL_ACTION,
            owner_email=_current_google_email(),
            limit=limit,
        )
        return jsonify({"status": "ok", "items": snapshots})

    @app.get("/api/superagent/health")
    def superagent_health_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_work_memory_enabled(settings):
            return jsonify(_build_local_agent_client(settings).superagent_health(owner_email=_current_google_email()))
        return jsonify(_get_work_memory_store().superagent_health(owner_email=_current_google_email()))

    @app.post("/api/superagent/query")
    def superagent_query_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        task_type = str(payload.get("task_type") or "general").strip() or "general"
        query_text = str(payload.get("query") or "").strip()
        if _local_agent_work_memory_enabled(settings):
            return jsonify(
                _build_local_agent_client(settings).superagent_query(
                    owner_email=_current_google_email(),
                    user_email=_current_google_email(),
                    query=query_text,
                    task_type=task_type,
                    visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
                    limit=int(payload.get("limit") or 12),
                )
            )
        context = _get_work_memory_store().query_superagent_context(
            query=query_text,
            owner_email=_current_google_email(),
            visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
            task_type=task_type,
            limit=int(payload.get("limit") or 12),
        )
        result = _get_work_memory_store().generate_llm_superagent_answer(task_type=task_type, query=query_text, context=context)
        audit = _get_work_memory_store().record_superagent_audit_log(
            owner_email=_current_google_email(),
            user_email=_current_google_email(),
            query=query_text,
            task_type=task_type,
            visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
            context=context,
            answer=result,
            metadata={"route": "/api/superagent/query"},
        )
        return jsonify({"status": "ok", "context": context, "audit": audit, **result})

    @app.post("/api/superagent/explain")
    def superagent_explain_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        if _local_agent_work_memory_enabled(settings):
            return jsonify(
                _build_local_agent_client(settings).superagent_explain(
                    owner_email=_current_google_email(),
                    query=str(payload.get("query") or "").strip(),
                    task_type=str(payload.get("task_type") or "general").strip() or "general",
                    visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
                )
            )
        result = _get_work_memory_store().explain_superagent_answer(
            owner_email=_current_google_email(),
            query=str(payload.get("query") or "").strip(),
            task_type=str(payload.get("task_type") or "general").strip() or "general",
            visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
        )
        return jsonify(result)

    @app.post("/api/superagent/eval")
    def superagent_eval_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        if _local_agent_work_memory_enabled(settings):
            return jsonify(
                _build_local_agent_client(settings).superagent_eval(
                    owner_email=_current_google_email(),
                    cases=payload.get("cases") if isinstance(payload.get("cases"), list) else None,
                    limit=int(payload.get("limit") or 30),
                    suite_id=str(request.args.get("suite_id") or payload.get("suite_id") or "").strip(),
                )
            )
        result = _get_work_memory_store().run_superagent_eval_cases(
            owner_email=_current_google_email(),
            cases=payload.get("cases") if isinstance(payload.get("cases"), list) else None,
            limit=int(payload.get("limit") or 30),
            suite_id=str(request.args.get("suite_id") or payload.get("suite_id") or "").strip(),
        )
        return jsonify(result)

    @app.post("/api/superagent/quality-gate")
    def superagent_quality_gate_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        suite_id = str(request.args.get("suite_id") or payload.get("suite_id") or "gold_v1").strip() or "gold_v1"
        limit = int(payload.get("limit") or 30)
        min_cases = int(payload.get("min_cases") or 1)
        if _local_agent_work_memory_enabled(settings):
            return jsonify(
                _build_local_agent_client(settings).superagent_quality_gate(
                    owner_email=_current_google_email(),
                    suite_id=suite_id,
                    limit=limit,
                    min_cases=min_cases,
                )
            )
        return jsonify(
            _get_work_memory_store().run_superagent_quality_gate(
                owner_email=_current_google_email(),
                suite_id=suite_id,
                limit=limit,
                min_cases=min_cases,
            )
        )

    @app.get("/api/superagent/audit")
    def superagent_audit_api():
        access_gate = _require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_work_memory_enabled(settings):
            return jsonify(
                {
                    "status": "ok",
                    "items": _build_local_agent_client(settings).superagent_audit(
                        owner_email=_current_google_email(),
                        limit=int(request.args.get("limit") or 50),
                    ),
                }
            )
        return jsonify(
            {
                "status": "ok",
                "items": _get_work_memory_store().superagent_audit_log(
                    owner_email=_current_google_email(),
                    limit=int(request.args.get("limit") or 50),
                ),
            }
        )

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
        return redirect(url_for("team_dashboard_page", tab="seatalk-name-mapping"))

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
            force_refresh = str(request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes", "on"}
            candidates = _build_seatalk_dashboard_service(settings).build_name_mappings(force_refresh=force_refresh)
            mappings = mapping_store.mappings()
            mapped_keys = {alias for key in mappings for alias in SeaTalkNameMappingStore.equivalent_keys(key)}
            candidates = dict(candidates)
            candidates["unknown_ids"] = _dedupe_seatalk_name_mapping_candidates([
                row for row in (candidates.get("unknown_ids") or [])
                if isinstance(row, dict) and not (SeaTalkNameMappingStore.equivalent_keys(row.get("id")) & mapped_keys)
            ])
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

    @app.get("/meeting-recorder")
    def meeting_recorder_page():
        access_gate = _require_meeting_recorder_access(settings)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        return render_template(
            "meeting_recorder.html",
            page_title="Meeting Recorder",
            user_identity=user_identity,
            calendar_connected=_google_credentials_have_scopes(CALENDAR_READONLY_SCOPE),
            gmail_send_connected=_google_credentials_have_scopes("https://www.googleapis.com/auth/gmail.send"),
            selected_record_id=str(request.args.get("record") or "").strip(),
            asset_revision=_current_release_revision(),
        )

    @app.get("/api/meeting-recorder/diagnostics")
    def meeting_recorder_diagnostics_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_meeting_recorder_enabled(settings):
            try:
                return jsonify({"status": "ok", **_build_local_agent_client(settings).meeting_recorder_diagnostics()})
            except ToolError as error:
                return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_GATEWAY
        return jsonify({"status": "ok", **_get_meeting_recorder_runtime().diagnostics()})

    @app.get("/api/meeting-recorder/calendar/upcoming")
    def meeting_recorder_upcoming_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(CALENDAR_READONLY_SCOPE):
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Google Calendar access is not available yet. Sign in with Google again to grant calendar read access.",
                    }
                ),
                HTTPStatus.BAD_REQUEST,
            )
        try:
            meetings = _build_calendar_meeting_service().upcoming_meetings()
            return jsonify({"status": "ok", "meetings": meetings[:MEETING_RECORDER_UPCOMING_DISPLAY_LIMIT]})
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "meeting_recorder_calendar_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=_classify_portal_error(error)),
            )
            current_app.logger.exception("Meeting Recorder calendar load failed.")
            return jsonify({"status": "error", "message": "Upcoming meetings could not be loaded right now."}), HTTPStatus.INTERNAL_SERVER_ERROR

    @app.get("/api/meeting-recorder/records")
    def meeting_recorder_records_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_meeting_recorder_enabled(settings):
            try:
                records = _build_local_agent_client(settings).meeting_recorder_records(owner_email=_current_google_email())
                return jsonify({"status": "ok", "records": records})
            except ToolError as error:
                return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_GATEWAY
        records = _get_meeting_record_store().list_records(owner_email=_current_google_email())
        return jsonify({"status": "ok", "records": [_meeting_record_summary(record) for record in records]})

    @app.get("/api/meeting-recorder/records/<record_id>")
    def meeting_recorder_record_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                record_payload = _build_local_agent_client(settings).meeting_recorder_record(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                )
                return jsonify({"status": "ok", "record": record_payload.get("record") or {}})
            record = _get_meeting_record_store().get_record(record_id)
            if str(record.get("owner_email") or "").strip().lower() != _current_google_email():
                return jsonify({"status": "error", "message": "Meeting record is not available for this Google account."}), HTTPStatus.FORBIDDEN
            return jsonify({"status": "ok", "record": record})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/meeting-recorder/start")
    def meeting_recorder_start_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            meeting_link = str(payload.get("meeting_link") or payload.get("meetingLink") or "").strip()
            recording_mode = str(payload.get("recording_mode") or payload.get("recordingMode") or "").strip()
            transcript_language = normalize_meeting_transcript_language(payload.get("transcript_language") or payload.get("transcriptLanguage"))
            if not recording_mode:
                recording_mode = "audio_only"
            if _local_agent_meeting_recorder_enabled(settings):
                remote_payload = dict(payload)
                remote_payload.update(
                    {
                        "owner_email": _current_google_email(),
                        "meeting_link": meeting_link,
                        "recording_mode": recording_mode,
                        "transcript_language": transcript_language,
                        "platform": str(payload.get("platform") or meeting_platform_from_link(meeting_link) or "unknown").strip(),
                    }
                )
                result = _build_local_agent_client(settings).meeting_recorder_start(remote_payload)
                return jsonify({"status": "ok", "record": result.get("record") or {}})
            record = _get_meeting_recorder_runtime().start_recording(
                owner_email=_current_google_email(),
                title=str(payload.get("title") or "Untitled meeting").strip(),
                platform=str(payload.get("platform") or meeting_platform_from_link(meeting_link) or "unknown").strip(),
                meeting_link=meeting_link,
                recording_mode=recording_mode,
                calendar_event_id=str(payload.get("calendar_event_id") or payload.get("calendarEventId") or "").strip(),
                scheduled_start=str(payload.get("scheduled_start") or payload.get("scheduledStart") or "").strip(),
                scheduled_end=str(payload.get("scheduled_end") or payload.get("scheduledEnd") or "").strip(),
                attendees=payload.get("attendees") if isinstance(payload.get("attendees"), list) else [],
                transcript_language=transcript_language,
            )
            return jsonify({"status": "ok", "record": _meeting_record_summary(record)})
        except (ConfigError, ToolError) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/meeting-recorder/records/<record_id>/stop")
    def meeting_recorder_stop_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email()
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                result = _build_local_agent_client(settings).meeting_recorder_stop(
                    record_id=record_id,
                    owner_email=owner_email,
                )
                process_payload = _meeting_recorder_auto_process_payload(
                    settings=settings,
                    record_id=record_id,
                    owner_email=owner_email,
                )
                return jsonify({"status": "ok", "record": result.get("record") or {}, **process_payload})
            record = _get_meeting_recorder_runtime().stop_recording(record_id=record_id, owner_email=owner_email)
            process_payload = _meeting_recorder_auto_process_payload(
                settings=settings,
                record_id=record_id,
                owner_email=owner_email,
            )
            return jsonify({"status": "ok", "record": _meeting_record_summary(record), **process_payload})
        except (ToolError, requests.RequestException) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/meeting-recorder/records/<record_id>/signal-check")
    def meeting_recorder_signal_check_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                result = _build_local_agent_client(settings).meeting_recorder_signal_check(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                )
                return jsonify({"status": "ok", "record": result.get("record") or {}})
            record = _get_meeting_recorder_runtime().check_recording_signal(record_id=record_id, owner_email=_current_google_email())
            return jsonify({"status": "ok", "record": _meeting_record_summary(record)})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/meeting-recorder/records/<record_id>/process")
    def meeting_recorder_process_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                result = _build_local_agent_client(settings).meeting_recorder_process_start(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                )
                return jsonify({
                    "status": "queued",
                    "state": result.get("state") or "queued",
                    "job_id": result.get("job_id") or "",
                    "record": result.get("record") or {},
                })
            payload = _queue_meeting_recorder_process_job(
                app=current_app._get_current_object(),
                settings=settings,
                record_id=record_id,
                owner_email=_current_google_email(),
            )
            return jsonify(payload)
        except (ConfigError, ToolError, requests.RequestException) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.get("/api/meeting-recorder/process-jobs/<job_id>")
    def meeting_recorder_process_job_api(job_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                snapshot = _build_local_agent_client(settings).meeting_recorder_process_job(
                    job_id=job_id,
                    owner_email=_current_google_email(),
                )
                return jsonify(_public_meeting_recorder_process_job_snapshot(snapshot))
            snapshot = _meeting_recorder_process_job_snapshot_for_current_user(job_id)
            if snapshot is None:
                return jsonify({
                    "status": "error",
                    "message": "Meeting Recorder process job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }), HTTPStatus.NOT_FOUND
            return jsonify(_public_meeting_recorder_process_job_snapshot(snapshot))
        except (ConfigError, ToolError, requests.RequestException) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/meeting-recorder/records/<record_id>/send-email")
    def meeting_recorder_send_email_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                result = _build_local_agent_client(settings).meeting_recorder_send_email(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                    recipient=str(payload.get("recipient") or "").strip() or _current_google_email(),
                )
                return jsonify({"status": "ok", "email": result.get("email") or {}})
            email_payload = _build_meeting_processing_service(settings).send_minutes_email(
                record_id=record_id,
                owner_email=_current_google_email(),
                recipient=str(payload.get("recipient") or "").strip() or _current_google_email(),
            )
            return jsonify({"status": "ok", "email": email_payload})
        except (ConfigError, ToolError) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.delete("/api/meeting-recorder/records/<record_id>")
    def meeting_recorder_delete_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                _build_local_agent_client(settings).meeting_recorder_delete(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                )
                return jsonify({"status": "ok"})
            _get_meeting_record_store().delete_record(record_id=record_id, owner_email=_current_google_email())
            return jsonify({"status": "ok"})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.route("/meeting-recorder/assets/<record_id>/<path:relative_path>", methods=["GET", "HEAD"])
    def meeting_recorder_asset(record_id: str, relative_path: str):
        access_gate = _require_meeting_recorder_access(settings)
        if access_gate is not None:
            return access_gate
        as_download = str(request.args.get("download") or "").strip().lower() in {"1", "true", "yes"}
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                upstream = _build_local_agent_client(settings).meeting_recorder_asset_response(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                    relative_path=relative_path,
                    range_header=str(request.headers.get("Range") or ""),
                    method=request.method,
                    download=as_download,
                )
                content_type = str(upstream.headers.get("Content-Type") or "")
                if as_download and "text/html" in content_type.lower():
                    upstream.close()
                    return jsonify({
                        "status": "error",
                        "message": "Meeting audio download returned an HTML response instead of the requested file. Refresh the page and sign in again, then retry.",
                    }), HTTPStatus.BAD_GATEWAY
                excluded_headers = {"content-encoding", "connection", "transfer-encoding"}
                headers = [
                    (key, value)
                    for key, value in upstream.headers.items()
                    if key.lower() not in excluded_headers and (not as_download or key.lower() != "content-disposition")
                ]
                if as_download:
                    filename = Path(upstream.headers.get("X-Meeting-Recorder-Filename") or relative_path).name or "meeting-recording.mp4"
                    headers.append(("Content-Disposition", f'attachment; filename="{filename}"'))
                if request.method == "HEAD":
                    upstream.close()
                    return Response(status=upstream.status_code, headers=headers)

                def stream_upstream():
                    try:
                        for chunk in upstream.iter_content(chunk_size=1024 * 256):
                            if chunk:
                                yield chunk
                    finally:
                        upstream.close()

                return Response(stream_upstream(), status=upstream.status_code, headers=headers, direct_passthrough=True)
            record = _get_meeting_record_store().get_record(record_id)
            if str(record.get("owner_email") or "").strip().lower() != _current_google_email():
                return jsonify({"status": "error", "message": "Meeting record is not available for this Google account."}), HTTPStatus.FORBIDDEN
            root_dir = _get_meeting_record_store().record_dir(record_id).resolve()
            asset_path = (root_dir / relative_path).resolve()
            if root_dir not in asset_path.parents and asset_path != root_dir:
                return jsonify({"status": "error", "message": "Invalid meeting asset path."}), HTTPStatus.BAD_REQUEST
            if not asset_path.exists():
                return jsonify({"status": "error", "message": "Meeting asset not found."}), HTTPStatus.NOT_FOUND
            media = record.get("media") if isinstance(record.get("media"), dict) else {}
            active_media_paths = [str(media.get("audio_path") or "").strip(), str(media.get("video_path") or "").strip()]
            active_asset_paths = {
                (_get_meeting_record_store().root_dir / media_path).resolve()
                for media_path in active_media_paths
                if media_path
            }
            active_asset_paths.update(
                (root_dir / Path(media_path).name).resolve()
                for media_path in active_media_paths
                if media_path
            )
            if str(record.get("status") or "") == "recording" and asset_path in active_asset_paths:
                return jsonify({
                    "status": "error",
                    "message": "Stop the recording before downloading the meeting media file.",
                }), HTTPStatus.CONFLICT
            return send_file(asset_path, conditional=True, as_attachment=as_download, download_name=asset_path.name)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

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
                "spreadsheet_link": "",
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
            flash("Your web Jira config was saved for this user and will be used for BPMIS Projects.", "success")
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

    @app.get("/api/team-dashboard/config")
    def team_dashboard_config():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        return jsonify({"status": "ok", "config": _get_team_dashboard_config_store().load()})

    @app.post("/admin/team-dashboard/members")
    def save_team_dashboard_members():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _can_manage_team_dashboard(_get_user_identity(settings)):
            return jsonify({"status": "error", "message": "Team Dashboard admin access is restricted."}), HTTPStatus.FORBIDDEN
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            payload = {
                "teams": {
                    team_key: {"member_emails": request.form.get(f"team_dashboard_members_{team_key}", "")}
                    for team_key in TEAM_DASHBOARD_TEAMS
                }
            }
        store = _get_team_dashboard_config_store()
        existing_config = store.load()
        if isinstance(existing_config.get("key_project_overrides"), dict):
            payload["key_project_overrides"] = existing_config["key_project_overrides"]
        if isinstance(existing_config.get("task_cache"), dict):
            payload["task_cache"] = existing_config["task_cache"]
        payload["monthly_report_template"] = existing_config.get("monthly_report_template") or DEFAULT_MONTHLY_REPORT_TEMPLATE
        payload["report_intelligence_config"] = existing_config.get("report_intelligence_config") or normalize_report_intelligence_config({})
        saved = store.save(payload)
        _log_portal_event(
            "team_dashboard_members_save_success",
            **_build_request_log_context(
                settings,
                user_identity=_get_user_identity(settings),
                extra={
                    "team_counts": {
                        team_key: len(team.get("member_emails") or [])
                        for team_key, team in (saved.get("teams") or {}).items()
                        if isinstance(team, dict)
                    }
                },
            ),
        )
        return jsonify({"status": "ok", "config": saved})

    @app.get("/api/team-dashboard/monthly-report/template")
    def team_dashboard_monthly_report_template():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        config = _get_team_dashboard_config_store().load()
        return jsonify(
            {
                "status": "ok",
                "template": normalize_monthly_report_template(config.get("monthly_report_template")),
                "subject": monthly_report_subject(),
                "recipient": DEFAULT_MONTHLY_REPORT_RECIPIENT,
            }
        )

    @app.get("/api/team-dashboard/monthly-report/latest-draft")
    def team_dashboard_monthly_report_latest_draft():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _remote_bpmis_config_enabled(settings):
            return jsonify(_build_local_agent_client(settings).team_dashboard_monthly_report_latest_draft())
        result = current_app.config["JOB_STORE"].latest_completed_result("team-dashboard-monthly-report-draft")
        draft_markdown = str((result or {}).get("draft_markdown") or "").strip()
        if not draft_markdown:
            return jsonify({"status": "empty", "draft_markdown": ""})
        generation_summary = (result or {}).get("generation_summary") if isinstance((result or {}).get("generation_summary"), dict) else {}
        generation_version = str((result or {}).get("generation_version") or generation_summary.get("generation_version") or "").strip()
        if not generation_version and not generation_summary.get("period_start"):
            return jsonify({"status": "empty", "draft_markdown": "", "message": "Latest Monthly Report draft was generated by an older format."})
        subject = str((result or {}).get("subject") or "").strip() or monthly_report_subject()
        return jsonify(
            {
                "status": "ok",
                "draft_markdown": draft_markdown,
                "subject": subject,
                "job_id": (result or {}).get("job_id") or "",
                "generated_at": (result or {}).get("generated_at") or 0,
                "generation_version": generation_version,
                "period_start": generation_summary.get("period_start") or "",
                "period_end": generation_summary.get("period_end") or "",
                "period_end_exclusive": generation_summary.get("period_end_exclusive") or "",
            }
        )

    @app.get("/api/team-dashboard/daily-briefs")
    def team_dashboard_daily_briefs():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_seatalk_enabled(settings):
            briefs = _build_local_agent_client(settings).team_dashboard_daily_briefs()
            return jsonify(
                {
                    "status": "ok",
                    "briefs": [
                        {
                            "brief_id": item.get("brief_id") or "",
                            "time_period": item.get("time_period") or "",
                            "subject": item.get("subject") or "",
                            "message_id": item.get("message_id") or "",
                            "generated_at": item.get("generated_at") or item.get("sent_at") or "",
                            "download_url": url_for("team_dashboard_daily_brief_download", brief_id=item.get("brief_id") or ""),
                        }
                        for item in briefs
                        if item.get("brief_id")
                    ],
                }
            )
        briefs = _get_daily_brief_archive_store(settings).list_recent(limit=30)
        return jsonify(
            {
                "status": "ok",
                "briefs": [
                    {
                        "brief_id": item.get("brief_id") or "",
                        "time_period": item.get("time_period") or "",
                        "subject": item.get("subject") or "",
                        "message_id": item.get("message_id") or "",
                        "generated_at": item.get("sent_at") or "",
                        "download_url": url_for("team_dashboard_daily_brief_download", brief_id=item.get("brief_id") or ""),
                    }
                    for item in briefs
                    if item.get("brief_id")
                ],
            }
        )

    @app.get("/api/team-dashboard/daily-briefs/<brief_id>/download")
    def team_dashboard_daily_brief_download(brief_id: str):
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_seatalk_enabled(settings):
            response = _build_local_agent_client(settings).team_dashboard_daily_brief_download(brief_id)
            headers = []
            content_disposition = response.headers.get("Content-Disposition")
            if content_disposition:
                headers.append(("Content-Disposition", content_disposition))
            return current_app.response_class(
                response.content,
                status=response.status_code,
                headers=headers,
                mimetype=response.headers.get("Content-Type") or "application/pdf",
            )
        item = _get_daily_brief_archive_store(settings).get(brief_id)
        if item is None:
            return jsonify({"status": "error", "message": "Daily Brief was not found."}), HTTPStatus.NOT_FOUND
        pdf_bytes = daily_brief_pdf_bytes(
            title=str(item.get("subject") or "Daily Brief"),
            body=str(item.get("text_body") or ""),
            html_body=str(item.get("html_body") or ""),
        )
        run_date = re.sub(r"[^0-9-]", "", str(item.get("run_date") or "daily-brief")) or "daily-brief"
        run_slot = re.sub(r"[^a-z0-9_-]", "-", str(item.get("run_slot") or "daily").lower()) or "daily"
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"daily-brief-{run_date}-{run_slot}.pdf",
        )

    @app.post("/admin/team-dashboard/monthly-report-template")
    def save_team_dashboard_monthly_report_template():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        if not _can_manage_team_dashboard(user_identity):
            return jsonify({"status": "error", "message": "Team Dashboard admin access is restricted."}), HTTPStatus.FORBIDDEN
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            payload = {"template": request.form.get("monthly_report_template", "")}
        store = _get_team_dashboard_config_store()
        config = store.load()
        config["monthly_report_template"] = normalize_monthly_report_template(payload.get("template"))
        saved = store.save(config)
        _log_portal_event(
            "team_dashboard_monthly_report_template_save_success",
            **_build_request_log_context(
                settings,
                user_identity=user_identity,
                extra={"template_chars": len(str(saved.get("monthly_report_template") or ""))},
            ),
        )
        return jsonify({"status": "ok", "template": saved.get("monthly_report_template") or DEFAULT_MONTHLY_REPORT_TEMPLATE})

    @app.post("/admin/team-dashboard/report-intelligence")
    def save_team_dashboard_report_intelligence():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        if not _can_manage_team_dashboard(user_identity):
            return jsonify({"status": "error", "message": "Team Dashboard admin access is restricted."}), HTTPStatus.FORBIDDEN
        payload = request.get_json(silent=True) or {}
        store = _get_team_dashboard_config_store()
        config = store.load()
        config["report_intelligence_config"] = normalize_report_intelligence_config(payload.get("report_intelligence_config") or payload)
        saved = store.save(config)
        _log_portal_event(
            "team_dashboard_report_intelligence_save_success",
            **_build_request_log_context(
                settings,
                user_identity=user_identity,
                extra={
                    "vip_count": len(saved.get("report_intelligence_config", {}).get("vip_people") or []),
                    "keyword_count": len(saved.get("report_intelligence_config", {}).get("priority_keywords") or []),
                },
            ),
        )
        return jsonify({"status": "ok", "report_intelligence_config": saved.get("report_intelligence_config") or normalize_report_intelligence_config({})})

    @app.route("/api/team-dashboard/report-intelligence/seatalk/name-mappings", methods=["GET", "POST"])
    def team_dashboard_report_intelligence_seatalk_name_mappings():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        mapping_store = _get_seatalk_name_mapping_store(settings)
        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            mappings = mapping_store.merge_mappings(payload.get("mappings") or {})
            SeaTalkDashboardService.clear_cache()
            return jsonify({"status": "ok", "mappings": mappings})
        force_refresh = str(request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes"}
        try:
            candidates = _build_seatalk_dashboard_service(settings).build_name_mappings(force_refresh=force_refresh)
            mappings = mapping_store.mappings()
            mapped_keys = {alias for key in mappings for alias in SeaTalkNameMappingStore.equivalent_keys(key)}
            candidates = dict(candidates)
            candidates["unknown_ids"] = _dedupe_seatalk_name_mapping_candidates([
                row for row in (candidates.get("unknown_ids") or [])
                if isinstance(row, dict) and not (SeaTalkNameMappingStore.equivalent_keys(row.get("id")) & mapped_keys)
            ])
            return jsonify({"status": "ok", "mappings": mappings, **candidates})
        except (ConfigError, ToolError) as error:
            _log_portal_event(
                "team_dashboard_report_intelligence_name_mapping_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=_classify_portal_error(error)),
            )
            current_app.logger.warning("Team Dashboard Report Intelligence name mapping failed: %s", error)
            return jsonify({"status": "error", "message": str(error), **_classify_portal_error(error)}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_report_intelligence_name_mapping_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=_classify_portal_error(error)),
            )
            current_app.logger.exception("Team Dashboard Report Intelligence name mapping failed.")
            return jsonify({"status": "error", "message": "Could not load SeaTalk name mappings.", **_classify_portal_error(error)}), HTTPStatus.INTERNAL_SERVER_ERROR

    @app.get("/api/team-dashboard/tasks")
    def team_dashboard_tasks():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        route_started_at = time.monotonic()
        config_started_at = time.monotonic()
        store = _get_team_dashboard_config_store()
        config = store.load()
        config_elapsed = round(time.monotonic() - config_started_at, 3)
        key_project_overrides = config.get("key_project_overrides") if isinstance(config.get("key_project_overrides"), dict) else {}
        requested_team_key = str(request.args.get("team") or request.args.get("team_key") or "").strip().upper()
        force_reload = str(request.args.get("reload") or "").strip().lower() in {"1", "true", "yes"}
        if requested_team_key and requested_team_key not in TEAM_DASHBOARD_TEAMS:
            return (
                jsonify({"status": "error", "message": f"Unknown team: {requested_team_key}."}),
                HTTPStatus.BAD_REQUEST,
            )
        team_items = (
            [(requested_team_key, TEAM_DASHBOARD_TEAMS[requested_team_key])]
            if requested_team_key
            else list(TEAM_DASHBOARD_TEAMS.items())
        )
        if force_reload and not requested_team_key:
            try:
                team_payloads = _load_team_dashboard_tasks_for_all_teams_merged(
                    settings,
                    store,
                    config,
                    config_elapsed=config_elapsed,
                    route_started_at=route_started_at,
                    key_project_overrides=key_project_overrides,
                )
                _record_team_dashboard_work_memory(team_payloads, owner_email=_current_google_email())
                response = jsonify(
                    {
                        "status": "ok",
                        "teams": team_payloads,
                        "team": None,
                        "team_key": "",
                        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }
                )
                response.headers["Cache-Control"] = "no-store, private, max-age=0"
                response.headers["Pragma"] = "no-cache"
                response.headers["Expires"] = "0"
                return response
            except Exception as error:  # noqa: BLE001 - keep the API shape stable on upstream failure.
                error_details = _classify_portal_error(error)
                _log_portal_event(
                    "team_dashboard_tasks_all_team_reload_error",
                    level=logging.WARNING,
                    **_build_request_log_context(
                        settings,
                        user_identity=_get_user_identity(settings),
                        extra=error_details,
                    ),
                )
        team_payloads: list[dict[str, Any]] = []
        has_error = False
        for team_key, label in team_items:
            timing_stats = _team_dashboard_new_timing()
            timing_stats["config_load"] = config_elapsed
            team_config = (config.get("teams") or {}).get(team_key) or {}
            emails = _normalize_team_dashboard_emails(team_config.get("member_emails") or [])
            cache_started_at = time.monotonic()
            cached_team = None if force_reload else _cached_team_dashboard_task_payload(config, team_key, emails)
            _team_dashboard_add_timing(timing_stats, "cache_check", cache_started_at)
            if cached_team is not None:
                timing_stats["total"] = round(time.monotonic() - route_started_at, 3)
                cached_team["timing_stats"] = timing_stats
                cached_team["elapsed_seconds"] = timing_stats["total"]
                team_payloads.append(cached_team)
                continue
            started_at = time.monotonic()
            try:
                bpmis_client = _build_bpmis_client_for_current_user(settings)
                biz_bpmis_client = _build_bpmis_client_for_current_user(settings)
                tasks, biz_projects = _team_dashboard_load_jira_and_biz_projects(
                    bpmis_client,
                    biz_bpmis_client,
                    emails,
                    timing_stats,
                )
                step_started_at = time.monotonic()
                team_payload = _build_team_dashboard_task_group(
                    team_key,
                    label,
                    emails,
                    tasks,
                    biz_projects,
                    key_project_overrides=key_project_overrides,
                )
                _team_dashboard_add_timing(timing_stats, "group_projects", step_started_at)
                step_started_at = time.monotonic()
                _backfill_team_dashboard_empty_project_jira_tasks(bpmis_client, team_payload)
                _remove_team_dashboard_zero_jira_pending_live_projects(team_payload)
                _team_dashboard_add_timing(timing_stats, "backfill_zero_jira_projects", step_started_at)
                timing_stats.update(_team_dashboard_combined_request_timings(bpmis_client, biz_bpmis_client))
                team_payload["elapsed_seconds"] = round(time.monotonic() - started_at, 2)
                team_payload["fetch_stats"] = _team_dashboard_combined_fetch_stats(bpmis_client, biz_bpmis_client)
                timing_stats["total"] = team_payload["elapsed_seconds"]
                team_payload["timing_stats"] = timing_stats
                team_payloads.append(team_payload)
                step_started_at = time.monotonic()
                _store_team_dashboard_task_payload(store, team_key, emails, team_payload)
                _team_dashboard_add_timing(timing_stats, "cache_store", step_started_at)
                timing_stats["total"] = round(time.monotonic() - started_at, 2)
                team_payload["elapsed_seconds"] = timing_stats["total"]
                team_payload["timing_stats"] = timing_stats
                _log_portal_event(
                    "team_dashboard_tasks_team_loaded",
                    **_build_request_log_context(
                        settings,
                        user_identity=_get_user_identity(settings),
                        extra={
                            "team_key": team_key,
                            "email_count": len(emails),
                            "raw_task_count": len(tasks or []),
                            "raw_biz_project_count": len(biz_projects or []),
                            "elapsed_seconds": team_payload["elapsed_seconds"],
                            "fetch_stats": team_payload["fetch_stats"],
                            "timing_stats": team_payload["timing_stats"],
                        },
                    ),
                )
            except Exception as error:  # noqa: BLE001 - keep other team groups renderable.
                has_error = True
                timing_stats["total"] = round(time.monotonic() - started_at, 2)
                error_details = _classify_portal_error(error)
                _log_portal_event(
                    "team_dashboard_tasks_team_error",
                    level=logging.WARNING,
                    **_build_request_log_context(
                        settings,
                        user_identity=_get_user_identity(settings),
                        extra={**error_details, "team_key": team_key},
                    ),
                )
                team_payloads.append(
                    {
                        "team_key": team_key,
                        "label": label,
                        "member_emails": emails,
                        "under_prd": [],
                        "pending_live": [],
                        "error": str(error),
                        "elapsed_seconds": timing_stats["total"],
                        "fetch_stats": {},
                        "timing_stats": timing_stats,
                    }
                )
        _record_team_dashboard_work_memory(team_payloads, owner_email=_current_google_email())
        response = jsonify(
            {
                "status": "partial" if has_error else "ok",
                "teams": team_payloads,
                "team": team_payloads[0] if requested_team_key and team_payloads else None,
                "team_key": requested_team_key,
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        )
        response.headers["Cache-Control"] = "no-store, private, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.post("/api/team-dashboard/key-projects")
    def save_team_dashboard_key_project():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        if not _can_manage_team_dashboard(user_identity):
            return jsonify({"status": "error", "message": "Team Dashboard admin access is restricted."}), HTTPStatus.FORBIDDEN
        payload = request.get_json(silent=True) or {}
        bpmis_id = str(payload.get("bpmis_id") or "").strip()
        if not bpmis_id:
            return jsonify({"status": "error", "message": "BPMIS ID is required."}), HTTPStatus.BAD_REQUEST
        if "is_key_project" not in payload:
            return jsonify({"status": "error", "message": "Key Project value is required."}), HTTPStatus.BAD_REQUEST
        is_key_project = bool(payload.get("is_key_project"))
        store = _get_team_dashboard_config_store()
        config = store.load()
        overrides = config.get("key_project_overrides") if isinstance(config.get("key_project_overrides"), dict) else {}
        overrides[bpmis_id] = {
            "is_key_project": is_key_project,
            "updated_by": str(user_identity.get("email") or "").strip().lower(),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        config["key_project_overrides"] = overrides
        saved = store.save(config)
        effective = _apply_team_dashboard_key_project_state(
            {"bpmis_id": bpmis_id, "priority": str(payload.get("priority") or "").strip()},
            saved.get("key_project_overrides") if isinstance(saved.get("key_project_overrides"), dict) else {},
        )
        _log_portal_event(
            "team_dashboard_key_project_save_success",
            **_build_request_log_context(
                settings,
                user_identity=user_identity,
                extra={
                    "bpmis_id": bpmis_id,
                    "is_key_project": is_key_project,
                    "key_project_source": effective.get("key_project_source"),
                },
            ),
        )
        return jsonify(
            {
                "status": "ok",
                "bpmis_id": bpmis_id,
                "override": (saved.get("key_project_overrides") or {}).get(bpmis_id) or {},
                "is_key_project": effective.get("is_key_project"),
                "key_project_source": effective.get("key_project_source"),
            }
        )

    @app.get("/api/team-dashboard/link-biz-projects")
    def team_dashboard_link_biz_projects():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            config = _get_team_dashboard_config_store().load()
            started_at = time.monotonic()
            rows = _load_team_dashboard_link_biz_jira_rows(settings, config)
            return jsonify({"status": "ok", "rows": rows, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_link_biz_project_load_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=_classify_portal_error(error)),
            )
            current_app.logger.exception("Team Dashboard Link Biz Project load failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Could not load unlinked Jira tickets. Please retry or share the request ID.",
                        **_classify_portal_error(error),
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.get("/api/team-dashboard/link-biz-projects/jira")
    def team_dashboard_link_biz_project_jira():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        try:
            config = _get_team_dashboard_config_store().load()
            started_at = time.monotonic()
            rows = _load_team_dashboard_link_biz_jira_rows(settings, config)
            elapsed_seconds = round(time.monotonic() - started_at, 2)
            _log_portal_event(
                "team_dashboard_link_biz_project_jira_loaded",
                **_build_request_log_context(settings, user_identity=user_identity, extra={"row_count": len(rows), "elapsed_seconds": elapsed_seconds}),
            )
            return jsonify(
                {
                    "status": "ok",
                    "rows": rows,
                    "elapsed_seconds": elapsed_seconds,
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            )
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_link_biz_project_jira_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=user_identity, extra=_classify_portal_error(error)),
            )
            current_app.logger.exception("Team Dashboard Link Biz Project Jira load failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Could not load unlinked Jira tickets. Please retry or share the request ID.",
                        **_classify_portal_error(error),
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/team-dashboard/link-biz-projects/suggestions")
    def team_dashboard_link_biz_project_suggestions():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
        team_payloads = payload.get("team_payloads") if isinstance(payload.get("team_payloads"), list) else None
        user_identity = _get_user_identity(settings)
        try:
            config = _get_team_dashboard_config_store().load()
            started_at = time.monotonic()
            result = _suggest_team_dashboard_link_biz_project_rows(settings, config, rows, team_payloads=team_payloads)
            elapsed_seconds = round(time.monotonic() - started_at, 2)
            _log_portal_event(
                "team_dashboard_link_biz_project_suggestions_loaded",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={
                        "row_count": len(result["rows"]),
                        "matched_count": result["matched_count"],
                        "team_candidate_count": result["team_candidate_count"],
                        "keyword_candidate_count": result["keyword_candidate_count"],
                        "elapsed_seconds": elapsed_seconds,
                    },
                ),
            )
            return jsonify({"status": "ok", "elapsed_seconds": elapsed_seconds, **result})
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_link_biz_project_suggestions_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=user_identity, extra=_classify_portal_error(error)),
            )
            current_app.logger.exception("Team Dashboard Link Biz Project suggestions failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Could not suggest BPMIS Biz Projects. Please retry or share the request ID.",
                        **_classify_portal_error(error),
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/team-dashboard/link-biz-projects")
    def link_team_dashboard_biz_project():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        jira_id = _extract_issue_key_from_text(str(payload.get("jira_id") or payload.get("jira_link") or ""))
        jira_link = str(payload.get("jira_link") or "").strip()
        reporter_email = str(payload.get("reporter_email") or payload.get("pm_email") or "").strip().lower()
        bpmis_id = str(
            payload.get("selected_bpmis_id")
            or payload.get("suggested_bpmis_id")
            or payload.get("bpmis_id")
            or ""
        ).strip()
        if not jira_id:
            return jsonify({"status": "error", "message": "Jira ID is required."}), HTTPStatus.BAD_REQUEST
        if not bpmis_id:
            return jsonify({"status": "error", "message": "Suggested BPMIS ID is required."}), HTTPStatus.BAD_REQUEST
        if not reporter_email:
            return jsonify({"status": "error", "message": "Reporter email is required to validate the BPMIS Biz Project owner."}), HTTPStatus.BAD_REQUEST

        user_identity = _get_user_identity(settings)
        try:
            bpmis_client = _build_bpmis_client_for_current_user(settings)
            allowed_candidates = _team_dashboard_link_biz_candidate_projects_by_pm(
                bpmis_client,
                [reporter_email],
                team_payloads=None,
            ).get(reporter_email, [])
            allowed_bpmis_ids = {str(project.get("bpmis_id") or "").strip() for project in allowed_candidates}
            if bpmis_id not in allowed_bpmis_ids:
                raise ToolError("Selected BPMIS Biz Project must belong to the Jira PM and be in an allowed status.")
            linked_detail = bpmis_client.link_jira_ticket_to_project(jira_id, bpmis_id)
            if bpmis_id not in _extract_parent_issue_ids_from_any(linked_detail):
                raise BPMISError("BPMIS link verification failed because the Jira detail does not include this Biz Project parent.")

            project_detail = {}
            try:
                project_detail = bpmis_client.get_issue_detail(bpmis_id)
            except Exception:  # noqa: BLE001 - the verified link is the source of truth; project cache can be sparse.
                project_detail = {}
            project = _normalize_team_dashboard_project(
                {
                    **(project_detail if isinstance(project_detail, dict) else {}),
                    "bpmis_id": bpmis_id,
                    "issue_id": bpmis_id,
                }
            )
            if not project.get("project_name"):
                project["project_name"] = str(
                    (project_detail if isinstance(project_detail, dict) else {}).get("project_name")
                    or (project_detail if isinstance(project_detail, dict) else {}).get("summary")
                    or payload.get("selected_project_title")
                    or payload.get("suggested_project_title")
                    or ""
                ).strip()
            if not jira_link:
                jira_link = f"{_jira_browse_base_url()}{jira_id}"

            _log_portal_event(
                "team_dashboard_link_biz_project_success",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={"jira_id": jira_id, "bpmis_id": bpmis_id},
                ),
            )
            return jsonify(
                {
                    "status": "ok",
                    "jira_id": jira_id,
                    "jira_link": jira_link,
                    "bpmis_id": bpmis_id,
                    "project": project,
                    "ticket": {},
                }
            )
        except (BPMISError, ToolError) as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_link_biz_project_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=user_identity, extra={**error_details, "jira_id": jira_id, "bpmis_id": bpmis_id}),
            )
            return jsonify({"status": "error", "message": str(error), **error_details}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_link_biz_project_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=user_identity, extra={"jira_id": jira_id, "bpmis_id": bpmis_id}),
            )
            current_app.logger.exception("Team Dashboard Link Biz Project failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Link Biz Project failed unexpectedly. Please retry or share the request ID.",
                        **_classify_portal_error(error),
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/team-dashboard/monthly-report/draft")
    def team_dashboard_monthly_report_draft():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        try:
            config = _get_team_dashboard_config_store().load()
            team_payloads = _load_all_team_dashboard_task_payloads(settings, config)
            report_period = resolve_monthly_report_period()
            request_payload = {
                "template": normalize_monthly_report_template(config.get("monthly_report_template")),
                "team_payloads": team_payloads,
                "report_intelligence_config": normalize_report_intelligence_config(config.get("report_intelligence_config")),
                "period_start": report_period.start.isoformat(),
                "period_end": report_period.end_date,
                "period_end_exclusive": report_period.end_exclusive.isoformat(),
                "product_scope": list(MONTHLY_REPORT_PRODUCT_SCOPE),
            }
            if _remote_bpmis_config_enabled(settings):
                data = _build_local_agent_client(settings).team_dashboard_monthly_report_draft_start(request_payload)
                job_id = str(data.get("job_id") or "").strip()
                if not job_id:
                    raise ToolError("Mac local-agent did not return a Monthly Report job id.")
                _log_portal_event(
                    "team_dashboard_monthly_report_draft_queued",
                    **_build_request_log_context(settings, user_identity=user_identity, extra={"job_id": job_id, "job_backend": "local_agent"}),
                )
                return jsonify({"status": "queued", "job_id": job_id, "job_backend": "local_agent"})
            job_store: JobStore = current_app.config["JOB_STORE"]
            job = job_store.create("team-dashboard-monthly-report-draft", title="Generate Monthly Report Draft")
            app_obj = current_app._get_current_object()
            thread = threading.Thread(
                target=_run_team_dashboard_monthly_report_draft_job,
                args=(app_obj, job.job_id, settings, request_payload, user_identity),
                daemon=True,
            )
            thread.start()
            _log_portal_event(
                "team_dashboard_monthly_report_draft_queued",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={"job_id": job.job_id},
                ),
            )
            return jsonify({"status": "queued", "job_id": job.job_id})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_monthly_report_draft_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=user_identity, extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error), **error_details}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_monthly_report_draft_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=user_identity, extra=_classify_portal_error(error)),
            )
            current_app.logger.exception("Team Dashboard Monthly Report draft failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Monthly Report draft generation failed unexpectedly. Please retry or share the request ID.",
                        **_classify_portal_error(error),
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/team-dashboard/monthly-report/send")
    def team_dashboard_monthly_report_send():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        draft_markdown = str(payload.get("draft_markdown") or "").strip()
        subject = str(payload.get("subject") or "").strip() or monthly_report_subject()
        recipient = str(payload.get("recipient") or "").strip() or DEFAULT_MONTHLY_REPORT_RECIPIENT
        user_identity = _get_user_identity(settings)
        try:
            send_payload = {
                "draft_markdown": draft_markdown,
                "subject": subject,
                "recipient": recipient,
            }
            if _local_agent_seatalk_enabled(settings):
                data = _build_local_agent_client(settings).team_dashboard_monthly_report_send(send_payload)
            else:
                result = send_monthly_report_email(
                    credential_store=current_app.config["GOOGLE_CREDENTIAL_STORE"],
                    owner_email=str(settings.gmail_seatalk_demo_owner_email or settings.seatalk_owner_email or "").strip().lower(),
                    recipient=recipient,
                    subject=subject,
                    draft_markdown=draft_markdown,
                )
                data = asdict(result)
            _log_portal_event(
                "team_dashboard_monthly_report_send_success",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={"recipient": recipient, "subject": subject, "message_id": str(data.get("message_id") or "")},
                ),
            )
            memory_result = {"recorded": 0, "failed": 0}
            if _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
                try:
                    memory_result = _ingest_sent_monthly_reports_from_gmail(settings)
                except Exception:  # noqa: BLE001 - sent-mail memory ingestion must not block email sending.
                    current_app.logger.exception("Monthly Report sent-mail Work Memory ingestion failed.")
            else:
                current_app.logger.info("Skipping Monthly Report sent-mail Work Memory ingestion because gmail.readonly scope is absent.")
            data["work_memory"] = memory_result
            return jsonify({"status": "ok", **data})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_monthly_report_send_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=user_identity, extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error), **error_details}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_monthly_report_send_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=user_identity, extra=_classify_portal_error(error)),
            )
            current_app.logger.exception("Team Dashboard Monthly Report send failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Monthly Report email failed unexpectedly. Please retry or share the request ID.",
                        **_classify_portal_error(error),
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/team-dashboard/prd-review")
    def team_dashboard_prd_review():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        user_identity = _get_user_identity(settings)
        review_payload = {
            "owner_key": str(user_identity.get("config_key") or ""),
            "jira_id": str(payload.get("jira_id") or ""),
            "jira_link": str(payload.get("jira_link") or ""),
            "prd_url": str(payload.get("prd_url") or ""),
            "force_refresh": bool(payload.get("force_refresh")),
        }
        try:
            if _local_agent_source_code_qa_enabled(settings):
                data = _build_local_agent_client(settings).prd_review(review_payload)
            else:
                data = _build_prd_review_service(settings).review(PRDReviewRequest(**review_payload))
            _log_portal_event(
                "team_dashboard_prd_review_success",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={
                        "jira_id": review_payload["jira_id"],
                        "prd_url_hash": hashlib.sha256(review_payload["prd_url"].encode("utf-8")).hexdigest()[:12],
                        "cached": bool(data.get("cached")),
                    },
                ),
            )
            return jsonify(data)
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_prd_review_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={**error_details, "jira_id": review_payload["jira_id"]},
                ),
            )
            return jsonify({"status": "error", "message": str(error), **error_details}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_prd_review_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=user_identity, extra={"jira_id": review_payload["jira_id"]}),
            )
            current_app.logger.exception("Team Dashboard PRD review failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "PRD review failed unexpectedly. Please retry or share the request ID.",
                        **_classify_portal_error(error),
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/team-dashboard/prd-summary")
    def team_dashboard_prd_summary():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        user_identity = _get_user_identity(settings)
        summary_payload = {
            "owner_key": str(user_identity.get("config_key") or ""),
            "jira_id": str(payload.get("jira_id") or ""),
            "jira_link": str(payload.get("jira_link") or ""),
            "prd_url": str(payload.get("prd_url") or ""),
            "force_refresh": bool(payload.get("force_refresh")),
        }
        try:
            if _local_agent_source_code_qa_enabled(settings):
                data = _build_local_agent_client(settings).prd_summary(summary_payload)
            else:
                data = _build_prd_review_service(settings).summarize(PRDReviewRequest(**summary_payload))
            _log_portal_event(
                "team_dashboard_prd_summary_success",
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={
                        "jira_id": summary_payload["jira_id"],
                        "prd_url_hash": hashlib.sha256(summary_payload["prd_url"].encode("utf-8")).hexdigest()[:12],
                        "cached": bool(data.get("cached")),
                    },
                ),
            )
            return jsonify(data)
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_prd_summary_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(
                    settings,
                    user_identity=user_identity,
                    extra={**error_details, "jira_id": summary_payload["jira_id"]},
                ),
            )
            return jsonify({"status": "error", "message": str(error), **error_details}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "team_dashboard_prd_summary_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=user_identity, extra={"jira_id": summary_payload["jira_id"]}),
            )
            current_app.logger.exception("Team Dashboard PRD summary failed.")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "PRD summary failed unexpectedly. Please retry or share the request ID.",
                        **_classify_portal_error(error),
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/prd-self-assessment/review")
    def prd_self_assessment_review_api():
        return _run_prd_self_assessment_action(settings, action="review")

    @app.post("/api/prd-self-assessment/summary")
    def prd_self_assessment_summary_api():
        return _run_prd_self_assessment_action(settings, action="summary")

    @app.get("/api/prd-self-assessment/latest")
    def prd_self_assessment_latest_api():
        access_gate = _require_prd_self_assessment_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        owner_key = str(user_identity.get("config_key") or "")
        try:
            if _local_agent_source_code_qa_enabled(settings):
                return jsonify(_build_local_agent_client(settings).prd_self_assessment_latest(owner_key=owner_key))
            latest = _get_prd_latest_result(owner_key=owner_key, tool_key="prd_self_assessment")
            return jsonify({"status": "ok", "latest": latest})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001
            current_app.logger.exception("PRD Self-Assessment latest load failed.")
            return jsonify({"status": "error", "message": str(error) or "Could not load latest PRD Self-Assessment result."}), HTTPStatus.BAD_REQUEST

    @app.post("/api/jobs/sync-bpmis-projects")
    def create_sync_bpmis_projects_job():
        return _start_job("sync-bpmis-projects")

    @app.route("/api/local-agent/<path:agent_path>", methods=["GET", "POST", "PATCH", "DELETE"])
    def local_agent_public_proxy(agent_path: str):
        return _proxy_local_agent_request(agent_path)

    @app.route("/uat-local-agent/healthz", methods=["GET"])
    def uat_local_agent_health_proxy():
        return _proxy_local_agent_request("healthz", base_url=_uat_local_agent_loopback_base_url(), use_api_prefix=False)

    @app.route("/uat-local-agent/api/local-agent/<path:agent_path>", methods=["GET", "POST", "PATCH", "DELETE"])
    def uat_local_agent_public_proxy(agent_path: str):
        return _proxy_local_agent_request(agent_path, base_url=_uat_local_agent_loopback_base_url(), use_api_prefix=True)

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

    @app.patch("/api/bpmis-projects/order")
    def reorder_bpmis_projects():
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        payload = request.get_json(silent=True) or {}
        bpmis_ids = payload.get("bpmis_ids") if isinstance(payload.get("bpmis_ids"), list) else []
        user_identity = _get_user_identity(settings)
        projects = _get_bpmis_project_store().reorder_projects(
            user_key=user_identity["config_key"],
            bpmis_ids=[str(item or "") for item in bpmis_ids],
        )
        return jsonify({"status": "ok", "projects": projects, "scope": "portal_only"})

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

        started_at = time.monotonic()
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
                    "elapsed_seconds": round(time.monotonic() - started_at, 3),
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

        started_at = time.monotonic()
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
                    "elapsed_seconds": round(time.monotonic() - started_at, 3),
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

        started_at = time.monotonic()
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
                    "elapsed_seconds": round(time.monotonic() - started_at, 3),
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
            if _remote_bpmis_config_enabled(settings):
                try:
                    remote_snapshot = _build_local_agent_client(settings).team_dashboard_monthly_report_job(job_id)
                    if isinstance(remote_snapshot, dict) and str(remote_snapshot.get("state") or ""):
                        return jsonify(remote_snapshot)
                except ToolError:
                    pass
            return jsonify({"status": "error", "message": "Job not found."}), 404
        action = str(snapshot.get("action") or "")
        if action == "source-code-qa-query":
            access_gate = _require_source_code_qa_access(settings, api=True)
            if access_gate is not None:
                return access_gate
            scoped_snapshot = _source_code_qa_job_snapshot_for_current_user(job_id)
            if scoped_snapshot is None:
                return jsonify({"status": "error", "message": "Job not found."}), 404
            return jsonify(_public_source_code_qa_job_snapshot(scoped_snapshot))
        if action == "source-code-qa-effort-assessment":
            access_gate = _require_source_code_qa_manage_access(settings, api=True)
            if access_gate is not None:
                return access_gate
            return jsonify(_public_source_code_qa_job_snapshot(snapshot))
        return jsonify(snapshot)

    @app.get("/api/source-code-qa/query-jobs/<job_id>")
    def source_code_qa_query_job_api(job_id: str):
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        snapshot = _source_code_qa_job_snapshot_for_current_user(job_id)
        if snapshot is None:
            return jsonify(
                {
                    "status": "error",
                    "message": "Source Code Q&A job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }
            ), HTTPStatus.NOT_FOUND
        return jsonify(_public_source_code_qa_job_snapshot(snapshot))

    @app.get("/api/source-code-qa/query-jobs/<job_id>/events")
    def source_code_qa_query_job_events_api(job_id: str):
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _source_code_qa_job_snapshot_for_current_user(job_id) is None:
            return jsonify(
                {
                    "status": "error",
                    "message": "Source Code Q&A job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }
            ), HTTPStatus.NOT_FOUND

        def event_stream():
            last_payload = ""
            deadline = time.time() + 900
            while time.time() < deadline:
                snapshot = _source_code_qa_job_snapshot_for_current_user(job_id)
                if snapshot is None:
                    payload = {
                        "status": "error",
                        "state": "failed",
                        "message": "Source Code Q&A job was not found.",
                        "error": "Source Code Q&A job was not found.",
                        "error_category": "job_not_found",
                        "error_code": "job_not_found",
                        "error_retryable": False,
                    }
                    yield f"event: failed\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    return
                payload = _public_source_code_qa_job_snapshot(snapshot)
                payload_text = json.dumps(payload, ensure_ascii=False)
                if payload_text != last_payload:
                    event_name = "message"
                    if payload.get("state") == "completed":
                        event_name = "completed"
                    elif payload.get("state") == "failed":
                        event_name = "failed"
                    yield f"event: {event_name}\ndata: {payload_text}\n\n"
                    last_payload = payload_text
                    if payload.get("state") in {"completed", "failed"}:
                        return
                else:
                    yield ": keepalive\n\n"
                time.sleep(0.9)

        return Response(
            stream_with_context(event_stream()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/api/source-code-qa/effort-assessment-jobs/<job_id>")
    def source_code_qa_effort_assessment_job_api(job_id: str):
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
        if snapshot is None or snapshot.get("action") != "source-code-qa-effort-assessment":
            return jsonify(
                {
                    "status": "error",
                    "message": "Source Code Q&A effort assessment job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }
            ), HTTPStatus.NOT_FOUND
        return jsonify(_public_source_code_qa_job_snapshot(snapshot))

    @app.get("/api/source-code-qa/effort-assessment-jobs/<job_id>/events")
    def source_code_qa_effort_assessment_job_events_api(job_id: str):
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate

        def event_stream():
            last_payload = ""
            deadline = time.time() + 900
            while time.time() < deadline:
                snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
                if snapshot is None or snapshot.get("action") != "source-code-qa-effort-assessment":
                    payload = {
                        "status": "error",
                        "state": "failed",
                        "message": "Source Code Q&A effort assessment job was not found.",
                        "error": "Source Code Q&A effort assessment job was not found.",
                        "error_category": "job_not_found",
                        "error_code": "job_not_found",
                        "error_retryable": False,
                    }
                    yield f"event: failed\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    return
                payload = _public_source_code_qa_job_snapshot(snapshot)
                payload_text = json.dumps(payload, ensure_ascii=False)
                if payload_text != last_payload:
                    event_name = "message"
                    if payload.get("state") == "completed":
                        event_name = "completed"
                    elif payload.get("state") == "failed":
                        event_name = "failed"
                    yield f"event: {event_name}\ndata: {payload_text}\n\n"
                    last_payload = payload_text
                    if payload.get("state") in {"completed", "failed"}:
                        return
                else:
                    yield ": keepalive\n\n"
                time.sleep(0.9)

        return Response(
            stream_with_context(event_stream()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    def _start_job(action: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate

        user_identity = _get_user_identity(settings)
        config_data = _load_user_config_for_identity(settings, user_identity) or config_store._normalize({})
        config_data = _hydrate_setup_defaults(config_data, user_identity)
        _apply_sync_email_policy(config_data, user_identity)
        config_data["_user_key"] = user_identity["config_key"]
        job_store: JobStore = current_app.config["JOB_STORE"]
        title = {
            "sync-bpmis-projects": "Sync BPMIS Projects",
        }.get(action, "Background Job")
        job = job_store.create(action, title=title)

        thread = threading.Thread(
            target=_run_background_job,
            args=(app, job.job_id, action, settings, config_data),
            daemon=True,
        )
        thread.start()
        _log_portal_event(
            "job_queued",
            **_build_request_log_context(
                settings,
                user_identity=user_identity,
                extra={"job_id": job.job_id, "action": action},
            ),
        )
        return jsonify({"status": "queued", "job_id": job.job_id})

    return app


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


def _build_gmail_dashboard_service() -> GmailDashboardService:
    credentials = get_google_credentials()
    report_config = {}
    try:
        report_config = _get_team_dashboard_config_store().load().get("report_intelligence_config") or {}
    except Exception:
        report_config = {}
    return GmailDashboardService(credentials=credentials, cache_key=_current_google_email(), report_intelligence_config=report_config)


def _meeting_recorder_config(settings: Settings) -> MeetingRecorderConfig:
    return MeetingRecorderConfig(
        ffmpeg_bin=settings.meeting_recorder_ffmpeg_bin,
        audio_input=settings.meeting_recorder_audio_input,
        transcribe_provider=settings.meeting_recorder_transcribe_provider,
        whisper_cpp_bin=settings.meeting_recorder_whisper_cpp_bin,
        whisper_model=settings.meeting_recorder_whisper_model,
        whisper_language=settings.meeting_recorder_whisper_language,
        transcript_segment_workers=settings.meeting_recorder_transcript_segment_workers,
        whisper_threads=settings.meeting_recorder_whisper_threads,
        background_nice=settings.meeting_recorder_background_nice,
        capture_status_every_buffers=settings.meeting_recorder_capture_status_every_buffers,
        startup_silence_grace_seconds=settings.meeting_recorder_startup_silence_grace_seconds,
    )


def _get_meeting_record_store() -> MeetingRecordStore:
    return current_app.config["MEETING_RECORD_STORE"]


def _get_meeting_recorder_runtime() -> MeetingRecorderRuntime:
    return current_app.config["MEETING_RECORDER_RUNTIME"]


def _get_work_memory_store() -> WorkMemoryStore:
    return current_app.config["WORK_MEMORY_STORE"]


def _record_work_memory_items(items: list[dict[str, Any]], *, event: str, owner_email: str = "", write_ledger: bool = True) -> dict[str, int]:
    recorded = 0
    failed = 0
    duplicate = 0
    if not items:
        if write_ledger:
            try:
                _get_work_memory_store().record_ingestion_run(
                    source_type=event,
                    owner_email=owner_email or _current_google_email(),
                    status="ok",
                    recorded_count=0,
                    failed_count=0,
                )
            except Exception:
                current_app.logger.debug("Work Memory empty ingestion ledger write failed for %s.", event, exc_info=True)
        return {"recorded": 0, "failed": 0, "duplicate": 0}
    store = _get_work_memory_store()
    for item in items:
        try:
            source_type = str(item.get("source_type") or "").strip()
            source_id = str(item.get("source_id") or "").strip()
            item_type = str(item.get("item_type") or "").strip()
            owner_email = str(item.get("owner_email") or "").strip().lower()
            existing_id = None
            if source_type and source_id and item_type and owner_email:
                existing_id = hashlib.sha256("\x1f".join([source_type, source_id, item_type, owner_email]).encode("utf-8")).hexdigest()[:32]
                if store.get_item(existing_id):
                    duplicate += 1
            store.record_memory_item(**item)
            recorded += 1
        except Exception:  # noqa: BLE001 - memory ingestion must not break the primary tool flow.
            failed += 1
            current_app.logger.exception("Work Memory ingestion failed for %s.", event)
    if write_ledger:
        try:
            store.record_ingestion_run(
                source_type=event,
                owner_email=owner_email or _current_google_email(),
                status="ok" if failed == 0 else "partial_error",
                scanned_count=len(items),
                matched_count=len(items),
                recorded_count=recorded,
                duplicate_count=duplicate,
                failed_count=failed,
            )
        except Exception:
            current_app.logger.debug("Work Memory ingestion ledger write failed for %s.", event, exc_info=True)
    return {"recorded": recorded, "failed": failed, "duplicate": duplicate}


def _record_team_dashboard_work_memory(team_payloads: list[dict[str, Any]], *, owner_email: str) -> dict[str, int]:
    items: list[dict[str, Any]] = []
    for team_payload in team_payloads or []:
        if isinstance(team_payload, dict) and not team_payload.get("error"):
            items.extend(team_dashboard_memory_items(team_payload, owner_email=owner_email))
    return _record_work_memory_items(items, event="team_dashboard")


def _record_meeting_work_memory(record: dict[str, Any]) -> dict[str, int]:
    return _record_work_memory_items(meeting_record_memory_items(record), event="meeting_recorder")


def _record_source_code_qa_work_memory(
    *,
    owner_email: str,
    pm_team: str,
    country: str,
    question: str,
    result: dict[str, Any],
    session_id: str = "",
    job_id: str = "",
) -> dict[str, int]:
    item = source_code_qa_memory_item(
        owner_email=owner_email,
        pm_team=pm_team,
        country=country,
        question=question,
        result=result,
        session_id=session_id,
        job_id=job_id,
    )
    return _record_work_memory_items([item], event="source_code_qa")


SENT_MONTHLY_REPORT_SUBJECT_PATTERN = re.compile(
    r"^\[Banking\]\s+Product\s+Update\s+\("
    r"\d{1,2}\s+[A-Za-z]{3}\s*-\s*\d{1,2}\s+[A-Za-z]{3}"
    r"\)\s+-\s+Anti-Fraud,\s+Credit\s+Risk\s+&\s+Ops\s+Risk$",
    re.IGNORECASE,
)


def _is_sent_monthly_report_subject(subject: str) -> bool:
    return bool(SENT_MONTHLY_REPORT_SUBJECT_PATTERN.match(str(subject or "").strip()))


def _ingest_existing_work_memory_sources(settings: Settings, *, date_range: str = "90d", sources: list[str] | None = None) -> dict[str, Any]:
    owner_email = _current_google_email()
    items: list[dict[str, Any]] = []
    source_counts = {"meeting_records": 0, "team_dashboard_cache": 0}
    source_filter = {str(source or "").strip() for source in sources or [] if str(source or "").strip()}

    if not source_filter or "meeting_recorder" in source_filter:
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                records = _build_local_agent_client(settings).meeting_recorder_records(owner_email=owner_email)
            else:
                records = _get_meeting_record_store().list_records(owner_email=owner_email)
            for record in records:
                if str(record.get("status") or "").strip().lower() != "completed":
                    continue
                record_items = meeting_record_memory_items(record)
                if record_items:
                    source_counts["meeting_records"] += 1
                    items.extend(record_items)
        except Exception:
            current_app.logger.exception("Work Memory meeting backfill failed.")

    if not source_filter or "team_dashboard" in source_filter:
        try:
            config = _get_team_dashboard_config_store().load()
            task_cache = config.get("task_cache") if isinstance(config.get("task_cache"), dict) else {}
            cached_teams = task_cache.get("teams") if isinstance(task_cache.get("teams"), dict) else {}
            for team_key, cached_team in cached_teams.items():
                if not isinstance(cached_team, dict):
                    continue
                team_payload = {
                    **cached_team,
                    "team_key": str(cached_team.get("team_key") or team_key),
                    "member_emails": _normalize_team_dashboard_emails(cached_team.get("member_emails") or []),
                    "loading": False,
                    "loaded": True,
                    "error": "",
                    "progress_text": "",
                    "cache_source": "backfill",
                }
                team_items = team_dashboard_memory_items(team_payload, owner_email=owner_email)
                if team_items:
                    source_counts["team_dashboard_cache"] += 1
                    items.extend(team_items)
        except Exception:
            current_app.logger.exception("Work Memory Team Dashboard cache backfill failed.")

    result = _record_work_memory_items(items, event="work_memory_existing_backfill")
    return {**result, **source_counts, "date_range": date_range, "sources": sorted(source_filter)}


def _ingest_sent_monthly_reports_from_gmail(settings: Settings) -> dict[str, Any]:
    del settings
    if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
        raise ConfigError("Gmail read permission is missing. Reconnect Google once to grant gmail.readonly.")
    service = _build_gmail_dashboard_service()
    owner_email = _current_google_email()
    queries = [
        'in:sent newer_than:180d from:me subject:"[Banking] Product Update"',
        'in:sent newer_than:180d from:me subject:"Anti-Fraud, Credit Risk & Ops Risk"',
        'in:sent newer_than:180d from:me "[Banking] Product Update" "Anti-Fraud, Credit Risk & Ops Risk"',
    ]
    seen_ids: set[str] = set()
    records = []
    for query in queries:
        for message_id in service._list_message_ids(query=query, max_messages=20):
            if message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            records.append(service._fetch_message_full(message_id))
    items = []
    for record in records:
        headers = getattr(record, "headers", {}) or {}
        sender_text = str(headers.get("from") or "").casefold()
        if owner_email and owner_email not in sender_text:
            continue
        if not _is_sent_monthly_report_subject(str(headers.get("subject") or "")):
            continue
        items.append(sent_monthly_report_memory_item_from_gmail_record(owner_email=owner_email, record=record))
    result = _record_work_memory_items(items, event="gmail_sent_monthly_report_scan")
    return {"scanned": len(records), "matched": len(items), **result}


def _run_incremental_memory_ingestion(settings: Settings, *, window: str = "7d", reconciliation: bool = False) -> dict[str, Any]:
    effective_window = "90d" if reconciliation else (str(window or "7d").strip() or "7d")
    sources = ["meeting_recorder", "team_dashboard"]
    result: dict[str, Any] = {
        "window": effective_window,
        "reconciliation": bool(reconciliation),
        "sources": {},
        "recorded": 0,
        "failed": 0,
        "duplicate": 0,
    }
    existing = _ingest_existing_work_memory_sources(settings, date_range=effective_window, sources=sources)
    result["sources"]["existing"] = existing
    for key in ("recorded", "failed", "duplicate"):
        result[key] += int(existing.get(key) or 0)
    if _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
        try:
            sent_reports = _ingest_sent_monthly_reports_from_gmail(settings)
            result["sources"]["gmail_sent_monthly_report"] = sent_reports
            for key in ("recorded", "failed", "duplicate"):
                result[key] += int(sent_reports.get(key) or 0)
        except Exception as error:  # noqa: BLE001 - incremental ingestion should keep partial source results.
            current_app.logger.exception("Work Memory incremental Gmail sent report ingestion failed.")
            result["sources"]["gmail_sent_monthly_report"] = {"status": "error", "message": str(error)}
            result["failed"] += 1
            _get_work_memory_store().record_ingestion_run(
                source_type="gmail_sent_monthly_report_incremental",
                owner_email=_current_google_email(),
                cursor=effective_window,
                status="error",
                failed_count=1,
                error=str(error),
            )
    else:
        result["sources"]["gmail_sent_monthly_report"] = {
            "status": "skipped",
            "message": "Gmail readonly scope is missing.",
        }
    distilled = _get_work_memory_store().distill_work_memory(owner_email=_current_google_email(), date_range=effective_window)
    result["sources"]["distill"] = distilled
    _get_work_memory_store().record_ingestion_run(
        source_type="work_memory_incremental",
        owner_email=_current_google_email(),
        cursor=effective_window,
        status="ok" if result["failed"] == 0 else "partial_error",
        scanned_count=sum(int((source_result or {}).get("scanned") or (source_result or {}).get("meeting_records") or 0) for source_result in result["sources"].values() if isinstance(source_result, dict)),
        matched_count=result["recorded"],
        recorded_count=result["recorded"],
        duplicate_count=result["duplicate"],
        failed_count=result["failed"],
        metadata={"sources": sources, "reconciliation": bool(reconciliation)},
    )
    return result


def _run_work_memory_gmail_backfill_job(app: Flask, job_id: str, payload: dict[str, Any]) -> None:
    with app.app_context():
        job_store: JobStore = current_app.config["JOB_STORE"]
        owner_email = str(payload.get("owner_email") or "").strip().lower()
        try:
            job_store.update(job_id, state="running", stage="starting", message="Starting Gmail Work Memory backfill.")
            result = _backfill_gmail_work_memory(
                owner_email=owner_email,
                credentials_payload=payload.get("credentials") if isinstance(payload.get("credentials"), dict) else {},
                report_intelligence_config=payload.get("report_intelligence_config") if isinstance(payload.get("report_intelligence_config"), dict) else {},
                days=int(payload.get("days") or 90),
                max_messages=payload.get("max_messages") if payload.get("max_messages") is not None else None,
                drive_read_enabled=bool(payload.get("drive_read_enabled")),
                job_id=job_id,
            )
            job_store.complete(
                job_id,
                results=[result],
                notice={"level": "success" if int(result.get("failed") or 0) == 0 else "warning", "message": "Gmail Work Memory backfill completed."},
            )
        except Exception as error:  # noqa: BLE001 - keep background job failures visible in the job store.
            current_app.logger.exception("Gmail Work Memory backfill failed.")
            _get_work_memory_store().record_ingestion_run(
                source_type="gmail_backfill",
                owner_email=owner_email,
                status="error",
                failed_count=1,
                error=str(error),
            )
            error_details = _classify_portal_error(error)
            job_store.fail(
                job_id,
                str(error),
                error_category=str(error_details.get("error_category") or ""),
                error_code=str(error_details.get("error_code") or ""),
                error_retryable=bool(error_details.get("error_retryable")),
            )


def _backfill_gmail_work_memory(
    *,
    owner_email: str,
    credentials_payload: dict[str, Any],
    report_intelligence_config: dict[str, Any],
    days: int,
    max_messages: int | None,
    drive_read_enabled: bool,
    job_id: str,
) -> dict[str, Any]:
    if not credentials_payload:
        raise ConfigError("Google credentials are missing for Gmail backfill. Reconnect Google and retry.")
    credentials = Credentials(**credentials_payload)
    service = GmailDashboardService(
        credentials=credentials,
        cache_key=owner_email,
        report_intelligence_config=report_intelligence_config,
    )
    job_store: JobStore = current_app.config["JOB_STORE"]
    started_at = datetime.now(timezone.utc).isoformat()
    refs = service.list_work_memory_message_refs(days=days, max_messages=max_messages)
    unique_message_ids: list[str] = []
    seen_message_ids: set[str] = set()
    for ref in refs:
        message_id = str(ref.get("id") or "").strip()
        if message_id and message_id not in seen_message_ids:
            seen_message_ids.add(message_id)
            unique_message_ids.append(message_id)
    original_total = len(unique_message_ids)
    work_memory_store = _get_work_memory_store()
    existing_message_ids = work_memory_store.existing_source_ids(
        source_type="gmail",
        owner_email=owner_email,
        source_ids=unique_message_ids,
    )
    processed_message_ids = work_memory_store.processed_source_ids(
        source_type="gmail",
        owner_email=owner_email,
        source_ids=unique_message_ids,
    )
    existing_message_ids.update(processed_message_ids)
    pending_message_ids = [message_id for message_id in unique_message_ids if message_id not in existing_message_ids]
    skipped_existing = original_total - len(pending_message_ids)
    total = len(pending_message_ids)
    skip_suffix = f" Skipping {skipped_existing} already recorded." if skipped_existing else ""
    job_store.update(job_id, stage="scanning", message=f"Scanning {total}/{original_total} Gmail message(s).{skip_suffix}", current=0, total=total)
    vip_people = _gmail_work_memory_vip_people(report_intelligence_config)
    key_projects = _gmail_work_memory_key_projects()
    scanned = 0
    matched = 0
    recorded = 0
    duplicate = 0
    failed = 0
    attachment_processed = 0
    drive_links_processed = 0
    last_error = ""
    for batch_start in range(0, total, GMAIL_WORK_MEMORY_FETCH_BATCH_SIZE):
        batch_ids = pending_message_ids[batch_start:batch_start + GMAIL_WORK_MEMORY_FETCH_BATCH_SIZE]
        batch_items: list[dict[str, Any]] = []
        processed_batch_ids: list[str] = []
        scanned += len(batch_ids)
        if scanned == len(batch_ids) or scanned % 100 == 0 or scanned == total:
            job_store.update(job_id, stage="fetching", message=f"Fetching {scanned}/{total} Gmail message(s); skipped {skipped_existing}.", current=scanned, total=total)
        records, fetch_failures, fetch_error = _fetch_gmail_work_memory_records(
            credentials_payload=credentials_payload,
            owner_email=owner_email,
            report_intelligence_config=report_intelligence_config,
            message_ids=batch_ids,
        )
        failed += fetch_failures
        if fetch_error:
            last_error = fetch_error
        for record in records:
            try:
                message_id = str(getattr(record, "message_id", "") or "").strip()
                if not message_id:
                    continue
                headers = getattr(record, "headers", {}) or {}
                if service.is_export_noise(headers):
                    processed_batch_ids.append(message_id)
                    continue
                matched_vips, vip_roles = _gmail_work_memory_matched_vips(headers, vip_people)
                body_text = str(getattr(record, "body_text", "") or "")
                match_text = "\n".join(
                    [
                        str(headers.get("subject") or ""),
                        str(headers.get("from") or ""),
                        str(headers.get("to") or ""),
                        str(headers.get("cc") or ""),
                        body_text,
                    ]
                )
                report_matches = _gmail_work_memory_report_matches(match_text, report_intelligence_config, key_projects)
                batch_items.append(
                    gmail_message_memory_item(
                        owner_email=owner_email,
                        record=record,
                        matched_vips=matched_vips,
                        vip_email_roles=vip_roles,
                        report_matches=report_matches,
                    )
                )
                matched += 1
                if matched_vips:
                    attachment_items, attachment_failures = _gmail_work_memory_vip_attachment_items(
                        service=service,
                        owner_email=owner_email,
                        record=record,
                        matched_vips=matched_vips,
                    )
                    batch_items.extend(attachment_items)
                    attachment_processed += len(attachment_items)
                    failed += attachment_failures
                    drive_items = _gmail_work_memory_vip_drive_items(
                        credentials=credentials,
                        owner_email=owner_email,
                        record=record,
                        matched_vips=matched_vips,
                        drive_read_enabled=drive_read_enabled,
                    )
                    batch_items.extend(drive_items)
                    drive_links_processed += len(drive_items)
                processed_batch_ids.append(message_id)
            except Exception as error:  # noqa: BLE001 - one bad message must not stop the backfill.
                failed += 1
                last_error = str(error)
                current_app.logger.warning("Gmail Work Memory message backfill skipped message_id=%s: %s", message_id, error)
        result = _record_work_memory_items(batch_items, event="gmail_backfill", owner_email=owner_email, write_ledger=False)
        work_memory_store.record_processed_source_ids(
            source_type="gmail",
            owner_email=owner_email,
            source_ids=processed_batch_ids,
            metadata={"event": "gmail_backfill", "days": days},
        )
        recorded += int(result.get("recorded") or 0)
        duplicate += int(result.get("duplicate") or 0)
        failed += int(result.get("failed") or 0)
    status = "ok" if failed == 0 else "partial_error"
    _get_work_memory_store().record_ingestion_run(
        source_type="gmail_backfill",
        owner_email=owner_email,
        cursor=f"days={days};messages={scanned};skipped={skipped_existing}",
        status=status,
        scanned_count=scanned,
        matched_count=matched,
        recorded_count=recorded,
        duplicate_count=duplicate,
        failed_count=failed,
        error=last_error,
        metadata={
            "days": days,
            "max_messages": max_messages,
            "original_message_count": original_total,
            "skipped_existing_message_count": skipped_existing,
            "vip_people_count": len(vip_people),
            "attachment_processed": attachment_processed,
            "drive_links_processed": drive_links_processed,
            "drive_read_enabled": drive_read_enabled,
        },
        started_at=started_at,
        completed_at=datetime.now(timezone.utc).isoformat(),
    )
    distilled = _get_work_memory_store().distill_work_memory(owner_email=owner_email, date_range=f"{days}d", sources=["gmail", "gmail_attachment", "gmail_drive_link"])
    return {
        "status": status,
        "days": days,
        "scanned": scanned,
        "skipped_existing": skipped_existing,
        "original_message_count": original_total,
        "matched": matched,
        "recorded": recorded,
        "duplicate": duplicate,
        "failed": failed,
        "attachment_processed": attachment_processed,
        "drive_links_processed": drive_links_processed,
        "distill": distilled,
    }


def _gmail_work_memory_fetch_workers() -> int:
    raw_value = str(os.getenv("GMAIL_WORK_MEMORY_FETCH_WORKERS") or "").strip()
    if not raw_value:
        return GMAIL_WORK_MEMORY_FETCH_WORKERS
    try:
        return max(1, min(int(raw_value), 8))
    except ValueError:
        return GMAIL_WORK_MEMORY_FETCH_WORKERS


def _fetch_gmail_work_memory_records(
    *,
    credentials_payload: dict[str, Any],
    owner_email: str,
    report_intelligence_config: dict[str, Any],
    message_ids: list[str],
) -> tuple[list[Any], int, str]:
    if not message_ids:
        return [], 0, ""
    workers = min(_gmail_work_memory_fetch_workers(), len(message_ids))
    records: list[Any] = []
    failed = 0
    last_error = ""

    def fetch_one(message_id: str) -> Any:
        local_service = GmailDashboardService(
            credentials=Credentials(**credentials_payload),
            cache_key=owner_email,
            report_intelligence_config=report_intelligence_config,
        )
        return local_service.fetch_work_memory_message(message_id)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, message_id): message_id for message_id in message_ids}
        for future in as_completed(futures):
            message_id = futures[future]
            try:
                records.append(future.result())
            except Exception as error:  # noqa: BLE001 - one message failure must not stop the backfill batch.
                failed += 1
                last_error = str(error)
                current_app.logger.warning("Gmail Work Memory full fetch failed message_id=%s: %s", message_id, error)
    records.sort(key=lambda item: getattr(item, "internal_date", datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
    return records, failed, last_error


def _gmail_work_memory_vip_people(report_intelligence_config: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = normalize_report_intelligence_config(report_intelligence_config)
    return [vip for vip in normalized.get("vip_people") or [] if isinstance(vip, dict)]


def _gmail_work_memory_key_projects() -> list[dict[str, Any]]:
    try:
        config = _get_team_dashboard_config_store().load()
        task_cache = config.get("task_cache") if isinstance(config.get("task_cache"), dict) else {}
        teams = task_cache.get("teams") if isinstance(task_cache.get("teams"), dict) else {}
        projects: list[dict[str, Any]] = []
        for team in teams.values():
            if not isinstance(team, dict):
                continue
            for section_key in ("under_prd", "pending_live"):
                projects.extend(project for project in (team.get(section_key) or []) if isinstance(project, dict))
        return projects
    except Exception:
        return []


def _gmail_work_memory_matched_vips(headers: dict[str, str], vip_people: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    role_addresses = {
        "from": _gmail_header_addresses(headers.get("from", "")),
        "to": _gmail_header_addresses(headers.get("to", "")),
        "cc": _gmail_header_addresses(headers.get("cc", "")),
    }
    matched: list[dict[str, Any]] = []
    roles: dict[str, list[str]] = {}
    for vip in vip_people:
        emails = {str(email or "").strip().lower() for email in vip.get("emails") or [] if str(email or "").strip()}
        if not emails:
            continue
        vip_roles = sorted(role for role, addresses in role_addresses.items() if emails.intersection(addresses))
        if not vip_roles:
            continue
        label = str(vip.get("display_name") or sorted(emails)[0]).strip()
        matched.append({"display_name": label, "role_tags": vip.get("role_tags") or [], "emails": sorted(emails)})
        roles[label] = vip_roles
    return matched, roles


def _gmail_header_addresses(value: str) -> set[str]:
    return {str(address or "").strip().lower() for _name, address in getaddresses([value or ""]) if str(address or "").strip()}


def _gmail_work_memory_report_matches(text: str, report_intelligence_config: dict[str, Any], key_projects: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        from bpmis_jira_tool.report_intelligence import match_report_intelligence

        return match_report_intelligence(text, config=report_intelligence_config, key_projects=key_projects)
    except Exception:
        return {"matched_vips": [], "matched_keywords": [], "matched_key_projects": []}


def _gmail_work_memory_vip_attachment_items(
    *,
    service: GmailDashboardService,
    owner_email: str,
    record: Any,
    matched_vips: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    items: list[dict[str, Any]] = []
    failed = 0
    message_id = str(getattr(record, "message_id", "") or "").strip()
    attachments = [
        attachment
        for attachment in (getattr(record, "attachments", []) or [])
        if _gmail_attachment_is_pdf(attachment)
    ][:GMAIL_WORK_MEMORY_MAX_VIP_ATTACHMENTS_PER_MESSAGE]
    for attachment in attachments:
        try:
            if int(getattr(attachment, "size", 0) or 0) > GMAIL_WORK_MEMORY_MAX_ATTACHMENT_BYTES:
                continue
            content = service.download_attachment(message_id=message_id, attachment_id=str(getattr(attachment, "attachment_id") or ""))
            if len(content) > GMAIL_WORK_MEMORY_MAX_ATTACHMENT_BYTES:
                continue
            text = _extract_pdf_text_for_work_memory(content)
            sha256 = hashlib.sha256(content).hexdigest()
            items.append(
                gmail_attachment_memory_item(
                    owner_email=owner_email,
                    record=record,
                    attachment=attachment,
                    text=text[:GMAIL_WORK_MEMORY_CONTENT_CHARS],
                    sha256=sha256,
                    matched_vips=matched_vips,
                )
            )
        except Exception as error:  # noqa: BLE001 - attachment failures are counted but isolated.
            failed += 1
            current_app.logger.info("VIP Gmail PDF attachment skipped: %s", error)
    return items, failed


def _gmail_attachment_is_pdf(attachment: Any) -> bool:
    filename = str(getattr(attachment, "filename", "") or "").strip().lower()
    mime_type = str(getattr(attachment, "mime_type", "") or "").strip().lower()
    return filename.endswith(".pdf") or mime_type == "application/pdf"


def _extract_pdf_text_for_work_memory(content: bytes) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except ImportError as error:
        raise ToolError("PDF attachments are supported only when pypdf is installed on the server.") from error
    reader = PdfReader(io.BytesIO(content))
    lines: list[str] = []
    for page in reader.pages[:12]:
        lines.append(str(page.extract_text() or ""))
    text = "\n".join(lines).strip()
    if not text:
        raise ToolError("Unable to extract readable text from this PDF attachment.")
    return text[:GMAIL_WORK_MEMORY_CONTENT_CHARS]


def _gmail_work_memory_vip_drive_items(
    *,
    credentials: Credentials,
    owner_email: str,
    record: Any,
    matched_vips: list[dict[str, Any]],
    drive_read_enabled: bool,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    links = list(dict.fromkeys(str(link or "").strip() for link in (getattr(record, "drive_links", []) or []) if str(link or "").strip()))
    for link in links[:GMAIL_WORK_MEMORY_MAX_VIP_ATTACHMENTS_PER_MESSAGE]:
        title = ""
        text = ""
        access_status = "unavailable"
        if drive_read_enabled:
            try:
                title, text = _read_google_drive_link_text(credentials=credentials, url=link)
                access_status = "ok" if text else "unavailable"
            except HttpError as error:
                status = getattr(getattr(error, "resp", None), "status", 0)
                access_status = "permission_denied" if int(status or 0) in {401, 403, 404} else "unavailable"
            except Exception:
                access_status = "unavailable"
        else:
            access_status = "missing_drive_scope"
        items.append(
            gmail_drive_link_memory_item(
                owner_email=owner_email,
                record=record,
                url=link,
                title=title,
                text=text[:GMAIL_WORK_MEMORY_CONTENT_CHARS],
                access_status=access_status,
                matched_vips=matched_vips,
            )
        )
    return items


def _read_google_drive_link_text(*, credentials: Credentials, url: str) -> tuple[str, str]:
    file_id = _google_drive_file_id_from_url(url)
    if not file_id:
        return "", ""
    service = _build_google_drive_service(credentials)
    metadata = service.files().get(fileId=file_id, fields="id,name,mimeType,size").execute()
    title = str(metadata.get("name") or file_id)
    mime_type = str(metadata.get("mimeType") or "")
    if mime_type.startswith("application/vnd.google-apps."):
        for export_mime in ("text/plain", "text/csv"):
            try:
                content = service.files().export_media(fileId=file_id, mimeType=export_mime).execute()
                return title, _bytes_to_text(content)
            except HttpError:
                continue
        return title, ""
    if mime_type == "application/pdf":
        content = service.files().get_media(fileId=file_id).execute()
        return title, _extract_pdf_text_for_work_memory(content)
    return title, ""


def _google_drive_http_timeout_seconds() -> int:
    raw_value = str(os.getenv("GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS") or "").strip()
    if not raw_value:
        return GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS
    try:
        return max(5, min(int(raw_value), 120))
    except ValueError:
        return GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS


def _build_google_drive_service(credentials: Credentials) -> Any:
    http = httplib2.Http(timeout=_google_drive_http_timeout_seconds())
    authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)
    return build_google_api("drive", "v3", http=authed_http, cache_discovery=False)


def _google_drive_file_id_from_url(url: str) -> str:
    parsed = urlparse(str(url or ""))
    path_parts = [part for part in parsed.path.split("/") if part]
    if "d" in path_parts:
        index = path_parts.index("d")
        if index + 1 < len(path_parts):
            return path_parts[index + 1]
    query = parse_qs(parsed.query)
    if query.get("id"):
        return str(query["id"][0] or "").strip()
    if path_parts and path_parts[0] == "open" and query.get("id"):
        return str(query["id"][0] or "").strip()
    return ""


def _bytes_to_text(value: bytes | str) -> str:
    if isinstance(value, str):
        return value.strip()
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return value.decode(encoding).strip()
        except UnicodeDecodeError:
            continue
    return ""


def _build_calendar_meeting_service() -> GoogleCalendarMeetingService:
    return GoogleCalendarMeetingService(get_google_credentials())


def _build_meeting_processing_service(settings: Settings) -> MeetingProcessingService:
    text_client = CodexTextGenerationClient(
        settings=settings,
        workspace_root=PROJECT_ROOT,
        prompt_mode="meeting_recorder_minutes_codex",
        codex_model=settings.prd_briefing_codex_model,
    )
    return MeetingProcessingService(
        store=_get_meeting_record_store(),
        config=_meeting_recorder_config(settings),
        text_client=text_client,
        credential_store=current_app.config.get("GOOGLE_CREDENTIAL_STORE"),
        portal_base_url=settings.team_portal_base_url,
    )


def _persist_owner_google_credentials(settings: Settings) -> None:
    current_email = _current_google_email()
    owner_emails = {
        str(settings.gmail_seatalk_demo_owner_email or settings.seatalk_owner_email or "").strip().lower(),
        str(settings.meeting_recorder_owner_email or "").strip().lower(),
    }
    owner_emails.discard("")
    if not current_email or current_email not in owner_emails:
        return
    store = current_app.config.get("GOOGLE_CREDENTIAL_STORE") if current_app else None
    credentials_payload = dict(session.get("google_credentials") or {})
    if store is None or not credentials_payload:
        return
    store.save(owner_email=current_email, credentials_payload=credentials_payload)


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


def _get_config_store() -> WebConfigStore:
    return current_app.config["CONFIG_STORE"]


def _get_bpmis_project_store() -> BPMISProjectStore:
    settings: Settings = current_app.config["SETTINGS"]
    if _remote_bpmis_config_enabled(settings):
        return RemoteBPMISProjectStore(_build_local_agent_client(settings))
    return current_app.config["BPMIS_PROJECT_STORE"]


def _get_team_dashboard_config_store() -> TeamDashboardConfigStore:
    settings: Settings = current_app.config["SETTINGS"]
    if _remote_bpmis_config_enabled(settings):
        return RemoteTeamDashboardConfigStore(_build_local_agent_client(settings))
    return current_app.config["TEAM_DASHBOARD_CONFIG_STORE"]


def _build_prd_review_service(settings: Settings) -> PRDReviewService:
    store: BriefingStore = current_app.config["PRD_BRIEFING_STORE"]
    confluence = ConfluenceConnector(
        base_url=settings.confluence_base_url,
        email=settings.confluence_email,
        api_token=settings.confluence_api_token,
        bearer_token=settings.confluence_bearer_token,
        store=store,
    )
    return PRDReviewService(
        store=store,
        confluence=confluence,
        settings=settings,
        workspace_root=PROJECT_ROOT,
    )


def _build_monthly_report_service(settings: Settings) -> MonthlyReportService:
    store: BriefingStore = current_app.config["PRD_BRIEFING_STORE"]
    confluence = ConfluenceConnector(
        base_url=settings.confluence_base_url,
        email=settings.confluence_email,
        api_token=settings.confluence_api_token,
        bearer_token=settings.confluence_bearer_token,
        store=store,
    )
    return MonthlyReportService(
        settings=settings,
        workspace_root=PROJECT_ROOT,
        seatalk_service=_build_seatalk_dashboard_service(settings),
        confluence=confluence,
    )


def _get_source_code_qa_session_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQASessionStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_SESSION_STORE"]


def _get_source_code_qa_attachment_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQAAttachmentStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_ATTACHMENT_STORE"]


def _get_source_code_qa_generated_artifact_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQAGeneratedArtifactStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_GENERATED_ARTIFACT_STORE"]


def _get_source_code_qa_runtime_evidence_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQARuntimeEvidenceStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_RUNTIME_EVIDENCE_STORE"]


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


def _is_local_agent_unavailable_error(error: ToolError) -> bool:
    message = str(error or "").strip().lower()
    if "local-agent" not in message and "local agent" not in message:
        return False
    unavailable_markers = (
        "unavailable",
        "offline",
        "connection refused",
        "connecttimeout",
        "read timed out",
        "timed out",
        "max retries exceeded",
        "err_ngrok_3200",
        "endpoint",
    )
    return any(marker in message for marker in unavailable_markers)


def _tool_error_category(error: ToolError) -> str:
    return "local_agent_unavailable" if _is_local_agent_unavailable_error(error) else "tool_error"


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


def _is_portal_admin(email: str | None = None) -> bool:
    current_email = str(email or _current_google_email() or "").strip().lower()
    return current_email == PORTAL_ADMIN_EMAIL


def _is_portal_user(email: str | None = None) -> bool:
    current_email = str(email or _current_google_email() or "").strip().lower()
    return bool(
        current_email
        and (
            current_email.endswith("@npt.sg")
            or current_email == PORTAL_TEST_USER_EMAIL
        )
    )


def _current_google_user_is_blocked(settings: Settings) -> bool:
    if not _shared_portal_enabled(settings):
        return False
    email = _current_google_email()
    if not email:
        return False
    return not _is_portal_user(email)


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
    return _is_portal_user()


def _can_access_prd_self_assessment(settings: Settings) -> bool:
    return _is_portal_user()


def _can_access_gmail_seatalk_demo(settings: Settings) -> bool:
    return _is_portal_admin()


def _can_access_meeting_recorder(settings: Settings) -> bool:
    return _is_portal_admin()


def _can_access_source_code_qa(settings: Settings) -> bool:
    return _is_portal_user()


def _can_manage_source_code_qa(settings: Settings) -> bool:
    return _is_portal_admin()


def _can_access_work_memory(settings: Settings) -> bool:
    return _is_portal_admin()


def _source_code_qa_auth_payload(settings: Settings) -> dict[str, Any]:
    email = _current_google_email()
    owner_email = settings.source_code_qa_owner_email.strip().lower()
    normalized_admins = {PORTAL_ADMIN_EMAIL}
    if _is_portal_admin(email):
        match_source = "portal_admin"
    else:
        match_source = ""
    return {
        "signed_in_email": email,
        "can_manage": _is_portal_admin(email),
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
    normalized_provider = SourceCodeQAService.normalize_query_llm_provider(llm_provider)
    resolved = service if llm_provider is None else service.with_llm_provider(normalized_provider)
    if _local_agent_source_code_qa_enabled(current_app.config["SETTINGS"]):
        return RemoteSourceCodeQAService(_build_local_agent_client(current_app.config["SETTINGS"]), service, llm_provider=normalized_provider or resolved.llm_provider_name)
    return resolved


def _source_code_qa_query_sync_mode(settings: Settings) -> str:
    mode = str(os.getenv("SOURCE_CODE_QA_QUERY_SYNC_MODE") or "").strip().lower()
    if mode in {"blocking", "background", "disabled"}:
        return mode
    return "background" if _local_agent_source_code_qa_enabled(settings) else "blocking"


def _source_code_qa_scope_has_queryable_index(service: Any, key: str) -> bool:
    try:
        health = service.index_health_payload()
    except Exception:  # noqa: BLE001
        current_app.logger.warning("Source Code Q&A index health check failed before query auto-sync.", exc_info=True)
        return True
    scope = (health.get("keys") or {}).get(key) if isinstance(health, dict) else None
    repos = scope.get("repos") if isinstance(scope, dict) else []
    if not isinstance(repos, list):
        return True
    return any(
        (repo.get("index") or {}).get("queryable")
        and str((repo.get("index") or {}).get("state") or "").lower() in {"ready", "stale"}
        for repo in repos
        if isinstance(repo, dict)
    )


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
        if not _source_code_qa_scope_has_queryable_index(service, key):
            if progress_callback:
                progress_callback("auto_sync_check", "Preparing the first repository index for this scope.", 0, 1)
            result = service.ensure_synced_today(pm_team=pm_team, country=country)
            if progress_callback:
                progress_callback("auto_sync_completed", "Repository index is ready; starting code search.", 1, 1)
            return result
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


def _proxy_local_agent_request(agent_path: str, *, base_url: str | None = None, use_api_prefix: bool | None = None):
    settings: Settings = current_app.config["SETTINGS"]
    configured_base_url = (base_url if base_url is not None else settings.local_agent_base_url or "").strip().rstrip("/")
    local_base_url = configured_base_url or _local_agent_loopback_base_url()
    normalized_agent_path = agent_path.lstrip("/")
    if use_api_prefix is None:
        use_api_prefix = bool(configured_base_url)
    if use_api_prefix:
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


def _uat_local_agent_loopback_base_url() -> str:
    value = str(os.environ.get("UAT_LOCAL_AGENT_LOOPBACK_BASE_URL") or "").strip().rstrip("/")
    if value:
        return value
    host = str(os.environ.get("UAT_LOCAL_AGENT_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    port = str(os.environ.get("UAT_LOCAL_AGENT_PORT") or "7008").strip() or "7008"
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


def _local_agent_meeting_recorder_enabled(settings: Settings) -> bool:
    return bool(
        _local_agent_mode_enabled(settings)
        and settings.local_agent_base_url
        and settings.local_agent_hmac_secret
    )


def _local_agent_work_memory_enabled(settings: Settings) -> bool:
    return bool(
        _local_agent_mode_enabled(settings)
        and settings.local_agent_base_url
        and settings.local_agent_hmac_secret
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
    options["runtime_capabilities"] = _source_code_qa_runtime_capabilities_payload()
    return options


def _source_code_qa_runtime_capabilities_payload() -> dict[str, dict[str, dict[str, bool]]]:
    teams = ("AF", "GRC", "CRMS")
    countries = (ALL_COUNTRY, *tuple(CRMS_COUNTRIES))
    capabilities = {
        team: {
            country: {"hasConfig": False, "hasDB": False, "hasDictionary": False}
            for country in countries
        }
        for team in teams
    }
    try:
        store = _get_source_code_qa_runtime_evidence_store()
        for team in teams:
            for country in countries:
                try:
                    evidence_items = store.list(pm_team=team, country=country)
                except ToolError:
                    evidence_items = []
                for item in evidence_items:
                    source_type = str(item.get("source_type") or "").strip().lower()
                    if source_type == "apollo":
                        capabilities[team][country]["hasConfig"] = True
                    elif source_type == "db":
                        capabilities[team][country]["hasDB"] = True
                    elif source_type == "data_dictionary":
                        capabilities[team][country]["hasDictionary"] = True
    except Exception:  # noqa: BLE001 - capability badges are advisory only.
        return capabilities
    return capabilities


def _source_code_qa_provider_available(llm_provider: str | None) -> bool:
    provider = SourceCodeQAService.normalize_query_llm_provider(llm_provider)
    return bool(_source_code_qa_model_availability().get(provider, False))


def _source_code_qa_public_answer_mode(answer_mode: str | None) -> str:
    mode = str(answer_mode or "auto").strip()
    return mode if mode in {"auto", "gemini_flash"} else "auto"


def _source_code_qa_query_mode(query_mode: str | None) -> str:
    return "deep"


def _source_code_qa_attachment_ids(payload: dict[str, Any]) -> list[str]:
    raw_ids = payload.get("attachment_ids") if isinstance(payload, dict) else []
    if raw_ids is None:
        return []
    if not isinstance(raw_ids, list):
        raise ToolError("attachment_ids must be a list.")
    attachment_ids = [str(item or "").strip() for item in raw_ids if str(item or "").strip()]
    if len(attachment_ids) > SourceCodeQAAttachmentStore.MAX_ATTACHMENTS:
        raise ToolError(f"At most {SourceCodeQAAttachmentStore.MAX_ATTACHMENTS} attachments are supported per Source Code Q&A question.")
    return attachment_ids


def _resolve_source_code_qa_query_attachments(
    payload: dict[str, Any],
    *,
    owner_email: str,
    session_id: str,
) -> list[dict[str, Any]]:
    attachment_ids = _source_code_qa_attachment_ids(payload)
    if not attachment_ids:
        return []
    if not session_id:
        raise ToolError("A Source Code Q&A session is required before sending attachments.")
    session_payload = _get_source_code_qa_session_store().get(session_id, owner_email=owner_email)
    if session_payload is None:
        raise ToolError("Source Code Q&A session was not found for these attachments.")
    return _get_source_code_qa_attachment_store().resolve_many(
        owner_email=owner_email,
        session_id=session_id,
        attachment_ids=attachment_ids,
    )


def _source_code_qa_public_attachments(attachments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        SourceCodeQAAttachmentStore.public_metadata(item)
        for item in attachments
        if isinstance(item, dict)
    ]


def _resolve_source_code_qa_runtime_evidence(*, pm_team: str, country: str) -> list[dict[str, Any]]:
    try:
        return _get_source_code_qa_runtime_evidence_store().resolve_scope(pm_team=pm_team, country=country)
    except ToolError:
        raise
    except Exception as error:  # noqa: BLE001 - runtime evidence must not break code Q&A.
        current_app.logger.warning("Source Code Q&A runtime evidence could not be loaded: %s", error)
        return []


def _source_code_qa_public_runtime_evidence(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        SourceCodeQARuntimeEvidenceStore.public_metadata(item)
        for item in evidence
        if isinstance(item, dict)
    ]


def _source_code_qa_public_generated_artifacts(artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        SourceCodeQAGeneratedArtifactStore.public_metadata(item)
        for item in artifacts
        if isinstance(item, dict)
    ]


def _build_source_code_qa_generated_artifacts(
    *,
    owner_email: str,
    session_id: str,
    pm_team: str,
    country: str,
    question: str,
    result: dict[str, Any],
    runtime_evidence: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    answer_text = str(result.get("llm_answer") or result.get("answer") or result.get("rendered_answer") or "")
    sql_blocks = _extract_source_code_qa_sql_blocks(answer_text)
    if not sql_blocks:
        return []
    try:
        artifact = _get_source_code_qa_generated_artifact_store().save_sql_package(
            owner_email=owner_email,
            session_id=session_id,
            pm_team=pm_team,
            country=country,
            question=question,
            sql=sql_blocks[0],
            readme=_build_source_code_qa_sql_readme(
                pm_team=pm_team,
                country=country,
                question=question,
                sql=sql_blocks[0],
                result=result,
                runtime_evidence=runtime_evidence,
            ),
        )
    except Exception as error:  # noqa: BLE001 - artifact packaging must not fail the answer.
        current_app.logger.warning("Source Code Q&A generated SQL artifact could not be saved: %s", error)
        return []
    return [artifact]


def _source_code_qa_effort_assessment_language(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"en", "english"}:
        return "en"
    return "zh"


def _source_code_qa_effort_sentences(text: str) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    parts = re.split(r"(?<=[。！？!?])\s+|\n+", raw)
    return [part.strip() for part in parts if part.strip()]


def _source_code_qa_effort_matches(text: str, patterns: list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in patterns)


def _source_code_qa_effort_unique(items: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for item in items:
        if item is None:
            continue
        key = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, dict) else str(item)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _source_code_qa_load_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


@lru_cache(maxsize=1)
def _load_source_code_qa_effort_dictionaries() -> dict[str, Any]:
    return _source_code_qa_load_json_file(SOURCE_CODE_QA_EFFORT_DICTIONARY_PATH)


@lru_cache(maxsize=1)
def _load_source_code_qa_domain_profile_config() -> dict[str, Any]:
    return _source_code_qa_load_json_file(SOURCE_CODE_QA_DOMAIN_PROFILES_PATH)


@lru_cache(maxsize=1)
def _load_source_code_qa_domain_knowledge_config() -> dict[str, Any]:
    return _source_code_qa_load_json_file(SOURCE_CODE_QA_DOMAIN_KNOWLEDGE_PATH)


def _source_code_qa_effort_domain_entries(pm_team: str) -> list[dict[str, Any]]:
    dictionaries = _load_source_code_qa_effort_dictionaries()
    domain = ((dictionaries.get("domains") or {}).get(str(pm_team or "").upper()) or {})
    entries = domain.get("entries") if isinstance(domain, dict) else []
    entries = entries if isinstance(entries, list) else []
    return [entry for entry in entries if isinstance(entry, dict)]


SOURCE_CODE_QA_EFFORT_SCOPE_ALIASES: dict[str, list[str]] = {
    "CRMS": [
        "crms",
        "credit risk",
        "credit risk admin",
        "bti",
        "cbs",
        "apc",
        "manual review",
        "monthly review",
        "suspension",
        "appeal",
        "income review",
        "b score",
        "underwriting",
        "cashline",
        "cash installment",
        "myinfo",
        "cpf",
        "noa",
        "dwh_creditrisk",
    ],
    "GRC": [
        "grc",
        "parameter management",
        "authorization management",
        "global lock",
        "globallock",
        "bcf_global_lock",
        "function unit",
        "functionunit",
        "parameterEditFunctionUnit",
        "approval visibility",
    ],
    "AF": [
        "anti-fraud",
        "anti fraud",
        "fraud",
        "risk decision",
        "risk flow engine",
        "blacklist",
        "whitelist",
        "black white list",
        "flow report",
        "crc",
        "transactionid",
    ],
}

SOURCE_CODE_QA_EFFORT_SCOPE_COMMON_TERMS = {
    "api",
    "service",
    "strategy",
    "workflow",
    "config",
    "configuration",
    "table",
    "mapper",
    "sql",
    "frontend",
    "screen",
    "page",
    "approval",
    "review",
    "report",
    "reporting",
    "submission",
    "downstream",
    "integration",
    "test",
    "rule",
    "risk",
    "status",
    "permission",
    "role",
    "parameter",
    "limit",
    "credit",
    "income",
}


def _source_code_qa_effort_country_hint(requirement: str, fallback: str) -> str:
    text = f" {str(requirement or '').lower()} "
    if re.search(r"(?<![a-z0-9])sg(?![a-z0-9])", text) or "singapore" in text:
        return "SG"
    if re.search(r"(?<![a-z0-9])id(?![a-z0-9])", text) or "indonesia" in text:
        return "ID"
    if re.search(r"(?<![a-z0-9])ph(?![a-z0-9])", text) or "philippines" in text:
        return "PH"
    return str(fallback or "").strip() or "All"


def _source_code_qa_effort_term_matches(text: str, term: str) -> bool:
    normalized_text = str(text or "").lower()
    normalized_term = str(term or "").strip().lower()
    if not normalized_term:
        return False
    if len(normalized_term) <= 3 and re.fullmatch(r"[a-z0-9]+", normalized_term):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(normalized_term)}(?![a-z0-9])", normalized_text))
    return normalized_term in normalized_text


def _source_code_qa_effort_scope_terms_by_team() -> dict[str, list[str]]:
    teams = set(SOURCE_CODE_QA_EFFORT_SCOPE_ALIASES)
    dictionaries = _load_source_code_qa_effort_dictionaries()
    teams.update(str(team or "").upper() for team in ((dictionaries.get("domains") or {}).keys()))
    knowledge = _load_source_code_qa_domain_knowledge_config()
    teams.update(str(team or "").upper() for team in (((knowledge.get("domains") or {}) if isinstance(knowledge, dict) else {}).keys()))
    terms_by_team: dict[str, list[str]] = {}
    for team in sorted(team for team in teams if team):
        terms: list[str] = list(SOURCE_CODE_QA_EFFORT_SCOPE_ALIASES.get(team, []))
        for entry in _source_code_qa_effort_domain_entries(team):
            terms.append(str(entry.get("id") or ""))
            for key in ("business_aliases", "technical_terms", "product_terms", "limit_terms", "evidence_hints"):
                terms.extend(str(item) for item in (entry.get(key) or []) if item)
        domain = ((knowledge.get("domains") or {}).get(team) or {}) if isinstance(knowledge, dict) else {}
        if isinstance(domain, dict):
            for module in domain.get("module_map") or []:
                if not isinstance(module, dict):
                    continue
                terms.append(str(module.get("name") or ""))
                for key in ("aliases", "repo_hints", "code_hints", "business_flows"):
                    terms.extend(str(item) for item in (module.get(key) or []) if item)
            for item in domain.get("terminology") or []:
                if not isinstance(item, dict):
                    continue
                terms.append(str(item.get("term") or ""))
                terms.extend(str(value) for value in (item.get("aliases") or []) if value)
                terms.extend(str(value) for value in (item.get("code_terms") or []) if value)
            retrieval_terms = domain.get("retrieval_terms") if isinstance(domain.get("retrieval_terms"), dict) else {}
            for values in retrieval_terms.values():
                terms.extend(str(item) for item in (values or []) if item)
        filtered = []
        for term in _source_code_qa_effort_unique(terms):
            normalized = str(term or "").strip()
            lowered = normalized.lower()
            if len(lowered) < 3 or lowered in SOURCE_CODE_QA_EFFORT_SCOPE_COMMON_TERMS:
                continue
            filtered.append(normalized)
        terms_by_team[team] = filtered[:120]
    return terms_by_team


def _source_code_qa_effort_scope_guard(
    *,
    pm_team: str,
    country: str,
    requirement: str,
) -> dict[str, Any]:
    selected_team = str(pm_team or "").strip().upper()
    terms_by_team = _source_code_qa_effort_scope_terms_by_team()
    scores: dict[str, int] = {}
    matched_terms: dict[str, list[str]] = {}
    for team, terms in terms_by_team.items():
        score = 0
        hits: list[str] = []
        for term in terms:
            if not _source_code_qa_effort_term_matches(requirement, term):
                continue
            lowered = str(term).lower()
            explicit = lowered in {item.lower() for item in SOURCE_CODE_QA_EFFORT_SCOPE_ALIASES.get(team, [])}
            score += 6 if explicit else 2
            hits.append(str(term))
        if selected_team == team and selected_team and _source_code_qa_effort_term_matches(requirement, selected_team):
            score += 8
            hits.append(selected_team)
        scores[team] = score
        matched_terms[team] = _source_code_qa_effort_unique(hits)[:12]
    best_team = max(scores, key=lambda key: scores.get(key, 0), default=selected_team)
    selected_score = scores.get(selected_team, 0)
    best_score = scores.get(best_team, 0)
    mismatch = (
        bool(selected_team)
        and bool(best_team)
        and best_team != selected_team
        and best_score >= 10
        and best_score >= selected_score + 6
        and best_score >= selected_score * 2 + 4
    )
    return {
        "status": "mismatch" if mismatch else "ok",
        "selected_pm_team": selected_team,
        "selected_country": str(country or "").strip() or "All",
        "suggested_pm_team": best_team if mismatch else selected_team,
        "suggested_country": _source_code_qa_effort_country_hint(requirement, country) if mismatch else str(country or "").strip() or "All",
        "scores": scores,
        "matched_terms": matched_terms,
        "reason": (
            f"Requirement terms match {best_team} more strongly than selected {selected_team}."
            if mismatch
            else "Selected scope is compatible with requirement terms."
        ),
    }


def _source_code_qa_effort_scope_mismatch_result(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
    llm_provider: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    scope_guard: dict[str, Any],
) -> dict[str, Any]:
    suggested_team = str(scope_guard.get("suggested_pm_team") or "").strip() or "the correct PM team"
    suggested_country = str(scope_guard.get("suggested_country") or "").strip() or "the correct country"
    matched = ", ".join(str(item) for item in (scope_guard.get("matched_terms") or {}).get(suggested_team, [])[:8])
    if language == "zh":
        answer = "\n".join(
            [
                "Scope Mismatch / 选择范围不匹配",
                f"当前选择的是 {pm_team}:{country}，但需求文本更像 {suggested_team}:{suggested_country} 的改动。",
                f"命中的范围信号: {matched or '未记录'}。",
                f"请切换 PM Team 到 {suggested_team}、Country 到 {suggested_country} 后重新运行 Effort Assessment。",
                "这次不会基于当前 repo 生成 BE/FE 人天估算，避免把错误代码库里的能力硬套到需求上。",
            ]
        )
    else:
        answer = "\n".join(
            [
                "Scope Mismatch",
                f"The selected scope is {pm_team}:{country}, but the requirement matches {suggested_team}:{suggested_country} more strongly.",
                f"Matched scope signals: {matched or 'not recorded'}.",
                f"Switch PM Team to {suggested_team} and Country to {suggested_country}, then run Effort Assessment again.",
                "No BE/FE estimate was generated from the selected repository scope to avoid misleading output.",
            ]
        )
    missing_evidence = [
        f"Selected repository scope {pm_team}:{country} does not match requirement signals for {suggested_team}:{suggested_country}."
    ]
    return {
        "status": "scope_mismatch",
        "summary": "Selected repository scope does not match the requirement.",
        "llm_answer": answer,
        "llm_provider": llm_provider or "default",
        "llm_model": "",
        "trace_id": f"effort-scope-{hashlib.sha1(str(requirement or '').encode('utf-8')).hexdigest()[:12]}",
        "matches": [],
        "citations": [],
        "missing_evidence": missing_evidence,
        "assessment_confidence": "scope_mismatch",
        "effort_evidence_status": "scope_mismatch",
        "effort_scope_guard": scope_guard,
        "effort_evidence_matrix": {"version": 1, "groups": [], "quality": {"confirmed_group_count": 0, "inferred_group_count": 0, "missing_group_count": 0, "status": "scope_mismatch"}},
        "effort_evidence_matrix_quality": {"confirmed_group_count": 0, "inferred_group_count": 0, "missing_group_count": 0, "status": "scope_mismatch"},
        "effort_generic_output_guard": {"status": "blocked", "issues": ["scope_mismatch"], "confirmed_or_inferred_group_count": 0},
        "effort_timing": {"cache_hit": False, "scope_guard": scope_guard},
        "structured_assessment": {
            "version": 2,
            "language": language,
            "confidence": "scope_mismatch",
            "business_understanding": business_plan,
            "code_change_points": [],
            "be_estimate": [],
            "fe_estimate": [],
            "confirmed_evidence": [],
            "inferred_impact": [],
            "missing_evidence": missing_evidence,
            "questions": [f"Should this request be assessed under {suggested_team}:{suggested_country}?"],
        },
        "assessment": {
            "type": "effort_assessment",
            "pm_team": pm_team,
            "country": country,
            "language": language,
            "requirement": requirement,
            "business_plan": business_plan,
            "technical_candidates": technical_candidates,
            "estimation_rubric": estimation_rubric,
            "structured_assessment": {},
            "confidence": "scope_mismatch",
            "missing_evidence": missing_evidence,
            "evidence_status": "scope_mismatch",
            "scope_guard": scope_guard,
        },
    }


def _source_code_qa_effort_seed_terms(pm_team: str) -> list[str]:
    team = str(pm_team or "").upper()
    terms: list[str] = []
    profiles = _load_source_code_qa_domain_profile_config()
    profile = profiles.get(team) if isinstance(profiles, dict) else {}
    if isinstance(profile, dict):
        for key in ("data_carriers", "source_terms", "api_terms", "config_terms", "logic_terms", "field_population_terms"):
            terms.extend(str(item) for item in (profile.get(key) or []) if item)
    knowledge = _load_source_code_qa_domain_knowledge_config()
    domain = ((knowledge.get("domains") or {}).get(team) or {}) if isinstance(knowledge, dict) else {}
    if isinstance(domain, dict):
        for module in domain.get("module_map") or []:
            if not isinstance(module, dict):
                continue
            terms.append(str(module.get("name") or ""))
            terms.extend(str(item) for item in (module.get("aliases") or []) if item)
            terms.extend(str(item) for item in (module.get("code_hints") or []) if item)
        for term in domain.get("terminology") or []:
            if not isinstance(term, dict):
                continue
            terms.append(str(term.get("term") or ""))
            terms.extend(str(item) for item in (term.get("aliases") or []) if item)
            terms.extend(str(item) for item in (term.get("code_terms") or []) if item)
        retrieval_terms = domain.get("retrieval_terms") if isinstance(domain.get("retrieval_terms"), dict) else {}
        for values in retrieval_terms.values():
            terms.extend(str(item) for item in (values or []) if item)
    return [str(item) for item in _source_code_qa_effort_unique([item for item in terms if str(item or "").strip()])]


def _source_code_qa_effort_entry_applies(entry: dict[str, Any], *, country: str, requirement: str) -> bool:
    countries = [str(item).upper() for item in (entry.get("country_terms") or []) if item]
    if countries and str(country or "").upper() not in countries:
        return False
    aliases = [str(item) for item in (entry.get("business_aliases") or []) if str(item or "").strip()]
    technical_terms = [str(item) for item in (entry.get("technical_terms") or []) if str(item or "").strip()]
    haystack = str(requirement or "").lower()
    for value in [*aliases, *technical_terms]:
        normalized = value.lower().strip()
        if not normalized:
            continue
        if normalized in haystack:
            return True
    return False


def _source_code_qa_effort_group_typed_candidates(entries: list[dict[str, Any]], *, seed_terms: list[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {
        "backend_service": [],
        "frontend_surface": [],
        "table_or_config": [],
        "workflow_rule": [],
        "downstream_reporting": [],
    }
    for entry in entries:
        terms = [str(item) for item in (entry.get("technical_terms") or []) if item]
        for surface in entry.get("surfaces") or []:
            surface_key = str(surface or "").strip()
            if surface_key in grouped:
                grouped[surface_key].extend(terms)
    if seed_terms:
        grouped["backend_service"].extend(seed_terms[:30])
    return {key: _source_code_qa_effort_unique(values)[:60] for key, values in grouped.items()}


def _build_source_code_qa_effort_business_plan(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
) -> dict[str, Any]:
    raw_requirement = str(requirement or "").strip()
    sentences = _source_code_qa_effort_sentences(raw_requirement)
    option_matches = list(
        re.finditer(
            r"(方案\s*[一二12]|option\s*[12])\s*[:：]?\s*(.*?)(?=(?:方案\s*[一二12]|option\s*[12])\s*[:：]|$)",
            raw_requirement,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )
    options = []
    for index, match in enumerate(option_matches, start=1):
        body = re.sub(r"\s+", " ", match.group(2).strip())
        if body:
            options.append({"id": f"option_{index}", "label": match.group(1).strip(), "summary": body[:1200]})
    has_explicit_options = bool(options)
    if not options:
        options.append({"id": "option_1", "label": "single proposed change", "summary": raw_requirement[:1200]})

    user_segments = []
    if _source_code_qa_effort_matches(raw_requirement, [r"高收入", r"annual\s*income", r"income\s*[><=]", r"120k"]):
        user_segments.append("high income customers")
    if _source_code_qa_effort_matches(raw_requirement, [r"好用户", r"good\s+customer", r"premium"]):
        user_segments.append("qualified or good customers")

    products = []
    if _source_code_qa_effort_matches(raw_requirement, [r"信用卡", r"credit\s*card"]):
        products.append("credit card")
    if _source_code_qa_effort_matches(raw_requirement, [r"现金分期", r"cash\s*installment", r"cash\s*instalment"]):
        products.append("cash installment")
    if _source_code_qa_effort_matches(raw_requirement, [r"cashline", r"cash\s*line"]):
        products.append("cashline")

    limit_types = []
    if _source_code_qa_effort_matches(raw_requirement, [r"额度", r"limit"]):
        limit_types.append("limit amount")
    if _source_code_qa_effort_matches(raw_requirement, [r"信用卡.*额度", r"credit\s*card.*limit"]):
        limit_types.append("credit card limit")
    if _source_code_qa_effort_matches(raw_requirement, [r"现金分期.*(专项)?额度", r"cash\s*installment.*limit"]):
        limit_types.append("cash installment dedicated limit")
    if _source_code_qa_effort_matches(raw_requirement, [r"103", r"104", r"sub\s*product", r"子产品"]):
        limit_types.append("sub-product limit 103/104")

    flow_changes = []
    if _source_code_qa_effort_matches(raw_requirement, [r"申请", r"apply", r"application"]):
        flow_changes.append("application flow")
    if _source_code_qa_effort_matches(raw_requirement, [r"报送", r"submission", r"reporting"]):
        flow_changes.append("reporting or downstream submission")
    if _source_code_qa_effort_matches(raw_requirement, [r"用户教育", r"感知", r"引导", r"education", r"guide"]):
        flow_changes.append("user education or guidance")

    decision_points = [
        sentence[:800]
        for sentence in sentences
        if _source_code_qa_effort_matches(sentence, [r"核心问题", r"是否", r"是不是", r"可以讨论", r"确认", r"how", r"whether"])
    ]
    goals = []
    if _source_code_qa_effort_matches(raw_requirement, [r"策略区分", r"区分", r"separate", r"differentiat"]):
        goals.append("differentiate limit strategy by customer/product context")
    if _source_code_qa_effort_matches(raw_requirement, [r"感知", r"转化", r"教育", r"conversion"]):
        goals.append("make the limit or product path understandable to customers")
    if not goals:
        goals.append("assess technical impact for the requested business change")

    return {
        "raw_requirement": raw_requirement,
        "pm_team": pm_team,
        "country": country,
        "language": language,
        "business_goals": _source_code_qa_effort_unique(goals),
        "options": options,
        "has_explicit_options": has_explicit_options,
        "user_segments": _source_code_qa_effort_unique(user_segments),
        "products": _source_code_qa_effort_unique(products),
        "limit_types": _source_code_qa_effort_unique(limit_types),
        "flow_changes": _source_code_qa_effort_unique(flow_changes),
        "decision_points": _source_code_qa_effort_unique(decision_points)[:8],
    }


def _build_source_code_qa_effort_technical_candidates(
    *,
    pm_team: str,
    country: str,
    business_plan: dict[str, Any],
    requirement: str,
) -> dict[str, Any]:
    raw_requirement = str(requirement or "")
    seed_terms = _source_code_qa_effort_seed_terms(pm_team)
    domain_entries = _source_code_qa_effort_domain_entries(pm_team)
    matched_entries = [
        entry for entry in domain_entries
        if _source_code_qa_effort_entry_applies(entry, country=country, requirement=raw_requirement)
    ]
    terms = [
        "limit",
        "credit limit",
        "product limit",
        "sub product limit",
        "API",
        "config",
        "strategy",
        "workflow",
        "front end screen",
        "application flow",
    ]
    backend_surfaces = ["API validation", "service strategy", "workflow decision rule", "config lookup"]
    frontend_surfaces = ["limit display", "application entry", "customer guidance copy"]
    configs_or_tables = ["limitAmount", "productCode", "productType", "subProductCode"]
    product_terms = []
    limit_terms = []
    evidence_hints = []
    domain_notes = []
    for entry in matched_entries:
        terms.extend(str(item) for item in (entry.get("technical_terms") or []) if item)
        product_terms.extend(str(item) for item in (entry.get("product_terms") or []) if item)
        limit_terms.extend(str(item) for item in (entry.get("limit_terms") or []) if item)
        evidence_hints.extend(str(item) for item in (entry.get("evidence_hints") or []) if item)
        for surface in entry.get("surfaces") or []:
            surface_value = str(surface or "")
            if surface_value == "backend_service":
                backend_surfaces.append(str(entry.get("id") or "backend service impact"))
            elif surface_value == "frontend_surface":
                frontend_surfaces.append(str(entry.get("id") or "frontend impact"))
            elif surface_value in {"table_or_config", "downstream_reporting"}:
                configs_or_tables.extend(str(item) for item in (entry.get("technical_terms") or []) if item)
    terms.extend(seed_terms[:80])

    if str(pm_team or "").upper() == "CRMS":
        backend_surfaces.extend(
            [
                "CRMS underwriting and eligibility decision",
                "borrower/product/sub-product limit calculation",
                "cash installment limit strategy",
                "credit card daily consumption limit strategy",
                "cashline application or redirect flow",
                "downstream reporting payload",
            ]
        )
        frontend_surfaces.extend(
            [
                "credit card and cash installment limit display",
                "cashline application entry",
                "limit explanation and customer education",
            ]
        )
        domain_notes.append("CRMS dictionary v1: income-based credit, cash installment, cashline, and product/sub-product limits.")

    for key in ("products", "limit_types", "flow_changes"):
        for value in business_plan.get(key) or []:
            terms.append(str(value))
    if _source_code_qa_effort_matches(raw_requirement, [r"报送", r"submission", r"reporting"]):
        terms.extend(["reporting", "submission", "report payload"])
    if _source_code_qa_effort_matches(raw_requirement, [r"前端", r"展示", r"入口", r"引导", r"screen", r"display", r"entry"]):
        terms.extend(["screen", "display", "entry point", "guide copy"])

    return {
        "pm_team": pm_team,
        "country": country,
        "search_terms": _source_code_qa_effort_unique(terms)[:80],
        "backend_surfaces": _source_code_qa_effort_unique(backend_surfaces)[:40],
        "frontend_surfaces": _source_code_qa_effort_unique(frontend_surfaces)[:30],
        "configs_or_tables": _source_code_qa_effort_unique(configs_or_tables)[:50],
        "product_terms": _source_code_qa_effort_unique(product_terms)[:40],
        "limit_terms": _source_code_qa_effort_unique(limit_terms)[:40],
        "evidence_hints": _source_code_qa_effort_unique(evidence_hints)[:40],
        "matched_dictionary_entries": [str(entry.get("id") or "") for entry in matched_entries if entry.get("id")],
        "typed_candidates": _source_code_qa_effort_group_typed_candidates(matched_entries, seed_terms=seed_terms),
        "domain_notes": domain_notes,
    }


def _build_source_code_qa_effort_estimation_rubric(
    *,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> dict[str, Any]:
    text = " ".join(
        [
            " ".join(str(item) for item in business_plan.get("products") or []),
            " ".join(str(item) for item in business_plan.get("limit_types") or []),
            " ".join(str(item) for item in business_plan.get("flow_changes") or []),
            " ".join(str(item) for item in technical_candidates.get("backend_surfaces") or []),
            " ".join(str(item) for item in technical_candidates.get("frontend_surfaces") or []),
        ]
    )
    high_complexity = _source_code_qa_effort_matches(
        text,
        [r"underwriting", r"borrower", r"sub[-\s]?product", r"reporting", r"submission", r"授信", r"额度模型"],
    )
    medium_complexity = _source_code_qa_effort_matches(text, [r"api", r"service", r"strategy", r"workflow", r"limit"])
    frontend_required = bool(technical_candidates.get("frontend_surfaces"))
    option_estimates = []
    for index, option in enumerate(business_plan.get("options") or [], start=1):
        option_text = str(option.get("summary") or "")
        option_high = high_complexity or _source_code_qa_effort_matches(option_text, [r"cashline", r"独立", r"报送", r"模型", r"多产品"])
        option_medium = medium_complexity or _source_code_qa_effort_matches(option_text, [r"额度", r"limit", r"策略", r"rule"])
        be_range = "8-15 PD" if option_high else ("3-6 PD" if option_medium else "1-3 PD")
        fe_range = "3-6 PD" if _source_code_qa_effort_matches(option_text, [r"用户教育", r"感知", r"入口", r"展示", r"guide", r"display"]) else ("1-3 PD" if frontend_required else "0-1 PD")
        option_estimates.append(
            {
                "id": option.get("id") or f"option_{index}",
                "label": option.get("label") or f"Option {index}",
                "be_person_days": be_range,
                "fe_person_days": fe_range,
                "basis": "planning-grade estimate before Dev final sizing",
            }
        )
    return {
        "rules": [
            "Config or rule parameter only: low complexity.",
            "BE API plus service, strategy, or limit-flow change: medium complexity.",
            "Underwriting engine, limit model, reporting, or multi-product limit linkage: high complexity.",
            "FE display, guidance, application entry, and customer education are estimated separately.",
            "Final answer must separate confirmed evidence, inferred impact, and missing evidence.",
        ],
        "complexity_drivers": {
            "backend": "high" if high_complexity else ("medium" if medium_complexity else "low"),
            "frontend": "medium" if frontend_required else "low",
        },
        "option_estimates": option_estimates,
    }


def _source_code_qa_effort_json_block(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)[:12000]


def _build_source_code_qa_effort_assessment_prompt(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
    llm_provider: str,
    runtime_evidence: list[dict[str, Any]],
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
) -> str:
    team_label = str((TEAM_PROFILE_DEFAULTS.get(pm_team) or {}).get("label") or pm_team or "Selected PM Team").strip()
    output_language = "Chinese" if language == "zh" else "English"
    runtime_items = [
        item for item in runtime_evidence
        if isinstance(item, dict)
    ]
    runtime_summary = ", ".join(
        sorted(
            {
                f"{item.get('pm_team') or pm_team}:{item.get('country') or country}:{item.get('source_type') or 'runtime'}"
                for item in runtime_items
            }
        )
    ) or "none"
    raw_requirement = str(requirement or "").strip()[:8000]
    has_explicit_options = bool(business_plan.get("has_explicit_options"))
    technical_change_section = "方案 1/2 代码改动点" if has_explicit_options else "代码改动点"
    option_rule = (
        "- The requirement contains explicit alternatives; keep the original option labels and compare each option separately."
        if has_explicit_options
        else "- The requirement does not contain explicit Option 1/Option 2 alternatives; do not invent option labels. Use 'proposed change' instead."
    )
    return "\n".join(
        [
            "You are performing a Source Code Q&A Effort Assessment for a new business requirement.",
            "",
            "Context:",
            f"- PM Team: {pm_team} ({team_label})",
            f"- Country: {country}",
            f"- Answer language: {output_language}",
            f"- Selected model provider: {llm_provider or 'default'}",
            f"- Runtime evidence available: {len(runtime_items)} item(s): {runtime_summary}",
            "",
            "Original business requirement, verbatim:",
            raw_requirement,
            "",
            "Business plan extracted from the requirement:",
            _source_code_qa_effort_json_block(business_plan),
            "",
            "Technical candidates for repository evidence search:",
            _source_code_qa_effort_json_block(technical_candidates),
            "",
            "Estimation rubric:",
            _source_code_qa_effort_json_block(estimation_rubric),
            "",
            "Optimized assessment task:",
            "- Use the business plan and technical candidates as the focused search map. Do not rely only on the original business wording.",
            "- Map the requirement to likely impacted repositories, modules, files, APIs, tables, configs, scheduled jobs, front-end screens and components, and tests.",
            "- Use current source-code evidence internally as the basis for implementation impact and person-day estimates.",
            "- If exact table or path lookup misses, record it as a warning and continue with focused technical-candidate search.",
            "- Use runtime evidence only as supporting context.",
            "- Translate technical findings into business-readable change points. Do not expose evidence, citations, or file-path proof lists in the visible final answer.",
            "- Estimate BE and FE work as ranges in person-days. Use 0 person-days if no FE or BE change is found, but explain why.",
            "- Include QA/test and integration impact in the relevant BE/FE estimate notes instead of creating a third estimate bucket.",
            "",
            "Required output sections:",
            "1. 业务理解 / Business Understanding",
            f"2. {technical_change_section} / Code Change Points",
            "3. BE 人天 / BE Person-days",
            "4. FE 人天 / FE Person-days",
            "5. QA / Integration Impact",
            "6. Assumptions / Risks",
            "7. Confirmation Questions",
            "",
            "Output rules:",
            f"- Write the final answer in {output_language}.",
            option_rule,
            "- Keep the answer concise but specific enough for PM and engineering planning.",
            "- Do not include visible sections named Evidence, Source / Runtime Evidence, Confirmed / Inferred / Missing Evidence, or Missing Evidence.",
            "- Do not include source citations, S-id references such as [S1], file-path proof lists, or runtime-evidence filenames in the final answer.",
            "- Code change points must be understandable to business users: describe behavior, process, rule, UI, API, data, integration, and testing changes before technical names.",
            "- Person-day estimates must be ranges such as 1-2 PD or 3-5 PD, with one sentence explaining the driver for each range.",
            "- If source evidence is weak, still estimate with low confidence and state the planning assumption without adding an evidence section.",
        ]
    )


def _source_code_qa_effort_compact_terms(technical_candidates: dict[str, Any], requirement: str) -> list[str]:
    terms: list[str] = []
    for key in ("search_terms", "configs_or_tables", "product_terms", "limit_terms", "evidence_hints"):
        terms.extend(str(item) for item in (technical_candidates.get(key) or []) if str(item or "").strip())
    typed_candidates = technical_candidates.get("typed_candidates") if isinstance(technical_candidates.get("typed_candidates"), dict) else {}
    for values in typed_candidates.values():
        terms.extend(str(item) for item in (values or []) if str(item or "").strip())
    terms.extend(IDENTIFIER_PATTERN.findall(str(requirement or "")))
    return [str(item) for item in _source_code_qa_effort_unique(terms) if str(item or "").strip()][:36]


def _build_source_code_qa_effort_evidence_query(
    *,
    requirement: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> str:
    goals = ", ".join(str(item) for item in (business_plan.get("business_goals") or [])[:4])
    products = ", ".join(str(item) for item in (business_plan.get("products") or [])[:6])
    limit_types = ", ".join(str(item) for item in (business_plan.get("limit_types") or [])[:6])
    flow_changes = ", ".join(str(item) for item in (business_plan.get("flow_changes") or [])[:6])
    terms = ", ".join(_source_code_qa_effort_compact_terms(technical_candidates, requirement)[:28])
    dictionary_entries = ", ".join(str(item) for item in (technical_candidates.get("matched_dictionary_entries") or [])[:10])
    return "\n".join(
        [
            "Effort assessment evidence lookup. Find current source-code evidence for implementation impact.",
            f"Requirement summary: {str(requirement or '').strip()[:1200]}",
            f"Business goals: {goals or 'planning-grade technical impact assessment'}",
            f"Products: {products or 'n/a'}",
            f"Limit/flow terms: {', '.join(item for item in (limit_types, flow_changes) if item) or 'n/a'}",
            f"Technical search terms: {terms or 'n/a'}",
            f"Dictionary hits: {dictionary_entries or 'none'}",
            "Focus on impacted APIs, services, strategies, configs/tables, frontend screens, tests, and downstream/reporting paths.",
        ]
    )


def _source_code_qa_effort_evidence_digest(evidence_result: dict[str, Any]) -> dict[str, Any]:
    matches = evidence_result.get("matches") if isinstance(evidence_result.get("matches"), list) else []
    return {
        "index_freshness": evidence_result.get("index_freshness") or {},
        "matches": [
            {
                "repo": match.get("repo"),
                "path": match.get("path"),
                "line_start": match.get("line_start"),
                "line_end": match.get("line_end"),
                "retrieval": match.get("retrieval"),
                "trace_stage": match.get("trace_stage"),
            }
            for match in matches[:16]
            if isinstance(match, dict)
        ],
    }


def _source_code_qa_effort_runtime_digest(runtime_evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    digest: list[dict[str, Any]] = []
    for item in runtime_evidence:
        if not isinstance(item, dict):
            continue
        digest.append(
            {
                "source_type": item.get("source_type") or "",
                "filename": item.get("filename") or "",
                "sha256": hashlib.sha256(str(item.get("text") or "").encode("utf-8")).hexdigest()[:16],
            }
        )
    return digest


def _source_code_qa_effort_cache_key(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
    llm_provider: str,
    evidence_query: str,
    evidence_result: dict[str, Any],
    evidence_matrix: dict[str, Any],
    runtime_evidence: list[dict[str, Any]],
) -> str:
    dictionaries = _load_source_code_qa_effort_dictionaries()
    payload = {
        "version": 4,
        "pm_team": pm_team,
        "country": country,
        "language": language,
        "requirement_sha256": hashlib.sha256(str(requirement or "").encode("utf-8")).hexdigest(),
        "llm_provider": llm_provider or "default",
        "evidence_query_sha256": hashlib.sha256(str(evidence_query or "").encode("utf-8")).hexdigest(),
        "effort_dictionary_version": dictionaries.get("version"),
        "effort_dictionary_updated_at": dictionaries.get("updated_at"),
        "runtime_evidence": _source_code_qa_effort_runtime_digest(runtime_evidence),
        "evidence": _source_code_qa_effort_evidence_digest(evidence_result),
        "evidence_matrix": {
            "version": evidence_matrix.get("version") if isinstance(evidence_matrix, dict) else 0,
            "groups": [
                {
                    "key": group.get("key"),
                    "status": group.get("status"),
                    "terms": group.get("terms") or [],
                    "match_count": len(group.get("matches") or []),
                }
                for group in ((evidence_matrix or {}).get("groups") or [])
                if isinstance(group, dict)
            ],
        },
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _source_code_qa_effort_cache_root(settings: Settings) -> Path:
    return _team_portal_data_root(settings) / "source_code_qa" / "effort_assessment_cache"


def _load_source_code_qa_effort_cached_result(settings: Settings, cache_key: str) -> dict[str, Any] | None:
    try:
        path = _source_code_qa_effort_cache_root(settings) / f"{cache_key}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        return None
    result = dict(result)
    result["effort_cache_hit"] = True
    result["effort_cache_key"] = cache_key
    if isinstance(result.get("llm_route"), dict):
        result["llm_route"] = {**result["llm_route"], "task": "effort_assessment", "effort_cache_hit": True}
    return result


def _store_source_code_qa_effort_cached_result(settings: Settings, cache_key: str, result: dict[str, Any]) -> None:
    try:
        cache_root = _source_code_qa_effort_cache_root(settings)
        cache_root.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 4,
            "cache_key": cache_key,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "result": result,
        }
        (cache_root / f"{cache_key}.json").write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    except (OSError, TypeError):
        current_app.logger.warning("Could not store Source Code Q&A effort assessment cache.", exc_info=True)


def _source_code_qa_effort_compact_evidence(result: dict[str, Any]) -> dict[str, Any]:
    matches = result.get("matches") if isinstance(result.get("matches"), list) else []
    citations = result.get("citations") if isinstance(result.get("citations"), list) else []
    evidence_outline = result.get("evidence_outline") if isinstance(result.get("evidence_outline"), dict) else {}
    return {
        "status": result.get("status") or "",
        "summary": result.get("summary") or "",
        "answer_quality": result.get("answer_quality") or {},
        "evidence_outline": evidence_outline,
        "citations": citations[:12],
        "matches": [
            {
                "repo": match.get("repo"),
                "path": match.get("path"),
                "line_start": match.get("line_start"),
                "line_end": match.get("line_end"),
                "retrieval": match.get("retrieval"),
                "trace_stage": match.get("trace_stage"),
                "reason": match.get("reason"),
                "snippet": str(match.get("snippet") or "")[:900],
            }
            for match in matches[:12]
            if isinstance(match, dict)
        ],
    }


def _source_code_qa_effort_matrix_terms(
    *,
    key: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> list[str]:
    typed_candidates = technical_candidates.get("typed_candidates") if isinstance(technical_candidates.get("typed_candidates"), dict) else {}
    if key == "business_rule":
        values = (
            list(business_plan.get("business_goals") or [])
            + list(business_plan.get("products") or [])
            + list(business_plan.get("limit_types") or [])
            + list(business_plan.get("decision_points") or [])
            + list(technical_candidates.get("product_terms") or [])
            + list(technical_candidates.get("limit_terms") or [])
        )
    elif key == "workflow_api":
        values = (
            list(business_plan.get("flow_changes") or [])
            + list(technical_candidates.get("backend_surfaces") or [])
            + list(typed_candidates.get("backend_service") or [])
            + list(typed_candidates.get("api") or [])
            + list(typed_candidates.get("workflow") or [])
        )
    elif key == "config_table":
        values = (
            list(technical_candidates.get("configs_or_tables") or [])
            + list(typed_candidates.get("configuration") or [])
            + list(typed_candidates.get("table") or [])
        )
    elif key == "frontend_surface":
        values = list(technical_candidates.get("frontend_surfaces") or []) + list(typed_candidates.get("frontend_surface") or [])
    elif key == "downstream_reporting":
        values = (
            list(typed_candidates.get("downstream_reporting") or [])
            + list(typed_candidates.get("integration") or [])
            + list(typed_candidates.get("downstream") or [])
        )
    else:
        values = list(typed_candidates.get("test") or []) + ["test", "qa", "regression", "integration"]
    return _source_code_qa_effort_unique(str(item) for item in values if str(item or "").strip())[:12]


def _source_code_qa_effort_match_text(match: dict[str, Any]) -> str:
    return " ".join(
        str(match.get(field) or "")
        for field in ("repo", "path", "reason", "snippet", "retrieval", "trace_stage")
    ).lower()


def _source_code_qa_effort_matrix_quality(matrix: dict[str, Any]) -> dict[str, Any]:
    groups = matrix.get("groups") if isinstance(matrix, dict) else []
    if not isinstance(groups, list):
        groups = []
    counts = {"confirmed": 0, "inferred": 0, "missing": 0}
    for group in groups:
        if not isinstance(group, dict):
            continue
        status = str(group.get("status") or "missing")
        if status in counts:
            counts[status] += 1
    return {
        "confirmed_group_count": counts["confirmed"],
        "inferred_group_count": counts["inferred"],
        "missing_group_count": counts["missing"],
        "status": "confirmed" if counts["confirmed"] >= 3 and counts["missing"] == 0 else ("partial" if counts["confirmed"] else "planning_assumption"),
    }


def _build_source_code_qa_effort_evidence_matrix(
    *,
    evidence_result: dict[str, Any],
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> dict[str, Any]:
    matches = evidence_result.get("matches") if isinstance(evidence_result.get("matches"), list) else []
    group_defs = [
        ("business_rule", "Business rule / decision logic", ("rule", "decision", "limit", "amount", "income", "product", "strategy")),
        ("workflow_api", "Workflow / API / service path", ("api", "service", "controller", "workflow", "approval", "review", "appeal", "suspension")),
        ("config_table", "Config / table / parameter", ("config", "table", "mapper", "sql", "apollo", "properties", "param", "dictionary")),
        ("frontend_surface", "Frontend screen / operation path", ("frontend", "screen", "page", "component", "vue", "react", "template", "webform")),
        ("downstream_reporting", "Downstream / reporting / integration", ("report", "submission", "downstream", "dwh", "cbs", "integration", "mq", "sync")),
        ("tests", "Tests / QA regression", ("test", "spec", "qa", "regression", "integration")),
    ]
    groups: list[dict[str, Any]] = []
    for key, title, fallback_markers in group_defs:
        terms = _source_code_qa_effort_matrix_terms(
            key=key,
            business_plan=business_plan,
            technical_candidates=technical_candidates,
        )
        markers = [marker.lower() for marker in list(fallback_markers) + [term for term in terms if len(str(term)) >= 3]]
        group_matches: list[dict[str, Any]] = []
        for match in matches:
            if not isinstance(match, dict):
                continue
            match_text = _source_code_qa_effort_match_text(match)
            if any(str(marker).lower() in match_text for marker in markers):
                group_matches.append(
                    {
                        "repo": match.get("repo"),
                        "path": match.get("path"),
                        "line_start": match.get("line_start"),
                        "line_end": match.get("line_end"),
                        "reason": match.get("reason"),
                        "retrieval": match.get("retrieval"),
                    }
                )
        status = "confirmed" if group_matches else ("inferred" if terms else "missing")
        groups.append(
            {
                "key": key,
                "title": title,
                "status": status,
                "terms": terms,
                "matches": group_matches[:6],
                "planning_note": (
                    "Grounded by retrieved source-code references."
                    if status == "confirmed"
                    else (
                        "Candidate impact inferred from requirement and domain dictionary; visible answer must phrase this as a planning assumption."
                        if status == "inferred"
                        else "No source or candidate evidence found for this workstream."
                    )
                ),
            }
        )
    matrix = {
        "version": 1,
        "groups": groups,
    }
    matrix["quality"] = _source_code_qa_effort_matrix_quality(matrix)
    return matrix


def _source_code_qa_effort_generic_output_guard(answer: str, evidence_matrix: dict[str, Any]) -> dict[str, Any]:
    text = str(answer or "")
    generic_patterns = (
        "api validation, service strategy",
        "service strategy, workflow decision rule",
        "config lookup",
        "frontend_guidance",
        "test/regression suite",
    )
    issues = [pattern for pattern in generic_patterns if pattern.lower() in text.lower()]
    confirmed_or_inferred = [
        group for group in (evidence_matrix.get("groups") or [])
        if isinstance(group, dict) and group.get("status") in {"confirmed", "inferred"}
    ]
    if "code change" in text.lower() or "代码改动" in text:
        status = "ok" if not issues and confirmed_or_inferred else "warning"
    else:
        status = "warning"
        issues.append("missing_code_change_section")
    return {
        "status": status,
        "issues": _source_code_qa_effort_unique(issues),
        "confirmed_or_inferred_group_count": len(confirmed_or_inferred),
    }


def _build_source_code_qa_effort_compact_synthesis_prompt(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
    llm_provider: str,
    runtime_evidence: list[dict[str, Any]],
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    evidence_result: dict[str, Any],
    evidence_matrix: dict[str, Any],
) -> str:
    output_language = "Chinese" if language == "zh" else "English"
    return "\n".join(
        [
            "You are performing a Source Code Q&A Effort Assessment. Use the compact evidence pack below; do not restart broad repository exploration unless required evidence is contradictory.",
            "",
            "Context:",
            f"- PM Team: {pm_team}",
            f"- Country: {country}",
            f"- Answer language: {output_language}",
            f"- Selected model provider: {llm_provider or 'default'}",
            f"- Runtime evidence available: {len(runtime_evidence)} item(s)",
            "",
            "Original business requirement, verbatim:",
            str(requirement or "").strip()[:4000],
            "",
            "Business plan:",
            _source_code_qa_effort_json_block(business_plan),
            "",
            "Technical candidates:",
            _source_code_qa_effort_json_block(technical_candidates),
            "",
            "Estimation rubric:",
            _source_code_qa_effort_json_block(estimation_rubric),
            "",
            "Compact source-code evidence pack from the indexed repositories:",
            _source_code_qa_effort_json_block(_source_code_qa_effort_compact_evidence(evidence_result)),
            "",
            "Internal evidence matrix for planning quality. Use it for grounding only; do not expose it in the final answer:",
            _source_code_qa_effort_json_block(evidence_matrix),
            "",
            "Instructions:",
            "- Produce the final effort assessment from this evidence pack, business plan, runtime evidence, and rubric.",
            "- Use source-code evidence internally to decide impact, but do not expose evidence, citations, S-id references, file paths, or proof lists in the visible final answer.",
            "- Explain detailed code change points in business-readable language: behavior/process/rule/UI/API/data/integration/testing impact first, technical names only when useful.",
            "- Every visible code change point must be grounded in a confirmed or inferred evidence-matrix workstream; if a workstream is only inferred, phrase it as a planning assumption.",
            "- Do not output generic keyword strings such as API validation, service strategy, config lookup, frontend_guidance, or test/regression suite as change points.",
            "- Missing source-code evidence is acceptable; continue with low confidence and state planning assumptions without creating a visible evidence section.",
            "- Do not invent Option 1/Option 2 labels unless the original requirement had explicit alternatives.",
            "- Estimate BE and FE as person-day ranges and break down the driver by rule/workflow, backend/API, config/data, frontend, and integration/QA where applicable.",
            "- Include QA/test and integration impact inside the relevant BE/FE notes.",
            "",
            "Required output sections:",
            "1. 业务理解 / Business Understanding",
            "2. 代码改动点 / Code Change Points",
            "3. BE 人天 / BE Person-days",
            "4. FE 人天 / FE Person-days",
            "5. QA / Integration Impact",
            "6. Assumptions / Risks",
            "7. Confirmation Questions",
            "",
            "Do not include visible sections named Evidence, Source / Runtime Evidence, Confirmed / Inferred / Missing Evidence, Missing Evidence, or source/runtime proof.",
            "",
            f"Write the final answer in {output_language}.",
        ]
    )


def _source_code_qa_effort_missing_evidence(
    *,
    result: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> list[str]:
    missing: list[str] = []
    matches = result.get("matches") if isinstance(result, dict) else []
    if not isinstance(matches, list) or not matches:
        missing.append("No confirmed source-code references were found for the technical candidates.")
    exact_lookup = result.get("exact_lookup") if isinstance(result, dict) else None
    if isinstance(exact_lookup, dict) and exact_lookup.get("terms") and not exact_lookup.get("matched_terms"):
        missing.append("Exact table/path lookup did not match; assessment continued with focused candidate search.")
    if technical_candidates.get("backend_surfaces") and (not isinstance(matches, list) or not matches):
        missing.append("Backend impact surfaces need Dev confirmation against current repositories.")
    return _source_code_qa_effort_unique(missing)


def _source_code_qa_effort_confidence(result: dict[str, Any], missing_evidence: list[str]) -> str:
    matches = result.get("matches") if isinstance(result, dict) else []
    if str(result.get("status") or "").lower() == "no_match":
        return "low"
    if isinstance(matches, list) and len(matches) >= 3 and not missing_evidence:
        return "high"
    if isinstance(matches, list) and matches:
        return "medium"
    return "low"


def _source_code_qa_effort_code_change_points(
    *,
    language: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
) -> list[dict[str, str]]:
    is_zh = language == "zh"
    typed_candidates = technical_candidates.get("typed_candidates") if isinstance(technical_candidates.get("typed_candidates"), dict) else {}
    backend_terms = [str(item) for item in (technical_candidates.get("backend_surfaces") or [])[:6] if str(item or "").strip()]
    frontend_terms = [str(item) for item in (technical_candidates.get("frontend_surfaces") or [])[:6] if str(item or "").strip()]
    config_terms = [str(item) for item in (technical_candidates.get("configs_or_tables") or [])[:6] if str(item or "").strip()]
    reporting_terms = [str(item) for item in (typed_candidates.get("downstream_reporting") or [])[:6] if str(item or "").strip()]
    products = [str(item) for item in (business_plan.get("products") or [])[:4] if str(item or "").strip()]
    limit_types = [str(item) for item in (business_plan.get("limit_types") or [])[:4] if str(item or "").strip()]
    flow_changes = [str(item) for item in (business_plan.get("flow_changes") or [])[:4] if str(item or "").strip()]
    estimates = [item for item in (estimation_rubric.get("option_estimates") or []) if isinstance(item, dict)]

    def join_terms(items: list[str], fallback: str) -> str:
        return ", ".join(items) if items else fallback

    points: list[dict[str, str]] = []

    def add(area: str, change: str, technical_surface: str, impact: str) -> None:
        if not change.strip():
            return
        points.append(
            {
                "area": area,
                "change": change,
                "likely_technical_surface": technical_surface,
                "impact": impact,
            }
        )

    if flow_changes or products or limit_types:
        add(
            "业务规则 / Business Rules" if is_zh else "Business Rules",
            (
                f"把需求中的流程、产品和额度规则落到系统判断中，覆盖 {join_terms(flow_changes + products + limit_types, '当前业务规则')}。"
                if is_zh
                else f"Map the requested flow, product, and limit-rule changes into system decision logic for {join_terms(flow_changes + products + limit_types, 'the affected business rules')}."
            ),
            join_terms(backend_terms + config_terms, "service/config rule layer" if not is_zh else "服务/配置规则层"),
            "影响审批、授信、额度或流程判断口径。" if is_zh else "Affects approval, credit, limit, or workflow decisions.",
        )
    if backend_terms or typed_candidates.get("backend_service"):
        add(
            "后端服务 / Backend" if is_zh else "Backend",
            (
                f"调整后端接口、服务或策略逻辑，让新规则能在核心流程中被计算、校验和保存。"
                if is_zh
                else "Update backend APIs, services, or strategy logic so the new rule can be calculated, validated, and persisted in the core flow."
            ),
            join_terms(backend_terms or [str(item) for item in (typed_candidates.get("backend_service") or [])[:6]], "backend service/API layer"),
            "主要决定 BE 人天范围。" if is_zh else "This is the main driver for BE person-days.",
        )
    if config_terms or typed_candidates.get("configuration"):
        add(
            "配置与数据 / Config & Data" if is_zh else "Config & Data",
            (
                f"新增或调整配置、字典、表字段映射或参数，确保规则可配置且不同环境口径一致。"
                if is_zh
                else "Add or adjust configuration, dictionary, table-field mapping, or parameters so the rule is configurable and consistent across environments."
            ),
            join_terms(config_terms or [str(item) for item in (typed_candidates.get("configuration") or [])[:6]], "config/table mapping"),
            "需要迁移、参数发布或数据校验配合。" if is_zh else "May require migration, parameter rollout, or data validation.",
        )
    if frontend_terms or typed_candidates.get("frontend_surface"):
        add(
            "前端页面 / Frontend" if is_zh else "Frontend",
            (
                "调整页面入口、字段展示、提示文案或用户操作路径，让用户能理解并使用新的业务规则。"
                if is_zh
                else "Update screen entry points, field display, helper copy, or user flow so users can understand and use the new business rule."
            ),
            join_terms(frontend_terms or [str(item) for item in (typed_candidates.get("frontend_surface") or [])[:6]], "frontend screen/component"),
            "决定是否需要 FE 人天；无页面变化时可为 0-1 PD。" if is_zh else "Determines FE effort; can be 0-1 PD if no user-facing screen changes.",
        )
    if reporting_terms or typed_candidates.get("integration") or typed_candidates.get("downstream"):
        add(
            "下游与报送 / Integration" if is_zh else "Integration",
            (
                "检查并调整下游接口、报送字段或同步任务，避免新规则只在主流程生效但下游口径不一致。"
                if is_zh
                else "Check and adjust downstream APIs, reporting fields, or sync jobs so the new rule does not diverge between the main flow and downstream consumers."
            ),
            join_terms(reporting_terms, "downstream/reporting path" if not is_zh else "下游/报送链路"),
            "增加联调和回归测试成本。" if is_zh else "Adds integration and regression testing cost.",
        )
    add(
        "测试与验收 / QA" if is_zh else "QA",
        (
            "补充单元测试、接口测试和关键业务场景回归，覆盖正常、边界和回退场景。"
            if is_zh
            else "Add unit, API, and key business regression tests covering normal, boundary, and rollback scenarios."
        ),
        "test/regression suite",
        "测试工作包含在 BE/FE 估算说明中。" if is_zh else "Testing work is included in the BE/FE estimate notes.",
    )

    if estimates:
        estimate_summary = "; ".join(
            f"{item.get('label') or item.get('id') or 'option'}: BE {item.get('be_person_days') or 'n/a'}, FE {item.get('fe_person_days') or 'n/a'}"
            for item in estimates[:3]
        )
        for point in points:
            point.setdefault("estimate_hint", estimate_summary)
    return points[:6]


def _source_code_qa_effort_fallback_answer(
    *,
    language: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    missing_evidence: list[str],
) -> str:
    options = estimation_rubric.get("option_estimates") or []
    has_explicit_options = bool(business_plan.get("has_explicit_options"))
    option_lines = "\n".join(
        f"- {item.get('label')}: BE {item.get('be_person_days')}, FE {item.get('fe_person_days')} ({item.get('basis')})"
        for item in options
        if isinstance(item, dict)
    )
    code_change_points = _source_code_qa_effort_code_change_points(
        language=language,
        business_plan=business_plan,
        technical_candidates=technical_candidates,
        estimation_rubric=estimation_rubric,
    )
    point_lines = "\n".join(
        f"- {item['area']}: {item['change']} ({item['impact']})"
        for item in code_change_points
    )
    if language == "zh":
        goals = ", ".join(str(item) for item in business_plan.get("business_goals") or [])
        technical_title = "方案 1/2 代码改动点" if has_explicit_options else "代码改动点"
        confirmation_questions = [
            "- 额度策略是否只改参数，还是需要新增产品/子产品额度模型?",
            "- 是否需要前端新增 cashline 申请入口或额度解释文案?",
        ]
        if has_explicit_options:
            confirmation_questions.insert(0, "- 方案 1 和方案 2 是否二选一，还是都需要落地?")
        return "\n".join(
            [
                "业务理解",
                f"- 目标: {goals or '评估业务需求对应的技术改造范围'}",
                "",
                technical_title,
                point_lines or "- 按当前需求描述，需要调整业务规则、后端流程、可能的前端展示和测试回归范围。",
                "",
                "BE 人天 / FE 人天",
                option_lines or "- 单方案: BE 3-6 PD, FE 1-3 PD，低置信度。",
                "",
                "QA / Integration Impact",
                "- 需要覆盖核心业务路径、边界条件、配置发布和下游联调回归。",
                "",
                "Assumptions / Risks",
                "- 这是 planning-grade 低置信度估算，不替代 Dev final sizing。",
                "- 如果涉及授信引擎、额度模型、报送或多产品额度联动，BE 复杂度应按高复杂度处理。",
                "",
                "Confirmation Questions",
                *confirmation_questions,
            ]
        )
    technical_title = "Option Code Change Points" if has_explicit_options else "Code Change Points"
    confirmation_questions = [
        "- Is the requested limit change a config-only rule update or a new limit model?",
        "- Does the change require FE display, application entry, or customer education copy?",
    ]
    if has_explicit_options:
        confirmation_questions.insert(0, "- Are the listed options alternatives, or should more than one be implemented?")
    return "\n".join(
        [
            "Business Understanding",
            f"- Goals: {', '.join(str(item) for item in business_plan.get('business_goals') or [])}",
            "",
            technical_title,
            point_lines or "- Adjust business rules, backend flow, possible frontend display, and regression testing scope based on the requirement.",
            "",
            "BE / FE Person-days",
            option_lines or "- Single option: BE 3-6 PD, FE 1-3 PD, low confidence.",
            "",
            "QA / Integration Impact",
            "- Cover core business paths, boundary conditions, configuration rollout, and downstream integration regression.",
            "",
            "Assumptions / Risks",
            "- This is a planning-grade low-confidence estimate and does not replace Dev final sizing.",
            "",
            "Confirmation Questions",
            *confirmation_questions,
        ]
    )


def _build_source_code_qa_effort_structured_assessment(
    *,
    result: dict[str, Any],
    language: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    missing_evidence: list[str],
    confidence: str,
) -> dict[str, Any]:
    matches = result.get("matches") if isinstance(result.get("matches"), list) else []
    evidence_matrix = result.get("effort_evidence_matrix") if isinstance(result.get("effort_evidence_matrix"), dict) else {}
    evidence_groups = evidence_matrix.get("groups") if isinstance(evidence_matrix.get("groups"), list) else []
    confirmed_evidence = [
        {
            "repo": str(match.get("repo") or ""),
            "path": str(match.get("path") or ""),
            "line_start": match.get("line_start") or 0,
            "line_end": match.get("line_end") or 0,
        }
        for match in matches[:8]
        if isinstance(match, dict)
    ]
    typed_candidates = technical_candidates.get("typed_candidates") if isinstance(technical_candidates.get("typed_candidates"), dict) else {}
    inferred_impact = [
        {
            "surface": surface,
            "terms": [str(item) for item in (terms or [])[:12]],
        }
        for surface, terms in typed_candidates.items()
        if terms
    ]
    code_change_points = _source_code_qa_effort_code_change_points(
        language=language,
        business_plan=business_plan,
        technical_candidates=technical_candidates,
        estimation_rubric=estimation_rubric,
    )
    matrix_status_by_key = {
        str(group.get("key") or ""): str(group.get("status") or "missing")
        for group in evidence_groups
        if isinstance(group, dict)
    }
    workstream_by_area = (
        ("business_rule", ("business", "业务规则")),
        ("workflow_api", ("backend", "后端", "api", "服务")),
        ("config_table", ("config", "data", "配置", "数据")),
        ("frontend_surface", ("frontend", "前端", "页面")),
        ("downstream_reporting", ("integration", "下游", "报送", "联调")),
        ("tests", ("qa", "测试", "验收")),
    )
    for point in code_change_points:
        area_text = str(point.get("area") or "").lower()
        workstream_key = next(
            (
                key
                for key, markers in workstream_by_area
                if any(marker in area_text for marker in markers)
            ),
            "",
        )
        status = matrix_status_by_key.get(workstream_key, "inferred" if workstream_key else "missing")
        point["evidence_status"] = status
        point["workstream"] = workstream_key or "planning"
        if status == "inferred":
            point["planning_assumption"] = "Candidate impact inferred from requirement/domain dictionary; Dev confirmation required."
        elif status == "missing":
            point["planning_assumption"] = "No direct source evidence found; treat as planning assumption."
    return {
        "version": 2,
        "language": language,
        "confidence": confidence,
        "business_understanding": {
            "goals": business_plan.get("business_goals") or [],
            "user_segments": business_plan.get("user_segments") or [],
            "products": business_plan.get("products") or [],
            "limit_types": business_plan.get("limit_types") or [],
            "flow_changes": business_plan.get("flow_changes") or [],
            "decision_points": business_plan.get("decision_points") or [],
        },
        "option_impacts": [
            {
                "id": item.get("id") or "",
                "label": item.get("label") or "",
                "summary": item.get("summary") or "",
            }
            for item in (business_plan.get("options") or [])
            if isinstance(item, dict)
        ],
        "code_change_points": code_change_points,
        "be_estimate": [
            {
                "option_id": item.get("id") or "",
                "person_days": item.get("be_person_days") or "",
                "basis": item.get("basis") or "",
            }
            for item in (estimation_rubric.get("option_estimates") or [])
            if isinstance(item, dict)
        ],
        "fe_estimate": [
            {
                "option_id": item.get("id") or "",
                "person_days": item.get("fe_person_days") or "",
                "basis": item.get("basis") or "",
            }
            for item in (estimation_rubric.get("option_estimates") or [])
            if isinstance(item, dict)
        ],
        "confirmed_evidence": confirmed_evidence,
        "inferred_impact": inferred_impact,
        "missing_evidence": missing_evidence,
        "evidence_matrix_quality": evidence_matrix.get("quality") or _source_code_qa_effort_matrix_quality(evidence_matrix),
        "questions": (
            ["Are the listed options alternatives, or should more than one be implemented?"]
            if business_plan.get("has_explicit_options")
            else []
        ) + [
            "Is the requested limit change a config-only rule update or a new limit model?",
            "Does the change require FE display, application entry, or customer education copy?",
        ],
        "dictionary_entries": technical_candidates.get("matched_dictionary_entries") or [],
    }


def _source_code_qa_effort_sanitize_visible_answer(value: Any) -> str:
    text = str(value or "")
    if not text.strip():
        return ""
    allowed_heading_patterns = (
        "business understanding",
        "业务理解",
        "code change",
        "代码改动",
        "technical change",
        "技术改造",
        "be person",
        "be 人天",
        "fe person",
        "fe 人天",
        "qa",
        "integration",
        "assumptions",
        "risks",
        "assumptions / risks",
        "假设",
        "风险",
        "confirmation questions",
        "确认问题",
        "需要确认",
    )
    blocked_heading_patterns = (
        "confirmed / inferred / missing evidence",
        "source / runtime evidence",
        "source/runtime evidence",
        "missing evidence",
        "runtime evidence",
        "source evidence",
        "evidence",
        "证据",
    )

    def normalized_heading(line: str) -> str:
        value = re.sub(r"^[#*\s>\-]*", "", line.strip())
        value = re.sub(r"^\d+[\.)、]\s*", "", value)
        value = value.strip("*:： ").lower()
        return value

    output: list[str] = []
    skipping = False
    for line in text.splitlines():
        heading = normalized_heading(line)
        is_blocked = any(pattern in heading for pattern in blocked_heading_patterns)
        is_allowed = any(pattern in heading for pattern in allowed_heading_patterns)
        if is_blocked and not is_allowed:
            skipping = True
            continue
        if skipping and is_allowed:
            skipping = False
        if skipping:
            continue
        output.append(line)
    cleaned = "\n".join(output).strip()
    cleaned = re.sub(r"\s*\[S\d+\]", "", cleaned)
    cleaned = re.sub(
        r"(?:[\w.-]+/)+[\w.-]+\.(?:py|java|js|ts|tsx|vue|sql|xml|yaml|yml|properties|kt|go|rb|php|html|css|sh)(?::\d+(?:-\d+)?)?",
        "source module",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned


def _normalize_source_code_qa_effort_assessment_result(
    *,
    result: dict[str, Any],
    language: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    evidence_matrix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict(result or {})
    if evidence_matrix is not None:
        normalized["effort_evidence_matrix"] = evidence_matrix
    elif not isinstance(normalized.get("effort_evidence_matrix"), dict):
        normalized["effort_evidence_matrix"] = _build_source_code_qa_effort_evidence_matrix(
            evidence_result=normalized,
            business_plan=business_plan,
            technical_candidates=technical_candidates,
        )
    missing_evidence = _source_code_qa_effort_missing_evidence(
        result=normalized,
        technical_candidates=technical_candidates,
    )
    confidence = _source_code_qa_effort_confidence(normalized, missing_evidence)
    if str(normalized.get("status") or "").lower() == "no_match":
        normalized["status"] = "ok"
        normalized["effort_evidence_status"] = "warning"
        normalized["summary"] = "Effort assessment completed with low confidence because source-code evidence is missing."
        normalized["llm_answer"] = _source_code_qa_effort_fallback_answer(
            language=language,
            business_plan=business_plan,
            technical_candidates=technical_candidates,
            estimation_rubric=estimation_rubric,
            missing_evidence=missing_evidence,
        )
    else:
        normalized["effort_evidence_status"] = "warning" if missing_evidence else "confirmed"
        if not normalized.get("summary"):
            normalized["summary"] = "Effort assessment completed."
        normalized["llm_answer"] = _source_code_qa_effort_sanitize_visible_answer(normalized.get("llm_answer") or normalized.get("answer") or "")
    normalized["assessment_confidence"] = confidence
    normalized["missing_evidence"] = missing_evidence
    normalized["effort_evidence_matrix_quality"] = _source_code_qa_effort_matrix_quality(
        normalized.get("effort_evidence_matrix") if isinstance(normalized.get("effort_evidence_matrix"), dict) else {}
    )
    normalized["effort_generic_output_guard"] = _source_code_qa_effort_generic_output_guard(
        normalized.get("llm_answer") or "",
        normalized.get("effort_evidence_matrix") if isinstance(normalized.get("effort_evidence_matrix"), dict) else {},
    )
    normalized["structured_assessment"] = _build_source_code_qa_effort_structured_assessment(
        result=normalized,
        language=language,
        business_plan=business_plan,
        technical_candidates=technical_candidates,
        estimation_rubric=estimation_rubric,
        missing_evidence=missing_evidence,
        confidence=confidence,
    )
    return normalized



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
        "attachments": compact.get("attachments") or [],
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
        "query_mode": compact.get("query_mode") or "",
        "deadline_seconds": compact.get("deadline_seconds") or 0,
        "deadline_hit": bool(compact.get("deadline_hit")),
        "fallback_used": bool(compact.get("fallback_used")),
        "fallback_answer_quality": compact.get("fallback_answer_quality") or "",
        "fallback_evidence_count": compact.get("fallback_evidence_count") or 0,
        "fallback_claim_count": compact.get("fallback_claim_count") or 0,
        "deadline_fallback_reason": compact.get("deadline_fallback_reason") or "",
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
    message = f"SeaTalk Management is restricted to {PORTAL_ADMIN_EMAIL}."
    if not _can_access_gmail_seatalk_demo(settings):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("index"))
    return None


def _require_meeting_recorder_access(settings: Settings, *, api: bool = False):
    login_gate = _require_google_login(settings, api=api)
    if login_gate is not None:
        return login_gate
    message = f"Meeting Recorder is restricted to {PORTAL_ADMIN_EMAIL}."
    if not _can_access_meeting_recorder(settings):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("index"))
    return None


def _require_source_code_qa_access(settings: Settings, *, api: bool = False):
    login_gate = _require_google_login(settings, api=api)
    if login_gate is not None:
        return login_gate
    message = "Source Code Q&A is available to signed-in @npt.sg users and the configured test account."
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
        f"Source Code Q&A repository admin is restricted to {PORTAL_ADMIN_EMAIL}. "
        f"Signed in as {auth_payload['signed_in_email'] or 'unknown'}."
    )
    if not _can_manage_source_code_qa(settings):
        if api:
            return jsonify({"status": "error", "message": message, "auth": auth_payload}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("source_code_qa"))
    return None


def _require_work_memory_access(settings: Settings, *, api: bool = False):
    login_gate = _require_google_login(settings, api=api)
    if login_gate is not None:
        return login_gate
    message = f"AI Memory is restricted to {PORTAL_ADMIN_EMAIL}."
    if not _can_access_work_memory(settings):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("index"))
    return None


def _require_team_dashboard_access(settings: Settings, *, api: bool = False):
    login_gate = _require_google_login(settings, api=api)
    if login_gate is not None:
        return login_gate
    user_identity = _get_user_identity(settings)
    message = f"Team Dashboard is restricted to {PORTAL_ADMIN_EMAIL}."
    if not _can_access_team_dashboard(user_identity):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("access_denied"))
    return None


def _require_prd_self_assessment_access(settings: Settings, *, api: bool = False):
    login_gate = _require_google_login(settings, api=api)
    if login_gate is not None:
        return login_gate
    message = "PRD Self-Assessment is available to signed-in npt.sg users and the configured test account."
    if not _can_access_prd_self_assessment(settings):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("access_denied"))
    return None


def _run_prd_self_assessment_action(settings: Settings, *, action: str):
    access_gate = _require_prd_self_assessment_access(settings, api=True)
    if access_gate is not None:
        return access_gate
    payload = request.get_json(silent=True) or {}
    user_identity = _get_user_identity(settings)
    request_payload = {
        "owner_key": str(user_identity.get("config_key") or ""),
        "prd_url": str(payload.get("prd_url") or payload.get("page_ref") or ""),
        "language": str(payload.get("language") or "zh"),
        "force_refresh": bool(payload.get("force_refresh")),
    }
    event_prefix = f"prd_self_assessment_{action}"
    try:
        if _local_agent_source_code_qa_enabled(settings):
            client = _build_local_agent_client(settings)
            data = (
                client.prd_self_assessment_summary(request_payload)
                if action == "summary"
                else client.prd_self_assessment_review(request_payload)
            )
        else:
            service = _build_prd_review_service(settings)
            request_model = PRDBriefingReviewRequest(**request_payload)
            data = service.summarize_url(request_model) if action == "summary" else service.review_url(request_model)
        _log_portal_event(
            f"{event_prefix}_success",
            **_build_request_log_context(
                settings,
                user_identity=user_identity,
                extra={
                    "prd_url_hash": hashlib.sha256(request_payload["prd_url"].encode("utf-8")).hexdigest()[:12],
                    "language": request_payload["language"],
                    "cached": bool(data.get("cached")),
                },
            ),
        )
        _save_prd_latest_result(
            owner_key=request_payload["owner_key"],
            tool_key="prd_self_assessment",
            payload={"action": action, "payload": data},
        )
        return jsonify(data)
    except ToolError as error:
        error_details = _classify_portal_error(error)
        _log_portal_event(
            f"{event_prefix}_tool_error",
            level=logging.WARNING,
            **_build_request_log_context(
                settings,
                user_identity=user_identity,
                extra={**error_details, "language": request_payload["language"]},
            ),
        )
        return jsonify({"status": "error", "message": str(error), **error_details}), HTTPStatus.BAD_REQUEST
    except Exception as error:  # noqa: BLE001
        _log_portal_event(
            f"{event_prefix}_unexpected_error",
            level=logging.ERROR,
            **_build_request_log_context(settings, user_identity=user_identity, extra=_classify_portal_error(error)),
        )
        current_app.logger.exception("PRD Self-Assessment %s failed.", action)
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"PRD {action} failed unexpectedly. Please retry or share the request ID.",
                    **_classify_portal_error(error),
                }
            ),
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _save_prd_latest_result(*, owner_key: str, tool_key: str, payload: dict[str, Any]) -> None:
    if not owner_key:
        return
    store: BriefingStore = current_app.config["PRD_BRIEFING_STORE"]
    store.save_latest_tool_result(owner_key=owner_key, tool_key=tool_key, payload=payload)


def _get_prd_latest_result(*, owner_key: str, tool_key: str) -> dict[str, Any] | None:
    if not owner_key:
        return None
    store: BriefingStore = current_app.config["PRD_BRIEFING_STORE"]
    return store.get_latest_tool_result(owner_key=owner_key, tool_key=tool_key)


def _require_team_dashboard_monthly_report_access(settings: Settings, *, api: bool = False):
    access_gate = _require_team_dashboard_access(settings, api=api)
    if access_gate is not None:
        return access_gate
    user_identity = _get_user_identity(settings)
    if _can_access_team_dashboard_monthly_report(user_identity):
        return None
    message = "Monthly Report is restricted to xiaodong.zheng@npt.sg."
    if api:
        return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
    flash(message, "error")
    return redirect(url_for("access_denied"))


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
    return _is_portal_admin(str(user_identity.get("email") or ""))


def _can_access_team_dashboard(user_identity: dict[str, str | None]) -> bool:
    return _is_portal_admin(str(user_identity.get("email") or ""))


def _can_manage_team_dashboard(user_identity: dict[str, str | None]) -> bool:
    return _is_portal_admin(str(user_identity.get("email") or ""))


def _can_access_team_dashboard_monthly_report(user_identity: dict[str, str | None]) -> bool:
    return _is_portal_admin(str(user_identity.get("email") or ""))


def _can_edit_sync_email(user_identity: dict[str, str | None]) -> bool:
    return _is_portal_admin(str(user_identity.get("email") or ""))


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


def _classify_source_code_qa_job_error(message: str) -> dict[str, Any]:
    normalized = str(message or "").lower()
    if "local-agent" in normalized or "local agent" in normalized or "connection refused" in normalized:
        return {"error_category": "local_agent_offline", "error_code": "local_agent_unavailable", "error_retryable": True}
    if "ngrok" in normalized or "err_ngrok_3200" in normalized or "gateway" in normalized or "html error" in normalized:
        return {"error_category": "gateway_disconnected", "error_code": "gateway_disconnected", "error_retryable": True}
    if "rate limit" in normalized or "quota" in normalized:
        return {"error_category": "codex_timeout_or_rate_limit", "error_code": "llm_rate_limited", "error_retryable": True}
    if "timeout" in normalized or "timed out" in normalized:
        return {"error_category": "codex_timeout_or_rate_limit", "error_code": "llm_timeout", "error_retryable": True}
    return {"error_category": "job_failed", "error_code": "source_code_qa_job_failed", "error_retryable": True}


def _public_source_code_qa_job_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    payload = dict(snapshot)
    payload.pop("owner_email", None)
    payload.setdefault("status", "ok")
    payload["progress"] = snapshot.get("progress") if isinstance(snapshot.get("progress"), dict) else {
        "stage": snapshot.get("stage") or "",
        "current": snapshot.get("current") or 0,
        "total": snapshot.get("total") or 0,
        "message": snapshot.get("message") or "",
    }
    state = str(payload.get("state") or "")
    payload["query_mode"] = _source_code_qa_query_mode(payload.get("query_mode"))
    payload["queued_position"] = int(payload.get("queued_position") or 0)
    payload["eta_seconds_range"] = [
        max(0, int(value or 0))
        for value in (payload.get("eta_seconds_range") if isinstance(payload.get("eta_seconds_range"), list) else [])
    ][:2]
    payload["running_user_count"] = int(payload.get("running_user_count") or 0)
    payload["last_progress_at"] = float(payload.get("last_progress_at") or payload.get("updated_at") or 0)
    if payload.get("stalled_retryable"):
        payload["error_category"] = payload.get("error_category") or "job_stalled"
        payload["error_code"] = payload.get("error_code") or "job_stalled_retryable"
        payload["error_retryable"] = True
    if state == "running":
        payload.setdefault("error_category", "job_running")
        payload.setdefault("error_code", "")
        payload.setdefault("error_retryable", True)
    if state == "queued":
        payload.setdefault("error_category", "job_queued")
        payload.setdefault("error_code", "")
        payload.setdefault("error_retryable", True)
    if state == "failed":
        classification = _classify_source_code_qa_job_error(str(payload.get("error") or payload.get("message") or ""))
        for key, value in classification.items():
            if not payload.get(key):
                payload[key] = value
    return payload


def _source_code_qa_job_snapshot_for_current_user(job_id: str) -> dict[str, Any] | None:
    snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
    if snapshot is None or snapshot.get("action") != "source-code-qa-query":
        return None
    owner_email = str(snapshot.get("owner_email") or "").strip().lower()
    current_email = _current_google_email()
    if owner_email and owner_email != current_email and not _can_manage_source_code_qa(current_app.config["SETTINGS"]):
        return None
    return snapshot


def _sanitize_meeting_recorder_job_error(error: object, *, unexpected: bool = False) -> str:
    if unexpected:
        return "Meeting processing failed unexpectedly. Check server logs for details."
    message = " ".join(str(error or "").split()).strip() or "Meeting processing failed."
    if re.search(r"traceback|token|secret|authorization|api[_ -]?key", message, re.IGNORECASE):
        return "Meeting processing failed. Check server logs for details."
    return message[:500]


def _meeting_recorder_process_job_snapshot_for_current_user(job_id: str) -> dict[str, Any] | None:
    snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
    if snapshot is None or snapshot.get("action") != MEETING_RECORDER_PROCESS_ACTION:
        return None
    owner_email = str(snapshot.get("owner_email") or "").strip().lower()
    if owner_email and owner_email != _current_google_email():
        return None
    return snapshot


def _public_meeting_recorder_process_job_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    payload = dict(snapshot)
    payload.pop("owner_email", None)
    payload.setdefault("status", "ok")
    payload["progress"] = snapshot.get("progress") if isinstance(snapshot.get("progress"), dict) else {
        "stage": snapshot.get("stage") or "",
        "current": snapshot.get("current") or 0,
        "total": snapshot.get("total") or 0,
        "message": snapshot.get("message") or "",
    }
    state = str(payload.get("state") or "")
    if state == "queued":
        payload.setdefault("message", "Meeting processing is queued.")
        payload.setdefault("error_category", "job_queued")
        payload.setdefault("error_code", "")
        payload.setdefault("error_retryable", True)
    elif state == "running":
        payload.setdefault("message", "Meeting processing is running.")
        payload.setdefault("error_category", "job_running")
        payload.setdefault("error_code", "")
        payload.setdefault("error_retryable", True)
    elif state == "failed":
        payload["error_category"] = payload.get("error_category") or "meeting_processing_failed"
        payload["error_code"] = payload.get("error_code") or "meeting_processing_failed"
        payload["error_retryable"] = bool(payload.get("error_retryable", True))
    return payload


def _meeting_record_for_processing_job(record_id: str, owner_email: str) -> dict[str, Any]:
    record = _get_meeting_record_store().get_record(record_id)
    if str(record.get("owner_email") or "").strip().lower() != str(owner_email or "").strip().lower():
        raise ToolError("Meeting record is not available for this Google account.")
    status = str(record.get("status") or "").strip().lower()
    if status in {"recorded", "failed", "completed", "processing"}:
        return record
    raise ToolError("Stop the recording before processing this meeting.")


def _meeting_recorder_auto_process_payload(*, settings: Settings, record_id: str, owner_email: str) -> dict[str, Any]:
    try:
        if _local_agent_meeting_recorder_enabled(settings):
            result = _build_local_agent_client(settings).meeting_recorder_process_start(
                record_id=record_id,
                owner_email=owner_email,
                send_email_on_complete=True,
            )
            return {
                "state": result.get("state") or "queued",
                "job_id": result.get("job_id") or "",
            }
        payload = _queue_meeting_recorder_process_job(
            app=current_app._get_current_object(),
            settings=settings,
            record_id=record_id,
            owner_email=owner_email,
            send_email_on_complete=True,
        )
        return {
            "state": payload.get("state") or "queued",
            "job_id": payload.get("job_id") or "",
        }
    except (ConfigError, ToolError, requests.RequestException) as error:
        current_app.logger.warning(
            "Meeting Recorder auto process queue failed. record_id=%s",
            record_id,
            exc_info=True,
        )
        return {"auto_process_error": _sanitize_meeting_recorder_job_error(error)}


def _queue_meeting_recorder_process_job(
    *,
    app: Flask,
    settings: Settings,
    record_id: str,
    owner_email: str,
    send_email_on_complete: bool = False,
) -> dict[str, Any]:
    del settings
    normalized_owner = str(owner_email or "").strip().lower()
    record = _meeting_record_for_processing_job(record_id, normalized_owner)
    job_store: JobStore = current_app.config["JOB_STORE"]
    active_job = job_store.active_for_record(
        MEETING_RECORDER_PROCESS_ACTION,
        owner_email=normalized_owner,
        record_id=str(record.get("record_id") or record_id),
    )
    if active_job is not None:
        return {
            "status": "queued",
            "state": active_job.get("state") or "queued",
            "job_id": active_job.get("job_id") or "",
            "record": _meeting_record_summary(record),
        }
    if str(record.get("status") or "").strip().lower() == "processing":
        record["status"] = "recorded"
        record["error"] = ""
        _get_meeting_record_store().save_record(record)
    job = job_store.create(
        MEETING_RECORDER_PROCESS_ACTION,
        title="Process Meeting Recording",
        owner_email=normalized_owner,
        record_id=str(record.get("record_id") or record_id),
    )
    job_store.update(
        job.job_id,
        state="queued",
        stage="queued",
        message="Meeting processing is queued.",
        current=0,
        total=1,
    )
    thread = threading.Thread(
        target=_run_meeting_recorder_process_job,
        args=(app, job.job_id, str(record.get("record_id") or record_id), normalized_owner, bool(send_email_on_complete)),
        daemon=True,
    )
    thread.start()
    return {
        "status": "queued",
        "state": "queued",
        "job_id": job.job_id,
        "record": _meeting_record_summary(record),
    }


def _mark_meeting_record_process_failed(record_id: str, owner_email: str, message: str) -> None:
    try:
        record = _get_meeting_record_store().get_record(record_id)
        if str(record.get("owner_email") or "").strip().lower() != str(owner_email or "").strip().lower():
            return
        record["status"] = "failed"
        record["error"] = message
        _get_meeting_record_store().save_record(record)
    except Exception:  # pragma: no cover - best effort cleanup after a worker failure.
        current_app.logger.exception("Failed to mark meeting recorder record as failed.")


def _mark_meeting_record_email_failed(record_id: str, owner_email: str, message: str) -> None:
    try:
        record = _get_meeting_record_store().get_record(record_id)
        if str(record.get("owner_email") or "").strip().lower() != str(owner_email or "").strip().lower():
            return
        record["email"] = {
            "status": "failed",
            "error": message,
            "failed_at": _utc_now(),
            "recipient": str(owner_email or "").strip().lower(),
        }
        _get_meeting_record_store().save_record(record)
    except Exception:  # pragma: no cover - best effort annotation after processing succeeds.
        current_app.logger.exception("Failed to mark meeting recorder email as failed.")


def _send_meeting_recorder_minutes_email_after_process(
    *,
    settings: Settings,
    record_id: str,
    owner_email: str,
) -> dict[str, Any] | None:
    try:
        return _build_meeting_processing_service(settings).send_minutes_email(
            record_id=record_id,
            owner_email=owner_email,
            recipient=owner_email,
        )
    except (ConfigError, ToolError) as error:
        message = _sanitize_meeting_recorder_job_error(error)
        _mark_meeting_record_email_failed(record_id, owner_email, message)
        current_app.logger.warning(
            "Meeting Recorder auto email failed. record_id=%s",
            record_id,
            exc_info=True,
        )
        return {"status": "failed", "error": message, "recipient": str(owner_email or "").strip().lower()}


def _run_meeting_recorder_process_job(
    app: Flask,
    job_id: str,
    record_id: str,
    owner_email: str,
    send_email_on_complete: bool = False,
) -> None:
    with app.app_context():
        job_store: JobStore = app.config["JOB_STORE"]
        job_store.update(
            job_id,
            state="running",
            stage="processing",
            message="Transcribing audio and generating meeting minutes.",
            current=0,
            total=1,
        )
        try:
            record = _build_meeting_processing_service(app.config["SETTINGS"]).process_recording(
                record_id=record_id,
                owner_email=owner_email,
            )
            email_payload = (
                _send_meeting_recorder_minutes_email_after_process(
                    settings=app.config["SETTINGS"],
                    record_id=record_id,
                    owner_email=owner_email,
                )
                if send_email_on_complete
                else None
            )
            completed_record = _get_meeting_record_store().get_record(record_id)
            _record_meeting_work_memory(completed_record)
            details = [f"Record: {record_id}"]
            if email_payload:
                if email_payload.get("status") == "sent":
                    details.append(f"Email sent to {email_payload.get('recipient') or owner_email}")
                elif email_payload.get("status") == "failed":
                    details.append("Email was not sent automatically.")
            job_store.complete(
                job_id,
                results=[{"record": _meeting_record_summary(completed_record), "email": email_payload or {}}],
                notice={
                    "title": "Meeting Processing Completed",
                    "tone": "success",
                    "summary": "Transcript and minutes are ready.",
                    "details": details,
                },
            )
        except ToolError as error:
            message = _sanitize_meeting_recorder_job_error(error)
            _mark_meeting_record_process_failed(record_id, owner_email, message)
            job_store.fail(
                job_id,
                message,
                error_category="meeting_processing_failed",
                error_code="meeting_processing_failed",
                error_retryable=True,
            )
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            message = _sanitize_meeting_recorder_job_error(error, unexpected=True)
            current_app.logger.error(
                "Meeting Recorder process job failed unexpectedly. job_id=%s record_id=%s",
                job_id,
                record_id,
            )
            _mark_meeting_record_process_failed(record_id, owner_email, message)
            job_store.fail(
                job_id,
                message,
                error_category="meeting_processing_failed",
                error_code="meeting_processing_unexpected_error",
                error_retryable=True,
            )


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
            query_mode = _source_code_qa_query_mode(payload.get("query_mode"))
            session_store = _get_source_code_qa_session_store()
            session_id = str(payload.get("session_id") or "").strip()
            owner_email = str(payload.get("_session_owner_email") or "").strip().lower() or "local"
            conversation_context = payload.get("conversation_context") if isinstance(payload.get("conversation_context"), dict) else None
            if conversation_context is None and session_id:
                conversation_context = session_store.get_context(session_id, owner_email=owner_email)
            if isinstance(payload.get("_resolved_attachments"), list):
                attachments = payload.get("_resolved_attachments") or []
            else:
                attachments = _resolve_source_code_qa_query_attachments(payload, owner_email=owner_email, session_id=session_id)
            runtime_evidence = _resolve_source_code_qa_runtime_evidence(pm_team=pm_team, country=country)
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
                    query_mode=query_mode,
                    conversation_context=conversation_context,
                    attachments=attachments,
                    runtime_evidence=runtime_evidence,
                    progress_callback=progress_callback,
                )

            if service.llm_provider_name == "codex_cli_bridge" and session_id:
                progress_callback("codex_session_lock", "Waiting for this chat's Codex session slot.", 0, 1)
                with _source_code_qa_codex_session_lock(session_id):
                    result = run_query()
            else:
                result = run_query()
            result["auto_sync"] = auto_sync
            result["attachments"] = _source_code_qa_public_attachments(attachments)
            result["runtime_evidence"] = _source_code_qa_public_runtime_evidence(runtime_evidence)
            if session_id:
                result["generated_artifacts"] = _build_source_code_qa_generated_artifacts(
                    owner_email=owner_email,
                    session_id=session_id,
                    pm_team=pm_team,
                    country=country,
                    question=str(payload.get("question") or ""),
                    result=result,
                    runtime_evidence=runtime_evidence,
                )
                session_write_started = time.perf_counter()
                session_payload = session_store.append_exchange(
                    session_id,
                    owner_email=owner_email,
                    pm_team=pm_team,
                    country=country,
                    llm_provider=str(payload.get("llm_provider") or ""),
                    question=str(payload.get("question") or ""),
                    result=result,
                    context=_build_source_code_qa_session_context(result, payload),
                    attachments=attachments,
                )
                current_app.logger.warning(
                    "source_code_qa_timing %s",
                    json.dumps(
                        {
                            "event": "source_code_qa_timing",
                            "component": "session_write",
                            "elapsed_ms": int((time.perf_counter() - session_write_started) * 1000),
                            "job_id": job_id,
                            "trace_id": str(result.get("trace_id") or ""),
                            "session_id": session_id,
                            "owner_email": owner_email,
                            "message_count": len(session_payload.get("messages") or []) if isinstance(session_payload, dict) else 0,
                            "status": "ok" if session_payload is not None else "missing_session",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                if session_payload is not None:
                    result["session"] = session_payload
                    result["session_id"] = session_id
            status = str(result.get("status") or "ok")
            started_at = float((job_store.snapshot(job_id) or {}).get("started_at") or time.time())
            elapsed_seconds = max(0.0, time.time() - started_at)
            if elapsed_seconds > 120:
                current_app.logger.warning(
                    "source_code_qa_slow_query job_id=%s owner=%s elapsed_seconds=%.1f stage=%s",
                    job_id,
                    owner_email,
                    elapsed_seconds,
                    str((job_store.snapshot(job_id) or {}).get("stage") or ""),
                )
            _record_source_code_qa_work_memory(
                owner_email=owner_email,
                pm_team=pm_team,
                country=country,
                question=str(payload.get("question") or ""),
                result=result,
                session_id=session_id,
                job_id=job_id,
            )
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
            job_store.fail(job_id, str(error), **_classify_source_code_qa_job_error(str(error)))
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            app.logger.exception("Source code QA query job failed unexpectedly.")
            message = f"Unexpected error: {error}"
            job_store.fail(job_id, message, **_classify_source_code_qa_job_error(message))


def _run_source_code_qa_effort_assessment_job(app: Flask, job_id: str, payload: dict[str, Any]) -> None:
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
            language = _source_code_qa_effort_assessment_language(payload.get("language"))
            requirement = str(payload.get("requirement") or "").strip()
            if not requirement:
                raise ToolError("Business requirement is empty.")
            settings: Settings = app.config["SETTINGS"]
            progress_callback("assessment_prompt", "Building optimized effort assessment evidence query.", 0, 1)
            runtime_evidence = _resolve_source_code_qa_runtime_evidence(pm_team=pm_team, country=country)
            business_plan = _build_source_code_qa_effort_business_plan(
                pm_team=pm_team,
                country=country,
                language=language,
                requirement=requirement,
            )
            technical_candidates = _build_source_code_qa_effort_technical_candidates(
                pm_team=pm_team,
                country=country,
                business_plan=business_plan,
                requirement=requirement,
            )
            estimation_rubric = _build_source_code_qa_effort_estimation_rubric(
                business_plan=business_plan,
                technical_candidates=technical_candidates,
            )
            llm_provider = str(payload.get("llm_provider") or "")
            scope_guard = _source_code_qa_effort_scope_guard(pm_team=pm_team, country=country, requirement=requirement)
            if scope_guard.get("status") == "mismatch":
                result = _source_code_qa_effort_scope_mismatch_result(
                    pm_team=pm_team,
                    country=country,
                    language=language,
                    requirement=requirement,
                    llm_provider=llm_provider,
                    business_plan=business_plan,
                    technical_candidates=technical_candidates,
                    estimation_rubric=estimation_rubric,
                    scope_guard=scope_guard,
                )
                current_app.logger.warning(
                    "source_code_qa_effort_assessment_scope_mismatch %s",
                    json.dumps(
                        {
                            "event": "source_code_qa_effort_assessment_scope_mismatch",
                            "job_id": job_id,
                            "pm_team": pm_team,
                            "country": country,
                            "scope_guard": scope_guard,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                job_store.complete(
                    job_id,
                    results=[result],
                    notice={
                        "title": "Effort Assessment",
                        "tone": "warning",
                        "summary": result["summary"],
                        "details": [
                            f"Selected: {pm_team}:{country}",
                            f"Suggested: {scope_guard.get('suggested_pm_team')}:{scope_guard.get('suggested_country')}",
                            "Status: scope_mismatch",
                        ],
                    },
                )
                return
            effort_started = time.perf_counter()
            evidence_query = _build_source_code_qa_effort_evidence_query(
                requirement=requirement,
                business_plan=business_plan,
                technical_candidates=technical_candidates,
            )
            progress_callback("effort_evidence", "Searching focused source-code evidence for effort assessment.", 0, 1)
            evidence_started = time.perf_counter()
            evidence_result = service.query(
                pm_team=pm_team,
                country=country,
                question=evidence_query,
                limit=16,
                answer_mode="retrieval_only",
                llm_budget_mode="cheap",
                query_mode="deep",
                conversation_context=None,
                attachments=[],
                runtime_evidence=runtime_evidence,
                progress_callback=progress_callback,
            )
            evidence_latency_ms = int((time.perf_counter() - evidence_started) * 1000)
            evidence_matrix = _build_source_code_qa_effort_evidence_matrix(
                evidence_result=evidence_result,
                business_plan=business_plan,
                technical_candidates=technical_candidates,
            )
            evidence_matrix["scope_guard"] = scope_guard
            evidence_matrix_quality = _source_code_qa_effort_matrix_quality(evidence_matrix)
            cache_key = _source_code_qa_effort_cache_key(
                pm_team=pm_team,
                country=country,
                language=language,
                requirement=requirement,
                llm_provider=llm_provider,
                evidence_query=evidence_query,
                evidence_result=evidence_result,
                evidence_matrix=evidence_matrix,
                runtime_evidence=runtime_evidence,
            )
            result = _load_source_code_qa_effort_cached_result(settings, cache_key)
            if result is None:
                synthesis_prompt = _build_source_code_qa_effort_compact_synthesis_prompt(
                    pm_team=pm_team,
                    country=country,
                    language=language,
                    requirement=requirement,
                    llm_provider=llm_provider,
                    runtime_evidence=runtime_evidence,
                    business_plan=business_plan,
                    technical_candidates=technical_candidates,
                    estimation_rubric=estimation_rubric,
                    evidence_result=evidence_result,
                    evidence_matrix=evidence_matrix,
                )
                progress_callback("effort_synthesis", "Generating compact code-grounded effort assessment.", 0, 1)
                synthesis_started = time.perf_counter()
                result = service.query(
                    pm_team=pm_team,
                    country=country,
                    question=synthesis_prompt,
                    limit=16,
                    answer_mode="auto",
                    llm_budget_mode="auto",
                    query_mode="deep",
                    conversation_context=None,
                    attachments=[],
                    runtime_evidence=runtime_evidence,
                    progress_callback=progress_callback,
                    effort_assessment=True,
                )
                synthesis_latency_ms = int((time.perf_counter() - synthesis_started) * 1000)
                if isinstance(result.get("llm_route"), dict):
                    result["llm_route"] = {
                        **result["llm_route"],
                        "task": "effort_assessment",
                        "effort_cache_hit": False,
                        "effort_evidence_query_sha256": hashlib.sha256(evidence_query.encode("utf-8")).hexdigest()[:16],
                        "effort_evidence_matrix_quality": evidence_matrix_quality,
                    }
                result["effort_evidence_matrix"] = evidence_matrix
                result["effort_timing"] = {
                    "evidence_retrieval_ms": evidence_latency_ms,
                    "synthesis_ms": synthesis_latency_ms,
                    "repair_decision_ms": int(
                        ((result.get("llm_route") or {}).get("codex_repair_decision_ms") or 0)
                        if isinstance(result.get("llm_route"), dict)
                        else 0
                    ),
                    "total_ms": int((time.perf_counter() - effort_started) * 1000),
                    "cache_hit": False,
                    "evidence_matrix_quality": evidence_matrix_quality,
                }
                result["effort_cache_key"] = cache_key
                _store_source_code_qa_effort_cached_result(settings, cache_key, result)
            else:
                result["effort_evidence_matrix"] = evidence_matrix
                result["effort_timing"] = {
                    **(result.get("effort_timing") if isinstance(result.get("effort_timing"), dict) else {}),
                    "evidence_retrieval_ms": evidence_latency_ms,
                    "cache_hit": True,
                    "evidence_matrix_quality": evidence_matrix_quality,
                    "repair_decision_ms": int(
                        ((result.get("llm_route") or {}).get("codex_repair_decision_ms") or 0)
                        if isinstance(result.get("llm_route"), dict)
                        else 0
                    ),
                }
            result = _normalize_source_code_qa_effort_assessment_result(
                result=result,
                language=language,
                business_plan=business_plan,
                technical_candidates=technical_candidates,
                estimation_rubric=estimation_rubric,
                evidence_matrix=evidence_matrix,
            )
            if isinstance(result.get("effort_timing"), dict):
                result["effort_timing"] = {
                    **result["effort_timing"],
                    "generic_output_guard": result.get("effort_generic_output_guard") or {},
                }
            result["effort_evidence_query"] = evidence_query
            result["effort_evidence_result"] = _source_code_qa_effort_compact_evidence(evidence_result)
            result["assessment"] = {
                "type": "effort_assessment",
                "pm_team": pm_team,
                "country": country,
                "language": language,
                "requirement": requirement,
                "business_plan": business_plan,
                "technical_candidates": technical_candidates,
                "estimation_rubric": estimation_rubric,
                "structured_assessment": result.get("structured_assessment") or {},
                "confidence": result.get("assessment_confidence") or "low",
                "missing_evidence": result.get("missing_evidence") or [],
                "evidence_status": result.get("effort_evidence_status") or "warning",
            }
            current_app.logger.warning(
                "source_code_qa_effort_assessment_quality %s",
                json.dumps(
                    {
                        "event": "source_code_qa_effort_assessment_quality",
                        "job_id": job_id,
                        "trace_id": str(result.get("trace_id") or ""),
                        "evidence_matrix_quality": result.get("effort_evidence_matrix_quality") or {},
                        "generic_output_guard": result.get("effort_generic_output_guard") or {},
                        "cache_hit": bool((result.get("effort_timing") or {}).get("cache_hit")) if isinstance(result.get("effort_timing"), dict) else False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            result["runtime_evidence"] = _source_code_qa_public_runtime_evidence(runtime_evidence)
            status = str(result.get("status") or "ok")
            notice_tone = "warning" if result.get("effort_evidence_status") == "warning" else ("success" if status == "ok" else "warning")
            job_store.complete(
                job_id,
                results=[result],
                notice={
                    "title": "Effort Assessment",
                    "tone": notice_tone,
                    "summary": result.get("summary") or "Effort assessment completed.",
                    "details": [
                        f"Status: {status}",
                        f"Evidence: {result.get('effort_evidence_status') or 'n/a'}",
                        f"Trace: {result.get('trace_id') or 'n/a'}",
                    ],
                },
            )
        except ToolError as error:
            job_store.fail(job_id, str(error), **_classify_source_code_qa_job_error(str(error)))
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            app.logger.exception("Source code QA effort assessment job failed unexpectedly.")
            message = f"Unexpected error: {error}"
            job_store.fail(job_id, message, **_classify_source_code_qa_job_error(message))


def _run_team_dashboard_monthly_report_draft_job(
    app: Flask,
    job_id: str,
    settings: Settings,
    request_payload: dict[str, Any],
    user_identity: dict[str, str | None],
) -> None:
    with app.app_context():
        job_store: JobStore = app.config["JOB_STORE"]

        def progress_callback(
            stage: str,
            message: str,
            current: int,
            total: int,
            *,
            estimated_prompt_tokens: int = 0,
            token_risk: str = "",
        ) -> None:
            job_store.update(
                job_id,
                state="running",
                stage=stage,
                message=message,
                current=current,
                total=total,
                estimated_prompt_tokens=estimated_prompt_tokens,
                token_risk=token_risk,
            )

        try:
            progress_callback("preparing_sources", "Preparing Monthly Report sources.", 0, 0)
            if _local_agent_seatalk_enabled(settings):
                data = _build_local_agent_client(settings).team_dashboard_monthly_report_draft(
                    request_payload,
                    progress_callback=progress_callback,
                )
            else:
                data = _build_monthly_report_service(settings).generate_draft(
                    **request_payload,
                    progress_callback=progress_callback,
                )
            evidence = data.get("evidence_summary") if isinstance(data.get("evidence_summary"), dict) else {}
            generation = data.get("generation_summary") if isinstance(data.get("generation_summary"), dict) else {}
            _log_portal_event(
                "team_dashboard_monthly_report_draft_success",
                user=_safe_email_identity(user_identity),
                job_id=job_id,
                **evidence,
                **{f"generation_{key}": value for key, value in generation.items() if key in {"total_batches", "max_batch_estimated_tokens", "final_estimated_tokens", "elapsed_seconds"}},
            )
            job_store.complete(
                job_id,
                results=[data],
                notice={
                    "title": "Monthly Report",
                    "tone": "success",
                    "summary": "Monthly Report draft generated.",
                    "details": [
                        f"Total batches: {generation.get('total_batches', 0)}",
                        f"Final estimated tokens: {generation.get('final_estimated_tokens', generation.get('estimated_prompt_tokens', 0))}",
                    ],
                },
            )
        except ToolError as error:
            _log_portal_event(
                "team_dashboard_monthly_report_draft_tool_error",
                level=logging.WARNING,
                user=_safe_email_identity(user_identity),
                job_id=job_id,
                **_classify_portal_error(error),
            )
            job_store.fail(job_id, str(error))
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            _log_portal_event(
                "team_dashboard_monthly_report_draft_unexpected_error",
                level=logging.ERROR,
                user=_safe_email_identity(user_identity),
                job_id=job_id,
                **_classify_portal_error(error),
            )
            app.logger.exception("Team Dashboard Monthly Report draft job failed unexpectedly.")
            job_store.fail(job_id, f"Unexpected error: {error}")


def _run_background_job(
    app: Flask,
    job_id: str,
    action: str,
    settings: Settings,
    config_data: dict[str, Any],
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
            if action != "sync-bpmis-projects":
                raise ToolError(f"Unsupported background job action: {action}")
            service = _build_portal_project_sync_service(settings, config_data)
            results = service.sync_projects(
                user_key=str(config_data.get("_user_key", "")).strip(),
                pm_email=str(config_data.get("sync_pm_email", "")).strip(),
                progress_callback=progress_callback,
            )
            notice = _build_sync_notice(results)
            job_store.complete(
                job_id,
                results=_serialize_results(results, include_skipped=True),
                notice=notice,
            )
            _log_portal_event(
                "job_completed",
                logger=logger,
                job_id=job_id,
                action=action,
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
                **error_details,
            )
            logger.exception("Background job failed unexpectedly.")


def _resolve_bpmis_access_token(config_data: dict[str, Any], settings: Settings) -> str | None:
    configured_token = str(config_data.get("bpmis_api_access_token", "") or "").strip()
    return configured_token or settings.bpmis_api_access_token



def _build_team_dashboard_task_group(
    team_key: str,
    label: str,
    member_emails: list[str],
    tasks: list[dict[str, Any]],
    biz_projects: list[dict[str, Any]] | None = None,
    *,
    key_project_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    under_prd: list[dict[str, Any]] = []
    pending_live: list[dict[str, Any]] = []
    seen: set[str] = set()
    for task in tasks:
        normalized_task = _normalize_team_dashboard_task(task)
        dedupe_key = normalized_task["jira_id"] or normalized_task["issue_id"]
        if dedupe_key and dedupe_key in seen:
            continue
        if dedupe_key:
            seen.add(dedupe_key)
        status_key = normalized_task["jira_status"].strip().casefold()
        if status_key in TEAM_DASHBOARD_UNDER_PRD_STATUSES:
            under_prd.append(normalized_task)
        elif status_key not in TEAM_DASHBOARD_EXCLUDED_PENDING_STATUSES:
            pending_live.append(normalized_task)
    under_prd_biz_projects, pending_live_biz_projects = _split_team_dashboard_biz_projects_by_status(biz_projects or [])
    under_prd_projects = _group_team_dashboard_tasks_by_project(under_prd)
    under_prd_projects = _merge_team_dashboard_biz_projects(under_prd_projects, under_prd_biz_projects)
    _sort_team_dashboard_under_prd_projects(under_prd_projects)
    pending_live_projects = _group_team_dashboard_tasks_by_project(pending_live, sort_by_release=True)
    pending_live_projects = _merge_team_dashboard_biz_projects(
        pending_live_projects,
        pending_live_biz_projects,
        sort_by_release=True,
    )
    _apply_team_dashboard_key_project_states(under_prd_projects, key_project_overrides or {})
    _apply_team_dashboard_key_project_states(pending_live_projects, key_project_overrides or {})
    return {
        "team_key": team_key,
        "label": label,
        "member_emails": member_emails,
        "under_prd": under_prd_projects,
        "pending_live": pending_live_projects,
        "error": "",
    }


def _team_dashboard_biz_projects_for_emails(bpmis_client: Any, emails: list[str]) -> list[dict[str, Any]]:
    projects: dict[str, dict[str, Any]] = {}
    normalized_emails = _normalize_team_dashboard_emails(emails)
    if hasattr(bpmis_client, "list_biz_projects_for_pm_emails"):
        try:
            rows = bpmis_client.list_biz_projects_for_pm_emails(normalized_emails)
        except Exception as error:  # noqa: BLE001 - fall back to the stable single-email path.
            current_app.logger.warning("Team Dashboard bulk Biz Project lookup failed: %s", error)
        else:
            for row in rows or []:
                project = _normalize_team_dashboard_project(row if isinstance(row, dict) else {})
                matched_emails = _normalize_team_dashboard_emails(
                    row.get("matched_pm_emails") if isinstance(row, dict) else []
                )
                if not matched_emails:
                    matched_emails = _normalize_team_dashboard_emails(project.get("matched_pm_emails") or [])
                if not matched_emails:
                    regional_pm = str(project.get("regional_pm_pic") or "").strip().lower()
                    if regional_pm in normalized_emails:
                        matched_emails = [regional_pm]
                _merge_team_dashboard_biz_project_lookup(projects, project, matched_emails)
            return list(projects.values())
    for email in normalized_emails:
        normalized_email = str(email or "").strip().lower()
        if not normalized_email:
            continue
        try:
            rows = bpmis_client.list_biz_projects_for_pm_email(normalized_email)
        except Exception as error:  # noqa: BLE001 - Jira data should still render when project coverage fails.
            current_app.logger.warning("Team Dashboard Biz Project lookup failed for %s: %s", normalized_email, error)
            continue
        for row in rows or []:
            project = _normalize_team_dashboard_project(row if isinstance(row, dict) else {})
            _merge_team_dashboard_biz_project_lookup(projects, project, [normalized_email])
    return list(projects.values())


def _team_dashboard_load_jira_and_biz_projects(
    jira_bpmis_client: Any,
    biz_bpmis_client: Any,
    emails: list[str],
    timing_stats: dict[str, float],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    app_obj = current_app._get_current_object()

    def load_jira_tasks() -> tuple[list[dict[str, Any]], float]:
        started_at = time.monotonic()
        rows = jira_bpmis_client.list_jira_tasks_created_by_emails(
            emails,
            max_pages=_team_dashboard_jira_max_pages(),
            enrich_missing_parent=False,
            release_after=_team_dashboard_jira_release_after(),
        )
        return rows, round(time.monotonic() - started_at, 3)

    def load_biz_projects() -> tuple[list[dict[str, Any]], float]:
        started_at = time.monotonic()
        with app_obj.app_context():
            rows = _team_dashboard_biz_projects_for_emails(biz_bpmis_client, emails)
        return rows, round(time.monotonic() - started_at, 3)

    started_at = time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as executor:
        jira_future = executor.submit(load_jira_tasks)
        biz_future = executor.submit(load_biz_projects)
        tasks, jira_elapsed = jira_future.result()
        biz_projects, biz_elapsed = biz_future.result()
    timing_stats["list_jira_tasks"] = jira_elapsed
    timing_stats["list_biz_projects"] = biz_elapsed
    timing_stats["list_jira_and_biz_projects"] = round(time.monotonic() - started_at, 3)
    timing_stats.update(_team_dashboard_combined_request_timings(jira_bpmis_client, biz_bpmis_client))
    return tasks, biz_projects


def _load_team_dashboard_tasks_for_all_teams_merged(
    settings: Settings,
    store: TeamDashboardConfigStore | RemoteTeamDashboardConfigStore,
    config: dict[str, Any],
    *,
    config_elapsed: float,
    route_started_at: float,
    key_project_overrides: dict[str, Any],
) -> list[dict[str, Any]]:
    started_at = time.monotonic()
    shared_timing = _team_dashboard_new_timing()
    shared_timing["config_load"] = config_elapsed
    team_items: list[tuple[str, str, list[str]]] = []
    all_emails: list[str] = []
    for team_key, label in TEAM_DASHBOARD_TEAMS.items():
        team_config = (config.get("teams") or {}).get(team_key) or {}
        emails = _normalize_team_dashboard_emails(team_config.get("member_emails") or [])
        team_items.append((team_key, label, emails))
        all_emails.extend(emails)
    all_emails = _normalize_team_dashboard_emails(all_emails)
    bpmis_client = _build_bpmis_client_for_current_user(settings)
    biz_bpmis_client = _build_bpmis_client_for_current_user(settings)
    tasks, biz_projects = _team_dashboard_load_jira_and_biz_projects(
        bpmis_client,
        biz_bpmis_client,
        all_emails,
        shared_timing,
    )

    team_payloads: list[dict[str, Any]] = []
    group_started_at = time.monotonic()
    for team_key, label, emails in team_items:
        team_tasks = _filter_team_dashboard_tasks_for_emails(tasks, emails)
        team_biz_projects = _filter_team_dashboard_biz_projects_for_emails(biz_projects, emails)
        team_payloads.append(
            _build_team_dashboard_task_group(
                team_key,
                label,
                emails,
                team_tasks,
                team_biz_projects,
                key_project_overrides=key_project_overrides,
            )
        )
    _team_dashboard_add_timing(shared_timing, "group_projects", group_started_at)

    backfill_started_at = time.monotonic()
    _backfill_all_team_dashboard_empty_project_jira_tasks(bpmis_client, team_payloads)
    for team_payload in team_payloads:
        _remove_team_dashboard_zero_jira_pending_live_projects(team_payload)
    _team_dashboard_add_timing(shared_timing, "backfill_zero_jira_projects", backfill_started_at)
    shared_timing.update(_team_dashboard_combined_request_timings(bpmis_client, biz_bpmis_client))

    fetch_stats = _team_dashboard_combined_fetch_stats(bpmis_client, biz_bpmis_client)
    elapsed = round(time.monotonic() - started_at, 2)
    for team_payload in team_payloads:
        timing_stats = dict(shared_timing)
        timing_stats["total"] = elapsed
        team_payload["elapsed_seconds"] = elapsed
        team_payload["fetch_stats"] = fetch_stats
        team_payload["timing_stats"] = timing_stats

    cache_started_at = time.monotonic()
    for team_key, _label, emails in team_items:
        team_payload = next((payload for payload in team_payloads if payload.get("team_key") == team_key), None)
        if team_payload is not None:
            _store_team_dashboard_task_payload(store, team_key, emails, team_payload)
    _team_dashboard_add_timing(shared_timing, "cache_store", cache_started_at)
    elapsed = round(time.monotonic() - started_at, 2)
    for team_payload in team_payloads:
        timing_stats = dict(shared_timing)
        timing_stats["total"] = elapsed
        team_payload["elapsed_seconds"] = elapsed
        team_payload["fetch_stats"] = fetch_stats
        team_payload["timing_stats"] = timing_stats

    for team_key, _label, emails in team_items:
        team_payload = next((payload for payload in team_payloads if payload.get("team_key") == team_key), {})
        _log_portal_event(
            "team_dashboard_tasks_team_loaded",
            **_build_request_log_context(
                settings,
                user_identity=_get_user_identity(settings),
                extra={
                    "team_key": team_key,
                    "email_count": len(emails),
                    "raw_task_count": len(_filter_team_dashboard_tasks_for_emails(tasks, emails)),
                    "raw_biz_project_count": len(_filter_team_dashboard_biz_projects_for_emails(biz_projects, emails)),
                    "elapsed_seconds": elapsed,
                    "fetch_stats": fetch_stats,
                    "timing_stats": team_payload.get("timing_stats") or {},
                    "all_team_merged_reload": True,
                    "route_elapsed_seconds": round(time.monotonic() - route_started_at, 2),
                },
            ),
        )
    return team_payloads


def _filter_team_dashboard_tasks_for_emails(tasks: list[dict[str, Any]], emails: list[str]) -> list[dict[str, Any]]:
    allowed_emails = set(_normalize_team_dashboard_emails(emails))
    if not allowed_emails:
        return []
    filtered: list[dict[str, Any]] = []
    for task in tasks or []:
        if not isinstance(task, dict):
            continue
        normalized_task = _normalize_team_dashboard_task(task)
        pm_email = str(normalized_task.get("pm_email") or "").strip().lower()
        if pm_email in allowed_emails:
            filtered.append(task)
    return filtered


def _filter_team_dashboard_biz_projects_for_emails(
    biz_projects: list[dict[str, Any]],
    emails: list[str],
) -> list[dict[str, Any]]:
    allowed_emails = set(_normalize_team_dashboard_emails(emails))
    if not allowed_emails:
        return []
    filtered: list[dict[str, Any]] = []
    for raw_project in biz_projects or []:
        if not isinstance(raw_project, dict):
            continue
        project = _normalize_team_dashboard_project(raw_project)
        matched_emails = _normalize_team_dashboard_emails(project.get("matched_pm_emails") or [])
        if not matched_emails:
            regional_pm = str(project.get("regional_pm_pic") or "").strip().lower()
            if regional_pm in allowed_emails:
                matched_emails = [regional_pm]
        team_matches = [email for email in matched_emails if email in allowed_emails]
        if not team_matches:
            continue
        project["matched_pm_emails"] = team_matches
        filtered.append(project)
    return filtered


def _merge_team_dashboard_biz_project_lookup(
    projects: dict[str, dict[str, Any]],
    project: dict[str, Any],
    matched_emails: list[str],
) -> None:
    bpmis_id = str(project.get("bpmis_id") or "").strip()
    if not bpmis_id:
        return
    existing = projects.setdefault(
        bpmis_id,
        {
            **project,
            "matched_pm_emails": [],
        },
    )
    for key, value in project.items():
        if value and not existing.get(key):
            existing[key] = value
    matched = existing.setdefault("matched_pm_emails", [])
    for normalized_email in _normalize_team_dashboard_emails(matched_emails):
        if normalized_email not in matched:
            matched.append(normalized_email)


def _team_dashboard_fetch_stats(bpmis_client: Any) -> dict[str, int]:
    stats = getattr(bpmis_client, "request_stats", None)
    if not isinstance(stats, dict):
        return {}
    return {
        key: int(stats.get(key) or 0)
        for key in (
            "api_call_count",
            "issue_created_before_cutoff_count",
            "issue_release_before_cutoff_count",
            "issue_release_missing_included_count",
            "issue_detail_lookup_count",
            "issue_detail_bulk_lookup_count",
            "issue_detail_bulk_issue_count",
            "issue_detail_single_fallback_count",
            "jira_live_detail_lookup_count",
            "jira_live_bulk_lookup_count",
            "jira_live_bulk_issue_count",
            "jira_live_status_override_count",
            "bpmis_release_query_filter_probe_count",
            "bpmis_release_query_filter_enabled_count",
            "bpmis_release_query_filter_disabled_count",
            "bpmis_release_query_filter_probe_failed_count",
            "bpmis_release_query_filter_used_count",
            "issue_detail_enrichment_skipped_count",
            "issue_list_created_cutoff_hit",
            "issue_list_page_cap_hit",
            "issue_list_page_count",
            "issue_rows_scanned",
            "issue_tree_page_count",
            "issue_tree_rows_scanned",
            "issue_tree_fallback_count",
            "release_version_lookup_count",
            "release_version_count",
            "release_version_lookup_failed_count",
            "team_dashboard_zero_jira_fallback_candidate_count",
            "team_dashboard_zero_jira_bulk_project_count",
            "team_dashboard_zero_jira_bulk_hit_count",
            "team_dashboard_zero_jira_bulk_failed_count",
            "team_dashboard_zero_jira_per_project_fallback_count",
            "team_dashboard_zero_jira_per_project_fallback_skipped_count",
            "user_lookup_count",
        )
    }


def _team_dashboard_combined_fetch_stats(*bpmis_clients: Any) -> dict[str, int]:
    combined: dict[str, int] = {}
    seen: set[int] = set()
    for bpmis_client in bpmis_clients:
        if bpmis_client is None:
            continue
        identity = id(bpmis_client)
        if identity in seen:
            continue
        seen.add(identity)
        for key, value in _team_dashboard_fetch_stats(bpmis_client).items():
            combined[key] = int(combined.get(key) or 0) + int(value or 0)
    return combined


def _team_dashboard_combined_request_timings(*bpmis_clients: Any) -> dict[str, float]:
    combined: dict[str, float] = {}
    seen: set[int] = set()
    for bpmis_client in bpmis_clients:
        if bpmis_client is None:
            continue
        identity = id(bpmis_client)
        if identity in seen:
            continue
        seen.add(identity)
        timings = getattr(bpmis_client, "request_timings", None)
        if not isinstance(timings, dict):
            continue
        for key, value in timings.items():
            try:
                numeric = float(value or 0.0)
            except (TypeError, ValueError):
                continue
            combined[key] = round(float(combined.get(key) or 0.0) + numeric, 3)
    return combined


def _team_dashboard_new_timing() -> dict[str, float]:
    return {
        "config_load": 0.0,
        "cache_check": 0.0,
        "list_jira_tasks": 0.0,
        "list_biz_projects": 0.0,
        "list_jira_and_biz_projects": 0.0,
        "bpmis_user_lookup": 0.0,
        "release_versions": 0.0,
        "issue_tree_reporter": 0.0,
        "issue_tree_jiraRegionalPmPicId": 0.0,
        "parent_detail_bulk": 0.0,
        "jira_live_bulk": 0.0,
        "zero_jira_bulk": 0.0,
        "group_projects": 0.0,
        "backfill_zero_jira_projects": 0.0,
        "cache_store": 0.0,
        "total": 0.0,
    }


def _team_dashboard_add_timing(timing_stats: dict[str, float], key: str, started_at: float) -> None:
    timing_stats[key] = round(float(timing_stats.get(key) or 0.0) + (time.monotonic() - started_at), 3)


def _team_dashboard_increment_request_stat(bpmis_client: Any, key: str, amount: int = 1) -> None:
    stats = getattr(bpmis_client, "request_stats", None)
    if isinstance(stats, dict):
        stats[key] = int(stats.get(key) or 0) + amount


def _load_all_team_dashboard_task_payloads(settings: Settings, config: dict[str, Any]) -> list[dict[str, Any]]:
    key_project_overrides = config.get("key_project_overrides") if isinstance(config.get("key_project_overrides"), dict) else {}
    bpmis_client = _build_bpmis_client_for_current_user(settings)
    team_payloads: list[dict[str, Any]] = []
    for team_key, label in TEAM_DASHBOARD_TEAMS.items():
        team_config = (config.get("teams") or {}).get(team_key) or {}
        emails = _normalize_team_dashboard_emails(team_config.get("member_emails") or [])
        tasks = bpmis_client.list_jira_tasks_created_by_emails(
            emails,
            max_pages=_team_dashboard_jira_max_pages(),
            enrich_missing_parent=False,
            release_after=_team_dashboard_monthly_report_jira_release_after(),
        )
        biz_projects = _team_dashboard_biz_projects_for_emails(bpmis_client, emails)
        team_payload = _build_team_dashboard_task_group(
            team_key,
            label,
            emails,
            tasks,
            biz_projects,
            key_project_overrides=key_project_overrides,
        )
        _backfill_team_dashboard_empty_project_jira_tasks(bpmis_client, team_payload)
        team_payloads.append(team_payload)
    return team_payloads


def _load_team_dashboard_link_biz_project_payloads(settings: Settings, config: dict[str, Any], bpmis_client: Any | None = None) -> list[dict[str, Any]]:
    key_project_overrides = config.get("key_project_overrides") if isinstance(config.get("key_project_overrides"), dict) else {}
    bpmis_client = bpmis_client or _build_bpmis_client_for_current_user(settings)
    team_payloads: list[dict[str, Any]] = []
    for team_key, label in TEAM_DASHBOARD_TEAMS.items():
        team_config = (config.get("teams") or {}).get(team_key) or {}
        emails = _normalize_team_dashboard_emails(team_config.get("member_emails") or [])
        tasks = bpmis_client.list_jira_tasks_created_by_emails(
            emails,
            max_pages=_team_dashboard_jira_max_pages(),
            enrich_missing_parent=False,
            release_after=_team_dashboard_jira_release_after(),
        )
        biz_projects = _team_dashboard_biz_projects_for_emails(bpmis_client, emails)
        team_payload = _build_team_dashboard_task_group(
            team_key,
            label,
            emails,
            tasks,
            biz_projects,
            key_project_overrides=key_project_overrides,
        )
        _backfill_team_dashboard_empty_project_jira_tasks(bpmis_client, team_payload)
        _remove_team_dashboard_zero_jira_pending_live_projects(team_payload)
        team_payloads.append(team_payload)
    return team_payloads


def _team_dashboard_task_cache_signature(emails: list[str]) -> str:
    return "|".join(_normalize_team_dashboard_emails(emails))


def _cached_team_dashboard_task_payload(config: dict[str, Any], team_key: str, emails: list[str]) -> dict[str, Any] | None:
    task_cache = config.get("task_cache") if isinstance(config.get("task_cache"), dict) else {}
    if int(task_cache.get("version") or 1) != TEAM_DASHBOARD_TASK_CACHE_VERSION:
        return None
    cached_teams = task_cache.get("teams") if isinstance(task_cache.get("teams"), dict) else {}
    cached_team = cached_teams.get(team_key)
    if not isinstance(cached_team, dict):
        return None
    if str(cached_team.get("email_signature") or "") != _team_dashboard_task_cache_signature(emails):
        return None
    payload = {
        **cached_team,
        "team_key": team_key,
        "member_emails": emails,
        "loading": False,
        "loaded": True,
        "error": "",
        "progress_text": "",
        "cache_source": "server",
    }
    key_project_overrides = config.get("key_project_overrides") if isinstance(config.get("key_project_overrides"), dict) else {}
    for section_key in ("under_prd", "pending_live"):
        projects = payload.get(section_key)
        if isinstance(projects, list):
            _apply_team_dashboard_key_project_states(projects, key_project_overrides)
    return payload


def _store_team_dashboard_task_payload(
    store: TeamDashboardConfigStore | RemoteTeamDashboardConfigStore,
    team_key: str,
    emails: list[str],
    team_payload: dict[str, Any],
) -> None:
    if not team_key or team_payload.get("error"):
        return
    config = store.load()
    task_cache = config.get("task_cache") if isinstance(config.get("task_cache"), dict) else {}
    cached_teams = task_cache.get("teams") if isinstance(task_cache.get("teams"), dict) else {}
    cached_team = {
        **team_payload,
        "team_key": team_key,
        "member_emails": emails,
        "email_signature": _team_dashboard_task_cache_signature(emails),
        "cached_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "loading": False,
        "loaded": True,
        "error": "",
        "progress_text": "",
    }
    cached_teams[team_key] = cached_team
    config["task_cache"] = {
        "version": TEAM_DASHBOARD_TASK_CACHE_VERSION,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "teams": cached_teams,
    }
    store.save(config)


def _load_team_dashboard_link_biz_jira_rows(settings: Settings, config: dict[str, Any]) -> list[dict[str, Any]]:
    bpmis_client = _build_bpmis_client_for_current_user(settings)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for team_key, _label in TEAM_DASHBOARD_TEAMS.items():
        team_config = (config.get("teams") or {}).get(team_key) or {}
        emails = _normalize_team_dashboard_emails(team_config.get("member_emails") or [])
        tasks = bpmis_client.list_jira_tasks_created_by_emails(
            emails,
            max_pages=_team_dashboard_jira_max_pages(),
            enrich_missing_parent=False,
            release_after=_team_dashboard_jira_release_after(),
        )
        for raw_task in tasks or []:
            if not isinstance(raw_task, dict):
                continue
            task = _normalize_team_dashboard_task(raw_task)
            if str((task.get("parent_project") or {}).get("bpmis_id") or "").strip():
                continue
            status_key = str(task.get("jira_status") or "").strip().casefold()
            if status_key not in TEAM_DASHBOARD_UNDER_PRD_STATUSES and status_key in TEAM_DASHBOARD_EXCLUDED_PENDING_STATUSES:
                continue
            jira_id = str(task.get("jira_id") or task.get("issue_id") or "").strip()
            if not jira_id or jira_id in seen:
                continue
            seen.add(jira_id)
            rows.append(_team_dashboard_link_biz_row_from_ticket(team_key, task, {}))
    rows.sort(key=lambda row: (str(row.get("team_key") or ""), str(row.get("jira_id") or "")))
    return rows


def _build_team_dashboard_link_biz_project_rows(team_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidate_projects: list[dict[str, Any]] = []
    unlinked_items: list[tuple[str, dict[str, Any]]] = []
    for team in team_payloads or []:
        team_key = str(team.get("team_key") or "").strip()
        for section_key in ("under_prd", "pending_live"):
            for project in team.get(section_key) or []:
                if not isinstance(project, dict):
                    continue
                bpmis_id = str(project.get("bpmis_id") or "").strip()
                tickets = project.get("jira_tickets") if isinstance(project.get("jira_tickets"), list) else []
                if bpmis_id:
                    candidate_projects.append(project)
                    continue
                for ticket in tickets:
                    if isinstance(ticket, dict):
                        unlinked_items.append((team_key, ticket))

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for team_key, ticket in unlinked_items:
        jira_id = str(ticket.get("jira_id") or ticket.get("issue_id") or "").strip()
        if not jira_id or jira_id in seen:
            continue
        seen.add(jira_id)
        pm_email = str(ticket.get("pm_email") or ticket.get("reporter_email") or "").strip().lower()
        pm_candidates = _team_dashboard_link_biz_filter_candidates_for_pm(candidate_projects, pm_email)
        suggestion = _suggest_team_dashboard_biz_project(ticket, _tag_team_dashboard_candidate_source(pm_candidates, "pm"))
        suggestion["select_biz_project_options"] = _team_dashboard_link_biz_project_options(pm_candidates)
        rows.append(_team_dashboard_link_biz_row_from_ticket(team_key, ticket, suggestion))
    rows.sort(key=lambda row: (str(row.get("team_key") or ""), str(row.get("jira_id") or "")))
    return rows


def _team_dashboard_link_biz_row_from_ticket(
    team_key: str,
    ticket: dict[str, Any],
    suggestion: dict[str, Any],
    *,
    select_options: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    jira_id = str(ticket.get("jira_id") or ticket.get("issue_id") or "").strip()
    jira_title = str(ticket.get("jira_title") or "").strip()
    special_version = _team_dashboard_link_biz_special_version(jira_title)
    row = {
        "team_key": team_key,
        "jira_id": jira_id,
        "jira_link": str(ticket.get("jira_link") or (f"{_jira_browse_base_url()}{jira_id}" if jira_id else "")).strip(),
        "jira_title": jira_title,
        "reporter_email": str(ticket.get("pm_email") or ticket.get("reporter_email") or "").strip().lower(),
        "suggested_bpmis_id": str(suggestion.get("bpmis_id") or ""),
        "suggested_project_title": str(suggestion.get("project_name") or ""),
        "match_score": float(suggestion.get("match_score") or 0.0),
        "match_source": str(suggestion.get("match_source") or ""),
        "select_biz_project_options": list(suggestion.get("select_biz_project_options") or []),
    }
    if special_version:
        row["special_version"] = special_version
        row["match_source"] = row["match_source"] or "version"
    if select_options is not None:
        row["select_biz_project_options"] = select_options
    return row


def _suggest_team_dashboard_link_biz_project_rows(
    settings: Settings,
    config: dict[str, Any],
    rows: list[Any],
    *,
    team_payloads: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    bpmis_client = _build_bpmis_client_for_current_user(settings)
    parsed_tickets: list[tuple[str, dict[str, str]]] = []
    pm_emails: list[str] = []
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            continue
        ticket = {
            "jira_id": str(raw_row.get("jira_id") or "").strip(),
            "jira_link": str(raw_row.get("jira_link") or "").strip(),
            "jira_title": str(raw_row.get("jira_title") or "").strip(),
            "pm_email": str(raw_row.get("reporter_email") or raw_row.get("pm_email") or "").strip().lower(),
        }
        if not ticket["jira_id"]:
            continue
        parsed_tickets.append((str(raw_row.get("team_key") or ""), ticket))
        if ticket["pm_email"]:
            pm_emails.append(ticket["pm_email"])
    candidates_by_pm = _team_dashboard_link_biz_candidate_projects_by_pm(
        bpmis_client,
        _normalize_team_dashboard_emails(pm_emails),
        team_payloads=team_payloads,
    )
    suggested_rows: list[dict[str, Any]] = []
    all_options: dict[str, dict[str, str]] = {}
    version_candidate_count = 0
    version_search_count = 0
    version_option_cache: dict[str, list[dict[str, str]]] = {}
    for team_key, ticket in parsed_tickets:
        special_version = _team_dashboard_link_biz_special_version(ticket["jira_title"])
        if special_version:
            version_search_count += 1
            if special_version not in version_option_cache:
                version_option_cache[special_version] = _team_dashboard_link_biz_version_project_options(
                    bpmis_client,
                    special_version,
                )
            version_options = version_option_cache[special_version]
            version_candidate_count += len(version_options)
            suggestion = version_options[0] if version_options else {}
            suggested_rows.append(
                _team_dashboard_link_biz_row_from_ticket(
                    team_key,
                    ticket,
                    suggestion,
                    select_options=version_options,
                )
            )
            continue
        pm_candidates = candidates_by_pm.get(ticket["pm_email"], [])
        row_options = _team_dashboard_link_biz_project_options(pm_candidates)
        for option in row_options:
            all_options[str(option.get("bpmis_id") or "")] = option
        suggestion = _suggest_team_dashboard_biz_project(ticket, _tag_team_dashboard_candidate_source(pm_candidates, "pm"))
        suggestion["select_biz_project_options"] = row_options
        suggested_rows.append(_team_dashboard_link_biz_row_from_ticket(team_key, ticket, suggestion))

    matched_count = len([row for row in suggested_rows if str(row.get("suggested_bpmis_id") or "").strip()])
    unique_candidate_ids = {
        str(project.get("bpmis_id") or "").strip()
        for candidates in candidates_by_pm.values()
        for project in candidates
        if str(project.get("bpmis_id") or "").strip()
    }
    return {
        "rows": suggested_rows,
        "matched_count": matched_count,
        "team_candidate_count": len(unique_candidate_ids),
        "keyword_candidate_count": 0,
        "keyword_search_count": 0,
        "version_candidate_count": version_candidate_count,
        "version_search_count": version_search_count,
        "select_biz_project_options": sorted(
            all_options.values(),
            key=lambda item: (str(item.get("project_name") or "").casefold(), str(item.get("bpmis_id") or "").casefold()),
        ),
    }


def _team_dashboard_link_biz_candidate_projects_from_payloads(team_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for team in team_payloads or []:
        if not isinstance(team, dict):
            continue
        for section_key in ("under_prd", "pending_live"):
            for project in team.get(section_key) or []:
                if isinstance(project, dict) and str(project.get("bpmis_id") or "").strip():
                    candidates.append({**project, "team_key": str(team.get("team_key") or project.get("team_key") or "").strip()})
    return _tag_team_dashboard_candidate_source(_dedupe_team_dashboard_candidate_projects(candidates), "team")


def _team_dashboard_link_biz_allowed_project_statuses() -> set[str]:
    return set(TEAM_DASHBOARD_UNDER_PRD_BIZ_PROJECT_STATUSES) | set(TEAM_DASHBOARD_PENDING_LIVE_BIZ_PROJECT_STATUSES)


def _team_dashboard_link_biz_project_status_allowed(project: dict[str, Any]) -> bool:
    status_key = str(project.get("status") or "").strip().casefold()
    return status_key in _team_dashboard_link_biz_allowed_project_statuses()


def _team_dashboard_link_biz_filter_candidates_for_pm(candidates: list[dict[str, Any]], pm_email: str) -> list[dict[str, Any]]:
    normalized_pm = str(pm_email or "").strip().lower()
    if not normalized_pm:
        return []
    filtered: list[dict[str, Any]] = []
    for raw_project in candidates or []:
        if not isinstance(raw_project, dict):
            continue
        project = _normalize_team_dashboard_project(raw_project)
        if not _team_dashboard_link_biz_project_status_allowed(project):
            continue
        matched_emails = _normalize_team_dashboard_emails(project.get("matched_pm_emails") or [])
        if not matched_emails:
            regional_pm = str(project.get("regional_pm_pic") or "").strip().lower()
            if regional_pm:
                matched_emails = _normalize_team_dashboard_emails([regional_pm])
        if normalized_pm not in matched_emails:
            continue
        filtered.append(
            {
                **project,
                "matched_pm_emails": matched_emails,
                "team_key": str(raw_project.get("team_key") or project.get("team_key") or "").strip(),
            }
        )
    return _dedupe_team_dashboard_candidate_projects(filtered)


def _team_dashboard_link_biz_candidate_projects_by_pm(
    bpmis_client: Any,
    pm_emails: list[str],
    *,
    team_payloads: list[dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    normalized_emails = _normalize_team_dashboard_emails(pm_emails)
    if not normalized_emails:
        return {}
    candidates_by_pm: dict[str, list[dict[str, Any]]] = {email: [] for email in normalized_emails}
    payload_candidates = _team_dashboard_link_biz_candidate_projects_from_payloads(team_payloads or []) if team_payloads else []
    for email in normalized_emails:
        candidates_by_pm[email] = _team_dashboard_link_biz_filter_candidates_for_pm(payload_candidates, email)

    missing_emails = [email for email in normalized_emails if not candidates_by_pm.get(email)]
    if missing_emails:
        loaded_projects = _team_dashboard_biz_projects_for_emails(bpmis_client, missing_emails)
        for email in missing_emails:
            candidates_by_pm[email] = _team_dashboard_link_biz_filter_candidates_for_pm(loaded_projects, email)
    return candidates_by_pm


def _team_dashboard_link_biz_project_options(candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    options: dict[str, dict[str, str]] = {}
    for raw_project in candidates or []:
        project = _normalize_team_dashboard_project(raw_project if isinstance(raw_project, dict) else {})
        if not _team_dashboard_link_biz_project_status_allowed(project):
            continue
        bpmis_id = str(project.get("bpmis_id") or "").strip()
        project_name = str(project.get("project_name") or "").strip()
        if not bpmis_id or not project_name:
            continue
        options[bpmis_id] = {
            "bpmis_id": bpmis_id,
            "project_name": project_name,
            "team_key": str(raw_project.get("team_key") or "").strip() if isinstance(raw_project, dict) else "",
            "market": str(project.get("market") or "").strip(),
        }
    return sorted(options.values(), key=lambda item: (item["project_name"].casefold(), item["bpmis_id"].casefold()))


def _team_dashboard_zero_jira_biz_project_options(team_payloads: list[dict[str, Any]]) -> list[dict[str, str]]:
    options: dict[str, dict[str, str]] = {}
    for team in team_payloads or []:
        if not isinstance(team, dict):
            continue
        for section_key in ("under_prd", "pending_live"):
            for raw_project in team.get(section_key) or []:
                if not isinstance(raw_project, dict):
                    continue
                project = _normalize_team_dashboard_project(raw_project)
                bpmis_id = str(project.get("bpmis_id") or "").strip()
                project_name = str(project.get("project_name") or "").strip()
                if not bpmis_id or not project_name or len(raw_project.get("jira_tickets") or []) > 0:
                    continue
                options[bpmis_id] = {
                    "bpmis_id": bpmis_id,
                    "project_name": project_name,
                    "team_key": str(team.get("team_key") or "").strip(),
                    "market": str(project.get("market") or "").strip(),
                }
    return sorted(options.values(), key=lambda item: (item["project_name"].casefold(), item["bpmis_id"].casefold()))


def _dedupe_team_dashboard_candidate_projects(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for raw_project in candidates:
        project = _normalize_team_dashboard_project(raw_project if isinstance(raw_project, dict) else {})
        if isinstance(raw_project, dict) and raw_project.get("team_key"):
            project["team_key"] = str(raw_project.get("team_key") or "").strip()
        bpmis_id = str(project.get("bpmis_id") or "").strip()
        if not bpmis_id or bpmis_id in deduped:
            continue
        deduped[bpmis_id] = project
    return list(deduped.values())


def _tag_team_dashboard_candidate_source(candidates: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    return [{**(candidate if isinstance(candidate, dict) else {}), "match_source": source} for candidate in candidates]


def _team_dashboard_link_biz_keyword_fallback_threshold() -> float:
    raw_value = str(os.getenv("TEAM_DASHBOARD_LINK_BIZ_KEYWORD_FALLBACK_SCORE") or "0.78").strip()
    try:
        return max(0.0, min(1.0, float(raw_value)))
    except ValueError:
        return 0.78


def _team_dashboard_link_biz_keywords(title: str) -> str:
    normalized = _normalize_team_dashboard_link_match_text(title)
    stop_words = {"the", "and", "for", "with", "from", "into", "jira", "feature", "support", "tech"}
    tokens = [token for token in normalized.split() if len(token) >= 3 and token not in stop_words]
    return " ".join(tokens[:8])


def _team_dashboard_link_biz_title_excluded(title: str) -> bool:
    normalized = str(title or "").casefold()
    return any(phrase in normalized for phrase in TEAM_DASHBOARD_LINK_BIZ_EXCLUDED_TITLE_PHRASES)


def _team_dashboard_link_biz_special_version(title: str) -> str:
    if not _team_dashboard_link_biz_title_excluded(title):
        return ""
    match = re.search(r"(?<!\d)(\d+\.\d+\.\d{2})(?!\d)", str(title or ""))
    return match.group(1) if match else ""


def _team_dashboard_link_biz_version_project_options(bpmis_client: Any, version: str) -> list[dict[str, str]]:
    prefix = f"AF_v{version}"
    if not version or not hasattr(bpmis_client, "search_versions") or not hasattr(bpmis_client, "list_issues_for_version"):
        return []
    try:
        version_rows = bpmis_client.search_versions(prefix) or []
    except Exception:  # noqa: BLE001 - version hints are best-effort; normal linking must still work.
        return []

    matching_versions: list[dict[str, str]] = []
    for raw_version in version_rows:
        if not isinstance(raw_version, dict):
            continue
        version_item = _serialize_productization_version_candidate(raw_version)
        version_name = str(version_item.get("version_name") or "").strip()
        version_id = str(version_item.get("version_id") or "").strip()
        if version_id and version_name.casefold().startswith(prefix.casefold()):
            matching_versions.append(version_item)

    options: dict[str, dict[str, str]] = {}
    for version_item in matching_versions:
        version_id = str(version_item.get("version_id") or "").strip()
        if not version_id:
            continue
        try:
            issue_rows = bpmis_client.list_issues_for_version(version_id) or []
        except Exception:  # noqa: BLE001 - ignore one bad version and keep other candidate versions usable.
            continue
        for issue in issue_rows:
            if not isinstance(issue, dict):
                continue
            for parent_id in sorted(_extract_parent_issue_ids_from_any(issue)):
                if parent_id in options:
                    continue
                project = _team_dashboard_link_biz_project_option_from_parent(bpmis_client, parent_id)
                if project:
                    options[parent_id] = project
    return sorted(options.values(), key=lambda item: (item["project_name"].casefold(), item["bpmis_id"].casefold()))


def _team_dashboard_link_biz_project_option_from_parent(bpmis_client: Any, parent_id: str) -> dict[str, str]:
    bpmis_id = str(parent_id or "").strip()
    if not bpmis_id:
        return {}
    detail: dict[str, Any] = {}
    if hasattr(bpmis_client, "get_issue_detail"):
        try:
            raw_detail = bpmis_client.get_issue_detail(bpmis_id)
            detail = raw_detail if isinstance(raw_detail, dict) else {}
        except Exception:  # noqa: BLE001 - a parent ID without detail should not break all suggestions.
            detail = {}
    project = _normalize_team_dashboard_project({**detail, "bpmis_id": bpmis_id, "issue_id": bpmis_id})
    project_name = str(project.get("project_name") or "").strip() or _extract_first_text(
        detail,
        "project_name",
        "summary",
        "title",
        "name",
        "issueName",
    )
    if not project_name:
        project_name = f"BPMIS {bpmis_id}"
    return {
        "bpmis_id": bpmis_id,
        "project_name": project_name,
        "team_key": "AF",
        "market": str(project.get("market") or "").strip(),
        "match_score": 1.0,
        "match_source": "version",
    }


def _normalize_team_dashboard_link_match_text(value: str) -> str:
    text = str(value or "").strip()
    while True:
        updated = re.sub(r"^\s*\[[^\]]+\]\s*", "", text).strip()
        if updated == text:
            break
        text = updated
    text = re.sub(r"[_\-|:/\\]+", " ", text.casefold())
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _suggest_team_dashboard_biz_project(ticket: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    source = _normalize_team_dashboard_link_match_text(str(ticket.get("jira_title") or ""))
    best: dict[str, Any] = {}
    best_score = -1.0
    best_sort_key: tuple[str, str] = ("", "")
    for project in candidates:
        bpmis_id = str(project.get("bpmis_id") or "").strip()
        project_name = str(project.get("project_name") or "").strip()
        target = _normalize_team_dashboard_link_match_text(project_name)
        if not bpmis_id or not target:
            continue
        score = difflib.SequenceMatcher(None, source, target).ratio() if source else 0.0
        sort_key = (project_name.casefold(), bpmis_id.casefold())
        if score > best_score or (score == best_score and (not best or sort_key < best_sort_key)):
            best_score = score
            best_sort_key = sort_key
            best = {
                "bpmis_id": bpmis_id,
                "project_name": project_name,
                "match_score": round(score, 4),
                "match_source": str(project.get("match_source") or "team"),
            }
    return best


def _extract_parent_issue_ids_from_any(value: Any) -> set[str]:
    if not isinstance(value, dict):
        return set()
    parent_values = value.get("parentIds")
    if parent_values is None:
        parent_values = value.get("parentIssueId")
    if parent_values is None and isinstance(value.get("raw_jira"), dict):
        return _extract_parent_issue_ids_from_any(value["raw_jira"])
    if not isinstance(parent_values, list):
        parent_values = [parent_values] if parent_values not in (None, "") else []
    ids: set[str] = set()
    for parent in parent_values:
        if isinstance(parent, dict):
            candidate = parent.get("id") or parent.get("issueId") or parent.get("value")
        else:
            candidate = parent
        text = str(candidate or "").strip()
        if text:
            ids.add(text)
    return ids


def _backfill_team_dashboard_empty_project_jira_tasks(bpmis_client: Any, team_payload: dict[str, Any]) -> None:
    candidates: list[dict[str, Any]] = []
    for section_key in ("under_prd", "pending_live"):
        projects = team_payload.get(section_key)
        if not isinstance(projects, list):
            continue
        for project in projects:
            if not isinstance(project, dict) or project.get("jira_tickets"):
                continue
            bpmis_id = str(project.get("bpmis_id") or "").strip()
            if not bpmis_id:
                continue
            emails = _team_dashboard_project_fallback_emails(project)
            if not emails:
                continue
            candidates.append({"project": project, "bpmis_id": bpmis_id, "emails": emails})
    _team_dashboard_increment_request_stat(
        bpmis_client,
        "team_dashboard_zero_jira_fallback_candidate_count",
        len(candidates),
    )
    bulk_lookup_handled = False
    if candidates and hasattr(bpmis_client, "list_jira_tasks_for_projects_created_by_emails"):
        project_ids = [candidate["bpmis_id"] for candidate in candidates]
        all_emails: list[str] = []
        for candidate in candidates:
            all_emails.extend(candidate["emails"])
        _team_dashboard_increment_request_stat(
            bpmis_client,
            "team_dashboard_zero_jira_bulk_project_count",
            len(project_ids),
        )
        try:
            grouped_rows = bpmis_client.list_jira_tasks_for_projects_created_by_emails(
                project_ids,
                _normalize_team_dashboard_emails(all_emails),
            )
        except Exception as error:  # noqa: BLE001 - keep the older per-project fallback available.
            _team_dashboard_increment_request_stat(bpmis_client, "team_dashboard_zero_jira_bulk_failed_count")
            current_app.logger.warning("Team Dashboard bulk Jira fallback lookup failed: %s", error)
            grouped_rows = None
        if isinstance(grouped_rows, dict):
            bulk_lookup_handled = True
            for candidate in candidates:
                rows = grouped_rows.get(candidate["bpmis_id"]) or []
                tickets = _normalize_team_dashboard_project_fallback_rows(
                    rows,
                    candidate["project"],
                    candidate["emails"],
                )
                if tickets:
                    _team_dashboard_increment_request_stat(bpmis_client, "team_dashboard_zero_jira_bulk_hit_count")
                    candidate["project"]["jira_tickets"] = tickets
                    candidate["project"]["task_count"] = len(tickets)
                    _apply_team_dashboard_project_release_date(candidate["project"])
    if bulk_lookup_handled:
        skipped_count = sum(
            1
            for candidate in candidates
            if isinstance(candidate.get("project"), dict) and not candidate["project"].get("jira_tickets")
        )
        _team_dashboard_increment_request_stat(
            bpmis_client,
            "team_dashboard_zero_jira_per_project_fallback_skipped_count",
            skipped_count,
        )
    if bulk_lookup_handled:
        for section_key in ("under_prd", "pending_live"):
            projects = team_payload.get(section_key)
            if section_key == "under_prd" and isinstance(projects, list):
                _sort_team_dashboard_under_prd_projects(projects)
        return
    for section_key in ("under_prd", "pending_live"):
        projects = team_payload.get(section_key)
        if not isinstance(projects, list):
            continue
        for project in projects:
            if not isinstance(project, dict) or project.get("jira_tickets"):
                continue
            bpmis_id = str(project.get("bpmis_id") or "").strip()
            if not bpmis_id:
                continue
            _team_dashboard_increment_request_stat(bpmis_client, "team_dashboard_zero_jira_per_project_fallback_count")
            tickets = _team_dashboard_project_fallback_jira_tasks(bpmis_client, project)
            if not tickets:
                continue
            project["jira_tickets"] = tickets
            project["task_count"] = len(tickets)
            _apply_team_dashboard_project_release_date(project)
        if section_key == "under_prd":
            _sort_team_dashboard_under_prd_projects(projects)


def _backfill_all_team_dashboard_empty_project_jira_tasks(
    bpmis_client: Any,
    team_payloads: list[dict[str, Any]],
) -> None:
    candidates: list[dict[str, Any]] = []
    for team_payload in team_payloads or []:
        if not isinstance(team_payload, dict):
            continue
        for section_key in ("under_prd", "pending_live"):
            projects = team_payload.get(section_key)
            if not isinstance(projects, list):
                continue
            for project in projects:
                if not isinstance(project, dict) or project.get("jira_tickets"):
                    continue
                bpmis_id = str(project.get("bpmis_id") or "").strip()
                if not bpmis_id:
                    continue
                emails = _team_dashboard_project_fallback_emails(project)
                if not emails:
                    continue
                candidates.append(
                    {
                        "team_key": str(team_payload.get("team_key") or ""),
                        "section_key": section_key,
                        "project": project,
                        "bpmis_id": bpmis_id,
                        "emails": emails,
                    }
                )
    _team_dashboard_increment_request_stat(
        bpmis_client,
        "team_dashboard_zero_jira_fallback_candidate_count",
        len(candidates),
    )
    if not candidates:
        return
    if not hasattr(bpmis_client, "list_jira_tasks_for_projects_created_by_emails"):
        for team_payload in team_payloads or []:
            if isinstance(team_payload, dict):
                _backfill_team_dashboard_empty_project_jira_tasks(bpmis_client, team_payload)
        return

    project_ids = list(dict.fromkeys(candidate["bpmis_id"] for candidate in candidates))
    all_emails: list[str] = []
    for candidate in candidates:
        all_emails.extend(candidate["emails"])
    all_emails = _normalize_team_dashboard_emails(all_emails)
    _team_dashboard_increment_request_stat(
        bpmis_client,
        "team_dashboard_zero_jira_bulk_project_count",
        len(project_ids),
    )
    try:
        grouped_rows = bpmis_client.list_jira_tasks_for_projects_created_by_emails(project_ids, all_emails)
    except Exception as error:  # noqa: BLE001 - preserve old per-team fallback behavior when bulk fails.
        _team_dashboard_increment_request_stat(bpmis_client, "team_dashboard_zero_jira_bulk_failed_count")
        current_app.logger.warning("Team Dashboard all-team bulk Jira fallback lookup failed: %s", error)
        for team_payload in team_payloads or []:
            if isinstance(team_payload, dict):
                _backfill_team_dashboard_empty_project_jira_tasks(bpmis_client, team_payload)
        return
    if not isinstance(grouped_rows, dict):
        for team_payload in team_payloads or []:
            if isinstance(team_payload, dict):
                _backfill_team_dashboard_empty_project_jira_tasks(bpmis_client, team_payload)
        return

    for candidate in candidates:
        rows = grouped_rows.get(candidate["bpmis_id"]) or []
        tickets = _normalize_team_dashboard_project_fallback_rows(
            rows,
            candidate["project"],
            candidate["emails"],
        )
        if tickets:
            _team_dashboard_increment_request_stat(bpmis_client, "team_dashboard_zero_jira_bulk_hit_count")
            candidate["project"]["jira_tickets"] = tickets
            candidate["project"]["task_count"] = len(tickets)
            _apply_team_dashboard_project_release_date(candidate["project"])
    skipped_count = sum(
        1
        for candidate in candidates
        if isinstance(candidate.get("project"), dict) and not candidate["project"].get("jira_tickets")
    )
    _team_dashboard_increment_request_stat(
        bpmis_client,
        "team_dashboard_zero_jira_per_project_fallback_skipped_count",
        skipped_count,
    )
    for team_payload in team_payloads or []:
        if not isinstance(team_payload, dict):
            continue
        projects = team_payload.get("under_prd")
        if isinstance(projects, list):
            _sort_team_dashboard_under_prd_projects(projects)


def _remove_team_dashboard_zero_jira_pending_live_projects(team_payload: dict[str, Any]) -> None:
    pending_live = team_payload.get("pending_live")
    if not isinstance(pending_live, list):
        return
    team_payload["pending_live"] = [
        project
        for project in pending_live
        if isinstance(project, dict) and len(project.get("jira_tickets") or []) > 0
    ]


def _team_dashboard_project_fallback_emails(project: dict[str, Any]) -> list[str]:
    emails = _normalize_team_dashboard_emails(project.get("matched_pm_emails") or [])
    if not emails:
        regional_pm = str(project.get("regional_pm_pic") or "").strip()
        if "@" in regional_pm:
            emails = _normalize_team_dashboard_emails([regional_pm])
    return emails


def _team_dashboard_project_parent_payload(project: dict[str, Any]) -> dict[str, str]:
    return {
        "bpmis_id": str(project.get("bpmis_id") or "").strip(),
        "project_name": str(project.get("project_name") or "").strip(),
        "market": str(project.get("market") or "").strip(),
        "priority": str(project.get("priority") or "").strip(),
        "regional_pm_pic": str(project.get("regional_pm_pic") or "").strip(),
        "status": str(project.get("status") or "").strip(),
    }


def _normalize_team_dashboard_project_fallback_rows(
    rows: list[dict[str, Any]],
    project: dict[str, Any],
    emails: list[str],
) -> list[dict[str, Any]]:
    parent_project = _team_dashboard_project_parent_payload(project)
    allowed_emails = set(_normalize_team_dashboard_emails(emails))
    tickets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        row_email = str(row.get("pm_email") or "").strip().lower()
        if allowed_emails and row_email and row_email not in allowed_emails:
            continue
        normalized = _normalize_team_dashboard_task(
            {
                **row,
                "pm_email": row.get("pm_email") or (next(iter(allowed_emails)) if allowed_emails else ""),
                "parent_project": row.get("parent_project") if isinstance(row.get("parent_project"), dict) else parent_project,
            }
        )
        status_key = str(normalized.get("jira_status") or "").strip().casefold()
        if status_key in TEAM_DASHBOARD_EXCLUDED_PENDING_STATUSES:
            continue
        dedupe_key = normalized.get("jira_id") or normalized.get("issue_id")
        if dedupe_key and dedupe_key in seen:
            continue
        if dedupe_key:
            seen.add(dedupe_key)
        tickets.append(normalized)
    tickets.sort(key=_team_dashboard_sort_key)
    return tickets


def _team_dashboard_project_fallback_jira_tasks(bpmis_client: Any, project: dict[str, Any]) -> list[dict[str, Any]]:
    bpmis_id = str(project.get("bpmis_id") or "").strip()
    if not bpmis_id or not hasattr(bpmis_client, "list_jira_tasks_for_project_created_by_email"):
        return []
    emails = _team_dashboard_project_fallback_emails(project)
    if not emails:
        return []
    tickets: list[dict[str, Any]] = []
    seen: set[str] = set()
    parent_project = _team_dashboard_project_parent_payload(project)
    for email in emails:
        try:
            rows = bpmis_client.list_jira_tasks_for_project_created_by_email(bpmis_id, email)
        except Exception as error:  # noqa: BLE001 - keep the project card renderable when fallback lookup fails.
            current_app.logger.warning("Team Dashboard Jira fallback lookup failed for %s/%s: %s", bpmis_id, email, error)
            continue
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            normalized = _normalize_team_dashboard_task(
                {
                    **row,
                    "pm_email": row.get("pm_email") or email,
                    "parent_project": row.get("parent_project") if isinstance(row.get("parent_project"), dict) else parent_project,
                }
            )
            status_key = str(normalized.get("jira_status") or "").strip().casefold()
            if status_key in TEAM_DASHBOARD_EXCLUDED_PENDING_STATUSES:
                continue
            dedupe_key = normalized.get("jira_id") or normalized.get("issue_id")
            if dedupe_key and dedupe_key in seen:
                continue
            if dedupe_key:
                seen.add(dedupe_key)
            tickets.append(normalized)
    tickets.sort(key=_team_dashboard_sort_key)
    return tickets


def _team_dashboard_jira_max_pages() -> int:
    raw_value = str(os.getenv("TEAM_DASHBOARD_JIRA_MAX_PAGES") or "5").strip()
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 5


def _team_dashboard_jira_release_after() -> str:
    configured = str(os.getenv("TEAM_DASHBOARD_JIRA_RELEASE_AFTER") or "").strip()
    if configured:
        return configured
    return time.strftime("%Y-%m-%d", time.localtime())


def _team_dashboard_monthly_report_jira_release_after() -> str:
    configured = str(os.getenv("TEAM_DASHBOARD_MONTHLY_REPORT_JIRA_RELEASE_AFTER") or "").strip()
    if configured:
        return configured
    return time.strftime("%Y-%m-%d", time.localtime(time.time() - 60 * 60 * 24 * 45))


def _normalize_team_dashboard_task(task: dict[str, Any]) -> dict[str, Any]:
    jira_id = str(task.get("jira_id") or task.get("ticket_key") or "").strip()
    issue_id = str(task.get("issue_id") or "").strip()
    jira_link = str(task.get("jira_link") or task.get("ticket_link") or "").strip()
    if not jira_link and jira_id:
        jira_link = f"{_jira_browse_base_url()}{jira_id}"
    raw_prd_links = task.get("prd_links")
    if not raw_prd_links and task.get("prd_link"):
        raw_prd_links = str(task.get("prd_link") or "").splitlines()
    prd_links = _team_dashboard_link_items(raw_prd_links)
    return {
        "issue_id": issue_id,
        "jira_id": jira_id or issue_id,
        "jira_link": jira_link,
        "jira_title": str(task.get("jira_title") or "").strip(),
        "pm_email": str(task.get("pm_email") or "").strip().lower(),
        "jira_status": str(task.get("jira_status") or task.get("status") or "").strip(),
        "created_at": str(task.get("created_at") or task.get("created") or "").strip(),
        "release_date": _format_team_dashboard_release_date(task.get("release_date") or task.get("release")),
        "version": str(task.get("version") or task.get("fix_version_name") or "").strip(),
        "description": str(task.get("description") or task.get("desc") or task.get("jiraDescription") or "").strip(),
        "prd_links": prd_links,
        "parent_project": _normalize_team_dashboard_project(task.get("parent_project") if isinstance(task.get("parent_project"), dict) else {}),
    }


def _normalize_team_dashboard_project(project: dict[str, Any]) -> dict[str, str]:
    bpmis_id = str(project.get("bpmis_id") or project.get("issue_id") or "").strip()
    matched_pm_emails = _normalize_team_dashboard_emails(project.get("matched_pm_emails") or [])
    normalized: dict[str, Any] = {
        "bpmis_id": bpmis_id,
        "project_name": str(project.get("project_name") or "").strip(),
        "market": str(project.get("market") or "").strip(),
        "priority": str(project.get("priority") or "").strip(),
        "regional_pm_pic": str(project.get("regional_pm_pic") or "").strip(),
        "status": str(project.get("status") or project.get("biz_project_status") or "").strip(),
    }
    if matched_pm_emails:
        normalized["matched_pm_emails"] = matched_pm_emails
    return normalized


def _split_team_dashboard_biz_projects_by_status(
    biz_projects: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    under_prd: list[dict[str, Any]] = []
    pending_live: list[dict[str, Any]] = []
    for raw_project in biz_projects:
        project = _normalize_team_dashboard_project(raw_project if isinstance(raw_project, dict) else {})
        status_key = str(project.get("status") or "").strip().casefold()
        if status_key in TEAM_DASHBOARD_UNDER_PRD_BIZ_PROJECT_STATUSES:
            under_prd.append(project)
        elif status_key in TEAM_DASHBOARD_PENDING_LIVE_BIZ_PROJECT_STATUSES:
            pending_live.append(project)
    return under_prd, pending_live


def _group_team_dashboard_tasks_by_project(
    tasks: list[dict[str, Any]],
    *,
    sort_by_release: bool = False,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for task in tasks:
        project = task.get("parent_project") if isinstance(task.get("parent_project"), dict) else {}
        project = _normalize_team_dashboard_project(project)
        key = project.get("bpmis_id") or "unknown"
        if key not in grouped:
            if key == "unknown":
                project = {
                    "bpmis_id": "",
                    "project_name": "BPMIS unavailable",
                    "market": "",
                    "priority": "",
                    "regional_pm_pic": "",
                }
            grouped[key] = {
                **project,
                "jira_tickets": [],
                "task_count": 0,
                "release_date": "-",
                "release_date_sort": "",
            }
        grouped[key]["jira_tickets"].append(task)
        grouped[key]["task_count"] = len(grouped[key]["jira_tickets"])

    projects = list(grouped.values())
    for project in projects:
        project["jira_tickets"].sort(key=_team_dashboard_sort_key)
        _apply_team_dashboard_project_release_date(project)
    if sort_by_release:
        projects.sort(key=_team_dashboard_project_release_sort_key)
    else:
        projects.sort(key=_team_dashboard_project_name_sort_key)
    for project in projects:
        project.pop("release_date_sort", None)
    return projects


def _merge_team_dashboard_biz_projects(
    projects: list[dict[str, Any]],
    biz_projects: list[dict[str, Any]],
    *,
    sort_by_release: bool = False,
) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {
        str(project.get("bpmis_id") or "").strip(): project
        for project in projects
        if str(project.get("bpmis_id") or "").strip()
    }
    merged = list(projects)
    for raw_project in biz_projects:
        project = _normalize_team_dashboard_project(raw_project if isinstance(raw_project, dict) else {})
        bpmis_id = project.get("bpmis_id")
        if not bpmis_id:
            continue
        existing = by_id.get(bpmis_id)
        if existing:
            for key in ("project_name", "market", "priority", "regional_pm_pic", "status"):
                if project.get(key) and not existing.get(key):
                    existing[key] = project[key]
            _merge_team_dashboard_project_pm_emails(existing, project.get("matched_pm_emails") or [])
            continue
        project.update(
            {
                "jira_tickets": [],
                "task_count": 0,
                "release_date": "-",
                "release_date_sort": "",
            }
        )
        merged.append(project)
        by_id[bpmis_id] = project
    if sort_by_release:
        merged.sort(key=_team_dashboard_project_release_sort_key)
    else:
        merged.sort(key=_team_dashboard_project_name_sort_key)
    for project in merged:
        project.pop("release_date_sort", None)
    return merged


def _merge_team_dashboard_project_pm_emails(project: dict[str, Any], emails: list[str]) -> None:
    existing = _normalize_team_dashboard_emails(project.get("matched_pm_emails") or [])
    for email in _normalize_team_dashboard_emails(emails):
        if email not in existing:
            existing.append(email)
    if existing:
        project["matched_pm_emails"] = existing


def _apply_team_dashboard_key_project_states(projects: list[dict[str, Any]], overrides: dict[str, Any]) -> None:
    for project in projects:
        _apply_team_dashboard_key_project_state(project, overrides)


def _apply_team_dashboard_key_project_state(project: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    bpmis_id = str(project.get("bpmis_id") or "").strip()
    override = overrides.get(bpmis_id) if bpmis_id and isinstance(overrides, dict) else None
    if isinstance(override, dict) and "is_key_project" in override:
        is_key_project = bool(override.get("is_key_project"))
        project["is_key_project"] = is_key_project
        project["key_project_source"] = "manual_on" if is_key_project else "manual_off"
        project["key_project_override"] = {
            "is_key_project": is_key_project,
            "updated_by": str(override.get("updated_by") or "").strip().lower(),
            "updated_at": str(override.get("updated_at") or "").strip(),
        }
        return project
    priority = str(project.get("priority") or "").strip().casefold()
    is_priority_default = priority in {"sp", "p0"}
    project["is_key_project"] = is_priority_default
    project["key_project_source"] = "priority_default" if is_priority_default else "none"
    project.pop("key_project_override", None)
    return project


def _apply_team_dashboard_project_release_date(project: dict[str, Any]) -> None:
    latest = None
    for task in project.get("jira_tickets") or []:
        parsed, _text = _parse_team_dashboard_release_date(task.get("release_date"))
        if parsed and (latest is None or parsed > latest):
            latest = parsed
    if latest:
        project["release_date"] = time.strftime("%Y-%m-%d", latest)
        project["release_date_sort"] = time.strftime("%Y-%m-%d", latest)
    else:
        project["release_date"] = "-"
        project["release_date_sort"] = ""


def _format_team_dashboard_release_date(value: Any) -> str:
    parsed, text = _parse_team_dashboard_release_date(value)
    if parsed:
        return time.strftime("%Y-%m-%d", parsed)
    return text


def _parse_team_dashboard_release_date(value: Any) -> tuple[time.struct_time | None, str]:
    text = str(value or "").strip()
    if not text:
        return None, ""
    for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return time.strptime(text[:10], pattern), text
        except ValueError:
            continue
    return None, text


def _team_dashboard_project_name_sort_key(project: dict[str, Any]) -> tuple[str, str]:
    return (
        str(project.get("project_name") or "").casefold(),
        str(project.get("bpmis_id") or "").casefold(),
    )


def _sort_team_dashboard_under_prd_projects(projects: list[dict[str, Any]]) -> None:
    projects.sort(key=_team_dashboard_under_prd_project_sort_key)


def _team_dashboard_under_prd_project_sort_key(project: dict[str, Any]) -> tuple[int, str, str, str]:
    release_sort = str(project.get("release_date_sort") or "").strip()
    if not release_sort:
        parsed, _text = _parse_team_dashboard_release_date(project.get("release_date"))
        if parsed:
            release_sort = time.strftime("%Y-%m-%d", parsed)
    jira_count = len(project.get("jira_tickets") or [])
    if release_sort:
        bucket = 0
    elif jira_count > 0:
        bucket = 1
    else:
        bucket = 2
    return (
        bucket,
        release_sort,
        str(project.get("project_name") or "").casefold(),
        str(project.get("bpmis_id") or "").casefold(),
    )


def _team_dashboard_project_release_sort_key(project: dict[str, Any]) -> tuple[int, str, str, str]:
    release_sort = str(project.get("release_date_sort") or "").strip()
    if not release_sort:
        parsed, _text = _parse_team_dashboard_release_date(project.get("release_date"))
        if parsed:
            release_sort = time.strftime("%Y-%m-%d", parsed)
    return (
        0 if release_sort else 1,
        release_sort,
        str(project.get("project_name") or "").casefold(),
        str(project.get("bpmis_id") or "").casefold(),
    )


def _team_dashboard_link_items(value: Any) -> list[dict[str, str]]:
    raw_links: list[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                raw_links.append(str(item.get("url") or item.get("label") or "").strip())
            else:
                raw_links.append(str(item or "").strip())
    elif isinstance(value, str):
        raw_links.extend(item.strip() for item in re.split(r"[\n,]+", value) if item.strip())
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for link in raw_links:
        if not link or link in seen:
            continue
        seen.add(link)
        deduped.append({"label": link, "url": link})
    return deduped


def _team_dashboard_sort_key(task: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(task.get("pm_email") or "").casefold(),
        str(task.get("version") or "").casefold(),
        str(task.get("jira_id") or "").casefold(),
    )


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
