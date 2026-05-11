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
from types import SimpleNamespace
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
from bpmis_jira_tool.meeting_translation import (
    MEETING_TRANSLATION_LANGUAGES,
    MeetingTranslationConfig,
    MeetingTranslationRuntime,
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
from bpmis_jira_tool.source_code_qa_llm_providers import LLM_PROVIDER_ALLOWED_QUERY_CHOICES, LLM_PROVIDER_CODEX_CLI_BRIDGE
from bpmis_jira_tool.source_code_qa_sql_artifacts import (
    build_source_code_qa_sql_readme as _build_source_code_qa_sql_readme,
    extract_source_code_qa_sql_blocks as _extract_source_code_qa_sql_blocks,
    format_source_code_qa_sql_text as _format_source_code_qa_sql_text,
)
from bpmis_jira_tool.web_source_code_qa_runtime import (
    bind_source_code_qa_runtime_helpers,
    _source_code_qa_codex_session_lock,
    _record_source_code_qa_work_memory,
    _get_source_code_qa_session_store,
    _get_source_code_qa_attachment_store,
    _get_source_code_qa_generated_artifact_store,
    _get_source_code_qa_runtime_evidence_store,
    _can_access_source_code_qa,
    _can_manage_source_code_qa,
    _source_code_qa_auth_payload,
    _source_code_qa_git_auth_ready,
    _build_source_code_qa_service,
    _source_code_qa_query_sync_mode,
    _source_code_qa_scope_has_queryable_index,
    _prepare_source_code_qa_auto_sync,
    _local_agent_source_code_qa_enabled,
    _source_code_qa_options_payload,
    _source_code_qa_runtime_capabilities_payload,
    _source_code_qa_provider_available,
    _source_code_qa_public_answer_mode,
    _source_code_qa_query_mode,
    _source_code_qa_attachment_ids,
    _resolve_source_code_qa_query_attachments,
    _source_code_qa_public_attachments,
    _resolve_source_code_qa_runtime_evidence,
    _source_code_qa_public_runtime_evidence,
    _source_code_qa_public_generated_artifacts,
    _build_source_code_qa_generated_artifacts,
    _build_source_code_qa_session_context,
    _source_code_qa_release_gate_payload,
    _require_source_code_qa_access,
    _require_source_code_qa_manage_access,
    _classify_source_code_qa_job_error,
    _public_source_code_qa_job_snapshot,
    _source_code_qa_job_snapshot_for_current_user,
)
from bpmis_jira_tool.web_source_code_qa_effort import (
    bind_source_code_qa_effort_helpers,
    _source_code_qa_effort_assessment_language,
    _source_code_qa_effort_sentences,
    _source_code_qa_effort_matches,
    _source_code_qa_effort_unique,
    _source_code_qa_load_json_file,
    _load_source_code_qa_effort_dictionaries,
    _load_source_code_qa_domain_profile_config,
    _load_source_code_qa_domain_knowledge_config,
    _source_code_qa_effort_domain_entries,
    _source_code_qa_effort_country_hint,
    _source_code_qa_effort_term_matches,
    _source_code_qa_effort_scope_terms_by_team,
    _source_code_qa_effort_scope_guard,
    _source_code_qa_effort_scope_mismatch_result,
    _source_code_qa_effort_seed_terms,
    _source_code_qa_effort_entry_applies,
    _source_code_qa_effort_group_typed_candidates,
    _build_source_code_qa_effort_business_plan,
    _build_source_code_qa_effort_technical_candidates,
    _build_source_code_qa_effort_estimation_rubric,
    _source_code_qa_effort_json_block,
    _build_source_code_qa_effort_assessment_prompt,
    _source_code_qa_effort_compact_terms,
    _build_source_code_qa_effort_evidence_query,
    _source_code_qa_effort_evidence_digest,
    _source_code_qa_effort_runtime_digest,
    _source_code_qa_effort_cache_key,
    _source_code_qa_effort_cache_root,
    _load_source_code_qa_effort_cached_result,
    _store_source_code_qa_effort_cached_result,
    _source_code_qa_effort_compact_evidence,
    _source_code_qa_effort_matrix_terms,
    _source_code_qa_effort_match_text,
    _source_code_qa_effort_matrix_quality,
    _build_source_code_qa_effort_evidence_matrix,
    _source_code_qa_effort_generic_output_guard,
    _build_source_code_qa_effort_compact_synthesis_prompt,
    _source_code_qa_effort_missing_evidence,
    _source_code_qa_effort_confidence,
    _source_code_qa_effort_code_change_points,
    _source_code_qa_effort_fallback_answer,
    _build_source_code_qa_effort_structured_assessment,
    _source_code_qa_effort_sanitize_visible_answer,
    _normalize_source_code_qa_effort_assessment_result,
    _run_source_code_qa_effort_assessment_job,
)
from bpmis_jira_tool.web_source_code_qa_jobs import (
    bind_source_code_qa_job_helpers,
    _run_source_code_qa_sync_job,
    _run_source_code_qa_query_job,
)
from bpmis_jira_tool.source_code_qa_stores import (
    SourceCodeQAAttachmentStore,
    SourceCodeQAGeneratedArtifactStore,
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
from bpmis_jira_tool.web_bpmis_routes import build_bpmis_handlers, register_bpmis_routes
from bpmis_jira_tool.web_gmail_seatalk_routes import build_gmail_seatalk_handlers, register_gmail_seatalk_routes
from bpmis_jira_tool.web_meeting_recorder_routes import build_meeting_recorder_handlers, register_meeting_recorder_routes
from bpmis_jira_tool.web_prd_self_assessment_routes import build_prd_self_assessment_handlers, register_prd_self_assessment_routes
from bpmis_jira_tool.web_productization_routes import build_productization_handlers, register_productization_routes
from bpmis_jira_tool.web_source_code_qa_routes import register_source_code_qa_routes
from bpmis_jira_tool.web_team_dashboard_routes import build_team_dashboard_handlers, register_team_dashboard_routes
from bpmis_jira_tool.web_work_memory_routes import register_work_memory_routes
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
        if SeaTalkNameMappingStore.is_ignored_key(row.get("id")):
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
        "scheduled_auto_stop": record.get("scheduled_auto_stop") or {},
        "status": record.get("status"),
        "transcript_language": normalize_meeting_transcript_language(record.get("transcript_language")),
        "transcript_language_label": record.get("transcript_language_label") or "",
        "recording_started_at": record.get("recording_started_at"),
        "recording_stopped_at": record.get("recording_stopped_at"),
        "recording_stop_reason": record.get("recording_stop_reason") or "",
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
        elif ("codex" in normalized or "llm" in normalized) and ("rate limit" in normalized or "quota" in normalized):
            details.update({"error_category": "codex_timeout_or_rate_limit", "error_code": "llm_rate_limited", "error_retryable": True})
        elif ("codex" in normalized or "llm" in normalized) and ("timeout" in normalized or "timed out" in normalized):
            details.update({"error_category": "codex_timeout_or_rate_limit", "error_code": "llm_timeout", "error_retryable": True})
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
    bind_source_code_qa_runtime_helpers(globals())
    bind_source_code_qa_effort_helpers(globals())
    bind_source_code_qa_job_helpers(globals())

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

    def _queue_scheduled_meeting_auto_process(record: dict[str, Any]) -> dict[str, Any]:
        with app.app_context():
            return _meeting_recorder_auto_process_payload(
                settings=settings,
                record_id=str(record.get("record_id") or ""),
                owner_email=str(record.get("owner_email") or ""),
            )

    app.config["MEETING_RECORDER_RUNTIME"] = MeetingRecorderRuntime(
        store=meeting_store,
        config=_meeting_recorder_config(settings),
        scheduled_auto_stop_callback=_queue_scheduled_meeting_auto_process,
    )
    app.config["MEETING_TRANSLATION_RUNTIME"] = MeetingTranslationRuntime(
        root_dir=data_root / "run" / "meeting_translation",
        config=_meeting_translation_config(settings),
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

        def _nav_group(label: str, href: str, children: list[dict[str, Any]]) -> dict[str, Any]:
            return {
                "label": label,
                "href": href,
                "active": any(child.get("active") for child in children),
                "children": children,
            }

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
                    "label": "Source Code",
                    "href": url_for("source_code_qa"),
                    "active": request.path.startswith("/source-code-qa"),
                }
            )
        meeting_tabs = []
        if _can_access_meeting_recorder(settings):
            meeting_tabs.append(
                {
                    "label": "Meeting Recorder",
                    "href": url_for("meeting_recorder_page"),
                    "active": request.path.startswith("/meeting-recorder"),
                }
            )
            meeting_tabs.append(
                {
                    "label": "Meeting Translation",
                    "href": url_for("meeting_translation_page"),
                    "active": request.path.startswith("/meeting-translation"),
                }
            )
            site_tabs.append(_nav_group("Meeting Module", url_for("meeting_recorder_page"), meeting_tabs))
        prd_tabs = []
        if _can_access_prd_self_assessment(settings):
            prd_tabs.append(
                {
                    "label": "PRD Self-Assessment",
                    "href": url_for("prd_self_assessment_page"),
                    "active": current_endpoint == "prd_self_assessment_page",
                }
            )
        if _can_access_prd_briefing(settings):
            prd_tabs.append(
                {
                    "label": "PRD Briefing Tool",
                    "href": url_for("prd_briefing.portal"),
                    "active": current_endpoint.startswith("prd_briefing"),
                }
            )
        if prd_tabs:
            site_tabs.append(_nav_group("PRDs", prd_tabs[0]["href"], prd_tabs))
        project_tabs = []
        if can_access_team_dashboard:
            project_tabs.append(
                {
                    "label": "Team Dashboard",
                    "href": url_for("team_dashboard_page"),
                    "active": current_endpoint == "team_dashboard_page",
                }
            )
        project_tabs.append(
            {
                "label": "BPMIS Automation Tool",
                "href": url_for("index", workspace="run"),
                "active": current_endpoint == "index",
            }
        )
        if can_access_team_dashboard:
            project_tabs.append(
                {
                    "label": "Reports",
                    "href": url_for("reports_page"),
                    "active": current_endpoint == "reports_page",
                }
            )
        if _can_access_work_memory(settings):
            project_tabs.append(
                {
                    "label": "AI Memory",
                    "href": url_for("work_memory_page"),
                    "active": request.path.startswith("/work-memory"),
                }
            )
        project_href = url_for("team_dashboard_page") if can_access_team_dashboard else url_for("index", workspace="run")
        site_tabs.append(_nav_group("Projects", project_href, project_tabs))
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
        if request.endpoint == "static" and request.path.endswith((".css", ".js")):
            response.headers["Cache-Control"] = "no-store, private, max-age=0, must-revalidate"
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

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok", "revision": _current_release_revision()}), HTTPStatus.OK

    @app.get("/access-denied")
    def access_denied():
        return render_template("access_denied.html", page_title="Access Restricted"), HTTPStatus.FORBIDDEN

    register_source_code_qa_routes(app, settings, globals())
    register_prd_self_assessment_routes(
        app,
        build_prd_self_assessment_handlers(
            SimpleNamespace(
                settings=settings,
                _require_prd_self_assessment_access=_require_prd_self_assessment_access,
                _get_user_identity=_get_user_identity,
                _current_release_revision=_current_release_revision,
                _run_prd_self_assessment_action=_run_prd_self_assessment_action,
                _local_agent_source_code_qa_enabled=_local_agent_source_code_qa_enabled,
                _build_local_agent_client=_build_local_agent_client,
                _get_prd_latest_result=_get_prd_latest_result,
                web_globals=globals(),
            )
        ),
    )

    register_work_memory_routes(
        app,
        settings,
        SimpleNamespace(
            require_work_memory_access=_require_work_memory_access,
            local_agent_work_memory_enabled=_local_agent_work_memory_enabled,
            build_local_agent_client=_build_local_agent_client,
            get_work_memory_store=_get_work_memory_store,
            current_google_email=_current_google_email,
            ingest_sent_monthly_reports_from_gmail=_ingest_sent_monthly_reports_from_gmail,
            classify_portal_error=_classify_portal_error,
            ingest_existing_work_memory_sources=_ingest_existing_work_memory_sources,
            run_incremental_memory_ingestion=_run_incremental_memory_ingestion,
            google_credentials_have_scopes=_google_credentials_have_scopes,
            get_team_dashboard_config_store=_get_team_dashboard_config_store,
            run_work_memory_gmail_backfill_job=_run_work_memory_gmail_backfill_job,
            GMAIL_READONLY_SCOPE=GMAIL_READONLY_SCOPE,
            GOOGLE_DRIVE_READONLY_SCOPE=GOOGLE_DRIVE_READONLY_SCOPE,
            WORK_MEMORY_GMAIL_BACKFILL_ACTION=WORK_MEMORY_GMAIL_BACKFILL_ACTION,
        ),
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

    register_gmail_seatalk_routes(
        app,
        build_gmail_seatalk_handlers(
            SimpleNamespace(
                web_globals=globals(),
                settings=settings,
                GMAIL_READONLY_SCOPE=GMAIL_READONLY_SCOPE,
                _require_gmail_seatalk_demo_access=_require_gmail_seatalk_demo_access,
                _google_credentials_have_scopes=_google_credentials_have_scopes,
                _build_gmail_dashboard_service=_build_gmail_dashboard_service,
                _build_seatalk_dashboard_service=_build_seatalk_dashboard_service,
                _classify_portal_error=_classify_portal_error,
                _log_portal_event=_log_portal_event,
                _build_request_log_context=_build_request_log_context,
                _get_user_identity=_get_user_identity,
                _safe_email_identity=_safe_email_identity,
                _try_acquire_gmail_export_lock=_try_acquire_gmail_export_lock,
                _release_gmail_export_lock=_release_gmail_export_lock,
                _current_google_email=_current_google_email,
                _get_seatalk_todo_store=_get_seatalk_todo_store,
                _get_seatalk_name_mapping_store=_get_seatalk_name_mapping_store,
                _callable_accepts_keyword=_callable_accepts_keyword,
                _dedupe_seatalk_name_mapping_candidates=_dedupe_seatalk_name_mapping_candidates,
            )
        ),
    )

    @app.post("/auth/google/logout")
    def google_logout():
        session.pop("google_credentials", None)
        session.pop("google_profile", None)
        flash("Google session cleared.", "success")
        return redirect(url_for("index"))

    @app.route("/api/local-agent/<path:agent_path>", methods=["GET", "POST", "PATCH", "DELETE"])
    def local_agent_public_proxy(agent_path: str):
        return _proxy_local_agent_request(agent_path)

    @app.route("/uat-local-agent/healthz", methods=["GET"])
    def uat_local_agent_health_proxy():
        return _proxy_local_agent_request("healthz", base_url=_uat_local_agent_loopback_base_url(), use_api_prefix=False)

    @app.route("/uat-local-agent/api/local-agent/<path:agent_path>", methods=["GET", "POST", "PATCH", "DELETE"])
    def uat_local_agent_public_proxy(agent_path: str):
        return _proxy_local_agent_request(agent_path, base_url=_uat_local_agent_loopback_base_url(), use_api_prefix=True)

    register_productization_routes(
        app,
        build_productization_handlers(
            SimpleNamespace(
                web_globals=globals(),
                settings=settings,
                _require_google_login=_require_google_login,
                _build_bpmis_client_for_current_user=_build_bpmis_client_for_current_user,
                _serialize_productization_version_candidate=_serialize_productization_version_candidate,
                _load_current_user_config=_load_current_user_config,
                _filter_productization_issue_rows_for_pm_team=_filter_productization_issue_rows_for_pm_team,
                _normalize_productization_issue_row=_normalize_productization_issue_row,
                _apply_codex_productization_detailed_features=_apply_codex_productization_detailed_features,
                _classify_portal_error=_classify_portal_error,
                _log_portal_event=_log_portal_event,
                _build_request_log_context=_build_request_log_context,
            )
        ),
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

    register_meeting_recorder_routes(
        app,
        build_meeting_recorder_handlers(
            SimpleNamespace(
                settings=settings,
                _require_meeting_recorder_access=lambda *args, **kwargs: _require_meeting_recorder_access(*args, **kwargs),
                _get_user_identity=lambda *args, **kwargs: _get_user_identity(*args, **kwargs),
                _google_credentials_have_scopes=lambda *args, **kwargs: _google_credentials_have_scopes(*args, **kwargs),
                _current_release_revision=lambda *args, **kwargs: _current_release_revision(*args, **kwargs),
                _current_google_email=lambda *args, **kwargs: _current_google_email(*args, **kwargs),
                _local_agent_meeting_recorder_enabled=lambda *args, **kwargs: _local_agent_meeting_recorder_enabled(*args, **kwargs),
                _log_portal_event=lambda *args, **kwargs: _log_portal_event(*args, **kwargs),
                _build_local_agent_client=lambda *args, **kwargs: _build_local_agent_client(*args, **kwargs),
                _get_meeting_translation_runtime=lambda *args, **kwargs: _get_meeting_translation_runtime(*args, **kwargs),
                _meeting_translation_sse_events=lambda *args, **kwargs: _meeting_translation_sse_events(*args, **kwargs),
                _get_meeting_recorder_runtime=lambda *args, **kwargs: _get_meeting_recorder_runtime(*args, **kwargs),
                _build_request_log_context=lambda *args, **kwargs: _build_request_log_context(*args, **kwargs),
                _classify_portal_error=lambda *args, **kwargs: _classify_portal_error(*args, **kwargs),
                _build_calendar_meeting_service=lambda *args, **kwargs: _build_calendar_meeting_service(*args, **kwargs),
                _get_meeting_record_store=lambda *args, **kwargs: _get_meeting_record_store(*args, **kwargs),
                _meeting_record_summary=lambda *args, **kwargs: _meeting_record_summary(*args, **kwargs),
                _meeting_recorder_auto_process_payload=lambda *args, **kwargs: _meeting_recorder_auto_process_payload(*args, **kwargs),
                _queue_meeting_recorder_process_job=lambda *args, **kwargs: _queue_meeting_recorder_process_job(*args, **kwargs),
                _meeting_recorder_process_job_snapshot_for_current_user=lambda *args, **kwargs: _meeting_recorder_process_job_snapshot_for_current_user(*args, **kwargs),
                _public_meeting_recorder_process_job_snapshot=lambda *args, **kwargs: _public_meeting_recorder_process_job_snapshot(*args, **kwargs),
                _build_meeting_processing_service=lambda *args, **kwargs: _build_meeting_processing_service(*args, **kwargs),
                MEETING_RECORDER_UPCOMING_DISPLAY_LIMIT=MEETING_RECORDER_UPCOMING_DISPLAY_LIMIT,
            )
        ),
    )
    register_team_dashboard_routes(
        app,
        build_team_dashboard_handlers(
            SimpleNamespace(
                settings=settings,
                _require_team_dashboard_access=lambda *args, **kwargs: _require_team_dashboard_access(*args, **kwargs),
                _require_team_dashboard_monthly_report_access=lambda *args, **kwargs: _require_team_dashboard_monthly_report_access(*args, **kwargs),
                _get_user_identity=lambda *args, **kwargs: _get_user_identity(*args, **kwargs),
                _get_team_dashboard_config_store=lambda *args, **kwargs: _get_team_dashboard_config_store(*args, **kwargs),
                _can_manage_team_dashboard=lambda *args, **kwargs: _can_manage_team_dashboard(*args, **kwargs),
                _can_access_team_dashboard_monthly_report=lambda *args, **kwargs: _can_access_team_dashboard_monthly_report(*args, **kwargs),
                _seatalk_dashboard_is_configured=lambda *args, **kwargs: _seatalk_dashboard_is_configured(*args, **kwargs),
                _log_portal_event=lambda *args, **kwargs: _log_portal_event(*args, **kwargs),
                _build_request_log_context=lambda *args, **kwargs: _build_request_log_context(*args, **kwargs),
                _local_agent_seatalk_enabled=lambda *args, **kwargs: _local_agent_seatalk_enabled(*args, **kwargs),
                _build_local_agent_client=lambda *args, **kwargs: _build_local_agent_client(*args, **kwargs),
                _get_daily_brief_archive_store=lambda *args, **kwargs: _get_daily_brief_archive_store(*args, **kwargs),
                _get_seatalk_name_mapping_store=lambda *args, **kwargs: _get_seatalk_name_mapping_store(*args, **kwargs),
                _build_seatalk_dashboard_service=lambda *args, **kwargs: _build_seatalk_dashboard_service(*args, **kwargs),
                _dedupe_seatalk_name_mapping_candidates=lambda *args, **kwargs: _dedupe_seatalk_name_mapping_candidates(*args, **kwargs),
                _classify_portal_error=lambda *args, **kwargs: _classify_portal_error(*args, **kwargs),
                _load_team_dashboard_tasks_for_all_teams_merged=lambda *args, **kwargs: _load_team_dashboard_tasks_for_all_teams_merged(*args, **kwargs),
                _record_team_dashboard_work_memory=lambda *args, **kwargs: _record_team_dashboard_work_memory(*args, **kwargs),
                _current_google_email=lambda *args, **kwargs: _current_google_email(*args, **kwargs),
                _team_dashboard_new_timing=lambda *args, **kwargs: _team_dashboard_new_timing(*args, **kwargs),
                _team_dashboard_add_timing=lambda *args, **kwargs: _team_dashboard_add_timing(*args, **kwargs),
                _normalize_team_dashboard_emails=lambda *args, **kwargs: _normalize_team_dashboard_emails(*args, **kwargs),
                _cached_team_dashboard_task_payload=lambda *args, **kwargs: _cached_team_dashboard_task_payload(*args, **kwargs),
                _build_bpmis_client_for_current_user=lambda *args, **kwargs: _build_bpmis_client_for_current_user(*args, **kwargs),
                _team_dashboard_load_jira_and_biz_projects=lambda *args, **kwargs: _team_dashboard_load_jira_and_biz_projects(*args, **kwargs),
                _build_team_dashboard_task_group=lambda *args, **kwargs: _build_team_dashboard_task_group(*args, **kwargs),
                _backfill_team_dashboard_empty_project_jira_tasks=lambda *args, **kwargs: _backfill_team_dashboard_empty_project_jira_tasks(*args, **kwargs),
                _remove_team_dashboard_zero_jira_pending_live_projects=lambda *args, **kwargs: _remove_team_dashboard_zero_jira_pending_live_projects(*args, **kwargs),
                _team_dashboard_combined_request_timings=lambda *args, **kwargs: _team_dashboard_combined_request_timings(*args, **kwargs),
                _team_dashboard_combined_fetch_stats=lambda *args, **kwargs: _team_dashboard_combined_fetch_stats(*args, **kwargs),
                _store_team_dashboard_task_payload=lambda *args, **kwargs: _store_team_dashboard_task_payload(*args, **kwargs),
                _apply_team_dashboard_key_project_state=lambda *args, **kwargs: _apply_team_dashboard_key_project_state(*args, **kwargs),
                _load_team_dashboard_link_biz_jira_rows=lambda *args, **kwargs: _load_team_dashboard_link_biz_jira_rows(*args, **kwargs),
                _suggest_team_dashboard_link_biz_project_rows=lambda *args, **kwargs: _suggest_team_dashboard_link_biz_project_rows(*args, **kwargs),
                _extract_issue_key_from_text=lambda *args, **kwargs: _extract_issue_key_from_text(*args, **kwargs),
                _team_dashboard_link_biz_candidate_projects_by_pm=lambda *args, **kwargs: _team_dashboard_link_biz_candidate_projects_by_pm(*args, **kwargs),
                _extract_parent_issue_ids_from_any=lambda *args, **kwargs: _extract_parent_issue_ids_from_any(*args, **kwargs),
                _normalize_team_dashboard_project=lambda *args, **kwargs: _normalize_team_dashboard_project(*args, **kwargs),
                _jira_browse_base_url=lambda *args, **kwargs: _jira_browse_base_url(*args, **kwargs),
                _load_all_team_dashboard_task_payloads=lambda *args, **kwargs: _load_all_team_dashboard_task_payloads(*args, **kwargs),
                _remote_bpmis_config_enabled=lambda *args, **kwargs: _remote_bpmis_config_enabled(*args, **kwargs),
                _run_team_dashboard_monthly_report_draft_job=lambda *args, **kwargs: _run_team_dashboard_monthly_report_draft_job(*args, **kwargs),
                _google_credentials_have_scopes=lambda *args, **kwargs: _google_credentials_have_scopes(*args, **kwargs),
                _ingest_sent_monthly_reports_from_gmail=lambda *args, **kwargs: _ingest_sent_monthly_reports_from_gmail(*args, **kwargs),
                _local_agent_work_memory_enabled=lambda *args, **kwargs: _local_agent_work_memory_enabled(*args, **kwargs),
                _get_work_memory_store=lambda *args, **kwargs: _get_work_memory_store(*args, **kwargs),
                _local_agent_source_code_qa_enabled=lambda *args, **kwargs: _local_agent_source_code_qa_enabled(*args, **kwargs),
                _build_prd_review_service=lambda *args, **kwargs: _build_prd_review_service(*args, **kwargs),
                resolve_monthly_report_period=lambda *args, **kwargs: resolve_monthly_report_period(*args, **kwargs),
                send_monthly_report_email=lambda *args, **kwargs: send_monthly_report_email(*args, **kwargs),
            )
        ),
    )
    register_bpmis_routes(
        app,
        build_bpmis_handlers(
            SimpleNamespace(
                settings=settings,
                config_store=config_store,
                MARKET_KEYS=MARKET_KEYS,
                _require_google_login=lambda *args, **kwargs: _require_google_login(*args, **kwargs),
                _get_user_identity=lambda *args, **kwargs: _get_user_identity(*args, **kwargs),
                _load_user_config_for_identity=lambda *args, **kwargs: _load_user_config_for_identity(*args, **kwargs),
                _apply_sync_email_policy=lambda *args, **kwargs: _apply_sync_email_policy(*args, **kwargs),
                _hydrate_setup_defaults=lambda *args, **kwargs: _hydrate_setup_defaults(*args, **kwargs),
                _load_effective_team_profiles=lambda *args, **kwargs: _load_effective_team_profiles(*args, **kwargs),
                _validate_config_security=lambda *args, **kwargs: _validate_config_security(*args, **kwargs),
                _save_user_config_for_identity=lambda *args, **kwargs: _save_user_config_for_identity(*args, **kwargs),
                _log_portal_event=lambda *args, **kwargs: _log_portal_event(*args, **kwargs),
                _build_request_log_context=lambda *args, **kwargs: _build_request_log_context(*args, **kwargs),
                _build_mapping_log_summary=lambda *args, **kwargs: _build_mapping_log_summary(*args, **kwargs),
                _classify_portal_error=lambda *args, **kwargs: _classify_portal_error(*args, **kwargs),
                _validate_team_profile_setup=lambda *args, **kwargs: _validate_team_profile_setup(*args, **kwargs),
                _is_team_profile_admin=lambda *args, **kwargs: _is_team_profile_admin(*args, **kwargs),
                _save_team_profile=lambda *args, **kwargs: _save_team_profile(*args, **kwargs),
                _count_configured_lines=lambda *args, **kwargs: _count_configured_lines(*args, **kwargs),
                _start_job=lambda *args, **kwargs: _start_job(*args, **kwargs),
                _get_bpmis_project_store=lambda *args, **kwargs: _get_bpmis_project_store(*args, **kwargs),
                _build_portal_jira_creation_service=lambda *args, **kwargs: _build_portal_jira_creation_service(*args, **kwargs),
            )
        ),
    )
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


def _meeting_translation_config(settings: Settings) -> MeetingTranslationConfig:
    return MeetingTranslationConfig(
        ffmpeg_bin=settings.meeting_recorder_ffmpeg_bin,
        openai_api_key=settings.meeting_translation_openai_api_key,
        model=settings.meeting_translation_model,
        background_nice=settings.meeting_recorder_background_nice,
        capture_status_every_buffers=settings.meeting_recorder_capture_status_every_buffers,
    )


def _get_meeting_record_store() -> MeetingRecordStore:
    return current_app.config["MEETING_RECORD_STORE"]


def _get_meeting_recorder_runtime() -> MeetingRecorderRuntime:
    return current_app.config["MEETING_RECORDER_RUNTIME"]


def _get_meeting_translation_runtime() -> MeetingTranslationRuntime:
    return current_app.config["MEETING_TRANSLATION_RUNTIME"]


def _get_work_memory_store() -> WorkMemoryStore:
    return current_app.config["WORK_MEMORY_STORE"]


def _meeting_translation_sse_events(events: Any):
    for event in events:
        yield f"data: {json.dumps(event, ensure_ascii=False, separators=(',', ':'))}\n\n"


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
        'in:sent newer_than:365d from:me subject:"[Banking] Product Update"',
        'in:sent newer_than:365d from:me subject:"Anti-Fraud, Credit Risk & Ops Risk"',
        'in:sent newer_than:365d from:me "[Banking] Product Update" "Anti-Fraud, Credit Risk & Ops Risk"',
    ]
    seen_ids: set[str] = set()
    records = []
    for query in queries:
        for message_id in service._list_message_ids(query=query, max_messages=50):
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






def _can_access_work_memory(settings: Settings) -> bool:
    return _is_portal_admin()














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
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_monthly_report_draft_tool_error",
                level=logging.WARNING,
                user=_safe_email_identity(user_identity),
                job_id=job_id,
                **error_details,
            )
            job_store.fail(
                job_id,
                str(error),
                error_category=str(error_details.get("error_category") or "tool_error"),
                error_code=str(error_details.get("error_code") or "tool_error"),
                error_retryable=bool(error_details.get("error_retryable", True)),
            )
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
