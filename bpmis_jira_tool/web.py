from __future__ import annotations

from dataclasses import asdict, dataclass, field
from functools import lru_cache
import html
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

from flask import Flask, current_app, flash, g, jsonify, redirect, render_template, request, send_file, session, url_for
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from openpyxl import Workbook
from openpyxl.styles import Font

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.google_auth import (
    create_google_authorization_url,
    finish_google_oauth,
    get_google_credentials,
)
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE, GmailDashboardService
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService
from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.project_sync import BPMISProjectSyncService
from bpmis_jira_tool.service import JiraCreationService, build_bpmis_client
from bpmis_jira_tool.source_code_qa import CRMS_COUNTRIES, ALL_COUNTRY, SourceCodeQAService
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
_gmail_export_active_users: set[str] = set()
_gmail_export_active_users_lock = threading.Lock()


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
        self._jobs: dict[str, JobState] = self._load()
        self._lock = threading.Lock()

    def _load(self) -> dict[str, JobState]:
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
                jobs[str(job_id)] = JobState(**raw_job)
            except TypeError:
                continue
        return jobs

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
            job = self._jobs[job_id]
            job.state = "failed"
            job.stage = "failed"
            job.message = error
            job.error = error
            job.updated_at = time.time()
            self._persist_locked()

    def get(self, job_id: str) -> JobState | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return JobState(**asdict(job))

    def snapshot(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        return asdict(job) if job else None


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
    app.config["JOB_STORE"] = JobStore(data_root / "run" / "jobs.json")
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
        if _can_access_prd_briefing(settings):
            site_tabs.append(
                {
                    "label": "PRD Briefing Tool",
                    "href": url_for("prd_briefing.portal"),
                    "active": current_endpoint.startswith("prd_briefing"),
                }
            )
        if _can_access_gmail_seatalk_demo(settings):
            site_tabs.append(
                {
                    "label": "Gmail & SeaTalk Demo",
                    "href": url_for("gmail_seatalk_demo"),
                    "active": request.path.startswith("/gmail-sea-talk-demo"),
                }
            )
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
        raw_config_data = config_store.load(config_key) if config_key else None
        effective_team_profiles = _load_effective_team_profiles(config_store)
        config_data = raw_config_data or config_store._normalize({})
        config_data = _hydrate_setup_defaults(config_data, user_identity, team_profiles=effective_team_profiles)
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
            options=service.options_payload(),
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
            return jsonify(
                {
                    "status": "ok",
                    "answer_mode": "retrieval_only",
                    "can_manage": _can_manage_source_code_qa(settings),
                    "git_auth_ready": bool(settings.source_code_qa_gitlab_token),
                    "llm_ready": service.llm_ready(),
                    "llm_provider": settings.source_code_qa_llm_provider,
                    "llm_model": service.llm_budgets["balanced"]["model"],
                    "llm_fast_model": service.llm_budgets["cheap"]["model"],
                    "llm_deep_model": service.llm_budgets["deep"]["model"],
                    "llm_fallback_model": service._llm_fallback_model(),
                    "llm_policy": service.llm_policy_payload(),
                    "index_health": service.index_health_payload(),
                    "release_gate": _source_code_qa_release_gate_payload(settings),
                    "domain_knowledge": service.domain_knowledge_payload(),
                    "options": service.options_payload(),
                    "config": service.load_config(),
                }
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

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

    @app.post("/api/source-code-qa/query")
    def source_code_qa_query_api():
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            service = _build_source_code_qa_service()
            auto_sync = service.ensure_synced_today(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
            )
            result = service.query(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
                question=str(payload.get("question") or ""),
                answer_mode=str(payload.get("answer_mode") or "retrieval_only"),
                llm_budget_mode="auto",
                conversation_context=payload.get("conversation_context") if isinstance(payload.get("conversation_context"), dict) else None,
            )
            result["auto_sync"] = auto_sync
            return jsonify(result)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

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
                config_store.migrate(previous_identity["config_key"], current_identity["config_key"])
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
            page_title="Gmail & SeaTalk Demo",
            user_identity=user_identity,
            google_connected="google_credentials" in session,
            gmail_scope_ready=_google_credentials_have_scopes(GMAIL_READONLY_SCOPE),
            seatalk_configured=_seatalk_dashboard_is_configured(settings),
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
            existing_config = config_store.load(user_identity["config_key"]) or config_store._normalize({})
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
            config = config_store._normalize(
                _hydrate_setup_defaults(
                    config,
                    user_identity,
                    team_profiles=_load_effective_team_profiles(config_store),
                )
            )
            if save_mode == "route_only":
                _validate_config_security(settings, config)
                _save_route_only_config(existing_config, config, user_identity["config_key"])
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
            config_store.save(config, user_identity["config_key"])
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
            existing_config = config_store.load(user_identity["config_key"]) or config_store._normalize({})
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
                user_identity["config_key"],
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
        user_key: str,
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
        config_store.save(normalized, user_key)
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
            saved_profile = config_store.save_team_profile(
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
            return jsonify(
                {
                    "status": "ok",
                    "items": [_normalize_productization_issue_row(item) for item in rows],
                    "raw_count": raw_count,
                    "filtered_count": len(rows),
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
        if "google_credentials" not in session:
            return jsonify({"status": "error", "message": "Please connect Google Sheets first."}), 400

        user_identity = _get_user_identity(settings)
        config_data = config_store.load(user_identity["config_key"]) or config_store._normalize({})
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
    config_data = config_store.load(user_identity["config_key"]) or config_store._normalize({})
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
    config_data = config_store.load(user_identity["config_key"]) or config_store._normalize({})
    return _hydrate_setup_defaults(config_data, user_identity)


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


def _build_sheets_service(settings: Settings, config_data: dict[str, object] | None = None) -> GoogleSheetsService:
    credentials = get_google_credentials()
    return _build_sheets_service_with_credentials(settings, config_data or {}, credentials)


def _build_gmail_dashboard_service() -> GmailDashboardService:
    credentials = get_google_credentials()
    return GmailDashboardService(credentials=credentials, cache_key=_current_google_email())


def _build_seatalk_dashboard_service(settings: Settings) -> SeaTalkDashboardService:
    return SeaTalkDashboardService(
        owner_email=settings.seatalk_owner_email,
        seatalk_app_path=settings.seatalk_local_app_path,
        seatalk_data_dir=settings.seatalk_local_data_dir,
    )


def _seatalk_dashboard_is_configured(settings: Settings) -> bool:
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
    return bool(email and email == settings.gmail_seatalk_demo_owner_email.strip().lower())


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
    return bool(email and email == settings.source_code_qa_owner_email.strip().lower())


def _build_source_code_qa_service() -> SourceCodeQAService:
    return current_app.config["SOURCE_CODE_QA_SERVICE"]


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


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
    message = f"Gmail & SeaTalk Demo is restricted to {settings.gmail_seatalk_demo_owner_email.strip().lower()}."
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
    message = f"Source Code Q&A repository admin is restricted to {settings.source_code_qa_owner_email.strip().lower()}."
    if not _can_manage_source_code_qa(settings):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
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
    for team_key, stored_profile in config_store.load_team_profiles().items():
        if team_key not in profiles:
            continue
        profiles[team_key].update(stored_profile)
    return profiles


def _is_team_profile_admin(user_identity: dict[str, str | None]) -> bool:
    return str(user_identity.get("email") or "").strip().lower() == TEAM_PROFILE_ADMIN_EMAIL


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
    errors = [result for result in results if getattr(result, "status", "") == "error"]
    skipped = [result for result in results if getattr(result, "status", "") == "skipped"]

    details: list[str] = []
    for result in created[:3]:
        details.append(f"Added BPMIS Issue ID {result.issue_id} to row {result.row_number}.")
    for result in errors[:3]:
        details.append(f"{result.issue_id or 'Unknown Issue'}: {result.message}")
    if not details:
        for result in skipped[:3]:
            details.append(f"{result.issue_id}: {result.message}")

    return {
        "title": "BPMIS Sync Completed" if not errors else "BPMIS Sync Completed With Issues",
        "tone": "success" if not errors else "warning",
        "summary": f"{len(created)} added, {len(skipped)} skipped, {len(errors)} error.",
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
                service = _build_project_sync_service_from_config(settings, config_data, credentials_payload)
                results = service.sync_projects(
                    pm_email=str(config_data.get("sync_pm_email", "")).strip(),
                    issue_id_header=str(config_data.get("issue_id_header", "")).strip() or "Issue ID",
                    project_name_header=str(config_data.get("sync_project_name_header", "")).strip() or "Project Name",
                    market_header=str(config_data.get("sync_market_header", "")).strip() or "Market",
                    brd_link_header=str(config_data.get("sync_brd_link_header", "")).strip(),
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
        "detailed_feature": _summarize_productization_detail(
            _extract_first_text(row, "desc", "description", "jiraDescription")
        ),
        "pm": _extract_person_display(
            _extract_first_value(row, "jiraRegionalPmPicId", "regionalPmPic", "productManager", "pm", "regionalPm")
        )
        or "-",
        "prd_links": _extract_link_values(
            _extract_first_value(row, "jiraPrdLink", "prdLink", "prdLinks", "prd", "brdLink")
        ),
    }


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


def _summarize_productization_detail(value: str) -> str:
    text = _clean_productization_detail_text(value)
    if not text:
        return "-"
    return text


def _clean_productization_detail_text(value: str) -> str:
    if not str(value or "").strip():
        return ""

    text = html.unescape(value)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\r", "\n")
    lines = [_clean_productization_detail_line(line) for line in text.split("\n")]
    chunks = [line for line in lines if line]
    return "\n".join(chunks).strip()


def _clean_productization_detail_line(line: str) -> str:
    text = re.sub(r"\s+", " ", str(line or "")).strip(" -*\t")
    if not text:
        return ""

    text = re.sub(r"\[(?:https?://[^\]]+|[^\]]*link[^\]]*)\]", " ", text, flags=re.I)
    text = re.sub(r"\[(https?://[^\]]+)\]", " ", text, flags=re.I)
    text = re.sub(r"https?://\S+", " ", text, flags=re.I)
    text = re.sub(r"\[\s*\d+(?:\.\d+)+\s*", "", text)
    text = re.sub(
        r"^(?:\[(?:feature|productization effort|tech|support)\]\s*)+",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"^(?:prd|sheet|link|docs?)\s*:\s*", "", text, flags=re.I)
    text = re.sub(r"section\s+\d+(?:\.\d+)+\s*", "", text, flags=re.I)
    text = re.sub(r"\b(?:scenario|scene)\s*[a-z]*\d+\b", " ", text, flags=re.I)
    text = re.sub(r"\brow\s*\d+(?:\s*-\s*\d+)?\b", " ", text, flags=re.I)
    text = re.sub(r"\bmain\s+track\s+jira\s*:\s*", " ", text, flags=re.I)
    text = re.sub(
        r"\b[a-z]{2}\s+anti\s*fraud\s+ver(?:sion|ision)\s+plan\s*_?feature\s+timeline\s*:?\s*",
        " ",
        text,
        flags=re.I,
    )
    text = re.sub(r"\bprd\s*:?\s*", " ", text, flags=re.I)
    text = re.sub(r"\b(?:infra|version|release)\s*[:：]?\s*v?\d+(?:\.\d+)+(?:[_-]\d+)?\b", " ", text, flags=re.I)
    text = re.sub(r"\bv?\d+(?:\.\d+){1,}(?:[_-]\d+)?\b", " ", text, flags=re.I)
    text = re.sub(r"\|\s*", " ", text)
    text = re.sub(r"\[\s*\]", " ", text)
    text = re.sub(r"\(\s*\)", " ", text)
    text = text.replace("[", " ").replace("]", " ")
    text = re.sub(
        r"(?i)(?:,?\s*(?:refer to|see also|see|please refer to|please see)\b.*$)",
        "",
        text,
    )
    text = re.sub(r"(?:，?\s*(?:详见|见|请参考|参考)\s*[:：]?\s*.*$)", "", text)
    text = re.sub(r"\b(?:for\s+(?:retail|sme|retail and sme))\b", " ", text, flags=re.I)
    text = re.sub(r"\(\s*[^)]*\s*\)", lambda match: " " if len(match.group(0)) <= 24 else match.group(0), text)
    text = re.sub(r"\s+", " ", text).strip(" ,;:-")
    if text.lower() in {"prd", "sheet", "link", "docs", "doc"}:
        return ""

    if not text:
        return ""
    if re.fullmatch(r"[^\w\u4e00-\u9fff]*", text):
        return ""
    if _looks_like_productization_outline_fragment(text):
        return ""
    return text


def _looks_like_productization_outline_fragment(text: str) -> bool:
    normalized = text.strip().lower()
    if not normalized:
        return True

    outline_markers = (
        "fe ui pages",
        "be ui pages",
        "ui data points",
        "data points",
        "prd section",
        "appendix",
        "checklist",
        "release note",
        "reference",
    )
    if any(marker in normalized for marker in outline_markers):
        token_count = len(re.findall(r"[a-zA-Z\u4e00-\u9fff0-9]+", normalized))
        has_action_word = bool(
            re.search(
                r"\b(add|update|support|enable|disable|improve|optimi[sz]e|create|build|fix|remove|change|migrate|"
                r"launch|implement|allow|introduce|enhance)\b",
                normalized,
            )
        )
        if token_count <= 12 and not has_action_word:
            return True
    return False


@lru_cache(maxsize=1)
def _build_productization_argos_translator():
    try:
        from argostranslate import package, translate
    except ImportError:
        return None

    installed_languages = translate.get_installed_languages()
    from_language = next((language for language in installed_languages if language.code == "zh"), None)
    to_language = next((language for language in installed_languages if language.code == "en"), None)
    if from_language is None or to_language is None:
        return None

    try:
        return from_language.get_translation(to_language)
    except Exception:
        return None


def _translate_productization_text_to_english(text: str) -> str:
    normalized = str(text or "").strip()
    if not normalized or not re.search(r"[\u4e00-\u9fff]", normalized):
        return normalized

    translator = _build_productization_argos_translator()
    if translator is None:
        return normalized

    translated_lines: list[str] = []
    for line in normalized.split("\n"):
        cleaned_line = line.strip()
        if not cleaned_line:
            continue
        if not re.search(r"[\u4e00-\u9fff]", cleaned_line):
            translated_lines.append(cleaned_line)
            continue
        try:
            translated_line = translator.translate(cleaned_line).strip()
        except Exception:
            translated_line = cleaned_line
        translated_lines.append(translated_line or cleaned_line)
    return "\n".join(translated_lines).strip()


@lru_cache(maxsize=1)
def _build_productization_textrank_pipeline():
    try:
        import pytextrank  # noqa: F401
        import spacy
    except ImportError:
        return None

    nlp = None
    for model_name in ("en_core_web_sm", "en_core_web_md"):
        try:
            nlp = spacy.load(model_name)
            break
        except OSError:
            continue

    if nlp is None:
        nlp = spacy.blank("en")
        if "sentencizer" not in nlp.pipe_names:
            nlp.add_pipe("sentencizer")

    if "textrank" not in nlp.pipe_names:
        nlp.add_pipe("textrank")
    return nlp


def _try_productization_textrank_summary(text: str) -> str:
    nlp = _build_productization_textrank_pipeline()
    if nlp is None:
        return ""

    try:
        doc = nlp(text)
        summary_spans = list(doc._.textrank.summary(limit_phrases=12, limit_sentences=2))
    except Exception:
        return ""

    summary = " ".join(span.text.strip() for span in summary_spans if span.text.strip())
    return re.sub(r"\s+", " ", summary).strip()


def _fallback_productization_summary(text: str) -> str:
    lines = [re.sub(r"\s+", " ", line).strip(" -*\t") for line in text.split("\n")]
    chunks = [line for line in lines if line]
    if not chunks:
        return "-"

    selected: list[str] = []
    for chunk in chunks:
        parts = [part.strip() for part in re.split(r"(?<=[.!?;。！？；])\s+", chunk) if part.strip()]
        for part in parts or [chunk]:
            if len(part) >= 8:
                selected.append(part)
            if len(selected) == 2:
                break
        if len(selected) == 2:
            break

    summary = " ".join(selected or chunks[:1]).strip()
    return _trim_productization_summary(summary)


def _trim_productization_summary(summary: str) -> str:
    summary = re.sub(r"\s+", " ", summary).strip()
    return summary or "-"


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
