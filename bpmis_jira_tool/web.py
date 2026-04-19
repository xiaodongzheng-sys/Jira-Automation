from __future__ import annotations

from dataclasses import asdict, dataclass, field
import io
from http import HTTPStatus
from pathlib import Path
import re
import secrets
import threading
import time
from typing import Any
import uuid

from flask import Flask, current_app, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.google_auth import (
    create_google_authorization_url,
    finish_google_oauth,
    get_google_credentials,
)
from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.project_sync import BPMISProjectSyncService
from bpmis_jira_tool.service import JiraCreationService, build_bpmis_client
from bpmis_jira_tool.user_config import (
    CONFIGURED_FIELDS,
    DEFAULT_NEED_UAT_BY_MARKET,
    DEFAULT_SHEET_HEADERS,
    TEAM_DEFAULT_EMAIL_PLACEHOLDER,
    TEAM_PROFILE_DEFAULTS,
    WebConfigStore,
)
from prd_briefing import create_prd_briefing_blueprint
from prd_briefing.storage import BriefingStore
from incident_mockup.app import create_app as create_incident_mockup_app


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
MARKET_KEYS = ["ID", "SG", "PH", "Regional"]


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
    def __init__(self) -> None:
        self._jobs: dict[str, JobState] = {}
        self._lock = threading.Lock()

    def create(self, action: str, title: str) -> JobState:
        with self._lock:
            job = JobState(
                job_id=uuid.uuid4().hex,
                action=action,
                title=title,
                message="Queued and waiting to start.",
            )
            self._jobs[job.job_id] = job
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

    def complete(self, job_id: str, *, results: list[dict[str, Any]], notice: dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.state = "completed"
            job.stage = "completed"
            job.message = "Finished."
            job.results = results
            job.notice = notice
            job.updated_at = time.time()

    def fail(self, job_id: str, error: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.state = "failed"
            job.stage = "failed"
            job.message = error
            job.error = error
            job.updated_at = time.time()

    def get(self, job_id: str) -> JobState | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return JobState(**asdict(job))

    def snapshot(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        return asdict(job) if job else None


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
    app.config["JOB_STORE"] = JobStore()
    app.config["PRD_BRIEFING_STORE"] = BriefingStore(data_root / "prd_briefing")
    app.config["GET_USER_IDENTITY"] = lambda: _get_user_identity(settings)
    app.config["CAN_ACCESS_PRD_BRIEFING"] = lambda: _can_access_prd_briefing(settings)
    app.config["CAN_ACCESS_GRC_DEMO"] = lambda: _can_access_grc_demo(settings)
    app.register_blueprint(create_prd_briefing_blueprint())
    grc_demo_app = create_incident_mockup_app(
        owner_email=settings.grc_demo_owner_email,
        secret_key=settings.flask_secret_key,
    )
    app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/grc-demo": grc_demo_app.wsgi_app})

    @app.context_processor
    def inject_primary_navigation():
        current_endpoint = request.endpoint or ""
        if _site_requires_google_login(settings) and not _google_session_is_connected():
            return {
                "site_tabs": [],
                "site_requires_google_login": True,
                "can_access_prd_briefing": False,
                "can_access_grc_demo": False,
            }
        site_tabs = [
            {
                "label": "BPMIS Automation Tool",
                "href": url_for("index"),
                "active": current_endpoint == "index",
            }
        ]
        if _can_access_prd_briefing(settings):
            site_tabs.append(
                {
                    "label": "PRD Briefing Tool",
                    "href": url_for("prd_briefing.portal"),
                    "active": current_endpoint.startswith("prd_briefing"),
                }
            )
        if _can_access_grc_demo(settings):
            site_tabs.append(
                {
                    "label": "GRC Demo",
                    "href": "/grc-demo/",
                    "active": request.path.startswith("/grc-demo/"),
                }
            )
        return {
            "site_tabs": site_tabs,
            "site_requires_google_login": _site_requires_google_login(settings),
            "can_access_prd_briefing": _can_access_prd_briefing(settings),
            "can_access_grc_demo": _can_access_grc_demo(settings),
        }

    @app.before_request
    def enforce_team_access():
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

    @app.get("/")
    def index():
        if _current_google_user_is_blocked(settings):
            session.pop("google_credentials", None)
            session.pop("google_profile", None)
            flash("This Google account is not authorized for the team portal. Please contact the maintainer.", "error")
            return redirect(url_for("access_denied"))

        if _site_requires_google_login(settings) and not _google_session_is_connected():
            return render_template("login_gate.html", page_title="Sign In")

        results = session.pop("last_results", [])
        run_notice = session.pop("run_notice", None)
        user_identity = _get_user_identity(settings)
        config_key = user_identity.get("config_key")
        config_data = config_store.load(config_key) if config_key else None
        config_data = config_data or config_store._normalize({})
        config_data = _hydrate_setup_defaults(config_data, user_identity)
        input_headers: list[str] = []
        has_saved_config = bool(config_key and config_store.load(config_key))

        if "google_credentials" in session:
            try:
                sheets = _build_sheets_service(settings, config_data)
                snapshot = sheets.read_snapshot()
                input_headers = snapshot.headers
            except ToolError as error:
                flash(str(error), "error")
            except Exception:
                flash("Google Sheets is connected, but the current sheet could not be read. Please reconnect Google or verify your Spreadsheet settings.", "error")

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
            team_profiles=_build_team_profiles_for_display(config_data, user_identity),
            default_workspace_tab="run" if has_saved_config else "setup",
            input_headers=input_headers,
            google_authorized=True,
        )

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok"}), HTTPStatus.OK

    @app.get("/access-denied")
    def access_denied():
        return render_template("access_denied.html", page_title="Access Restricted"), HTTPStatus.FORBIDDEN

    @app.get("/auth/google/login")
    def google_login():
        try:
            authorization_url = create_google_authorization_url(settings)
            return redirect(authorization_url)
        except ConfigError as error:
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
            flash("Google Sheets connected successfully.", "success")
        except ToolError as error:
            flash(str(error), "error")
        return redirect(url_for("index"))

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
            config = config_store._normalize(_hydrate_setup_defaults(config, user_identity))
            _validate_config_security(settings, config)
            _validate_team_profile_setup(config)
            config_store.build_field_mappings(config)
            config_store.save(config, user_identity["config_key"])
            flash("Your web Jira config was saved for this user and will be used for preview/run.", "success")
        except ToolError as error:
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.get("/download/default-sheet-template.csv")
    def download_default_sheet_template():
        sample_row = [
            "225159",
            "Standalone Cash Loan",
            "SG",
            "https://docs.google.com/document/d/example",
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
            "SG",
            "https://docs.google.com/document/d/example",
            "AF",
            "Fraud rule improvement",
            "https://confluence/example-prd",
            "Detailed Jira description goes here.",
            "",
        ]
        worksheet.append(DEFAULT_SHEET_HEADERS)
        worksheet.append(sample_row)

        header_fill = PatternFill(fill_type="solid", fgColor="DCEBFF")
        header_font = Font(bold=True, color="1F2937")
        for cell in worksheet[1]:
            cell.fill = header_fill
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
            session["last_results"] = [result.__dict__ for result in results]
        except ToolError as error:
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
            session["last_results"] = [result.__dict__ for result in results]
            session["run_notice"] = _build_run_notice(results, dry_run=dry_run)
            if dry_run:
                flash("Dry run completed without updating the sheet.", "success")
            else:
                flash("Run completed. Check the results table below.", "success")
        except ToolError as error:
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

    @app.get("/api/jobs/<job_id>")
    def get_job(job_id: str):
        snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
        if snapshot is None:
            return jsonify({"status": "error", "message": "Job not found."}), 404
        return jsonify(snapshot)

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


def _current_google_user_is_blocked(settings: Settings) -> bool:
    if not settings.team_allowed_emails and not settings.team_allowed_email_domains:
        return False
    profile = session.get("google_profile") or {}
    email = str(profile.get("email") or "").strip().lower()
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
        or settings.team_allowed_emails
        or settings.team_allowed_email_domains
    )


def _site_requires_google_login(settings: Settings) -> bool:
    return _shared_portal_enabled(settings)


def _google_session_is_connected() -> bool:
    return "google_credentials" in session and bool((session.get("google_profile") or {}).get("email"))


def _can_access_prd_briefing(settings: Settings) -> bool:
    profile = session.get("google_profile") or {}
    email = str(profile.get("email") or "").strip().lower()
    return bool(email and email == settings.prd_briefing_owner_email.strip().lower())


def _can_access_grc_demo(settings: Settings) -> bool:
    profile = session.get("google_profile") or {}
    email = str(profile.get("email") or "").strip().lower()
    return bool(email and email == settings.grc_demo_owner_email.strip().lower())


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


def _validate_config_security(settings: Settings, config_data: dict[str, Any]) -> None:
    portal_token = str(config_data.get("bpmis_api_access_token", "") or "").strip()
    if _shared_portal_enabled(settings) and portal_token and not settings.team_portal_config_encryption_key:
        raise ToolError(
            "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY must be configured on the host before saving BPMIS tokens in shared mode."
        )


def _hydrate_setup_defaults(config_data: dict[str, Any], user_identity: dict[str, str | None]) -> dict[str, Any]:
    hydrated = dict(config_data)
    email = str(user_identity.get("email") or "").strip().lower()
    pm_team = str(hydrated.get("pm_team", "") or "").strip().upper()
    if pm_team:
        hydrated["pm_team"] = pm_team
    profile = TEAM_PROFILE_DEFAULTS.get(pm_team)
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
) -> dict[str, dict[str, Any]]:
    display_email = (
        str(user_identity.get("email") or "").strip().lower()
        or str(config_data.get("sync_pm_email", "") or "").strip().lower()
        or str(config_data.get("product_manager_value", "") or "").strip().lower()
        or str(config_data.get("reporter_value", "") or "").strip().lower()
        or str(config_data.get("biz_pic_value", "") or "").strip().lower()
    )

    profiles: dict[str, dict[str, Any]] = {}
    for team_key, profile in TEAM_PROFILE_DEFAULTS.items():
        rendered_profile = dict(profile)
        if display_email:
            for field in ("component_route_rules_text", "component_default_rules_text"):
                value = str(rendered_profile.get(field, "") or "")
                if TEAM_DEFAULT_EMAIL_PLACEHOLDER in value:
                    rendered_profile[field] = value.replace(TEAM_DEFAULT_EMAIL_PLACEHOLDER, display_email)
        profiles[team_key] = rendered_profile
    return profiles


def _validate_team_profile_setup(config_data: dict[str, Any]) -> None:
    pm_team = str(config_data.get("pm_team", "") or "").strip().upper()
    valid_team_labels = ", ".join(profile["label"] for profile in TEAM_PROFILE_DEFAULTS.values())
    if not pm_team:
        raise ToolError(f"PM Team is required. Choose {valid_team_labels} before saving setup.")
    if pm_team not in TEAM_PROFILE_DEFAULTS:
        raise ToolError(f"Unsupported PM Team: {pm_team}. Choose {valid_team_labels}.")
    profile = TEAM_PROFILE_DEFAULTS[pm_team]
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


def _serialize_results(results: list[object]) -> list[dict[str, Any]]:
    return [result.__dict__ for result in results]


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
        job_store.update(
            job_id,
            state="running",
            stage="starting",
            message="Preparing your request.",
            current=0,
            total=0,
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
            job_store.complete(job_id, results=_serialize_results(results), notice=notice)
        except ToolError as error:
            job_store.fail(job_id, str(error))
        except Exception as error:  # noqa: BLE001
            job_store.fail(job_id, f"Unexpected error: {error}")


def _csv_escape(value: str) -> str:
    text = str(value)
    if any(character in text for character in [",", "\"", "\n"]):
        return "\"" + text.replace("\"", "\"\"") + "\""
    return text


def _resolve_bpmis_access_token(config_data: dict[str, Any], settings: Settings) -> str | None:
    configured_token = str(config_data.get("bpmis_api_access_token", "") or "").strip()
    return configured_token or settings.bpmis_api_access_token


def _get_user_identity(settings: Settings | None = None) -> dict[str, str | None]:
    profile = session.get("google_profile") or {}
    email = str(profile.get("email") or "").strip().lower()
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
