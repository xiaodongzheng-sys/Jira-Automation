from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
import re
import secrets
import threading
import time
from typing import Any
import uuid

import requests
from flask import Flask, current_app, flash, jsonify, redirect, render_template, request, session, url_for
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.google_auth import (
    create_google_authorization_url,
    finish_google_oauth,
    get_google_credentials,
)
from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.service import JiraCreationService
from bpmis_jira_tool.bpmis import BPMISHelperClient
from bpmis_jira_tool.user_config import CONFIGURED_FIELDS, WebConfigStore


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
    config_store = WebConfigStore(data_root, legacy_root=project_root)
    app = Flask(
        __name__,
        template_folder=str(project_root / "templates"),
        static_folder=str(project_root / "static"),
    )
    app.config["SECRET_KEY"] = settings.flask_secret_key
    app.config["SETTINGS"] = settings
    app.config["CONFIG_STORE"] = config_store
    app.config["JOB_STORE"] = JobStore()

    @app.before_request
    def enforce_team_access():
        if request.endpoint in {None, "static", "index", "google_login", "google_callback", "google_logout"}:
            return None
        if _current_google_user_is_blocked(settings):
            session.pop("google_credentials", None)
            session.pop("google_profile", None)
            flash("This Google account is not authorized for the team portal. Please contact the maintainer.", "error")
            return redirect(url_for("index"))
        return None

    @app.get("/")
    def index():
        results = session.pop("last_results", [])
        run_notice = session.pop("run_notice", None)
        blocked_google_user = _current_google_user_is_blocked(settings)
        if blocked_google_user:
            session.pop("google_credentials", None)
            session.pop("google_profile", None)
            flash("This Google account is not authorized for the team portal. Please contact the maintainer.", "error")
        user_identity = _get_user_identity()
        config_data = config_store.load(user_identity["config_key"]) or config_store._normalize({})
        input_headers: list[str] = []

        if "google_credentials" in session and not blocked_google_user:
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
            google_connected="google_credentials" in session,
            user_identity=user_identity,
            results=results,
            run_notice=run_notice,
            mapping_fields=CONFIGURED_FIELDS,
            mapping_config=config_data,
            input_headers=input_headers,
            google_authorized=not blocked_google_user,
        )

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
            previous_identity = _get_user_identity()
            finish_google_oauth(settings, request.url)
            if _current_google_user_is_blocked(settings):
                session.pop("google_credentials", None)
                session.pop("google_profile", None)
                flash("This Google account is not authorized for the team portal. Please contact the maintainer.", "error")
                return redirect(url_for("index"))
            current_identity = _get_user_identity()
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
        try:
            user_identity = _get_user_identity()
            config = {
                "spreadsheet_link": request.form.get("spreadsheet_link", ""),
                "input_tab_name": request.form.get("input_tab_name", ""),
                "issue_id_header": request.form.get("issue_id_header", ""),
                "jira_ticket_link_header": request.form.get("jira_ticket_link_header", ""),
                "helper_base_url": request.form.get("helper_base_url", ""),
                "market_header": request.form.get("market_header", ""),
                "summary_header": request.form.get("summary_header", ""),
                "prd_links_header": request.form.get("prd_links_header", ""),
                "task_type_value": request.form.get("task_type_value", ""),
                "fix_version_value": request.form.get("fix_version_value", ""),
                "priority_value": request.form.get("priority_value", ""),
                "assignee_value": request.form.get("assignee_value", ""),
                "product_manager_value": request.form.get("product_manager_value", ""),
                "dev_pic_value": request.form.get("dev_pic_value", ""),
                "qa_pic_value": request.form.get("qa_pic_value", ""),
                "reporter_value": request.form.get("reporter_value", ""),
                "biz_pic_value": request.form.get("biz_pic_value", ""),
                "component_by_market": {
                    market: request.form.get(f"component_{market}", "")
                    for market in MARKET_KEYS
                },
                "need_uat_by_market": {
                    market: request.form.get(f"need_uat_{market}", "")
                    for market in MARKET_KEYS
                },
            }
            config_store.save(config, user_identity["config_key"])
            flash("Your web Jira config was saved for this user and will be used for preview/run.", "success")
        except ToolError as error:
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.post("/preview")
    def preview():
        try:
            service = _build_service(settings)
            results, _headers = service.preview()
            session["last_results"] = [result.__dict__ for result in results]
        except ToolError as error:
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.post("/run")
    def run():
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

    @app.get("/api/self-check")
    def self_check():
        try:
            user_identity = _get_user_identity()
            config_data = config_store.load(user_identity["config_key"]) or config_store._normalize({})
            payload = _run_self_check(settings, config_data)
            return jsonify(payload)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), 400

    @app.post("/api/jobs/preview")
    def create_preview_job():
        return _start_job("preview", dry_run=True)

    @app.post("/api/jobs/run")
    def create_run_job():
        return _start_job("run", dry_run=False)

    @app.get("/api/jobs/<job_id>")
    def get_job(job_id: str):
        snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
        if snapshot is None:
            return jsonify({"status": "error", "message": "Job not found."}), 404
        return jsonify(snapshot)

    def _start_job(action: str, *, dry_run: bool):
        if "google_credentials" not in session:
            return jsonify({"status": "error", "message": "Please connect Google Sheets first."}), 400

        user_identity = _get_user_identity()
        config_data = config_store.load(user_identity["config_key"]) or config_store._normalize({})
        job_store: JobStore = current_app.config["JOB_STORE"]
        title = "Preview Eligible Rows" if dry_run else "Run Ticket Creation"
        job = job_store.create(action, title=title)
        credentials_payload = dict(session.get("google_credentials") or {})

        thread = threading.Thread(
            target=_run_background_job,
            args=(app, job.job_id, settings, config_data, credentials_payload, dry_run),
            daemon=True,
        )
        thread.start()
        return jsonify({"status": "queued", "job_id": job.job_id})

    return app


def _build_service(settings: Settings) -> JiraCreationService:
    user_identity = _get_user_identity()
    config_store = _get_config_store()
    config_data = config_store.load(user_identity["config_key"]) or config_store._normalize({})
    sheets = _build_sheets_service(settings, config_data)
    field_mappings_override = config_store.build_field_mappings(config_data)
    helper_base_url = str(config_data.get("helper_base_url", "")).strip()
    bpmis_client = BPMISHelperClient(helper_base_url) if helper_base_url else None
    return JiraCreationService(
        settings,
        sheets,
        field_mappings_override=field_mappings_override,
        bpmis_client=bpmis_client,
    )


def _build_service_from_config(
    settings: Settings,
    config_data: dict[str, Any],
    credentials_payload: dict[str, Any],
) -> JiraCreationService:
    credentials = Credentials(**credentials_payload)
    sheets = _build_sheets_service_with_credentials(settings, config_data, credentials)
    field_mappings_override = _get_config_store().build_field_mappings(config_data)
    helper_base_url = str(config_data.get("helper_base_url", "")).strip()
    bpmis_client = BPMISHelperClient(helper_base_url) if helper_base_url else None
    return JiraCreationService(
        settings,
        sheets,
        field_mappings_override=field_mappings_override,
        bpmis_client=bpmis_client,
    )


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


def _serialize_results(results: list[object]) -> list[dict[str, Any]]:
    return [result.__dict__ for result in results]


def _run_background_job(
    app: Flask,
    job_id: str,
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


def _run_self_check(settings: Settings, config_data: dict[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    google_connected = "google_credentials" in session
    checks.append(
        {
            "name": "Google Sheets connection",
            "status": "pass" if google_connected else "warn",
            "detail": "Connected and ready." if google_connected else "Connect Google before previewing or running.",
        }
    )

    helper_base_url = str(config_data.get("helper_base_url", "")).strip() or "http://127.0.0.1:8787"
    try:
        response = requests.get(f"{helper_base_url.rstrip('/')}/diagnostics", timeout=8)
        payload = response.json()
        helper_ok = response.ok and payload.get("status") == "ok"
        bpmis_ok = bool(payload.get("checks", {}).get("bpmis_tab", {}).get("ok"))
        checks.append(
            {
                "name": "Local Helper",
                "status": "pass" if helper_ok else "fail",
                "detail": payload.get("message") or ("Helper is online." if helper_ok else "Helper is not responding."),
            }
        )
        checks.append(
            {
                "name": "BPMIS in Chrome",
                "status": "pass" if bpmis_ok else "warn",
                "detail": payload.get("checks", {}).get("bpmis_tab", {}).get("detail")
                or "Open BPMIS in Chrome and make sure you are still logged in.",
            }
        )
    except Exception:  # noqa: BLE001
        checks.append(
            {
                "name": "Local Helper",
                "status": "fail",
                "detail": f"Could not reach helper at {helper_base_url}. Start the helper first.",
            }
        )
        checks.append(
            {
                "name": "BPMIS in Chrome",
                "status": "warn",
                "detail": "Could not check BPMIS because the local helper is offline.",
            }
        )

    spreadsheet_link = str(config_data.get("spreadsheet_link", "")).strip()
    input_tab_name = str(config_data.get("input_tab_name", "")).strip()
    if not google_connected:
        checks.append(
            {
                "name": "Spreadsheet access",
                "status": "warn",
                "detail": "Connect Google first, then run the self-check again.",
            }
        )
    elif not spreadsheet_link or not input_tab_name:
        checks.append(
            {
                "name": "Spreadsheet access",
                "status": "warn",
                "detail": "Fill in Spreadsheet Link and Input Tab Name, then save your config.",
            }
        )
    else:
        try:
            sheets = _build_sheets_service(settings, config_data)
            snapshot = sheets.read_snapshot()
            checks.append(
                {
                    "name": "Spreadsheet access",
                    "status": "pass",
                    "detail": f"Input tab is readable. Found {len(snapshot.rows)} data row(s).",
                }
            )
        except ToolError as error:
            checks.append(
                {
                    "name": "Spreadsheet access",
                    "status": "fail",
                    "detail": str(error),
                }
            )

    overall = "pass"
    if any(check["status"] == "fail" for check in checks):
        overall = "fail"
    elif any(check["status"] == "warn" for check in checks):
        overall = "warn"
    return {"status": overall, "checks": checks}


def _get_user_identity() -> dict[str, str | None]:
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
