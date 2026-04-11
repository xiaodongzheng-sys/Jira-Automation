from __future__ import annotations

from pathlib import Path
import re

from flask import Flask, flash, redirect, render_template, request, session, url_for
from dotenv import load_dotenv

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.google_auth import (
    create_google_authorization_url,
    finish_google_oauth,
    get_google_credentials,
)
from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.service import JiraCreationService
from bpmis_jira_tool.user_config import CONFIGURED_FIELDS, WebConfigStore


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
CONFIG_STORE = WebConfigStore(PROJECT_ROOT)
MARKET_KEYS = ["ID", "SG", "PH", "Regional"]


def create_app() -> Flask:
    settings = Settings.from_env()
    package_dir = Path(__file__).resolve().parent
    project_root = package_dir.parent
    app = Flask(
        __name__,
        template_folder=str(project_root / "templates"),
        static_folder=str(project_root / "static"),
    )
    app.config["SECRET_KEY"] = settings.flask_secret_key
    app.config["SETTINGS"] = settings

    @app.get("/")
    def index():
        results = session.pop("last_results", None)
        run_notice = session.pop("run_notice", None)
        config_data = CONFIG_STORE.load() or CONFIG_STORE._normalize({})
        input_headers: list[str] = []

        if "google_credentials" in session:
            try:
                sheets = _build_sheets_service(settings, config_data)
                snapshot = sheets.read_snapshot()
                input_headers = snapshot.headers
            except ToolError as error:
                flash(str(error), "error")

        return render_template(
            "index.html",
            settings=settings,
            google_connected="google_credentials" in session,
            results=results,
            run_notice=run_notice,
            mapping_fields=CONFIGURED_FIELDS,
            mapping_config=config_data,
            input_headers=input_headers,
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
            finish_google_oauth(settings, request.url)
            flash("Google Sheets connected successfully.", "success")
        except ToolError as error:
            flash(str(error), "error")
        return redirect(url_for("index"))

    @app.post("/auth/google/logout")
    def google_logout():
        session.pop("google_credentials", None)
        flash("Google session cleared.", "success")
        return redirect(url_for("index"))

    @app.post("/config/save")
    def save_mapping_config():
        try:
            config = {
                "spreadsheet_link": request.form.get("spreadsheet_link", ""),
                "input_tab_name": request.form.get("input_tab_name", ""),
                "issue_id_header": request.form.get("issue_id_header", ""),
                "jira_ticket_link_header": request.form.get("jira_ticket_link_header", ""),
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
            CONFIG_STORE.save(config)
            flash("Web Jira config saved and will be used for preview/run.", "success")
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

    return app


def _build_service(settings: Settings) -> JiraCreationService:
    config_data = CONFIG_STORE.load() or CONFIG_STORE._normalize({})
    sheets = _build_sheets_service(settings, config_data)
    field_mappings_override = CONFIG_STORE.build_field_mappings(config_data)
    return JiraCreationService(settings, sheets, field_mappings_override=field_mappings_override)


def _build_sheets_service(settings: Settings, config_data: dict[str, object] | None = None) -> GoogleSheetsService:
    credentials = get_google_credentials()
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
