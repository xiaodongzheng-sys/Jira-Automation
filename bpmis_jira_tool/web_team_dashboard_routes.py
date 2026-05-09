"""Team Dashboard route handlers."""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import io
import logging
import re
import threading
import time
from http import HTTPStatus
from types import SimpleNamespace
from typing import Any, Callable

from flask import current_app, jsonify, render_template, request, send_file, url_for

from bpmis_jira_tool.daily_brief_archive import daily_brief_pdf_bytes
from bpmis_jira_tool.errors import BPMISError, ConfigError, ToolError
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE
from bpmis_jira_tool.monthly_report import (
    DEFAULT_MONTHLY_REPORT_RECIPIENT,
    DEFAULT_MONTHLY_REPORT_TEMPLATE,
    MONTHLY_REPORT_PRODUCT_SCOPE,
    monthly_report_subject,
    normalize_monthly_report_highlight_topics,
    normalize_monthly_report_template,
    resolve_monthly_report_period_from_user_range,
    resolve_monthly_report_period,
    send_monthly_report_email,
)
from bpmis_jira_tool.report_intelligence import normalize_report_intelligence_config
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService
from bpmis_jira_tool.seatalk_stores import SeaTalkNameMappingStore
from bpmis_jira_tool.team_dashboard_config import TEAM_DASHBOARD_TEAMS
from prd_briefing.reviewer import PRDReviewRequest


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    app.add_url_rule(rule, endpoint=view_func.__name__, view_func=view_func, methods=methods)


def build_team_dashboard_handlers(ctx: Any) -> Any:
    settings = ctx.settings
    _require_team_dashboard_access = ctx._require_team_dashboard_access
    _require_team_dashboard_monthly_report_access = ctx._require_team_dashboard_monthly_report_access
    _get_user_identity = ctx._get_user_identity
    _get_team_dashboard_config_store = ctx._get_team_dashboard_config_store
    _can_manage_team_dashboard = ctx._can_manage_team_dashboard
    _can_access_team_dashboard_monthly_report = ctx._can_access_team_dashboard_monthly_report
    _seatalk_dashboard_is_configured = ctx._seatalk_dashboard_is_configured
    _log_portal_event = ctx._log_portal_event
    _build_request_log_context = ctx._build_request_log_context
    _local_agent_seatalk_enabled = ctx._local_agent_seatalk_enabled
    _build_local_agent_client = ctx._build_local_agent_client
    _get_daily_brief_archive_store = ctx._get_daily_brief_archive_store
    _get_seatalk_name_mapping_store = ctx._get_seatalk_name_mapping_store
    _build_seatalk_dashboard_service = ctx._build_seatalk_dashboard_service
    _dedupe_seatalk_name_mapping_candidates = ctx._dedupe_seatalk_name_mapping_candidates
    _classify_portal_error = ctx._classify_portal_error
    _load_team_dashboard_tasks_for_all_teams_merged = ctx._load_team_dashboard_tasks_for_all_teams_merged
    _record_team_dashboard_work_memory = ctx._record_team_dashboard_work_memory
    _current_google_email = ctx._current_google_email
    _team_dashboard_new_timing = ctx._team_dashboard_new_timing
    _team_dashboard_add_timing = ctx._team_dashboard_add_timing
    _normalize_team_dashboard_emails = ctx._normalize_team_dashboard_emails
    _cached_team_dashboard_task_payload = ctx._cached_team_dashboard_task_payload
    _build_bpmis_client_for_current_user = ctx._build_bpmis_client_for_current_user
    _team_dashboard_load_jira_and_biz_projects = ctx._team_dashboard_load_jira_and_biz_projects
    _build_team_dashboard_task_group = ctx._build_team_dashboard_task_group
    _backfill_team_dashboard_empty_project_jira_tasks = ctx._backfill_team_dashboard_empty_project_jira_tasks
    _remove_team_dashboard_zero_jira_pending_live_projects = ctx._remove_team_dashboard_zero_jira_pending_live_projects
    _team_dashboard_combined_request_timings = ctx._team_dashboard_combined_request_timings
    _team_dashboard_combined_fetch_stats = ctx._team_dashboard_combined_fetch_stats
    _store_team_dashboard_task_payload = ctx._store_team_dashboard_task_payload
    _apply_team_dashboard_key_project_state = ctx._apply_team_dashboard_key_project_state
    _load_team_dashboard_link_biz_jira_rows = ctx._load_team_dashboard_link_biz_jira_rows
    _suggest_team_dashboard_link_biz_project_rows = ctx._suggest_team_dashboard_link_biz_project_rows
    _extract_issue_key_from_text = ctx._extract_issue_key_from_text
    _team_dashboard_link_biz_candidate_projects_by_pm = ctx._team_dashboard_link_biz_candidate_projects_by_pm
    _extract_parent_issue_ids_from_any = ctx._extract_parent_issue_ids_from_any
    _normalize_team_dashboard_project = ctx._normalize_team_dashboard_project
    _jira_browse_base_url = ctx._jira_browse_base_url
    _load_all_team_dashboard_task_payloads = ctx._load_all_team_dashboard_task_payloads
    _remote_bpmis_config_enabled = ctx._remote_bpmis_config_enabled
    _run_team_dashboard_monthly_report_draft_job = ctx._run_team_dashboard_monthly_report_draft_job
    _google_credentials_have_scopes = ctx._google_credentials_have_scopes
    _ingest_sent_monthly_reports_from_gmail = ctx._ingest_sent_monthly_reports_from_gmail
    _local_agent_source_code_qa_enabled = ctx._local_agent_source_code_qa_enabled
    _build_prd_review_service = ctx._build_prd_review_service
    resolve_monthly_report_period = ctx.resolve_monthly_report_period
    send_monthly_report_email = ctx.send_monthly_report_email

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


    def reports_page():
        access_gate = _require_team_dashboard_access(settings)
        if access_gate is not None:
            return access_gate
        return render_template(
            "reports.html",
            page_title="Reports",
            user_identity=_get_user_identity(settings),
            team_dashboard_config=_get_team_dashboard_config_store().load(),
            can_manage_team_dashboard=_can_manage_team_dashboard(_get_user_identity(settings)),
            can_view_team_dashboard_monthly_report=_can_access_team_dashboard_monthly_report(
                _get_user_identity(settings)
            ),
            seatalk_configured=_seatalk_dashboard_is_configured(settings),
        )


    def team_dashboard_config():
        access_gate = _require_team_dashboard_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        return jsonify({"status": "ok", "config": _get_team_dashboard_config_store().load()})


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


    def team_dashboard_monthly_report_template():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        config = _get_team_dashboard_config_store().load()
        report_period = resolve_monthly_report_period()
        return jsonify(
            {
                "status": "ok",
                "template": normalize_monthly_report_template(config.get("monthly_report_template")),
                "subject": monthly_report_subject(),
                "recipient": DEFAULT_MONTHLY_REPORT_RECIPIENT,
                "period_start": report_period.start_date,
                "period_end": report_period.end_date,
            }
        )


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
        highlight_topics = (result or {}).get("highlight_topics") or generation_summary.get("highlight_topics") or []
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
                "highlight_topics": highlight_topics if isinstance(highlight_topics, list) else [],
                "generation_summary": generation_summary,
                "evidence_summary": (result or {}).get("evidence_summary") if isinstance((result or {}).get("evidence_summary"), dict) else {},
                "highlight_evidence_map": (result or {}).get("highlight_evidence_map") if isinstance((result or {}).get("highlight_evidence_map"), list) else [],
                "highlight_narratives": (result or {}).get("highlight_narratives") if isinstance((result or {}).get("highlight_narratives"), list) else [],
                "generation_diagnostics": (result or {}).get("generation_diagnostics") if isinstance((result or {}).get("generation_diagnostics"), dict) else {},
            }
        )


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


    def team_dashboard_report_intelligence_seatalk_name_mappings():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        mapping_store = _get_seatalk_name_mapping_store(settings)
        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            mappings = mapping_store.replace_mappings(payload.get("mappings") or {})
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
            visible_keys = {
                alias
                for row in (candidates.get("unknown_ids") or [])
                if isinstance(row, dict)
                for alias in SeaTalkNameMappingStore.equivalent_keys(row.get("id"))
            }
            visible_mappings = {key: value for key, value in mappings.items() if key in visible_keys}
            return jsonify({"status": "ok", "mappings": visible_mappings, **candidates})
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


    def team_dashboard_monthly_report_draft():
        access_gate = _require_team_dashboard_monthly_report_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        try:
            payload = request.get_json(silent=True) or {}
            highlight_topics = normalize_monthly_report_highlight_topics(payload.get("highlight_topics"))
            report_period = resolve_monthly_report_period_from_user_range(
                period_start=str(payload.get("period_start") or "") or None,
                period_end=str(payload.get("period_end") or "") or None,
            )
            config = _get_team_dashboard_config_store().load()
            team_payloads = _load_all_team_dashboard_task_payloads(settings, config)
            request_payload = {
                "template": normalize_monthly_report_template(config.get("monthly_report_template")),
                "team_payloads": team_payloads,
                "report_intelligence_config": normalize_report_intelligence_config(config.get("report_intelligence_config")),
                "period_start": report_period.start.isoformat(),
                "period_end": report_period.end_date,
                "period_end_exclusive": report_period.end_exclusive.isoformat(),
                "highlight_topics": highlight_topics,
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

    return SimpleNamespace(
        team_dashboard_page=team_dashboard_page,
        reports_page=reports_page,
        team_dashboard_config=team_dashboard_config,
        save_team_dashboard_members=save_team_dashboard_members,
        team_dashboard_monthly_report_template=team_dashboard_monthly_report_template,
        team_dashboard_monthly_report_latest_draft=team_dashboard_monthly_report_latest_draft,
        team_dashboard_daily_briefs=team_dashboard_daily_briefs,
        team_dashboard_daily_brief_download=team_dashboard_daily_brief_download,
        save_team_dashboard_monthly_report_template=save_team_dashboard_monthly_report_template,
        save_team_dashboard_report_intelligence=save_team_dashboard_report_intelligence,
        team_dashboard_report_intelligence_seatalk_name_mappings=team_dashboard_report_intelligence_seatalk_name_mappings,
        team_dashboard_tasks=team_dashboard_tasks,
        save_team_dashboard_key_project=save_team_dashboard_key_project,
        team_dashboard_link_biz_projects=team_dashboard_link_biz_projects,
        team_dashboard_link_biz_project_jira=team_dashboard_link_biz_project_jira,
        team_dashboard_link_biz_project_suggestions=team_dashboard_link_biz_project_suggestions,
        link_team_dashboard_biz_project=link_team_dashboard_biz_project,
        team_dashboard_monthly_report_draft=team_dashboard_monthly_report_draft,
        team_dashboard_monthly_report_send=team_dashboard_monthly_report_send,
        team_dashboard_prd_review=team_dashboard_prd_review,
        team_dashboard_prd_summary=team_dashboard_prd_summary,
    )


def register_team_dashboard_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/team-dashboard", handlers.team_dashboard_page)
    _add_route(app, "/reports", handlers.reports_page)
    _add_route(app, "/admin/team-dashboard/members", handlers.save_team_dashboard_members, methods=["POST"])
    _add_route(app, "/api/team-dashboard/config", handlers.team_dashboard_config)
    _add_route(app, "/api/team-dashboard/monthly-report/template", handlers.team_dashboard_monthly_report_template)
    _add_route(app, "/api/team-dashboard/monthly-report/latest-draft", handlers.team_dashboard_monthly_report_latest_draft)
    _add_route(app, "/api/team-dashboard/daily-briefs", handlers.team_dashboard_daily_briefs)
    _add_route(app, "/api/team-dashboard/daily-briefs/<brief_id>/download", handlers.team_dashboard_daily_brief_download)
    _add_route(app, "/admin/team-dashboard/monthly-report-template", handlers.save_team_dashboard_monthly_report_template, methods=["POST"])
    _add_route(app, "/admin/team-dashboard/report-intelligence", handlers.save_team_dashboard_report_intelligence, methods=["POST"])
    _add_route(app, "/api/team-dashboard/report-intelligence/seatalk/name-mappings", handlers.team_dashboard_report_intelligence_seatalk_name_mappings, methods=["GET", "POST"])
    _add_route(app, "/api/team-dashboard/tasks", handlers.team_dashboard_tasks)
    _add_route(app, "/api/team-dashboard/key-projects", handlers.save_team_dashboard_key_project, methods=["POST"])
    _add_route(app, "/api/team-dashboard/link-biz-projects", handlers.team_dashboard_link_biz_projects)
    _add_route(app, "/api/team-dashboard/link-biz-projects/jira", handlers.team_dashboard_link_biz_project_jira)
    _add_route(app, "/api/team-dashboard/link-biz-projects/suggestions", handlers.team_dashboard_link_biz_project_suggestions, methods=["POST"])
    _add_route(app, "/api/team-dashboard/link-biz-projects", handlers.link_team_dashboard_biz_project, methods=["POST"])
    _add_route(app, "/api/team-dashboard/monthly-report/draft", handlers.team_dashboard_monthly_report_draft, methods=["POST"])
    _add_route(app, "/api/team-dashboard/monthly-report/send", handlers.team_dashboard_monthly_report_send, methods=["POST"])
    _add_route(app, "/api/team-dashboard/prd-review", handlers.team_dashboard_prd_review, methods=["POST"])
    _add_route(app, "/api/team-dashboard/prd-summary", handlers.team_dashboard_prd_summary, methods=["POST"])
