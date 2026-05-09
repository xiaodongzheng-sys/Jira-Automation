"""BPMIS project and Jira workflow route handlers."""
from __future__ import annotations

import logging
from http import HTTPStatus
from types import SimpleNamespace
from typing import Any, Callable

from flask import flash, jsonify, redirect, request, session, url_for

from bpmis_jira_tool.errors import ToolError


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    app.add_url_rule(rule, endpoint=view_func.__name__, view_func=view_func, methods=methods)


def build_bpmis_handlers(ctx: Any) -> Any:
    settings = ctx.settings
    config_store = ctx.config_store
    MARKET_KEYS = ctx.MARKET_KEYS
    _require_google_login = ctx._require_google_login
    _get_user_identity = ctx._get_user_identity
    _load_user_config_for_identity = ctx._load_user_config_for_identity
    _apply_sync_email_policy = ctx._apply_sync_email_policy
    _hydrate_setup_defaults = ctx._hydrate_setup_defaults
    _load_effective_team_profiles = ctx._load_effective_team_profiles
    _validate_config_security = ctx._validate_config_security
    _save_user_config_for_identity = ctx._save_user_config_for_identity
    _log_portal_event = ctx._log_portal_event
    _build_request_log_context = ctx._build_request_log_context
    _build_mapping_log_summary = ctx._build_mapping_log_summary
    _classify_portal_error = ctx._classify_portal_error
    _validate_team_profile_setup = ctx._validate_team_profile_setup
    _is_team_profile_admin = ctx._is_team_profile_admin
    _save_team_profile = ctx._save_team_profile
    _count_configured_lines = ctx._count_configured_lines
    _start_job = ctx._start_job
    _get_bpmis_project_store = ctx._get_bpmis_project_store
    _build_portal_jira_creation_service = ctx._build_portal_jira_creation_service

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


    def create_sync_bpmis_projects_job():
        return _start_job("sync-bpmis-projects")


    def bpmis_projects():
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        user_identity = _get_user_identity(settings)
        store = _get_bpmis_project_store()
        return jsonify({"status": "ok", "projects": store.list_projects(user_key=user_identity["config_key"])})


    def delete_bpmis_project(bpmis_id: str):
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate
        user_identity = _get_user_identity(settings)
        deleted = _get_bpmis_project_store().soft_delete_project(user_key=user_identity["config_key"], bpmis_id=bpmis_id)
        return jsonify({"status": "ok", "deleted": deleted, "scope": "portal_only"})


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

    return SimpleNamespace(
        save_mapping_config=save_mapping_config,
        save_mapping_route_only=save_mapping_route_only,
        save_team_profile_admin=save_team_profile_admin,
        create_sync_bpmis_projects_job=create_sync_bpmis_projects_job,
        bpmis_projects=bpmis_projects,
        delete_bpmis_project=delete_bpmis_project,
        reorder_bpmis_projects=reorder_bpmis_projects,
        update_bpmis_project_comment=update_bpmis_project_comment,
        bpmis_project_jira_options=bpmis_project_jira_options,
        bpmis_project_jira_tickets=bpmis_project_jira_tickets,
        delete_bpmis_project_jira_ticket=delete_bpmis_project_jira_ticket,
        update_bpmis_project_jira_ticket_status=update_bpmis_project_jira_ticket_status,
        update_bpmis_project_jira_ticket_version=update_bpmis_project_jira_ticket_version,
        create_bpmis_project_jira_tickets=create_bpmis_project_jira_tickets,
    )


def register_bpmis_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/config/save", handlers.save_mapping_config, methods=["POST"])
    _add_route(app, "/config/save-route", handlers.save_mapping_route_only, methods=["POST"])
    _add_route(app, "/admin/team-profiles/save", handlers.save_team_profile_admin, methods=["POST"])
    _add_route(app, "/api/jobs/sync-bpmis-projects", handlers.create_sync_bpmis_projects_job, methods=["POST"])
    _add_route(app, "/api/bpmis-projects", handlers.bpmis_projects)
    _add_route(app, "/api/bpmis-projects/<bpmis_id>", handlers.delete_bpmis_project, methods=["DELETE"])
    _add_route(app, "/api/bpmis-projects/order", handlers.reorder_bpmis_projects, methods=["PATCH"])
    _add_route(app, "/api/bpmis-projects/<bpmis_id>/comment", handlers.update_bpmis_project_comment, methods=["PATCH"])
    _add_route(app, "/api/bpmis-projects/<bpmis_id>/jira-options", handlers.bpmis_project_jira_options)
    _add_route(app, "/api/bpmis-projects/<bpmis_id>/jira-tickets", handlers.bpmis_project_jira_tickets)
    _add_route(app, "/api/bpmis-projects/<bpmis_id>/jira-tickets/<ticket_id>", handlers.delete_bpmis_project_jira_ticket, methods=["DELETE"])
    _add_route(app, "/api/bpmis-projects/<bpmis_id>/jira-tickets/<ticket_id>/status", handlers.update_bpmis_project_jira_ticket_status, methods=["PATCH"])
    _add_route(app, "/api/bpmis-projects/<bpmis_id>/jira-tickets/<ticket_id>/version", handlers.update_bpmis_project_jira_ticket_version, methods=["PATCH"])
    _add_route(app, "/api/bpmis-projects/<bpmis_id>/jira-tickets", handlers.create_bpmis_project_jira_tickets, methods=["POST"])
