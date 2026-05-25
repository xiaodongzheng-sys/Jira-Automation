"""SeaTalk route handlers used by Team Dashboard."""
from __future__ import annotations

from http import HTTPStatus
import logging
from typing import Any, Callable

from flask import current_app, jsonify, request

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService
from bpmis_jira_tool.seatalk_stores import SeaTalkTodoStore


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    app.add_url_rule(rule, endpoint=view_func.__name__, view_func=view_func, methods=methods)


def build_team_dashboard_seatalk_handlers(ctx: Any) -> Any:
    settings = ctx.settings
    web_globals = getattr(ctx, "web_globals", {})

    def _web_helper(name: str) -> Any:
        return web_globals.get(name) or getattr(ctx, name)

    _require_seatalk_management_access = ctx._require_seatalk_management_access
    _classify_portal_error = ctx._classify_portal_error
    _log_portal_event = ctx._log_portal_event
    _build_request_log_context = ctx._build_request_log_context
    _get_user_identity = ctx._get_user_identity
    _current_google_email = ctx._current_google_email
    _get_seatalk_todo_store = ctx._get_seatalk_todo_store
    _callable_accepts_keyword = ctx._callable_accepts_keyword

    def _owner_email() -> str:
        return _current_google_email() or settings.gmail_seatalk_demo_owner_email

    def team_dashboard_seatalk_insights_api():
        access_gate = _require_seatalk_management_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _owner_email()
            todo_store = _get_seatalk_todo_store(settings)
            service = _web_helper("_build_seatalk_dashboard_service")(settings)
            todo_since = todo_store.processed_until(owner_email=owner_email)
            payload = service.build_insights(todo_since=todo_since) if _callable_accepts_keyword(service.build_insights, "todo_since") else service.build_insights()
            completed_ids = todo_store.completed_ids(owner_email=owner_email)
            payload = dict(payload)
            open_todos = todo_store.merge_open_todos(owner_email=owner_email, todos=[todo for todo in (payload.get("my_todos") or []) if isinstance(todo, dict)])
            todo_store.mark_processed_until(owner_email=owner_email, processed_until=str(payload.get("todo_processed_until") or ""))
            payload["my_todos"] = [todo for todo in SeaTalkDashboardService._sort_todos(open_todos) if SeaTalkTodoStore.todo_id(todo) not in completed_ids]
            payload["team_todos"] = []
            return jsonify({"status": "ok", **payload})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_seatalk_insights_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "team_dashboard_seatalk_insights_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Team Dashboard SeaTalk insights load failed.")
            return jsonify({"status": "error", "message": "SeaTalk insights could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def team_dashboard_seatalk_project_updates_api():
        access_gate = _require_seatalk_management_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            service = _web_helper("_build_seatalk_dashboard_service")(settings)
            payload = service.build_project_updates() if hasattr(service, "build_project_updates") else service.build_insights()
            payload = dict(payload)
            payload["my_todos"] = []
            payload["team_todos"] = []
            return jsonify({"status": "ok", **payload})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_seatalk_project_updates_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "team_dashboard_seatalk_project_updates_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Team Dashboard SeaTalk project updates load failed.")
            return jsonify({"status": "error", "message": "SeaTalk project updates could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def team_dashboard_seatalk_open_todos_api():
        access_gate = _require_seatalk_management_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _owner_email()
            todo_store = _get_seatalk_todo_store(settings)
            completed_ids = todo_store.completed_ids(owner_email=owner_email)
            open_todos = [todo for todo in SeaTalkDashboardService._sort_todos(todo_store.open_todos(owner_email=owner_email)) if SeaTalkTodoStore.todo_id(todo) not in completed_ids]
            return jsonify({"status": "ok", "my_todos": open_todos, "team_todos": [], "project_updates": []})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_seatalk_open_todos_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "team_dashboard_seatalk_open_todos_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Team Dashboard SeaTalk open to-dos load failed.")
            return jsonify({"status": "error", "message": "Saved SeaTalk to-dos could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def team_dashboard_seatalk_todos_api():
        access_gate = _require_seatalk_management_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _owner_email()
            todo_store = _get_seatalk_todo_store(settings)
            service = _web_helper("_build_seatalk_dashboard_service")(settings)
            todo_since = todo_store.processed_until(owner_email=owner_email)
            if hasattr(service, "build_todos") and _callable_accepts_keyword(service.build_todos, "todo_since"):
                payload = service.build_todos(todo_since=todo_since)
            elif _callable_accepts_keyword(service.build_insights, "todo_since"):
                payload = service.build_insights(todo_since=todo_since)
            else:
                payload = service.build_insights()
            completed_ids = todo_store.completed_ids(owner_email=owner_email)
            payload = dict(payload)
            open_todos = todo_store.merge_open_todos(owner_email=owner_email, todos=[todo for todo in (payload.get("my_todos") or []) if isinstance(todo, dict)])
            todo_store.mark_processed_until(owner_email=owner_email, processed_until=str(payload.get("todo_processed_until") or ""))
            payload["project_updates"] = []
            payload["my_todos"] = [todo for todo in SeaTalkDashboardService._sort_todos(open_todos) if SeaTalkTodoStore.todo_id(todo) not in completed_ids]
            payload["team_todos"] = []
            return jsonify({"status": "ok", **payload})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "team_dashboard_seatalk_todos_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "team_dashboard_seatalk_todos_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Team Dashboard SeaTalk to-do load failed.")
            return jsonify({"status": "error", "message": "SeaTalk to-dos could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def team_dashboard_seatalk_todo_complete():
        access_gate = _require_seatalk_management_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        todo = payload.get("todo") if isinstance(payload.get("todo"), dict) else payload
        try:
            todo_store = _get_seatalk_todo_store(settings)
            result = todo_store.mark_completed(owner_email=_owner_email(), todo=todo)
            return jsonify(result)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    class _Handlers:
        pass

    handlers = _Handlers()
    for name, value in locals().copy().items():
        if name.startswith("team_dashboard_seatalk"):
            setattr(handlers, name, value)
    return handlers


def register_team_dashboard_seatalk_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/api/team-dashboard/seatalk/insights", handlers.team_dashboard_seatalk_insights_api)
    _add_route(app, "/api/team-dashboard/seatalk/project-updates", handlers.team_dashboard_seatalk_project_updates_api)
    _add_route(app, "/api/team-dashboard/seatalk/todos/open", handlers.team_dashboard_seatalk_open_todos_api)
    _add_route(app, "/api/team-dashboard/seatalk/todos", handlers.team_dashboard_seatalk_todos_api)
    _add_route(app, "/api/team-dashboard/seatalk/todos/complete", handlers.team_dashboard_seatalk_todo_complete, methods=["POST"])
