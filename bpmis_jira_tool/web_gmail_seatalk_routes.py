"""Gmail and SeaTalk demo route handlers."""
from __future__ import annotations

from http import HTTPStatus
import io
import logging
from typing import Any, Callable

from flask import current_app, jsonify, redirect, request, send_file, url_for

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService
from bpmis_jira_tool.seatalk_stores import SeaTalkNameMappingStore, SeaTalkTodoStore


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    app.add_url_rule(rule, endpoint=view_func.__name__, view_func=view_func, methods=methods)


def build_gmail_seatalk_handlers(ctx: Any) -> Any:
    settings = ctx.settings
    web_globals = getattr(ctx, "web_globals", {})

    def _web_helper(name: str) -> Any:
        return web_globals.get(name) or getattr(ctx, name)

    GMAIL_READONLY_SCOPE = ctx.GMAIL_READONLY_SCOPE
    _require_gmail_seatalk_demo_access = ctx._require_gmail_seatalk_demo_access
    _google_credentials_have_scopes = ctx._google_credentials_have_scopes
    _classify_portal_error = ctx._classify_portal_error
    _log_portal_event = ctx._log_portal_event
    _build_request_log_context = ctx._build_request_log_context
    _get_user_identity = ctx._get_user_identity
    _safe_email_identity = ctx._safe_email_identity
    _try_acquire_gmail_export_lock = ctx._try_acquire_gmail_export_lock
    _release_gmail_export_lock = ctx._release_gmail_export_lock
    _current_google_email = ctx._current_google_email
    _get_seatalk_todo_store = ctx._get_seatalk_todo_store
    _get_seatalk_name_mapping_store = ctx._get_seatalk_name_mapping_store
    _callable_accepts_keyword = ctx._callable_accepts_keyword
    _dedupe_seatalk_name_mapping_candidates = ctx._dedupe_seatalk_name_mapping_candidates

    def _gmail_scope_error():
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Gmail access is not available for this Google session yet. Please sign in with Google again to grant Gmail read access.",
                }
            ),
            HTTPStatus.BAD_REQUEST,
        )

    def gmail_seatalk_demo():
        access_gate = _require_gmail_seatalk_demo_access(settings)
        if access_gate is not None:
            return access_gate
        return redirect(url_for("reports_page", tab="seatalk-name-mapping"))

    def gmail_seatalk_demo_dashboard_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return _gmail_scope_error()
        try:
            dashboard = _web_helper("_build_gmail_dashboard_service")().build_overview()
            return jsonify({"status": "ok", **dashboard})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_dashboard_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
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

    def gmail_seatalk_demo_network_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return _gmail_scope_error()
        try:
            network = _web_helper("_build_gmail_dashboard_service")().build_network()
            return jsonify({"status": "ok", **network})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_network_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
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

    def gmail_seatalk_demo_gmail_export_manifest():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return _gmail_scope_error()
        try:
            manifest = _web_helper("_build_gmail_dashboard_service")().build_export_manifest()
            return jsonify({"status": "ok", **manifest})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_gmail_export_manifest_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_gmail_export_manifest_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Gmail export manifest failed.")
            return jsonify({"status": "error", "message": "Gmail export batches could not be prepared right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def gmail_seatalk_demo_gmail_export():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return _gmail_scope_error()
        try:
            batch = max(int(request.args.get("batch", "1")), 1)
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid Gmail export batch. Please refresh and try again."}), HTTPStatus.BAD_REQUEST
        user_email = _safe_email_identity(_get_user_identity(settings))
        service = _web_helper("_build_gmail_dashboard_service")()
        if not _try_acquire_gmail_export_lock(user_email):
            cached_payload = service.get_cached_export_history_text(batch=batch)
            if cached_payload is not None:
                content, filename = cached_payload
                return send_file(io.BytesIO(content.encode("utf-8")), mimetype="text/plain; charset=utf-8", as_attachment=True, download_name=filename)
            return jsonify({"status": "error", "message": "A Gmail export is already running for this account. Please wait a few seconds and try again."}), HTTPStatus.TOO_MANY_REQUESTS
        try:
            content, filename = service.export_history_text(batch=batch)
            return send_file(io.BytesIO(content.encode("utf-8")), mimetype="text/plain; charset=utf-8", as_attachment=True, download_name=filename)
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_gmail_export_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_gmail_export_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Gmail history export failed.")
            return jsonify({"status": "error", "message": "Gmail mail history could not be exported right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR
        finally:
            _release_gmail_export_lock(user_email)

    def gmail_seatalk_demo_gmail_export_prewarm():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(GMAIL_READONLY_SCOPE):
            return _gmail_scope_error()
        try:
            batch = max(int(request.args.get("batch", "1")), 1)
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid Gmail export batch. Please refresh and try again."}), HTTPStatus.BAD_REQUEST
        user_email = _safe_email_identity(_get_user_identity(settings))
        service = _web_helper("_build_gmail_dashboard_service")()
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
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_gmail_export_prewarm_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("Gmail export prewarm failed.")
            return jsonify({"status": "error", "message": "Gmail export prewarm could not be completed right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR
        finally:
            _release_gmail_export_lock(user_email)

    def gmail_seatalk_demo_seatalk_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            payload = _web_helper("_build_seatalk_dashboard_service")(settings).build_overview()
            return jsonify({"status": "ok", **payload})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk dashboard load failed.")
            return jsonify({"status": "error", "message": "SeaTalk data could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def gmail_seatalk_demo_seatalk_insights_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _current_google_email() or settings.gmail_seatalk_demo_owner_email
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
                "gmail_seatalk_seatalk_insights_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_insights_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk insights load failed.")
            return jsonify({"status": "error", "message": "SeaTalk insights could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def gmail_seatalk_demo_seatalk_project_updates_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
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
                "gmail_seatalk_seatalk_project_updates_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_project_updates_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk project updates load failed.")
            return jsonify({"status": "error", "message": "SeaTalk project updates could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def gmail_seatalk_demo_seatalk_open_todos_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _current_google_email() or settings.gmail_seatalk_demo_owner_email
            todo_store = _get_seatalk_todo_store(settings)
            completed_ids = todo_store.completed_ids(owner_email=owner_email)
            open_todos = [todo for todo in SeaTalkDashboardService._sort_todos(todo_store.open_todos(owner_email=owner_email)) if SeaTalkTodoStore.todo_id(todo) not in completed_ids]
            return jsonify({"status": "ok", "my_todos": open_todos, "team_todos": [], "project_updates": []})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_open_todos_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_open_todos_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk open to-dos load failed.")
            return jsonify({"status": "error", "message": "Saved SeaTalk to-dos could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def gmail_seatalk_demo_seatalk_todos_api():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            owner_email = _current_google_email() or settings.gmail_seatalk_demo_owner_email
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
                "gmail_seatalk_seatalk_todos_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_todos_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk to-do load failed.")
            return jsonify({"status": "error", "message": "SeaTalk to-dos could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def gmail_seatalk_demo_seatalk_todo_complete():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        todo = payload.get("todo") if isinstance(payload.get("todo"), dict) else payload
        try:
            todo_store = _get_seatalk_todo_store(settings)
            result = todo_store.mark_completed(owner_email=_current_google_email() or settings.gmail_seatalk_demo_owner_email, todo=todo)
            return jsonify(result)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

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
            candidates = _web_helper("_build_seatalk_dashboard_service")(settings).build_name_mappings(force_refresh=force_refresh)
            mappings = mapping_store.mappings()
            mapped_keys = {alias for key in mappings for alias in SeaTalkNameMappingStore.equivalent_keys(key)}
            candidates = dict(candidates)
            candidates["unknown_ids"] = _dedupe_seatalk_name_mapping_candidates(
                [
                    row for row in (candidates.get("unknown_ids") or [])
                    if isinstance(row, dict) and not (SeaTalkNameMappingStore.equivalent_keys(row.get("id")) & mapped_keys)
                ]
            )
            visible_keys = {
                alias
                for row in (candidates.get("unknown_ids") or [])
                if isinstance(row, dict)
                for alias in SeaTalkNameMappingStore.equivalent_keys(row.get("id"))
            }
            visible_mappings = {key: value for key, value in mappings.items() if key in visible_keys}
            return jsonify({"status": "ok", "mappings": visible_mappings, **candidates})
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_name_mappings_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_name_mappings_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk name mapping load failed.")
            return jsonify({"status": "error", "message": "SeaTalk name mappings could not be loaded right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    def gmail_seatalk_demo_seatalk_export():
        access_gate = _require_gmail_seatalk_demo_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            content, filename = _web_helper("_build_seatalk_dashboard_service")(settings).export_history_text()
            return send_file(io.BytesIO(content.encode("utf-8")), mimetype="text/plain; charset=utf-8", as_attachment=True, download_name=filename)
        except ToolError as error:
            error_details = _classify_portal_error(error)
            _log_portal_event(
                "gmail_seatalk_seatalk_export_tool_error",
                level=logging.WARNING,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=error_details),
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception:
            _log_portal_event(
                "gmail_seatalk_seatalk_export_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings)),
            )
            current_app.logger.exception("SeaTalk history export failed.")
            return jsonify({"status": "error", "message": "SeaTalk chat history could not be exported right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

    class _Handlers:
        pass

    handlers = _Handlers()
    for name, value in locals().copy().items():
        if name.startswith("gmail_seatalk_demo"):
            setattr(handlers, name, value)
    return handlers


def register_gmail_seatalk_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/gmail-sea-talk-demo", handlers.gmail_seatalk_demo)
    _add_route(app, "/api/gmail-sea-talk-demo/dashboard", handlers.gmail_seatalk_demo_dashboard_api)
    _add_route(app, "/api/gmail-sea-talk-demo/network", handlers.gmail_seatalk_demo_network_api)
    _add_route(app, "/api/gmail-sea-talk-demo/gmail/export-manifest", handlers.gmail_seatalk_demo_gmail_export_manifest)
    _add_route(app, "/api/gmail-sea-talk-demo/gmail/export", handlers.gmail_seatalk_demo_gmail_export)
    _add_route(app, "/api/gmail-sea-talk-demo/gmail/export-prewarm", handlers.gmail_seatalk_demo_gmail_export_prewarm, methods=["POST"])
    _add_route(app, "/api/gmail-sea-talk-demo/seatalk", handlers.gmail_seatalk_demo_seatalk_api)
    _add_route(app, "/api/gmail-sea-talk-demo/seatalk/insights", handlers.gmail_seatalk_demo_seatalk_insights_api)
    _add_route(app, "/api/gmail-sea-talk-demo/seatalk/project-updates", handlers.gmail_seatalk_demo_seatalk_project_updates_api)
    _add_route(app, "/api/gmail-sea-talk-demo/seatalk/todos/open", handlers.gmail_seatalk_demo_seatalk_open_todos_api)
    _add_route(app, "/api/gmail-sea-talk-demo/seatalk/todos", handlers.gmail_seatalk_demo_seatalk_todos_api)
    _add_route(app, "/api/gmail-sea-talk-demo/seatalk/todos/complete", handlers.gmail_seatalk_demo_seatalk_todo_complete, methods=["POST"])
    _add_route(app, "/api/gmail-sea-talk-demo/seatalk/name-mappings", handlers.gmail_seatalk_demo_seatalk_name_mappings, methods=["GET", "POST"])
    _add_route(app, "/api/gmail-sea-talk-demo/seatalk/export", handlers.gmail_seatalk_demo_seatalk_export)
