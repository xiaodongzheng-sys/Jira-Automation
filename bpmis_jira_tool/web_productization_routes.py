"""Productization Upgrade Summary route handlers."""
from __future__ import annotations

from http import HTTPStatus
import logging
import time
from typing import Any, Callable

from flask import current_app, g, jsonify, request

from bpmis_jira_tool.errors import ToolError


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    app.add_url_rule(rule, endpoint=view_func.__name__, view_func=view_func, methods=methods)


def build_productization_handlers(ctx: Any) -> Any:
    settings = ctx.settings
    web_globals = getattr(ctx, "web_globals", {})

    def _web_helper(name: str) -> Any:
        return web_globals.get(name) or getattr(ctx, name)

    _require_google_login = ctx._require_google_login
    _serialize_productization_version_candidate = ctx._serialize_productization_version_candidate
    _load_current_user_config = ctx._load_current_user_config
    _filter_productization_issue_rows_for_pm_team = ctx._filter_productization_issue_rows_for_pm_team
    _normalize_productization_issue_row = ctx._normalize_productization_issue_row
    _classify_portal_error = ctx._classify_portal_error
    _log_portal_event = ctx._log_portal_event
    _build_request_log_context = ctx._build_request_log_context

    def productization_upgrade_summary_versions():
        login_gate = _require_google_login(settings, api=True)
        if login_gate is not None:
            return login_gate

        started_at = time.monotonic()
        query = str(request.args.get("q") or "").strip()
        if not query:
            return jsonify({"status": "error", "message": "Version keyword is required."}), HTTPStatus.BAD_REQUEST

        try:
            bpmis_client = _web_helper("_build_bpmis_client_for_current_user")(settings)
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
            return jsonify({"status": "error", "message": "Unable to search versions right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

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
            bpmis_client = _web_helper("_build_bpmis_client_for_current_user")(settings)
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
            return jsonify({"status": "error", "message": "Unable to load upgrade tickets right now. Please try again shortly."}), HTTPStatus.INTERNAL_SERVER_ERROR

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
            bpmis_client = _web_helper("_build_bpmis_client_for_current_user")(settings)
            rows = bpmis_client.list_issues_for_version(version_id)
            config_data = _load_current_user_config(settings)
            raw_count = len(rows)
            rows, filter_metadata = _filter_productization_issue_rows_for_pm_team(
                rows,
                config_data,
                show_all_before_team_filtering=show_all_before_team_filtering,
            )
            normalized_items = [_normalize_productization_issue_row(item) for item in rows]
            codex_metadata = _web_helper("_apply_codex_productization_detailed_features")(
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

    class _Handlers:
        pass

    handlers = _Handlers()
    handlers.productization_upgrade_summary_versions = productization_upgrade_summary_versions
    handlers.productization_upgrade_summary_issues = productization_upgrade_summary_issues
    handlers.productization_upgrade_summary_llm_descriptions = productization_upgrade_summary_llm_descriptions
    return handlers


def register_productization_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/api/productization-upgrade-summary/versions", handlers.productization_upgrade_summary_versions)
    _add_route(app, "/api/productization-upgrade-summary/issues", handlers.productization_upgrade_summary_issues)
    _add_route(app, "/api/productization-upgrade-summary/llm-descriptions", handlers.productization_upgrade_summary_llm_descriptions)
