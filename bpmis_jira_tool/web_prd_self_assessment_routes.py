"""PRD Self-Assessment route handlers."""
from __future__ import annotations

from http import HTTPStatus
from typing import Any, Callable

from flask import current_app, jsonify, render_template, url_for

from bpmis_jira_tool.errors import ToolError


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    app.add_url_rule(rule, endpoint=view_func.__name__, view_func=view_func, methods=methods)


def build_prd_self_assessment_handlers(ctx: Any) -> Any:
    settings = ctx.settings
    web_globals = getattr(ctx, "web_globals", {})

    def _web_helper(name: str) -> Any:
        return web_globals.get(name) or getattr(ctx, name)

    _require_prd_self_assessment_access = ctx._require_prd_self_assessment_access
    _get_user_identity = ctx._get_user_identity
    _current_release_revision = ctx._current_release_revision
    _run_prd_self_assessment_action = ctx._run_prd_self_assessment_action
    _run_prd_self_assessment_sections = ctx._run_prd_self_assessment_sections

    def prd_self_assessment_page():
        access_gate = _require_prd_self_assessment_access(settings)
        if access_gate is not None:
            return access_gate
        return render_template(
            "prd_self_assessment.html",
            page_title="PRD Self-Assessment",
            user_identity=_get_user_identity(settings),
            review_url=url_for("prd_self_assessment_review_api"),
            summary_url=url_for("prd_self_assessment_summary_api"),
            asset_revision=_current_release_revision(),
        )

    def prd_self_assessment_review_api():
        return _run_prd_self_assessment_action(settings, action="review")

    def prd_self_assessment_summary_api():
        return _run_prd_self_assessment_action(settings, action="summary")

    def prd_self_assessment_sections_api():
        return _run_prd_self_assessment_sections(settings)

    def prd_self_assessment_latest_api():
        access_gate = _require_prd_self_assessment_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        owner_key = str(user_identity.get("config_key") or "")
        try:
            if _web_helper("_local_agent_source_code_qa_enabled")(settings):
                return jsonify(_web_helper("_build_local_agent_client")(settings).prd_self_assessment_latest(owner_key=owner_key))
            latest = _web_helper("_get_prd_latest_result")(owner_key=owner_key, tool_key="prd_self_assessment")
            return jsonify({"status": "ok", "latest": latest})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001
            current_app.logger.exception("PRD Self-Assessment latest load failed.")
            return jsonify({"status": "error", "message": str(error) or "Could not load latest PRD Self-Assessment result."}), HTTPStatus.BAD_REQUEST

    class _Handlers:
        pass

    handlers = _Handlers()
    handlers.prd_self_assessment_page = prd_self_assessment_page
    handlers.prd_self_assessment_review_api = prd_self_assessment_review_api
    handlers.prd_self_assessment_summary_api = prd_self_assessment_summary_api
    handlers.prd_self_assessment_sections_api = prd_self_assessment_sections_api
    handlers.prd_self_assessment_latest_api = prd_self_assessment_latest_api
    return handlers


def register_prd_self_assessment_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/prd-self-assessment", handlers.prd_self_assessment_page)
    _add_route(app, "/prd-self-assessment/", handlers.prd_self_assessment_page)
    _add_route(app, "/api/prd-self-assessment/review", handlers.prd_self_assessment_review_api, methods=["POST"])
    _add_route(app, "/api/prd-self-assessment/summary", handlers.prd_self_assessment_summary_api, methods=["POST"])
    _add_route(app, "/api/prd-self-assessment/sections", handlers.prd_self_assessment_sections_api, methods=["POST"])
    _add_route(app, "/api/prd-self-assessment/latest", handlers.prd_self_assessment_latest_api)
