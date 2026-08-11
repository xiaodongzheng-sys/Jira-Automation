"""Issue Management V1 demo page routes.

The first release is intentionally a front-end demonstration surface.  Its
records are kept in the browser by ``static/issue_management.js`` so a PM can
walk through the V1 workflow without mutating the portal's production stores.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable

from flask import render_template, request


def _add_route(app: Any, rule: str, view_func: Callable[..., Any]) -> None:
    app.add_url_rule(rule, endpoint=view_func.__name__, view_func=view_func)


def build_issue_management_handlers(ctx: Any) -> Any:
    settings = ctx.settings
    _get_user_identity = ctx._get_user_identity

    def issue_management_page():
        return render_template(
            "issue_management.html",
            page_title="Issue Management",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            demo_mode=False,
        )

    def issue_management_create_page():
        return render_template(
            "issue_management_create.html",
            page_title="Create New Issue & Action Plan",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            initial_creator=str(request.args.get("creator") or "").strip(),
            demo_mode=False,
        )

    def issue_management_edit_page(issue_id: str):
        return render_template(
            "issue_management_edit.html",
            page_title="Edit Issue & Action Plan",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            issue_id=issue_id,
            demo_mode=False,
        )

    def issue_management_view_page(issue_id: str):
        return render_template(
            "issue_management_view.html",
            page_title="View Issue & Action Plan",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            issue_id=issue_id,
            demo_mode=False,
        )

    def issue_management_action_plan_view_page(issue_id: str, ap_id: str):
        return render_template(
            "issue_management_action_plan_view.html",
            page_title="View Action Plan",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            issue_id=issue_id,
            ap_id=ap_id,
            demo_mode=False,
        )

    def issue_management_demo_page():
        return render_template(
            "issue_management.html",
            page_title="Issue Management Demo",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            demo_mode=True,
        )

    def issue_management_demo_create_page():
        return render_template(
            "issue_management_create.html",
            page_title="Create New Issue & Action Plan Demo",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            initial_creator=str(request.args.get("creator") or "").strip(),
            demo_mode=True,
        )

    def issue_management_demo_edit_page(issue_id: str):
        return render_template(
            "issue_management_edit.html",
            page_title="Edit Issue & Action Plan Demo",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            issue_id=issue_id,
            demo_mode=True,
        )

    def issue_management_demo_view_page(issue_id: str):
        return render_template(
            "issue_management_view.html",
            page_title="View Issue & Action Plan Demo",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            issue_id=issue_id,
            demo_mode=True,
        )

    def issue_management_demo_action_plan_view_page(issue_id: str, ap_id: str):
        return render_template(
            "issue_management_action_plan_view.html",
            page_title="View Action Plan Demo",
            user_identity=_get_user_identity(settings),
            cloud_auth_mode=bool(settings.cloud_home_enabled),
            suppress_site_navigation=True,
            suppress_admin_login=True,
            issue_id=issue_id,
            ap_id=ap_id,
            demo_mode=True,
        )

    return SimpleNamespace(
        issue_management_page=issue_management_page,
        issue_management_create_page=issue_management_create_page,
        issue_management_edit_page=issue_management_edit_page,
        issue_management_view_page=issue_management_view_page,
        issue_management_action_plan_view_page=issue_management_action_plan_view_page,
        issue_management_demo_page=issue_management_demo_page,
        issue_management_demo_create_page=issue_management_demo_create_page,
        issue_management_demo_edit_page=issue_management_demo_edit_page,
        issue_management_demo_view_page=issue_management_demo_view_page,
        issue_management_demo_action_plan_view_page=issue_management_demo_action_plan_view_page,
    )


def register_issue_management_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/issue-management", handlers.issue_management_page)
    _add_route(app, "/issue-management/", handlers.issue_management_page)
    _add_route(app, "/issue-management/create", handlers.issue_management_create_page)
    _add_route(app, "/issue-management/edit/<path:issue_id>", handlers.issue_management_edit_page)
    _add_route(app, "/issue-management/view/<path:issue_id>/action-plan/<path:ap_id>", handlers.issue_management_action_plan_view_page)
    _add_route(app, "/issue-management/view/<path:issue_id>", handlers.issue_management_view_page)
    _add_route(app, "/demo", handlers.issue_management_demo_page)
    _add_route(app, "/demo/", handlers.issue_management_demo_page)
    _add_route(app, "/demo/create", handlers.issue_management_demo_create_page)
    _add_route(app, "/demo/edit/<path:issue_id>", handlers.issue_management_demo_edit_page)
    _add_route(app, "/demo/view/<path:issue_id>/action-plan/<path:ap_id>", handlers.issue_management_demo_action_plan_view_page)
    _add_route(app, "/demo/view/<path:issue_id>", handlers.issue_management_demo_view_page)
