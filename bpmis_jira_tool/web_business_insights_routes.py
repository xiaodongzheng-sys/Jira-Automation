"""Business Insights MIS report routes."""
from __future__ import annotations

from http import HTTPStatus
from types import SimpleNamespace
from typing import Any, Callable

from flask import Response, jsonify, redirect, render_template, request, send_file, url_for

from bpmis_jira_tool.business_insights import BusinessInsightsStore, UNDERWRITING_FUNNEL_REPORT_ID
from bpmis_jira_tool.errors import ToolError


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    endpoint = view_func.__name__
    app.add_url_rule(rule, endpoint, view_func, methods=methods)


def build_business_insights_handlers(ctx: Any) -> Any:
    settings = ctx.settings

    def _store() -> BusinessInsightsStore:
        return ctx._get_business_insights_store()

    def _report_payload(report: dict[str, Any]) -> dict[str, Any]:
        item = dict(report)
        artifact = item.get("artifact") if isinstance(item.get("artifact"), dict) else None
        if artifact:
            artifact_id = str(artifact.get("id") or "")
            if artifact_id:
                artifact = dict(artifact)
                artifact["url"] = url_for("business_insights_artifact", artifact_id=artifact_id)
                if artifact.get("visualization_filename"):
                    artifact["visualization_url"] = url_for("business_insights_visualization", artifact_id=artifact_id)
                item["artifact"] = artifact
        item["sql_url"] = url_for("business_insights_report_sql", report_id=item["id"])
        item["ingest_url"] = url_for("business_insights_report_ingest", report_id=item["id"])
        return item

    def _reports_by_domain() -> dict[str, list[dict[str, Any]]]:
        return {
            domain["key"]: [_report_payload(report) for report in _store().reports(domain["key"])]
            for domain in _store().domains()
        }

    def business_insights_page():
        access_gate = ctx._require_business_insights_access(settings)
        if access_gate is not None:
            return access_gate
        active_domain = str(request.args.get("domain") or "anti-fraud").strip().lower()
        domain_keys = {domain["key"] for domain in _store().domains()}
        if active_domain not in domain_keys:
            active_domain = "anti-fraud"
        return render_template(
            "business_insights.html",
            page_title="Business Insights",
            user_identity=ctx._get_user_identity(settings),
            domains=_store().domains(),
            active_domain=active_domain,
            reports_by_domain=_reports_by_domain(),
            underwriting_report_id=UNDERWRITING_FUNNEL_REPORT_ID,
            asset_revision=ctx._current_release_revision(),
        )

    def business_insights_reports_api():
        access_gate = ctx._require_business_insights_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        domain = str(request.args.get("domain") or "").strip().lower()
        return jsonify({"status": "ok", "domain": domain, "reports": [_report_payload(report) for report in _store().reports(domain)]})

    def business_insights_report_sql(report_id: str):
        access_gate = ctx._require_business_insights_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            sql = _store().sql_for_report(report_id)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.NOT_FOUND
        if str(request.args.get("format") or "").lower() == "raw":
            response = Response(sql, mimetype="text/plain")
            if str(request.args.get("download") or "").lower() in {"1", "true", "yes"}:
                response.headers["Content-Disposition"] = f"attachment; filename={_store().sql_filename_for_report(report_id)}"
            return response
        return jsonify({"status": "ok", "report_id": report_id, "sql": sql})

    def business_insights_report_ingest(report_id: str):
        access_gate = ctx._require_business_insights_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if report_id != UNDERWRITING_FUNNEL_REPORT_ID:
            return jsonify({"status": "error", "message": "Export ingestion is not configured for this report yet."}), HTTPStatus.NOT_FOUND
        uploaded = request.files.get("file")
        if uploaded is None:
            return jsonify({"status": "error", "message": "Upload a Data Workbench export using the file field."}), HTTPStatus.BAD_REQUEST
        try:
            artifact = _store().save_underwriting_export(content=uploaded.read(), filename=uploaded.filename or "export.csv")
            artifact["url"] = url_for("business_insights_artifact", artifact_id=artifact["id"])
            return jsonify({"status": "ok", "artifact": artifact})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    def business_insights_artifact(artifact_id: str):
        access_gate = ctx._require_business_insights_access(settings)
        if access_gate is not None:
            return access_gate
        try:
            metadata, path = _store().artifact_path(artifact_id)
        except ToolError:
            return redirect(url_for("business_insights_page", domain="credit-risk"))
        return send_file(
            path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            download_name=str(metadata.get("filename") or "business-insights.xlsx"),
            as_attachment=True,
        )

    def business_insights_visualization(artifact_id: str):
        access_gate = ctx._require_business_insights_access(settings)
        if access_gate is not None:
            return access_gate
        try:
            metadata, path = _store().visualization_path(artifact_id)
        except ToolError:
            return redirect(url_for("business_insights_page", domain="credit-risk"))
        return send_file(
            path,
            mimetype="text/html; charset=utf-8",
            download_name=str(metadata.get("visualization_filename") or "business-insights-visualization.html"),
            as_attachment=False,
        )

    return SimpleNamespace(
        business_insights_page=business_insights_page,
        business_insights_reports_api=business_insights_reports_api,
        business_insights_report_sql=business_insights_report_sql,
        business_insights_report_ingest=business_insights_report_ingest,
        business_insights_artifact=business_insights_artifact,
        business_insights_visualization=business_insights_visualization,
    )


def register_business_insights_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/business-insights", handlers.business_insights_page)
    _add_route(app, "/api/business-insights/reports", handlers.business_insights_reports_api)
    _add_route(app, "/api/business-insights/reports/<report_id>/sql", handlers.business_insights_report_sql)
    _add_route(app, "/api/business-insights/reports/<report_id>/ingest", handlers.business_insights_report_ingest, methods=["POST"])
    _add_route(app, "/business-insights/artifacts/<artifact_id>.xlsx", handlers.business_insights_artifact)
    _add_route(app, "/business-insights/visualizations/<artifact_id>.html", handlers.business_insights_visualization)
