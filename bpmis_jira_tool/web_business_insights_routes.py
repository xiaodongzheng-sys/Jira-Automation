"""Business Insights MIS report routes."""
from __future__ import annotations

from http import HTTPStatus
from types import SimpleNamespace
from typing import Any, Callable

from flask import Response, jsonify, redirect, render_template, request, send_file, url_for

from bpmis_jira_tool.business_insights import (
    BusinessInsightsStore,
    GENERATOR_REPORT_IDS,
    UNDERWRITING_FUNNEL_REPORT_ID,
)
from bpmis_jira_tool.business_insights_jobs import generation_job_status, start_generation_job
from bpmis_jira_tool.errors import ToolError


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    endpoint = view_func.__name__
    app.add_url_rule(rule, endpoint, view_func, methods=methods)


def build_business_insights_handlers(ctx: Any) -> Any:
    settings = ctx.settings

    def _store() -> BusinessInsightsStore:
        return ctx._get_business_insights_store()

    def _anti_fraud_only() -> bool:
        # Restricted Business-Insights-only guests (monee/seamoney + test user).
        return bool(ctx._restricted_to_anti_fraud_business_insights(settings))

    def _visible_domains() -> list[dict[str, Any]]:
        domains = list(_store().domains())
        if _anti_fraud_only():
            return [d for d in domains if d.get("key") == "anti-fraud"]
        return domains

    def _visible_domain_keys() -> set[str]:
        return {str(d.get("key")) for d in _visible_domains()}

    def _report_domain_visible(report_id: str) -> bool:
        if not _anti_fraud_only():
            return True
        report = _store().report(report_id)
        return bool(report) and str(report.get("domain") or "") in _visible_domain_keys()

    def _visible_artifact_ids() -> set[str]:
        ids: set[str] = set()
        for domain in _visible_domains():
            for report in _store().reports(domain["key"]):
                artifact = report.get("artifact")
                if isinstance(artifact, dict) and artifact.get("id"):
                    ids.add(str(artifact["id"]))
        return ids

    def _report_payload(report: dict[str, Any]) -> dict[str, Any]:
        item = dict(report)
        artifact = item.get("artifact") if isinstance(item.get("artifact"), dict) else None
        if artifact:
            artifact_id = str(artifact.get("id") or "")
            if artifact_id:
                from bpmis_jira_tool.timefmt import format_gmt8

                artifact = dict(artifact)
                artifact["url"] = url_for("business_insights_artifact", artifact_id=artifact_id)
                if artifact.get("visualization_filename"):
                    artifact["visualization_url"] = url_for("business_insights_visualization", artifact_id=artifact_id)
                artifact["created_at_display"] = format_gmt8(artifact.get("created_at"))
                item["artifact"] = artifact
        item["sql_url"] = url_for("business_insights_report_sql", report_id=item["id"])
        item["ingest_url"] = url_for("business_insights_report_ingest", report_id=item["id"])
        # The on-demand "Refresh data" button re-runs the Data Workbench generator; admins only.
        item["can_generate"] = item["id"] in GENERATOR_REPORT_IDS and bool(ctx._can_refresh_business_insights(settings))
        if item["can_generate"]:
            item["generate_url"] = url_for("business_insights_report_generate", report_id=item["id"])
            item["generate_status_url"] = url_for("business_insights_report_generate_status", report_id=item["id"])
        return item

    def _reports_by_domain() -> dict[str, list[dict[str, Any]]]:
        return {
            domain["key"]: [_report_payload(report) for report in _store().reports(domain["key"])]
            for domain in _visible_domains()
        }

    def business_insights_page():
        access_gate = ctx._require_business_insights_access(settings)
        if access_gate is not None:
            return access_gate
        domains = _visible_domains()
        domain_keys = {domain["key"] for domain in domains}
        active_domain = str(request.args.get("domain") or "anti-fraud").strip().lower()
        if active_domain not in domain_keys:
            active_domain = "anti-fraud" if "anti-fraud" in domain_keys else (next(iter(domain_keys), "anti-fraud"))
        return render_template(
            "business_insights.html",
            page_title="Business Insights",
            user_identity=ctx._get_user_identity(settings),
            domains=domains,
            active_domain=active_domain,
            reports_by_domain=_reports_by_domain(),
            underwriting_report_id=UNDERWRITING_FUNNEL_REPORT_ID,
            # Assets and auth links go through the Cloud Run surface so the
            # public page keeps working while the Mac host is offline.
            cloud_auth_mode=True,
            asset_revision=ctx._current_release_revision(),
        )

    def business_insights_reports_api():
        access_gate = ctx._require_business_insights_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        domain = str(request.args.get("domain") or "").strip().lower()
        if _anti_fraud_only() and domain not in _visible_domain_keys():
            return jsonify({"status": "ok", "domain": domain, "reports": []})
        return jsonify({"status": "ok", "domain": domain, "reports": [_report_payload(report) for report in _store().reports(domain)]})

    def business_insights_report_sql(report_id: str):
        access_gate = ctx._require_business_insights_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _report_domain_visible(report_id):
            return jsonify({"status": "error", "message": "Report not found."}), HTTPStatus.NOT_FOUND
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
        if not _report_domain_visible(report_id):
            return jsonify({"status": "error", "message": "Report not found."}), HTTPStatus.NOT_FOUND
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

    def business_insights_report_generate(report_id: str):
        access_gate = ctx._require_business_insights_admin(settings, api=True)
        if access_gate is not None:
            return access_gate
        if report_id not in GENERATOR_REPORT_IDS:
            return jsonify({"status": "error", "message": "On-demand refresh is not available for this report."}), HTTPStatus.NOT_FOUND
        try:
            job = start_generation_job(root_dir=_store().root_dir, report_id=report_id)
        except (OSError, ValueError) as error:
            return jsonify({"status": "error", "message": f"Could not start refresh: {error}"}), HTTPStatus.INTERNAL_SERVER_ERROR
        return jsonify({"status": "ok", "report_id": report_id, "job": job})

    def business_insights_report_generate_status(report_id: str):
        access_gate = ctx._require_business_insights_admin(settings, api=True)
        if access_gate is not None:
            return access_gate
        if report_id not in GENERATOR_REPORT_IDS:
            return jsonify({"status": "error", "message": "On-demand refresh is not available for this report."}), HTTPStatus.NOT_FOUND
        job = generation_job_status(root_dir=_store().root_dir, report_id=report_id)
        payload: dict[str, Any] = {"status": "ok", "report_id": report_id, "job": job}
        if job.get("status") == "completed":
            report = _store().report(report_id)
            if report is not None:
                payload["report"] = _report_payload(report)
        return jsonify(payload)

    def business_insights_artifact(artifact_id: str):
        access_gate = ctx._require_business_insights_access(settings)
        if access_gate is not None:
            return access_gate
        if _anti_fraud_only() and artifact_id not in _visible_artifact_ids():
            return redirect(url_for("business_insights_page", domain="anti-fraud"))
        try:
            metadata, path = _store().artifact_path(artifact_id)
        except ToolError:
            return redirect(url_for("business_insights_page", domain="anti-fraud"))
        response = send_file(
            path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            download_name=str(metadata.get("filename") or "business-insights.xlsx"),
            as_attachment=True,
        )
        # Hash-named, password-gated -> safe to cache in the browser (private,
        # never a shared cache) so repeat opens don't re-download.
        # Artifacts are regenerated in place under the same filename (e.g. on a data refresh or a
        # visualization rebuild), so the browser must revalidate each load instead of serving a day-old
        # cached copy. no-cache = "always revalidate"; the files are small and admin-only.
        response.headers["Cache-Control"] = "private, no-cache, must-revalidate"
        return response

    def business_insights_visualization(artifact_id: str):
        access_gate = ctx._require_business_insights_access(settings)
        if access_gate is not None:
            return access_gate
        if _anti_fraud_only() and artifact_id not in _visible_artifact_ids():
            return redirect(url_for("business_insights_page", domain="anti-fraud"))
        try:
            metadata, path = _store().visualization_path(artifact_id)
        except ToolError:
            return redirect(url_for("business_insights_page", domain="anti-fraud"))
        response = send_file(
            path,
            mimetype="text/html; charset=utf-8",
            download_name=str(metadata.get("visualization_filename") or "business-insights-visualization.html"),
            as_attachment=False,
        )
        # Artifacts are regenerated in place under the same filename (e.g. on a data refresh or a
        # visualization rebuild), so the browser must revalidate each load instead of serving a day-old
        # cached copy. no-cache = "always revalidate"; the files are small and admin-only.
        response.headers["Cache-Control"] = "private, no-cache, must-revalidate"
        return response

    return SimpleNamespace(
        business_insights_page=business_insights_page,
        business_insights_reports_api=business_insights_reports_api,
        business_insights_report_sql=business_insights_report_sql,
        business_insights_report_ingest=business_insights_report_ingest,
        business_insights_report_generate=business_insights_report_generate,
        business_insights_report_generate_status=business_insights_report_generate_status,
        business_insights_artifact=business_insights_artifact,
        business_insights_visualization=business_insights_visualization,
    )


def register_business_insights_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/business-insights", handlers.business_insights_page)
    _add_route(app, "/api/business-insights/reports", handlers.business_insights_reports_api)
    _add_route(app, "/api/business-insights/reports/<report_id>/sql", handlers.business_insights_report_sql)
    _add_route(app, "/api/business-insights/reports/<report_id>/ingest", handlers.business_insights_report_ingest, methods=["POST"])
    _add_route(app, "/api/business-insights/reports/<report_id>/generate", handlers.business_insights_report_generate, methods=["POST"])
    _add_route(app, "/api/business-insights/reports/<report_id>/generate/status", handlers.business_insights_report_generate_status)
    _add_route(app, "/business-insights/artifacts/<artifact_id>.xlsx", handlers.business_insights_artifact)
    _add_route(app, "/business-insights/visualizations/<artifact_id>.html", handlers.business_insights_visualization)
