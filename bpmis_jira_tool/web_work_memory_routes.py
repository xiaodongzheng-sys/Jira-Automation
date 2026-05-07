"""Work Memory and Superagent route registration."""
from __future__ import annotations

from dataclasses import asdict
from http import HTTPStatus
import threading
from typing import Any

from flask import current_app, jsonify, render_template, request, session

from bpmis_jira_tool.errors import ConfigError, ToolError


def register_work_memory_routes(app: Any, settings: Any, deps: Any) -> None:
    @app.get("/work-memory")
    def work_memory_page():
        access_gate = deps.require_work_memory_access(settings)
        if access_gate is not None:
            return access_gate
        return render_template("work_memory.html", page_title="AI Memory")

    @app.get("/api/work-memory/health")
    def work_memory_health_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if deps.local_agent_work_memory_enabled(settings):
            return jsonify(deps.build_local_agent_client(settings).work_memory_health())
        return jsonify(deps.get_work_memory_store().health())

    @app.get("/api/work-memory/recent")
    def work_memory_recent_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        query_args = {
            "owner_email": deps.current_google_email(),
            "visibility_scope": str(request.args.get("scope") or "owner").strip().lower() or "owner",
            "query": str(request.args.get("q") or ""),
            "filters": {
                "source_type": str(request.args.get("source_type") or "").strip(),
                "item_type": str(request.args.get("item_type") or "").strip(),
            },
            "limit": int(request.args.get("limit") or 50),
        }
        if deps.local_agent_work_memory_enabled(settings):
            items = deps.build_local_agent_client(settings).work_memory_recent(**query_args)
        else:
            items = deps.get_work_memory_store().query_work_memory(**query_args)
        return jsonify({"status": "ok", "items": items})

    @app.get("/api/work-memory/review-candidates")
    def work_memory_review_candidates_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if deps.local_agent_work_memory_enabled(settings):
            items = deps.build_local_agent_client(settings).work_memory_review_candidates(owner_email=deps.current_google_email(), limit=int(request.args.get("limit") or 50))
        else:
            items = deps.get_work_memory_store().review_candidates(owner_email=deps.current_google_email(), limit=int(request.args.get("limit") or 50))
        return jsonify({"status": "ok", "items": items})

    @app.get("/api/work-memory/project-timeline")
    def work_memory_project_timeline_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        project_ref = str(request.args.get("project_ref") or request.args.get("q") or "").strip()
        query_args = {
            "project_ref": project_ref,
            "owner_email": deps.current_google_email(),
            "visibility_scope": str(request.args.get("scope") or "owner").strip().lower() or "owner",
            "limit": int(request.args.get("limit") or 100),
        }
        if deps.local_agent_work_memory_enabled(settings):
            items = deps.build_local_agent_client(settings).work_memory_project_timeline(**query_args)
        else:
            items = deps.get_work_memory_store().project_timeline(**query_args)
        return jsonify({"status": "ok", "items": items})

    @app.get("/api/work-memory/entity-resolution")
    def work_memory_entity_resolution_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        query_args = {
            "query": str(request.args.get("q") or request.args.get("query") or "").strip(),
            "owner_email": deps.current_google_email(),
            "entity_type": str(request.args.get("entity_type") or "").strip(),
        }
        if deps.local_agent_work_memory_enabled(settings):
            result = deps.build_local_agent_client(settings).work_memory_entity_resolution(**query_args)
        else:
            result = deps.get_work_memory_store().resolve_work_entity(**query_args)
        return jsonify(result)

    @app.post("/api/work-memory/feedback")
    def work_memory_feedback_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            feedback_args = {
                "item_id": str(payload.get("item_id") or "").strip(),
                "action": str(payload.get("action") or "").strip(),
                "owner_email": deps.current_google_email(),
                "correction_text": str(payload.get("correction_text") or "").strip(),
                "visibility_override": str(payload.get("visibility_override") or "").strip(),
                "reason": str(payload.get("reason") or "").strip(),
            }
            if deps.local_agent_work_memory_enabled(settings):
                result = deps.build_local_agent_client(settings).work_memory_feedback(**feedback_args)
            else:
                result = deps.get_work_memory_store().record_memory_feedback(**feedback_args)
            return jsonify(result)
        except (KeyError, ValueError) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/work-memory/ingest-sent-monthly-reports")
    def work_memory_ingest_sent_monthly_reports_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            result = deps.ingest_sent_monthly_reports_from_gmail(settings)
            return jsonify({"status": "ok", **result})
        except (ConfigError, ToolError) as error:
            return jsonify({"status": "error", "message": str(error), **deps.classify_portal_error(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/work-memory/backfill-existing")
    def work_memory_backfill_existing_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        backfill_args = {
            "owner_email": deps.current_google_email(),
            "date_range": str(payload.get("date_range") or "90d").strip() or "90d",
            "sources": [str(item or "").strip() for item in payload.get("sources") or [] if str(item or "").strip()] if isinstance(payload.get("sources"), list) else [],
        }
        if deps.local_agent_work_memory_enabled(settings):
            result = deps.build_local_agent_client(settings).work_memory_backfill_existing(**backfill_args)
        else:
            result = deps.ingest_existing_work_memory_sources(settings, date_range=backfill_args["date_range"], sources=backfill_args["sources"])
        return jsonify({"status": "ok", **result})

    @app.post("/api/work-memory/distill")
    def work_memory_distill_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        distill_args = {
            "owner_email": deps.current_google_email(),
            "date_range": str(payload.get("date_range") or "90d").strip() or "90d",
            "sources": [str(item or "").strip() for item in payload.get("sources") or [] if str(item or "").strip()] if isinstance(payload.get("sources"), list) else [],
            "project_refs": [str(item or "").strip() for item in payload.get("project_refs") or [] if str(item or "").strip()] if isinstance(payload.get("project_refs"), list) else [],
        }
        if deps.local_agent_work_memory_enabled(settings):
            result = deps.build_local_agent_client(settings).work_memory_distill(**distill_args)
        else:
            result = deps.get_work_memory_store().distill_work_memory(**distill_args)
        return jsonify({"status": "ok", **result})

    @app.post("/api/work-memory/ingest-incremental")
    def work_memory_ingest_incremental_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            if deps.local_agent_work_memory_enabled(settings):
                result = deps.build_local_agent_client(settings).work_memory_ingest_incremental(
                    owner_email=deps.current_google_email(),
                    window=str(payload.get("window") or "7d").strip() or "7d",
                    reconciliation=bool(payload.get("reconciliation")),
                )
            else:
                result = deps.run_incremental_memory_ingestion(
                    settings,
                    window=str(payload.get("window") or "7d").strip() or "7d",
                    reconciliation=bool(payload.get("reconciliation")),
                )
            return jsonify({"status": "ok", **result})
        except (ConfigError, ToolError) as error:
            return jsonify({"status": "error", "message": str(error), **deps.classify_portal_error(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/work-memory/backfill-gmail")
    def work_memory_backfill_gmail_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not deps.google_credentials_have_scopes(deps.GMAIL_READONLY_SCOPE):
            return jsonify(
                {
                    "status": "error",
                    "message": "Gmail read permission is missing. Reconnect Google once to grant gmail.readonly.",
                }
            ), HTTPStatus.BAD_REQUEST
        payload = request.get_json(silent=True) or {}
        owner_email = deps.current_google_email()
        job_store = current_app.config["JOB_STORE"]
        active = job_store.active_for_record(deps.WORK_MEMORY_GMAIL_BACKFILL_ACTION, owner_email=owner_email, record_id=owner_email)
        if active:
            return jsonify({**active, "status": "queued" if active.get("state") == "queued" else "running"}), HTTPStatus.ACCEPTED
        try:
            days = max(1, min(int(payload.get("days") or 90), 365))
            max_messages_raw = payload.get("max_messages")
            max_messages = None
            if max_messages_raw is not None and str(max_messages_raw).strip():
                max_messages = max(1, min(int(max_messages_raw), 10000))
        except (TypeError, ValueError):
            return jsonify({"status": "error", "message": "days and max_messages must be numbers."}), HTTPStatus.BAD_REQUEST
        job = job_store.create(deps.WORK_MEMORY_GMAIL_BACKFILL_ACTION, "Gmail Work Memory Backfill", owner_email=owner_email, record_id=owner_email)
        app_obj = current_app._get_current_object()
        runner_payload = {
            "owner_email": owner_email,
            "days": days,
            "max_messages": max_messages,
            "credentials": dict(session.get("google_credentials") or {}),
            "report_intelligence_config": deps.get_team_dashboard_config_store().load().get("report_intelligence_config") or {},
            "drive_read_enabled": deps.google_credentials_have_scopes(deps.GOOGLE_DRIVE_READONLY_SCOPE),
        }
        threading.Thread(target=deps.run_work_memory_gmail_backfill_job, args=(app_obj, job.job_id, runner_payload), daemon=True).start()
        snapshot = job_store.snapshot(job.job_id) or asdict(job)
        return jsonify({**snapshot, "status": "queued", "job_id": job.job_id}), HTTPStatus.ACCEPTED

    @app.get("/api/work-memory/ingestion-jobs")
    def work_memory_ingestion_jobs_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            limit = max(1, min(int(request.args.get("limit") or 20), 100))
        except (TypeError, ValueError):
            return jsonify({"status": "error", "message": "limit must be a number."}), HTTPStatus.BAD_REQUEST
        snapshots = current_app.config["JOB_STORE"].list_snapshots(
            action=deps.WORK_MEMORY_GMAIL_BACKFILL_ACTION,
            owner_email=deps.current_google_email(),
            limit=limit,
        )
        return jsonify({"status": "ok", "items": snapshots})

    @app.get("/api/superagent/health")
    def superagent_health_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if deps.local_agent_work_memory_enabled(settings):
            return jsonify(deps.build_local_agent_client(settings).superagent_health(owner_email=deps.current_google_email()))
        return jsonify(deps.get_work_memory_store().superagent_health(owner_email=deps.current_google_email()))

    @app.post("/api/superagent/query")
    def superagent_query_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        task_type = str(payload.get("task_type") or "general").strip() or "general"
        query_text = str(payload.get("query") or "").strip()
        if deps.local_agent_work_memory_enabled(settings):
            return jsonify(
                deps.build_local_agent_client(settings).superagent_query(
                    owner_email=deps.current_google_email(),
                    user_email=deps.current_google_email(),
                    query=query_text,
                    task_type=task_type,
                    visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
                    limit=int(payload.get("limit") or 12),
                )
            )
        context = deps.get_work_memory_store().query_superagent_context(
            query=query_text,
            owner_email=deps.current_google_email(),
            visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
            task_type=task_type,
            limit=int(payload.get("limit") or 12),
        )
        result = deps.get_work_memory_store().generate_llm_superagent_answer(task_type=task_type, query=query_text, context=context)
        audit = deps.get_work_memory_store().record_superagent_audit_log(
            owner_email=deps.current_google_email(),
            user_email=deps.current_google_email(),
            query=query_text,
            task_type=task_type,
            visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
            context=context,
            answer=result,
            metadata={"route": "/api/superagent/query"},
        )
        return jsonify({"status": "ok", "context": context, "audit": audit, **result})

    @app.post("/api/superagent/explain")
    def superagent_explain_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        if deps.local_agent_work_memory_enabled(settings):
            return jsonify(
                deps.build_local_agent_client(settings).superagent_explain(
                    owner_email=deps.current_google_email(),
                    query=str(payload.get("query") or "").strip(),
                    task_type=str(payload.get("task_type") or "general").strip() or "general",
                    visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
                )
            )
        result = deps.get_work_memory_store().explain_superagent_answer(
            owner_email=deps.current_google_email(),
            query=str(payload.get("query") or "").strip(),
            task_type=str(payload.get("task_type") or "general").strip() or "general",
            visibility_scope=str(payload.get("visibility_scope") or "owner").strip().lower() or "owner",
        )
        return jsonify(result)

    @app.post("/api/superagent/eval")
    def superagent_eval_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        if deps.local_agent_work_memory_enabled(settings):
            return jsonify(
                deps.build_local_agent_client(settings).superagent_eval(
                    owner_email=deps.current_google_email(),
                    cases=payload.get("cases") if isinstance(payload.get("cases"), list) else None,
                    limit=int(payload.get("limit") or 30),
                    suite_id=str(request.args.get("suite_id") or payload.get("suite_id") or "").strip(),
                )
            )
        result = deps.get_work_memory_store().run_superagent_eval_cases(
            owner_email=deps.current_google_email(),
            cases=payload.get("cases") if isinstance(payload.get("cases"), list) else None,
            limit=int(payload.get("limit") or 30),
            suite_id=str(request.args.get("suite_id") or payload.get("suite_id") or "").strip(),
        )
        return jsonify(result)

    @app.post("/api/superagent/quality-gate")
    def superagent_quality_gate_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        suite_id = str(request.args.get("suite_id") or payload.get("suite_id") or "gold_v1").strip() or "gold_v1"
        limit = int(payload.get("limit") or 30)
        min_cases = int(payload.get("min_cases") or 1)
        if deps.local_agent_work_memory_enabled(settings):
            return jsonify(
                deps.build_local_agent_client(settings).superagent_quality_gate(
                    owner_email=deps.current_google_email(),
                    suite_id=suite_id,
                    limit=limit,
                    min_cases=min_cases,
                )
            )
        return jsonify(
            deps.get_work_memory_store().run_superagent_quality_gate(
                owner_email=deps.current_google_email(),
                suite_id=suite_id,
                limit=limit,
                min_cases=min_cases,
            )
        )

    @app.get("/api/superagent/audit")
    def superagent_audit_api():
        access_gate = deps.require_work_memory_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if deps.local_agent_work_memory_enabled(settings):
            return jsonify(
                {
                    "status": "ok",
                    "items": deps.build_local_agent_client(settings).superagent_audit(
                        owner_email=deps.current_google_email(),
                        limit=int(request.args.get("limit") or 50),
                    ),
                }
            )
        return jsonify(
            {
                "status": "ok",
                "items": deps.get_work_memory_store().superagent_audit_log(
                    owner_email=deps.current_google_email(),
                    limit=int(request.args.get("limit") or 50),
                ),
            }
        )

