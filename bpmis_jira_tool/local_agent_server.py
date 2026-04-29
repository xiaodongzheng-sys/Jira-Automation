from __future__ import annotations

import base64
import json
import os
import tempfile
import threading
import time
from dataclasses import asdict, is_dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Any
import uuid

from flask import Flask, current_app, jsonify, request

from bpmis_jira_tool.bpmis import BPMISDirectApiClient
from bpmis_jira_tool.bpmis_projects import BPMISProjectStore
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_protocol import NONCE_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, verify_signature
from bpmis_jira_tool.models import ProjectMatch
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService
from bpmis_jira_tool.source_code_qa import SourceCodeQAService
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS, WebConfigStore
from bpmis_jira_tool.web import (
    SeaTalkNameMappingStore,
    SeaTalkTodoStore,
    SourceCodeQAAttachmentStore,
    SourceCodeQAModelAvailabilityStore,
    SourceCodeQARuntimeEvidenceStore,
    SourceCodeQASessionStore,
    _generate_productization_detailed_features_with_local_codex,
)


BPMIS_PROXY_OPERATIONS = {
    "ping",
    "find_project",
    "create_jira_ticket",
    "list_biz_projects_for_pm_email",
    "list_jira_tasks_for_project_created_by_email",
    "list_jira_tasks_created_by_emails",
    "get_single_brd_doc_link_for_project",
    "get_single_brd_doc_links_for_projects",
    "get_brd_doc_links_for_projects",
    "search_versions",
    "list_issues_for_version",
    "get_issue_detail",
    "get_jira_ticket_detail",
    "update_jira_ticket_status",
    "update_jira_ticket_fix_version",
    "delink_jira_ticket_from_project",
}

_SOURCE_CODE_QA_AUTO_SYNC_LOCK = threading.Lock()
_SOURCE_CODE_QA_AUTO_SYNC_KEYS: set[str] = set()
_SOURCE_CODE_QA_QUERY_JOB_TTL_SECONDS = 3600


def create_local_agent_app() -> Flask:
    settings = Settings.from_env()
    app = Flask(__name__)
    app.config["SETTINGS"] = settings
    app.config["SOURCE_CODE_QA_SERVICE"] = _build_source_code_qa_service(settings)
    app.config["SOURCE_CODE_QA_QUERY_JOBS"] = {}
    app.config["SOURCE_CODE_QA_QUERY_JOBS_LOCK"] = threading.Lock()

    @app.before_request
    def verify_local_agent_signature():
        if request.path in {"/healthz", "/api/local-agent/healthz"}:
            return None
        try:
            verify_signature(
                secret=settings.local_agent_hmac_secret or "",
                method=request.method,
                path=request.path,
                body=request.get_data() or b"",
                timestamp=request.headers.get(TIMESTAMP_HEADER, ""),
                nonce=request.headers.get(NONCE_HEADER, ""),
                signature=request.headers.get(SIGNATURE_HEADER, ""),
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.UNAUTHORIZED
        return None

    @app.get("/healthz")
    @app.get("/api/local-agent/healthz")
    def healthz():
        service: SourceCodeQAService = app.config["SOURCE_CODE_QA_SERVICE"]
        return jsonify(
            {
                "status": "ok",
                "capabilities": {
                    "source_code_qa": True,
                    "codex_ready": service.with_llm_provider("codex_cli_bridge").llm_ready(),
                    "seatalk_configured": _seatalk_configured(settings),
                    "bpmis_mode": settings.bpmis_call_mode,
                    "bpmis_proxy": settings.local_agent_bpmis_enabled,
                },
            }
        )

    @app.post("/api/local-agent/source-code-qa/config")
    def source_code_qa_config():
        payload = request.get_json(silent=True) or {}
        service = _source_code_qa_service(payload.get("llm_provider"))
        return jsonify(
            {
                "status": "ok",
                "git_auth_ready": bool(service.gitlab_token),
                "llm_ready": service.llm_ready(),
                "llm_provider": service.llm_provider_name,
                "llm_policy": service.llm_policy_payload(),
                "index_health": service.index_health_payload(),
                "domain_knowledge": service.domain_knowledge_payload(),
                "config": service.load_config(),
            }
        )

    @app.post("/api/local-agent/source-code-qa/config/save")
    def source_code_qa_save_config():
        payload = request.get_json(silent=True) or {}
        result = _source_code_qa_service().save_mapping(
            pm_team=str(payload.get("pm_team") or ""),
            country=str(payload.get("country") or ""),
            repositories=payload.get("repositories") or [],
        )
        return jsonify({"status": "ok", **result})

    @app.post("/api/local-agent/source-code-qa/sync")
    def source_code_qa_sync():
        payload = request.get_json(silent=True) or {}
        result = _source_code_qa_service().sync(
            pm_team=str(payload.get("pm_team") or ""),
            country=str(payload.get("country") or ""),
        )
        return jsonify({"status": "ok", **result})

    @app.post("/api/local-agent/source-code-qa/ensure-synced-today")
    def source_code_qa_ensure_synced_today():
        payload = request.get_json(silent=True) or {}
        service = _source_code_qa_service()
        pm_team = str(payload.get("pm_team") or "")
        country = str(payload.get("country") or "")
        if payload.get("background"):
            result = _queue_source_code_qa_auto_sync(service, pm_team=pm_team, country=country)
        else:
            result = service.ensure_synced_today(pm_team=pm_team, country=country)
        return jsonify({"status": "ok", **result})

    @app.post("/api/local-agent/source-code-qa/query")
    def source_code_qa_query():
        payload = request.get_json(silent=True) or {}
        service = _source_code_qa_service(payload.get("llm_provider"))
        result = service.query(
            pm_team=str(payload.get("pm_team") or ""),
            country=str(payload.get("country") or ""),
            question=str(payload.get("question") or ""),
            answer_mode=str(payload.get("answer_mode") or "auto"),
            llm_budget_mode=str(payload.get("llm_budget_mode") or "auto"),
            conversation_context=payload.get("conversation_context") if isinstance(payload.get("conversation_context"), dict) else None,
            attachments=payload.get("attachments") if isinstance(payload.get("attachments"), list) else None,
            runtime_evidence=payload.get("runtime_evidence") if isinstance(payload.get("runtime_evidence"), list) else None,
        )
        return jsonify({"status": "ok", **result})

    @app.post("/api/local-agent/source-code-qa/query-async")
    def source_code_qa_query_async():
        payload = request.get_json(silent=True) or {}
        job_id = uuid.uuid4().hex
        _update_query_job(
            job_id,
            state="queued",
            stage="queued",
            message="Queued Source Code Q&A query on Mac local-agent.",
            current=0,
            total=0,
        )
        thread = threading.Thread(
            target=_run_source_code_qa_query_job,
            args=(app, job_id, payload),
            daemon=True,
        )
        thread.start()
        return jsonify({"status": "ok", "job_id": job_id})

    @app.get("/api/local-agent/source-code-qa/query-jobs/<job_id>")
    def source_code_qa_query_job_status(job_id: str):
        snapshot = _snapshot_query_job(job_id)
        if snapshot is None:
            return jsonify({"status": "error", "message": "Source Code Q&A local-agent job was not found."}), HTTPStatus.NOT_FOUND
        return jsonify({"status": "ok", **snapshot})

    @app.post("/api/local-agent/productization/llm-descriptions")
    def productization_llm_descriptions():
        payload = request.get_json(silent=True) or {}
        items = payload.get("items")
        if not isinstance(items, list):
            return jsonify({"status": "error", "message": "items must be a list."}), HTTPStatus.BAD_REQUEST
        generated = _generate_productization_detailed_features_with_local_codex(
            [item for item in items if isinstance(item, dict)],
            settings=settings,
        )
        return jsonify({"status": "ok", "items": generated})

    @app.post("/api/local-agent/source-code-qa/sessions/list")
    def source_code_qa_sessions_list():
        payload = request.get_json(silent=True) or {}
        sessions = _build_source_code_qa_session_store(settings).list(
            owner_email=str(payload.get("owner_email") or ""),
            limit=int(payload.get("limit") or 30),
        )
        return jsonify({"status": "ok", "sessions": sessions})

    @app.post("/api/local-agent/source-code-qa/sessions/create")
    def source_code_qa_session_create():
        payload = request.get_json(silent=True) or {}
        session = _build_source_code_qa_session_store(settings).create(
            owner_email=str(payload.get("owner_email") or ""),
            pm_team=str(payload.get("pm_team") or ""),
            country=str(payload.get("country") or ""),
            llm_provider=str(payload.get("llm_provider") or ""),
            title=str(payload.get("title") or ""),
        )
        return jsonify({"status": "ok", "session": session})

    @app.post("/api/local-agent/source-code-qa/sessions/get")
    def source_code_qa_session_get():
        payload = request.get_json(silent=True) or {}
        session = _build_source_code_qa_session_store(settings).get(
            str(payload.get("session_id") or ""),
            owner_email=str(payload.get("owner_email") or ""),
        )
        return jsonify({"status": "ok", "session": session})

    @app.post("/api/local-agent/source-code-qa/sessions/archive")
    def source_code_qa_session_archive():
        payload = request.get_json(silent=True) or {}
        archived = _build_source_code_qa_session_store(settings).archive(
            str(payload.get("session_id") or ""),
            owner_email=str(payload.get("owner_email") or ""),
        )
        return jsonify({"status": "ok", "archived": archived})

    @app.post("/api/local-agent/source-code-qa/sessions/context")
    def source_code_qa_session_context():
        payload = request.get_json(silent=True) or {}
        context = _build_source_code_qa_session_store(settings).get_context(
            str(payload.get("session_id") or ""),
            owner_email=str(payload.get("owner_email") or ""),
        )
        return jsonify({"status": "ok", "context": context})

    @app.post("/api/local-agent/source-code-qa/sessions/append")
    def source_code_qa_session_append():
        payload = request.get_json(silent=True) or {}
        session = _build_source_code_qa_session_store(settings).append_exchange(
            str(payload.get("session_id") or ""),
            owner_email=str(payload.get("owner_email") or ""),
            pm_team=str(payload.get("pm_team") or ""),
            country=str(payload.get("country") or ""),
            llm_provider=str(payload.get("llm_provider") or ""),
            question=str(payload.get("question") or ""),
            result=payload.get("result") if isinstance(payload.get("result"), dict) else {},
            context=payload.get("context") if isinstance(payload.get("context"), dict) else {},
            attachments=payload.get("attachments") if isinstance(payload.get("attachments"), list) else None,
        )
        return jsonify({"status": "ok", "session": session})

    @app.post("/api/local-agent/source-code-qa/attachments/save")
    def source_code_qa_attachment_save():
        payload = request.get_json(silent=True) or {}
        try:
            content = base64.b64decode(str(payload.get("content_base64") or "").encode("ascii"))
            attachment = _build_source_code_qa_attachment_store(settings).save_bytes(
                owner_email=str(payload.get("owner_email") or ""),
                session_id=str(payload.get("session_id") or ""),
                filename=str(payload.get("filename") or "attachment"),
                mime_type=str(payload.get("mime_type") or ""),
                content=content,
            )
            return jsonify({"status": "ok", "attachment": attachment})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/local-agent/source-code-qa/attachments/resolve")
    def source_code_qa_attachments_resolve():
        payload = request.get_json(silent=True) or {}
        try:
            attachments = _build_source_code_qa_attachment_store(settings).resolve_many(
                owner_email=str(payload.get("owner_email") or ""),
                session_id=str(payload.get("session_id") or ""),
                attachment_ids=[str(item or "") for item in (payload.get("attachment_ids") or [])],
            )
            return jsonify({"status": "ok", "attachments": attachments})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/local-agent/source-code-qa/attachments/get")
    def source_code_qa_attachment_get():
        payload = request.get_json(silent=True) or {}
        try:
            metadata, content = _build_source_code_qa_attachment_store(settings).get_bytes(
                owner_email=str(payload.get("owner_email") or ""),
                session_id=str(payload.get("session_id") or ""),
                attachment_id=str(payload.get("attachment_id") or ""),
            )
            return jsonify(
                {
                    "status": "ok",
                    "attachment": metadata,
                    "content_base64": base64.b64encode(content).decode("ascii"),
                }
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.NOT_FOUND

    @app.post("/api/local-agent/source-code-qa/model-availability/get")
    def source_code_qa_model_availability_get():
        return jsonify({"status": "ok", "availability": _build_source_code_qa_model_availability_store(settings).get()})

    @app.post("/api/local-agent/source-code-qa/model-availability/save")
    def source_code_qa_model_availability_save():
        payload = request.get_json(silent=True) or {}
        availability = _build_source_code_qa_model_availability_store(settings).save(
            payload.get("availability") if isinstance(payload.get("availability"), dict) else {}
        )
        return jsonify({"status": "ok", "availability": availability})

    @app.post("/api/local-agent/source-code-qa/runtime-evidence/list")
    def source_code_qa_runtime_evidence_list():
        payload = request.get_json(silent=True) or {}
        try:
            evidence = _build_source_code_qa_runtime_evidence_store(settings).list(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
            )
            return jsonify({"status": "ok", "evidence": evidence})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/local-agent/source-code-qa/runtime-evidence/save")
    def source_code_qa_runtime_evidence_save():
        payload = request.get_json(silent=True) or {}
        try:
            content = base64.b64decode(str(payload.get("content_base64") or "").encode("ascii"))
            evidence = _build_source_code_qa_runtime_evidence_store(settings).save_bytes(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
                source_type=str(payload.get("source_type") or ""),
                uploaded_by=str(payload.get("uploaded_by") or ""),
                filename=str(payload.get("filename") or "runtime-evidence"),
                mime_type=str(payload.get("mime_type") or ""),
                content=content,
            )
            return jsonify({"status": "ok", "evidence": evidence})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/local-agent/source-code-qa/runtime-evidence/resolve")
    def source_code_qa_runtime_evidence_resolve():
        payload = request.get_json(silent=True) or {}
        try:
            evidence = _build_source_code_qa_runtime_evidence_store(settings).resolve_scope(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
            )
            return jsonify({"status": "ok", "evidence": evidence})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/local-agent/source-code-qa/runtime-evidence/delete")
    def source_code_qa_runtime_evidence_delete():
        payload = request.get_json(silent=True) or {}
        try:
            deleted = _build_source_code_qa_runtime_evidence_store(settings).delete(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
                evidence_id=str(payload.get("evidence_id") or ""),
            )
            return jsonify({"status": "ok", "deleted": deleted})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/local-agent/seatalk/overview")
    def seatalk_overview():
        return jsonify({"status": "ok", **_build_seatalk_service(settings).build_overview()})

    @app.post("/api/local-agent/seatalk/insights")
    def seatalk_insights():
        payload = request.get_json(silent=True) or {}
        with _seatalk_name_overrides(payload.get("name_mappings")) as name_overrides_path:
            return jsonify(
                {
                    "status": "ok",
                    **_build_seatalk_service(settings, name_overrides_path=name_overrides_path).build_insights(
                        todo_since=str(payload.get("todo_since") or "")
                    ),
                }
            )

    @app.post("/api/local-agent/seatalk/project-updates")
    def seatalk_project_updates():
        payload = request.get_json(silent=True) or {}
        with _seatalk_name_overrides(payload.get("name_mappings")) as name_overrides_path:
            return jsonify(
                {
                    "status": "ok",
                    **_build_seatalk_service(settings, name_overrides_path=name_overrides_path).build_project_updates(),
                }
            )

    @app.post("/api/local-agent/seatalk/todos")
    def seatalk_todos():
        payload = request.get_json(silent=True) or {}
        with _seatalk_name_overrides(payload.get("name_mappings")) as name_overrides_path:
            return jsonify(
                {
                    "status": "ok",
                    **_build_seatalk_service(settings, name_overrides_path=name_overrides_path).build_todos(
                        todo_since=str(payload.get("todo_since") or "")
                    ),
                }
            )

    @app.post("/api/local-agent/seatalk/name-mappings")
    def seatalk_name_mappings():
        return jsonify({"status": "ok", **_build_seatalk_service(settings).build_name_mappings()})

    @app.post("/api/local-agent/seatalk/todos/completed-ids")
    def seatalk_todos_completed_ids():
        payload = request.get_json(silent=True) or {}
        completed = sorted(_build_seatalk_todo_store(settings).completed_ids(owner_email=str(payload.get("owner_email") or "")))
        return jsonify({"status": "ok", "completed_ids": completed})

    @app.post("/api/local-agent/seatalk/todos/open")
    def seatalk_todos_open():
        payload = request.get_json(silent=True) or {}
        todos = _build_seatalk_todo_store(settings).open_todos(owner_email=str(payload.get("owner_email") or ""))
        return jsonify({"status": "ok", "todos": todos})

    @app.post("/api/local-agent/seatalk/todos/processed-until")
    def seatalk_todos_processed_until():
        payload = request.get_json(silent=True) or {}
        processed_until = _build_seatalk_todo_store(settings).processed_until(owner_email=str(payload.get("owner_email") or ""))
        return jsonify({"status": "ok", "processed_until": processed_until})

    @app.post("/api/local-agent/seatalk/todos/mark-processed-until")
    def seatalk_todos_mark_processed_until():
        payload = request.get_json(silent=True) or {}
        _build_seatalk_todo_store(settings).mark_processed_until(
            owner_email=str(payload.get("owner_email") or ""),
            processed_until=str(payload.get("processed_until") or ""),
        )
        return jsonify({"status": "ok"})

    @app.post("/api/local-agent/seatalk/todos/merge-open")
    def seatalk_todos_merge_open():
        payload = request.get_json(silent=True) or {}
        todos = payload.get("todos") if isinstance(payload.get("todos"), list) else []
        merged = _build_seatalk_todo_store(settings).merge_open_todos(
            owner_email=str(payload.get("owner_email") or ""),
            todos=[todo for todo in todos if isinstance(todo, dict)],
        )
        return jsonify({"status": "ok", "todos": merged})

    @app.post("/api/local-agent/seatalk/todos/complete")
    def seatalk_todo_complete():
        payload = request.get_json(silent=True) or {}
        result = _build_seatalk_todo_store(settings).mark_completed(
            owner_email=str(payload.get("owner_email") or ""),
            todo=payload.get("todo") if isinstance(payload.get("todo"), dict) else {},
        )
        return jsonify(result)

    @app.post("/api/local-agent/seatalk/name-mappings/store/get")
    def seatalk_name_mapping_store_get():
        return jsonify({"status": "ok", "mappings": _build_seatalk_name_mapping_store(settings).mappings()})

    @app.post("/api/local-agent/seatalk/name-mappings/store/merge")
    def seatalk_name_mapping_store_merge():
        payload = request.get_json(silent=True) or {}
        mappings = _build_seatalk_name_mapping_store(settings).merge_mappings(
            payload.get("mappings") if isinstance(payload.get("mappings"), dict) else {}
        )
        return jsonify({"status": "ok", "mappings": mappings})

    @app.post("/api/local-agent/seatalk/export")
    def seatalk_export():
        payload = request.get_json(silent=True) or {}
        with _seatalk_name_overrides(payload.get("name_mappings")) as name_overrides_path:
            content, filename = _build_seatalk_service(settings, name_overrides_path=name_overrides_path).export_history_text()
        return jsonify({"status": "ok", "content": content, "filename": filename})

    @app.post("/api/local-agent/bpmis/call")
    def bpmis_call():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        operation = str(payload.get("operation") or "").strip()
        if operation not in BPMIS_PROXY_OPERATIONS:
            raise ToolError("Unsupported BPMIS local-agent operation.")
        args = payload.get("args") if isinstance(payload.get("args"), list) else []
        kwargs = payload.get("kwargs") if isinstance(payload.get("kwargs"), dict) else {}
        client = BPMISDirectApiClient(settings, access_token=str(payload.get("access_token") or "").strip() or None)
        result = getattr(client, operation)(*_deserialize_bpmis_args(operation, args), **kwargs)
        return jsonify({"status": "ok", "result": _serialize_bpmis_result(result)})

    @app.post("/api/local-agent/bpmis/config/load")
    def bpmis_config_load():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        user_key = str(payload.get("user_key") or "").strip()
        if not user_key:
            return jsonify({"status": "error", "message": "user_key is required."}), HTTPStatus.BAD_REQUEST
        config = _build_config_store(settings).load(user_key)
        return jsonify({"status": "ok", "config": config})

    @app.post("/api/local-agent/bpmis/config/save")
    def bpmis_config_save():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        user_key = str(payload.get("user_key") or "").strip()
        config = payload.get("config")
        if not user_key:
            return jsonify({"status": "error", "message": "user_key is required."}), HTTPStatus.BAD_REQUEST
        if not isinstance(config, dict):
            return jsonify({"status": "error", "message": "config must be an object."}), HTTPStatus.BAD_REQUEST
        saved = _build_config_store(settings).save(config, user_key)
        return jsonify({"status": "ok", "config": saved})

    @app.post("/api/local-agent/bpmis/config/migrate")
    def bpmis_config_migrate():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        from_user_key = str(payload.get("from_user_key") or "").strip()
        to_user_key = str(payload.get("to_user_key") or "").strip()
        if not from_user_key or not to_user_key:
            return jsonify({"status": "error", "message": "from_user_key and to_user_key are required."}), HTTPStatus.BAD_REQUEST
        _build_config_store(settings).migrate(from_user_key, to_user_key)
        return jsonify({"status": "ok"})

    @app.post("/api/local-agent/bpmis/team-profiles/load")
    def bpmis_team_profiles_load():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        return jsonify({"status": "ok", "profiles": _build_config_store(settings).load_team_profiles()})

    @app.post("/api/local-agent/bpmis/team-profiles/save")
    def bpmis_team_profile_save():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        team_key = str(payload.get("team_key") or "").strip()
        profile = payload.get("profile")
        if not team_key:
            return jsonify({"status": "error", "message": "team_key is required."}), HTTPStatus.BAD_REQUEST
        if not isinstance(profile, dict):
            return jsonify({"status": "error", "message": "profile must be an object."}), HTTPStatus.BAD_REQUEST
        saved = _build_config_store(settings).save_team_profile(team_key, profile)
        return jsonify({"status": "ok", "profile": saved})

    @app.post("/api/local-agent/team-dashboard/config/load")
    def team_dashboard_config_load():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        return jsonify({"status": "ok", "config": _build_team_dashboard_config_store(settings).load()})

    @app.post("/api/local-agent/team-dashboard/config/save")
    def team_dashboard_config_save():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        config = payload.get("config")
        if not isinstance(config, dict):
            return jsonify({"status": "error", "message": "config must be an object."}), HTTPStatus.BAD_REQUEST
        saved = _build_team_dashboard_config_store(settings).save(config)
        return jsonify({"status": "ok", "config": saved})

    @app.post("/api/local-agent/bpmis/projects/list")
    def bpmis_projects_list():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        user_key = str(payload.get("user_key") or "").strip()
        if not user_key:
            return jsonify({"status": "error", "message": "user_key is required."}), HTTPStatus.BAD_REQUEST
        return jsonify({"status": "ok", "projects": _build_bpmis_project_store(settings).list_projects(user_key=user_key)})

    @app.post("/api/local-agent/bpmis/projects/reorder")
    def bpmis_projects_reorder():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        user_key = str(payload.get("user_key") or "").strip()
        if not user_key:
            return jsonify({"status": "error", "message": "user_key is required."}), HTTPStatus.BAD_REQUEST
        bpmis_ids = payload.get("bpmis_ids") if isinstance(payload.get("bpmis_ids"), list) else []
        projects = _build_bpmis_project_store(settings).reorder_projects(
            user_key=user_key,
            bpmis_ids=[str(item or "") for item in bpmis_ids],
        )
        return jsonify({"status": "ok", "projects": projects, "scope": "portal_only"})

    @app.post("/api/local-agent/bpmis/projects/upsert")
    def bpmis_project_upsert():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        result = _build_bpmis_project_store(settings).upsert_project(
            user_key=str(payload.get("user_key") or ""),
            bpmis_id=str(payload.get("bpmis_id") or ""),
            project_name=str(payload.get("project_name") or ""),
            brd_link=str(payload.get("brd_link") or ""),
            market=str(payload.get("market") or ""),
        )
        return jsonify({"status": "ok", "result": result})

    @app.post("/api/local-agent/bpmis/projects/delete")
    def bpmis_project_delete():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        deleted = _build_bpmis_project_store(settings).soft_delete_project(
            user_key=str(payload.get("user_key") or ""),
            bpmis_id=str(payload.get("bpmis_id") or ""),
        )
        return jsonify({"status": "ok", "deleted": deleted, "scope": "portal_only"})

    @app.post("/api/local-agent/bpmis/projects/comment")
    def bpmis_project_comment():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        updated = _build_bpmis_project_store(settings).update_project_comment(
            user_key=str(payload.get("user_key") or ""),
            bpmis_id=str(payload.get("bpmis_id") or ""),
            pm_comment=str(payload.get("pm_comment") or ""),
        )
        return jsonify({"status": "ok", "updated": updated, "scope": "portal_only"})

    @app.post("/api/local-agent/bpmis/projects/jira-tickets/add")
    def bpmis_project_ticket_add():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        ticket = _build_bpmis_project_store(settings).add_jira_ticket(
            user_key=str(payload.get("user_key") or ""),
            bpmis_id=str(payload.get("bpmis_id") or ""),
            component=str(payload.get("component") or ""),
            market=str(payload.get("market") or ""),
            system=str(payload.get("system") or ""),
            jira_title=str(payload.get("jira_title") or ""),
            prd_link=str(payload.get("prd_link") or ""),
            description=str(payload.get("description") or ""),
            fix_version_name=str(payload.get("fix_version_name") or ""),
            fix_version_id=str(payload.get("fix_version_id") or ""),
            ticket_key=str(payload.get("ticket_key") or ""),
            ticket_link=str(payload.get("ticket_link") or ""),
            status=str(payload.get("status") or "created"),
            message=str(payload.get("message") or ""),
            raw_response=payload.get("raw_response") if isinstance(payload.get("raw_response"), dict) else {},
        )
        return jsonify({"status": "ok", "ticket": ticket})

    @app.post("/api/local-agent/bpmis/projects/jira-tickets/upsert-synced")
    def bpmis_project_ticket_upsert_synced():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        ticket = _build_bpmis_project_store(settings).upsert_synced_jira_ticket(
            user_key=str(payload.get("user_key") or ""),
            bpmis_id=str(payload.get("bpmis_id") or ""),
            component=str(payload.get("component") or ""),
            market=str(payload.get("market") or ""),
            system=str(payload.get("system") or ""),
            jira_title=str(payload.get("jira_title") or ""),
            prd_link=str(payload.get("prd_link") or ""),
            description=str(payload.get("description") or ""),
            fix_version_name=str(payload.get("fix_version_name") or ""),
            fix_version_id=str(payload.get("fix_version_id") or ""),
            ticket_key=str(payload.get("ticket_key") or ""),
            ticket_link=str(payload.get("ticket_link") or ""),
            status=str(payload.get("status") or "synced"),
            message=str(payload.get("message") or ""),
            raw_response=payload.get("raw_response") if isinstance(payload.get("raw_response"), dict) else {},
        )
        return jsonify({"status": "ok", "ticket": ticket})

    @app.post("/api/local-agent/bpmis/projects/jira-tickets/delete")
    def bpmis_project_ticket_delete():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        deleted = _build_bpmis_project_store(settings).delete_jira_ticket(
            user_key=str(payload.get("user_key") or ""),
            bpmis_id=str(payload.get("bpmis_id") or ""),
            ticket_id=payload.get("ticket_id") or "",
        )
        return jsonify({"status": "ok", "deleted": deleted, "scope": "portal_only"})

    @app.post("/api/local-agent/bpmis/projects/jira-tickets/status")
    def bpmis_project_ticket_status():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        updated = _build_bpmis_project_store(settings).update_jira_ticket_status(
            user_key=str(payload.get("user_key") or ""),
            bpmis_id=str(payload.get("bpmis_id") or ""),
            ticket_id=payload.get("ticket_id") or "",
            status=str(payload.get("status") or ""),
        )
        return jsonify({"status": "ok", "updated": updated})

    @app.post("/api/local-agent/bpmis/projects/jira-tickets/version")
    def bpmis_project_ticket_version():
        if not settings.local_agent_bpmis_enabled:
            raise ToolError("BPMIS local-agent proxy is disabled.")
        payload = request.get_json(silent=True) or {}
        updated = _build_bpmis_project_store(settings).update_jira_ticket_version(
            user_key=str(payload.get("user_key") or ""),
            bpmis_id=str(payload.get("bpmis_id") or ""),
            ticket_id=payload.get("ticket_id") or "",
            version_name=str(payload.get("version_name") or ""),
            version_id=str(payload.get("version_id") or ""),
        )
        return jsonify({"status": "ok", "updated": updated})

    @app.errorhandler(ToolError)
    def handle_tool_error(error: ToolError):
        return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.errorhandler(Exception)
    def handle_unexpected_error(error: Exception):
        current_app.logger.exception("Local-agent request failed unexpectedly.")
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"Mac local-agent failed unexpectedly: {error}",
                }
            ),
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    return app


def _queue_source_code_qa_auto_sync(service: SourceCodeQAService, *, pm_team: str, country: str) -> dict[str, Any]:
    key = service.mapping_key(pm_team, country)
    logger = current_app.logger
    with _SOURCE_CODE_QA_AUTO_SYNC_LOCK:
        if key in _SOURCE_CODE_QA_AUTO_SYNC_KEYS:
            return {
                "attempted": False,
                "status": "background_running",
                "reason": "repository freshness check is already running in the Mac local-agent",
                "key": key,
            }
        _SOURCE_CODE_QA_AUTO_SYNC_KEYS.add(key)

    def run() -> None:
        try:
            service.ensure_synced_today(pm_team=pm_team, country=country)
        except Exception:
            logger.exception("Source Code Q&A background auto-sync failed for %s.", key)
        finally:
            with _SOURCE_CODE_QA_AUTO_SYNC_LOCK:
                _SOURCE_CODE_QA_AUTO_SYNC_KEYS.discard(key)

    threading.Thread(target=run, daemon=True).start()
    return {
        "attempted": False,
        "status": "background_queued",
        "reason": "repository freshness check queued in the Mac local-agent",
        "key": key,
    }


def _deserialize_bpmis_args(operation: str, args: list[Any]) -> list[Any]:
    if operation != "create_jira_ticket" or not args or not isinstance(args[0], dict):
        return args
    project_payload = args[0]
    return [
        ProjectMatch(
            project_id=str(project_payload.get("project_id") or ""),
            raw=project_payload.get("raw") if isinstance(project_payload.get("raw"), dict) else {},
        ),
        *args[1:],
    ]


def _serialize_bpmis_result(result: Any) -> Any:
    if is_dataclass(result):
        return asdict(result)
    if isinstance(result, list):
        return [_serialize_bpmis_result(item) for item in result]
    if isinstance(result, tuple):
        return [_serialize_bpmis_result(item) for item in result]
    if isinstance(result, dict):
        return {str(key): _serialize_bpmis_result(value) for key, value in result.items()}
    return result


def _update_query_job(job_id: str, **fields: Any) -> None:
    jobs = current_app.config["SOURCE_CODE_QA_QUERY_JOBS"]
    lock = current_app.config["SOURCE_CODE_QA_QUERY_JOBS_LOCK"]
    with lock:
        _cleanup_query_jobs_locked(jobs)
        snapshot = dict(jobs.get(job_id) or {})
        snapshot.update(fields)
        snapshot["updated_at"] = time.time()
        jobs[job_id] = snapshot


def _snapshot_query_job(job_id: str) -> dict[str, Any] | None:
    jobs = current_app.config["SOURCE_CODE_QA_QUERY_JOBS"]
    lock = current_app.config["SOURCE_CODE_QA_QUERY_JOBS_LOCK"]
    with lock:
        _cleanup_query_jobs_locked(jobs)
        snapshot = jobs.get(job_id)
        return dict(snapshot) if isinstance(snapshot, dict) else None


def _cleanup_query_jobs_locked(jobs: dict[str, Any]) -> None:
    cutoff = time.time() - _SOURCE_CODE_QA_QUERY_JOB_TTL_SECONDS
    expired = [
        job_id
        for job_id, snapshot in jobs.items()
        if isinstance(snapshot, dict)
        and str(snapshot.get("state") or "") in {"completed", "failed"}
        and _query_job_updated_at(snapshot) < cutoff
    ]
    for job_id in expired:
        jobs.pop(job_id, None)


def _query_job_updated_at(snapshot: dict[str, Any]) -> float:
    try:
        return float(snapshot.get("updated_at") or 0)
    except (TypeError, ValueError):
        return 0.0


def _run_source_code_qa_query_job(app: Flask, job_id: str, payload: dict[str, Any]) -> None:
    with app.app_context():
        def progress_callback(stage: str, message: str, current: int, total: int) -> None:
            _update_query_job(
                job_id,
                state="running",
                stage=stage,
                message=message,
                current=current,
                total=total,
            )

        try:
            progress_callback("starting", "Starting Source Code Q&A query on Mac local-agent.", 0, 0)
            service = _source_code_qa_service(payload.get("llm_provider"))
            result = service.query(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
                question=str(payload.get("question") or ""),
                answer_mode=str(payload.get("answer_mode") or "auto"),
                llm_budget_mode=str(payload.get("llm_budget_mode") or "auto"),
                conversation_context=payload.get("conversation_context") if isinstance(payload.get("conversation_context"), dict) else None,
                attachments=payload.get("attachments") if isinstance(payload.get("attachments"), list) else None,
                runtime_evidence=payload.get("runtime_evidence") if isinstance(payload.get("runtime_evidence"), list) else None,
                progress_callback=progress_callback,
            )
            _update_query_job(
                job_id,
                state="completed",
                stage="completed",
                message="Source Code Q&A query completed on Mac local-agent.",
                current=1,
                total=1,
                result={"status": "ok", **result},
            )
        except ToolError as error:
            _update_query_job(
                job_id,
                state="failed",
                stage="failed",
                message=str(error),
                error=str(error),
                current=0,
                total=0,
            )
        except Exception as error:  # noqa: BLE001 - keep async status JSON-readable for Cloud Run.
            current_app.logger.exception("Source Code Q&A local-agent async job failed unexpectedly.")
            _update_query_job(
                job_id,
                state="failed",
                stage="failed",
                message=f"Mac local-agent Source Code Q&A job failed unexpectedly: {error}",
                error=f"Mac local-agent Source Code Q&A job failed unexpectedly: {error}",
                current=0,
                total=0,
            )


def _source_code_qa_service(llm_provider: str | None = None) -> SourceCodeQAService:
    service: SourceCodeQAService = current_app.config["SOURCE_CODE_QA_SERVICE"]
    return service.with_llm_provider(str(llm_provider or "")) if llm_provider else service


def _build_config_store(settings: Settings) -> WebConfigStore:
    data_root = settings.team_portal_data_dir
    if not data_root.is_absolute():
        data_root = (Path(__file__).resolve().parent.parent / data_root).resolve()
    return WebConfigStore(
        data_root,
        legacy_root=Path(__file__).resolve().parent.parent,
        encryption_key=settings.team_portal_config_encryption_key,
    )


def _build_team_dashboard_config_store(settings: Settings):
    from bpmis_jira_tool.web import TeamDashboardConfigStore

    return TeamDashboardConfigStore(_build_config_store(settings).db_path)


def _build_bpmis_project_store(settings: Settings) -> BPMISProjectStore:
    return BPMISProjectStore(_build_config_store(settings).db_path)


def _data_root(settings: Settings) -> Path:
    data_root = settings.team_portal_data_dir
    if not data_root.is_absolute():
        data_root = (Path(__file__).resolve().parent.parent / data_root).resolve()
    return data_root


def _build_source_code_qa_session_store(settings: Settings) -> SourceCodeQASessionStore:
    return SourceCodeQASessionStore(_data_root(settings) / "source_code_qa" / "sessions.json")


def _build_source_code_qa_attachment_store(settings: Settings) -> SourceCodeQAAttachmentStore:
    return SourceCodeQAAttachmentStore(_data_root(settings) / "source_code_qa" / "attachments")


def _build_source_code_qa_runtime_evidence_store(settings: Settings) -> SourceCodeQARuntimeEvidenceStore:
    return SourceCodeQARuntimeEvidenceStore(_data_root(settings) / "source_code_qa" / "runtime_evidence")


def _build_source_code_qa_model_availability_store(settings: Settings) -> SourceCodeQAModelAvailabilityStore:
    return SourceCodeQAModelAvailabilityStore(_data_root(settings) / "source_code_qa" / "model_availability.json")


def _build_seatalk_todo_store(settings: Settings) -> SeaTalkTodoStore:
    return SeaTalkTodoStore(_data_root(settings) / "seatalk" / "completed_todos.json")


def _build_seatalk_name_mapping_store(settings: Settings) -> SeaTalkNameMappingStore:
    return SeaTalkNameMappingStore(_data_root(settings) / "seatalk" / "name_overrides.json")


def _build_source_code_qa_service(settings: Settings) -> SourceCodeQAService:
    data_root = settings.team_portal_data_dir
    if not data_root.is_absolute():
        data_root = (Path(__file__).resolve().parent.parent / data_root).resolve()
    return SourceCodeQAService(
        data_root=data_root,
        team_profiles=TEAM_PROFILE_DEFAULTS,
        gitlab_token=settings.source_code_qa_gitlab_token,
        gitlab_username=settings.source_code_qa_gitlab_username,
        llm_provider=settings.source_code_qa_llm_provider,
        gemini_api_key=settings.source_code_qa_gemini_api_key,
        gemini_api_base_url=settings.source_code_qa_gemini_api_base_url,
        openai_api_key=settings.source_code_qa_openai_api_key,
        openai_api_base_url=settings.source_code_qa_openai_api_base_url,
        openai_model=settings.source_code_qa_openai_model,
        openai_fast_model=settings.source_code_qa_openai_fast_model,
        openai_deep_model=settings.source_code_qa_openai_deep_model,
        openai_fallback_model=settings.source_code_qa_openai_fallback_model,
        gemini_model=settings.source_code_qa_gemini_model,
        gemini_fast_model=settings.source_code_qa_gemini_fast_model,
        gemini_deep_model=settings.source_code_qa_gemini_deep_model,
        gemini_fallback_model=settings.source_code_qa_gemini_fallback_model,
        vertex_credentials_file=settings.source_code_qa_vertex_credentials_file,
        vertex_project_id=settings.source_code_qa_vertex_project_id,
        vertex_location=settings.source_code_qa_vertex_location,
        vertex_model=settings.source_code_qa_vertex_model,
        vertex_fast_model=settings.source_code_qa_vertex_fast_model,
        vertex_deep_model=settings.source_code_qa_vertex_deep_model,
        vertex_fallback_model=settings.source_code_qa_vertex_fallback_model,
        query_rewrite_model=settings.source_code_qa_query_rewrite_model,
        planner_model=settings.source_code_qa_planner_model,
        answer_model=settings.source_code_qa_answer_model,
        judge_model=settings.source_code_qa_judge_model,
        repair_model=settings.source_code_qa_repair_model,
        llm_judge_enabled=settings.source_code_qa_llm_judge_enabled,
        semantic_index_model=settings.source_code_qa_embedding_model,
        semantic_index_enabled=settings.source_code_qa_semantic_index_enabled,
        embedding_provider=settings.source_code_qa_embedding_provider,
        embedding_api_key=settings.source_code_qa_embedding_api_key,
        embedding_api_base_url=settings.source_code_qa_embedding_api_base_url,
        llm_cache_ttl_seconds=settings.source_code_qa_llm_cache_ttl_seconds,
        llm_timeout_seconds=settings.source_code_qa_llm_timeout_seconds,
        codex_timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        codex_concurrency=settings.source_code_qa_codex_concurrency,
        codex_top_path_limit=settings.source_code_qa_codex_top_path_limit,
        codex_repair_enabled=settings.source_code_qa_codex_repair_enabled,
        codex_session_mode=settings.source_code_qa_codex_session_mode,
        codex_session_max_turns=settings.source_code_qa_codex_session_max_turns,
        codex_fast_path_enabled=settings.source_code_qa_codex_fast_path_enabled,
        codex_cache_followups=settings.source_code_qa_codex_cache_followups,
        llm_max_retries=settings.source_code_qa_llm_max_retries,
        llm_backoff_seconds=settings.source_code_qa_llm_backoff_seconds,
        llm_max_backoff_seconds=settings.source_code_qa_llm_max_backoff_seconds,
        git_timeout_seconds=settings.source_code_qa_git_timeout_seconds,
        max_file_bytes=settings.source_code_qa_max_file_bytes,
    )


def _build_seatalk_service(settings: Settings, *, name_overrides_path: str | Path | None = None) -> SeaTalkDashboardService:
    daily_cache_dir = _data_root(settings) / "seatalk" / "cache"
    return SeaTalkDashboardService(
        owner_email=settings.seatalk_owner_email,
        seatalk_app_path=settings.seatalk_local_app_path,
        seatalk_data_dir=settings.seatalk_local_data_dir,
        codex_workspace_root=Path(__file__).resolve().parent.parent,
        codex_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
        codex_timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        codex_concurrency=settings.source_code_qa_codex_concurrency,
        name_overrides_path=name_overrides_path,
        daily_cache_dir=daily_cache_dir,
    )


def _seatalk_configured(settings: Settings) -> bool:
    data_dir = Path(str(settings.seatalk_local_data_dir or "")).expanduser()
    app_path = Path(str(settings.seatalk_local_app_path or "")).expanduser()
    return bool(app_path.exists() and data_dir.exists() and (data_dir / "config.json").exists())


class _seatalk_name_overrides:
    def __init__(self, mappings: Any) -> None:
        self.mappings = mappings if isinstance(mappings, dict) else {}
        self.temp_file: tempfile.NamedTemporaryFile[str] | None = None

    def __enter__(self) -> str | None:
        if not self.mappings:
            return None
        self.temp_file = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False)
        json.dump({"mappings": self.mappings}, self.temp_file, ensure_ascii=False, sort_keys=True)
        self.temp_file.close()
        return self.temp_file.name

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.temp_file is not None:
            try:
                Path(self.temp_file.name).unlink()
            except OSError:
                pass
