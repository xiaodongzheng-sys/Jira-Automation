from __future__ import annotations

import json
import os
import tempfile
from http import HTTPStatus
from pathlib import Path
from typing import Any

from flask import Flask, current_app, jsonify, request

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_protocol import NONCE_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, verify_signature
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService
from bpmis_jira_tool.source_code_qa import SourceCodeQAService
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS


def create_local_agent_app() -> Flask:
    settings = Settings.from_env()
    app = Flask(__name__)
    app.config["SETTINGS"] = settings
    app.config["SOURCE_CODE_QA_SERVICE"] = _build_source_code_qa_service(settings)

    @app.before_request
    def verify_local_agent_signature():
        if request.path == "/healthz":
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
        result = _source_code_qa_service().ensure_synced_today(
            pm_team=str(payload.get("pm_team") or ""),
            country=str(payload.get("country") or ""),
        )
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
        )
        return jsonify({"status": "ok", **result})

    @app.post("/api/local-agent/seatalk/overview")
    def seatalk_overview():
        return jsonify({"status": "ok", **_build_seatalk_service(settings).build_overview()})

    @app.post("/api/local-agent/seatalk/insights")
    def seatalk_insights():
        payload = request.get_json(silent=True) or {}
        with _seatalk_name_overrides(payload.get("name_mappings")) as name_overrides_path:
            return jsonify({"status": "ok", **_build_seatalk_service(settings, name_overrides_path=name_overrides_path).build_insights()})

    @app.post("/api/local-agent/seatalk/name-mappings")
    def seatalk_name_mappings():
        return jsonify({"status": "ok", **_build_seatalk_service(settings).build_name_mappings()})

    @app.post("/api/local-agent/seatalk/export")
    def seatalk_export():
        payload = request.get_json(silent=True) or {}
        with _seatalk_name_overrides(payload.get("name_mappings")) as name_overrides_path:
            content, filename = _build_seatalk_service(settings, name_overrides_path=name_overrides_path).export_history_text()
        return jsonify({"status": "ok", "content": content, "filename": filename})

    @app.errorhandler(ToolError)
    def handle_tool_error(error: ToolError):
        return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    return app


def _source_code_qa_service(llm_provider: str | None = None) -> SourceCodeQAService:
    service: SourceCodeQAService = current_app.config["SOURCE_CODE_QA_SERVICE"]
    return service.with_llm_provider(str(llm_provider or "")) if llm_provider else service


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
    return SeaTalkDashboardService(
        owner_email=settings.seatalk_owner_email,
        seatalk_app_path=settings.seatalk_local_app_path,
        seatalk_data_dir=settings.seatalk_local_data_dir,
        codex_workspace_root=Path(__file__).resolve().parent.parent,
        codex_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
        codex_timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        codex_concurrency=settings.source_code_qa_codex_concurrency,
        name_overrides_path=name_overrides_path,
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
