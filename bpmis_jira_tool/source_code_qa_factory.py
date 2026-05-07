from __future__ import annotations

from pathlib import Path

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.source_code_qa import SourceCodeQAService
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS


def source_code_qa_data_root(settings: Settings) -> Path:
    data_root = settings.team_portal_data_dir
    if not data_root.is_absolute():
        data_root = (Path(__file__).resolve().parent.parent / data_root).resolve()
    return data_root


def build_source_code_qa_service_from_settings(settings: Settings) -> SourceCodeQAService:
    return SourceCodeQAService(
        data_root=source_code_qa_data_root(settings),
        team_profiles=TEAM_PROFILE_DEFAULTS,
        gitlab_token=settings.source_code_qa_gitlab_token,
        gitlab_username=settings.source_code_qa_gitlab_username,
        llm_provider=settings.source_code_qa_llm_provider,
        query_rewrite_model=settings.source_code_qa_query_rewrite_model,
        planner_model=settings.source_code_qa_planner_model,
        answer_model=settings.source_code_qa_answer_model,
        repair_model=settings.source_code_qa_repair_model,
        semantic_index_model=settings.source_code_qa_embedding_model,
        semantic_index_enabled=settings.source_code_qa_semantic_index_enabled,
        llm_cache_ttl_seconds=settings.source_code_qa_llm_cache_ttl_seconds,
        llm_timeout_seconds=settings.source_code_qa_llm_timeout_seconds,
        codex_timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        codex_concurrency=settings.source_code_qa_codex_concurrency,
        codex_top_path_limit=settings.source_code_qa_codex_top_path_limit,
        codex_repair_enabled=settings.source_code_qa_codex_repair_enabled,
        codex_session_mode=settings.source_code_qa_codex_session_mode,
        codex_session_max_turns=settings.source_code_qa_codex_session_max_turns,
        codex_cache_followups=settings.source_code_qa_codex_cache_followups,
        git_timeout_seconds=settings.source_code_qa_git_timeout_seconds,
        max_file_bytes=settings.source_code_qa_max_file_bytes,
    )
