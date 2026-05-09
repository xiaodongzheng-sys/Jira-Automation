from __future__ import annotations

import ast
from datetime import date, datetime, timedelta, timezone
import functools
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import time
from typing import Any
from urllib.parse import urlsplit, urlunsplit
import uuid


from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa_evidence_policy import (
    ANSWER_CONCRETE_SOURCE_HINTS,
    ANSWER_POLICY_REGISTRY,
    API_HINTS,
    CONFIG_HINTS,
    CONCRETE_SOURCE_HINTS,
    DATA_CARRIER_SUFFIXES,
    DATA_SOURCE_HINTS,
    DEPENDENCY_PATH_HINTS,
    DEPENDENCY_QUESTION_TERMS,
    DEPENDENCY_SYMBOL_SUFFIXES,
    ERROR_HINTS,
    FIELD_POPULATION_HINTS,
    IMPACT_ANALYSIS_HINTS,
    LOW_VALUE_CALL_SYMBOLS,
    LOW_VALUE_FOCUS_TERMS,
    MODULE_DEPENDENCY_HINTS,
    QUALITY_GATE_TRACE_STAGE,
    RULE_HINTS,
    STATIC_QA_RULES,
    TEST_COVERAGE_HINTS,
    TOOL_LOOP_TRACE_PREFIX,
)
from bpmis_jira_tool.source_code_qa_cache_telemetry import attach_cache_telemetry_helpers
from bpmis_jira_tool.source_code_qa_components import (
    SourceCodeQAAnswerGenerationComponent,
    SourceCodeQAQualityJudgeComponent,
    SourceCodeQARetrievalComponent,
)
from bpmis_jira_tool.source_code_qa_indexing import attach_indexing_helpers
from bpmis_jira_tool.source_code_qa_structure import attach_structure_helpers
from bpmis_jira_tool.source_code_qa_retrieval_tools import attach_retrieval_tool_helpers
from bpmis_jira_tool.source_code_qa_retrieval_cache import attach_retrieval_cache_helpers
from bpmis_jira_tool.source_code_qa_codex_refs import (
    codex_candidate_path_layers,
    codex_repo_relative_root,
    extract_direct_file_refs,
    resolve_codex_file_ref,
)
from bpmis_jira_tool.source_code_qa_codex_prompts import (
    build_codex_payload,
    build_codex_repair_brief,
    build_codex_sql_generation_brief,
    codex_system_instruction,
)
from bpmis_jira_tool.source_code_qa_llm_providers import (
    CodexCliBridgeSourceCodeQALLMProvider,
    LLM_PROVIDER_ALLOWED_QUERY_CHOICES,
    LLM_PROVIDER_CODEX_CLI_BRIDGE,
    SourceCodeQALLMProvider,
)
from bpmis_jira_tool.source_code_qa_match_grading import (
    evidence_role,
    match_answer_grade,
    match_is_definition_only,
)
from bpmis_jira_tool.source_code_qa_patterns import (
    HTTPS_URL_PATTERN,
    IDENTIFIER_PATTERN,
    CLASS_DEF_PATTERN,
    JAVA_PACKAGE_PATTERN,
    JAVA_IMPORT_PATTERN,
    PY_DEF_PATTERN,
    JS_DEF_PATTERN,
    JAVA_METHOD_DEF_PATTERN,
    SETTER_CALL_PATTERN,
    BUILDER_FIELD_PATTERN,
    ASSIGNMENT_PATTERN,
    ANNOTATION_ROUTE_PATTERN,
    FEIGN_CLIENT_PATTERN,
    MYBATIS_NAMESPACE_PATTERN,
    MYBATIS_STATEMENT_PATTERN,
    MYBATIS_RESULT_MAP_PATTERN,
    MYBATIS_INCLUDE_PATTERN,
    MYBATIS_ATTR_REFERENCE_PATTERN,
    HTTP_LITERAL_PATTERN,
    SQL_TABLE_PATTERN,
    SQL_READ_TABLE_PATTERN,
    SQL_WRITE_TABLE_PATTERN,
    EXACT_LOOKUP_TERM_PATTERN,
    PROPERTIES_KEY_PATTERN,
    CONFIG_ASSIGNMENT_PATTERN,
    CONFIG_PLACEHOLDER_PATTERN,
    SPRING_VALUE_PATTERN,
    SPRING_QUALIFIER_PATTERN,
    SPRING_QUALIFIED_VARIABLE_PATTERN,
    SPRING_PROFILE_PATTERN,
    SPRING_CONDITIONAL_ON_PROPERTY_PATTERN,
    SPRING_BEAN_NAME_PATTERN,
    SPRING_PRIMARY_PATTERN,
    SPRING_AOP_PATTERN,
    SPRING_SCHEDULED_PATTERN,
    SPRING_ASPECT_PATTERN,
    SPRING_INTERCEPTOR_PATTERN,
    MESSAGE_LISTENER_PATTERN,
    MESSAGE_SEND_PATTERN,
    EVENT_PUBLISH_PATTERN,
    MAVEN_DEPENDENCY_BLOCK_PATTERN,
    MAVEN_TAG_PATTERN,
    GRADLE_COORDINATE_PATTERN,
    GRADLE_PROJECT_DEPENDENCY_PATTERN,
    GRADLE_INCLUDE_PATTERN,
    RUNTIME_TRACE_FILENAMES,
    TEST_PATH_MARKERS,
    TEST_ANNOTATION_PATTERN,
    TEST_ASSERTION_PATTERN,
    OPERATIONAL_BOUNDARY_PATTERN,
    FTS_TOKEN_PATTERN,
    DECLARATION_HINT_PATTERN,
    PATHISH_PATTERN,
    CALL_SYMBOL_PATTERN,
    MEMBER_CALL_PATTERN,
    CLASS_CONSTRUCTION_PATTERN,
    FIELD_OR_PARAM_TYPE_PATTERN,
    FIELD_VAR_TYPE_PATTERN,
    GENERIC_FIELD_VAR_TYPE_PATTERN,
    SERVICE_LIKE_TYPE_PATTERN,
    STREAM_LAMBDA_PATTERN,
    PROVIDER_CHAIN_CALL_PATTERN,
    THIS_FIELD_ASSIGNMENT_PATTERN,
)
from bpmis_jira_tool.source_code_qa_runtime_policy import (
    LLM_PROMPT_VERSION,
    LLM_RESPONSE_SCHEMA_VERSION,
    LLM_ROUTER_VERSION,
    LLM_CACHE_VERSION,
    LLM_RUNTIME_VERSION,
    PLANNER_TOOL_DSL_VERSION,
    COMPACT_DEEP_BUDGET_MODE,
    LLM_PROMPT_COMPACT_THRESHOLD_TOKENS,
    LLM_PROMPT_TIGHT_THRESHOLD_TOKENS,
    LLM_TOKEN_ESTIMATE_CHARS_PER_TOKEN,
    DEFAULT_SEMANTIC_INDEX_MODEL,
    DEFAULT_DOMAIN_PROFILE_PATH,
    DEFAULT_DOMAIN_KNOWLEDGE_PACK_PATH,
    DEFAULT_LLM_TIMEOUT_SECONDS,
    DEFAULT_CODEX_CLI_MODEL,
    DEFAULT_CODEX_TIMEOUT_SECONDS,
    DEFAULT_CODEX_TOP_PATH_LIMIT,
    DEFAULT_CODEX_REPAIR_TOP_PATH_LIMIT,
    DEFAULT_CODEX_REPAIR_PROMPT_TOKEN_LIMIT,
    CODEX_INVESTIGATION_PROMPT_MODE,
    CODEX_SQL_GENERATION_PROMPT_MODE,
    CODEX_SQL_RUNTIME_EVIDENCE_CHAR_LIMIT,
    CODEX_SESSION_MODE_EPHEMERAL,
    CODEX_SESSION_MODE_RESUME,
    DEFAULT_INDEX_LOCK_STALE_SECONDS,
    DEFAULT_AUTO_SYNC_START_DATE,
    DEFAULT_AUTO_SYNC_INTERVAL_DAYS,
    MAX_CACHED_INDEX_LINES,
    MAX_CACHED_SEMANTIC_CHUNKS,
    MAX_TARGETED_INDEX_FILES,
    MAX_TARGETED_INDEX_LINES,
    MAX_TARGETED_SEMANTIC_CHUNKS,
    SYNC_JOB_LOCK_TIMEOUT_SECONDS,
    DEFAULT_LLM_BUDGETS,
    ANSWER_SELF_CHECK_WEAK_PHRASES,
    PRODUCTION_EVIDENCE_TIERS,
    SKIP_DIRS,
    TEXT_SUFFIXES,
    STOPWORDS,
)
from bpmis_jira_tool.source_code_qa_types import (
    RepositoryEntry,
    SourceCodeQAIndexUnavailable,
)


LOGGER = logging.getLogger(__name__)

ALL_COUNTRY = "All"
CRMS_COUNTRIES = ("SG", "ID", "PH")
ANSWER_MODE_AUTO = "auto"
ANSWER_MODE = "retrieval_only"
QUERY_MODE_DEEP = "deep"
CONFIG_VERSION = 1
CODE_INDEX_VERSION = 30


def _log_source_code_qa_timing(component: str, *, elapsed_ms: int, **fields: Any) -> None:
    payload = {
        "event": "source_code_qa_timing",
        "component": component,
        "elapsed_ms": max(0, int(elapsed_ms)),
    }
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            payload[key] = value
        elif isinstance(value, (list, tuple, set)):
            payload[key] = list(value)[:20]
        elif isinstance(value, dict):
            payload[key] = {
                str(item_key): item_value
                for item_key, item_value in value.items()
                if isinstance(item_value, (str, int, float, bool))
            }
        else:
            payload[key] = str(value)
    LOGGER.warning("source_code_qa_timing %s", json.dumps(payload, ensure_ascii=False, sort_keys=True))


class SourceCodeQAService:
    def __init__(
        self,
        *,
        data_root: Path,
        team_profiles: dict[str, dict[str, Any]],
        gitlab_token: str | None = None,
        gitlab_username: str = "oauth2",
        llm_provider: str = LLM_PROVIDER_CODEX_CLI_BRIDGE,
        query_rewrite_model: str | None = None,
        planner_model: str | None = None,
        answer_model: str | None = None,
        repair_model: str | None = None,
        semantic_index_model: str = DEFAULT_SEMANTIC_INDEX_MODEL,
        semantic_index_enabled: bool = True,
        llm_cache_ttl_seconds: int = 1800,
        llm_timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        codex_timeout_seconds: int | None = None,
        codex_concurrency: int = 1,
        codex_top_path_limit: int = DEFAULT_CODEX_TOP_PATH_LIMIT,
        codex_repair_enabled: bool = True,
        codex_session_mode: str = CODEX_SESSION_MODE_EPHEMERAL,
        codex_session_max_turns: int = 8,
        codex_cache_followups: bool = False,
        git_timeout_seconds: int = 90,
        max_file_bytes: int = 500_000,
    ) -> None:
        self.base_data_root = data_root
        self.data_root = data_root / "source_code_qa"
        self.config_path = self.data_root / "config.json"
        self.repo_root = self.data_root / "repos"
        self.index_root = self.data_root / "indexes"
        self.answer_cache_root = self.data_root / "answer_cache"
        self.telemetry_path = self.data_root / "telemetry.jsonl"
        self.feedback_path = self.data_root / "feedback.jsonl"
        self.sync_jobs_path = self.data_root / "sync_jobs.json"
        self.lock_root = self.data_root / "locks"
        self.domain_profile_path = Path(os.getenv("SOURCE_CODE_QA_DOMAIN_PROFILES", str(DEFAULT_DOMAIN_PROFILE_PATH)))
        self.domain_knowledge_pack_path = Path(os.getenv("SOURCE_CODE_QA_DOMAIN_KNOWLEDGE_PACKS", str(DEFAULT_DOMAIN_KNOWLEDGE_PACK_PATH)))
        self.team_profiles = team_profiles
        self.gitlab_token = str(gitlab_token or "").strip()
        self.gitlab_username = str(gitlab_username or "oauth2").strip() or "oauth2"
        self.llm_provider_name = self.normalize_query_llm_provider(llm_provider)
        self.query_rewrite_model = str(query_rewrite_model or "").strip()
        self.planner_model = str(planner_model or "").strip()
        self.answer_model = str(answer_model or "").strip()
        self.repair_model = str(repair_model or "").strip()
        self.semantic_index_model = str(semantic_index_model or DEFAULT_SEMANTIC_INDEX_MODEL).strip() or DEFAULT_SEMANTIC_INDEX_MODEL
        self.semantic_index_enabled = bool(semantic_index_enabled)
        self.llm_timeout_seconds = max(5, int(llm_timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.codex_timeout_seconds = max(
            10,
            int(codex_timeout_seconds if codex_timeout_seconds is not None else DEFAULT_CODEX_TIMEOUT_SECONDS),
        )
        self.codex_concurrency = max(1, min(int(codex_concurrency or 1), 4))
        self.codex_top_path_limit = max(5, min(int(codex_top_path_limit or DEFAULT_CODEX_TOP_PATH_LIMIT), 80))
        self.codex_repair_top_path_limit = max(
            5,
            min(
                int(os.getenv("SOURCE_CODE_QA_CODEX_REPAIR_TOP_PATH_LIMIT", DEFAULT_CODEX_REPAIR_TOP_PATH_LIMIT) or DEFAULT_CODEX_REPAIR_TOP_PATH_LIMIT),
                self.codex_top_path_limit,
            ),
        )
        self.codex_repair_prompt_token_limit = max(
            4_000,
            int(os.getenv("SOURCE_CODE_QA_CODEX_REPAIR_PROMPT_TOKEN_LIMIT", DEFAULT_CODEX_REPAIR_PROMPT_TOKEN_LIMIT) or DEFAULT_CODEX_REPAIR_PROMPT_TOKEN_LIMIT),
        )
        self.codex_repair_enabled = bool(codex_repair_enabled)
        normalized_codex_session_mode = str(codex_session_mode or CODEX_SESSION_MODE_EPHEMERAL).strip().lower()
        self.codex_session_mode = normalized_codex_session_mode if normalized_codex_session_mode in {CODEX_SESSION_MODE_EPHEMERAL, CODEX_SESSION_MODE_RESUME} else CODEX_SESSION_MODE_EPHEMERAL
        self.codex_session_max_turns = max(1, min(int(codex_session_max_turns or 8), 30))
        self.codex_cache_followups = bool(codex_cache_followups)
        self.codex_model = self._codex_cli_model()
        self.llm_provider = self._build_llm_provider()
        self.llm_budgets = self._build_llm_budgets()
        self.model_policy = self._build_model_policy_matrix()
        self._tree_sitter_parsers: dict[str, Any | None] = {}
        self._tree_sitter_load_errors: dict[str, str] = {}
        self.llm_cache_ttl_seconds = max(60, int(llm_cache_ttl_seconds or 1800))
        self.git_timeout_seconds = max(5, int(git_timeout_seconds or 90))
        self.max_file_bytes = max(20_000, int(max_file_bytes or 500_000))
        self._retrieval = SourceCodeQARetrievalComponent(self)
        self._answer_generation = SourceCodeQAAnswerGenerationComponent(self)
        self._quality_judge = SourceCodeQAQualityJudgeComponent(self)

    def with_llm_provider(self, llm_provider: str) -> "SourceCodeQAService":
        normalized = self.normalize_query_llm_provider(llm_provider)
        if normalized == self.llm_provider_name:
            return self
        return self._clone(llm_provider=normalized)

    def with_codex_timeout_seconds(self, codex_timeout_seconds: int | None) -> "SourceCodeQAService":
        timeout_seconds = max(
            10,
            int(codex_timeout_seconds if codex_timeout_seconds is not None else self.codex_timeout_seconds),
        )
        if timeout_seconds == self.codex_timeout_seconds:
            return self
        return self._clone(codex_timeout_seconds=timeout_seconds)

    def _clone(
        self,
        *,
        llm_provider: str | None = None,
        codex_timeout_seconds: int | None = None,
    ) -> "SourceCodeQAService":
        return SourceCodeQAService(
            data_root=self.base_data_root,
            team_profiles=self.team_profiles,
            gitlab_token=self.gitlab_token,
            gitlab_username=self.gitlab_username,
            llm_provider=llm_provider or self.llm_provider_name,
            query_rewrite_model=self.query_rewrite_model,
            planner_model=self.planner_model,
            answer_model=self.answer_model,
            repair_model=self.repair_model,
            semantic_index_model=self.semantic_index_model,
            semantic_index_enabled=self.semantic_index_enabled,
            llm_cache_ttl_seconds=self.llm_cache_ttl_seconds,
            llm_timeout_seconds=self.llm_timeout_seconds,
            codex_timeout_seconds=self.codex_timeout_seconds if codex_timeout_seconds is None else codex_timeout_seconds,
            codex_concurrency=self.codex_concurrency,
            codex_top_path_limit=self.codex_top_path_limit,
            codex_repair_enabled=self.codex_repair_enabled,
            codex_session_mode=self.codex_session_mode,
            codex_session_max_turns=self.codex_session_max_turns,
            codex_cache_followups=self.codex_cache_followups,
            git_timeout_seconds=self.git_timeout_seconds,
            max_file_bytes=self.max_file_bytes,
        )

    @staticmethod
    def normalize_query_llm_provider(llm_provider: str | None) -> str:
        provider = str(llm_provider or LLM_PROVIDER_CODEX_CLI_BRIDGE).strip().lower() or LLM_PROVIDER_CODEX_CLI_BRIDGE
        return provider if provider in LLM_PROVIDER_ALLOWED_QUERY_CHOICES else LLM_PROVIDER_CODEX_CLI_BRIDGE

    @staticmethod
    def normalize_query_mode(query_mode: str | None) -> str:
        return QUERY_MODE_DEEP

    def options_payload(self) -> dict[str, Any]:
        return {
            "pm_teams": [
                {"code": code, "label": str(profile.get("label") or code)}
                for code, profile in self.team_profiles.items()
            ],
            "countries": list(CRMS_COUNTRIES),
            "all_country": ALL_COUNTRY,
            "answer_modes": [
                {"value": ANSWER_MODE_AUTO, "label": "Smart Answer"},
            ],
            "query_modes": [
                {"value": QUERY_MODE_DEEP, "label": "Deep Mode", "description": "Codex deep investigation is the default source-code answer path."},
            ],
            "llm_providers": [
                {"value": LLM_PROVIDER_CODEX_CLI_BRIDGE, "label": "Codex"},
            ],
        }

    def llm_policy_payload(self) -> dict[str, Any]:
        return {
            "provider": self.llm_provider.public_config(),
            "versions": self._llm_versions(),
            "budgets": self.llm_budgets,
            "model_policy": self.model_policy,
            "router": {
                "version": LLM_ROUTER_VERSION,
                "auto_rules": [
                    {"budget": "deep", "reason": "data_source_trace"},
                    {"budget": "deep", "reason": "root_cause_or_error"},
                    {"budget": "deep", "reason": "agentic_or_graph_trace"},
                    {"budget": COMPACT_DEEP_BUDGET_MODE, "reason": "prompt_token_pressure"},
                    {"budget": "balanced", "reason": "api_config_rule_or_5_plus_matches"},
                    {"budget": "cheap", "reason": "simple_lookup"},
                ],
                "token_pressure": {
                    "compact_threshold": LLM_PROMPT_COMPACT_THRESHOLD_TOKENS,
                    "tight_threshold": LLM_PROMPT_TIGHT_THRESHOLD_TOKENS,
                    "strategy": "switch balanced/deep answers to compact_deep before the model call when estimated prompt tokens are high",
                },
            },
            "planner_tools": self._planner_tool_registry(),
            "cache": {
                "version": LLM_CACHE_VERSION,
                "ttl_seconds": self.llm_cache_ttl_seconds,
                "atomic_writes": True,
            },
            "runtime": {
                "version": LLM_RUNTIME_VERSION,
                "timeout_seconds": self.llm_timeout_seconds,
                "fallback_model": self._llm_fallback_model(),
            },
            "semantic_retrieval": {
                "enabled": self.semantic_index_enabled,
                "model": self.semantic_index_model,
                "index_version": CODE_INDEX_VERSION,
                "embedding_provider": {"provider": "local_token_hybrid", "ready": True},
            },
            "judge": {
                "enabled": True,
                "mode": "deterministic_evidence_judge",
            },
        }

    @staticmethod
    def _planner_tool_registry() -> dict[str, Any]:
        return {
            "version": PLANNER_TOOL_DSL_VERSION,
            "tools": [
                {"name": "find_definition", "source": "definitions", "purpose": "Find symbol/class/method/config definitions."},
                {"name": "find_references", "source": "references_index", "purpose": "Find references to symbols, routes, tables, calls, or data-flow targets."},
                {"name": "find_callers", "source": "flow_edges", "purpose": "Find callers or upstream files that point to a target name."},
                {"name": "find_callees", "source": "flow_edges", "purpose": "Find downstream calls, services, repositories, routes, tables, and clients from seed files."},
                {"name": "open_file_window", "source": "lines", "purpose": "Open a wider source window around current evidence."},
                {"name": "find_tables", "source": "references_index", "purpose": "Find SQL table references."},
                {"name": "find_api_routes", "source": "references_index", "purpose": "Find route, HTTP endpoint, and downstream API references."},
                {"name": "trace_graph", "source": "graph_edges", "purpose": "Trace symbol graph edges from seed files."},
                {"name": "trace_flow", "source": "flow_edges", "purpose": "Trace normalized code-flow edges from seed files."},
                {"name": "trace_entity", "source": "entity_edges", "purpose": "Trace entity-level edges from seed files."},
                {"name": "find_static_findings", "source": "lines", "purpose": "Find deterministic static QA findings such as hardcoded secrets, unsafe SQL, broad exceptions, command execution, and TODO/FIXME markers."},
                {"name": "find_test_coverage", "source": "lines/references_index", "purpose": "Find tests, assertions, mocks, and verification evidence covering a target symbol or behavior."},
                {"name": "find_operational_boundaries", "source": "references_index/entity_edges", "purpose": "Find transaction, cache, async, retry, circuit breaker, lock, and authorization boundary annotations."},
                {"name": "search_code", "source": "hybrid_index", "purpose": "Fallback keyword and semantic search."},
            ],
        }

    def llm_unavailable_message(self) -> str:
        return "Codex is unavailable. Run `codex login` with ChatGPT on this server before using Codex mode."

    def _build_llm_provider(self) -> SourceCodeQALLMProvider:
        return CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=self.repo_root,
            timeout_seconds=self.codex_timeout_seconds,
            concurrency_limit=self.codex_concurrency,
            session_mode=self.codex_session_mode,
        )

    def _build_llm_budgets(self) -> dict[str, dict[str, Any]]:
        budgets = json.loads(json.dumps(DEFAULT_LLM_BUDGETS))
        budgets["cheap"]["model"] = self.codex_model
        budgets["balanced"]["model"] = self.codex_model
        budgets["deep"]["model"] = self.codex_model
        budgets[COMPACT_DEEP_BUDGET_MODE]["model"] = self.codex_model
        return budgets

    def _build_model_policy_matrix(self) -> dict[str, dict[str, Any]]:
        role_defaults = {
            "query_rewrite": ("cheap", self.query_rewrite_model, "Normalize follow-up and fuzzy user wording before retrieval."),
            "planner": ("cheap", self.planner_model, "Choose deterministic retrieval tools and trace expansion steps."),
            "answer": ("balanced", self.answer_model, "Generate the user-facing evidence-grounded answer."),
            "repair": ("deep", self.repair_model, "Rewrite after a failed claim check or missing-evidence judge finding."),
        }
        matrix: dict[str, dict[str, Any]] = {}
        for role, (budget, override, purpose) in role_defaults.items():
            budget_model = str((self.llm_budgets.get(budget) or {}).get("model") or self._llm_default_model()).strip()
            model = str(override or budget_model or self._llm_default_model()).strip()
            matrix[role] = {
                "model": model,
                "budget": budget,
                "override": bool(override),
                "budget_routed": role in {"answer", "repair"} and not bool(override),
                "purpose": purpose,
            }
        return matrix

    def _model_for_role(self, role: str, *, fallback: str | None = None) -> str:
        policy = self.model_policy.get(role) or {}
        model = str(policy.get("model") or "").strip()
        if model:
            return model
        return str(fallback or self._llm_default_model()).strip() or self._llm_default_model()

    def _model_for_role_or_budget(self, role: str, budget: dict[str, Any]) -> str:
        policy = self.model_policy.get(role) or {}
        if policy.get("override"):
            model = str(policy.get("model") or "").strip()
            if model:
                return model
        return str((budget or {}).get("model") or self._llm_default_model()).strip() or self._llm_default_model()

    @staticmethod
    def _finish_reason_needs_generation_repair(finish_reason: str | None) -> bool:
        reason = str(finish_reason or "").strip()
        return reason.upper() in {"MAX_TOKENS", "SAFETY", "RECITATION"} or reason.lower() in {"length", "content_filter"}

    @staticmethod
    def _estimate_llm_tokens(text: str) -> int:
        return max(1, int((len(str(text or "")) / LLM_TOKEN_ESTIMATE_CHARS_PER_TOKEN) + 0.999))

    def _llm_fallback_model(self) -> str:
        return self.codex_model

    def _llm_default_model(self) -> str:
        return self.codex_model

    @staticmethod
    def _codex_cli_model() -> str:
        return str(os.getenv("SOURCE_CODE_QA_CODEX_MODEL") or DEFAULT_CODEX_CLI_MODEL).strip() or DEFAULT_CODEX_CLI_MODEL

    @staticmethod
    def _normalize_llm_usage(usage: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(usage or {})
        prompt_tokens = normalized.get("prompt_tokens", normalized.get("promptTokenCount"))
        completion_tokens = normalized.get("completion_tokens", normalized.get("candidatesTokenCount"))
        total_tokens = normalized.get("total_tokens", normalized.get("totalTokenCount"))
        if total_tokens is None and prompt_tokens is not None and completion_tokens is not None:
            try:
                total_tokens = int(prompt_tokens) + int(completion_tokens)
            except (TypeError, ValueError):
                total_tokens = None
        if prompt_tokens is not None:
            normalized["prompt_tokens"] = prompt_tokens
        if completion_tokens is not None:
            normalized["completion_tokens"] = completion_tokens
        if total_tokens is not None:
            normalized["total_tokens"] = total_tokens
        return normalized

    @staticmethod
    def _merge_llm_usage(*usage_rows: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        token_keys = {
            "promptTokenCount",
            "candidatesTokenCount",
            "totalTokenCount",
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
        }
        for usage in usage_rows:
            for key, value in (usage or {}).items():
                if key in token_keys:
                    try:
                        merged[key] = int(merged.get(key) or 0) + int(value)
                    except (TypeError, ValueError):
                        merged[key] = value
                elif key not in merged:
                    merged[key] = value
        return SourceCodeQAService._normalize_llm_usage(merged)

    @staticmethod
    def _llm_finish_reason(payload: dict[str, Any]) -> str:
        candidates = payload.get("candidates") or []
        for candidate in candidates:
            reason = str(candidate.get("finishReason") or "").strip()
            if reason:
                return reason
        choices = payload.get("choices") or []
        for choice in choices:
            reason = str(choice.get("finish_reason") or "").strip()
            if reason:
                return reason
        reason = str(payload.get("finish_reason") or "").strip()
        if reason:
            return reason
        return ""

    def load_config(self) -> dict[str, Any]:
        if not self.config_path.exists():
            return {"version": CONFIG_VERSION, "mappings": {}, "updated_at": None}
        try:
            payload = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ToolError("Source Code Q&A config could not be read. Please save it again.") from error
        mappings = payload.get("mappings") if isinstance(payload, dict) else {}
        if not isinstance(mappings, dict):
            mappings = {}
        return {
            "version": CONFIG_VERSION,
            "mappings": {
                str(key): [self._entry_to_dict(entry) for entry in value if isinstance(entry, dict)]
                for key, value in mappings.items()
                if isinstance(value, list)
            },
            "updated_at": payload.get("updated_at") if isinstance(payload, dict) else None,
        }

    def load_domain_profiles(self) -> dict[str, Any]:
        if not self.domain_profile_path.exists():
            return {"default": {}}
        try:
            payload = json.loads(self.domain_profile_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"default": {}}
        return payload if isinstance(payload, dict) else {"default": {}}

    def load_domain_knowledge_packs(self) -> dict[str, Any]:
        if not self.domain_knowledge_pack_path.exists():
            return {"version": 1, "domains": {}}
        try:
            payload = json.loads(self.domain_knowledge_pack_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"version": 1, "domains": {}}
        if not isinstance(payload, dict):
            return {"version": 1, "domains": {}}
        domains = payload.get("domains") if isinstance(payload.get("domains"), dict) else {}
        return {**payload, "domains": domains}

    def domain_knowledge_payload(self) -> dict[str, Any]:
        payload = self.load_domain_knowledge_packs()
        domains = payload.get("domains") or {}
        return {
            "version": payload.get("version") or 1,
            "updated_at": payload.get("updated_at"),
            "domains": {
                key: {
                    "label": value.get("label"),
                    "summary": value.get("summary"),
                    "module_count": len(value.get("module_map") or []),
                    "question_count": len(value.get("question_seeds") or []),
                    "evidence_rules": value.get("evidence_rules") or [],
                }
                for key, value in domains.items()
                if isinstance(value, dict)
            },
        }

    def _domain_profile(self, pm_team: str, country: str) -> dict[str, Any]:
        del country
        profiles = self.load_domain_profiles()
        default = profiles.get("default") if isinstance(profiles.get("default"), dict) else {}
        team = str(pm_team or "").strip().upper()
        team_profile = profiles.get(team) if isinstance(profiles.get(team), dict) else {}
        merged = dict(default)
        for key, value in team_profile.items():
            if isinstance(value, list) and isinstance(merged.get(key), list):
                merged[key] = list(dict.fromkeys([*merged[key], *value]))
            else:
                merged[key] = value
        pack = self._domain_knowledge_pack(team)
        retrieval_terms = pack.get("retrieval_terms") if isinstance(pack.get("retrieval_terms"), dict) else {}
        for key, value in retrieval_terms.items():
            if isinstance(value, list):
                merged[key] = list(dict.fromkeys([*(merged.get(key) if isinstance(merged.get(key), list) else []), *[str(item) for item in value if str(item).strip()]]))
        knowledge_terms = self._domain_knowledge_terms(pack)
        if knowledge_terms:
            merged["knowledge_terms"] = list(dict.fromkeys([*(merged.get("knowledge_terms") if isinstance(merged.get("knowledge_terms"), list) else []), *knowledge_terms]))
        return merged

    def _domain_knowledge_pack(self, team: str) -> dict[str, Any]:
        domains = self.load_domain_knowledge_packs().get("domains") or {}
        pack = domains.get(str(team or "").strip().upper())
        return pack if isinstance(pack, dict) else {}

    @classmethod
    def _domain_knowledge_terms(cls, pack: dict[str, Any]) -> list[str]:
        terms: list[str] = []

        def add(value: Any) -> None:
            text = str(value or "").strip()
            if text:
                terms.append(text)

        for module in pack.get("module_map") or []:
            if not isinstance(module, dict):
                continue
            add(module.get("name"))
            for key in ("aliases", "code_hints", "repo_hints", "business_flows"):
                for item in module.get(key) or []:
                    add(item)
        for item in pack.get("terminology") or []:
            if not isinstance(item, dict):
                continue
            add(item.get("term"))
            for key in ("aliases", "code_terms"):
                for value in item.get(key) or []:
                    add(value)
        artifacts = pack.get("key_artifacts") if isinstance(pack.get("key_artifacts"), dict) else {}
        for values in artifacts.values():
            if isinstance(values, list):
                for item in values:
                    if isinstance(item, dict):
                        add(item.get("name"))
                        add(item.get("path"))
                        add(item.get("purpose"))
                    else:
                        add(item)
        for question in pack.get("question_seeds") or []:
            if isinstance(question, dict):
                add(question.get("question"))
                for item in question.get("expected_terms") or []:
                    add(item)
            else:
                add(question)
        return list(dict.fromkeys(terms))[:220]

    def _llm_domain_context(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        evidence_summary: dict[str, Any],
    ) -> str:
        team = str(pm_team or "").strip().upper()
        pack = self._domain_knowledge_pack(team)
        intent = evidence_summary.get("intent") or self._question_intent(question)
        lines: list[str] = []
        if pack:
            label = str(pack.get("label") or team or "Domain").strip()
            summary = str(pack.get("summary") or "").strip()
            country_text = str(country or ALL_COUNTRY).strip() or ALL_COUNTRY
            lines.append(f"Domain guidance: {label} / country={country_text}")
            if summary:
                lines.append(f"- Domain summary: {summary}")
            matched_modules = self._matched_domain_modules(pack, question, evidence_summary)
            if matched_modules:
                lines.append("- Relevant domain modules:")
                for module in matched_modules[:4]:
                    flows = ", ".join(str(item) for item in module.get("business_flows") or [] if str(item).strip())
                    hints = ", ".join(str(item) for item in module.get("code_hints") or [] if str(item).strip())
                    detail = f"; flows={flows}" if flows else ""
                    hint_text = f"; code_hints={hints}" if hints else ""
                    lines.append(f"  - {module.get('name')}{detail}{hint_text}")
            rules = [str(item).strip() for item in pack.get("evidence_rules") or [] if str(item).strip()]
            if rules:
                lines.append("- Domain evidence rules:")
                lines.extend(f"  - {rule}" for rule in rules[:5])
            artifact_lines = self._domain_artifact_lines(pack, intent)
            if artifact_lines:
                lines.append("- Domain artifact hints:")
                lines.extend(f"  - {line}" for line in artifact_lines[:8])
        blueprint = self._llm_answer_blueprint(intent)
        if blueprint:
            lines.append("- Answer blueprint:")
            lines.extend(f"  - {item}" for item in blueprint)
        lines.append("- Evidence priority: production code and mapper/client/SQL evidence > config snapshots > tests > docs/spec/generated files.")
        lines.append("- When repos disagree, explain the stronger code-backed path first and put weaker or older evidence under missing/uncertain evidence.")
        return "\n".join(lines)

    def _matched_domain_modules(
        self,
        pack: dict[str, Any],
        question: str,
        evidence_summary: dict[str, Any],
    ) -> list[dict[str, Any]]:
        haystack = " ".join(
            [
                str(question or ""),
                json.dumps(evidence_summary, ensure_ascii=False),
            ]
        ).lower()
        scored: list[tuple[int, int, dict[str, Any]]] = []
        for index, module in enumerate(pack.get("module_map") or []):
            if not isinstance(module, dict):
                continue
            terms = [module.get("name")]
            for key in ("aliases", "code_hints", "repo_hints", "business_flows"):
                terms.extend(module.get(key) or [])
            score = 0
            for term in terms:
                token = str(term or "").strip().lower()
                if token and token in haystack:
                    score += 3 if token == str(module.get("name") or "").strip().lower() else 1
                    continue
                term_tokens = [
                    part
                    for part in re.findall(r"[a-z0-9]+", token)
                    if len(part) >= 3 and part not in STOPWORDS and part not in LOW_VALUE_FOCUS_TERMS
                ]
                if len(term_tokens) >= 2 and all(part in haystack for part in term_tokens[:4]):
                    score += 1
            if score:
                scored.append((score, -index, module))
        scored.sort(reverse=True)
        if scored:
            return [item for _score, _index, item in scored[:4]]
        modules = [item for item in pack.get("module_map") or [] if isinstance(item, dict)]
        return modules[:2]

    @staticmethod
    def _domain_artifact_lines(pack: dict[str, Any], intent: dict[str, Any]) -> list[str]:
        artifacts = pack.get("key_artifacts") if isinstance(pack.get("key_artifacts"), dict) else {}
        selected_keys: list[str] = []
        if intent.get("data_source") or intent.get("module_dependency"):
            selected_keys.extend(["tables", "apis"])
        if intent.get("api"):
            selected_keys.append("apis")
        if intent.get("config") or intent.get("operational_boundary"):
            selected_keys.append("configs")
        if intent.get("rule_logic") or intent.get("impact_analysis"):
            selected_keys.extend(["apis", "configs", "tables"])
        if not selected_keys:
            selected_keys.extend(["tables", "apis", "configs"])
        lines: list[str] = []
        for key in list(dict.fromkeys(selected_keys)):
            for item in artifacts.get(key) or []:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or item.get("path") or "").strip()
                purpose = str(item.get("purpose") or "").strip()
                path = str(item.get("path") or "").strip()
                if not name and not path:
                    continue
                label = f"{key[:-1]} {name or path}"
                if path and path != name:
                    label = f"{label} ({path})"
                lines.append(f"{label}: {purpose}" if purpose else label)
        return lines

    @staticmethod
    def _llm_answer_blueprint(intent: dict[str, Any]) -> list[str]:
        if intent.get("data_source"):
            return [
                "Start with whether the final source/table/API is confirmed.",
                "Then list confirmed carriers, provider/builder/setter trail, and concrete repository/mapper/client/table evidence.",
                "If relation between two tables/entities is asked, separate co-occurrence in the same flow from proven upstream/downstream conversion.",
            ]
        if intent.get("api"):
            return [
                "Name the entry API/controller/client first.",
                "Then summarize service path, downstream calls, configs/tables touched, and missing hops.",
            ]
        if intent.get("config") or intent.get("operational_boundary"):
            return [
                "Name the config key/annotation/lock first.",
                "Then explain where it is loaded, what runtime boundary it controls, and which caller/service uses it.",
            ]
        if intent.get("static_qa"):
            return [
                "Rank findings by severity.",
                "For each finding, state the risky line pattern, why it matters, and whether exploit/runtime impact is proven.",
            ]
        if intent.get("impact_analysis"):
            return [
                "Separate upstream callers from downstream dependencies.",
                "Mention data tables/APIs/configs/tests only when directly supported by evidence.",
            ]
        if intent.get("test_coverage"):
            return [
                "Separate direct tests, mocks, assertions, and nearby production evidence.",
                "Call out missing test evidence explicitly instead of inferring coverage.",
            ]
        return [
            "Answer directly, then add evidence-backed bullets when useful.",
            "Prefer business-readable names while keeping citation tags on concrete claims.",
        ]

    @staticmethod
    def _profile_terms(profile: dict[str, Any], *keys: str) -> list[str]:
        terms: list[str] = []
        for key in keys:
            value = profile.get(key)
            if isinstance(value, list):
                terms.extend(str(term).strip() for term in value if str(term).strip())
        return list(dict.fromkeys(terms))

    def _expand_tokens_with_domain_profile(
        self,
        tokens: list[str],
        question: str,
        profile: dict[str, Any],
    ) -> list[str]:
        intent = self._question_intent(question)
        profile_terms: list[str] = []
        profile_terms.extend(self._question_specific_retrieval_terms(question))
        if intent.get("data_source"):
            profile_terms.extend(self._profile_terms(profile, "data_carriers", "source_terms", "field_population_terms", "knowledge_terms"))
        if intent.get("api"):
            profile_terms.extend(self._profile_terms(profile, "api_terms", "knowledge_terms"))
        if intent.get("config"):
            profile_terms.extend(self._profile_terms(profile, "config_terms", "knowledge_terms"))
        if intent.get("rule_logic") or intent.get("error"):
            profile_terms.extend(self._profile_terms(profile, "logic_terms", "knowledge_terms"))
        expanded = list(tokens)
        for term in profile_terms:
            for token in self._question_tokens(term):
                if token not in expanded:
                    expanded.append(token)
        return expanded[:40]

    @staticmethod
    def _question_specific_retrieval_terms(question: str) -> list[str]:
        lowered = f" {str(question or '').lower()} "
        cbs_markers = (" cbs ", "cbs report", "bureau report", "credit bureau")
        credit_review_markers = ("credit review", "monthly credit", "monthly review", "performing monthly", "review")
        if any(marker in lowered for marker in cbs_markers) and any(marker in lowered for marker in credit_review_markers):
            return [
                "CreditReviewCbsService",
                "CreditReviewCbsServiceImpl",
                "CreditReviewCbsBureauReportProvider",
                "CreditReviewCBSDataDTO",
                "CreditReviewCBSReqDTO",
                "CreditReviewHighRiskRetailStrategy",
                "CreditReviewHighRiskSmeStrategy",
                "CreditReviewRetailStrategy1",
                "RetailStrategy2CbsProvider",
                "CR_CBS_REPORT",
                "CbsSyncService",
                "SyncCBSReport4ManualEnquiryXxlJob",
                "CbsReportCacheRepository",
                "requestRetailCBSData",
                "CRMS_CBS_BATCH_JOB",
            ]
        income_doc_markers = (
            "payslip",
            "pay slip",
            "income doc",
            "income document",
            "credit card",
            "card income",
            "ops extracted",
            "ops field",
            "llm extracted",
            "extract field",
        )
        extraction_markers = (" extract", "extracted", "llm", "ops", "field", "fields", "table", "stores", "stored")
        if not any(marker in lowered for marker in income_doc_markers):
            return []
        if not any(marker in lowered for marker in extraction_markers):
            return []
        return [
            "ExtractRecord",
            "ExtractRecordDO",
            "ExtractRecordDAO",
            "ExtractRecordRepository",
            "ExtractInfoResultConsumer",
            "extract_record_tab",
            "response_body",
            "IntelExtractResultWrap",
            "IesResultDTO",
            "PayslipProcessStrategy",
            "PayslipExtractResult",
            "CardIncomeScreenProcessInfo",
            "CardIncomeScreeningFlowStatus",
            "CardIncomeScreeningFlowStatusDO",
            "CardIncomeScreeningFlowStatusDAO",
            "card_income_screening_flow_status_tab",
            "process_info",
            "CardIncomeDocAdminServiceImpl",
            "submitScreenInfo",
            "saveDraft",
            "opsReviewResult",
            "payslipGrossPay",
            "payslipPayrollFrequency",
        ]

    def _apply_conversation_context(
        self,
        question: str,
        conversation_context: dict[str, Any] | None,
        *,
        current_key: str | None = None,
        current_repositories: list[RepositoryEntry] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        if not isinstance(conversation_context, dict):
            return question, {"used": False}
        context_key = str(conversation_context.get("key") or "").strip()
        if current_key and context_key and context_key != current_key:
            return question, {"used": False, "reason": "scope_mismatch", "previous_key": context_key, "current_key": current_key}
        if current_key and not context_key:
            context_pm_team = str(conversation_context.get("pm_team") or "").strip()
            context_country = str(conversation_context.get("country") or "").strip() or ALL_COUNTRY
            if context_pm_team:
                try:
                    inferred_key = self.mapping_key(context_pm_team, context_country)
                except ToolError:
                    inferred_key = ""
                if inferred_key and inferred_key != current_key:
                    return question, {"used": False, "reason": "scope_mismatch", "previous_key": inferred_key, "current_key": current_key}
        repo_scope = self._conversation_repo_scope(question, conversation_context, current_repositories or [])
        if repo_scope.get("mismatch"):
            return question, {"used": False, "reason": "repo_scope_mismatch", **repo_scope}
        lowered = question.lower()
        tokens = set(self._question_tokens(question))
        english_followup_markers = {"this", "that", "it", "them", "above", "previous", "same", "continue"}
        english_followup_phrases = ("this method", "this table", "that method", "that table")
        chinese_followup_markers = ("这个", "那个", "上面", "继续", "它", "他们", "这个方法", "这个表")
        has_followup_marker = (
            bool(tokens & english_followup_markers)
            or any(re.search(rf"\b{re.escape(marker)}\b", lowered) for marker in english_followup_phrases)
            or any(marker in lowered for marker in chinese_followup_markers)
            or self._is_relationship_followup_question(question)
        )
        english_clarification_phrases = (
            "i mean",
            "what i mean",
            "actually",
            "not asking",
            "don't need",
            "do not need",
            "instead",
            "clarify",
        )
        chinese_clarification_markers = (
            "我问的是",
            "不是",
            "不需要",
            "不用",
            "要的话",
            "是否",
            "刚才",
            "上一",
            "前面",
        )
        has_clarification_marker = (
            any(phrase in lowered for phrase in english_clarification_phrases)
            or any(marker in question for marker in chinese_clarification_markers)
        )
        has_same_scope_context = bool(current_key and (context_key == current_key or not context_key))
        should_augment_question = has_followup_marker or has_clarification_marker
        if not should_augment_question and not has_same_scope_context:
            return question, {"used": False}
        title_terms = self._conversation_title_terms(str(conversation_context.get("session_title") or conversation_context.get("title") or ""))
        title_anchor_is_better = False
        if has_followup_marker and len(title_terms) >= 2:
            prior_text = " ".join(
                [
                    str(conversation_context.get("question") or ""),
                    str(conversation_context.get("answer") or conversation_context.get("rendered_answer") or ""),
                    str(conversation_context.get("summary") or ""),
                ]
            ).lower()
            title_anchor_is_better = not any(term.lower() in prior_text for term in title_terms)
        terms: list[str] = []
        for match in conversation_context.get("matches") or []:
            terms.extend(IDENTIFIER_PATTERN.findall(str(match.get("path") or "")))
            terms.extend(IDENTIFIER_PATTERN.findall(str(match.get("snippet") or ""))[:8])
        for path in conversation_context.get("trace_paths") or []:
            for edge in path.get("edges") or []:
                terms.extend(IDENTIFIER_PATTERN.findall(str(edge.get("to_name") or "")))
                terms.extend(IDENTIFIER_PATTERN.findall(str(edge.get("to_file") or "")))
        structured = conversation_context.get("structured_answer") or {}
        for claim in structured.get("claims") or []:
            if isinstance(claim, dict):
                terms.extend(IDENTIFIER_PATTERN.findall(str(claim.get("text") or "")))
        answer_contract = conversation_context.get("answer_contract") or {}
        for key_name in ("confirmed_sources", "data_carriers", "field_population", "missing_links"):
            for value in answer_contract.get(key_name) or []:
                terms.extend(IDENTIFIER_PATTERN.findall(str(value or "")))
        evidence_pack = conversation_context.get("evidence_pack") or {}
        for key_name in ("confirmed_facts", "inferred_facts", "missing_facts", "tables", "apis", "configs", "impact_surfaces"):
            for value in evidence_pack.get(key_name) or []:
                terms.extend(IDENTIFIER_PATTERN.findall(str(value or "")))
        for turn in conversation_context.get("recent_turns") or []:
            if not isinstance(turn, dict):
                continue
            terms.extend(IDENTIFIER_PATTERN.findall(str(turn.get("question") or ""))[:8])
            terms.extend(IDENTIFIER_PATTERN.findall(str(turn.get("answer") or ""))[:12])
            for match in turn.get("matches_snapshot") or []:
                if isinstance(match, dict):
                    terms.extend(IDENTIFIER_PATTERN.findall(str(match.get("path") or "")))
                    terms.extend(IDENTIFIER_PATTERN.findall(str(match.get("reason") or ""))[:4])
            prior_pack = turn.get("evidence_pack") if isinstance(turn.get("evidence_pack"), dict) else {}
            for key_name in ("confirmed_facts", "tables", "apis", "configs", "impact_surfaces"):
                for value in prior_pack.get(key_name) or []:
                    terms.extend(IDENTIFIER_PATTERN.findall(str(value or ""))[:6])
        if self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
            for item in conversation_context.get("codex_candidate_paths") or (conversation_context.get("llm_route") or {}).get("candidate_paths") or []:
                if isinstance(item, dict):
                    terms.extend(IDENTIFIER_PATTERN.findall(str(item.get("path") or "")))
                    terms.extend(IDENTIFIER_PATTERN.findall(str(item.get("reason") or ""))[:6])
            for turn in conversation_context.get("recent_turns") or []:
                if not isinstance(turn, dict):
                    continue
                for item in turn.get("codex_candidate_paths") or []:
                    if isinstance(item, dict):
                        terms.extend(IDENTIFIER_PATTERN.findall(str(item.get("path") or "")))
                        terms.extend(IDENTIFIER_PATTERN.findall(str(item.get("reason") or ""))[:4])
            validation = conversation_context.get("codex_citation_validation") or {}
            for item in validation.get("direct_file_refs") or []:
                if isinstance(item, dict):
                    terms.extend(IDENTIFIER_PATTERN.findall(str(item.get("path") or "")))
        terms = title_terms if title_anchor_is_better else [*title_terms, *terms]
        deduped: list[str] = []
        for term in terms:
            lowered_term = term.lower()
            if len(lowered_term) < 4 or lowered_term in STOPWORDS or lowered_term in LOW_VALUE_CALL_SYMBOLS:
                continue
            if lowered_term not in deduped:
                deduped.append(lowered_term)
        context_terms = deduped[:16]
        if not context_terms and not has_same_scope_context:
            return question, {"used": False}
        augmented = question
        if should_augment_question and context_terms:
            augmented = f"{question}\n\nPrevious Source Code Q&A context terms: {' '.join(context_terms)}"
        followup_payload = self._conversation_followup_payload(
            conversation_context,
            context_terms,
            implicit=not should_augment_question,
            reason="same_scope_session" if not should_augment_question else "followup_marker",
        )
        return augmented, followup_payload

    @staticmethod
    def _conversation_title_terms(title: str) -> list[str]:
        low_value = SourceCodeQAService._followup_low_value_terms()
        terms: list[str] = []
        seen: set[str] = set()
        for token in IDENTIFIER_PATTERN.findall(str(title or "")):
            clean = token.strip("._/-:")
            lowered = clean.lower()
            if len(lowered) < 4 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS or lowered in low_value:
                continue
            if re.fullmatch(r"s\d+", lowered):
                continue
            if lowered in seen:
                continue
            terms.append(clean)
            seen.add(lowered)
        return terms[:8]

    @staticmethod
    def _conversation_followup_payload(
        conversation_context: dict[str, Any],
        context_terms: list[str],
        *,
        implicit: bool = False,
        reason: str = "followup_marker",
    ) -> dict[str, Any]:
        return {
            "used": True,
            "implicit": implicit,
            "reason": reason,
            "terms": context_terms,
            "previous_question": str(conversation_context.get("question") or "")[:180],
            "question": str(conversation_context.get("question") or "")[:500],
            "answer": str(conversation_context.get("answer") or conversation_context.get("rendered_answer") or "")[:2000],
            "rendered_answer": str(conversation_context.get("rendered_answer") or conversation_context.get("answer") or "")[:2000],
            "summary": str(conversation_context.get("summary") or "")[:500],
            "trace_id": str(conversation_context.get("trace_id") or "")[:80],
            "llm_provider": str(conversation_context.get("llm_provider") or "")[:80],
            "llm_model": str(conversation_context.get("llm_model") or "")[:120],
            "llm_route": conversation_context.get("llm_route") if isinstance(conversation_context.get("llm_route"), dict) else {},
            "codex_cli_summary": conversation_context.get("codex_cli_summary") if isinstance(conversation_context.get("codex_cli_summary"), dict) else {},
            "codex_citation_validation": (
                conversation_context.get("codex_citation_validation")
                if isinstance(conversation_context.get("codex_citation_validation"), dict)
                else {}
            ),
            "codex_candidate_paths": [
                item for item in (conversation_context.get("codex_candidate_paths") or [])[:30]
                if isinstance(item, dict)
            ],
            "matches": [
                item for item in (conversation_context.get("matches") or [])[:10]
                if isinstance(item, dict)
            ],
            "matches_snapshot": [
                item for item in (conversation_context.get("matches_snapshot") or conversation_context.get("matches") or [])[:10]
                if isinstance(item, dict)
            ],
            "trace_paths": [
                item for item in (conversation_context.get("trace_paths") or [])[:5]
                if isinstance(item, dict)
            ],
            "structured_answer": (
                conversation_context.get("structured_answer")
                if isinstance(conversation_context.get("structured_answer"), dict)
                else {}
            ),
            "answer_contract": (
                conversation_context.get("answer_contract")
                if isinstance(conversation_context.get("answer_contract"), dict)
                else {}
            ),
            "evidence_pack": (
                conversation_context.get("evidence_pack")
                if isinstance(conversation_context.get("evidence_pack"), dict)
                else {}
            ),
            "recent_turns": [
                item for item in (conversation_context.get("recent_turns") or [])[:3]
                if isinstance(item, dict)
            ],
        }

    def _conversation_repo_scope(
        self,
        question: str,
        conversation_context: dict[str, Any],
        current_repositories: list[RepositoryEntry],
    ) -> dict[str, Any]:
        if not current_repositories:
            return {"mismatch": False}
        mentioned = self._mentioned_repository_aliases(question, current_repositories)
        if not mentioned:
            return {"mismatch": False}
        previous_aliases = self._conversation_context_repository_aliases(conversation_context)
        if not previous_aliases:
            return {
                "mismatch": True,
                "mentioned_repositories": sorted(mentioned),
                "previous_repositories": [],
            }
        if mentioned & previous_aliases:
            return {"mismatch": False, "mentioned_repositories": sorted(mentioned & previous_aliases)}
        return {
            "mismatch": True,
            "mentioned_repositories": sorted(mentioned),
            "previous_repositories": sorted(previous_aliases),
        }

    def _mentioned_repository_aliases(self, question: str, repositories: list[RepositoryEntry]) -> set[str]:
        normalized_question = self._repo_scope_normalize(question)
        mentioned: set[str] = set()
        if not normalized_question:
            return mentioned
        for entry in repositories:
            aliases = self._repository_scope_aliases(entry.display_name, entry.url)
            if any(self._repo_alias_in_text(alias, normalized_question) for alias in aliases):
                mentioned.update(aliases)
        return mentioned

    def _filter_entries_for_question_repository_scope(
        self,
        question: str,
        repositories: list[RepositoryEntry],
    ) -> tuple[list[RepositoryEntry], dict[str, Any]]:
        normalized_question = self._repo_scope_normalize(question)
        if not normalized_question or len(repositories) <= 1:
            return list(repositories), {
                "active": False,
                "selected_repositories": [entry.display_name for entry in repositories],
                "available_repository_count": len(repositories),
            }
        selected: list[RepositoryEntry] = []
        matched_aliases: dict[str, list[str]] = {}
        for entry in repositories:
            aliases = self._repository_scope_aliases(entry.display_name, entry.url)
            direct_matches = sorted(alias for alias in aliases if self._repo_alias_in_text(alias, normalized_question))
            if not direct_matches:
                continue
            selected.append(entry)
            matched_aliases[entry.display_name] = direct_matches[:8]
        if matched_aliases:
            specific_repo_names = {
                repo_name
                for repo_name, aliases in matched_aliases.items()
                if any(cls_alias for cls_alias in aliases if self._repo_scope_alias_is_specific(cls_alias))
            }
            if specific_repo_names:
                selected = [entry for entry in selected if entry.display_name in specific_repo_names]
                matched_aliases = {
                    repo_name: aliases
                    for repo_name, aliases in matched_aliases.items()
                    if repo_name in specific_repo_names
                }
        if not selected or len(selected) == len(repositories):
            return list(repositories), {
                "active": False,
                "selected_repositories": [entry.display_name for entry in repositories],
                "available_repository_count": len(repositories),
                "matched_aliases": matched_aliases,
            }
        return selected, {
            "active": True,
            "selected_repositories": [entry.display_name for entry in selected],
            "available_repository_count": len(repositories),
            "matched_aliases": matched_aliases,
        }

    def _conversation_context_repository_aliases(self, conversation_context: dict[str, Any]) -> set[str]:
        aliases: set[str] = set()
        raw_names: list[str] = []
        for item in conversation_context.get("repo_scope") or []:
            raw_names.append(str(item or ""))
        for match in conversation_context.get("matches") or []:
            raw_names.append(str(match.get("repo") or ""))
        for raw_name in raw_names:
            aliases.update(self._repository_scope_aliases(raw_name, ""))
        return aliases

    @classmethod
    def _repository_scope_aliases(cls, display_name: str, url: str) -> set[str]:
        raw_values = [display_name]
        if url:
            path = urlsplit(str(url)).path.rstrip("/")
            if path:
                raw_values.append(Path(path).name.removesuffix(".git"))
                parent = Path(path).parent.name
                if parent:
                    raw_values.append(f"{parent} {Path(path).name.removesuffix('.git')}")
        aliases: set[str] = set()
        for raw_value in raw_values:
            normalized = cls._repo_scope_normalize(raw_value)
            if cls._repo_scope_alias_is_useful(normalized):
                aliases.add(normalized)
            for token in normalized.split():
                if cls._repo_scope_alias_is_useful(token):
                    aliases.add(token)
        return aliases

    @staticmethod
    def _repo_scope_normalize(value: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold())).strip()

    @staticmethod
    def _repo_scope_alias_is_useful(alias: str) -> bool:
        if not alias:
            return False
        if alias in {"repo", "repository", "project", "code", "source", "service", "backend", "frontend", "master", "portal", "team", "group", "git", "gitlab"}:
            return False
        return len(alias) >= 4 or alias in {"af", "grc", "crms"}

    @staticmethod
    def _repo_scope_alias_is_specific(alias: str) -> bool:
        normalized = SourceCodeQAService._repo_scope_normalize(alias)
        if not normalized:
            return False
        if " " in normalized:
            return True
        return normalized in {"af", "grc", "crms"} or len(normalized) >= 8

    @staticmethod
    def _repo_alias_in_text(alias: str, normalized_text: str) -> bool:
        if not alias or not normalized_text:
            return False
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])", normalized_text))

    def _all_profile_terms(self, *keys: str) -> list[str]:
        terms: list[str] = []
        for profile in self.load_domain_profiles().values():
            if isinstance(profile, dict):
                terms.extend(self._profile_terms(profile, *keys))
        return list(dict.fromkeys(terms))

    def save_mapping(self, *, pm_team: str, country: str, repositories: list[dict[str, Any]]) -> dict[str, Any]:
        key = self.mapping_key(pm_team, country)
        if not isinstance(repositories, list):
            raise ToolError("Repositories must be submitted as a list.")
        entries = [self._normalize_entry(repo) for repo in repositories]
        payload = self.load_config()
        mappings = dict(payload.get("mappings") or {})
        mappings[key] = [self._entry_to_dict(entry) for entry in entries]
        updated_payload = {
            "version": CONFIG_VERSION,
            "updated_at": self._now_iso(),
            "mappings": mappings,
        }
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(json.dumps(updated_payload, indent=2, sort_keys=True), encoding="utf-8")
        return {"key": key, "repositories": updated_payload["mappings"][key], "config": updated_payload}

    def sync(self, *, pm_team: str, country: str) -> dict[str, Any]:
        key = self.mapping_key(pm_team, country)
        if not self.gitlab_token:
            raise ToolError("SOURCE_CODE_QA_GITLAB_TOKEN is required before Source Code Q&A can sync HTTPS repositories.")
        entries = self._load_entries_for_key(key)
        if not entries:
            return {
                "status": "empty_config",
                "key": key,
                "message": "No repositories are configured for this PM Team and country.",
                "results": [],
            }
        self.repo_root.mkdir(parents=True, exist_ok=True)
        job = self._start_sync_job(key, entries)
        try:
            results = [self._sync_entry(key, entry) for entry in entries]
            status = "ok" if all(result["state"] == "ok" for result in results) else "partial"
            self._finish_sync_job(key, job["job_id"], status=status, results=results)
            return {"status": status, "key": key, "job": self.sync_job_status(key), "results": results, "repo_status": self.repo_status(key)}
        except Exception:
            self._finish_sync_job(key, job["job_id"], status="failed", results=[])
            raise

    def _today(self) -> date:
        return datetime.now().astimezone().date()

    def _auto_sync_start_date(self) -> date:
        raw_value = str(os.getenv("SOURCE_CODE_QA_AUTO_SYNC_START_DATE") or "").strip()
        if raw_value:
            try:
                return date.fromisoformat(raw_value)
            except ValueError:
                return DEFAULT_AUTO_SYNC_START_DATE
        return DEFAULT_AUTO_SYNC_START_DATE

    def _auto_sync_interval_days(self) -> int:
        raw_value = str(os.getenv("SOURCE_CODE_QA_AUTO_SYNC_INTERVAL_DAYS") or "").strip()
        if raw_value:
            try:
                return max(1, int(raw_value))
            except ValueError:
                return DEFAULT_AUTO_SYNC_INTERVAL_DAYS
        return DEFAULT_AUTO_SYNC_INTERVAL_DAYS

    def _latest_completed_scheduled_sync_date(self, today: date) -> date:
        start_date = self._auto_sync_start_date()
        interval_days = self._auto_sync_interval_days()
        if today < start_date:
            return start_date
        periods = (today - start_date).days // interval_days
        return start_date + timedelta(days=periods * interval_days)

    def ensure_synced_today(self, *, pm_team: str, country: str) -> dict[str, Any]:
        key = self.mapping_key(pm_team, country)
        entries = self._load_entries_for_key(key)
        if not entries:
            return {"attempted": False, "status": "empty_config", "reason": "no repositories configured", "key": key}
        repo_status = self.repo_status(key)
        freshness = self._index_freshness_payload(repo_status)
        today = self._today()
        scheduled_sync_date = self._latest_completed_scheduled_sync_date(today)
        if today < scheduled_sync_date:
            return {
                "attempted": False,
                "status": "scheduled",
                "reason": f"next scheduled repository sync is {scheduled_sync_date.isoformat()}",
                "key": key,
                "index_freshness": freshness,
                "next_sync_date": scheduled_sync_date.isoformat(),
                "sync_interval_days": self._auto_sync_interval_days(),
            }
        newest_indexed_at = str(freshness.get("newest_indexed_at") or "").strip()
        last_synced_date = None
        if newest_indexed_at:
            try:
                last_synced_date = datetime.fromisoformat(newest_indexed_at).astimezone().date()
            except ValueError:
                last_synced_date = None
        needs_sync = freshness.get("status") != "fresh" or last_synced_date is None or last_synced_date < scheduled_sync_date
        if not needs_sync:
            return {
                "attempted": False,
                "status": "fresh",
                "reason": f"already synced for scheduled date {scheduled_sync_date.isoformat()}",
                "key": key,
                "index_freshness": freshness,
                "next_sync_date": (scheduled_sync_date + timedelta(days=self._auto_sync_interval_days())).isoformat(),
                "sync_interval_days": self._auto_sync_interval_days(),
            }
        if not self.gitlab_token:
            return {
                "attempted": False,
                "status": "skipped",
                "reason": "SOURCE_CODE_QA_GITLAB_TOKEN is not configured",
                "key": key,
                "index_freshness": freshness,
            }
        try:
            result = self.sync(pm_team=pm_team, country=country)
        except ToolError as error:
            return {
                "attempted": True,
                "status": "failed",
                "reason": str(error),
                "key": key,
                "index_freshness": freshness,
            }
        return {
            "attempted": True,
            "status": str(result.get("status") or "ok"),
            "reason": f"synced for scheduled date {scheduled_sync_date.isoformat()}",
            "key": key,
            "sync": result,
            "next_sync_date": (scheduled_sync_date + timedelta(days=self._auto_sync_interval_days())).isoformat(),
            "sync_interval_days": self._auto_sync_interval_days(),
        }

    def _query_exact_lookup_terms(self, question: str) -> tuple[list[str], list[str]]:
        exact_lookup_terms = self._extract_exact_lookup_terms(question)
        question_specific_terms = self._question_specific_retrieval_terms(question)
        if question_specific_terms:
            specific_exact_terms = [
                str(term or "").strip().lower()
                for term in question_specific_terms
                if "_" in str(term or "")
                and any(marker in str(term or "").lower() for marker in ("_tab", "_table", "process_info", "response_body"))
            ]
            exact_lookup_terms = list(dict.fromkeys([*exact_lookup_terms, *specific_exact_terms]))[:12]
        return exact_lookup_terms, question_specific_terms

    def _synced_query_entries(
        self,
        key: str,
        entries: list[RepositoryEntry],
    ) -> list[tuple[RepositoryEntry, Path]]:
        synced_entries: list[tuple[RepositoryEntry, Path]] = []
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if (repo_path / ".git").exists():
                synced_entries.append((entry, repo_path))
        return synced_entries

    def _queryable_index_entries(
        self,
        key: str,
        synced_entries: list[tuple[RepositoryEntry, Path]],
    ) -> list[tuple[RepositoryEntry, Path]]:
        queryable_entries: list[tuple[RepositoryEntry, Path]] = []
        for entry, repo_path in synced_entries:
            index_info = self._repo_index_info(key, entry, repo_path)
            if index_info.get("state") == "ready" or (index_info.get("state") == "stale" and index_info.get("queryable")):
                queryable_entries.append((entry, repo_path))
        return queryable_entries

    @staticmethod
    def _normalize_answer_mode(answer_mode: str) -> str:
        normalized_answer_mode = str(answer_mode or ANSWER_MODE).strip() or ANSWER_MODE
        if normalized_answer_mode not in {ANSWER_MODE, ANSWER_MODE_AUTO}:
            return ANSWER_MODE_AUTO
        return normalized_answer_mode

    @staticmethod
    def _answer_mode_requests_llm(normalized_answer_mode: str) -> bool:
        return normalized_answer_mode == ANSWER_MODE_AUTO

    @staticmethod
    def _query_uses_simple_quality_trace(intent: dict[str, Any]) -> bool:
        return (
            any(intent.get(intent_key) for intent_key in ("rule_logic", "api", "config"))
            and not any(
                intent.get(intent_key)
                for intent_key in (
                    "data_source",
                    "module_dependency",
                    "static_qa",
                    "impact_analysis",
                    "test_coverage",
                    "operational_boundary",
                )
            )
        )

    @staticmethod
    def _report_query_progress(
        progress_callback: Any | None,
        stage: str,
        message: str,
        current: int = 0,
        total: int = 0,
    ) -> None:
        if not progress_callback:
            return
        try:
            progress_callback(stage, message, current, total)
        except Exception:
            return

    def _query_success_payload(
        self,
        *,
        question: str,
        answer_mode: str,
        matches: list[dict[str, Any]],
        trace_paths: list[dict[str, Any]],
        repo_graph: dict[str, Any],
        evidence_summary: dict[str, Any],
        evidence_pack: dict[str, Any],
        repo_status: list[dict[str, Any]],
        index_freshness: dict[str, Any],
        answer_quality: dict[str, Any],
        query_plan: dict[str, Any],
        exact_lookup_terms: list[str],
        exact_lookup_matched_terms: list[str],
        exact_match_count: int,
        exact_lookup_sufficient: bool,
        tool_trace: list[dict[str, Any]],
        request_cache: dict[str, Any],
        followup_context: dict[str, Any],
        repository_scope: dict[str, Any],
        original_question: str,
        trace_id: str,
        query_mode: str,
        retrieval_latency_ms: int,
    ) -> dict[str, Any]:
        return {
            "status": "ok",
            "answer_mode": answer_mode,
            "summary": self._build_summary(matches),
            "matches": matches,
            "citations": self._build_citations(matches),
            "trace_paths": trace_paths,
            "repo_graph": repo_graph,
            "evidence_pack": evidence_pack,
            "evidence_outline": self._build_evidence_outline(evidence_pack, matches),
            "repo_status": repo_status,
            "index_freshness": index_freshness,
            "answer_quality": answer_quality,
            "agent_plan": self._build_agent_plan(question, evidence_summary, answer_quality),
            "query_plan": query_plan,
            "exact_lookup": {
                "terms": exact_lookup_terms,
                "matched_terms": exact_lookup_matched_terms,
                "match_count": exact_match_count,
                "sufficient": exact_lookup_sufficient,
            },
            "tool_trace": tool_trace,
            "retrieval_runtime": self._retrieval_cache_stats(request_cache),
            "followup_context": followup_context,
            "repository_scope": repository_scope,
            "original_question": original_question,
            "trace_id": trace_id,
            "query_mode": query_mode,
            "deadline_seconds": 0,
            "deadline_hit": False,
            "retrieval_latency_ms": retrieval_latency_ms,
            "codex_latency_ms": 0,
            "fallback_used": False,
            "background_deep_job_id": "",
        }

    def _record_empty_query_payload(
        self,
        *,
        key: str,
        question: str,
        answer_mode: str,
        llm_budget_mode: str,
        started_at: float,
        status: str,
        summary: str,
        trace_id: str,
        repo_status: list[dict[str, Any]] | None = None,
        index_freshness: dict[str, Any] | None = None,
        query_mode: str = QUERY_MODE_DEEP,
        extra_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self._empty_query_payload(
            key,
            repo_status=repo_status,
            index_freshness=index_freshness,
            status=status,
            summary=summary,
            trace_id=trace_id,
        )
        if extra_fields:
            payload.update(extra_fields)
        return self._record_query_payload(
            key=key,
            question=question,
            answer_mode=answer_mode,
            llm_budget_mode=llm_budget_mode,
            query_mode=query_mode,
            payload=payload,
            started_at=started_at,
        )

    def query(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        limit: int = 12,
        answer_mode: str = ANSWER_MODE,
        llm_budget_mode: str = "cheap",
        query_mode: str = QUERY_MODE_DEEP,
        conversation_context: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
        progress_callback: Any | None = None,
        effort_assessment: bool = False,
    ) -> dict[str, Any]:
        key = self.mapping_key(pm_team, country)
        question = str(question or "").strip()
        query_mode = self.normalize_query_mode(query_mode)
        started_at = time.time()
        trace_id = uuid.uuid4().hex
        report = functools.partial(self._report_query_progress, progress_callback)

        report("validating", "Validating question and repository scope.", 0, 0)
        if not question:
            raise ToolError("Please enter a source-code question.")
        original_question = question
        entries = self._load_entries_for_key(key)
        if not entries:
            return self._record_empty_query_payload(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                started_at=started_at,
                status="empty_config",
                summary="No repositories are configured for this PM Team and country yet.",
                trace_id=trace_id,
            )
        query_entries, repository_scope = self._filter_entries_for_question_repository_scope(original_question, entries)
        question, followup_context = self._apply_conversation_context(
            question,
            conversation_context,
            current_key=key,
            current_repositories=entries,
        )
        tokens = self._question_tokens(question)
        if not tokens:
            return self._record_empty_query_payload(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                started_at=started_at,
                status="weak_question",
                summary="No confident match. Try adding exact class, API, table, field, or function names.",
                trace_id=trace_id,
            )
        domain_profile = self._domain_profile(pm_team, country)
        tokens = self._expand_tokens_with_domain_profile(tokens, question, domain_profile)
        query_plan = self._build_query_decomposition(question, domain_profile)
        tool_trace: list[dict[str, Any]] = []
        matches: list[dict[str, Any]] = []
        request_cache = self._new_retrieval_request_cache()
        result_limit = self._query_result_limit(limit)
        cross_repo_context = self._requires_cross_repo_context(question)
        if cross_repo_context and repository_scope.get("active"):
            repository_scope = {
                **repository_scope,
                "active": False,
                "skipped": "cross_repo_context",
                "seed_repositories": repository_scope.get("selected_repositories") or [],
                "selected_repositories": [entry.display_name for entry in entries],
            }
            query_entries = entries
        repo_status = self.repo_status(key)
        index_freshness = self._index_freshness_payload(repo_status)
        exact_lookup_terms, question_specific_terms = self._query_exact_lookup_terms(question)
        exact_matches: list[dict[str, Any]] = []
        latency_guarded_query_expansion = False
        synced_entries = self._synced_query_entries(key, query_entries)
        if repository_scope.get("active"):
            self._increment_retrieval_stat(request_cache, "repository_scope_filters")
            report(
                "repository_scope",
                "Question names specific repositories; limiting retrieval scope.",
                len(query_entries),
                len(entries),
            )
        if not synced_entries:
            return self._record_empty_query_payload(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                started_at=started_at,
                repo_status=repo_status,
                index_freshness=index_freshness,
                status="not_synced",
                summary="No synced repositories are available in the selected scope. Run Sync / Refresh before asking code questions.",
                trace_id=trace_id,
                extra_fields={
                    "repository_scope": repository_scope,
                    "retrieval_runtime": self._retrieval_cache_stats(request_cache),
                },
            )
        queryable_entries = self._queryable_index_entries(key, synced_entries)
        if not queryable_entries:
            self._increment_retrieval_stat(request_cache, "index_not_ready_scopes")
            return self._record_empty_query_payload(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                started_at=started_at,
                repo_status=repo_status,
                index_freshness=index_freshness,
                status="index_not_ready",
                summary="Synced repositories exist in the selected scope, but none has a ready queryable index. Run Sync / Refresh before trusting code answers.",
                trace_id=trace_id,
                extra_fields={
                    "repository_scope": repository_scope,
                    "retrieval_runtime": self._retrieval_cache_stats(request_cache),
                },
            )
        intent = query_plan.get("intent") if isinstance(query_plan.get("intent"), dict) else {}
        simple_quality_trace = self._query_uses_simple_quality_trace(intent)
        if exact_lookup_terms:
            report(
                "exact_lookup",
                f"Checking exact table/path references for {len(exact_lookup_terms)} term{'s' if len(exact_lookup_terms) != 1 else ''}.",
                0,
                len(synced_entries),
            )
        for index, (entry, repo_path) in enumerate(synced_entries, start=1):
            if exact_lookup_terms:
                exact_matches.extend(
                    self._exact_table_path_lookup_repo(
                        entry,
                        repo_path,
                        exact_lookup_terms,
                        question=question,
                        request_cache=request_cache,
                    )
                )
                report("exact_lookup", f"Checked exact references in {entry.display_name}.", index, len(synced_entries))
        exact_lookup_matched_terms = sorted(
            {
                str((match.get("exact_lookup") or {}).get("term") or "").lower()
                for match in exact_matches
                if (match.get("exact_lookup") or {}).get("term")
            }
        )
        exact_lookup_sufficient = self._exact_lookup_is_sufficient(exact_lookup_terms, exact_matches) and not cross_repo_context
        if exact_matches:
            matches.extend(exact_matches)
        elif exact_lookup_terms and self._exact_lookup_miss_should_stop(exact_lookup_terms):
            report("completed", "Exact references were not found in the current indexes.", 0, 0)
            return self._record_empty_query_payload(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                started_at=started_at,
                repo_status=repo_status,
                index_freshness=index_freshness,
                status="no_match",
                summary="No exact table/path references were found in the indexed repositories.",
                trace_id=trace_id,
                extra_fields={
                    "exact_lookup_terms": exact_lookup_terms,
                    "repository_scope": repository_scope,
                },
            )
        elif exact_lookup_terms:
            self._increment_retrieval_stat(request_cache, "exact_lookup_soft_misses")
            report("exact_lookup", "Exact dotted references were not found; falling back to broader token retrieval.", 0, 0)
        if question_specific_terms and not exact_lookup_sufficient:
            specific_tokens: list[str] = []
            for term in question_specific_terms:
                lowered_term = str(term or "").strip().lower()
                if lowered_term:
                    specific_tokens.append(lowered_term)
                specific_tokens.extend(self._question_tokens(str(term or "")))
            specific_tokens = list(dict.fromkeys(term for term in specific_tokens if len(term) >= 3))[:32]
            report("focused_search", "Checking domain-specific storage and processing symbols.", 0, len(synced_entries))
            for index, (entry, repo_path) in enumerate(synced_entries, start=1):
                matches.extend(
                    self._search_repo(
                        entry,
                        repo_path,
                        specific_tokens,
                        question=question,
                        focus_terms=question_specific_terms,
                        trace_stage="focused_search",
                        request_cache=request_cache,
                    )
                )
                report("focused_search", f"Checked domain-specific symbols in {entry.display_name}.", index, len(synced_entries))
        if not exact_lookup_sufficient:
            direct_context = self._query_direct_and_decomposed_matches(
                question=question,
                matches=matches,
                tokens=tokens,
                synced_entries=synced_entries,
                simple_quality_trace=simple_quality_trace,
                started_at=started_at,
                result_limit=result_limit,
                limit=limit,
                query_plan=query_plan,
                request_cache=request_cache,
                report=report,
            )
            matches = direct_context["matches"]
            latency_guarded_query_expansion = direct_context["latency_guarded_query_expansion"]
        top_matches, should_expand_matches = self._rank_and_expand_query_matches(
            question=question,
            matches=matches,
            exact_matches=exact_matches,
            result_limit=result_limit,
            exact_lookup_sufficient=exact_lookup_sufficient,
            simple_quality_trace=simple_quality_trace,
            synced_entries=synced_entries,
            started_at=started_at,
            latency_guarded_query_expansion=latency_guarded_query_expansion,
            query_entries=query_entries,
            key=key,
            limit=limit,
            query_plan=query_plan,
            tool_trace=tool_trace,
            request_cache=request_cache,
            report=report,
        )
        if index_freshness.get("status") != "fresh":
            repo_status = self.repo_status(key)
            index_freshness = self._index_freshness_payload(repo_status)
        if not top_matches:
            return self._record_empty_query_payload(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                started_at=started_at,
                repo_status=repo_status,
                index_freshness=index_freshness,
                status="no_match",
                summary="No confident match. Try exact symbols, route paths, table names, or error codes.",
                trace_id=trace_id,
                query_mode=query_mode,
                extra_fields={"repository_scope": repository_scope},
            )
        retrieval_latency_ms = int((time.time() - started_at) * 1000)
        normalized_answer_mode = self._normalize_answer_mode(answer_mode)
        report(
            "evidence_pack",
            "Building evidence pack and answer context.",
            0,
            0,
        )
        evidence_summary, quality_gate, trace_paths, repo_graph, evidence_pack = self._build_query_answer_context(
            question=question,
            matches=top_matches,
            entries=query_entries,
            key=key,
            exact_lookup_sufficient=exact_lookup_sufficient,
            should_expand_matches=should_expand_matches,
            request_cache=request_cache,
        )
        payload = self._query_success_payload(
            question=question,
            answer_mode=normalized_answer_mode,
            matches=top_matches,
            trace_paths=trace_paths,
            repo_graph=repo_graph,
            evidence_summary=evidence_summary,
            evidence_pack=evidence_pack,
            repo_status=repo_status,
            index_freshness=index_freshness,
            answer_quality=quality_gate,
            query_plan=query_plan,
            exact_lookup_terms=exact_lookup_terms,
            exact_lookup_matched_terms=exact_lookup_matched_terms,
            exact_match_count=len(exact_matches),
            exact_lookup_sufficient=exact_lookup_sufficient,
            tool_trace=tool_trace,
            request_cache=request_cache,
            followup_context=followup_context,
            repository_scope=repository_scope,
            original_question=original_question,
            trace_id=trace_id,
            query_mode=query_mode,
            retrieval_latency_ms=retrieval_latency_ms,
        )
        if self._answer_mode_requests_llm(normalized_answer_mode):
            self._augment_query_payload_with_llm_answer(
                payload=payload,
                entries=query_entries,
                key=key,
                pm_team=pm_team,
                country=country,
                question=question,
                matches=top_matches,
                llm_budget_mode=llm_budget_mode,
                query_mode=query_mode,
                trace_id=trace_id,
                followup_context=followup_context,
                normalized_answer_mode=normalized_answer_mode,
                request_cache=request_cache,
                progress_callback=progress_callback,
                attachments=attachments,
                runtime_evidence=runtime_evidence,
                effort_assessment=effort_assessment,
                retrieval_latency_ms=retrieval_latency_ms,
                evidence_pack=evidence_pack,
                report=report,
            )
            report("completed", "LLM answer generated.", 0, 0)
        if not (self._answer_mode_requests_llm(normalized_answer_mode) and payload.get("llm_answer")):
            report("completed", "Code evidence retrieval completed.", 0, 0)
        return self._record_query_payload(
            key=key,
            question=question,
            answer_mode=answer_mode,
            llm_budget_mode=llm_budget_mode,
            query_mode=query_mode,
            payload=payload,
            started_at=started_at,
        )

    def _should_expand_query_matches(
        self,
        *,
        question: str,
        top_matches: list[dict[str, Any]],
        exact_lookup_sufficient: bool,
        simple_quality_trace: bool,
        synced_entries: list[tuple[RepositoryEntry, Path]],
        started_at: float,
        latency_guarded_query_expansion: bool,
        request_cache: dict[str, Any],
        report: Any,
    ) -> bool:
        should_expand_matches = not exact_lookup_sufficient
        if top_matches and should_expand_matches and simple_quality_trace:
            early_evidence_summary = self._compress_evidence_cached(question, top_matches, request_cache=request_cache)
            early_quality_gate = self._quality_gate_cached(question, early_evidence_summary, request_cache=request_cache)
            if (
                early_quality_gate.get("status") == "sufficient"
                and early_quality_gate.get("confidence") in {"medium", "high"}
                and (len(synced_entries) >= 5 or time.time() - started_at >= 6.0)
            ):
                self._increment_retrieval_stat(request_cache, "early_quality_short_circuits")
                report("quality_gate", "Ranked evidence is sufficient; skipping deeper local expansion.", 0, 0)
                return False
        if top_matches and should_expand_matches and latency_guarded_query_expansion:
            self._increment_retrieval_stat(request_cache, "deep_expansion_latency_guards")
            report("quality_gate", "Skipping deeper expansion because retrieval already hit the latency guard.", 0, 0)
            return False
        if top_matches and should_expand_matches and time.time() - started_at >= 8.0:
            self._increment_retrieval_stat(request_cache, "deep_expansion_latency_guards")
            report("quality_gate", "Skipping deeper expansion because retrieval already has enough evidence and hit the latency guard.", 0, 0)
            return False
        return should_expand_matches

    def _rank_and_expand_query_matches(
        self,
        *,
        question: str,
        matches: list[dict[str, Any]],
        exact_matches: list[dict[str, Any]],
        result_limit: int,
        exact_lookup_sufficient: bool,
        simple_quality_trace: bool,
        synced_entries: list[tuple[RepositoryEntry, Path]],
        started_at: float,
        latency_guarded_query_expansion: bool,
        query_entries: list[RepositoryEntry],
        key: str,
        limit: int,
        query_plan: dict[str, Any],
        tool_trace: list[dict[str, Any]],
        request_cache: dict[str, Any],
        report: Any,
    ) -> tuple[list[dict[str, Any]], bool]:
        return self._retrieval.rank_and_expand_query_matches(
            question=question,
            matches=matches,
            exact_matches=exact_matches,
            result_limit=result_limit,
            exact_lookup_sufficient=exact_lookup_sufficient,
            simple_quality_trace=simple_quality_trace,
            synced_entries=synced_entries,
            started_at=started_at,
            latency_guarded_query_expansion=latency_guarded_query_expansion,
            query_entries=query_entries,
            key=key,
            limit=limit,
            query_plan=query_plan,
            tool_trace=tool_trace,
            request_cache=request_cache,
            report=report,
        )

    def _rank_and_expand_query_matches_impl(
        self,
        *,
        question: str,
        matches: list[dict[str, Any]],
        exact_matches: list[dict[str, Any]],
        result_limit: int,
        exact_lookup_sufficient: bool,
        simple_quality_trace: bool,
        synced_entries: list[tuple[RepositoryEntry, Path]],
        started_at: float,
        latency_guarded_query_expansion: bool,
        query_entries: list[RepositoryEntry],
        key: str,
        limit: int,
        query_plan: dict[str, Any],
        tool_trace: list[dict[str, Any]],
        request_cache: dict[str, Any],
        report: Any,
    ) -> tuple[list[dict[str, Any]], bool]:
        report("ranking", "Ranking matched files and snippets.", 0, 0)
        matches = self._rank_matches(question, matches, request_cache=request_cache)
        top_matches = (
            self._select_result_matches(matches, result_limit, question=question)
            if exact_matches
            else matches[:result_limit]
        )
        should_expand_matches = self._should_expand_query_matches(
            question=question,
            top_matches=top_matches,
            exact_lookup_sufficient=exact_lookup_sufficient,
            simple_quality_trace=simple_quality_trace,
            synced_entries=synced_entries,
            started_at=started_at,
            latency_guarded_query_expansion=latency_guarded_query_expansion,
            request_cache=request_cache,
            report=report,
        )
        if top_matches and should_expand_matches and self._is_dependency_question(question):
            report("dependency_expansion", "Expanding dependency evidence from top matches.", 0, 0)
            dependency_matches = self._expand_dependency_matches(
                entries=query_entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
                request_cache=request_cache,
            )
            if dependency_matches:
                top_matches = self._merge_expanded_matches(
                    question=question,
                    current_matches=top_matches,
                    expanded_matches=dependency_matches,
                    limit=limit,
                    request_cache=request_cache,
                )
        if top_matches and should_expand_matches:
            report("two_hop_expansion", "Expanding two-hop evidence from top matches.", 0, 0)
            trace_matches = self._expand_two_hop_matches(
                entries=query_entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
                request_cache=request_cache,
            )
            if trace_matches:
                top_matches = self._merge_expanded_matches(
                    question=question,
                    current_matches=top_matches,
                    expanded_matches=trace_matches,
                    limit=limit,
                    request_cache=request_cache,
                )
        if top_matches and should_expand_matches:
            report("agent_trace", "Tracing related entities from top matches.", 0, 0)
            agent_matches = self._expand_agent_trace_matches(
                entries=query_entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
                request_cache=request_cache,
            )
            if agent_matches:
                top_matches = self._merge_expanded_matches(
                    question=question,
                    current_matches=top_matches,
                    expanded_matches=agent_matches,
                    limit=limit,
                    request_cache=request_cache,
                )
        if top_matches and should_expand_matches:
            report("tool_loop", "Running targeted local investigation tools.", 0, 0)
            tool_loop_matches = self._run_planner_tool_loop(
                entries=query_entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
                tool_trace=tool_trace,
                request_cache=request_cache,
            )
            if tool_loop_matches:
                top_matches = self._merge_expanded_matches(
                    question=question,
                    current_matches=top_matches,
                    expanded_matches=tool_loop_matches,
                    limit=limit,
                    request_cache=request_cache,
                )
        if top_matches and should_expand_matches:
            report("quality_gate", "Checking evidence sufficiency before deeper expansion.", 0, 0)
            evidence_summary = self._compress_evidence_cached(question, top_matches, request_cache=request_cache)
            quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
            agent_plan = self._build_agent_plan(question, evidence_summary, quality_gate)
            if quality_gate.get("status") != "sufficient" and agent_plan.get("steps"):
                report("agent_plan", "Running additional local plan because evidence is incomplete.", 0, 0)
                top_matches = self._run_agent_plan(
                    entries=query_entries,
                    key=key,
                    question=question,
                    matches=top_matches,
                    evidence_summary=evidence_summary,
                    quality_gate=quality_gate,
                    agent_plan=agent_plan,
                    limit=limit,
                    tool_trace=tool_trace,
                    request_cache=request_cache,
                )
        if top_matches and should_expand_matches:
            evidence_summary = self._compress_evidence_cached(question, top_matches, request_cache=request_cache)
            quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
            agent_plan = self._build_agent_plan(question, evidence_summary, quality_gate)
            if quality_gate.get("status") != "sufficient" and agent_plan.get("steps"):
                report("agent_plan", "Running final local plan to fill missing evidence.", 0, 0)
                top_matches = self._run_agent_plan(
                    entries=query_entries,
                    key=key,
                    question=question,
                    matches=top_matches,
                    evidence_summary=evidence_summary,
                    quality_gate=quality_gate,
                    agent_plan=agent_plan,
                    limit=limit,
                    tool_trace=tool_trace,
                    request_cache=request_cache,
                )
        if top_matches and should_expand_matches and query_plan.get("intent", {}).get("impact_analysis"):
            report("impact_analysis", "Expanding impact analysis evidence.", 0, 0)
            impact_matches = self._expand_impact_matches(
                entries=query_entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
                request_cache=request_cache,
            )
            if impact_matches:
                top_matches = self._merge_expanded_matches(
                    question=question,
                    current_matches=top_matches,
                    expanded_matches=impact_matches,
                    limit=limit,
                    request_cache=request_cache,
                )
        return top_matches, should_expand_matches

    def _query_direct_and_decomposed_matches(
        self,
        *,
        question: str,
        matches: list[dict[str, Any]],
        tokens: list[str],
        synced_entries: list[tuple[RepositoryEntry, Path]],
        simple_quality_trace: bool,
        started_at: float,
        result_limit: int,
        limit: int,
        query_plan: dict[str, Any],
        request_cache: dict[str, Any],
        report: Any,
    ) -> dict[str, Any]:
        return self._retrieval.query_direct_and_decomposed_matches(
            question=question,
            matches=matches,
            tokens=tokens,
            synced_entries=synced_entries,
            simple_quality_trace=simple_quality_trace,
            started_at=started_at,
            result_limit=result_limit,
            limit=limit,
            query_plan=query_plan,
            request_cache=request_cache,
            report=report,
        )

    def _query_direct_and_decomposed_matches_impl(
        self,
        *,
        question: str,
        matches: list[dict[str, Any]],
        tokens: list[str],
        synced_entries: list[tuple[RepositoryEntry, Path]],
        simple_quality_trace: bool,
        started_at: float,
        result_limit: int,
        limit: int,
        query_plan: dict[str, Any],
        request_cache: dict[str, Any],
        report: Any,
    ) -> dict[str, Any]:
        latency_guarded_query_expansion = False
        report("direct_search", f"Searching direct matches across {len(synced_entries)} repos.", 0, len(synced_entries))
        skip_broad_query_decomposition = False
        for index, (entry, repo_path) in enumerate(synced_entries, start=1):
            matches.extend(self._search_repo(entry, repo_path, tokens, question=question, request_cache=request_cache))
            report("direct_search", f"Searching direct matches in {entry.display_name}.", index, len(synced_entries))
            if matches and simple_quality_trace and index >= 3 and (len(matches) >= 80 or time.time() - started_at >= 4.0):
                direct_ranked = self._rank_matches(question, matches, request_cache=request_cache)
                direct_top = self._select_result_matches(direct_ranked, self._query_result_limit(limit), question=question)
                direct_evidence_summary = self._compress_evidence_cached(question, direct_top, request_cache=request_cache)
                direct_quality_gate = self._quality_gate_cached(question, direct_evidence_summary, request_cache=request_cache)
                if (
                    direct_quality_gate.get("status") == "sufficient"
                    and direct_quality_gate.get("confidence") in {"medium", "high"}
                    and (len(synced_entries) >= 5 or time.time() - started_at >= 6.0)
                ):
                    skip_broad_query_decomposition = True
                    self._increment_retrieval_stat(request_cache, "direct_scan_early_stops")
                    report(
                        "quality_gate",
                        "Direct matches are sufficient; stopping repository scan early.",
                        index,
                        len(synced_entries),
                    )
                    break
                if time.time() - started_at >= 6.0 and len(matches) >= max(60, result_limit * 5):
                    skip_broad_query_decomposition = True
                    self._increment_retrieval_stat(request_cache, "simple_latency_guards")
                    report(
                        "quality_gate",
                        "Direct scan has enough candidate evidence; stopping early to keep the response responsive.",
                        index,
                        len(synced_entries),
                    )
                    break
        if matches and simple_quality_trace:
            direct_ranked = self._rank_matches(question, matches, request_cache=request_cache)
            direct_top = self._select_result_matches(direct_ranked, self._query_result_limit(limit), question=question)
            direct_evidence_summary = self._compress_evidence_cached(question, direct_top, request_cache=request_cache)
            direct_quality_gate = self._quality_gate_cached(question, direct_evidence_summary, request_cache=request_cache)
            if (
                direct_quality_gate.get("status") == "sufficient"
                and direct_quality_gate.get("confidence") in {"medium", "high"}
                and (len(synced_entries) >= 5 or time.time() - started_at >= 6.0)
            ):
                skip_broad_query_decomposition = True
                self._increment_retrieval_stat(request_cache, "direct_quality_short_circuits")
                report(
                    "quality_gate",
                    "Direct matches are sufficient; skipping broader evidence expansion.",
                    len(synced_entries),
                    len(synced_entries),
                )
        if matches and not skip_broad_query_decomposition and time.time() - started_at >= 7.0 and len(matches) >= max(36, result_limit * 3):
            skip_broad_query_decomposition = True
            latency_guarded_query_expansion = True
            self._increment_retrieval_stat(request_cache, "query_decomposition_latency_guards")
            report(
                "quality_gate",
                "Direct scan has enough candidate evidence; skipping query expansion to keep the response responsive.",
                len(synced_entries),
                len(synced_entries),
            )
        if not skip_broad_query_decomposition:
            report("query_decomposition", "Expanding query terms from domain and intent profile.", 0, len(synced_entries))
        for index, (entry, repo_path) in enumerate(synced_entries, start=1):
            if skip_broad_query_decomposition:
                continue
            for component in query_plan.get("components") or []:
                component_terms = [str(term) for term in component.get("terms") or [] if str(term).strip()]
                expanded_tokens: list[str] = []
                for term in component_terms:
                    expanded_tokens.extend(self._question_tokens(term))
                expanded_tokens = list(dict.fromkeys(expanded_tokens))[:24]
                if not expanded_tokens:
                    continue
                matches.extend(
                    self._search_repo(
                        entry,
                        repo_path,
                        expanded_tokens,
                        question=question,
                        focus_terms=component_terms,
                        trace_stage="query_decomposition",
                        request_cache=request_cache,
                    )
                )
            report("query_decomposition", f"Expanded query terms in {entry.display_name}.", index, len(synced_entries))
            if query_plan.get("intent", {}).get("static_qa"):
                matches.extend(
                    self._tool_find_static_findings(
                        entry,
                        repo_path,
                        tokens,
                        question,
                        0,
                        request_cache=request_cache,
                    )
                )
            if query_plan.get("intent", {}).get("test_coverage"):
                matches.extend(
                    self._tool_find_test_coverage(
                        entry,
                        repo_path,
                        tokens,
                        question,
                        0,
                        request_cache=request_cache,
                    )
                )
            if query_plan.get("intent", {}).get("operational_boundary"):
                matches.extend(
                    self._tool_find_operational_boundaries(
                        entry,
                        repo_path,
                        tokens,
                        question,
                        0,
                        request_cache=request_cache,
                    )
                )
        return {
            "matches": matches,
            "latency_guarded_query_expansion": latency_guarded_query_expansion,
        }

    def _build_query_answer_context(
        self,
        *,
        question: str,
        matches: list[dict[str, Any]],
        entries: list[RepositoryEntry],
        key: str,
        exact_lookup_sufficient: bool,
        should_expand_matches: bool,
        request_cache: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        return self._retrieval.build_query_answer_context(
            question=question,
            matches=matches,
            entries=entries,
            key=key,
            exact_lookup_sufficient=exact_lookup_sufficient,
            should_expand_matches=should_expand_matches,
            request_cache=request_cache,
        )

    def _build_query_answer_context_impl(
        self,
        *,
        question: str,
        matches: list[dict[str, Any]],
        entries: list[RepositoryEntry],
        key: str,
        exact_lookup_sufficient: bool,
        should_expand_matches: bool,
        request_cache: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        evidence_summary = self._compress_evidence_cached(question, matches, request_cache=request_cache)
        quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
        trace_paths = (
            []
            if exact_lookup_sufficient or not should_expand_matches
            else self._build_trace_paths(
                entries=entries,
                key=key,
                matches=matches,
                question=question,
                request_cache=request_cache,
            )
        )
        if trace_paths:
            evidence_summary["trace_paths"] = trace_paths
        repo_graph = (
            {
                "version": 2,
                "nodes": [{"name": entry.display_name, "url": entry.url} for entry in entries],
                "edges": [],
                "skipped": "exact_lookup_sufficient",
            }
            if exact_lookup_sufficient or not should_expand_matches
            else self._build_repo_dependency_graph(key=key, entries=entries, request_cache=request_cache)
        )
        evidence_pack = self._build_evidence_pack(
            question=question,
            evidence_summary=evidence_summary,
            matches=matches,
            trace_paths=trace_paths,
            quality_gate=quality_gate,
        )
        return evidence_summary, quality_gate, trace_paths, repo_graph, evidence_pack

    def _augment_query_payload_with_llm_answer(
        self,
        *,
        payload: dict[str, Any],
        entries: list[RepositoryEntry],
        key: str,
        pm_team: str,
        country: str,
        question: str,
        matches: list[dict[str, Any]],
        llm_budget_mode: str,
        query_mode: str,
        trace_id: str,
        followup_context: dict[str, Any] | None,
        normalized_answer_mode: str,
        request_cache: dict[str, Any],
        progress_callback: Any | None,
        attachments: list[dict[str, Any]] | None,
        runtime_evidence: list[dict[str, Any]] | None,
        effort_assessment: bool,
        retrieval_latency_ms: int,
        evidence_pack: dict[str, Any],
        report: Any,
    ) -> None:
        self._answer_generation.augment_query_payload_with_llm_answer(
            payload=payload,
            entries=entries,
            key=key,
            pm_team=pm_team,
            country=country,
            question=question,
            matches=matches,
            llm_budget_mode=llm_budget_mode,
            query_mode=query_mode,
            trace_id=trace_id,
            followup_context=followup_context,
            normalized_answer_mode=normalized_answer_mode,
            request_cache=request_cache,
            progress_callback=progress_callback,
            attachments=attachments,
            runtime_evidence=runtime_evidence,
            effort_assessment=effort_assessment,
            retrieval_latency_ms=retrieval_latency_ms,
            evidence_pack=evidence_pack,
            report=report,
        )

    def _augment_query_payload_with_llm_answer_impl(
        self,
        *,
        payload: dict[str, Any],
        entries: list[RepositoryEntry],
        key: str,
        pm_team: str,
        country: str,
        question: str,
        matches: list[dict[str, Any]],
        llm_budget_mode: str,
        query_mode: str,
        trace_id: str,
        followup_context: dict[str, Any] | None,
        normalized_answer_mode: str,
        request_cache: dict[str, Any],
        progress_callback: Any | None,
        attachments: list[dict[str, Any]] | None,
        runtime_evidence: list[dict[str, Any]] | None,
        effort_assessment: bool,
        retrieval_latency_ms: int,
        evidence_pack: dict[str, Any],
        report: Any,
    ) -> None:
        llm_service = self
        payload["llm_provider"] = llm_service.llm_provider.name
        if llm_service.llm_provider.name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
            report("llm_generation", f"Scoped Codex search · {pm_team}:{country}. Retrieval is navigation hints.", 0, 0)
        else:
            report("llm_generation", "Calling LLM with retrieved evidence.", 0, 0)
        llm_payload = llm_service._build_llm_answer(
            entries=entries,
            key=key,
            pm_team=pm_team,
            country=country,
            question=question,
            matches=matches,
            llm_budget_mode=llm_budget_mode,
            query_mode=query_mode,
            trace_id=trace_id,
            followup_context=followup_context,
            requested_answer_mode=normalized_answer_mode,
            request_cache=request_cache,
            progress_callback=progress_callback,
            attachments=attachments or [],
            runtime_evidence=runtime_evidence or [],
            effort_assessment=effort_assessment,
        )
        payload.update(llm_payload)
        payload["query_mode"] = query_mode
        payload["requested_query_mode"] = payload.get("requested_query_mode") or ""
        payload["deadline_seconds"] = 0
        payload["retrieval_latency_ms"] = retrieval_latency_ms
        payload["codex_latency_ms"] = payload.get("llm_latency_ms") if llm_service.llm_provider.name == LLM_PROVIDER_CODEX_CLI_BRIDGE else 0
        if isinstance(payload.get("llm_route"), dict):
            payload["llm_route"]["retrieval_hints_ms"] = retrieval_latency_ms
        if isinstance(payload.get("codex_cli_summary"), dict):
            payload["codex_cli_summary"]["retrieval_hints_ms"] = retrieval_latency_ms
        payload["background_deep_job_id"] = payload.get("background_deep_job_id") or ""
        payload["evidence_outline"] = self._build_evidence_outline(payload.get("evidence_pack") or evidence_pack, matches)
        payload["answer_mode"] = normalized_answer_mode

    def _merge_expanded_matches(
        self,
        *,
        question: str,
        current_matches: list[dict[str, Any]],
        expanded_matches: list[dict[str, Any]],
        limit: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in current_matches}
        for item in expanded_matches:
            item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
            if item_key not in existing_keys:
                current_matches.append(item)
                existing_keys.add(item_key)
            else:
                self._annotate_duplicate_tool_match(current_matches, item)
        ranked_matches = self._rank_matches(question, current_matches, request_cache=request_cache)
        return self._select_result_matches(ranked_matches, self._query_result_limit(limit), question=question)

    def _record_query_payload(
        self,
        *,
        key: str,
        question: str,
        answer_mode: str,
        llm_budget_mode: str,
        payload: dict[str, Any],
        started_at: float,
        query_mode: str = QUERY_MODE_DEEP,
    ) -> dict[str, Any]:
        self._record_query_telemetry(
            key=key,
            question=question,
            answer_mode=answer_mode,
            llm_budget_mode=llm_budget_mode,
            query_mode=query_mode,
            payload=payload,
            started_at=started_at,
        )
        return payload

    @staticmethod
    def _query_result_limit(limit: int) -> int:
        return max(1, min(int(limit or 12), 30))

    def repo_status(self, key: str) -> list[dict[str, Any]]:
        entries = self._load_entries_for_key(key)
        statuses = []
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            synced = (repo_path / ".git").exists()
            index_info = self._repo_index_info(key, entry, repo_path) if synced else {"state": "missing"}
            statuses.append(
                {
                    "display_name": entry.display_name,
                    "url": entry.url,
                    "path": str(repo_path),
                    "state": "synced" if synced else "not_synced",
                    "message": (
                        f"Local clone is ready. Index: {index_info.get('state')}."
                        if synced
                        else "Run Sync / Refresh before asking questions."
                    ),
                    "index": index_info,
                    "sync_job": self.sync_job_status(key),
                }
            )
        return statuses

    def index_health_payload(self) -> dict[str, Any]:
        config = self.load_config()
        mappings = config.get("mappings") or {}
        keys: dict[str, Any] = {}
        totals = {
            "repos": 0,
            "ready": 0,
            "stale_or_missing": 0,
            "files": 0,
            "lines": 0,
            "definitions": 0,
            "references": 0,
            "semantic_chunks": 0,
            "tree_sitter_files": 0,
            "tree_sitter_errors": 0,
        }
        oldest_indexed_at = ""
        newest_indexed_at = ""
        for key in sorted(mappings):
            statuses = self.repo_status(key)
            freshness = self._index_freshness_payload(statuses)
            keys[key] = {"freshness": freshness, "repos": statuses}
            totals["repos"] += len(statuses)
            for item in statuses:
                index = item.get("index") or {}
                if index.get("state") == "ready":
                    totals["ready"] += 1
                else:
                    totals["stale_or_missing"] += 1
                for field in (
                    "files",
                    "lines",
                    "definitions",
                    "references",
                    "semantic_chunks",
                    "tree_sitter_files",
                    "tree_sitter_errors",
                ):
                    totals[field] += int(index.get(field) or 0)
                updated_at = str(index.get("updated_at") or "").strip()
                if updated_at:
                    oldest_indexed_at = updated_at if not oldest_indexed_at else min(oldest_indexed_at, updated_at)
                    newest_indexed_at = updated_at if not newest_indexed_at else max(newest_indexed_at, updated_at)
        health = "ready" if totals["repos"] and not totals["stale_or_missing"] else "needs_sync" if totals["repos"] else "not_configured"
        return {
            "status": health,
            "totals": totals,
            "oldest_indexed_at": oldest_indexed_at or None,
            "newest_indexed_at": newest_indexed_at or None,
            "keys": keys,
        }

    @staticmethod
    def _index_freshness_payload(repo_status: list[dict[str, Any]]) -> dict[str, Any]:
        repo_count = len(repo_status)
        stale_repos = []
        revisions = []
        updated_at_values = []
        for item in repo_status:
            index = item.get("index") or {}
            state = str(index.get("state") or "missing")
            if state != "ready":
                stale_repos.append(str(item.get("display_name") or item.get("url") or item.get("path") or "repository"))
            revision = str(index.get("git_revision") or "").strip()
            if revision:
                revisions.append({"repo": item.get("display_name"), "git_revision": revision})
            updated_at = str(index.get("updated_at") or "").strip()
            if updated_at:
                updated_at_values.append(updated_at)
        return {
            "status": "fresh" if not stale_repos and repo_count else "stale_or_missing",
            "repo_count": repo_count,
            "stale_repos": stale_repos,
            "git_revisions": revisions,
            "oldest_indexed_at": min(updated_at_values) if updated_at_values else None,
            "newest_indexed_at": max(updated_at_values) if updated_at_values else None,
            "warning": (
                ""
                if not stale_repos
                else "Some repositories have stale or missing indexes. Run Sync / Refresh before trusting code answers."
            ),
        }

    def save_feedback(self, *, user_email: str, payload: dict[str, Any]) -> dict[str, Any]:
        rating = str(payload.get("rating") or "").strip().lower()
        if rating not in {"useful", "not_useful", "wrong_file", "too_vague", "hallucinated", "missing_repo", "needs_deeper_trace", "incorrect", "missing_evidence", "stale_code"}:
            raise ToolError("Unknown feedback rating.")
        question = str(payload.get("question") or "").strip()
        replay_context = self._feedback_replay_context(payload)
        reason = str(payload.get("reason") or "").strip()
        allowed_reasons = {
            "",
            "deprecated_class",
            "opposite_logic",
            "off_topic",
            "missing_key_flow",
            "wrong_scope",
        }
        if reason not in allowed_reasons:
            raise ToolError("Unknown feedback reason.")
        record = {
            "timestamp": self._now_iso(),
            "user_email": str(user_email or "").strip().lower(),
            "rating": rating,
            "reason": reason,
            "pm_team": str(payload.get("pm_team") or "").strip().upper(),
            "country": str(payload.get("country") or "").strip(),
            "trace_id": replay_context.get("trace_id") or str(payload.get("trace_id") or "").strip(),
            "question_sha1": hashlib.sha1(question.encode("utf-8")).hexdigest() if question else "",
            "question_preview": question[:180],
            "top_paths": [str(path) for path in payload.get("top_paths") or []][:8],
            "comment": str(payload.get("comment") or "").strip()[:1000],
            "answer_quality": payload.get("answer_quality") if isinstance(payload.get("answer_quality"), dict) else {},
            "replay_context": replay_context,
        }
        self.feedback_path.parent.mkdir(parents=True, exist_ok=True)
        with self.feedback_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        return {"status": "ok", "message": "Feedback saved."}

    @classmethod
    def _feedback_replay_context(cls, payload: dict[str, Any]) -> dict[str, Any]:
        source = payload.get("replay_context") if isinstance(payload.get("replay_context"), dict) else payload
        matches = source.get("matches_snapshot") or source.get("matches") or []
        match_snapshots: list[dict[str, Any]] = []
        if isinstance(matches, list):
            for match in matches[:10]:
                if not isinstance(match, dict):
                    continue
                match_snapshots.append(
                    {
                        "repo": cls._trim_feedback_value(match.get("repo"), text_limit=180),
                        "path": cls._trim_feedback_value(match.get("path"), text_limit=300),
                        "line_start": match.get("line_start"),
                        "line_end": match.get("line_end"),
                        "retrieval": cls._trim_feedback_value(match.get("retrieval"), text_limit=80),
                        "trace_stage": cls._trim_feedback_value(match.get("trace_stage"), text_limit=80),
                        "score": match.get("score"),
                        "snippet": cls._trim_feedback_value(match.get("snippet"), text_limit=1600),
                    }
                )
        context = {
            "trace_id": str(source.get("trace_id") or payload.get("trace_id") or "").strip()[:64],
            "answer_mode": str(source.get("answer_mode") or payload.get("answer_mode") or "").strip()[:60],
            "llm_budget_mode": str(source.get("llm_budget_mode") or payload.get("llm_budget_mode") or "").strip()[:60],
            "llm_provider": str(source.get("llm_provider") or payload.get("llm_provider") or "").strip()[:80],
            "llm_model": str(source.get("llm_model") or payload.get("llm_model") or "").strip()[:120],
            "llm_route": cls._trim_feedback_value(source.get("llm_route") or payload.get("llm_route") or {}),
            "llm_finish_reason": str(source.get("llm_finish_reason") or payload.get("llm_finish_reason") or "").strip()[:120],
            "summary": cls._trim_feedback_value(source.get("summary") or payload.get("summary") or "", text_limit=1000),
            "rendered_answer": cls._trim_feedback_value(
                source.get("rendered_answer") or source.get("llm_answer") or payload.get("llm_answer") or "",
                text_limit=8000,
            ),
            "citations": cls._trim_feedback_value(source.get("citations") or payload.get("citations") or [], list_limit=20),
            "matches_snapshot": match_snapshots,
            "answer_contract": cls._trim_feedback_value(source.get("answer_contract") or payload.get("answer_contract") or {}),
            "evidence_pack": cls._trim_feedback_value(source.get("evidence_pack") or payload.get("evidence_pack") or {}, list_limit=30),
            "tool_trace": cls._trim_feedback_value(source.get("tool_trace") or payload.get("tool_trace") or [], list_limit=30, text_limit=1000),
        }
        return {key: value for key, value in context.items() if value not in ("", [], {})}

    @classmethod
    def _trim_feedback_value(
        cls,
        value: Any,
        *,
        depth: int = 0,
        text_limit: int = 4000,
        list_limit: int = 50,
        dict_limit: int = 80,
    ) -> Any:
        if depth >= 6:
            return None
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return value[:text_limit]
        if isinstance(value, dict):
            trimmed: dict[str, Any] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= dict_limit:
                    break
                trimmed[str(key)[:120]] = cls._trim_feedback_value(
                    item,
                    depth=depth + 1,
                    text_limit=text_limit,
                    list_limit=list_limit,
                    dict_limit=dict_limit,
                )
            return trimmed
        if isinstance(value, (list, tuple)):
            return [
                cls._trim_feedback_value(
                    item,
                    depth=depth + 1,
                    text_limit=text_limit,
                    list_limit=list_limit,
                    dict_limit=dict_limit,
                )
                for item in list(value)[:list_limit]
            ]
        return str(value)[:text_limit]

    def _load_entries_for_key(self, key: str) -> list[RepositoryEntry]:
        raw_entries = self.load_config().get("mappings", {}).get(key, [])
        return [self._normalize_entry(entry) for entry in raw_entries]

    def llm_ready(self) -> bool:
        return self.llm_provider.ready()

    def mapping_key(self, pm_team: str, country: str) -> str:
        team = str(pm_team or "").strip().upper()
        if team not in self.team_profiles:
            raise ToolError("Unknown PM Team. Please choose one of the configured BPMIS PM Teams.")
        normalized_country = self.normalize_country(team, country)
        return f"{team}:{normalized_country}"

    @staticmethod
    def normalize_country(pm_team: str, country: str) -> str:
        team = str(pm_team or "").strip().upper()
        value = str(country or "").strip().upper()
        if team == "CRMS":
            if value not in CRMS_COUNTRIES:
                raise ToolError("Credit Risk source repositories require country SG, ID, or PH.")
            return value
        return ALL_COUNTRY

    def _sync_entry(self, key: str, entry: RepositoryEntry) -> dict[str, Any]:
        repo_path = self._repo_path(key, entry)
        repo_path.parent.mkdir(parents=True, exist_ok=True)
        if (repo_path / ".git").exists():
            command = ["git", "-c", "credential.helper=", "-C", str(repo_path), "pull", "--ff-only"]
            action = "pull"
        else:
            if repo_path.exists():
                self._remove_incomplete_repo_dir(repo_path)
            command = ["git", "-c", "credential.helper=", "clone", "--depth", "1", self._authenticated_git_url(entry.url), str(repo_path)]
            action = "clone"
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.git_timeout_seconds,
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
                check=False,
            )
        except subprocess.TimeoutExpired:
            return self._sync_result(entry, repo_path, "error", f"Git {action} timed out after {self.git_timeout_seconds}s.")
        except OSError as error:
            return self._sync_result(entry, repo_path, "error", f"Git {action} could not start: {error}")
        if completed.returncode != 0:
            detail = self._sanitize_error_detail((completed.stderr or completed.stdout or "").strip())
            return self._sync_result(entry, repo_path, "error", f"Git {action} failed. {detail[:800]}")
        try:
            index_info = self._build_repo_index(key, entry, repo_path)
        except (OSError, sqlite3.Error, ValueError, IndexError) as error:
            return self._sync_result(entry, repo_path, "error", f"Git {action} completed, but code index failed: {error}")
        return self._sync_result(
            entry,
            repo_path,
            "ok",
            (
                f"Git {action} completed. Indexed {index_info['files']} files, "
                f"{index_info['lines']} lines, {index_info.get('definitions', 0)} definitions, "
                f"and {index_info.get('references', 0)} references."
            ),
        )

    def _sync_result(self, entry: RepositoryEntry, repo_path: Path, state: str, message: str) -> dict[str, Any]:
        return {
            "display_name": entry.display_name,
            "url": entry.url,
            "path": str(repo_path),
            "state": state,
            "message": message,
        }

    def sync_job_status(self, key: str) -> dict[str, Any]:
        try:
            try:
                payload = json.loads(self.sync_jobs_path.read_text(encoding="utf-8")) if self.sync_jobs_path.exists() else {}
            except json.JSONDecodeError:
                payload = {}
        except (OSError, json.JSONDecodeError):
            payload = {}
        return payload.get(key) if isinstance(payload.get(key), dict) else {"status": "idle", "key": key}

    def _start_sync_job(self, key: str, entries: list[RepositoryEntry]) -> dict[str, Any]:
        job = {
            "job_id": hashlib.sha1(f"{key}:{time.time()}".encode("utf-8")).hexdigest()[:12],
            "key": key,
            "status": "running",
            "started_at": self._now_iso(),
            "finished_at": None,
            "repo_count": len(entries),
            "results": [],
        }
        self._write_sync_job(key, job)
        return job

    def _finish_sync_job(self, key: str, job_id: str, *, status: str, results: list[dict[str, Any]]) -> None:
        job = self.sync_job_status(key)
        if job.get("job_id") != job_id:
            return
        job.update({"status": status, "finished_at": self._now_iso(), "results": results})
        self._write_sync_job(key, job)

    def _write_sync_job(self, key: str, job: dict[str, Any]) -> None:
        lock_path = self.sync_jobs_path.with_suffix(".lock")
        acquired = False
        started_at = time.monotonic()
        try:
            self.sync_jobs_path.parent.mkdir(parents=True, exist_ok=True)
            while True:
                try:
                    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    with os.fdopen(fd, "w", encoding="utf-8") as handle:
                        handle.write(self._now_iso())
                    acquired = True
                    break
                except FileExistsError:
                    if time.monotonic() - started_at >= SYNC_JOB_LOCK_TIMEOUT_SECONDS:
                        return
                    time.sleep(0.05)
            payload = json.loads(self.sync_jobs_path.read_text(encoding="utf-8")) if self.sync_jobs_path.exists() else {}
            if not isinstance(payload, dict):
                payload = {}
            payload[key] = job
            temp_path = self.sync_jobs_path.with_suffix(f".{os.getpid()}.tmp")
            temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.sync_jobs_path)
        except OSError:
            return
        finally:
            if acquired:
                lock_path.unlink(missing_ok=True)
























































































































































































    def _build_agent_plan(
        self,
        question: str,
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
    ) -> dict[str, Any]:
        intent = evidence_summary.get("intent") or self._question_intent(question)
        seed_terms = self._planner_seed_terms(question, evidence_summary)
        steps: list[dict[str, Any]] = []
        if intent.get("data_source"):
            steps.extend(
                [
                    {
                        "name": "trace_entry_to_source_lineage",
                        "purpose": "For underwriting/precheck/data-lineage questions, trace entry point through service, processor, mapper, repository, client, API, and SQL layers before answering.",
                        "terms": [
                            *seed_terms,
                            "underwriting",
                            "precheck",
                            "preCheck",
                            "layer",
                            "engine",
                            "strategy",
                            "processor",
                            "handler",
                            "DataSourceResult",
                            "repository",
                            "mapper",
                            "dao",
                            "client",
                            "integration",
                            "select",
                            "from",
                        ],
                        "tools": ["find_definition", "find_callers", "find_callees", "trace_flow", "trace_entity"],
                    },
                    {
                        "name": "trace_data_carriers",
                        "purpose": "Find DTO/context/result objects that carry the requested data.",
                        "terms": [*seed_terms, "dto", "input", "context", "record", "result", "request", "response", "profile", "info"],
                        "tools": ["find_definition", "find_references", "open_file_window"],
                    },
                    {
                        "name": "trace_field_population",
                        "purpose": "Trace DTO fields backward to provider, builder, converter, and setter/getter code.",
                        "terms": [
                            *seed_terms,
                            "set",
                            "get",
                            "populate",
                            "build",
                            "builder",
                            "provider",
                            "factory",
                            "converter",
                            "assembler",
                            "mapper",
                        ],
                        "tools": ["find_references", "find_callers", "find_callees", "trace_flow"],
                    },
                    {
                        "name": "trace_downstream_sources",
                        "purpose": "Follow services into repository, mapper, integration, client, SQL, or API calls.",
                        "terms": [*seed_terms, "repository", "mapper", "dao", "jdbcTemplate", "queryForObject", "select", "from", "integration", "client", "gateway"],
                        "tools": ["find_tables", "find_callees", "trace_flow", "trace_entity"],
                    },
                    {
                        "name": "trace_dao_mapper_methods",
                        "purpose": "Open DAO/Mapper classes and XML/SQL mappings to find concrete table, query, or upstream API source.",
                        "terms": [
                            *self._planner_suffix_terms(seed_terms, ("Repository", "Mapper", "Dao", "DAO", "Client", "Integration")),
                            "select",
                            "from",
                            "join",
                            "resultMap",
                            "namespace",
                            "queryForObject",
                            "statement",
                        ],
                        "tools": ["find_definition", "find_tables", "find_references"],
                    },
                ]
            )
        if intent.get("module_dependency") or any(term in str(question or "").lower() for term in ("cross-repo", "cross repo", "another repo", "dependency", "module")):
            steps.extend(
                [
                    {
                        "name": "trace_module_dependencies",
                        "purpose": "Find Maven, Gradle, npm, package, and module dependency evidence across repositories.",
                        "terms": [
                            *seed_terms,
                            "pom.xml",
                            "build.gradle",
                            "settings.gradle",
                            "package.json",
                            "artifactId",
                            "groupId",
                            "dependency",
                            "implementation",
                            "api",
                            "module_dependency",
                        ],
                        "tools": ["find_references", "trace_flow", "trace_entity", "search_code"],
                    },
                    {
                        "name": "trace_cross_repo_contracts",
                        "purpose": "Find cross-repo contracts through routes, clients, message topics, shared tables, or dependency coordinates.",
                        "terms": [
                            *seed_terms,
                            "FeignClient",
                            "RestTemplate",
                            "WebClient",
                            "KafkaListener",
                            "RabbitListener",
                            "JmsListener",
                            "topic",
                            "queue",
                            "shared table",
                            "requestmapping",
                        ],
                        "tools": ["find_api_routes", "find_references", "find_tables", "trace_flow", "search_code"],
                    },
                ]
            )
        if intent.get("api"):
            steps.append(
                {
                    "name": "trace_api_flow",
                    "purpose": "Find controllers, API clients, request mappings, and endpoint calls.",
                    "terms": [*seed_terms, "controller", "requestmapping", "postmapping", "getmapping", "client", "endpoint", "route", "url"],
                    "tools": ["find_api_routes", "find_definition", "find_callees", "trace_flow"],
                }
            )
        if intent.get("config"):
            steps.append(
                {
                    "name": "trace_config",
                    "purpose": "Find properties, YAML, feature flags, and configuration classes.",
                    "terms": [*seed_terms, "configuration", "properties", "yaml", "yml", "feature", "config", "flag"],
                    "tools": ["find_definition", "find_references", "search_code"],
                }
            )
        if any(term in str(question or "").lower() for term in ("topic", "queue", "kafka", "rabbit", "jms", "message", "consumer", "producer")):
            steps.append(
                {
                    "name": "trace_message_flow",
                    "purpose": "Find message producers, consumers, topics, queues, and event handoff code.",
                    "terms": [*seed_terms, "KafkaListener", "RabbitListener", "JmsListener", "topic", "queue", "send", "publishEvent", "consumer", "producer"],
                    "tools": ["find_references", "find_callers", "find_callees", "trace_flow", "search_code"],
                }
            )
        if intent.get("rule_logic") or intent.get("error"):
            steps.append(
                {
                    "name": "trace_decision_logic",
                    "purpose": "Find validations, rule branches, error handling, or approval logic.",
                    "terms": [*seed_terms, "validate", "validation", "rule", "condition", "exception", "status", "approval", "permission"],
                    "tools": ["find_references", "find_callers", "open_file_window", "search_code"],
                }
            )
        if intent.get("static_qa"):
            steps.append(
                {
                    "name": "scan_static_qa_findings",
                    "purpose": "Find deterministic static QA risks such as hardcoded secrets, unsafe SQL, swallowed exceptions, command execution, and TODO/FIXME markers.",
                    "terms": [
                        *seed_terms,
                        "password",
                        "secret",
                        "token",
                        "apiKey",
                        "catch",
                        "Exception",
                        "Throwable",
                        "printStackTrace",
                        "Runtime",
                        "ProcessBuilder",
                        "subprocess",
                        "eval",
                        "exec",
                        "TODO",
                        "FIXME",
                    ],
                    "tools": ["find_static_findings", "open_file_window", "search_code"],
                }
            )
        if intent.get("test_coverage"):
            steps.append(
                {
                    "name": "trace_test_coverage",
                    "purpose": "Find test files, test cases, assertions, mocks, and verification calls that cover the target symbol or behavior.",
                    "terms": [*seed_terms, "test", "assert", "assertThat", "verify", "mock", "when", "should", "junit", "pytest", "jest"],
                    "tools": ["find_test_coverage", "find_references", "open_file_window", "search_code"],
                }
            )
        if intent.get("operational_boundary"):
            steps.append(
                {
                    "name": "trace_operational_boundaries",
                    "purpose": "Find transaction, cache, async, retry, circuit breaker, lock, rate-limit, and authorization annotations that alter runtime behavior.",
                    "terms": [
                        *seed_terms,
                        "Transactional",
                        "rollbackFor",
                        "Cacheable",
                        "CacheEvict",
                        "Async",
                        "Retryable",
                        "CircuitBreaker",
                        "RateLimiter",
                        "Bulkhead",
                        "TimeLimiter",
                        "SchedulerLock",
                        "PreAuthorize",
                    ],
                    "tools": ["find_operational_boundaries", "find_references", "trace_entity", "open_file_window", "search_code"],
                }
            )
        if intent.get("impact_analysis"):
            steps.extend(
                [
                    {
                        "name": "trace_upstream_impact",
                        "purpose": "Find callers, controllers, handlers, jobs, consumers, and cross-repo clients that can be affected by a change.",
                        "terms": [*seed_terms, "controller", "handler", "consumer", "scheduler", "job", "client", "route", "caller", "usage"],
                        "tools": ["find_references", "find_callers", "trace_flow", "trace_entity"],
                    },
                    {
                        "name": "trace_downstream_impact",
                        "purpose": "Find downstream services, repositories, mappers, clients, APIs, tables, topics, and configs touched by the changed code.",
                        "terms": [*seed_terms, "service", "repository", "mapper", "dao", "client", "api", "table", "topic", "config", "callee"],
                        "tools": ["find_callees", "trace_flow", "trace_entity", "find_tables", "find_api_routes"],
                    },
                ]
            )
        if quality_gate.get("status") != "sufficient":
            steps.append(
                {
                    "name": "fill_quality_gap",
                    "purpose": "Search for the missing evidence reported by the quality gate.",
                    "terms": self._quality_gate_trace_terms(question, evidence_summary, quality_gate, []),
                    "tools": ["find_tables", "find_api_routes", "trace_flow", "search_code"],
                }
            )
        deduped_steps = self._dedupe_agent_plan_steps(steps)
        return {
            "mode": "local_agentic_retrieval",
            "recipe_version": 3,
            "status": "planned" if deduped_steps else "not_needed",
            "intent": intent,
            "steps": deduped_steps[:6],
        }

    def _dedupe_agent_plan_steps(self, steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped_steps: list[dict[str, Any]] = []
        seen: set[str] = set()
        for step in steps:
            name = str(step.get("name") or "")
            if not name or name in seen:
                continue
            terms = list(dict.fromkeys(str(term).strip() for term in step.get("terms") or [] if str(term).strip()))
            if not terms:
                continue
            tools = [
                str(tool).strip()
                for tool in step.get("tools") or []
                if str(tool).strip() in {tool_def["name"] for tool_def in self._planner_tool_registry()["tools"]}
            ]
            deduped_steps.append({**step, "terms": terms[:20], "tools": list(dict.fromkeys(tools))[:5]})
            seen.add(name)
        return deduped_steps

    def _planner_seed_terms(self, question: str, evidence_summary: dict[str, Any]) -> list[str]:
        terms: list[str] = []
        for bucket in ("entry_points", "data_carriers", "field_population", "downstream_components", "data_sources", "api_or_config", "rule_or_error_logic"):
            for fact in evidence_summary.get(bucket) or []:
                terms.extend(IDENTIFIER_PATTERN.findall(str(fact)))
                terms.extend(part for part in re.split(r"[/_.:-]+", str(fact)) if part)
        terms.extend(token for token in self._question_tokens(question) if token not in DEPENDENCY_QUESTION_TERMS)
        deduped: list[str] = []
        for term in terms:
            normalized = str(term or "").strip()
            lowered = normalized.lower()
            if len(lowered) < 4 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
                continue
            if lowered in LOW_VALUE_FOCUS_TERMS and len(lowered) < 10:
                continue
            if lowered not in {item.lower() for item in deduped}:
                deduped.append(normalized)
        return deduped[:16]

    @staticmethod
    def _planner_suffix_terms(seed_terms: list[str], suffixes: tuple[str, ...]) -> list[str]:
        generated: list[str] = []
        for term in seed_terms[:8]:
            base = re.sub(r"[^A-Za-z0-9_]", "", str(term or ""))
            if len(base) < 4:
                continue
            if any(base.lower().endswith(suffix.lower()) for suffix in suffixes):
                generated.append(base)
                continue
            for suffix in suffixes:
                generated.append(f"{base}{suffix}")
        return generated[:24]

    def _run_agent_plan(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        matches: list[dict[str, Any]],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        agent_plan: dict[str, Any],
        limit: int,
        tool_trace: list[dict[str, Any]] | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        collected = list(matches)
        seen_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in collected}
        current_summary = evidence_summary
        current_gate = quality_gate
        for step_index, step in enumerate(agent_plan.get("steps") or [], start=1):
            terms = self._agent_step_terms(question, step, current_summary, current_gate, collected)
            if not terms:
                continue
            expanded_tokens: list[str] = []
            for term in terms:
                expanded_tokens.extend(self._question_tokens(term))
            expanded_tokens = list(dict.fromkeys(expanded_tokens))[:30]
            step_matches: list[dict[str, Any]] = []
            for tool in step.get("tools") or []:
                tool_step = {"tool": str(tool), "terms": terms[:18]}
                before_tool_count = len(step_matches)
                step_matches.extend(
                    self._execute_tool_loop_step(
                        entries=entries,
                        key=key,
                        question=question,
                        matches=collected,
                        step=tool_step,
                        step_index=step_index,
                        request_cache=request_cache,
                    )
                )
                if tool_trace is not None:
                    tool_trace.append(
                        {
                            "phase": "agent_plan",
                            "round": step_index,
                            "step": str(step.get("name") or f"step_{step_index}"),
                            "tool": str(tool),
                            "terms": terms[:10],
                            "matches_found": len(step_matches) - before_tool_count,
                        }
                    )
            before_keyword_count = len(step_matches)
            ran_explicit_search = any(str(tool) == "search_code" for tool in (step.get("tools") or []))
            if not ran_explicit_search:
                for entry in entries:
                    repo_path = self._repo_path(key, entry)
                    if not (repo_path / ".git").exists():
                        continue
                    step_matches.extend(
                        self._search_repo(
                            entry,
                            repo_path,
                            expanded_tokens,
                            question=question,
                            focus_terms=terms,
                            trace_stage=f"agent_plan_{step_index}",
                            request_cache=request_cache,
                        )
                    )
            if tool_trace is not None:
                tool_trace.append(
                    {
                        "phase": "agent_plan",
                        "round": step_index,
                        "step": str(step.get("name") or f"step_{step_index}"),
                        "tool": "search_code" if not ran_explicit_search else "search_code_skipped_duplicate",
                        "terms": terms[:10],
                        "matches_found": len(step_matches) - before_keyword_count,
                    }
                )
            step_matches.sort(key=lambda item: item["score"], reverse=True)
            added = 0
            for item in step_matches[: max(6, min(int(limit or 12), 18))]:
                item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                if item_key in seen_keys:
                    continue
                collected.append(item)
                seen_keys.add(item_key)
                added += 1
            if tool_trace is not None:
                tool_trace.append(
                    {
                        "phase": "agent_plan",
                        "round": step_index,
                        "step": str(step.get("name") or f"step_{step_index}"),
                        "tool": "dedupe_rank",
                        "matches_found": len(step_matches),
                        "matches_added": added,
                        "matches_after": len(collected),
                    }
                )
            collected.sort(key=lambda item: item["score"], reverse=True)
            collected = self._select_result_matches(collected, self._query_result_limit(limit), question=question)
            current_summary = self._compress_evidence_cached(question, collected, request_cache=request_cache)
            current_gate = self._quality_gate_cached(question, current_summary, request_cache=request_cache)
            if self._should_stop_agent_plan(current_summary, current_gate, step_index):
                break
        return collected

    @staticmethod
    def _should_stop_agent_plan(
        current_summary: dict[str, Any],
        current_gate: dict[str, Any],
        step_index: int,
    ) -> bool:
        if current_summary.get("intent", {}).get("data_source"):
            return False
        return current_gate.get("status") == "sufficient" and step_index >= 2

    def _agent_step_terms(
        self,
        question: str,
        step: dict[str, Any],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        matches: list[dict[str, Any]],
    ) -> list[str]:
        terms = list(step.get("terms") or [])
        terms.extend(self._quality_gate_trace_terms(question, evidence_summary, quality_gate, matches))
        for bucket in ("entry_points", "data_carriers", "field_population", "downstream_components"):
            for fact in evidence_summary.get(bucket) or []:
                terms.extend(IDENTIFIER_PATTERN.findall(str(fact)))
        terms.extend(self._field_backward_terms(evidence_summary))
        for match in matches[:10]:
            terms.extend(self._extract_downstream_symbols(str(match.get("snippet") or "")))
            terms.extend(self._extract_assignment_sources(str(match.get("snippet") or "")))
        deduped: list[str] = []
        question_tokens = set(self._question_tokens(question))
        for term in terms:
            lowered = str(term or "").strip().lower()
            if len(lowered) < 4 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
                continue
            if lowered in LOW_VALUE_FOCUS_TERMS and lowered not in {"datasource"}:
                continue
            if lowered in question_tokens and len(lowered) < 8:
                continue
            if lowered not in deduped:
                deduped.append(lowered)
        return deduped[:20]

    def _field_backward_terms(self, evidence_summary: dict[str, Any]) -> list[str]:
        terms: list[str] = []
        for bucket in ("data_carriers", "field_population", "entry_points"):
            for fact in evidence_summary.get(bucket) or []:
                for symbol in IDENTIFIER_PATTERN.findall(str(fact)):
                    terms.append(symbol)
                    if symbol.endswith(("Info", "DTO", "Input", "Result", "Context")):
                        terms.extend([f"set{symbol}", f"get{symbol}", f"build{symbol}", f"populate{symbol}"])
                    if symbol.endswith(("Repository", "Mapper", "Dao", "DAO", "Client", "Integration", "Provider")):
                        terms.extend([symbol, f"{symbol}Impl"])
        terms.extend(["populate", "build", "provider", "converter", "assembler", "repository", "mapper", "dao", "client"])
        terms.extend(self._all_profile_terms("data_carriers", "source_terms", "field_population_terms"))
        return terms

    def _agent_trace_terms(
        self,
        question: str,
        matches: list[dict[str, Any]],
        seen_terms: set[str],
    ) -> list[str]:
        terms: list[str] = []
        lowered_question = question.lower()
        question_tokens = set(self._question_tokens(question))
        for match in matches[:10]:
            path = str(match.get("path") or "")
            path_parts = re.split(r"[/_.-]+", path)
            terms.extend(part for part in path_parts if part and len(part) >= 4)
            snippet = str(match.get("snippet") or "")
            terms.extend(self._extract_downstream_symbols(snippet))
            terms.extend(self._extract_assignment_sources(snippet))
        if "data" in lowered_question or "source" in lowered_question or "upstream" in lowered_question:
            terms.extend(
                [
                    "datasourceresult",
                    "getdatasourceresult",
                    "dataresult",
                    "datacontext",
                    "datarecord",
                    "dataprofile",
                    "datadto",
                    "datainput",
                    "integration",
                    "repository",
                    "mapper",
                    "dao",
                    "client",
                    "provider",
                ]
            )
        deduped: list[str] = []
        for term in terms:
            lowered = term.strip().lower()
            if len(lowered) < 4 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS or lowered in LOW_VALUE_FOCUS_TERMS:
                continue
            if lowered in question_tokens and len(lowered) < 8:
                continue
            if lowered in seen_terms or lowered in deduped:
                continue
            deduped.append(lowered)
        return deduped[:14]

    @staticmethod
    def _extract_assignment_sources(snippet: str) -> list[str]:
        terms: list[str] = []
        for left, right in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([^;\n]+)", snippet):
            terms.append(left)
            terms.extend(IDENTIFIER_PATTERN.findall(right))
        for call_chain in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){1,4})\s*\(", snippet):
            terms.extend(part for part in call_chain.split(".") if part)
        return terms

    def _two_hop_trace_terms(self, question: str, matches: list[dict[str, Any]]) -> list[str]:
        terms: list[str] = []
        question_tokens = set(self._question_tokens(question))
        for match in matches[:8]:
            terms.extend(self._extract_downstream_symbols(str(match.get("snippet") or "")))
        deduped: list[str] = []
        for term in terms:
            lowered = term.strip().lower()
            if len(lowered) < 3 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
                continue
            if lowered in question_tokens:
                continue
            if lowered not in deduped:
                deduped.append(lowered)
        return deduped[:12]

    @staticmethod
    def _extract_downstream_symbols(snippet: str) -> list[str]:
        symbols: list[str] = []
        for pattern in (FIELD_OR_PARAM_TYPE_PATTERN, CLASS_CONSTRUCTION_PATTERN):
            symbols.extend(match.group(1) for match in pattern.finditer(snippet))
        for symbol in IDENTIFIER_PATTERN.findall(snippet):
            lowered = symbol.lower()
            if any(lowered.endswith(suffix) for suffix in DEPENDENCY_SYMBOL_SUFFIXES):
                symbols.append(symbol)
        for call in CALL_SYMBOL_PATTERN.findall(snippet):
            lowered = call.lower()
            if lowered in LOW_VALUE_CALL_SYMBOLS or lowered in STOPWORDS:
                continue
            if lowered.startswith(("get", "set", "is")) and len(lowered) < 8:
                continue
            symbols.append(call)
        return symbols

    def _dependency_focus_terms(self, question: str, matches: list[dict[str, Any]]) -> list[str]:
        focus_terms: list[str] = []
        lowered_question = question.lower()
        for token in self._question_tokens(question):
            if token in DEPENDENCY_QUESTION_TERMS:
                continue
            if token in LOW_VALUE_FOCUS_TERMS:
                continue
            focus_terms.append(token)
        if "data" in lowered_question or "source" in lowered_question or "upstream" in lowered_question:
            focus_terms.extend(
                [
                    "datasourceresult",
                    "datacontext",
                    "datarecord",
                    "datadto",
                    "datainput",
                    "datarequest",
                    "dataresponse",
                    "dataprofile",
                    "build",
                    "strategy",
                    "provider",
                ]
            )
        for match in matches[:6]:
            path_parts = re.split(r"[/_.-]+", str(match.get("path") or ""))
            for part in path_parts:
                lowered = part.lower()
                if lowered and any(lowered.endswith(suffix) for suffix in DEPENDENCY_SYMBOL_SUFFIXES):
                    focus_terms.append(lowered)
            snippet = str(match.get("snippet") or "")
            for symbol in IDENTIFIER_PATTERN.findall(snippet):
                lowered = symbol.lower()
                if any(lowered.endswith(suffix) for suffix in DEPENDENCY_SYMBOL_SUFFIXES):
                    focus_terms.append(lowered)
        deduped: list[str] = []
        for term in focus_terms:
            term = term.strip().lower()
            if len(term) < 3 or term in STOPWORDS or term in deduped:
                continue
            deduped.append(term)
        return deduped[:16]

    def _question_intent(self, question: str) -> dict[str, Any]:
        lowered = f" {question.lower()} "
        exact_terms = self._extract_exact_lookup_terms(question)
        table_like_terms = [
            term
            for term in exact_terms
            if term.count("_") >= 2 and any(marker in term for marker in ("dwd_", "dim_", "ads_", "tmp_", "_df", "_di", "table"))
        ]
        api_intent = any(term in lowered for term in API_HINTS)
        explicit_data_source_intent = any(
            term in lowered
            for term in (
                "data source",
                "data sources",
                "source table",
                "upstream",
                "table",
                "database",
                "jdbc",
                "select",
                "repository",
                "mapper",
                "dao",
                " read ",
                " write ",
                " written ",
                "persisted",
                "数据源",
                "来源",
                "上游",
                "表",
                "数据库",
                "读取",
                "写入",
            )
        )
        table_relation_intent = len(table_like_terms) >= 1 and any(term in lowered for term in (" relation ", " between ", " relationship ", "source", "table", "数据", "表", "关系"))
        data_source_intent = (
            explicit_data_source_intent
            or table_relation_intent
            or (any(term in lowered for term in DATA_SOURCE_HINTS) and not api_intent)
        )
        sql_generation_intent = self._question_sql_generation_intent(question)
        return {
            "data_source": data_source_intent or sql_generation_intent,
            "sql_generation": sql_generation_intent,
            "api": api_intent,
            "config": any(term in lowered for term in CONFIG_HINTS),
            "module_dependency": any(term in lowered for term in MODULE_DEPENDENCY_HINTS),
            "message_flow": self._question_message_flow_intent(question),
            "error": any(term in lowered for term in ERROR_HINTS),
            "rule_logic": any(term in lowered for term in RULE_HINTS),
            "static_qa": self._question_static_qa_intent(question),
            "impact_analysis": any(term in lowered for term in IMPACT_ANALYSIS_HINTS),
            "test_coverage": any(term in lowered for term in TEST_COVERAGE_HINTS),
            "operational_boundary": self._question_operational_boundary_intent(question),
        }

    @staticmethod
    def _question_sql_generation_intent(question: str) -> bool:
        lowered = f" {str(question or '').lower()} "
        if not lowered.strip():
            return False
        negative_hints = (
            "sql injection",
            "injection",
            "static qa",
            "static analysis",
            "vulnerability",
            "漏洞",
            "注入",
            "代码质量",
        )
        if any(term in lowered for term in negative_hints):
            return False
        explicit_hints = (
            "write sql",
            "generate sql",
            "create sql",
            "sql query",
            "sql code",
            "query.sql",
            "download sql",
            "downloadable sql",
            "export sql",
            "sql file",
            "帮我写sql",
            "写sql",
            "生成sql",
            "sql代码",
            "sql查询",
            "下载sql",
        )
        if any(term in lowered for term in explicit_hints):
            return True
        return bool(re.search(r"\b(?:write|generate|create|build|draft)\b.{0,40}\bsql\b", lowered))

    @staticmethod
    def _question_static_qa_intent(question: str) -> bool:
        lowered = f" {str(question or '').lower()} "
        explicit_hints = (
            "static qa",
            "static analysis",
            "code quality",
            "code smell",
            "security",
            "vulnerability",
            "vulnerabilities",
            "unsafe",
            "hardcoded",
            "secret",
            "password",
            "token",
            "sql injection",
            "injection",
            "empty catch",
            "swallow",
            "broad exception",
            "todo",
            "fixme",
            "静态",
            "代码质量",
            "安全",
            "漏洞",
            "硬编码",
            "密码",
            "令牌",
            "注入",
        )
        if any(term in lowered for term in explicit_hints):
            return True
        # Domain labels such as "Credit Risk" and "Ops Risk" are not static-QA asks.
        domain_neutral = lowered.replace(" credit risk ", " ").replace(" ops risk ", " ")
        if re.search(r"\b(?:risk|risks|bug|bugs|smell|smells)\b", domain_neutral):
            return any(term in domain_neutral for term in (" code ", " static ", " quality ", " finding", "issue", "安全", "代码"))
        return False

    @staticmethod
    def _question_operational_boundary_intent(question: str) -> bool:
        lowered = f" {str(question or '').lower()} "
        explicit_hints = (
            "transaction",
            "transactional",
            "rollback",
            "commit",
            "cache",
            "cached",
            "cacheable",
            "cacheevict",
            "async",
            "asynchronous",
            "retry",
            "retryable",
            "circuit breaker",
            "circuitbreaker",
            "rate limit",
            "ratelimiter",
            "bulkhead",
            "timeout",
            "timelimiter",
            "schedulerlock",
            "preauthorize",
            "postauthorize",
            "authorization",
            "permission boundary",
            "事务",
            "回滚",
            "提交",
            "缓存",
            "异步",
            "重试",
            "熔断",
            "限流",
            "超时",
            "鉴权",
            "授权",
            "权限边界",
        )
        if any(term in lowered for term in explicit_hints):
            return True
        # Avoid treating table/config names like bcf_global_lock or globallock as a
        # boundary ask. Standalone "lock" questions still need boundary evidence.
        return bool(re.search(r"\b(?:lock|locks|locking)\b", lowered) or any(term in lowered for term in (" 锁 ", " 加锁 ", " 解锁 ")))

    @staticmethod
    def _question_message_flow_intent(question: str) -> bool:
        lowered = f" {str(question or '').lower()} "
        return any(
            term in lowered
            for term in (
                "topic",
                "queue",
                "kafka",
                "rabbit",
                "jms",
                "message",
                "event",
                "consumer",
                "consume",
                "producer",
                "publish",
                "listener",
                "消息",
                "事件",
                "主题",
                "队列",
                "消费",
                "发布",
            )
        )

    def _build_query_decomposition(self, question: str, domain_profile: dict[str, Any] | None = None) -> dict[str, Any]:
        intent = self._question_intent(question)
        question_terms = [token for token in self._question_tokens(question) if token not in DEPENDENCY_QUESTION_TERMS]
        profile_terms = self._all_profile_terms(
            "data_carriers",
            "source_terms",
            "field_population_terms",
            "api_terms",
            "config_terms",
            "logic_terms",
            "knowledge_terms",
        )
        if domain_profile:
            for value in domain_profile.values():
                if isinstance(value, list):
                    profile_terms.extend(str(item) for item in value)
        components: list[dict[str, Any]] = []

        def add(name: str, terms: list[str]) -> None:
            deduped: list[str] = []
            for term in terms:
                normalized = str(term or "").strip()
                lowered = normalized.lower()
                if len(lowered) < 3 or lowered in STOPWORDS or lowered in deduped:
                    continue
                deduped.append(lowered)
            if deduped:
                components.append({"name": name, "terms": deduped[:18]})

        add("entry_point", ["controller", "service", "engine", "handler", "consumer", *question_terms])
        if intent.get("api"):
            add("api_surface", ["requestmapping", "postmapping", "getmapping", "route", "endpoint", "client", "接口", "路由", "入口", *question_terms])
        if intent.get("data_source"):
            add("source_trace", ["repository", "mapper", "dao", "select", "from", "jdbcTemplate", "client", "integration", "数据源", "上游", "读取", "写入", *question_terms])
            add("carrier_backtrace", ["provider", "builder", "converter", "assembler", "来源", "取数", *profile_terms[:16], *question_terms])
        if intent.get("config"):
            add("configuration", ["properties", "yaml", "configuration", "feature", "config", "配置", "开关", "参数", *question_terms])
        if intent.get("module_dependency"):
            add("module_dependency", ["pom.xml", "build.gradle", "package.json", "maven", "gradle", "npm", "dependency", "artifactId", "groupId", "依赖", "模块", *question_terms])
        if intent.get("rule_logic") or intent.get("error"):
            add("decision_logic", ["validate", "rule", "condition", "exception", "status", "approval", "规则", "校验", "异常", "审批", *question_terms])
        if intent.get("static_qa"):
            add(
                "static_qa",
                [
                    "TODO", "FIXME", "password", "secret", "token", "apiKey", "catch",
                    "Exception", "Throwable", "printStackTrace", "Runtime", "ProcessBuilder",
                    "subprocess", "eval", "exec", "select", "format", *question_terms,
                ],
            )
        if intent.get("impact_analysis"):
            add(
                "impact_analysis",
                [
                    "controller", "handler", "consumer", "service", "repository", "mapper",
                    "dao", "client", "route", "endpoint", "table", "topic", "config",
                    "caller", "callee", "dependency", "影响", "调用方", "下游", "上游", *question_terms,
                ],
            )
        if intent.get("test_coverage"):
            add("test_coverage", ["test", "assert", "verify", "mock", "should", "junit", "pytest", "jest", "测试", "覆盖", "断言", *question_terms])
        if intent.get("operational_boundary"):
            add(
                "operational_boundary",
                [
                    "Transactional", "Cacheable", "CacheEvict", "CachePut", "Async",
                    "Retryable", "CircuitBreaker", "RateLimiter", "Bulkhead",
                    "TimeLimiter", "SchedulerLock", "PreAuthorize", "事务", "缓存", "异步", "重试", "熔断", "限流", "鉴权", *question_terms,
                ],
            )

        return {
            "mode": "query_decomposition",
            "intent": intent,
            "components": components[:5],
        }

    def _rank_matches(
        self,
        question: str,
        matches: list[dict[str, Any]],
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not matches:
            return []
        ranked = []
        question_features = self._question_retrieval_features(question, request_cache=request_cache)
        for match in matches:
            enriched = dict(match)
            enriched["rerank_score"] = self._rerank_score_cached(
                question,
                enriched,
                question_features=question_features,
                request_cache=request_cache,
            )
            ranked.append(enriched)
        ranked.sort(key=lambda item: item.get("rerank_score", item.get("score", 0)), reverse=True)
        return ranked

    def _question_retrieval_features(
        self,
        question: str,
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if request_cache is not None:
            features_cache = request_cache.setdefault("question_features", {})
            cached = features_cache.get(question)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "question_feature_hits")
                return cached
            self._increment_retrieval_stat(request_cache, "question_feature_misses")
        features = {
            "intent": self._question_intent(question),
            "tokens": set(self._question_tokens(question)),
            "specific_terms": {
                term.lower()
                for term in self._question_specific_retrieval_terms(question)
                if len(str(term or "").strip()) >= 4
            },
        }
        if request_cache is not None:
            request_cache.setdefault("question_features", {})[question] = features
        return features

    @staticmethod
    def _rerank_cache_key(question: str, match: dict[str, Any]) -> tuple[Any, ...]:
        return (
            question,
            match.get("path"),
            match.get("score"),
            match.get("snippet"),
            match.get("retrieval"),
            match.get("trace_stage"),
            bool(match.get("static_qa")),
            bool(match.get("test_coverage")),
            bool(match.get("operational_boundary")),
        )

    def _rerank_score_cached(
        self,
        question: str,
        match: dict[str, Any],
        *,
        question_features: dict[str, Any] | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> int:
        if request_cache is None:
            return self._rerank_score(question, match, question_features=question_features)
        cache_key = self._rerank_cache_key(question, match)
        rerank_cache = request_cache.setdefault("rerank", {})
        cached = rerank_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "rerank_hits")
            return int(cached)
        self._increment_retrieval_stat(request_cache, "rerank_misses")
        score = self._rerank_score(question, match, question_features=question_features)
        rerank_cache[cache_key] = score
        return score

    def _rerank_score(
        self,
        question: str,
        match: dict[str, Any],
        *,
        question_features: dict[str, Any] | None = None,
    ) -> int:
        score = int(match.get("score") or 0)
        question_features = question_features or self._question_retrieval_features(question)
        intent = question_features.get("intent") or {}
        path = str(match.get("path") or "").lower()
        snippet = str(match.get("snippet") or "").lower()
        retrieval = str(match.get("retrieval") or "")
        trace_stage = str(match.get("trace_stage") or "")
        lowered_question = str(question or "").lower()
        question_tokens = set(question_features.get("tokens") or [])
        specific_terms = set(question_features.get("specific_terms") or [])
        path_stem = Path(path).stem.lower()
        exact_lookup = match.get("exact_lookup") or {}
        if trace_stage == "exact_lookup" or retrieval == "exact_table_path_lookup":
            score += 260
            exact_term = str(exact_lookup.get("term") or "").lower()
            exact_lookup_value = str(exact_lookup.get("lookup_value") or exact_term).lower()
            if exact_term and (exact_term in snippet or exact_term in path):
                score += 120
            elif exact_lookup_value and (exact_lookup_value in snippet or exact_lookup_value in path):
                score += 90
            if any(marker in path for marker in ("repository", "mapper", "dao", "job", "service", "config", "properties", "yml", "yaml", "sql", "xml")):
                score += 35
        if path_stem and path_stem in question_tokens:
            score += 80
        if trace_stage == "focused_search":
            score += 90
        if specific_terms:
            path_specific_hits = [term for term in specific_terms if term in path]
            snippet_specific_hits = [term for term in specific_terms if term in snippet]
            score += min(len(path_specific_hits), 4) * 120
            score += min(len(snippet_specific_hits), 4) * 95
        if "cbs" in lowered_question and "credit review" in lowered_question:
            if "creditreview" in path and ("cbs" in path or "cbs" in snippet):
                score += 120
            if "bulk" in path and "creditreview" not in path:
                score -= 50
        if trace_stage == "query_decomposition":
            score += 20
        if retrieval in {"flow_graph", "code_graph"}:
            score += 18
        if retrieval in {"entity_graph", "planner_definition", "planner_reference"}:
            score += 14
        if intent.get("data_source"):
            if any(term in path for term in ("repository", "mapper", "dao", "client", "integration")):
                score += 35
            if re.search(r"\bselect\b.+\bfrom\b", snippet):
                score += 35
            if any(term in snippet for term in ("jdbctemplate", "resttemplate", "webclient", "feign")):
                score += 22
            if self._match_answer_grade(match, intent_label="data_source"):
                score += 150
            else:
                role = self._evidence_role(path, snippet, str(match.get("reason") or ""))
                if role in {"logic", "definition", "supporting", "test"}:
                    score -= 90
                if self._match_is_definition_only(match, list(specific_terms or question_tokens)):
                    score -= 60
                reason = str(match.get("reason") or "").lower()
                if any(marker in reason for marker in ("bm25 content match", "structure matched", "path matched")):
                    score -= 45
        if intent.get("api"):
            if any(term in path for term in ("controller", "client", "api")):
                score += 24
            if any(term in snippet for term in ("requestmapping", "postmapping", "getmapping", "endpoint")):
                score += 24
        if intent.get("config"):
            if path.endswith((".properties", ".yaml", ".yml", ".toml", ".conf")):
                score += 30
        if intent.get("module_dependency"):
            if path.endswith(("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "package.json")):
                score += 64
            if ("npm" in lowered_question or "package" in lowered_question or "node" in lowered_question) and path.endswith("package.json"):
                score += 90
            if ("gradle" in lowered_question or "multi-module" in lowered_question) and (
                path.endswith(("build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts"))
                or ".gradle" in path
            ):
                score += 90
            if ("maven" in lowered_question or "pom" in lowered_question) and path.endswith("pom.xml"):
                score += 90
            if any(term in snippet for term in ("artifactid", "groupid", "implementation project", "dependencies", "module_dependency")):
                score += 28
        if intent.get("message_flow"):
            if any(term in path for term in ("event", "message", "consumer", "producer", "listener")):
                score += 48
            if any(term in snippet for term in ("kafkalistener", "rabbitlistener", "jmslistener", "kafkatemplate", ".send(", "topic", "queue")):
                score += 32
        if intent.get("rule_logic") or intent.get("error"):
            if any(term in snippet for term in ("validate", "condition", "exception", "approval", "permission")):
                score += 20
        if intent.get("static_qa"):
            if retrieval == "static_qa" or match.get("static_qa"):
                score += 42
            if any(term in snippet for term in ("password", "secret", "token", "printstacktrace", "runtime", "processbuilder", "todo", "fixme")):
                score += 20
        if not intent.get("test_coverage"):
            if self._is_test_file_path(path):
                score -= 80
            elif "/src/main/" in path or any(term in path for term in ("service", "provider", "strategy", "repository", "mapper", "client", "controller", "cron", "job")):
                score += 28
        if intent.get("impact_analysis"):
            if retrieval in {"planner_caller", "planner_callee", "flow_graph", "entity_graph", "code_graph"}:
                score += 38
            if any(term in path for term in ("controller", "handler", "consumer", "service", "repository", "mapper", "client")):
                score += 16
        if intent.get("test_coverage"):
            if retrieval == "test_coverage" or match.get("test_coverage"):
                score += 54
            if self._is_test_file_path(path):
                score += 36
            if any(term in snippet for term in ("assert", "verify", "expect", "@test", "should")):
                score += 18
        if intent.get("operational_boundary"):
            if retrieval == "operational_boundary" or match.get("operational_boundary"):
                score += 54
            if any(term in snippet for term in ("@transactional", "@cacheable", "@cacheevict", "@async", "@retryable", "@circuitbreaker", "@ratelimiter", "@schedulerlock", "@preauthorize")):
                score += 30
        return score

    @staticmethod
    def _match_cache_signature(matches: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
        return [
            (
                match.get("repo"),
                match.get("path"),
                match.get("line_start"),
                match.get("line_end"),
                match.get("score"),
                match.get("trace_stage"),
                match.get("retrieval"),
                match.get("reason"),
            )
            for match in matches
        ]

    def _compress_evidence_cached(
        self,
        question: str,
        matches: list[dict[str, Any]],
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if request_cache is None:
            return self._compress_evidence(question, matches)
        cache_key = hashlib.sha1(
            json.dumps(
                {"question": question, "matches": self._match_cache_signature(matches)},
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        evidence_cache = request_cache.setdefault("evidence", {})
        cached = evidence_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "evidence_hits")
            return self._clone_jsonish(cached)
        self._increment_retrieval_stat(request_cache, "evidence_misses")
        summary = self._compress_evidence(question, matches)
        evidence_cache[cache_key] = self._clone_jsonish(summary)
        return summary

    def _compress_evidence(self, question: str, matches: list[dict[str, Any]]) -> dict[str, Any]:
        selected = self._select_llm_matches(matches, 12, question=question)
        summary: dict[str, Any] = {
            "intent": self._question_intent(question),
            "entry_points": [],
            "data_carriers": [],
            "field_population": [],
            "downstream_components": [],
            "data_sources": [],
            "api_or_config": [],
            "module_dependencies": [],
            "message_flows": [],
            "rule_or_error_logic": [],
            "static_findings": [],
            "impact_surfaces": [],
            "test_coverage": [],
            "operational_boundaries": [],
            "source_tiers": [],
            "data_source_tiers": [],
            "source_conflicts": [],
            "source_count": len(selected),
        }
        adders = {key: self._limited_fact_adder(summary[key], 12) for key in summary if isinstance(summary.get(key), list)}
        intent = summary["intent"]
        source_tier_counts: dict[str, int] = {}
        data_source_tier_counts: dict[str, int] = {}

        for match in selected:
            label = self._evidence_label(match)
            path = str(match.get("path") or "")
            path_lower = path.lower()
            snippet = str(match.get("snippet") or "")
            snippet_lower = snippet.lower()
            symbols = IDENTIFIER_PATTERN.findall(snippet)
            source_tier = self._match_source_tier(match)
            source_tier_counts[source_tier] = source_tier_counts.get(source_tier, 0) + 1

            if match.get("trace_stage") == "direct" or any(hint in path_lower for hint in ("controller", "consumer", "scene", "strategy", "service", "engine")):
                adders["entry_points"](f"{label}: {self._compact_path(path)}")

            retrieval = str(match.get("retrieval") or "")
            reason = str(match.get("reason") or "")
            if retrieval in {"planner_caller", "planner_callee", "flow_graph", "code_graph", "entity_graph"}:
                impact_role = {
                    "planner_caller": "upstream caller",
                    "planner_callee": "downstream callee",
                    "flow_graph": "flow dependency",
                    "code_graph": "symbol dependency",
                    "entity_graph": "entity dependency",
                }.get(retrieval, "dependency")
                adders["impact_surfaces"](f"{label}: {impact_role}: {reason or self._compact_path(path)}")
            if retrieval == "test_coverage" or match.get("test_coverage"):
                adders["test_coverage"](f"{label}: {reason or 'test coverage evidence'}")
            if retrieval == "operational_boundary" or match.get("operational_boundary"):
                adders["operational_boundaries"](f"{label}: {reason or 'operational boundary evidence'}")
            if intent.get("module_dependency") and (
                any(path_lower.endswith(suffix) for suffix in ("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "package.json"))
                or "module_dependency" in reason
                or "artifactid" in snippet_lower
                or "implementation project" in snippet_lower
            ):
                adders["module_dependencies"](f"{label}: {reason or self._compact_path(path)}")
            if intent.get("message_flow") and (
                any(term in path_lower for term in ("event", "message", "consumer", "producer", "listener"))
                or any(term in snippet_lower for term in ("kafkalistener", "rabbitlistener", "jmslistener", "kafkatemplate", ".send(", "topic", "queue"))
                or any(term in reason.lower() for term in ("message_publish", "message_consume", "event_publish", "event_consume", "message_topic"))
            ):
                adders["message_flows"](f"{label}: {reason or self._compact_path(path)}")

            for symbol in symbols:
                lowered = symbol.lower()
                if any(lowered.endswith(suffix) for suffix in DATA_CARRIER_SUFFIXES):
                    adders["data_carriers"](f"{symbol} ({label})")
                if any(lowered.endswith(suffix) for suffix in DEPENDENCY_SYMBOL_SUFFIXES):
                    adders["downstream_components"](f"{symbol} ({label})")

            for line in self._interesting_lines(snippet, DATA_SOURCE_HINTS):
                if self._is_concrete_source_line(line):
                    adders["data_sources"](f"{label}: {line}")
                    data_source_tier_counts[source_tier] = data_source_tier_counts.get(source_tier, 0) + 1
            for line in self._interesting_lines(snippet, FIELD_POPULATION_HINTS):
                adders["field_population"](f"{label}: {line}")
            if re.search(r"\bselect\b.+\bfrom\b", snippet_lower, flags=re.IGNORECASE):
                for line in self._interesting_lines(snippet, ("select", " from ")):
                    adders["data_sources"](f"{label}: {line}")
                    data_source_tier_counts[source_tier] = data_source_tier_counts.get(source_tier, 0) + 1
            for line in self._interesting_lines(snippet, API_HINTS + CONFIG_HINTS):
                adders["api_or_config"](f"{label}: {line}")
            for line in self._interesting_lines(snippet, ERROR_HINTS + RULE_HINTS):
                adders["rule_or_error_logic"](f"{label}: {line}")
            if match.get("retrieval") == "static_qa" or match.get("static_qa"):
                finding = match.get("static_qa") or {}
                finding_label = str(finding.get("kind") or "static_qa")
                severity = str(finding.get("severity") or "medium")
                reason = str(finding.get("reason") or match.get("reason") or "static QA finding")
                adders["static_findings"](f"{label}: {severity} {finding_label}: {reason}")
            else:
                for finding in self._static_qa_findings_for_line(snippet):
                    adders["static_findings"](
                        f"{label}: {finding['severity']} {finding['kind']}: {finding['reason']}"
                    )

        for tier, count in sorted(source_tier_counts.items()):
            adders["source_tiers"](f"{tier}:{count}")
        for tier, count in sorted(data_source_tier_counts.items()):
            adders["data_source_tiers"](f"{tier}:{count}")
        if intent.get("data_source") and summary["data_sources"]:
            confirmed_source_tiers = set(data_source_tier_counts)
            if confirmed_source_tiers and not (confirmed_source_tiers & PRODUCTION_EVIDENCE_TIERS):
                adders["source_conflicts"](
                    "Concrete source evidence appears only in test/docs/generated files; production repository/mapper/client evidence is still required."
                )
            if source_tier_counts.get("test") and not (confirmed_source_tiers & PRODUCTION_EVIDENCE_TIERS):
                adders["source_conflicts"](
                    "Test evidence was found without matching production source evidence."
                )
        summary["source_tier_counts"] = source_tier_counts
        summary["data_source_tier_counts"] = data_source_tier_counts
        return summary

    def _build_evidence_pack(
        self,
        *,
        question: str,
        evidence_summary: dict[str, Any],
        matches: list[dict[str, Any]],
        trace_paths: list[dict[str, Any]],
        quality_gate: dict[str, Any],
    ) -> dict[str, Any]:
        pack = self._new_evidence_pack(question, evidence_summary)
        adders = {
            key: self._limited_fact_adder(pack[key], 10)
            for key in pack
            if isinstance(pack.get(key), list)
        }

        def add_item(
            evidence_type: str,
            claim: str,
            *,
            source_id: str = "",
            match: dict[str, Any] | None = None,
            confidence: str = "medium",
            hop: str = "",
            supports_answer: bool = True,
        ) -> None:
            claim_text = re.sub(r"\s+", " ", str(claim or "")).strip()
            if not claim_text:
                return
            item = {
                "type": evidence_type,
                "claim": claim_text[:500],
                "source_id": source_id,
                "confidence": confidence,
                "hop": hop,
                "supports_answer": bool(supports_answer),
            }
            if match:
                item.update(
                    {
                        "repo": match.get("repo"),
                        "path": match.get("path"),
                        "line_start": match.get("line_start"),
                        "line_end": match.get("line_end"),
                        "retrieval": match.get("retrieval") or "file_scan",
                        "trace_stage": match.get("trace_stage") or "direct",
                    }
                )
            if not any(existing.get("type") == item["type"] and existing.get("claim") == item["claim"] for existing in pack["items"]):
                pack["items"].append(item)

        for fact in evidence_summary.get("entry_points") or []:
            adders["entry_points"](str(fact))
            add_item("entry_point", str(fact), confidence="medium", hop="entry")
        for fact in evidence_summary.get("data_sources") or []:
            lowered = str(fact).lower()
            for table in SQL_TABLE_PATTERN.findall(str(fact)):
                adders["tables"](f"{table} ({self._fact_source_label(str(fact))})")
                add_item("table", f"{table} is referenced by {self._fact_source_label(str(fact))}", confidence="high", hop="source")
            if any(term in lowered for term in ("client", "integration", "gateway", "feign", "resttemplate", "webclient", "endpoint", "http")):
                adders["external_dependencies"](str(fact))
                add_item("external_dependency", str(fact), confidence="high", hop="source")
            adders["read_write_points"](str(fact))
            add_item("read_write", str(fact), confidence="high", hop="source")
        for fact in evidence_summary.get("api_or_config") or []:
            lowered = str(fact).lower()
            if any(term in lowered for term in API_HINTS):
                adders["apis"](str(fact))
                add_item("api", str(fact), confidence="high", hop="api")
            if any(term in lowered for term in CONFIG_HINTS):
                adders["configs"](str(fact))
                add_item("config", str(fact), confidence="high", hop="config")
        for fact in evidence_summary.get("module_dependencies") or []:
            adders["module_dependencies"](str(fact))
            adders["external_dependencies"](str(fact))
            add_item("module_dependency", str(fact), confidence="high", hop="dependency")
        for fact in evidence_summary.get("message_flows") or []:
            adders["message_flows"](str(fact))
            adders["external_dependencies"](str(fact))
            add_item("message_flow", str(fact), confidence="high", hop="message")
        for fact in evidence_summary.get("field_population") or []:
            adders["read_write_points"](str(fact))
            add_item("field_population", str(fact), confidence="medium", hop="field_population")
        for fact in evidence_summary.get("downstream_components") or []:
            adders["call_chain"](str(fact))
            add_item("call_chain", str(fact), confidence="medium", hop="downstream")
        for fact in evidence_summary.get("static_findings") or []:
            severity = "high" if " high " in f" {str(fact).lower()} " else "medium" if " medium " in f" {str(fact).lower()} " else "low"
            adders["static_findings"](str(fact))
            add_item("static_finding", str(fact), confidence=severity, hop="static_qa")
        for fact in evidence_summary.get("impact_surfaces") or []:
            lowered = str(fact).lower()
            hop = "upstream" if "upstream" in lowered or "caller" in lowered else "downstream" if "downstream" in lowered or "callee" in lowered else "graph"
            adders["impact_surfaces"](str(fact))
            add_item("impact_surface", str(fact), confidence="medium", hop=hop)
        for fact in evidence_summary.get("test_coverage") or []:
            lowered = str(fact).lower()
            confidence = "high" if any(term in lowered for term in ("assert", "verify", "expect", "test case")) else "medium"
            adders["test_coverage"](str(fact))
            add_item("test_coverage", str(fact), confidence=confidence, hop="test")
        for fact in evidence_summary.get("operational_boundaries") or []:
            lowered = str(fact).lower()
            confidence = "high" if any(term in lowered for term in ("transactional", "cache", "async", "retry", "circuit", "rate", "lock", "authorize")) else "medium"
            adders["operational_boundaries"](str(fact))
            add_item("operational_boundary", str(fact), confidence=confidence, hop="runtime_boundary")
        for fact in evidence_summary.get("source_tiers") or []:
            adders["source_tiers"](str(fact))
        for fact in evidence_summary.get("source_conflicts") or []:
            adders["source_conflicts"](str(fact))
            adders["missing_hops"](str(fact))
            add_item("source_conflict", str(fact), confidence="low", hop="evidence_conflict", supports_answer=False)

        for path in trace_paths[:8]:
            edges = path.get("edges") or []
            if not edges:
                continue
            edge_text = " -> ".join(
                str(edge.get("to_name") or edge.get("to_file") or edge.get("edge_kind") or "").strip()
                for edge in edges
                if str(edge.get("to_name") or edge.get("to_file") or edge.get("edge_kind") or "").strip()
            )
            if edge_text:
                adders["call_chain"](f"{path.get('repo')}: {edge_text}")
                add_item("call_chain", f"{path.get('repo')}: {edge_text}", confidence=str(path.get("confidence") or "medium"), hop="trace_path")

        for index, match in enumerate(matches[:16], start=1):
            source_id = f"S{index}"
            label = self._evidence_label(match)
            path = str(match.get("path") or "")
            snippet = str(match.get("snippet") or "")
            snippet_lower = snippet.lower()
            exact_lookup = match.get("exact_lookup") or {}
            adders["citation_map"](
                f"{source_id}: {label} / {match.get('retrieval') or 'file_scan'} / {match.get('trace_stage') or 'direct'}"
            )
            if exact_lookup:
                term = str(exact_lookup.get("term") or "").strip()
                lookup_value = str(exact_lookup.get("lookup_value") or term).strip()
                display_term = term or lookup_value
                if display_term:
                    claim = f"{display_term} is referenced in {label}"
                    adders["tables"](f"{display_term} ({label})")
                    adders["read_write_points"](claim)
                    add_item("table", claim, source_id=source_id, match=match, confidence="high", hop="source")
                    if lookup_value and lookup_value != display_term:
                        adders["read_write_points"](f"{label}: matched unqualified table name {lookup_value}")
                        add_item(
                            "read_write",
                            f"{label}: matched unqualified table name {lookup_value}",
                            source_id=source_id,
                            match=match,
                            confidence="high",
                            hop="source",
                        )
            if any(term in path.lower() for term in ("controller", "consumer", "handler", "service", "engine", "strategy")):
                adders["entry_points"](f"{label}: {self._compact_path(path)}")
                add_item("entry_point", f"{label}: {self._compact_path(path)}", source_id=source_id, match=match, hop="entry")
            for table in SQL_TABLE_PATTERN.findall(snippet):
                adders["tables"](f"{table} ({label})")
                add_item("table", f"{table} is referenced in {label}", source_id=source_id, match=match, confidence="high", hop="source")
            for literal in HTTP_LITERAL_PATTERN.findall(snippet):
                if literal.startswith(("http://", "https://", "/")):
                    adders["apis"](f"{label}: {literal}")
                    add_item("api", f"{label}: {literal}", source_id=source_id, match=match, confidence="high", hop="api")
            for line in self._interesting_lines(snippet, API_HINTS):
                adders["apis"](f"{label}: {line}")
                add_item("api", f"{label}: {line}", source_id=source_id, match=match, confidence="medium", hop="api")
            for line in self._interesting_lines(snippet, CONFIG_HINTS):
                adders["configs"](f"{label}: {line}")
                add_item("config", f"{label}: {line}", source_id=source_id, match=match, confidence="medium", hop="config")
            if pack["intent"].get("module_dependency") and (
                path.lower().endswith(("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "package.json"))
                or "module_dependency" in str(match.get("reason") or "").lower()
                or "artifactid" in snippet_lower
                or "implementation project" in snippet_lower
            ):
                claim = f"{label}: {str(match.get('reason') or self._compact_path(path))}"
                adders["module_dependencies"](claim)
                adders["external_dependencies"](claim)
                add_item("module_dependency", claim, source_id=source_id, match=match, confidence="high", hop="dependency")
            if pack["intent"].get("message_flow") and (
                any(term in path.lower() for term in ("event", "message", "consumer", "producer", "listener"))
                or any(term in snippet_lower for term in ("kafkalistener", "rabbitlistener", "jmslistener", "kafkatemplate", ".send(", "topic", "queue"))
                or any(term in str(match.get("reason") or "").lower() for term in ("message_publish", "message_consume", "event_publish", "event_consume", "message_topic"))
            ):
                claim = f"{label}: {str(match.get('reason') or self._compact_path(path))}"
                adders["message_flows"](claim)
                adders["external_dependencies"](claim)
                add_item("message_flow", claim, source_id=source_id, match=match, confidence="high", hop="message")
            for line in self._interesting_lines(snippet, FIELD_POPULATION_HINTS):
                adders["read_write_points"](f"{label}: {line}")
                add_item("field_population", f"{label}: {line}", source_id=source_id, match=match, confidence="medium", hop="field_population")
            if any(term in snippet_lower for term in ("feign", "resttemplate", "webclient", "http://", "https://")):
                for line in self._interesting_lines(snippet, ("feign", "resttemplate", "webclient", "http://", "https://")):
                    adders["external_dependencies"](f"{label}: {line}")
                    add_item("external_dependency", f"{label}: {line}", source_id=source_id, match=match, confidence="high", hop="source")
            for finding in self._static_qa_findings_for_line(snippet):
                claim = f"{finding['severity']} {finding['kind']}: {finding['reason']} in {label}"
                adders["static_findings"](claim)
                add_item("static_finding", claim, source_id=source_id, match=match, confidence=str(finding["severity"]), hop="static_qa")
            retrieval = str(match.get("retrieval") or "")
            if retrieval in {"planner_caller", "planner_callee", "flow_graph", "code_graph", "entity_graph"}:
                reason = str(match.get("reason") or "").strip()
                role = {
                    "planner_caller": "upstream caller",
                    "planner_callee": "downstream callee",
                    "flow_graph": "flow dependency",
                    "code_graph": "symbol dependency",
                    "entity_graph": "entity dependency",
                }.get(retrieval, "dependency")
                claim = f"{role}: {reason or label}"
                adders["impact_surfaces"](claim)
                add_item("impact_surface", claim, source_id=source_id, match=match, confidence="medium", hop=role.split(" ")[0])
            if retrieval == "test_coverage" or match.get("test_coverage"):
                reason = str(match.get("reason") or "").strip()
                claim = f"test coverage: {reason or label}"
                adders["test_coverage"](claim)
                add_item("test_coverage", claim, source_id=source_id, match=match, confidence="high" if "assert" in claim.lower() or "verify" in claim.lower() else "medium", hop="test")
            if retrieval == "operational_boundary" or match.get("operational_boundary"):
                reason = str(match.get("reason") or "").strip()
                claim = f"operational boundary: {reason or label}"
                adders["operational_boundaries"](claim)
                add_item("operational_boundary", claim, source_id=source_id, match=match, confidence="high", hop="runtime_boundary")

        for item in quality_gate.get("missing") or []:
            adders["missing_hops"](str(item))
            add_item("missing_hop", str(item), confidence="low", hop="missing", supports_answer=False)
        if pack["intent"].get("data_source") and not pack["tables"] and not pack["external_dependencies"]:
            adders["missing_hops"]("No confirmed table/API/client source found in indexed evidence.")
            add_item("missing_hop", "No confirmed table/API/client source found in indexed evidence.", confidence="low", hop="missing", supports_answer=False)
        if pack["intent"].get("data_source") and evidence_summary.get("data_sources") and not self._has_production_source_tier(evidence_summary):
            adders["missing_hops"]("Production source evidence was not found; current source hits are limited to weaker evidence tiers.")
            add_item(
                "missing_hop",
                "Production source evidence was not found; current source hits are limited to weaker evidence tiers.",
                confidence="low",
                hop="missing",
                supports_answer=False,
            )
        pack["items"] = pack["items"][:40]
        self._classify_evidence_pack_items(pack)
        return pack

    def _new_evidence_pack(self, question: str, evidence_summary: dict[str, Any]) -> dict[str, Any]:
        return {
            "version": 2,
            "intent": evidence_summary.get("intent") or self._question_intent(question),
            "items": [],
            "entry_points": [],
            "call_chain": [],
            "read_write_points": [],
            "external_dependencies": [],
            "module_dependencies": [],
            "message_flows": [],
            "tables": [],
            "apis": [],
            "configs": [],
            "static_findings": [],
            "impact_surfaces": [],
            "test_coverage": [],
            "operational_boundaries": [],
            "source_tiers": [],
            "source_conflicts": [],
            "missing_hops": [],
            "citation_map": [],
            "confirmed_facts": [],
            "inferred_facts": [],
            "missing_facts": [],
            "evidence_limits": [],
        }

    def _classify_evidence_pack_items(self, pack: dict[str, Any]) -> None:
        confirmed_types = {"table", "api", "external_dependency", "config", "static_finding", "test_coverage", "operational_boundary"}
        inferred_types = {"entry_point", "call_chain", "field_population", "read_write", "impact_surface"}
        confirmed: list[str] = []
        inferred: list[str] = []
        missing: list[str] = []
        limits: list[str] = []
        for item in pack.get("items") or []:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "")
            claim = re.sub(r"\s+", " ", str(item.get("claim") or "")).strip()
            if not claim:
                continue
            if item.get("supports_answer") is False or item_type == "missing_hop":
                item["support_level"] = "missing"
                if claim not in missing:
                    missing.append(claim[:420])
                continue
            confidence = str(item.get("confidence") or "").lower()
            if item_type in confirmed_types and confidence in {"high", "medium"}:
                item["support_level"] = "confirmed"
                if claim not in confirmed:
                    confirmed.append(claim[:420])
            elif item_type == "read_write" and confidence == "high":
                item["support_level"] = "confirmed"
                if claim not in confirmed:
                    confirmed.append(claim[:420])
            elif item_type in inferred_types or confidence in {"low", "medium"}:
                item["support_level"] = "inferred"
                if claim not in inferred:
                    inferred.append(claim[:420])
            else:
                item["support_level"] = "confirmed"
                if claim not in confirmed:
                    confirmed.append(claim[:420])
        if pack.get("intent", {}).get("data_source") and not confirmed:
            limits.append("No confirmed table/API/client/config source evidence was found; carrier and call-chain evidence must not be treated as final source.")
        if pack.get("missing_hops"):
            limits.extend(str(item) for item in pack.get("missing_hops") or [])
        pack["confirmed_facts"] = confirmed[:12]
        pack["inferred_facts"] = inferred[:12]
        pack["missing_facts"] = missing[:12]
        pack["evidence_limits"] = list(dict.fromkeys(limits))[:8]

    @staticmethod
    def _fact_source_label(fact: str) -> str:
        match = re.match(r"^([^:]+:[^:]+:\d+-\d+)", str(fact or ""))
        return match.group(1) if match else "evidence"

    @staticmethod
    def _limited_fact_adder(target: list[str], limit: int):
        def add(value: str) -> None:
            value = re.sub(r"\s+", " ", str(value or "")).strip()
            if not value or value in target or len(target) >= limit:
                return
            target.append(value[:420])

        return add

    @staticmethod
    def _evidence_label(match: dict[str, Any]) -> str:
        return f"{match.get('repo')}:{match.get('path')}:{match.get('line_start')}-{match.get('line_end')}"

    @staticmethod
    def _evidence_source_tier(path: str) -> str:
        normalized = "/" + str(path or "").replace("\\", "/").lower().strip("/")
        name = Path(str(path or "")).name.lower()
        if any(marker in normalized for marker in ("/test/", "/tests/", "/src/test/", "/__tests__/", "/spec/")):
            return "test"
        if any(marker in normalized for marker in ("/docs/", "/doc/", "/readme", "/design/", "/wiki/")) or name.endswith((".md", ".adoc", ".rst")):
            return "docs"
        if any(marker in normalized for marker in ("/generated/", "/target/", "/build/", "/dist/", "/coverage/")):
            return "generated"
        if name.endswith((".properties", ".yml", ".yaml", ".toml", ".ini", ".conf")) or "/config/" in normalized:
            return "config"
        return "production"

    @classmethod
    def _match_source_tier(cls, match: dict[str, Any]) -> str:
        return cls._evidence_source_tier(str(match.get("path") or ""))

    @staticmethod
    def _compact_path(path: str) -> str:
        parts = [part for part in str(path or "").split("/") if part]
        if len(parts) <= 4:
            return str(path or "")
        return "/".join(parts[-4:])

    @staticmethod
    def _interesting_lines(snippet: str, hints: tuple[str, ...]) -> list[str]:
        lines: list[str] = []
        for raw_line in str(snippet or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            lowered = f" {line.lower()} "
            if any(hint in lowered for hint in hints) or re.search(r"\bselect\b.+\bfrom\b", lowered):
                lines.append(line)
            if len(lines) >= 5:
                break
        return lines

    @staticmethod
    def _static_qa_findings_for_line(line: str) -> list[dict[str, Any]]:
        text = str(line or "").strip()
        if not text:
            return []
        findings: list[dict[str, Any]] = []
        for rule in STATIC_QA_RULES:
            pattern = rule.get("pattern")
            if pattern.search(text):
                findings.append(
                    {
                        "kind": str(rule.get("kind") or "static_qa"),
                        "severity": str(rule.get("severity") or "medium"),
                        "score": int(rule.get("score") or 150),
                        "reason": str(rule.get("reason") or "static QA finding"),
                    }
                )
        return findings

    @staticmethod
    def _is_concrete_source_line(line: str) -> bool:
        lowered = str(line or "").strip().lower()
        if not lowered:
            return False
        if lowered.startswith(("import ", "package ")):
            return False
        if re.match(r"^\s*(private|protected|public)?\s*(final\s+)?[A-Za-z0-9_<>, ?]+\s+[A-Za-z0-9_]*(dao|mapper|repository|client|integration)\s*;?$", str(line or ""), re.IGNORECASE):
            return False
        return any(hint in f" {lowered} " for hint in CONCRETE_SOURCE_HINTS)

    def _answer_policy_names(self, intent: dict[str, Any]) -> list[str]:
        names: list[str] = []
        if intent.get("data_source"):
            names.append("data_source")
        if intent.get("api"):
            names.append("api")
        if intent.get("config"):
            names.append("config")
        if intent.get("module_dependency"):
            names.append("module_dependency")
        if intent.get("message_flow"):
            names.append("message_flow")
        if intent.get("error") or intent.get("rule_logic"):
            names.append("logic")
        if intent.get("static_qa"):
            names.append("static_qa")
        if intent.get("impact_analysis"):
            names.append("impact_analysis")
        if intent.get("test_coverage"):
            names.append("test_coverage")
        if intent.get("operational_boundary"):
            names.append("operational_boundary")
        return names or ["general"]

    def _evaluate_answer_policies(self, evidence_summary: dict[str, Any], intent: dict[str, Any]) -> dict[str, Any]:
        policy_results: list[dict[str, Any]] = []
        missing: list[str] = []
        score = 0
        for name in self._answer_policy_names(intent):
            policy = ANSWER_POLICY_REGISTRY[name]
            required_buckets = list(policy.get("required_any") or [])
            supporting_buckets = list(policy.get("supporting_any") or [])
            satisfied_buckets = [bucket for bucket in required_buckets if evidence_summary.get(bucket)]
            policy_ok = bool(satisfied_buckets)
            if name == "data_source":
                policy_ok = policy_ok and self._has_concrete_source_evidence(evidence_summary)
            if policy_ok:
                score += 2
                supporting_hits = [bucket for bucket in supporting_buckets if evidence_summary.get(bucket)]
                score += min(len(supporting_hits), 2)
            else:
                missing.append(str(policy["missing"]))
                supporting_hits = []
            policy_results.append(
                {
                    "name": name,
                    "label": policy["label"],
                    "status": "satisfied" if policy_ok else "missing",
                    "required_any": required_buckets,
                    "satisfied_buckets": satisfied_buckets,
                    "supporting_buckets": supporting_hits,
                    "missing": [] if policy_ok else [str(policy["missing"])],
                }
            )
        return {"policies": policy_results, "missing": list(dict.fromkeys(missing)), "score": score}

    def _quality_gate_cached(
        self,
        question: str,
        evidence_summary: dict[str, Any],
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._quality_judge.quality_gate_cached(
            question,
            evidence_summary,
            request_cache=request_cache,
        )

    def _quality_gate_cached_impl(
        self,
        question: str,
        evidence_summary: dict[str, Any],
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if request_cache is None:
            return self._quality_gate(question, evidence_summary)
        cache_key = hashlib.sha1(
            json.dumps(
                {"question": question, "evidence_summary": evidence_summary},
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        quality_cache = request_cache.setdefault("quality", {})
        cached = quality_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "quality_hits")
            return self._clone_jsonish(cached)
        self._increment_retrieval_stat(request_cache, "quality_misses")
        quality = self._quality_gate(question, evidence_summary)
        quality_cache[cache_key] = self._clone_jsonish(quality)
        return quality

    def _quality_gate(self, question: str, evidence_summary: dict[str, Any]) -> dict[str, Any]:
        return self._quality_judge.quality_gate(question, evidence_summary)

    def _quality_gate_impl(self, question: str, evidence_summary: dict[str, Any]) -> dict[str, Any]:
        intent = evidence_summary.get("intent") or self._question_intent(question)
        policy_evaluation = self._evaluate_answer_policies(evidence_summary, intent)
        missing: list[str] = []
        checks: list[str] = []
        score = int(policy_evaluation.get("score") or 0)

        if intent.get("data_source"):
            checks.append("data_source")
            if evidence_summary.get("data_carriers"):
                score += 1
            if evidence_summary.get("field_population"):
                score += 1
            if evidence_summary.get("downstream_components"):
                score += 1
            if evidence_summary.get("data_sources") and not self._has_production_source_tier(evidence_summary):
                missing.append("production repository/mapper/client/table evidence beyond test/docs/generated files")
                score = max(0, score - 2)

        if intent.get("api"):
            checks.append("api")

        if intent.get("config"):
            checks.append("config")

        if intent.get("module_dependency"):
            checks.append("module_dependency")

        if intent.get("message_flow"):
            checks.append("message_flow")

        if intent.get("error") or intent.get("rule_logic"):
            checks.append("logic")

        if intent.get("static_qa"):
            checks.append("static_qa")

        if intent.get("impact_analysis"):
            checks.append("impact_analysis")

        if intent.get("test_coverage"):
            checks.append("test_coverage")

        if intent.get("operational_boundary"):
            checks.append("operational_boundary")

        if not checks:
            checks.append("general")
        missing.extend(policy_evaluation.get("missing") or [])

        status = "sufficient" if not missing and score >= 2 else "needs_more_trace"
        confidence = "high" if status == "sufficient" and score >= 4 else "medium" if status == "sufficient" else "low"
        return {
            "status": status,
            "confidence": confidence,
            "checks": checks,
            "missing": list(dict.fromkeys(missing)),
            "score": score,
            "policies": policy_evaluation.get("policies") or [],
        }

    @staticmethod
    def _has_concrete_source_evidence(evidence_summary: dict[str, Any]) -> bool:
        combined = " ".join(str(value) for value in evidence_summary.get("data_sources") or []).lower()
        return any(hint in combined for hint in CONCRETE_SOURCE_HINTS)

    @staticmethod
    def _has_production_source_tier(evidence_summary: dict[str, Any]) -> bool:
        tiers = set(evidence_summary.get("data_source_tier_counts") or {})
        if not tiers:
            for item in evidence_summary.get("data_source_tiers") or []:
                tier = str(item).split(":", 1)[0].strip().lower()
                if tier:
                    tiers.add(tier)
        if not tiers and evidence_summary.get("data_sources"):
            return True
        return bool(tiers & PRODUCTION_EVIDENCE_TIERS)

    def _quality_gate_trace_terms(
        self,
        question: str,
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        matches: list[dict[str, Any]],
    ) -> list[str]:
        return self._quality_judge.quality_gate_trace_terms(question, evidence_summary, quality_gate, matches)

    def _quality_gate_trace_terms_impl(
        self,
        question: str,
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        matches: list[dict[str, Any]],
    ) -> list[str]:
        terms: list[str] = []
        lowered_question = question.lower()
        if "data_source" in (quality_gate.get("checks") or []) or "source" in lowered_question or "data" in lowered_question:
            terms.extend(
                [
                    "repository", "mapper", "dao", "jdbcTemplate", "queryForObject", "select",
                    "from", "DataSourceResult", "dataSourceResult", "integration", "client",
                    "provider", "DataSource", "input", "context", "result", "request",
                    "response", "profile", "info", "set", "get", "populate",
                ]
            )
        if "api" in (quality_gate.get("checks") or []):
            terms.extend(["controller", "requestmapping", "postmapping", "getmapping", "client", "endpoint"])
        if "config" in (quality_gate.get("checks") or []):
            terms.extend(["properties", "yaml", "configuration", "config", "feature"])
        if "static_qa" in (quality_gate.get("checks") or []):
            terms.extend(
                [
                    "TODO", "FIXME", "password", "secret", "token", "apiKey", "catch",
                    "Exception", "Throwable", "printStackTrace", "Runtime", "ProcessBuilder",
                    "subprocess", "eval", "exec", "select", "format",
                ]
            )
        if "impact_analysis" in (quality_gate.get("checks") or []):
            terms.extend(
                [
                    "controller", "service", "repository", "mapper", "client", "handler",
                    "consumer", "producer", "route", "endpoint", "call", "usage",
                    "dependency", "downstream", "upstream",
                ]
            )
        if "test_coverage" in (quality_gate.get("checks") or []):
            terms.extend(["test", "tests", "assert", "assertThat", "verify", "mock", "should", "junit", "pytest", "jest"])
        if "operational_boundary" in (quality_gate.get("checks") or []):
            terms.extend(["Transactional", "rollback", "Cacheable", "CacheEvict", "Async", "Retryable", "CircuitBreaker", "RateLimiter", "Bulkhead", "TimeLimiter", "SchedulerLock", "PreAuthorize"])

        for bucket in ("data_carriers", "field_population", "downstream_components", "entry_points"):
            for fact in evidence_summary.get(bucket) or []:
                terms.extend(IDENTIFIER_PATTERN.findall(str(fact)))
        terms.extend(self._field_backward_terms(evidence_summary))
        for match in matches[:8]:
            terms.extend(self._extract_downstream_symbols(str(match.get("snippet") or "")))
            terms.extend(self._extract_assignment_sources(str(match.get("snippet") or "")))
        deduped: list[str] = []
        question_tokens = set(self._question_tokens(question))
        for term in terms:
            lowered = term.strip().lower()
            if len(lowered) < 4 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
                continue
            if lowered in LOW_VALUE_FOCUS_TERMS and lowered not in {"datasource"}:
                continue
            if lowered in question_tokens and len(lowered) < 8:
                continue
            if lowered not in deduped:
                deduped.append(lowered)
        return deduped[:18]

    @staticmethod
    def _repo_match_score(entry: RepositoryEntry, tokens: list[str]) -> int:
        repo_text = f"{entry.display_name} {entry.url}".lower()
        return sum(10 for token in tokens if token in repo_text)

    @staticmethod
    def _collect_symbols(lines: list[str]) -> set[str]:
        symbols: set[str] = set()
        for line in lines:
            symbols.update(SourceCodeQAService._line_symbols(line.lower()))
        return symbols

    @staticmethod
    def _line_symbols(line: str) -> set[str]:
        return {match.group(0).lower() for match in IDENTIFIER_PATTERN.finditer(line)}

    @staticmethod
    def _is_declaration_line(line: str) -> bool:
        return bool(DECLARATION_HINT_PATTERN.search(line))

    @staticmethod
    def _keyword_proximity_bonus(line: str, tokens: list[str]) -> int:
        hits = [token for token in tokens if token in line]
        if len(hits) >= 3:
            return 10
        if len(hits) == 2:
            return 5
        return 0

    def _hybrid_query_bonus(
        self,
        question: str,
        line: str,
        line_symbols: set[str],
        *,
        intent: dict[str, Any] | None = None,
    ) -> int:
        intent = intent or self._question_intent(question)
        score = 0
        symbol_text = " ".join(line_symbols)
        combined = f" {line} {symbol_text} "
        if intent.get("data_source") and any(hint in combined for hint in CONCRETE_SOURCE_HINTS):
            score += 28
        if intent.get("api") and any(hint in combined for hint in API_HINTS):
            score += 22
        if intent.get("config") and any(hint in combined for hint in CONFIG_HINTS):
            score += 22
        if (intent.get("error") or intent.get("rule_logic")) and any(hint in combined for hint in ERROR_HINTS + RULE_HINTS):
            score += 18
        return score

    @staticmethod
    def _best_snippet_window(lines: list[str], best_line: int) -> tuple[int, int]:
        if not lines:
            return 1, 1
        max_radius = 18
        start = max(1, best_line - 2)
        end = min(len(lines), best_line + 6)
        best_line_text = lines[best_line - 1] if 1 <= best_line <= len(lines) else ""
        best_line_is_container = bool(re.search(r"\b(class|interface|enum)\b", best_line_text))

        for index in range(best_line, max(0, best_line - max_radius), -1):
            line = lines[index - 1]
            if SourceCodeQAService._is_declaration_line(line):
                start = index
                break
            if not line.strip() and index < best_line:
                start = index + 1
                break

        declaration_seen = False
        for index in range(best_line, min(len(lines), best_line + max_radius)):
            line = lines[index - 1]
            if index > best_line and SourceCodeQAService._is_declaration_line(line) and not best_line_is_container:
                end = index - 1
                declaration_seen = True
                break
            if index > best_line and not line.strip():
                end = index - 1
                declaration_seen = True
                break
            end = index

        if not declaration_seen:
            end = min(len(lines), max(end, best_line + 6))
        if end - start > 24:
            end = start + 24
        return start, max(start, end)

    @staticmethod
    def _build_summary(matches: list[dict[str, Any]]) -> str:
        top_files = []
        for match in matches[:5]:
            label = f"{match['repo']}:{match['path']}"
            if label not in top_files:
                top_files.append(label)
        static_count = sum(1 for match in matches if match.get("retrieval") == "static_qa" or match.get("static_qa"))
        test_count = sum(1 for match in matches if match.get("retrieval") == "test_coverage" or match.get("test_coverage"))
        boundary_count = sum(1 for match in matches if match.get("retrieval") == "operational_boundary" or match.get("operational_boundary"))
        if static_count:
            return f"Found {len(matches)} ranked code references including {static_count} static QA findings. Start with: " + "; ".join(top_files[:3])
        if test_count:
            return f"Found {len(matches)} ranked code references including {test_count} test coverage matches. Start with: " + "; ".join(top_files[:3])
        if boundary_count:
            return f"Found {len(matches)} ranked code references including {boundary_count} operational boundary matches. Start with: " + "; ".join(top_files[:3])
        return f"Found {len(matches)} ranked code references. Start with: " + "; ".join(top_files[:3])

    @staticmethod
    def _build_citations(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        citations = []
        seen: set[tuple[Any, Any, Any, Any]] = set()
        for index, match in enumerate(matches, start=1):
            key = (match.get("repo"), match.get("path"), match.get("line_start"), match.get("line_end"))
            if key in seen:
                continue
            seen.add(key)
            citations.append(
                {
                    "id": f"S{len(citations) + 1}",
                    "repo": match.get("repo"),
                    "path": match.get("path"),
                    "line_start": match.get("line_start"),
                    "line_end": match.get("line_end"),
                    "trace_stage": match.get("trace_stage") or "direct",
                    "reason": match.get("reason"),
                    "retrieval": match.get("retrieval") or "file_scan",
                    "rank": index,
                }
            )
        return citations

    @staticmethod
    def _build_evidence_outline(evidence_pack: dict[str, Any], matches: list[dict[str, Any]]) -> dict[str, Any]:
        items = [item for item in evidence_pack.get("items") or [] if isinstance(item, dict)]
        type_counts: dict[str, int] = {}
        support_counts: dict[str, int] = {}
        for item in items:
            item_type = str(item.get("type") or "unknown")
            support = str(item.get("support_level") or "unknown")
            type_counts[item_type] = type_counts.get(item_type, 0) + 1
            support_counts[support] = support_counts.get(support, 0) + 1
        source_counts: dict[str, int] = {}
        for item in items:
            source_id = str(item.get("source_id") or "").strip()
            if source_id:
                source_counts[source_id] = source_counts.get(source_id, 0) + 1
        primary_sources: list[dict[str, Any]] = []
        for index, match in enumerate(matches[:10], start=1):
            source_id = f"S{index}"
            primary_sources.append(
                {
                    "id": source_id,
                    "repo": match.get("repo"),
                    "path": match.get("path"),
                    "line_start": match.get("line_start"),
                    "line_end": match.get("line_end"),
                    "retrieval": match.get("retrieval") or "file_scan",
                    "trace_stage": match.get("trace_stage") or "direct",
                    "evidence_items": source_counts.get(source_id, 0),
                    "reason": match.get("reason"),
                }
            )
        return {
            "version": 1,
            "type_counts": dict(sorted(type_counts.items())),
            "support_counts": dict(sorted(support_counts.items())),
            "primary_sources": primary_sources,
            "confirmed_facts": evidence_pack.get("confirmed_facts") or [],
            "inferred_facts": evidence_pack.get("inferred_facts") or [],
            "missing_facts": evidence_pack.get("missing_facts") or [],
            "evidence_limits": evidence_pack.get("evidence_limits") or [],
            "source_conflicts": evidence_pack.get("source_conflicts") or [],
        }

    def _result_match_priority_sort_key(
        self,
        item: dict[str, Any],
        *,
        intent: dict[str, Any],
        question: str,
    ) -> tuple[int, int, int]:
        retrieval = str(item.get("retrieval") or "")
        trace_stage = str(item.get("trace_stage") or "")
        path = str(item.get("path") or "").lower()
        snippet = str(item.get("snippet") or "").lower()
        lowered_question = str(question or "").lower()
        priority = 0
        if trace_stage == "exact_lookup" or retrieval == "exact_table_path_lookup":
            priority = max(priority, 70)
            if path.endswith((".java", ".kt", ".go", ".py", ".ts", ".js")):
                priority = max(priority, 75)
        if intent.get("static_qa") and (retrieval == "static_qa" or item.get("static_qa")):
            priority = max(priority, 60)
        if intent.get("test_coverage") and (retrieval == "test_coverage" or item.get("test_coverage") or self._is_test_file_path(path)):
            priority = max(priority, 60)
        if intent.get("operational_boundary") and (retrieval == "operational_boundary" or item.get("operational_boundary")):
            priority = max(priority, 60)
        if intent.get("module_dependency") and (
            path.endswith(("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "package.json"))
            or "module_dependency" in str(item.get("reason") or "").lower()
        ):
            priority = max(priority, 58)
        if intent.get("module_dependency"):
            if ("npm" in lowered_question or "package" in lowered_question or "node" in lowered_question) and path.endswith("package.json"):
                priority = max(priority, 72)
            if ("gradle" in lowered_question or "multi-module" in lowered_question) and (
                path.endswith(("build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts"))
                or ".gradle" in path
            ):
                priority = max(priority, 72)
            if ("maven" in lowered_question or "pom" in lowered_question) and path.endswith("pom.xml"):
                priority = max(priority, 72)
        if intent.get("message_flow") and (
            any(term in path for term in ("event", "message", "consumer", "producer", "listener"))
            or any(term in snippet for term in ("kafkalistener", "rabbitlistener", "jmslistener", "kafkatemplate", ".send(", "topic", "queue"))
            or any(term in str(item.get("reason") or "").lower() for term in ("message_publish", "message_consume", "event_publish", "event_consume", "message_topic"))
        ):
            priority = max(priority, 58)
        if intent.get("impact_analysis") and retrieval in {"planner_caller", "planner_callee", "flow_graph", "entity_graph", "code_graph"}:
            priority = max(priority, 45)
        if intent.get("api") and (any(term in path for term in ("controller", "client", "api", "routes")) or any(term in snippet for term in ("requestmapping", "postmapping", "getmapping", "route("))):
            priority = max(priority, 35)
        if intent.get("data_source") and (any(term in path for term in ("repository", "mapper", "dao")) or re.search(r"\bselect\b.+\bfrom\b", snippet)):
            priority = max(priority, 35)
        return (priority, int(item.get("rerank_score", item.get("score", 0)) or 0), int(item.get("score") or 0))

    @staticmethod
    def _result_match_buckets(matches: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        buckets = {
            "direct": [match for match in matches if match.get("trace_stage") == "direct"],
            "exact_lookup": [match for match in matches if match.get("trace_stage") == "exact_lookup"],
            "focused_search": [match for match in matches if match.get("trace_stage") == "focused_search"],
            "query_decomposition": [match for match in matches if match.get("trace_stage") == "query_decomposition"],
            "dependency": [match for match in matches if match.get("trace_stage") == "dependency"],
            "two_hop": [match for match in matches if match.get("trace_stage") == "two_hop"],
            "tool_loop": [match for match in matches if str(match.get("trace_stage") or "").startswith(TOOL_LOOP_TRACE_PREFIX)],
            "impact_analysis": [match for match in matches if match.get("trace_stage") == "impact_analysis"],
            "test_coverage": [match for match in matches if match.get("trace_stage") == "test_coverage" or match.get("retrieval") == "test_coverage"],
            "operational_boundary": [match for match in matches if match.get("trace_stage") == "operational_boundary" or match.get("retrieval") == "operational_boundary"],
            "agent_trace": [match for match in matches if str(match.get("trace_stage") or "").startswith("agent_trace")],
            "agent_plan": [match for match in matches if str(match.get("trace_stage") or "").startswith("agent_plan")],
            "quality_gate": [match for match in matches if match.get("trace_stage") == QUALITY_GATE_TRACE_STAGE],
        }
        for bucket in buckets.values():
            bucket.sort(key=lambda item: item.get("rerank_score", item["score"]), reverse=True)
        buckets["exact_lookup"].sort(
            key=lambda item: (
                1 if str(item.get("path") or "").lower().endswith((".java", ".kt", ".go", ".py", ".ts", ".js")) else 0,
                item.get("rerank_score", item.get("score", 0)),
                item.get("score", 0),
            ),
            reverse=True,
        )
        return buckets

    def _select_result_matches(self, matches: list[dict[str, Any]], limit: int, *, question: str = "") -> list[dict[str, Any]]:
        limit = max(1, int(limit or 1))
        intent = self._question_intent(question) if question else {}
        buckets = self._result_match_buckets(matches)
        selected: list[dict[str, Any]] = []
        seen: set[tuple[Any, Any, Any, Any]] = set()
        exact_terms_seen: set[str] = set()

        def add(match: dict[str, Any] | None) -> None:
            if not match or len(selected) >= limit:
                return
            if match.get("trace_stage") == "exact_lookup" or match.get("retrieval") == "exact_table_path_lookup":
                exact_lookup = match.get("exact_lookup") or {}
                exact_term = str(exact_lookup.get("term") or exact_lookup.get("lookup_value") or "").lower()
                if exact_term and exact_term in exact_terms_seen:
                    return
            key = (match.get("repo"), match.get("path"), match.get("line_start"), match.get("line_end"))
            if key in seen:
                return
            selected.append(match)
            seen.add(key)
            if match.get("trace_stage") == "exact_lookup" or match.get("retrieval") == "exact_table_path_lookup":
                exact_lookup = match.get("exact_lookup") or {}
                exact_term = str(exact_lookup.get("term") or exact_lookup.get("lookup_value") or "").lower()
                if exact_term:
                    exact_terms_seen.add(exact_term)

        for match in buckets["exact_lookup"]:
            exact_lookup = match.get("exact_lookup") or {}
            term = str(exact_lookup.get("term") or "").lower()
            if term and term in exact_terms_seen:
                continue
            add(match)

        if intent.get("impact_analysis"):
            ranked_matches = sorted(matches, key=lambda item: item.get("rerank_score", item.get("score", 0)), reverse=True)
            for predicate in (
                lambda item: str(item.get("retrieval") or "") == "planner_caller",
                lambda item: str(item.get("retrieval") or "") == "planner_callee"
                and any(marker in str(item.get("path") or "").lower() for marker in ("repository", "mapper", "dao", "client")),
                lambda item: str(item.get("retrieval") or "") == "planner_callee",
                lambda item: str(item.get("retrieval") or "") in {"flow_graph", "entity_graph", "code_graph"},
                lambda item: any(marker in str(item.get("path") or "").lower() for marker in ("controller", "handler", "consumer", "job")),
                lambda item: any(marker in str(item.get("path") or "").lower() for marker in ("repository", "mapper", "dao", "client")),
            ):
                for match in ranked_matches:
                    if predicate(match):
                        add(match)
                        break
        if intent.get("test_coverage"):
            ranked_matches = sorted(matches, key=lambda item: item.get("rerank_score", item.get("score", 0)), reverse=True)
            for predicate in (
                lambda item: str(item.get("retrieval") or "") == "test_coverage",
                lambda item: self._is_test_file_path(str(item.get("path") or "")) and any(marker in str(item.get("snippet") or "").lower() for marker in ("assert", "verify", "expect")),
                lambda item: not self._is_test_file_path(str(item.get("path") or "")),
            ):
                for match in ranked_matches:
                    if predicate(match):
                        add(match)
                        break
        if intent.get("operational_boundary"):
            ranked_matches = sorted(matches, key=lambda item: item.get("rerank_score", item.get("score", 0)), reverse=True)
            for predicate in (
                lambda item: str(item.get("retrieval") or "") == "operational_boundary",
                lambda item: "@" in str(item.get("snippet") or "") and any(marker in str(item.get("snippet") or "").lower() for marker in ("transactional", "cache", "async", "retry", "circuit", "rate", "lock", "authorize")),
                lambda item: any(marker in str(item.get("path") or "").lower() for marker in ("service", "controller", "job", "config")),
            ):
                for match in ranked_matches:
                    if predicate(match):
                        add(match)
                        break

        for stage, stage_limit in (
            ("focused_search", 6),
            ("direct", 2 if intent.get("impact_analysis") else 3),
            ("query_decomposition", 2 if intent.get("impact_analysis") else 3),
            ("dependency", 2 if intent.get("impact_analysis") else 3),
            ("two_hop", 2 if intent.get("impact_analysis") else 3),
            ("impact_analysis", 6 if intent.get("impact_analysis") else 0),
            ("test_coverage", 6 if intent.get("test_coverage") else 0),
            ("operational_boundary", 6 if intent.get("operational_boundary") else 0),
            ("tool_loop", 7 if intent.get("impact_analysis") else 5),
            ("agent_trace", 8),
            ("agent_plan", 6),
            ("quality_gate", 4),
        ):
            for match in buckets[stage][:stage_limit]:
                add(match)
        for match in sorted(matches, key=lambda item: item["score"], reverse=True):
            add(match)
            if len(selected) >= limit:
                break

        if any(intent.get(key) for key in ("data_source", "static_qa", "test_coverage", "operational_boundary", "module_dependency", "message_flow")) and not intent.get("impact_analysis"):
            selected.sort(
                key=lambda item: self._result_match_priority_sort_key(
                    item,
                    intent=intent,
                    question=question,
                ),
                reverse=True,
            )
        else:
            selected.sort(key=lambda item: item["score"], reverse=True)
        return selected

    def _llm_answer_evidence_context(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        pm_team: str,
        country: str,
        matches: list[dict[str, Any]],
        match_limit: int,
        request_cache: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return self._answer_generation.llm_answer_evidence_context(
            entries=entries,
            key=key,
            question=question,
            pm_team=pm_team,
            country=country,
            matches=matches,
            match_limit=match_limit,
            request_cache=request_cache,
        )

    def _llm_answer_evidence_context_impl(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        pm_team: str,
        country: str,
        matches: list[dict[str, Any]],
        match_limit: int,
        request_cache: dict[str, Any] | None,
    ) -> dict[str, Any]:
        selected_matches = self._select_llm_matches(matches, match_limit, question=question)
        evidence_summary = self._compress_evidence_cached(question, selected_matches, request_cache=request_cache)
        trace_paths = self._build_trace_paths(
            entries=entries,
            key=key,
            matches=selected_matches,
            question=question,
            request_cache=request_cache,
        )
        if trace_paths:
            evidence_summary["trace_paths"] = trace_paths
        quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
        evidence_pack = self._build_evidence_pack(
            question=question,
            evidence_summary=evidence_summary,
            matches=selected_matches,
            trace_paths=trace_paths,
            quality_gate=quality_gate,
        )
        domain_context = self._llm_domain_context(
            pm_team=pm_team,
            country=country,
            question=question,
            evidence_summary=evidence_summary,
        )
        return {
            "selected_matches": selected_matches,
            "evidence_summary": evidence_summary,
            "trace_paths": trace_paths,
            "quality_gate": quality_gate,
            "evidence_pack": evidence_pack,
            "domain_context": domain_context,
        }

    def _build_llm_answer(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        pm_team: str,
        country: str,
        question: str,
        matches: list[dict[str, Any]],
        llm_budget_mode: str,
        query_mode: str = QUERY_MODE_DEEP,
        trace_id: str = "",
        followup_context: dict[str, Any] | None = None,
        requested_answer_mode: str = ANSWER_MODE_AUTO,
        request_cache: dict[str, Any] | None = None,
        progress_callback: Any | None = None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
        effort_assessment: bool = False,
    ) -> dict[str, Any]:
        return self._answer_generation.build_llm_answer(
            entries=entries,
            key=key,
            pm_team=pm_team,
            country=country,
            question=question,
            matches=matches,
            llm_budget_mode=llm_budget_mode,
            query_mode=query_mode,
            trace_id=trace_id,
            followup_context=followup_context,
            requested_answer_mode=requested_answer_mode,
            request_cache=request_cache,
            progress_callback=progress_callback,
            attachments=attachments,
            runtime_evidence=runtime_evidence,
            effort_assessment=effort_assessment,
        )

    def _build_llm_answer_impl(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        pm_team: str,
        country: str,
        question: str,
        matches: list[dict[str, Any]],
        llm_budget_mode: str,
        query_mode: str = QUERY_MODE_DEEP,
        trace_id: str = "",
        followup_context: dict[str, Any] | None = None,
        requested_answer_mode: str = ANSWER_MODE_AUTO,
        request_cache: dict[str, Any] | None = None,
        progress_callback: Any | None = None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
        effort_assessment: bool = False,
    ) -> dict[str, Any]:
        if not self.llm_ready():
            raise ToolError(self.llm_unavailable_message())
        query_mode = self.normalize_query_mode(query_mode)
        routed_budget_mode, budget, llm_route = self._resolve_llm_budget(llm_budget_mode, question, matches)
        llm_route = {
            **llm_route,
            "query_mode": query_mode,
            "deadline_seconds": 0,
        }
        if effort_assessment:
            llm_route["task"] = "effort_assessment"
        selected_model = self._model_for_role_or_budget("answer", budget)
        answer_context = self._llm_answer_evidence_context(
            entries=entries,
            key=key,
            question=question,
            matches=matches,
            match_limit=int(budget["match_limit"]),
            pm_team=pm_team,
            country=country,
            request_cache=request_cache,
        )
        selected_matches = answer_context["selected_matches"]
        evidence_summary = answer_context["evidence_summary"]
        quality_gate = answer_context["quality_gate"]
        evidence_pack = answer_context["evidence_pack"]
        return self._build_codex_llm_answer(
            entries=entries,
            key=key,
            pm_team=pm_team,
            country=country,
            question=question,
            matches=matches,
            selected_matches=selected_matches,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            evidence_pack=evidence_pack,
            llm_budget_mode=llm_budget_mode,
            query_mode=query_mode,
            trace_id=trace_id,
            routed_budget_mode=routed_budget_mode,
            budget=budget,
            llm_route=llm_route,
            selected_model=selected_model,
            followup_context=followup_context,
            requested_answer_mode=requested_answer_mode,
            request_cache=request_cache,
            progress_callback=progress_callback,
            attachments=attachments or [],
            runtime_evidence=runtime_evidence or [],
            effort_assessment=effort_assessment,
        )

    def _build_codex_llm_answer(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        pm_team: str,
        country: str,
        question: str,
        matches: list[dict[str, Any]],
        selected_matches: list[dict[str, Any]],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        evidence_pack: dict[str, Any],
        llm_budget_mode: str,
        routed_budget_mode: str,
        budget: dict[str, Any],
        llm_route: dict[str, Any],
        selected_model: str,
        followup_context: dict[str, Any] | None,
        requested_answer_mode: str,
        query_mode: str = QUERY_MODE_DEEP,
        trace_id: str = "",
        request_cache: dict[str, Any] | None = None,
        progress_callback: Any | None = None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
        effort_assessment: bool = False,
    ) -> dict[str, Any]:
        return self._answer_generation.build_codex_llm_answer(
            entries=entries,
            key=key,
            pm_team=pm_team,
            country=country,
            question=question,
            matches=matches,
            selected_matches=selected_matches,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            evidence_pack=evidence_pack,
            llm_budget_mode=llm_budget_mode,
            routed_budget_mode=routed_budget_mode,
            budget=budget,
            llm_route=llm_route,
            selected_model=selected_model,
            followup_context=followup_context,
            requested_answer_mode=requested_answer_mode,
            query_mode=query_mode,
            trace_id=trace_id,
            request_cache=request_cache,
            progress_callback=progress_callback,
            attachments=attachments,
            runtime_evidence=runtime_evidence,
            effort_assessment=effort_assessment,
        )

    def _build_codex_llm_answer_impl(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        pm_team: str,
        country: str,
        question: str,
        matches: list[dict[str, Any]],
        selected_matches: list[dict[str, Any]],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        evidence_pack: dict[str, Any],
        llm_budget_mode: str,
        routed_budget_mode: str,
        budget: dict[str, Any],
        llm_route: dict[str, Any],
        selected_model: str,
        followup_context: dict[str, Any] | None,
        requested_answer_mode: str,
        query_mode: str = QUERY_MODE_DEEP,
        trace_id: str = "",
        request_cache: dict[str, Any] | None = None,
        progress_callback: Any | None = None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
        effort_assessment: bool = False,
    ) -> dict[str, Any]:
        query_mode = self.normalize_query_mode(query_mode)
        timing: dict[str, int] = {}

        candidate_context = self._codex_initial_candidate_context(
            entries=entries,
            key=key,
            question=question,
            matches=matches,
            selected_matches=selected_matches,
            followup_context=followup_context,
        )
        candidate_matches = candidate_context["candidate_matches"]
        candidate_paths = candidate_context["candidate_paths"]
        candidate_path_layers = candidate_context["candidate_path_layers"]
        scope_roots = candidate_context["scope_roots"]
        prompt_mode = candidate_context["prompt_mode"]
        llm_route = {
            **llm_route,
            **self._codex_initial_route_fields(
                selected_model=selected_model,
                prompt_mode=prompt_mode,
                candidate_paths=candidate_paths,
                candidate_path_layers=candidate_path_layers,
                scope_roots=scope_roots,
                query_mode=query_mode,
            ),
        }
        if effort_assessment:
            llm_route["task"] = "effort_assessment"
        prompt_context = self._codex_initial_prompt_context(
            prompt_mode=prompt_mode,
            pm_team=pm_team,
            country=country,
            question=question,
            candidate_paths=candidate_paths,
            evidence_pack=evidence_pack,
            quality_gate=quality_gate,
            followup_context=followup_context,
            attachments=attachments or [],
            runtime_evidence=runtime_evidence or [],
            scope_roots=scope_roots,
        )
        initial_prompt_stats = self._codex_prompt_stats(prompt_context)
        candidate_repo_count = len({item.get("repo") for item in candidate_paths})
        self._log_codex_prompt_timing(
            prompt_context=prompt_context,
            prompt_stats=initial_prompt_stats,
            trace_id=trace_id,
            selected_model=selected_model,
            query_mode=query_mode,
            phase="initial",
            prompt_mode=prompt_mode,
            pm_team=pm_team,
            country=country,
            candidate_path_count=len(candidate_paths),
            candidate_repo_count=candidate_repo_count,
            scope_repo_count=len(scope_roots),
        )
        is_followup = bool(followup_context and (followup_context.get("used") or followup_context.get("question") or followup_context.get("recent_turns")))
        cache_key = self._answer_cache_key(
            provider=self.llm_provider.name,
            model=selected_model,
            question=question,
            answer_mode=requested_answer_mode,
            llm_budget_mode=routed_budget_mode,
            context=prompt_context,
        )
        cached = self._load_cached_answer(cache_key)
        if cached is not None:
            return self._cached_codex_answer_payload(
                cached=cached,
                question=question,
                structured_answer=self._parse_structured_answer(str(cached.get("answer") or "")),
                evidence_summary=evidence_summary,
                quality_gate=quality_gate,
                evidence_pack=evidence_pack,
                candidate_matches=candidate_matches,
                candidate_paths=candidate_paths,
                scope_roots=scope_roots,
                prompt_mode=prompt_mode,
                llm_route=llm_route,
                llm_budget_mode=llm_budget_mode,
                routed_budget_mode=routed_budget_mode,
                cache_key=cache_key,
            )
        codex_cli_session_id = self._codex_cli_session_id(followup_context)
        initial_result = self._codex_initial_answer_result(
            prompt_context=prompt_context,
            prompt_mode=prompt_mode,
            progress_callback=progress_callback,
            codex_cli_session_id=codex_cli_session_id,
            attachments=attachments or [],
            trace_id=trace_id,
            initial_prompt_stats=initial_prompt_stats,
            candidate_paths=candidate_paths,
            candidate_repo_count=candidate_repo_count,
            selected_model=selected_model,
            query_mode=query_mode,
            question=question,
            evidence_pack=evidence_pack,
            timing=timing,
            scope_roots=scope_roots,
        )
        answer = initial_result["answer"]
        structured_answer = initial_result["structured_answer"]
        usage = initial_result["usage"]
        effective_model = initial_result["effective_model"]
        attempts = initial_result["attempts"]
        llm_latency_ms = initial_result["llm_latency_ms"]
        llm_attempt_log = initial_result["llm_attempt_log"]
        finish_reason = initial_result["finish_reason"]
        codex_cli_trace = initial_result["codex_cli_trace"]
        codex_initial_ms = initial_result["codex_initial_ms"]
        codex_validation = initial_result["codex_validation"]
        claim_check = initial_result["claim_check"]
        answer_judge = initial_result["answer_judge"]
        repair_attempted = False
        repair_skipped_reason = ""
        repair_reason = ""
        deep_investigation_rounds = 0
        deep_investigation_terms: list[str] = []
        deep_investigation_added = 0
        repair_decision = self._codex_repair_decision(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            quality_gate=quality_gate,
            evidence_pack=evidence_pack,
            answer_judge=answer_judge,
            codex_validation=codex_validation,
            finish_reason=finish_reason,
            effort_assessment=effort_assessment,
            trace_id=trace_id,
            selected_model=selected_model,
            query_mode=query_mode,
        )
        severe_repair_reasons = repair_decision["severe_repair_reasons"]
        repair_issues = repair_decision["repair_issues"]
        deep_needed = repair_decision["deep_needed"]
        repair_issue_count = repair_decision["repair_issue_count"]
        repair_will_run = repair_decision["repair_will_run"]
        repair_decision_ms = repair_decision["repair_decision_ms"]
        if repair_will_run:
            repair_attempted = True
            repair_reason = "; ".join(severe_repair_reasons[:6])
            if deep_needed:
                deep_context = self._codex_deep_investigation_context(
                    entries=entries,
                    key=key,
                    question=question,
                    matches=matches,
                    candidate_matches=candidate_matches,
                    candidate_paths=candidate_paths,
                    candidate_path_layers=candidate_path_layers,
                    llm_route=llm_route,
                    evidence_summary=evidence_summary,
                    quality_gate=quality_gate,
                    evidence_pack=evidence_pack,
                    answer=answer,
                    structured_answer=structured_answer,
                    answer_judge=answer_judge,
                    codex_validation=codex_validation,
                    budget=budget,
                    request_cache=request_cache,
                    followup_context=followup_context,
                    progress_callback=progress_callback,
                    trace_id=trace_id,
                    selected_model=selected_model,
                    query_mode=query_mode,
                )
                candidate_matches = deep_context["candidate_matches"]
                candidate_paths = deep_context["candidate_paths"]
                candidate_path_layers = deep_context["candidate_path_layers"]
                llm_route = deep_context["llm_route"]
                evidence_summary = deep_context["evidence_summary"]
                quality_gate = deep_context["quality_gate"]
                evidence_pack = deep_context["evidence_pack"]
                deep_investigation_rounds = deep_context["deep_investigation_rounds"]
                deep_investigation_terms = deep_context["deep_investigation_terms"]
                deep_investigation_added = deep_context["deep_investigation_added"]
            repair_context_result = self._codex_repair_answer_context(
                pm_team=pm_team,
                country=country,
                question=question,
                answer=answer,
                structured_answer=structured_answer,
                scope_roots=scope_roots,
                candidate_paths=candidate_paths,
                runtime_evidence=runtime_evidence or [],
                repair_issues=repair_issues,
                deep_needed=deep_needed,
                repair_issue_count=repair_issue_count,
                repair_reason=repair_reason,
                deep_investigation_added=deep_investigation_added,
                selected_model=selected_model,
                query_mode=query_mode,
                trace_id=trace_id,
                progress_callback=progress_callback,
                codex_cli_session_id=codex_cli_session_id,
                attachments=attachments or [],
                timing=timing,
                evidence_pack=evidence_pack,
                codex_validation=codex_validation,
                claim_check=claim_check,
                answer_judge=answer_judge,
                usage=usage,
                effective_model=effective_model,
                attempts=attempts,
                llm_latency_ms=llm_latency_ms,
                llm_attempt_log=llm_attempt_log,
                finish_reason=finish_reason,
                codex_cli_trace=codex_cli_trace,
                repair_attempted=repair_attempted,
                repair_skipped_reason=repair_skipped_reason,
            )
            answer = repair_context_result["answer"]
            structured_answer = repair_context_result["structured_answer"]
            codex_validation = repair_context_result["codex_validation"]
            claim_check = repair_context_result["claim_check"]
            answer_judge = repair_context_result["answer_judge"]
            usage = repair_context_result["usage"]
            effective_model = repair_context_result["effective_model"]
            attempts = repair_context_result["attempts"]
            llm_latency_ms = repair_context_result["llm_latency_ms"]
            llm_attempt_log = repair_context_result["llm_attempt_log"]
            finish_reason = repair_context_result["finish_reason"]
            codex_cli_trace = repair_context_result["codex_cli_trace"]
            repair_attempted = repair_context_result["repair_attempted"]
            repair_skipped_reason = repair_context_result["repair_skipped_reason"]
        return self._codex_final_answer_payload(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            claim_check=claim_check,
            answer_judge=answer_judge,
            finish_reason=finish_reason,
            candidate_matches=candidate_matches,
            llm_route=llm_route,
            codex_validation=codex_validation,
            repair_attempted=repair_attempted,
            repair_reason=repair_reason,
            repair_skipped_reason=repair_skipped_reason,
            repair_decision_ms=repair_decision_ms,
            deep_investigation_rounds=deep_investigation_rounds,
            deep_investigation_terms=deep_investigation_terms,
            deep_investigation_added=deep_investigation_added,
            is_followup=is_followup,
            cache_key=cache_key,
            usage=usage,
            effective_model=effective_model,
            query_mode=query_mode,
            routed_budget_mode=routed_budget_mode,
            trace_id=trace_id,
            scope_roots=scope_roots,
            llm_latency_ms=llm_latency_ms,
            codex_initial_ms=codex_initial_ms,
            timing=timing,
            llm_attempt_log=llm_attempt_log,
            codex_cli_trace=codex_cli_trace,
            llm_budget_mode=llm_budget_mode,
            attempts=attempts,
            evidence_pack=evidence_pack,
        )

    def _codex_final_answer_payload(
        self,
        *,
        question: str,
        answer: str,
        structured_answer: dict[str, Any],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        claim_check: dict[str, Any],
        answer_judge: dict[str, Any],
        finish_reason: str,
        candidate_matches: list[dict[str, Any]],
        llm_route: dict[str, Any],
        codex_validation: dict[str, Any],
        repair_attempted: bool,
        repair_reason: str,
        repair_skipped_reason: str,
        repair_decision_ms: int,
        deep_investigation_rounds: int,
        deep_investigation_terms: list[str],
        deep_investigation_added: int,
        is_followup: bool,
        cache_key: str,
        usage: dict[str, Any],
        effective_model: str,
        query_mode: str,
        routed_budget_mode: str,
        trace_id: str,
        scope_roots: list[str],
        llm_latency_ms: int,
        codex_initial_ms: int,
        timing: dict[str, int],
        llm_attempt_log: list[dict[str, Any]],
        codex_cli_trace: dict[str, Any],
        llm_budget_mode: str,
        attempts: int,
        evidence_pack: dict[str, Any],
    ) -> dict[str, Any]:
        final = self._finalize_trusted_model_answer(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            claim_check=claim_check,
            answer_judge=answer_judge,
            finish_reason=finish_reason,
            selected_matches=candidate_matches,
        )
        llm_route = {
            **llm_route,
            **self._codex_repair_route_fields(
                codex_validation=codex_validation,
                repair_attempted=repair_attempted,
                repair_reason=repair_reason,
                repair_skipped_reason=repair_skipped_reason,
                repair_decision_ms=repair_decision_ms,
                deep_investigation_rounds=deep_investigation_rounds,
                deep_investigation_terms=deep_investigation_terms,
                deep_investigation_added=deep_investigation_added,
            ),
        }
        self._store_codex_answer_cache(
            is_followup=is_followup,
            cache_key=cache_key,
            final=final,
            usage=usage,
            quality_gate=quality_gate,
            effective_model=effective_model,
            finish_reason=finish_reason,
            query_mode=query_mode,
            routed_budget_mode=routed_budget_mode,
            trace_id=trace_id,
        )
        codex_cli_summary = self._build_codex_cli_summary(
            llm_route=llm_route,
            codex_validation=codex_validation,
            repair_attempted=repair_attempted,
            repair_reason=repair_reason,
            repair_skipped_reason=repair_skipped_reason,
            scope_roots=scope_roots,
            llm_latency_ms=llm_latency_ms,
            codex_initial_ms=codex_initial_ms,
            timing=timing,
            llm_attempt_log=llm_attempt_log,
            codex_cli_trace=codex_cli_trace,
            deep_investigation_rounds=deep_investigation_rounds,
            deep_investigation_added=deep_investigation_added,
        )
        return self._codex_llm_answer_result_payload(
            final=final,
            routed_budget_mode=routed_budget_mode,
            llm_budget_mode=llm_budget_mode,
            llm_route=llm_route,
            usage=usage,
            effective_model=effective_model,
            attempts=attempts,
            llm_latency_ms=llm_latency_ms,
            llm_attempt_log=llm_attempt_log,
            finish_reason=finish_reason,
            quality_gate=quality_gate,
            claim_check=claim_check,
            answer_judge=answer_judge,
            evidence_pack=evidence_pack,
            codex_cli_summary=codex_cli_summary,
            codex_cli_trace=codex_cli_trace,
            cache_key=cache_key,
            timing=timing,
        )

    def _codex_repair_answer_context(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        answer: str,
        structured_answer: dict[str, Any],
        scope_roots: list[str],
        candidate_paths: list[dict[str, Any]],
        runtime_evidence: list[dict[str, Any]],
        repair_issues: list[str],
        deep_needed: bool,
        repair_issue_count: int,
        repair_reason: str,
        deep_investigation_added: int,
        selected_model: str,
        query_mode: str,
        trace_id: str,
        progress_callback: Any | None,
        codex_cli_session_id: str,
        attachments: list[dict[str, Any]],
        timing: dict[str, int],
        evidence_pack: dict[str, Any],
        codex_validation: dict[str, Any],
        claim_check: dict[str, Any],
        answer_judge: dict[str, Any],
        usage: dict[str, Any],
        effective_model: str,
        attempts: int,
        llm_latency_ms: int,
        llm_attempt_log: list[dict[str, Any]],
        finish_reason: str,
        codex_cli_trace: dict[str, Any],
        repair_attempted: bool,
        repair_skipped_reason: str,
    ) -> dict[str, Any]:
        repair_context = self._codex_repair_brief(
            pm_team=pm_team,
            country=country,
            question=question,
            initial_answer=answer,
            scope_roots=scope_roots,
            candidate_paths=candidate_paths,
            runtime_evidence=runtime_evidence,
            repair_issues=list(dict.fromkeys([
                *[str(issue) for issue in repair_issues if issue],
                *(["Deep investigation: use the expanded candidate paths and explicitly resolve business ambiguity, caller/callee gaps, and missing source hops before finalizing."] if deep_needed else []),
            ])),
        )
        repair_prompt_stats = self._codex_prompt_stats(repair_context)
        repair_candidate_repo_count = len({item.get("repo") for item in candidate_paths})
        self._log_codex_prompt_timing(
            prompt_context=repair_context,
            prompt_stats=repair_prompt_stats,
            trace_id=trace_id,
            selected_model=selected_model,
            query_mode=query_mode,
            phase="repair",
            prompt_mode=CODEX_INVESTIGATION_PROMPT_MODE,
            pm_team=pm_team,
            country=country,
            candidate_path_count=len(candidate_paths),
            candidate_repo_count=repair_candidate_repo_count,
            scope_repo_count=len(scope_roots),
            include_repair_fields=True,
            repair_issue_count=repair_issue_count,
            repair_reason=repair_reason,
            deep_investigation_added=deep_investigation_added,
        )
        if int(repair_prompt_stats["estimated_prompt_tokens"]) > self.codex_repair_prompt_token_limit:
            repair_attempted = False
            repair_skipped_reason = (
                f"repair_prompt_too_large:{repair_prompt_stats['estimated_prompt_tokens']}>{self.codex_repair_prompt_token_limit}"
            )
            _log_source_code_qa_timing(
                "codex_repair_skip",
                elapsed_ms=0,
                trace_id=trace_id,
                provider=self.llm_provider.name,
                model=selected_model,
                query_mode=query_mode,
                reason="repair_prompt_too_large",
                phase="repair",
                estimated_prompt_tokens=repair_prompt_stats["estimated_prompt_tokens"],
                prompt_token_limit=self.codex_repair_prompt_token_limit,
                candidate_path_count=len(candidate_paths),
            )
        else:
            repair_payload = self._codex_payload(
                repair_context,
                progress_callback=progress_callback,
                codex_cli_session_id=codex_cli_session_id,
                image_paths=self._attachment_image_paths(attachments),
                trace_id=trace_id,
                phase="repair",
                prompt_stats=repair_prompt_stats,
                candidate_path_count=len(candidate_paths),
                candidate_repo_count=repair_candidate_repo_count,
                repair_issue_count=repair_issue_count,
            )
            try:
                repair_result = self.llm_provider.generate(
                    payload=repair_payload,
                    primary_model=selected_model,
                    fallback_model=self._llm_fallback_model(),
                )
            except ToolError as error:
                repair_skipped_reason = "repair_failed_kept_initial_answer"
                _log_source_code_qa_timing(
                    "codex_repair_failed",
                    elapsed_ms=0,
                    trace_id=trace_id,
                    provider=self.llm_provider.name,
                    model=selected_model,
                    query_mode=query_mode,
                    phase="repair",
                    error=str(error)[:500],
                )
            else:
                answer = self.llm_provider.extract_text(repair_result.payload)
                structured_answer = self._parse_structured_answer(answer)
                codex_validation = self._timed_codex_call(
                    timing,
                    "citation_validation",
                    lambda: self._validate_codex_citations(answer, candidate_paths, candidate_paths, scope_roots=scope_roots),
                    trace_id=trace_id,
                    selected_model=selected_model,
                    query_mode=query_mode,
                    phase="repair",
                )
                claim_check = self._merge_codex_validation(self._trusted_provider_check(), codex_validation)
                answer_judge = self._timed_codex_call(
                    timing,
                    "answer_judge",
                    lambda: self._run_answer_judge(question, answer, evidence_pack, claim_check),
                    trace_id=trace_id,
                    selected_model=selected_model,
                    query_mode=query_mode,
                    phase="repair",
                )
                repair_usage = self._normalize_llm_usage(repair_result.usage or repair_result.payload.get("usageMetadata") or {})
                usage = self._merge_llm_usage(usage, repair_usage)
                effective_model = repair_result.model
                attempts += repair_result.attempts
                llm_latency_ms += int(repair_result.latency_ms or 0)
                llm_attempt_log.extend(dict(item) for item in repair_result.attempt_log)
                finish_reason = self._llm_finish_reason(repair_result.payload)
                repair_trace = repair_result.payload.get("codex_cli_trace") if isinstance(repair_result.payload.get("codex_cli_trace"), dict) else {}
                if repair_trace:
                    codex_cli_trace = repair_trace
        return {
            "answer": answer,
            "structured_answer": structured_answer,
            "codex_validation": codex_validation,
            "claim_check": claim_check,
            "answer_judge": answer_judge,
            "usage": usage,
            "effective_model": effective_model,
            "attempts": attempts,
            "llm_latency_ms": llm_latency_ms,
            "llm_attempt_log": llm_attempt_log,
            "finish_reason": finish_reason,
            "codex_cli_trace": codex_cli_trace,
            "repair_attempted": repair_attempted,
            "repair_skipped_reason": repair_skipped_reason,
        }

    def _codex_deep_investigation_context(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        matches: list[dict[str, Any]],
        candidate_matches: list[dict[str, Any]],
        candidate_paths: list[dict[str, Any]],
        candidate_path_layers: dict[str, Any],
        llm_route: dict[str, Any],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        evidence_pack: dict[str, Any],
        answer: str,
        structured_answer: dict[str, Any],
        answer_judge: dict[str, Any],
        codex_validation: dict[str, Any],
        budget: dict[str, Any],
        request_cache: dict[str, Any] | None,
        followup_context: dict[str, Any] | None,
        progress_callback: Any | None,
        trace_id: str,
        selected_model: str,
        query_mode: str,
    ) -> dict[str, Any]:
        deep_started = time.perf_counter()
        self._report_query_progress(
            progress_callback,
            "codex_deep_investigation",
            "Expanding investigation from Codex gaps.",
            0,
            0,
        )
        before_keys = {
            (item.get("repo"), item.get("path"), item.get("line_start"), item.get("line_end"))
            for item in candidate_matches
        }
        terms_started = time.perf_counter()
        deep_investigation_terms = self._codex_deep_investigation_terms(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            answer_judge=answer_judge,
            codex_validation=codex_validation,
        )
        _log_source_code_qa_timing(
            "codex_deep_investigation_terms",
            elapsed_ms=int((time.perf_counter() - terms_started) * 1000),
            trace_id=trace_id,
            provider=self.llm_provider.name,
            model=selected_model,
            query_mode=query_mode,
            phase="repair",
            term_count=len(deep_investigation_terms),
        )
        matches_started = time.perf_counter()
        expanded_matches = self._codex_deep_investigation_matches(
            entries=entries,
            key=key,
            question=question,
            matches=matches,
            selected_matches=candidate_matches,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            structured_answer=structured_answer,
            answer_judge=answer_judge,
            codex_validation=codex_validation,
            limit=max(int(budget["match_limit"]), self.codex_top_path_limit),
            request_cache=request_cache,
        )
        _log_source_code_qa_timing(
            "codex_deep_investigation_matches",
            elapsed_ms=int((time.perf_counter() - matches_started) * 1000),
            trace_id=trace_id,
            provider=self.llm_provider.name,
            model=selected_model,
            query_mode=query_mode,
            phase="repair",
            expanded_match_count=len(expanded_matches or []),
            original_match_count=len(matches),
            candidate_path_count_before=len(candidate_paths),
        )
        deep_investigation_added = 0
        if expanded_matches:
            rebuild_started = time.perf_counter()
            candidate_matches = self._select_llm_matches(
                expanded_matches,
                self.codex_repair_top_path_limit,
                question=question,
            )
            candidate_paths = self._codex_candidate_paths(entries=entries, key=key, matches=candidate_matches)
            candidate_paths = self._merge_codex_followup_candidate_paths(candidate_paths, followup_context)
            candidate_paths = candidate_paths[: self.codex_repair_top_path_limit]
            candidate_path_layers = self._codex_candidate_path_layers(candidate_paths, followup_context)
            llm_route = {
                **llm_route,
                "candidate_paths": candidate_paths,
                "candidate_path_layers": candidate_path_layers,
                "candidate_repo_count": len({item.get("repo") for item in candidate_paths}),
                "candidate_path_count": len(candidate_paths),
            }
            evidence_summary = self._compress_evidence_cached(question, candidate_matches, request_cache=request_cache)
            trace_paths = self._build_trace_paths(
                entries=entries,
                key=key,
                matches=candidate_matches,
                question=question,
                request_cache=request_cache,
            )
            if trace_paths:
                evidence_summary["trace_paths"] = trace_paths
            quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
            evidence_pack = self._build_evidence_pack(
                question=question,
                evidence_summary=evidence_summary,
                matches=candidate_matches,
                trace_paths=trace_paths,
                quality_gate=quality_gate,
            )
            after_keys = {
                (item.get("repo"), item.get("path"), item.get("line_start"), item.get("line_end"))
                for item in candidate_matches
            }
            deep_investigation_added = len(after_keys - before_keys)
            _log_source_code_qa_timing(
                "codex_deep_investigation_rebuild",
                elapsed_ms=int((time.perf_counter() - rebuild_started) * 1000),
                trace_id=trace_id,
                provider=self.llm_provider.name,
                model=selected_model,
                query_mode=query_mode,
                phase="repair",
                candidate_path_count=len(candidate_paths),
                candidate_repo_count=len({item.get("repo") for item in candidate_paths}),
                deep_investigation_added=deep_investigation_added,
            )
        deep_investigation_rounds = 1
        _log_source_code_qa_timing(
            "codex_deep_investigation",
            elapsed_ms=int((time.perf_counter() - deep_started) * 1000),
            trace_id=trace_id,
            provider=self.llm_provider.name,
            model=selected_model,
            query_mode=query_mode,
            phase="repair",
            candidate_path_count=len(candidate_paths),
            candidate_repo_count=len({item.get("repo") for item in candidate_paths}),
            deep_investigation_added=deep_investigation_added,
            deep_investigation_rounds=deep_investigation_rounds,
            term_count=len(deep_investigation_terms),
        )
        return {
            "candidate_matches": candidate_matches,
            "candidate_paths": candidate_paths,
            "candidate_path_layers": candidate_path_layers,
            "llm_route": llm_route,
            "evidence_summary": evidence_summary,
            "quality_gate": quality_gate,
            "evidence_pack": evidence_pack,
            "deep_investigation_rounds": deep_investigation_rounds,
            "deep_investigation_terms": deep_investigation_terms,
            "deep_investigation_added": deep_investigation_added,
        }

    @staticmethod
    def _match_is_definition_only(match: dict[str, Any], focus_terms: list[str]) -> bool:
        return match_is_definition_only(match, focus_terms)

    @staticmethod
    def _evidence_role(path: str, snippet: str, reason: str) -> str:
        return evidence_role(path, snippet, reason)

    @classmethod
    def _match_answer_grade(cls, match: dict[str, Any], *, intent_label: str = "general") -> bool:
        return match_answer_grade(match, intent_label=intent_label)

    def _codex_initial_candidate_context(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        matches: list[dict[str, Any]],
        selected_matches: list[dict[str, Any]],
        followup_context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        candidate_matches = self._select_llm_matches(
            matches,
            self.codex_top_path_limit,
            question=question,
        )
        if not candidate_matches:
            candidate_matches = selected_matches
        candidate_paths = self._codex_candidate_paths(entries=entries, key=key, matches=candidate_matches)
        candidate_paths = self._merge_codex_followup_candidate_paths(candidate_paths, followup_context)
        candidate_path_layers = self._codex_candidate_path_layers(candidate_paths, followup_context)
        scope_roots = self._codex_scope_roots(entries=entries, key=key)
        question_intent = self._question_intent(question)
        prompt_mode = (
            CODEX_SQL_GENERATION_PROMPT_MODE
            if question_intent.get("sql_generation")
            else CODEX_INVESTIGATION_PROMPT_MODE
        )
        return {
            "candidate_matches": candidate_matches,
            "candidate_paths": candidate_paths,
            "candidate_path_layers": candidate_path_layers,
            "scope_roots": scope_roots,
            "question_intent": question_intent,
            "prompt_mode": prompt_mode,
        }

    @staticmethod
    def _codex_effort_assessment_repair_reasons(
        *,
        answer: str,
        repair_reasons: list[str],
    ) -> list[str]:
        required_section_groups = (
            ("业务理解", "business understanding"),
            ("代码改动", "code change", "技术改造"),
            ("be", "后端", "人天"),
            ("fe", "前端", "人天"),
            ("qa", "integration", "测试", "联调"),
        )
        adjusted_reasons = list(repair_reasons)
        lowered_answer = str(answer or "").lower()
        if not all(any(section in lowered_answer for section in group) for group in required_section_groups):
            adjusted_reasons.append("effort_assessment_missing_required_sections")
        allowed_effort_reasons = {
            "empty_codex_answer",
            "malformed_json_answer",
            "bad_request_answer",
            "out_of_scope_citations",
            "high_risk_claims_missing_scoped_file_evidence",
            "high_risk_answer_judge_requires_repair",
            "not_found_answer_conflicts_with_retrieval_hints",
            "effort_assessment_missing_required_sections",
        }
        if (
            "not_found_answer_conflicts_with_retrieval_hints" in adjusted_reasons
            or "high_risk_claims_missing_scoped_file_evidence" in adjusted_reasons
        ):
            allowed_effort_reasons.add("deep_investigation_needed_for_high_risk_question")
        return [
            reason for reason in adjusted_reasons
            if reason in allowed_effort_reasons or reason.startswith("finish_reason_")
        ]

    def _cached_codex_answer_payload(
        self,
        *,
        cached: dict[str, Any],
        question: str,
        structured_answer: dict[str, Any],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        evidence_pack: dict[str, Any],
        candidate_matches: list[dict[str, Any]],
        candidate_paths: list[dict[str, Any]],
        scope_roots: list[dict[str, str]],
        prompt_mode: str,
        llm_route: dict[str, Any],
        llm_budget_mode: str,
        routed_budget_mode: str,
        cache_key: str,
    ) -> dict[str, Any]:
        answer = str(cached.get("answer") or "")
        claim_check = self._trusted_provider_check()
        answer_judge = self._run_answer_judge(question, answer, evidence_pack, claim_check)
        final = self._finalize_trusted_model_answer(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            evidence_summary=evidence_summary,
            quality_gate=cached.get("answer_quality") or quality_gate,
            claim_check=claim_check,
            answer_judge=answer_judge,
            finish_reason=cached.get("finish_reason") or "cache_hit",
            selected_matches=candidate_matches,
        )
        candidate_repo_count = len({item.get("repo") for item in candidate_paths})
        return {
            "llm_answer": final["answer"],
            "llm_budget_mode": routed_budget_mode,
            "llm_requested_budget_mode": llm_budget_mode,
            "llm_route": {
                **llm_route,
                "cache_hit": True,
                "candidate_paths": candidate_paths,
                "candidate_repo_count": candidate_repo_count,
                "candidate_path_count": len(candidate_paths),
            },
            "llm_provider": cached.get("provider") or self.llm_provider.name,
            "llm_cached": True,
            "llm_usage": self._normalize_llm_usage(cached.get("usage") or {}),
            "llm_model": cached.get("model") or self.codex_model,
            "llm_thinking_budget": 0,
            "llm_attempts": 0,
            "llm_latency_ms": 0,
            "llm_attempt_log": [],
            "llm_finish_reason": cached.get("finish_reason") or "cache_hit",
            "answer_quality": cached.get("answer_quality") or quality_gate,
            "answer_self_check": self._skipped_codex_answer_check(),
            "answer_claim_check": claim_check,
            "answer_judge": answer_judge,
            "structured_answer": final["structured_answer"],
            "answer_contract": final["answer_contract"],
            "evidence_pack": evidence_pack,
            "codex_cli_summary": {
                "cached": True,
                "prompt_mode": prompt_mode,
                "candidate_path_count": len(candidate_paths),
                "candidate_repo_count": candidate_repo_count,
                "scope_repo_count": len(scope_roots),
            },
            "codex_cli_trace": {"cached": True},
            "cache_metadata": self._answer_cache_metadata(cache_key, cached),
            "llm_timing": {},
        }

    def _codex_llm_answer_result_payload(
        self,
        *,
        final: dict[str, Any],
        routed_budget_mode: str,
        llm_budget_mode: str,
        llm_route: dict[str, Any],
        usage: dict[str, Any],
        effective_model: str,
        attempts: int,
        llm_latency_ms: int,
        llm_attempt_log: list[dict[str, Any]],
        finish_reason: str,
        quality_gate: dict[str, Any],
        claim_check: dict[str, Any],
        answer_judge: dict[str, Any],
        evidence_pack: dict[str, Any],
        codex_cli_summary: dict[str, Any],
        codex_cli_trace: dict[str, Any],
        cache_key: str,
        timing: dict[str, int],
    ) -> dict[str, Any]:
        return {
            "llm_answer": final["answer"],
            "llm_budget_mode": routed_budget_mode,
            "llm_requested_budget_mode": llm_budget_mode,
            "llm_route": llm_route,
            "llm_provider": self.llm_provider.name,
            "llm_cached": False,
            "llm_usage": usage,
            "llm_model": effective_model,
            "llm_thinking_budget": 0,
            "llm_attempts": attempts,
            "llm_latency_ms": llm_latency_ms,
            "llm_attempt_log": llm_attempt_log,
            "llm_finish_reason": finish_reason,
            "answer_quality": quality_gate,
            "answer_self_check": self._skipped_codex_answer_check(),
            "answer_claim_check": claim_check,
            "answer_judge": answer_judge,
            "structured_answer": final["structured_answer"],
            "answer_contract": final["answer_contract"],
            "evidence_pack": evidence_pack,
            "codex_cli_summary": codex_cli_summary,
            "codex_cli_trace": codex_cli_trace,
            "cache_metadata": self._answer_cache_metadata(cache_key),
            "llm_timing": timing,
        }

    def _store_codex_answer_cache(
        self,
        *,
        is_followup: bool,
        cache_key: str,
        final: dict[str, Any],
        usage: dict[str, Any],
        quality_gate: dict[str, Any],
        effective_model: str,
        finish_reason: str,
        query_mode: str,
        routed_budget_mode: str,
        trace_id: str,
    ) -> None:
        if is_followup and not self.codex_cache_followups:
            return
        self._store_cached_answer(
            cache_key,
            answer=final["answer"],
            usage=usage,
            answer_quality=quality_gate,
            provider=self.llm_provider.name,
            model=effective_model,
            thinking_budget=0,
            finish_reason=finish_reason,
            query_mode=query_mode,
            llm_budget_mode=routed_budget_mode,
            trace_id=trace_id,
        )

    def _codex_initial_answer_result(
        self,
        *,
        prompt_context: str,
        prompt_mode: str,
        progress_callback: Any | None,
        codex_cli_session_id: str,
        attachments: list[dict[str, Any]],
        trace_id: str,
        initial_prompt_stats: dict[str, int],
        candidate_paths: list[dict[str, Any]],
        candidate_repo_count: int,
        selected_model: str,
        query_mode: str,
        question: str,
        evidence_pack: dict[str, Any],
        timing: dict[str, int],
        scope_roots: list[dict[str, str]],
    ) -> dict[str, Any]:
        payload = self._codex_payload(
            prompt_context,
            prompt_mode=prompt_mode,
            progress_callback=progress_callback,
            codex_cli_session_id=codex_cli_session_id,
            image_paths=self._attachment_image_paths(attachments),
            trace_id=trace_id,
            phase="initial",
            prompt_stats=initial_prompt_stats,
            candidate_path_count=len(candidate_paths),
            candidate_repo_count=candidate_repo_count,
        )
        try:
            result = self.llm_provider.generate(
                payload=payload,
                primary_model=selected_model,
                fallback_model=self._llm_fallback_model(),
            )
        except ToolError as error:
            raise
        answer = self.llm_provider.extract_text(result.payload)
        structured_answer = self._parse_structured_answer(answer)
        usage = self._normalize_llm_usage(result.usage or result.payload.get("usageMetadata") or {})
        effective_model = result.model
        attempts = result.attempts
        llm_latency_ms = int(result.latency_ms or 0)
        llm_attempt_log = [dict(item) for item in result.attempt_log]
        finish_reason = self._llm_finish_reason(result.payload)
        codex_cli_trace = result.payload.get("codex_cli_trace") if isinstance(result.payload.get("codex_cli_trace"), dict) else {}
        codex_validation = self._timed_codex_call(
            timing,
            "citation_validation",
            lambda: self._validate_codex_citations(answer, candidate_paths, candidate_paths, scope_roots=scope_roots),
            trace_id=trace_id,
            selected_model=selected_model,
            query_mode=query_mode,
            phase="initial",
        )
        claim_check = self._merge_codex_validation(self._trusted_provider_check(), codex_validation)
        answer_judge = self._timed_codex_call(
            timing,
            "answer_judge",
            lambda: self._run_answer_judge(question, answer, evidence_pack, claim_check),
            trace_id=trace_id,
            selected_model=selected_model,
            query_mode=query_mode,
            phase="initial",
        )
        return {
            "answer": answer,
            "structured_answer": structured_answer,
            "usage": usage,
            "effective_model": effective_model,
            "attempts": attempts,
            "llm_latency_ms": llm_latency_ms,
            "llm_attempt_log": llm_attempt_log,
            "finish_reason": finish_reason,
            "codex_cli_trace": codex_cli_trace,
            "codex_initial_ms": llm_latency_ms,
            "codex_validation": codex_validation,
            "claim_check": claim_check,
            "answer_judge": answer_judge,
        }

    def _codex_repair_decision(
        self,
        *,
        question: str,
        answer: str,
        structured_answer: dict[str, Any],
        quality_gate: dict[str, Any],
        evidence_pack: dict[str, Any],
        answer_judge: dict[str, Any],
        codex_validation: dict[str, Any],
        finish_reason: str,
        effort_assessment: bool,
        trace_id: str,
        selected_model: str,
        query_mode: str,
    ) -> dict[str, Any]:
        repair_prepare_started = time.perf_counter()
        deep_needed_raw = self._codex_deep_investigation_needed(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            quality_gate=quality_gate,
            answer_judge=answer_judge,
            codex_validation=codex_validation,
        )
        severe_repair_reasons = self._codex_severe_repair_reasons(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            quality_gate=quality_gate,
            evidence_pack=evidence_pack,
            answer_judge=answer_judge,
            codex_validation=codex_validation,
            finish_reason=finish_reason,
        )
        if deep_needed_raw and self._codex_high_risk_question(question):
            severe_repair_reasons.append("deep_investigation_needed_for_high_risk_question")
        if effort_assessment:
            severe_repair_reasons = self._codex_effort_assessment_repair_reasons(
                answer=answer,
                repair_reasons=severe_repair_reasons,
            )
        severe_repair_reasons = list(dict.fromkeys([reason for reason in severe_repair_reasons if reason]))
        repair_issues = severe_repair_reasons
        deep_needed = any(reason == "deep_investigation_needed_for_high_risk_question" for reason in severe_repair_reasons)
        repair_issue_count = len([issue for issue in repair_issues if issue]) + (1 if deep_needed else 0)
        repair_will_run = bool(self.codex_repair_enabled and severe_repair_reasons)
        repair_decision_ms = int((time.perf_counter() - repair_prepare_started) * 1000)
        _log_source_code_qa_timing(
            "codex_repair_prepare",
            elapsed_ms=repair_decision_ms,
            trace_id=trace_id,
            provider=self.llm_provider.name,
            model=selected_model,
            query_mode=query_mode,
            phase="repair_prepare",
            repair_enabled=self.codex_repair_enabled,
            repair_policy="severe_only",
            repair_will_run=repair_will_run,
            repair_reason="; ".join(severe_repair_reasons[:6]),
            repair_issue_count=repair_issue_count,
            validation_issue_count=len([issue for issue in codex_validation.get("issues") or [] if issue]),
            validation_warning_count=len([issue for issue in codex_validation.get("warnings") or [] if issue]),
            judge_issue_count=len([issue for issue in answer_judge.get("issues") or [] if issue]),
            deep_investigation_needed=bool(deep_needed),
        )
        return {
            "severe_repair_reasons": severe_repair_reasons,
            "repair_issues": repair_issues,
            "deep_needed": deep_needed,
            "repair_issue_count": repair_issue_count,
            "repair_will_run": repair_will_run,
            "repair_decision_ms": repair_decision_ms,
        }

    def _codex_deep_investigation_needed(
        self,
        *,
        question: str,
        answer: str,
        structured_answer: dict[str, Any],
        quality_gate: dict[str, Any],
        answer_judge: dict[str, Any],
        codex_validation: dict[str, Any],
    ) -> bool:
        if not self._is_deep_investigation_question(question):
            return False
        if codex_validation.get("status") not in {"ok", "skipped"}:
            return True
        if str(answer_judge.get("status") or "").lower() in {"repair", "warn", "insufficient_evidence"}:
            return True
        if quality_gate.get("status") == "needs_more_trace" or quality_gate.get("confidence") == "low":
            return True
        if structured_answer.get("not_found") or structured_answer.get("missing_evidence"):
            return True
        confidence = str(structured_answer.get("confidence") or "").lower()
        lowered_answer = str(answer or "").lower()
        uncertainty_markers = (
            "cannot confirm",
            "not found",
            "missing evidence",
            "likely",
            "probably",
            "可能",
            "无法确认",
            "没有找到",
            "未找到",
            "更像",
        )
        return confidence in {"", "low", "medium"} and any(marker in lowered_answer for marker in uncertainty_markers)

    def _is_deep_investigation_question(self, question: str) -> bool:
        intent = self._question_intent(question)
        if any(
            intent.get(key)
            for key in (
                "api",
                "config",
                "data_source",
                "error",
                "impact_analysis",
                "message_flow",
                "module_dependency",
                "operational_boundary",
                "rule_logic",
                "test_coverage",
            )
        ):
            return True
        lowered = f" {str(question or '').lower()} "
        return any(
            marker in lowered
            for marker in (
                "why",
                "root cause",
                "caller",
                "callee",
                "call chain",
                "upstream",
                "downstream",
                "report",
                "failed",
                "failure",
                "difference",
                "v2",
                "为什么",
                "什么原因",
                "调用链",
                "链路",
                "上游",
                "下游",
                "失败",
                "报错",
                "区别",
                "是什么意思",
                "配置迁移",
                "数据源",
            )
        )

    def _codex_deep_investigation_terms(
        self,
        *,
        question: str,
        answer: str,
        structured_answer: dict[str, Any],
        answer_judge: dict[str, Any],
        codex_validation: dict[str, Any],
    ) -> list[str]:
        texts = [
            question,
            answer,
            str(structured_answer.get("direct_answer") or ""),
            " ".join(str(item) for item in structured_answer.get("confirmed_from_code") or []),
            " ".join(str(item) for item in structured_answer.get("inferred_from_code") or []),
            " ".join(str(item) for item in structured_answer.get("not_found") or []),
            " ".join(str(item) for item in structured_answer.get("missing_evidence") or []),
            " ".join(str(item) for item in answer_judge.get("issues") or []),
            " ".join(str(item) for item in answer_judge.get("repair_targets") or []),
            " ".join(str(item) for item in codex_validation.get("unsupported_claims") or []),
        ]
        for claim in structured_answer.get("claims") or []:
            if isinstance(claim, dict):
                texts.append(str(claim.get("text") or ""))
        terms: list[str] = []
        for text in texts:
            for token in re.findall(r"[A-Za-z][A-Za-z0-9_./:-]{2,}", str(text or "")):
                cleaned = token.strip("._/-:").lower()
                if len(cleaned) < 4 or cleaned in STOPWORDS or cleaned in LOW_VALUE_CALL_SYMBOLS:
                    continue
                if cleaned in {"answer", "evidence", "missing", "claim", "claims", "source", "sources", "code"}:
                    continue
                if cleaned not in terms:
                    terms.append(cleaned)
        terms.extend(
            [
                "caller",
                "callee",
                "impl",
                "adapter",
                "controller",
                "service",
                "client",
                "mapper",
                "repository",
                "requestmapping",
                "postmapping",
                "configuration",
            ]
        )
        deduped: list[str] = []
        for term in terms:
            if term not in deduped:
                deduped.append(term)
        return deduped[:40]

    def _codex_deep_investigation_matches(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        matches: list[dict[str, Any]],
        selected_matches: list[dict[str, Any]],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        structured_answer: dict[str, Any],
        answer_judge: dict[str, Any],
        codex_validation: dict[str, Any],
        limit: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        terms = self._codex_deep_investigation_terms(
            question=question,
            answer=str(structured_answer.get("direct_answer") or ""),
            structured_answer=structured_answer,
            answer_judge=answer_judge,
            codex_validation=codex_validation,
        )
        if not terms:
            return selected_matches
        agent_plan = {
            "provider": "codex_deep_investigation",
            "steps": [
                {
                    "name": "codex_answer_gap_followup",
                    "purpose": "Use Codex missing evidence and uncertainty markers to trace callers, callees, APIs, configs, and source hops.",
                    "terms": terms,
                    "tools": ["find_references", "find_callers", "find_callees", "find_api_routes", "trace_flow", "search_code"],
                }
            ],
        }
        combined_matches: list[dict[str, Any]] = []
        seen_match_keys: set[tuple[Any, Any, Any, Any]] = set()
        for item in [*selected_matches, *matches]:
            item_key = (item.get("repo"), item.get("path"), item.get("line_start"), item.get("line_end"))
            if item_key in seen_match_keys:
                continue
            combined_matches.append(item)
            seen_match_keys.add(item_key)
        expanded = self._run_agent_plan(
            entries=entries,
            key=key,
            question=question,
            matches=combined_matches,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            agent_plan=agent_plan,
            limit=max(int(limit or 12), 18),
            request_cache=request_cache,
        )
        return expanded or selected_matches

    def _codex_repair_brief(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        initial_answer: str,
        scope_roots: list[dict[str, str]],
        candidate_paths: list[dict[str, Any]],
        runtime_evidence: list[dict[str, Any]],
        repair_issues: list[str],
    ) -> str:
        attachment_section = self._context_attachment_section([], runtime_evidence)
        return build_codex_repair_brief(
            pm_team=pm_team,
            country=country,
            question=question,
            initial_answer=initial_answer,
            scope_roots=scope_roots,
            candidate_paths=candidate_paths,
            attachment_section=attachment_section,
            repair_issues=repair_issues,
        )

    def _codex_sql_generation_brief(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        candidate_paths: list[dict[str, Any]],
        evidence_pack: dict[str, Any],
        quality_gate: dict[str, Any],
        followup_context: dict[str, Any] | None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
        scope_roots: list[dict[str, str]] | None = None,
    ) -> str:
        attachment_section = self._attachment_prompt_section(attachments or [])
        runtime_section = self._runtime_evidence_sql_prompt_section(
            runtime_evidence or [],
            question=question,
            evidence_pack=evidence_pack,
        )
        return build_codex_sql_generation_brief(
            pm_team=pm_team,
            country=country,
            question=question,
            candidate_paths=candidate_paths,
            evidence_pack=evidence_pack,
            quality_gate=quality_gate,
            followup_context=followup_context,
            scope_roots=scope_roots,
            attachment_section=attachment_section,
            runtime_section=runtime_section,
        )

    def _codex_initial_prompt_context(
        self,
        *,
        prompt_mode: str,
        pm_team: str,
        country: str,
        question: str,
        candidate_paths: list[dict[str, Any]],
        evidence_pack: dict[str, Any],
        quality_gate: dict[str, Any],
        followup_context: dict[str, Any] | None,
        attachments: list[dict[str, Any]],
        runtime_evidence: list[dict[str, Any]],
        scope_roots: list[dict[str, str]],
    ) -> str:
        if prompt_mode == CODEX_SQL_GENERATION_PROMPT_MODE:
            return self._codex_sql_generation_brief(
                pm_team=pm_team,
                country=country,
                question=question,
                candidate_paths=candidate_paths,
                evidence_pack=evidence_pack,
                quality_gate=quality_gate,
                followup_context=followup_context,
                attachments=attachments,
                runtime_evidence=runtime_evidence,
                scope_roots=scope_roots,
            )
        return self._codex_investigation_brief(
            pm_team=pm_team,
            country=country,
            question=question,
            candidate_paths=candidate_paths,
            evidence_pack=evidence_pack,
            quality_gate=quality_gate,
            followup_context=followup_context,
            attachments=attachments,
            runtime_evidence=runtime_evidence,
            scope_roots=scope_roots,
        )

    def _codex_cli_session_id(self, followup_context: dict[str, Any] | None) -> str:
        if self.codex_session_mode != CODEX_SESSION_MODE_RESUME or not isinstance(followup_context, dict):
            return ""
        session_meta = followup_context.get("codex_cli_session")
        if not isinstance(session_meta, dict):
            return ""
        return str(session_meta.get("session_id") or "").strip()

    def _log_codex_prompt_timing(
        self,
        *,
        prompt_context: str,
        prompt_stats: dict[str, Any],
        trace_id: str,
        selected_model: str,
        query_mode: str,
        phase: str,
        prompt_mode: str,
        pm_team: str,
        country: str,
        candidate_path_count: int,
        candidate_repo_count: int,
        scope_repo_count: int,
        include_repair_fields: bool = False,
        repair_issue_count: int = 0,
        repair_reason: str = "",
        deep_investigation_added: int = 0,
    ) -> None:
        fields: dict[str, Any] = {
            "phase": phase,
            "prompt_mode": prompt_mode,
            "prompt_sha256": hashlib.sha256(prompt_context.encode("utf-8")).hexdigest()[:16],
            "role_prompt_present": "Source Code & Runtime Evidence Assistant" in prompt_context,
            "pm_team": pm_team,
            "country": country,
            "candidate_path_count": candidate_path_count,
            "candidate_repo_count": candidate_repo_count,
            "scope_repo_count": scope_repo_count,
            "retrieval_role": "hints",
            "repair_policy": "severe_only",
            "prompt_chars": prompt_stats["prompt_chars"],
            "prompt_bytes": prompt_stats["prompt_bytes"],
            "estimated_prompt_tokens": prompt_stats["estimated_prompt_tokens"],
        }
        if include_repair_fields:
            fields["repair_issue_count"] = repair_issue_count
            fields["repair_reason"] = repair_reason
            fields["deep_investigation_added"] = deep_investigation_added
        _log_source_code_qa_timing(
            "codex_prompt",
            elapsed_ms=0,
            trace_id=trace_id,
            provider=self.llm_provider.name,
            model=selected_model,
            query_mode=query_mode,
            **fields,
        )

    def _timed_codex_call(
        self,
        timing: dict[str, int],
        component: str,
        callback: Any,
        *,
        trace_id: str,
        selected_model: str,
        query_mode: str,
        **fields: Any,
    ) -> Any:
        started = time.perf_counter()
        try:
            return callback()
        finally:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            timing[component] = timing.get(component, 0) + elapsed_ms
            _log_source_code_qa_timing(
                component,
                elapsed_ms=elapsed_ms,
                trace_id=trace_id,
                provider=self.llm_provider.name,
                model=selected_model,
                query_mode=query_mode,
                **fields,
            )

    def _build_codex_cli_summary(
        self,
        *,
        llm_route: dict[str, Any],
        codex_validation: dict[str, Any],
        repair_attempted: bool,
        repair_reason: str,
        repair_skipped_reason: str,
        scope_roots: list[dict[str, str]],
        llm_latency_ms: int,
        codex_initial_ms: int,
        timing: dict[str, int],
        llm_attempt_log: list[dict[str, Any]],
        codex_cli_trace: dict[str, Any],
        deep_investigation_rounds: int,
        deep_investigation_added: int,
    ) -> dict[str, Any]:
        return {
            "prompt_mode": llm_route.get("prompt_mode"),
            "candidate_repo_count": llm_route.get("candidate_repo_count"),
            "candidate_path_count": llm_route.get("candidate_path_count"),
            "cited_path_count": codex_validation.get("cited_path_count", 0),
            "citation_validation_status": codex_validation.get("status"),
            "scoped_file_refs": codex_validation.get("scoped_file_refs") or [],
            "out_of_scope_refs": codex_validation.get("out_of_scope_refs") or [],
            "warning_count": codex_validation.get("warning_count", 0),
            "repair_attempted": repair_attempted,
            "repair_policy": "severe_only",
            "repair_reason": repair_reason,
            "repair_skipped_reason": repair_skipped_reason,
            "retrieval_role": "hints",
            "scope_repo_count": len(scope_roots),
            "cli_latency_ms": llm_latency_ms,
            "codex_initial_ms": codex_initial_ms,
            "soft_validation_ms": int(timing.get("citation_validation", 0) + timing.get("answer_judge", 0)),
            "repair_ms": max(0, int(llm_latency_ms or 0) - int(codex_initial_ms or 0)),
            "exit_codes": [item.get("exit_code") for item in llm_attempt_log if item.get("exit_code") is not None],
            "timeout": any(bool(item.get("timeout")) for item in llm_attempt_log),
            "stream_message_count": len(codex_cli_trace.get("stream_messages") or []),
            "command_count": len(codex_cli_trace.get("command_summaries") or []),
            "probable_inspected_file_count": len(codex_cli_trace.get("probable_inspected_files") or []),
            "session_mode": self.codex_session_mode,
            "session_id": codex_cli_trace.get("session_id") or "",
            "deep_investigation_rounds": deep_investigation_rounds,
            "deep_investigation_added": deep_investigation_added,
        }

    @staticmethod
    def _codex_repair_route_fields(
        *,
        codex_validation: dict[str, Any],
        repair_attempted: bool,
        repair_reason: str,
        repair_skipped_reason: str,
        repair_decision_ms: int,
        deep_investigation_rounds: int,
        deep_investigation_terms: list[str],
        deep_investigation_added: int,
    ) -> dict[str, Any]:
        return {
            "codex_citation_validation_status": codex_validation.get("status"),
            "codex_repair_attempted": repair_attempted,
            "codex_repair_policy": "severe_only",
            "codex_repair_reason": repair_reason,
            "codex_repair_skipped_reason": repair_skipped_reason,
            "codex_cited_path_count": codex_validation.get("cited_path_count", 0),
            "codex_scoped_file_ref_count": len(codex_validation.get("scoped_file_refs") or []),
            "codex_out_of_scope_ref_count": len(codex_validation.get("out_of_scope_refs") or []),
            "codex_validation_warning_count": int(codex_validation.get("warning_count") or 0),
            "codex_repair_decision_ms": repair_decision_ms,
            "codex_deep_investigation_rounds": deep_investigation_rounds,
            "codex_deep_investigation_terms": deep_investigation_terms[:12],
            "codex_deep_investigation_added": deep_investigation_added,
        }

    def _codex_initial_route_fields(
        self,
        *,
        selected_model: str,
        prompt_mode: str,
        candidate_paths: list[dict[str, Any]],
        candidate_path_layers: list[dict[str, Any]],
        scope_roots: list[dict[str, str]],
        query_mode: str,
    ) -> dict[str, Any]:
        return {
            "answer_model": selected_model,
            "prompt_mode": prompt_mode,
            "candidate_paths": candidate_paths,
            "candidate_path_layers": candidate_path_layers,
            "candidate_repo_count": len({item.get("repo") for item in candidate_paths}),
            "candidate_path_count": len(candidate_paths),
            "scope_repo_roots": scope_roots,
            "scope_repo_count": len(scope_roots),
            "retrieval_role": "hints",
            "codex_repair_enabled": self.codex_repair_enabled,
            "codex_repair_allowed": bool(self.codex_repair_enabled),
            "codex_repair_policy": "severe_only",
            "codex_repair_skipped_reason": "",
            "query_mode": query_mode,
            "deadline_seconds": 0,
            "codex_session_mode": self.codex_session_mode,
            "codex_session_max_turns": self.codex_session_max_turns,
            "codex_cache_followups": self.codex_cache_followups,
        }

    def _codex_payload(
        self,
        prompt: str,
        *,
        prompt_mode: str = CODEX_INVESTIGATION_PROMPT_MODE,
        progress_callback: Any | None = None,
        codex_cli_session_id: str = "",
        image_paths: list[str] | None = None,
        trace_id: str = "",
        phase: str = "",
        prompt_stats: dict[str, Any] | None = None,
        candidate_path_count: int = 0,
        candidate_repo_count: int = 0,
        repair_issue_count: int = 0,
    ) -> dict[str, Any]:
        stats = prompt_stats if isinstance(prompt_stats, dict) else self._codex_prompt_stats(prompt)
        return build_codex_payload(
            prompt,
            prompt_mode=prompt_mode,
            system_instruction=self._codex_system_instruction(),
            prompt_stats=stats,
            progress_callback=progress_callback,
            codex_cli_session_id=codex_cli_session_id,
            image_paths=image_paths,
            trace_id=trace_id,
            phase=phase,
            candidate_path_count=candidate_path_count,
            candidate_repo_count=candidate_repo_count,
            repair_issue_count=repair_issue_count,
        )

    def _codex_prompt_stats(self, prompt: str) -> dict[str, int]:
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "systemInstruction": {"parts": [{"text": self._codex_system_instruction()}]},
        }
        full_prompt = CodexCliBridgeSourceCodeQALLMProvider._prompt_from_llm_payload(payload)
        return {
            "prompt_chars": len(full_prompt),
            "prompt_bytes": len(full_prompt.encode("utf-8")),
            "estimated_prompt_tokens": self._estimate_llm_tokens(full_prompt),
        }

    @staticmethod
    def _codex_sql_relevance_terms(question: str, evidence_pack: dict[str, Any] | None) -> list[str]:
        terms: list[str] = []
        seen: set[str] = set()

        def add(value: Any) -> None:
            text = str(value or "").strip()
            if not text:
                return
            for token in re.findall(r"[A-Za-z][A-Za-z0-9_]{2,}|[\u4e00-\u9fff]{2,}", text):
                lowered = token.lower()
                if lowered in seen or len(lowered) < 3:
                    continue
                if lowered in {
                    "the",
                    "and",
                    "for",
                    "with",
                    "from",
                    "where",
                    "select",
                    "sql",
                    "query",
                    "code",
                    "data",
                    "dictionary",
                    "based",
                    "help",
                    "please",
                }:
                    continue
                seen.add(lowered)
                terms.append(token)

        add(question)
        pack = evidence_pack if isinstance(evidence_pack, dict) else {}
        for key in ("tables", "read_write_points", "data_sources", "entry_points", "typed_items"):
            for item in pack.get(key) or []:
                if isinstance(item, dict):
                    add(item.get("claim"))
                    add(item.get("source_id"))
                else:
                    add(item)
        return terms[:40]

    @classmethod
    def _compact_sql_runtime_evidence_text(cls, text: str, *, question: str, evidence_pack: dict[str, Any] | None, limit: int) -> str:
        source = str(text or "").strip()
        if not source or len(source) <= limit:
            return source
        terms = [term.lower() for term in cls._codex_sql_relevance_terms(question, evidence_pack)]
        lines = source.splitlines()
        selected: list[str] = []
        for index, line in enumerate(lines):
            lowered = line.lower()
            if terms and any(term in lowered for term in terms):
                start = max(0, index - 1)
                end = min(len(lines), index + 3)
                selected.extend(lines[start:end])
        compact = "\n".join(dict.fromkeys([line for line in selected if str(line).strip()]))
        if compact:
            compact = f"{compact[: max(0, limit - 1200)]}\n...[data dictionary relevance-filtered for SQL prompt]"
            prefix = source[: min(1200, max(0, limit - len(compact) - 80))]
            return f"{prefix}\n...\n{compact}"[:limit]
        return f"{source[:limit]}\n...[runtime evidence text truncated for SQL prompt]"

    @classmethod
    def _runtime_evidence_sql_prompt_section(
        cls,
        runtime_evidence: list[dict[str, Any]],
        *,
        question: str,
        evidence_pack: dict[str, Any] | None,
        text_limit: int = CODEX_SQL_RUNTIME_EVIDENCE_CHAR_LIMIT,
    ) -> str:
        normalized = [item for item in runtime_evidence or [] if isinstance(item, dict)]
        if not normalized:
            return ""
        lines = [
            "Uploaded runtime evidence for SQL generation:",
            "- Treat these files as runtime/reference evidence, not repository source code.",
            "- Data dictionary uploads are shared schema/reference evidence for their pm_team scope.",
            "- For AF and GRC, data_dictionary uploads apply to SG, ID, PH, and All for that PM team; table and field definitions do not vary by country unless explicit DB evidence proves an override.",
            "- For AF and GRC, each selected country runs against that country's separate runtime DB instance. Do not invent a country filter or cross-country union unless code/runtime evidence proves one.",
            "- For GRC reviewer SQL questions, RC and Compliance are business aliases for the same review stage.",
            "- Cross-check table names, joins, filters, and timestamp assumptions against mapper/XML/DAO/repository/source SQL before finalizing SQL.",
        ]
        remaining = max(2000, int(text_limit or CODEX_SQL_RUNTIME_EVIDENCE_CHAR_LIMIT))
        for index, item in enumerate(normalized[:12], start=1):
            meta = cls._public_runtime_evidence_metadata(item)
            lines.append(
                f"- R{index}: pm_team={meta['pm_team']} country={meta['country']} "
                f"source_type={meta['source_type']} filename={meta['filename']} "
                f"kind={meta['kind']} sha256={meta['sha256'][:16]}"
            )
            text = str(item.get("text") or item.get("summary") or "").strip()
            if text and remaining > 0:
                per_item_limit = min(remaining, 9000 if meta["source_type"].lower() == "data_dictionary" else 2500)
                compact_text = cls._compact_sql_runtime_evidence_text(
                    text,
                    question=question,
                    evidence_pack=evidence_pack,
                    limit=per_item_limit,
                )
                remaining -= len(compact_text)
                lines.append(f"  SQL-relevant extracted text/summary:\n{compact_text}")
        return "\n".join(lines)

    @staticmethod
    def _public_attachment_metadata(attachment: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(attachment.get("id") or ""),
            "filename": str(attachment.get("filename") or ""),
            "mime_type": str(attachment.get("mime_type") or ""),
            "kind": str(attachment.get("kind") or ""),
            "size": int(attachment.get("size") or 0),
            "sha256": str(attachment.get("sha256") or ""),
            "summary": str(attachment.get("summary") or "")[:400],
            "text_char_count": int(attachment.get("text_char_count") or 0),
        }

    @classmethod
    def _attachment_prompt_section(cls, attachments: list[dict[str, Any]]) -> str:
        normalized = [item for item in attachments or [] if isinstance(item, dict)]
        if not normalized:
            return ""
        lines = [
            "User attachments:",
            "- Treat attachments as user-provided context, not repository facts.",
            "- Separate source-code evidence, attachment evidence, and missing evidence in the answer.",
            "- Do not cite an attachment as code evidence; only code paths/snippets count as source-code evidence.",
            "- For screenshots/images, first extract visible facts exactly before source investigation: IDs, trace IDs, timestamps, statuses, field names, expected vs actual behavior, and business impact.",
            "- Use extracted screenshot facts as search terms and investigation constraints; do not let them override source-code evidence.",
            "- If a screenshot describes a production incident, final RCA requires DB/log/runtime evidence for the visible IDs. Put that gap in missing_production_evidence when not provided.",
        ]
        for index, item in enumerate(normalized[:5], start=1):
            meta = cls._public_attachment_metadata(item)
            lines.append(
                f"- A{index}: filename={meta['filename']} kind={meta['kind']} "
                f"mime={meta['mime_type']} size={meta['size']} sha256={meta['sha256'][:16]}"
            )
            text = str(item.get("text") or item.get("summary") or "").strip()
            if text:
                if len(text) > 6000:
                    text = f"{text[:6000]}\n...[attachment text truncated]"
                lines.append(f"  Extracted text/summary:\n{text}")
            elif meta["kind"] == "image":
                lines.append("  Image content is attached to the provider when supported; inspect it directly before using it.")
        return "\n".join(lines)

    @staticmethod
    def _public_runtime_evidence_metadata(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(item.get("id") or ""),
            "filename": str(item.get("filename") or ""),
            "mime_type": str(item.get("mime_type") or ""),
            "kind": str(item.get("kind") or ""),
            "source_type": str(item.get("source_type") or ""),
            "pm_team": str(item.get("pm_team") or ""),
            "country": str(item.get("country") or ""),
            "size": int(item.get("size") or 0),
            "sha256": str(item.get("sha256") or ""),
            "created_at": str(item.get("created_at") or ""),
            "uploaded_by": str(item.get("uploaded_by") or ""),
            "summary": str(item.get("summary") or "")[:400],
            "text_char_count": int(item.get("text_char_count") or 0),
        }

    @classmethod
    def _runtime_evidence_prompt_section(cls, runtime_evidence: list[dict[str, Any]]) -> str:
        normalized = [item for item in runtime_evidence or [] if isinstance(item, dict)]
        if not normalized:
            return ""
        lines = [
            "Uploaded runtime evidence:",
            "- Treat these files as user-uploaded runtime/reference evidence, not repository source code.",
            "- Apollo uploads are UAT/non-Live configuration references unless the user explicitly proves otherwise; never use them as confirmed Live/production configuration facts.",
            "- Separate source-code evidence, runtime evidence, attachment evidence, and missing evidence in the answer.",
            "- Runtime evidence can be stale or partial; use its pm_team/country/source_type labels and do not generalize it across countries unless the file proves that.",
            "- Data dictionary uploads are shared schema/reference evidence for their pm_team scope; use them to interpret table names, columns, and business meanings, but do not cite them as source code.",
            "- For AF and GRC, data_dictionary uploads apply to all country selections for that PM team. SG, ID, and PH share the same table and data-field definitions; do not treat table or column names as country-specific unless uploaded DB evidence explicitly proves an override.",
            "- For GRC reviewer SQL questions, RC and Compliance are business aliases for the same review stage. Use this alias to interpret user wording, but do not claim the alias is source-code evidence unless repository evidence proves it.",
            "- For AF and GRC country-scoped SQL questions, first explain the actual SQL/table logic from code and data dictionary evidence, including the chosen table names, key filters, joins, reviewer/status rows, and timestamp assumptions.",
            "- For AF and GRC country-scoped SQL questions, assume the selected country points to that country's separate runtime DB instance. Do not invent a country filter, cross-country union, or shared physical database unless repository/runtime evidence proves it. Treat this as an execution caveat, not the first sentence or main conclusion.",
            "- For SQL-generation questions, prefer table/column names supported by data_dictionary runtime evidence or mapper/XML/SQL source-code evidence, and include the SQL in a fenced ```sql block.",
            "- If runtime evidence conflicts with source code, describe the conflict instead of silently choosing one.",
            "- For Apollo/config archives, first identify the app/env/namespace path that matches the user's component or business flow, for example authentication-center/UAT1/... before anti-fraud-admin/UAT1/... for authentication or AMR/FV flows.",
            "- Treat uploaded config key/value rows as runtime_evidence facts; use repository code only to prove which classes consume those keys and what behavior follows.",
            "- Do not say an Apollo export is missing when it is present in uploaded runtime evidence. If it is absent from the repo, say 'not in source repo, but present/absent in uploaded runtime evidence' explicitly.",
            "- When a question asks for 'current config' and an uploaded Apollo file is in scope, answer from the uploaded config first, then add the caveat that live/current runtime still needs Apollo release history, pod env, or startup logs.",
        ]
        for index, item in enumerate(normalized[:24], start=1):
            meta = cls._public_runtime_evidence_metadata(item)
            lines.append(
                f"- R{index}: pm_team={meta['pm_team']} country={meta['country']} "
                f"source_type={meta['source_type']} filename={meta['filename']} "
                f"kind={meta['kind']} mime={meta['mime_type']} size={meta['size']} "
                f"sha256={meta['sha256'][:16]} uploaded_at={meta['created_at']}"
            )
            text = str(item.get("text") or item.get("summary") or "").strip()
            if text:
                text_limit = 60000 if meta["source_type"].lower() == "data_dictionary" else 6000
                if len(text) > text_limit:
                    text = f"{text[:text_limit]}\n...[runtime evidence text truncated]"
                lines.append(f"  Extracted text/summary:\n{text}")
                if meta["source_type"].lower() == "apollo":
                    lines.append(
                        "  Apollo handling: preserve path context such as app/env/namespace from each ZIP member; "
                        "choose the matching app namespace before using similarly named keys from another app."
                    )
                if meta["source_type"].lower() == "data_dictionary":
                    lines.append(
                        "  Data dictionary handling: use this as table/column/business meaning reference evidence; "
                        "for AF/GRC, apply table and field definitions across SG/ID/PH/All while keeping each country's "
                        "runtime DB instance separate; cross-check generated SQL against code mappers or repository SQL when available."
                    )
        return "\n".join(lines)

    @classmethod
    def _context_attachment_section(cls, attachments: list[dict[str, Any]], runtime_evidence: list[dict[str, Any]]) -> str:
        sections = [
            section
            for section in (
                cls._attachment_prompt_section(attachments),
                cls._runtime_evidence_prompt_section(runtime_evidence),
            )
            if section
        ]
        return "\n\n".join(sections)

    @staticmethod
    def _attachment_image_paths(attachments: list[dict[str, Any]]) -> list[str]:
        paths: list[str] = []
        for item in attachments or []:
            if str(item.get("kind") or "") != "image":
                continue
            path = str(item.get("path") or "").strip()
            if path:
                paths.append(path)
        return paths[:3]

    @staticmethod
    def _codex_system_instruction() -> str:
        return codex_system_instruction()

    def _codex_candidate_paths(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        matches: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        entries_by_name = {entry.display_name: entry for entry in entries}
        fallback_entry = entries[0] if len(entries) == 1 else None
        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for index, match in enumerate(matches, start=1):
            repo = str(match.get("repo") or "").strip()
            path = str(match.get("path") or "").strip()
            if not path:
                continue
            entry = entries_by_name.get(repo) or fallback_entry
            repo_root = self._repo_path(key, entry) if entry is not None else self.repo_root
            row_key = (repo, path)
            if row_key in seen:
                continue
            seen.add(row_key)
            path_status = self._codex_candidate_path_status(repo_root, path)
            resolved_path = str(path_status.get("path") or path)
            rows.append(
                {
                    "id": f"S{len(rows) + 1}",
                    "repo": repo or (entry.display_name if entry else ""),
                    "repo_root": str(repo_root),
                    "repo_relative_root": self._repo_relative_root(repo_root),
                    "path": resolved_path,
                    "original_path": path if resolved_path != path else "",
                    "absolute_path": str((repo_root / resolved_path).resolve()),
                    "file_exists": bool(path_status.get("file_exists")),
                    "path_status": path_status.get("status") or "unknown",
                    "alternative_paths": path_status.get("alternative_paths") or [],
                    "line_start": match.get("line_start"),
                    "line_end": match.get("line_end"),
                    "retrieval": match.get("retrieval") or "file_scan",
                    "trace_stage": match.get("trace_stage") or "direct",
                    "reason": str(match.get("reason") or "")[:500],
                }
            )
        return rows

    def _codex_scope_roots(self, *, entries: list[RepositoryEntry], key: str) -> list[dict[str, str]]:
        roots: list[dict[str, str]] = []
        seen: set[str] = set()
        for entry in entries:
            repo_root = self._repo_path(key, entry).resolve()
            root_text = str(repo_root)
            if root_text in seen:
                continue
            seen.add(root_text)
            roots.append(
                {
                    "repo": entry.display_name,
                    "repo_root": root_text,
                    "repo_relative_root": self._repo_relative_root(repo_root),
                }
            )
        return roots

    def _codex_candidate_path_status(self, repo_root: Path, path: str) -> dict[str, Any]:
        relative_path = str(path or "").strip()
        if not relative_path or relative_path.startswith("/") or ".." in Path(relative_path).parts:
            return {"path": relative_path, "file_exists": False, "status": "invalid"}
        file_path = (repo_root / relative_path).resolve()
        try:
            file_path.relative_to(repo_root.resolve())
        except ValueError:
            return {"path": relative_path, "file_exists": False, "status": "invalid"}
        if file_path.exists() and file_path.is_file():
            return {"path": relative_path, "file_exists": True, "status": "exact"}
        basename = Path(relative_path).name.lower()
        if not basename:
            return {"path": relative_path, "file_exists": False, "status": "missing"}
        candidates: list[str] = []
        index_path = self._index_path(repo_root)
        if index_path.exists():
            try:
                with sqlite3.connect(index_path) as connection:
                    rows = connection.execute(
                        """
                        select path from files
                        where lower_path = ? or lower_path like ?
                        order by length(path), path
                        limit 6
                        """,
                        [basename, f"%/{basename}"],
                    ).fetchall()
                candidates = [str(row[0]) for row in rows if row and row[0]]
            except sqlite3.Error:
                candidates = []
        if len(candidates) == 1:
            return {
                "path": candidates[0],
                "file_exists": True,
                "status": "resolved_by_filename",
                "alternative_paths": candidates,
            }
        return {
            "path": relative_path,
            "file_exists": False,
            "status": "ambiguous_filename" if candidates else "missing",
            "alternative_paths": candidates,
        }

    def _merge_codex_followup_candidate_paths(
        self,
        candidate_paths: list[dict[str, Any]],
        followup_context: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        if not followup_context:
            return candidate_paths
        merged = list(candidate_paths)
        seen = {(str(item.get("repo") or ""), str(item.get("path") or "")) for item in merged}
        prior_items = [
            *((followup_context.get("codex_inspected_paths") or []) if isinstance(followup_context.get("codex_inspected_paths"), list) else []),
            *((followup_context.get("codex_candidate_paths") or []) if isinstance(followup_context.get("codex_candidate_paths"), list) else []),
            *(((followup_context.get("llm_route") or {}).get("candidate_paths") or []) if isinstance(followup_context.get("llm_route"), dict) else []),
        ]
        for turn in followup_context.get("recent_turns") or []:
            if isinstance(turn, dict):
                prior_items.extend(
                    item for item in (turn.get("codex_candidate_paths") or [])
                    if isinstance(item, dict)
                )
        for prior in prior_items:
            if not isinstance(prior, dict) or len(merged) >= self.codex_top_path_limit:
                break
            repo = str(prior.get("repo") or "").strip()
            path = str(prior.get("path") or "").strip()
            root = str(prior.get("repo_root") or "").strip()
            if not path or path.startswith("/") or ".." in Path(path).parts:
                continue
            key = (repo, path)
            if key in seen:
                continue
            if root:
                root_path = Path(root).expanduser().resolve()
                file_path = (root_path / path).resolve()
                try:
                    file_path.relative_to(root_path)
                except ValueError:
                    continue
                if not file_path.exists() or not file_path.is_file():
                    continue
            merged.append(
                {
                    "id": f"S{len(merged) + 1}",
                    "repo": repo,
                    "repo_root": root,
                    "repo_relative_root": self._repo_relative_root(Path(root)) if root else "",
                    "path": path,
                    "absolute_path": str((Path(root) / path).resolve()) if root else str(prior.get("absolute_path") or ""),
                    "line_start": prior.get("line_start"),
                    "line_end": prior.get("line_end"),
                    "retrieval": "previous_codex_context",
                    "trace_stage": "followup_memory",
                    "reason": str(prior.get("reason") or "previous Codex investigation path")[:500],
                }
            )
            seen.add(key)
        return merged

    @staticmethod
    def _is_relationship_followup_question(question: str) -> bool:
        lowered = f" {str(question or '').lower()} "
        phrase_markers = (
            "two fields",
            "relationship",
            "relation",
            "between the two",
            "between them",
            "deeper",
            "continue",
            "cross-field",
            "cross file",
            "cross-file",
            "any relationship",
            "关系",
            "继续分析",
            "深入",
            "这两个字段",
            "两个字段",
            "关联",
            "链路",
        )
        return any(marker in lowered for marker in phrase_markers)

    @staticmethod
    def _followup_low_value_terms() -> set[str]:
        return {
            "there",
            "their",
            "them",
            "this",
            "that",
            "any",
            "between",
            "field",
            "fields",
            "deeper",
            "relationship",
            "relation",
            "understand",
            "continue",
            "about",
            "with",
            "from",
            "what",
            "where",
            "which",
            "java",
            "tsx",
            "jsx",
            "src",
            "main",
            "config",
            "resources",
            "component",
            "components",
            "service",
            "controller",
            "repository",
            "common",
        }

    def _repo_relative_root(self, repo_root: Path) -> str:
        return codex_repo_relative_root(repo_root, self.repo_root)

    def _codex_candidate_path_layers(
        self,
        candidate_paths: list[dict[str, Any]],
        followup_context: dict[str, Any] | None,
    ) -> dict[str, list[dict[str, Any]]]:
        return codex_candidate_path_layers(candidate_paths, followup_context)

    def _codex_investigation_brief(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        candidate_paths: list[dict[str, Any]],
        evidence_pack: dict[str, Any],
        quality_gate: dict[str, Any],
        followup_context: dict[str, Any] | None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
        scope_roots: list[dict[str, str]] | None = None,
        repair_issues: list[str] | None = None,
    ) -> str:
        candidate_path_layers = self._codex_candidate_path_layers(candidate_paths, followup_context)
        pm_team_label = str(pm_team or "selected").strip() or "selected"
        normalized_scope_roots = [
            item for item in (scope_roots or [])
            if isinstance(item, dict) and str(item.get("repo_root") or "").strip()
        ]
        lines = [
            f"Prompt mode: {CODEX_INVESTIGATION_PROMPT_MODE}",
            f"PM Team: {pm_team}",
            f"Country: {country}",
            f"Question: {question}",
            "",
            "Role:",
            f"- You are the {pm_team_label} PM Team's Source Code & Runtime Evidence Assistant.",
            "- You are the central point of truth for developers, QA engineers, and Product Managers who need quick, accurate information about how the system is currently implemented and behaving.",
            "- Answer questions based strictly on the actual codebase and provided runtime evidence such as logs, stack traces, execution outputs, uploaded configs, and traces.",
            "",
            "Objectives:",
            "- Provide accurate and immediate answers about implemented business logic, data flow, and workflows.",
            "- Analyze runtime evidence to confirm behavior or identify anomalies when runtime evidence is provided.",
            "- Reduce reverse-engineering time by translating code logic into clear PM-facing summaries while preserving exact technical references for engineers.",
            "",
            "Required skills:",
            "- Code & log analysis: scan source files and runtime evidence without losing technical context.",
            f"- {pm_team_label} domain mapping: map business terms and shorthand to the correct code components before answering.",
            "- Audience adaptation: explain the what and why clearly for PMs, and cite exact files, classes, functions, variables, SQL, APIs, or log entries for engineers.",
            "",
            "Response format:",
            "- Always begin with a direct answer.",
            "- Follow with concise bulleted technical details.",
            "- End with citations/sources, using exact file paths, line numbers, function names, SQL/API names, or log entries when available.",
            "- For SQL answers, do not begin the direct answer with country database routing, 'no country filter', or similar execution caveats. Start with the table/row selection logic and business meaning, then mention execution scope later only if useful.",
            "- If the answer cannot be found in the provided source code or runtime evidence, reply exactly: \"This information is not present in the provided Source Code or Runtime Evidence. Please verify if the feature is implemented or escalate to the engineering lead.\"",
            "",
            "Execution policy:",
            "- Use only read-only shell/file inspection inside the synced repository workspace.",
            "- Local retrieval has only narrowed the candidate repo/path range; verify by opening files yourself.",
            "- Do not write files, run formatters, install packages, commit, push, deploy, or mutate runtime state.",
            "- Prefer `rg` when available; if it is unavailable, use `grep -R`, `find`, `sed -n`, `nl -ba`, and direct file reads.",
            f"- Codex starts in the synced repos parent directory: {self.repo_root}.",
            "- Strict scope boundary: search only the allowed scope roots listed below for the selected PM team/country.",
            "- Do not search or cite sibling PM teams, countries, or repos outside the allowed scope roots.",
            "- Candidate paths are starting hints from local retrieval, not the answer-quality boundary and not the only files you may inspect.",
            "- Allowed scope `root` values are absolute synced repo roots. Allowed `relative_root` values are relative to the current repos parent.",
            "- Use either `cd relative_root` from the repos parent or use the absolute `root`; do not use only the final directory basename.",
            "- Candidate `path` values are relative to that repo root; inspect files as root/path.",
            "- First confirm the cwd/repo root, then run at least one `rg` or read at least one file inside the allowed scope before answering.",
            "- Use confirmed_previous_paths, current_high_confidence_paths, and current_supporting_paths as hints; use maybe_relevant_paths only to redirect a scoped search.",
            "",
            "Scoped Codex search allowlist:",
        ]
        if normalized_scope_roots:
            for item in normalized_scope_roots:
                lines.append(
                    f"- repo={item.get('repo')} root={item.get('repo_root')} "
                    f"relative_root={item.get('repo_relative_root')}"
                )
        else:
            lines.append("- No explicit allowlist was provided; stay within the selected synced repository workspace.")
        lines.extend([
            "",
            "Three-stage investigation required:",
            "- Stage 1 candidate evidence: identify the most relevant files/configs/SQL/tests/routes from candidate paths and direct searches. Record this in investigation_steps.candidate_evidence.",
            "- Stage 2 gap verification: run targeted searches for expected missing links before answering. Search for full definitions, INSERT rows, rollback scripts, mapper/client/repository/table/API hops, enums, value mappings, and relevant tests when the question implies them. Record this in investigation_steps.gap_verification.",
            "- Stage 3 certainty split: answer by separating confirmed_from_code, inferred_from_code, and not_found/missing_evidence. Do not promote a high-confidence inference into confirmed_from_code.",
            "- For rule/config questions, explicitly distinguish full rule/config definitions from status-only migration updates. If only status updates are found, say the full rule row/expression is missing.",
            "- For Apollo/config questions with uploaded runtime evidence, search the uploaded evidence text/summary by app/env/namespace and key name before treating the config as missing. Record whether the key is present in runtime evidence, source repo code, both, or neither.",
            "- For component-specific config questions, choose the component that owns the runtime behavior. Example: AMR/FV/authentication flows should inspect authentication-center Apollo keys before anti-fraud-admin or anti-fraud-service keys.",
            "- For config-driven behavior, separate two claims: (1) uploaded runtime config key/value, and (2) repository code path that consumes the key. Do not use one evidence tier as proof of the other.",
            "- For data-source questions, explicitly distinguish DTO/carrier fields from upstream source tables/APIs/repos. If the upstream hop is absent, list that hop in missing_evidence.",
            "- For ambiguous business wording, map each possible meaning to concrete code surfaces before answering. Example: distinguish admin query endpoints from report ingestion endpoints, field aliases from true schema fields, and synchronous caller failures from async processing failures.",
            "- If a developer phrase sounds like a business shorthand rather than an exact class/API name, say which source-backed interpretation is confirmed and which interpretation still needs logs, traceId, config export, or the caller repo.",
            "- For screenshot-driven incident questions, first write down visible IDs/statuses/timestamps/expected-vs-actual facts from the screenshot, then use those exact terms to steer source searches.",
            "- Do not call a screenshot-based hypothesis an RCA unless DB/log/runtime evidence for the visible case IDs is present.",
            "- Do not rewrite code, suggest architectural refactoring, or instruct developers how to build new features unless the user explicitly asks for implementation guidance.",
            "- Maintain an objective, professional, analytical tone.",
            "",
            "Starting path hints from local retrieval:",
        ])
        for layer_name, layer_items in candidate_path_layers.items():
            if not layer_items:
                continue
            lines.append(f"- {layer_name}:")
            for item in layer_items[: self.codex_top_path_limit]:
                original_path = str(item.get("original_path") or "").strip()
                path_note = f" original_path={original_path}" if original_path else ""
                alternatives = item.get("alternative_paths") or []
                alternative_note = f" alternatives={alternatives[:5]}" if alternatives else ""
                lines.extend(
                    [
                        (
                            f"  - {item.get('id')} repo={item.get('repo')} root={item.get('repo_root')} "
                            f"relative_root={item.get('repo_relative_root')} "
                            f"path={item.get('path')}{path_note} file_exists={item.get('file_exists')} "
                            f"path_status={item.get('path_status')}{alternative_note} "
                            f"lines={item.get('line_start')}-{item.get('line_end')}"
                        ),
                        f"    retrieval={item.get('retrieval')} trace_stage={item.get('trace_stage')} reason={item.get('reason')}",
                    ]
                )
        attachment_section = self._context_attachment_section(attachments or [], runtime_evidence or [])
        if attachment_section:
            lines.extend(["", attachment_section])
        if followup_context:
            lines.extend(["", "Follow-up context:"])
            for label, key_name in (
                ("Previous question", "question"),
                ("Previous answer", "answer"),
                ("Previous rendered answer", "rendered_answer"),
                ("Previous summary", "summary"),
                ("Previous trace id", "trace_id"),
            ):
                value = str(followup_context.get(key_name) or "").strip()
                if value:
                    lines.append(f"- {label}: {value[:900]}")
            prior_paths = []
            for match in followup_context.get("matches_snapshot") or []:
                if isinstance(match, dict) and match.get("path"):
                    prior_paths.append(f"{match.get('repo')}:{match.get('path')}:{match.get('line_start')}-{match.get('line_end')}")
            if prior_paths:
                lines.append(f"- Prior cited paths: {', '.join(prior_paths[:8])}")
            prior_candidates = followup_context.get("codex_candidate_paths") or (followup_context.get("llm_route") or {}).get("candidate_paths") or []
            if prior_candidates:
                lines.append("- Previous Codex candidate paths:")
                for item in [candidate for candidate in prior_candidates if isinstance(candidate, dict)][:8]:
                    lines.append(
                        f"  - {item.get('repo')} root={item.get('repo_root')} path={item.get('path')} "
                        f"lines={item.get('line_start')}-{item.get('line_end')}"
                    )
            validation = followup_context.get("codex_citation_validation") or {}
            if validation:
                lines.append(
                    f"- Previous citation validation: status={validation.get('status')} "
                    f"cited_path_count={validation.get('cited_path_count', 0)}"
                )
                direct_refs = validation.get("direct_file_refs") or []
                for item in [ref for ref in direct_refs if isinstance(ref, dict)][:6]:
                    lines.append(f"  - verified file ref: {item.get('path')}:{item.get('line_start')}-{item.get('line_end')}")
            codex_summary = followup_context.get("codex_cli_summary") or {}
            if codex_summary:
                lines.append(
                    f"- Previous Codex run: prompt_mode={codex_summary.get('prompt_mode')} "
                    f"paths={codex_summary.get('candidate_path_count')} repair={codex_summary.get('repair_attempted')}"
                )
            inspected_paths = [
                item for item in (followup_context.get("codex_inspected_paths") or [])
                if isinstance(item, dict)
            ][:10]
            if inspected_paths:
                lines.append("- Previous Codex inspected paths:")
                for item in inspected_paths:
                    lines.append(
                        f"  - {item.get('repo')} root={item.get('repo_root')} path={item.get('path')} "
                        f"source={item.get('source') or 'trace'}"
                    )
            prior_pack = followup_context.get("evidence_pack") or {}
            if isinstance(prior_pack, dict):
                summary_bits = []
                for key_name in ("entry_points", "call_chain", "read_write_points", "tables", "apis", "missing_hops"):
                    values = prior_pack.get(key_name) or []
                    if values:
                        summary_bits.append(f"{key_name}={values[:3]}")
                if summary_bits:
                    lines.append(f"- Prior evidence summary: {'; '.join(summary_bits)[:1200]}")
            recent_turns = [
                item for item in (followup_context.get("recent_turns") or [])
                if isinstance(item, dict)
            ][-self.codex_session_max_turns:]
            if recent_turns:
                lines.append("- Earlier session turns:")
                for index, turn in enumerate(recent_turns, start=1):
                    turn_question = str(turn.get("question") or "").strip()
                    turn_answer = str(turn.get("answer") or "").strip()
                    turn_trace_id = str(turn.get("trace_id") or "").strip()
                    lines.append(f"  - Turn {index} question: {turn_question[:500]}")
                    if turn_answer:
                        lines.append(f"    answer: {turn_answer[:700]}")
                    if turn_trace_id:
                        lines.append(f"    trace_id: {turn_trace_id[:80]}")
                    turn_paths = []
                    for match in turn.get("matches_snapshot") or []:
                        if isinstance(match, dict) and match.get("path"):
                            turn_paths.append(f"{match.get('repo')}:{match.get('path')}:{match.get('line_start')}-{match.get('line_end')}")
                    if turn_paths:
                        lines.append(f"    cited_paths: {', '.join(turn_paths[:5])}")
                    turn_candidates = [
                        item for item in (turn.get("codex_candidate_paths") or [])
                        if isinstance(item, dict)
                    ][:5]
                    if turn_candidates:
                        candidate_bits = [f"{item.get('repo')}:{item.get('path')}" for item in turn_candidates]
                        lines.append(f"    candidate_paths: {', '.join(candidate_bits)}")
            lines.append("- Treat this as a follow-up unless the current question clearly redirects scope.")
        if repair_issues:
            lines.extend(["", "Repair required before final answer:"])
            lines.extend(f"- {issue}" for issue in repair_issues[:8])
            lines.append("- Re-open the files as needed and return claims with valid citations.")
        lines.extend(["", "Evidence pack navigation hints only, not an answer-quality gate:"])
        for key_name in ("entry_points", "call_chain", "read_write_points", "tables", "apis", "configs", "source_tiers", "source_conflicts", "missing_hops"):
            values = evidence_pack.get(key_name) or []
            if values:
                lines.append(f"- {key_name}: {values[:6]}")
        lines.append(
            f"- Local narrowing status: {quality_gate.get('status')} "
            f"confidence={quality_gate.get('confidence')} missing={quality_gate.get('missing') or []}"
        )
        lines.extend(
            [
                "",
                "Final answer contract:",
                '- Return JSON: {"direct_answer":"...","investigation_steps":{"candidate_evidence":["..."],"gap_verification":["..."],"certainty_split":["..."]},"attachment_facts":["..."],"screenshot_evidence":["..."],"source_code_evidence":["file/function/field evidence..."],"confirmed_from_code":["..."],"inferred_from_code":["..."],"not_found":["..."],"missing_production_evidence":["..."],"next_checks":["..."],"claims":[{"text":"...","citations":["S1"]}],"missing_evidence":[],"confidence":"high|medium|low"}.',
                "- For SQL direct_answer, lead with the data model and SQL strategy, for example the latest ticket_info rows, authorization_type filters, reviewer node table, RC/Compliance handling, and timestamp approximation. Do not lead with the selected country DB or 'no country filter' caveat.",
                "- Use S ids from candidate paths when they support the claim, or direct file citations like src/Foo.java:10-20 after you verify the file exists.",
                "- source_code_evidence must name concrete files/functions/classes/fields/tables or APIs when available; do not use generic phrases like 'the admin code'.",
                "- Put only file-verified production facts in confirmed_from_code/source_code_evidence; put carrier/call-chain deductions in inferred_from_code; put missing source hops in not_found.",
                "- Put uploaded Apollo/config values in attachment_facts or a runtime-evidence sentence in direct_answer, not in source_code_evidence.",
                "- If an uploaded Apollo/config export contains the requested app/env/namespace, do not write that the export is missing. If only the source repo lacks that export, say exactly that.",
                "- For auth/AMR/FV sampling questions, prefer authentication-center config keys and consumer classes over anti-fraud-admin keys unless the user specifically asks about admin behavior.",
                "- For screenshots, put visible screenshot facts in screenshot_evidence or attachment_facts, not in source_code_evidence.",
                "- Put missing DB rows, trace logs, production config exports, and one-case runtime checks in missing_production_evidence.",
                "- Put concrete verification actions in next_checks, such as querying by trace_id/FID/user_id, comparing component vs aggregate fields, or checking the writer service timestamp.",
                "- Production code, mapper, client, SQL, and config evidence beats tests, docs, specs, and generated files.",
                "- If evidence is missing, state the missing link instead of guessing.",
            ]
        )
        return "\n".join(lines)

    def _merge_codex_validation(self, claim_check: dict[str, Any], validation: dict[str, Any]) -> dict[str, Any]:
        merged = dict(claim_check or {})
        merged["codex_citation_validation"] = validation
        if validation.get("status") in {"ok", "warn"} and str(merged.get("status") or "") == "skipped":
            merged["status"] = "ok"
            merged["reason"] = "codex_citation_validation"
        if validation.get("warnings"):
            warnings = [*(merged.get("warnings") or []), *(validation.get("warnings") or [])]
            merged["warnings"] = list(dict.fromkeys(str(item) for item in warnings if item))
        if validation.get("status") == "needs_citation":
            issues = [*(merged.get("issues") or []), *(validation.get("issues") or [])]
            merged["issues"] = list(dict.fromkeys(str(issue) for issue in issues if issue))
            merged["status"] = "needs_citation"
            unsupported = list(merged.get("unsupported_claims") or [])
            unsupported.extend(validation.get("unsupported_claims") or [])
            merged["unsupported_claims"] = list(dict.fromkeys(unsupported))[:8]
        return merged

    def _validate_codex_citations(
        self,
        answer: str,
        candidate_paths: list[dict[str, Any]],
        selected_matches: list[dict[str, Any]],
        *,
        scope_roots: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        structured = self._parse_structured_answer(answer)
        claims = [
            claim for claim in structured.get("claims") or []
            if isinstance(claim, dict) and str(claim.get("text") or "").strip()
        ]
        valid_s_ids = {str(index) for index in range(1, len(selected_matches) + 1)}
        issues: list[str] = []
        warnings: list[str] = []
        unsupported_claims: list[str] = []
        direct_refs: list[dict[str, Any]] = []
        out_of_scope_refs: list[dict[str, Any]] = []
        invalid_refs: list[str] = []
        checked_claims = 0
        for claim in claims:
            claim_text = str(claim.get("text") or "").strip()
            lowered = claim_text.lower()
            concrete = any(term in lowered for term in ANSWER_CONCRETE_SOURCE_HINTS + API_HINTS + CONFIG_HINTS + RULE_HINTS)
            if not concrete:
                continue
            checked_claims += 1
            s_ids = self._claim_citation_numbers(claim_text, claim)
            direct_candidates = [
                str(item or "").strip()
                for item in claim.get("citations") or []
                if str(item or "").strip() and not re.fullmatch(r"\[?S?\d+\]?", str(item or "").strip(), flags=re.IGNORECASE)
            ]
            direct_candidates.extend(self._extract_direct_file_refs(claim_text))
            if s_ids and not s_ids <= valid_s_ids:
                warnings.append("Codex cited evidence ids outside the retrieval hint list")
            valid_s = s_ids & valid_s_ids
            valid_direct = []
            for raw_ref in direct_candidates:
                resolved = self._resolve_codex_file_ref(raw_ref, candidate_paths, scope_roots=scope_roots)
                if resolved.get("status") == "ok":
                    valid_direct.append(resolved)
                elif resolved.get("status") == "out_of_scope":
                    out_of_scope_refs.append(resolved)
                else:
                    invalid_refs.append(raw_ref)
            direct_refs.extend(valid_direct)
            if not valid_s and not valid_direct:
                unsupported_claims.append(claim_text[:220])
        if invalid_refs:
            warnings.append(f"Codex returned unresolved file references: {', '.join(invalid_refs[:4])}")
        if out_of_scope_refs:
            issues.append("Codex cited files outside the selected PM team/country scope")
        if unsupported_claims:
            warnings.append("Codex concrete claims lack valid retrieval-hint S-id or scoped file:line citations")
        status = "needs_citation" if issues else ("warn" if warnings else "ok")
        return {
            "status": status,
            "checked_claims": checked_claims,
            "issues": list(dict.fromkeys(issues)),
            "warnings": list(dict.fromkeys(warnings)),
            "warning_count": len(list(dict.fromkeys(warnings))),
            "unsupported_claims": unsupported_claims[:6],
            "cited_path_count": len({item.get("absolute_path") for item in direct_refs if item.get("absolute_path")}),
            "direct_file_refs": direct_refs[:12],
            "scoped_file_refs": direct_refs[:12],
            "out_of_scope_refs": out_of_scope_refs[:12],
        }

    @staticmethod
    def _extract_direct_file_refs(text: str) -> list[str]:
        return extract_direct_file_refs(text)

    def _resolve_codex_file_ref(
        self,
        raw_ref: str,
        candidate_paths: list[dict[str, Any]],
        *,
        scope_roots: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        return resolve_codex_file_ref(
            raw_ref,
            candidate_paths,
            repo_root=self.repo_root,
            scope_roots=scope_roots,
        )

    def _codex_high_risk_question(self, question: str) -> bool:
        if self._is_simple_symbol_lookup_question(question):
            return False
        intent = self._question_intent(question)
        if any(
            intent.get(key)
            for key in (
                "data_source",
                "error",
                "impact_analysis",
                "message_flow",
                "module_dependency",
                "operational_boundary",
                "rule_logic",
            )
        ):
            return True
        lowered = str(question or "").lower()
        return any(
            marker in lowered
            for marker in (
                "root cause",
                "why",
                "reject",
                "fail",
                "failure",
                "production",
                "live",
                "为什么",
                "原因",
                "拒绝",
                "失败",
                "报错",
                "线上",
                "生产",
            )
        )

    def _codex_severe_repair_reasons(
        self,
        *,
        question: str,
        answer: str,
        structured_answer: dict[str, Any],
        quality_gate: dict[str, Any],
        evidence_pack: dict[str, Any],
        answer_judge: dict[str, Any],
        codex_validation: dict[str, Any],
        finish_reason: str,
    ) -> list[str]:
        reasons: list[str] = []
        answer_text = str(answer or "").strip()
        if not answer_text:
            reasons.append("empty_codex_answer")
        if self._finish_reason_needs_generation_repair(finish_reason):
            reasons.append(f"finish_reason_{finish_reason or 'unknown'}")
        if structured_answer.get("format") == "prose_fallback" and answer_text.startswith("{"):
            reasons.append("malformed_json_answer")
        if '{"detail":"Bad Request"}' in answer_text or '"Bad Request"' in answer_text:
            reasons.append("bad_request_answer")
        if codex_validation.get("out_of_scope_refs"):
            reasons.append("out_of_scope_citations")
        high_risk = self._codex_high_risk_question(question)
        if high_risk and codex_validation.get("unsupported_claims"):
            reasons.append("high_risk_claims_missing_scoped_file_evidence")
        judge_status = str(answer_judge.get("status") or "").lower()
        if high_risk and judge_status in {"repair", "insufficient_evidence"} and codex_validation.get("status") == "needs_citation":
            reasons.append("high_risk_answer_judge_requires_repair")
        lowered_answer = answer_text.lower()
        not_foundish = any(
            marker in lowered_answer
            for marker in (
                "not present in the provided source code",
                "not present in the provided source code or runtime evidence",
                "not found",
                "not in the repository",
                "未找到",
                "没有找到",
                "不存在",
            )
        )
        evidence_hint_count = 0
        if isinstance(evidence_pack, dict):
            for key_name in ("entry_points", "call_chain", "read_write_points", "tables", "apis", "configs", "items"):
                evidence_hint_count += len(evidence_pack.get(key_name) or [])
        if not_foundish and evidence_hint_count and str(quality_gate.get("status") or "") == "sufficient":
            reasons.append("not_found_answer_conflicts_with_retrieval_hints")
        return list(dict.fromkeys(reason for reason in reasons if reason))

    def _resolve_llm_budget(
        self,
        requested_budget_mode: str,
        question: str,
        matches: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        requested = str(requested_budget_mode or "auto").strip().lower() or "auto"
        if requested in self.llm_budgets:
            return requested, self.llm_budgets[requested], {"mode": "manual", "requested": requested, "reason": "user_selected"}
        intent = self._question_intent(question)
        trace_stages = {str(match.get("trace_stage") or "") for match in matches}
        retrievals = {str(match.get("retrieval") or "") for match in matches}
        lowered_question = str(question or "").lower()
        simple_lookup = self._is_simple_symbol_lookup_question(question)
        deep_reasons: list[str] = []
        if intent.get("data_source"):
            deep_reasons.append("data_source_trace")
        if intent.get("error") or "root cause" in lowered_question or "why" in lowered_question:
            deep_reasons.append("root_cause_or_error")
        if not simple_lookup and any(stage.startswith(("agent_trace", "agent_plan", TOOL_LOOP_TRACE_PREFIX)) for stage in trace_stages):
            deep_reasons.append("agentic_trace_used")
        if not simple_lookup and {"flow_graph", "entity_graph", "code_graph"} & retrievals and len(matches) >= 8:
            deep_reasons.append("graph_evidence_bundle")
        if deep_reasons:
            mode = "deep"
        elif any(intent.get(key) for key in ("api", "config", "rule_logic")) or (
            len(matches) >= 5 and not simple_lookup
        ):
            mode = "balanced"
            deep_reasons.append("moderate_code_reasoning")
        else:
            mode = "cheap"
            deep_reasons.append("simple_lookup")
        return mode, self.llm_budgets[mode], {"mode": "auto", "requested": requested, "selected": mode, "reason": ",".join(deep_reasons)}

    def _is_simple_symbol_lookup_question(self, question: str) -> bool:
        lowered = str(question or "").lower()
        if any(term in lowered for term in ("why", "root cause", "data source", "upstream", "flow", "chain", "call graph", "cross-repo")):
            return False
        if any(intent for intent in (self._question_intent(question).get(key) for key in ("api", "config", "rule_logic", "error"))):
            return False
        return any(term in lowered for term in ("where is", "where are", "which file", "find ", "在哪里", "在哪"))

    @staticmethod
    def _llm_versions() -> dict[str, int]:
        return {
            "prompt": LLM_PROMPT_VERSION,
            "response_schema": LLM_RESPONSE_SCHEMA_VERSION,
            "router": LLM_ROUTER_VERSION,
            "cache": LLM_CACHE_VERSION,
            "runtime": LLM_RUNTIME_VERSION,
            "index": CODE_INDEX_VERSION,
        }

    @staticmethod
    def _trusted_provider_check() -> dict[str, Any]:
        return {
            "status": "skipped",
            "reason": "trusted_provider_passthrough",
            "issues": [],
            "unsupported_claims": [],
        }

    @staticmethod
    def _skipped_codex_answer_check() -> dict[str, Any]:
        return {
            "status": "skipped",
            "reason": "codex_raw_answer_passthrough",
            "issues": [],
            "missing": [],
        }

    def _finalize_trusted_model_answer(
        self,
        *,
        question: str,
        answer: str,
        structured_answer: dict[str, Any],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        claim_check: dict[str, Any],
        selected_matches: list[dict[str, Any]],
        answer_judge: dict[str, Any] | None = None,
        finish_reason: str | None = None,
    ) -> dict[str, Any]:
        intent = evidence_summary.get("intent") or self._question_intent(question)
        missing_links = list(
            dict.fromkeys(
                [
                    *(quality_gate.get("missing") or []),
                    *(structured_answer.get("not_found") or []),
                    *(structured_answer.get("missing_evidence") or []),
                ]
            )
        )
        contract = {
            "intent": intent,
            "status": "model_answer",
            "confirmed_sources": [
                self._append_fact_citation(fact, selected_matches)
                for fact in evidence_summary.get("data_sources") or []
                if self._is_concrete_source_line(str(fact))
            ][:8],
            "data_carriers": [
                self._append_fact_citation(fact, selected_matches)
                for fact in evidence_summary.get("data_carriers") or []
            ][:8],
            "field_population": [
                self._append_fact_citation(fact, selected_matches)
                for fact in evidence_summary.get("field_population") or []
            ][:8],
            "missing_links": missing_links[:8],
            "investigation_steps": structured_answer.get("investigation_steps") or {},
            "confirmed_from_code": list(dict.fromkeys([
                *(structured_answer.get("confirmed_from_code") or []),
                *[
                    self._append_fact_citation(fact, selected_matches)
                    for fact in evidence_summary.get("data_sources") or []
                    if self._is_concrete_source_line(str(fact))
                ],
            ]))[:8],
            "inferred_from_code": list(dict.fromkeys([
                *(structured_answer.get("inferred_from_code") or []),
                *[
                    self._append_fact_citation(fact, selected_matches)
                    for fact in evidence_summary.get("data_carriers") or []
                ][:4],
                *[
                    self._append_fact_citation(fact, selected_matches)
                    for fact in evidence_summary.get("field_population") or []
                ][:4],
            ]))[:8],
            "not_found": list(dict.fromkeys([*(structured_answer.get("not_found") or []), *missing_links]))[:8],
            "confidence": str(structured_answer.get("confidence") or quality_gate.get("confidence") or "medium").lower(),
            "claim_check": claim_check,
            "answer_judge": answer_judge or {},
            "finish_reason": finish_reason,
            "policies": quality_gate.get("policies") or [],
            "source_tiers": evidence_summary.get("source_tiers") or [],
            "source_conflicts": evidence_summary.get("source_conflicts") or [],
            "passthrough": True,
        }
        final_answer = str(answer or "").strip()
        return {
            "answer": final_answer,
            "structured_answer": structured_answer,
            "answer_contract": contract,
        }

    @staticmethod
    def _append_fact_citation(fact: str, selected_matches: list[dict[str, Any]]) -> str:
        text = str(fact or "").strip()
        if not text:
            return ""
        if re.search(r"\[S\d+\]", text):
            return text
        for index, match in enumerate(selected_matches, start=1):
            label = SourceCodeQAService._evidence_label(match)
            if label and label in text:
                return f"{text} [S{index}]"
        return text

    @staticmethod
    def _split_answer_claims(answer: str) -> list[str]:
        claims: list[str] = []
        for raw_line in str(answer or "").splitlines():
            line = raw_line.strip(" -\t")
            if not line:
                continue
            parts = re.split(r"(?<=[.!?])\s+", line)
            claims.extend(part.strip() for part in parts if part.strip())
        return claims[:16]

    def _parse_structured_answer(self, answer: str) -> dict[str, Any]:
        text = str(answer or "").strip()
        payload: dict[str, Any] | None = None
        candidates = [text]
        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.IGNORECASE | re.DOTALL)
        if fenced:
            candidates.insert(0, fenced.group(1))
        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                payload = parsed
                break
        if payload is None:
            claims = [
                {"text": claim, "citations": [f"S{number}" for number in re.findall(r"\[S(\d+)\]", claim)]}
                for claim in self._split_answer_claims(text)
            ]
            return {
                "direct_answer": text,
                "investigation_steps": {},
                "attachment_facts": [],
                "screenshot_evidence": [],
                "source_code_evidence": [],
                "confirmed_from_code": [],
                "inferred_from_code": [],
                "not_found": [],
                "missing_production_evidence": [],
                "next_checks": [],
                "claims": claims[:8],
                "missing_evidence": [],
                "confidence": "medium" if claims else "low",
                "format": "prose_fallback",
            }
        claims = payload.get("claims") if isinstance(payload.get("claims"), list) else []
        normalized_claims = []
        for claim in claims[:12]:
            if isinstance(claim, dict):
                text_value = str(claim.get("text") or "").strip()
                citations = [str(item).strip().lstrip("[S").rstrip("]") for item in claim.get("citations") or []]
                normalized_claims.append({"text": text_value, "citations": [f"S{item}" if item.isdigit() else item for item in citations if item]})
            else:
                normalized_claims.append({"text": str(claim).strip(), "citations": []})
        raw_investigation_steps = payload.get("investigation_steps") if isinstance(payload.get("investigation_steps"), dict) else {}
        investigation_steps = {
            key: [str(item) for item in raw_investigation_steps.get(key) or []]
            for key in ("candidate_evidence", "gap_verification", "certainty_split")
            if isinstance(raw_investigation_steps.get(key), list)
        }
        def list_field(name: str) -> list[str]:
            raw = payload.get(name)
            return [str(item) for item in raw if str(item).strip()] if isinstance(raw, list) else []
        return {
            "direct_answer": str(payload.get("direct_answer") or payload.get("answer") or text).strip(),
            "investigation_steps": investigation_steps,
            "attachment_facts": list_field("attachment_facts"),
            "screenshot_evidence": list_field("screenshot_evidence"),
            "source_code_evidence": list_field("source_code_evidence"),
            "confirmed_from_code": list_field("confirmed_from_code"),
            "inferred_from_code": list_field("inferred_from_code"),
            "not_found": list_field("not_found"),
            "missing_production_evidence": list_field("missing_production_evidence"),
            "next_checks": list_field("next_checks"),
            "claims": normalized_claims,
            "missing_evidence": list_field("missing_evidence"),
            "confidence": str(payload.get("confidence") or "medium").strip().lower(),
            "format": "json",
        }

    @staticmethod
    def _claim_citation_numbers(claim: str, parsed_claim: dict[str, Any]) -> set[str]:
        numbers = {token for token in re.findall(r"\[S(\d+)\]", str(claim or ""))}
        for raw_item in parsed_claim.get("citations") or []:
            item = str(raw_item or "").strip()
            match = re.fullmatch(r"\[?S?(\d+)\]?", item, flags=re.IGNORECASE)
            if match:
                numbers.add(match.group(1))
        return numbers

    def _judge_answer(
        self,
        question: str,
        answer: str,
        evidence_pack: dict[str, Any],
        claim_check: dict[str, Any],
    ) -> dict[str, Any]:
        return self._quality_judge.judge_answer(question, answer, evidence_pack, claim_check)

    def _judge_answer_impl(
        self,
        question: str,
        answer: str,
        evidence_pack: dict[str, Any],
        claim_check: dict[str, Any],
    ) -> dict[str, Any]:
        intent = evidence_pack.get("intent") or self._question_intent(question)
        answer_text = str(answer or "").strip()
        lowered_answer = answer_text.lower()
        issues: list[str] = []
        repair_targets: list[str] = []
        typed_items = [item for item in evidence_pack.get("items") or [] if isinstance(item, dict)]
        supported_items = [item for item in typed_items if item.get("supports_answer") is not False]
        missing_hops = evidence_pack.get("missing_hops") or []
        repairable_intent = any(intent.get(key) for key in ("data_source", "api", "config", "rule_logic", "error", "test_coverage", "operational_boundary"))

        if claim_check.get("status") != "ok":
            issues.extend(str(issue) for issue in claim_check.get("issues") or [])
            if repairable_intent:
                repair_targets.append("add citation-backed claims or remove unsupported concrete claims")
        if intent.get("data_source"):
            has_source_item = any(item.get("type") in {"table", "api", "external_dependency", "read_write"} for item in supported_items)
            if has_source_item and not any(
                str(item.get("claim") or "").split(" ")[0].lower() in lowered_answer
                for item in supported_items
                if item.get("type") in {"table", "api", "external_dependency"}
            ):
                issues.append("answer omits typed table/API/client source evidence")
                repair_targets.append("include the confirmed table/API/client source")
            if missing_hops and any(phrase in lowered_answer for phrase in ("likely", "suggest", "appears", "probably")):
                issues.append("answer speculates despite missing typed evidence")
                repair_targets.append("state missing evidence explicitly instead of speculating")
        if intent.get("api") and evidence_pack.get("apis") and not any(
            token.lower() in lowered_answer
            for token in re.findall(r"[A-Za-z0-9_./:-]{4,}", " ".join(evidence_pack.get("apis") or []))[:12]
        ):
            issues.append("answer omits API route evidence")
            repair_targets.append("include the route or API client evidence")
        if intent.get("static_qa") and evidence_pack.get("static_findings") and not any(
            token.lower() in lowered_answer
            for token in re.findall(r"[A-Za-z0-9_./:-]{4,}", " ".join(evidence_pack.get("static_findings") or []))[:16]
        ):
            issues.append("answer omits static QA finding evidence")
            repair_targets.append("include the highest-severity static QA findings")
        if intent.get("impact_analysis") and evidence_pack.get("impact_surfaces") and not any(
            token.lower() in lowered_answer
            for token in re.findall(r"[A-Za-z0-9_./:-]{4,}", " ".join(evidence_pack.get("impact_surfaces") or []))[:16]
        ):
            issues.append("answer omits impact surface evidence")
            repair_targets.append("include upstream callers and downstream dependencies from the impact evidence")
        if intent.get("test_coverage") and evidence_pack.get("test_coverage") and not any(
            token.lower() in lowered_answer
            for token in re.findall(r"[A-Za-z0-9_./:-]{4,}", " ".join(evidence_pack.get("test_coverage") or []))[:16]
        ):
            issues.append("answer omits test coverage evidence")
            repair_targets.append("include tests, assertions, mocks, or verification evidence")
        if intent.get("operational_boundary") and evidence_pack.get("operational_boundaries") and not any(
            token.lower() in lowered_answer
            for token in re.findall(r"[A-Za-z0-9_./:-]{4,}", " ".join(evidence_pack.get("operational_boundaries") or []))[:16]
        ):
            issues.append("answer omits operational boundary evidence")
            repair_targets.append("include transaction/cache/async/retry/circuit/security boundary evidence")

        status = "repair" if issues and (repair_targets or repairable_intent) else "warn" if issues else "ok"
        return {
            "status": status,
            "mode": "deterministic_evidence_judge",
            "checked_items": len(typed_items),
            "supporting_items": len(supported_items),
            "issues": list(dict.fromkeys(issues))[:6],
            "repair_targets": list(dict.fromkeys(repair_targets))[:6],
        }

    def _run_answer_judge(
        self,
        question: str,
        answer: str,
        evidence_pack: dict[str, Any],
        claim_check: dict[str, Any],
    ) -> dict[str, Any]:
        return self._quality_judge.run_answer_judge(question, answer, evidence_pack, claim_check)

    def _expand_answer_retry_matches(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        matches: list[dict[str, Any]],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        limit: int,
        draft_answer: str = "",
        answer_check: dict[str, Any] | None = None,
        claim_check: dict[str, Any] | None = None,
        answer_judge: dict[str, Any] | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        del draft_answer, answer_check, claim_check, answer_judge
        agent_plan = self._build_agent_plan(question, evidence_summary, quality_gate)
        if not agent_plan.get("steps"):
            agent_plan = {
                "steps": [
                    {
                        "name": "answer_retry_deeper_trace",
                        "purpose": "Gather extra concrete implementation details for a stronger final answer.",
                        "terms": self._quality_gate_trace_terms(question, evidence_summary, quality_gate, matches),
                    }
                ]
            }
        return self._run_agent_plan(
            entries=entries,
            key=key,
            question=question,
            matches=matches,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            agent_plan=agent_plan,
            limit=max(int(limit or 12), 12),
            request_cache=request_cache,
        )

    @staticmethod
    def _llm_match_buckets(matches: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        return {
            "exact_lookup": [match for match in matches if match.get("trace_stage") == "exact_lookup"],
            "direct": [match for match in matches if match.get("trace_stage") == "direct"],
            "query_decomposition": [match for match in matches if match.get("trace_stage") == "query_decomposition"],
            "dependency": [match for match in matches if match.get("trace_stage") == "dependency"],
            "two_hop": [match for match in matches if match.get("trace_stage") == "two_hop"],
            "tool_loop": [match for match in matches if str(match.get("trace_stage") or "").startswith(TOOL_LOOP_TRACE_PREFIX)],
            "impact_analysis": [match for match in matches if match.get("trace_stage") == "impact_analysis"],
            "test_coverage": [match for match in matches if match.get("trace_stage") == "test_coverage" or match.get("retrieval") == "test_coverage"],
            "operational_boundary": [match for match in matches if match.get("trace_stage") == "operational_boundary" or match.get("retrieval") == "operational_boundary"],
            "agent_trace": [match for match in matches if str(match.get("trace_stage") or "").startswith("agent_trace")],
            "agent_plan": [match for match in matches if str(match.get("trace_stage") or "").startswith("agent_plan")],
            "quality_gate": [match for match in matches if match.get("trace_stage") == QUALITY_GATE_TRACE_STAGE],
        }

    @staticmethod
    def _llm_match_stage_order(intent: dict[str, Any]) -> tuple[str, ...]:
        if intent.get("impact_analysis"):
            return ("exact_lookup", "direct", "impact_analysis", "tool_loop", "query_decomposition", "dependency", "two_hop", "agent_trace", "agent_plan", "quality_gate")
        if intent.get("test_coverage"):
            return ("exact_lookup", "direct", "test_coverage", "query_decomposition", "tool_loop", "dependency", "two_hop", "agent_trace", "agent_plan", "quality_gate")
        if intent.get("operational_boundary"):
            return ("exact_lookup", "direct", "operational_boundary", "query_decomposition", "tool_loop", "dependency", "two_hop", "agent_trace", "agent_plan", "quality_gate")
        return ("exact_lookup", "direct", "query_decomposition", "dependency", "two_hop", "tool_loop", "agent_trace", "agent_plan", "quality_gate")

    def _select_llm_matches(self, matches: list[dict[str, Any]], limit: int, *, question: str = "") -> list[dict[str, Any]]:
        if not matches:
            return []
        limit = max(1, int(limit or 1))
        intent = self._question_intent(question) if question else {}
        buckets = self._llm_match_buckets(matches)
        selected: list[dict[str, Any]] = []
        seen: set[tuple[Any, Any, Any, Any]] = set()

        def add(match: dict[str, Any] | None) -> None:
            if not match or len(selected) >= limit:
                return
            key = (match.get("repo"), match.get("path"), match.get("line_start"), match.get("line_end"))
            if key in seen:
                return
            selected.append(match)
            seen.add(key)

        # Keep a balanced evidence bundle: entry point, purpose-specific logic,
        # and downstream/common builders. This improves answer accuracy more than
        # sending only the highest raw scores.
        stage_order = self._llm_match_stage_order(intent)
        if intent.get("data_source"):
            for match in buckets["exact_lookup"]:
                add(match)
            for match in matches:
                if self._match_answer_grade(match, intent_label="data_source"):
                    add(match)
                if len(selected) >= min(limit, 6):
                    break
        for stage in stage_order:
            stage_take = 1 if intent.get("data_source") else 2
            for match in buckets[stage][:stage_take]:
                add(match)
        if intent.get("data_source"):
            purpose_buckets = (
                (
                    "concrete_source",
                    lambda item: self._match_has_concrete_source_evidence(item),
                ),
                (
                    "field_population",
                    lambda item: self._match_has_field_population_evidence(item),
                ),
                (
                    "carrier",
                    lambda item: any(
                        symbol.lower().endswith(DATA_CARRIER_SUFFIXES)
                        for symbol in IDENTIFIER_PATTERN.findall(str(item.get("snippet") or ""))
                    ),
                ),
            )
            for _name, predicate in purpose_buckets:
                for match in matches:
                    if predicate(match):
                        add(match)
                        break
        if intent.get("static_qa"):
            for match in matches:
                if match.get("retrieval") == "static_qa" or match.get("static_qa"):
                    add(match)
                if len(selected) >= limit:
                    break
        if intent.get("impact_analysis"):
            for match in matches:
                if str(match.get("retrieval") or "") in {"planner_caller", "planner_callee", "flow_graph", "entity_graph", "code_graph"}:
                    add(match)
                if len(selected) >= limit:
                    break
        if intent.get("test_coverage"):
            for match in matches:
                if str(match.get("retrieval") or "") == "test_coverage" or match.get("test_coverage"):
                    add(match)
                if len(selected) >= limit:
                    break
        if intent.get("operational_boundary"):
            for match in matches:
                if str(match.get("retrieval") or "") == "operational_boundary" or match.get("operational_boundary"):
                    add(match)
                if len(selected) >= limit:
                    break
        for stage in stage_order:
            for match in buckets[stage][:2]:
                add(match)
        for match in matches:
            add(match)
            if len(selected) >= limit:
                break
        return selected

    @staticmethod
    def _match_has_concrete_source_evidence(match: dict[str, Any]) -> bool:
        path = str(match.get("path") or "").lower()
        snippet = str(match.get("snippet") or "")
        if any(term in path for term in ("repository", "mapper", "dao", "client", "integration", "gateway")):
            return True
        return any(SourceCodeQAService._is_concrete_source_line(line) for line in snippet.splitlines())

    @staticmethod
    def _match_has_field_population_evidence(match: dict[str, Any]) -> bool:
        snippet = str(match.get("snippet") or "")
        return bool(SETTER_CALL_PATTERN.search(snippet) or ASSIGNMENT_PATTERN.search(snippet) or any(term in snippet.lower() for term in FIELD_POPULATION_HINTS))

    @staticmethod
    def _empty_query_payload(
        key: str,
        *,
        status: str,
        summary: str,
        repo_status: list[dict[str, Any]] | None = None,
        index_freshness: dict[str, Any] | None = None,
        trace_id: str = "",
    ) -> dict[str, Any]:
        return {
            "status": status,
            "answer_mode": ANSWER_MODE_AUTO,
            "summary": summary,
            "matches": [],
            "repo_status": repo_status or [],
            "index_freshness": index_freshness or {},
            "key": key,
            "trace_id": trace_id,
        }

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()


attach_indexing_helpers(SourceCodeQAService, globals())
attach_structure_helpers(SourceCodeQAService, globals())
attach_retrieval_tool_helpers(SourceCodeQAService, globals())
attach_retrieval_cache_helpers(SourceCodeQAService)
attach_cache_telemetry_helpers(SourceCodeQAService)
