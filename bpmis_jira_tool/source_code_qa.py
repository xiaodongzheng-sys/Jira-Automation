from __future__ import annotations

import ast
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import time
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests

from bpmis_jira_tool.errors import ToolError


ALL_COUNTRY = "All"
CRMS_COUNTRIES = ("SG", "ID", "PH")
ANSWER_MODE_AUTO = "auto"
ANSWER_MODE = "retrieval_only"
ANSWER_MODE_GEMINI = "gemini_flash"
CONFIG_VERSION = 1
CODE_INDEX_VERSION = 7
GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_DOMAIN_PROFILE_PATH = Path(__file__).resolve().parent.parent / "config" / "source_code_qa_domain_profiles.json"
DEFAULT_LLM_BUDGETS = {
    "cheap": {
        "match_limit": 4,
        "snippet_line_budget": 40,
        "snippet_char_budget": 5_000,
        "thinking_budget": 0,
        "max_output_tokens": 500,
        "model": "gemini-2.5-flash-lite",
    },
    "balanced": {
        "match_limit": 6,
        "snippet_line_budget": 70,
        "snippet_char_budget": 8_000,
        "thinking_budget": 0,
        "max_output_tokens": 700,
        "model": "gemini-2.5-flash",
    },
    "deep": {
        "match_limit": 12,
        "snippet_line_budget": 160,
        "snippet_char_budget": 24_000,
        "thinking_budget": 768,
        "max_output_tokens": 1_400,
        "model": "gemini-2.5-flash",
    },
}
LLM_BUDGETS = DEFAULT_LLM_BUDGETS
ANSWER_SELF_CHECK_WEAK_PHRASES = (
    "does not specify",
    "does not explicitly",
    "insufficient",
    "not enough evidence",
    "cannot determine",
    "not provided",
    "further inspection",
    "best next question",
    "suggesting that",
    "suggests that",
    "appears to",
    "likely",
)

SKIP_DIRS = {
    ".git",
    ".gradle",
    ".idea",
    ".mvn",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "build",
    "coverage",
    "dist",
    "node_modules",
    "target",
    "vendor",
}
TEXT_SUFFIXES = {
    ".c",
    ".cc",
    ".conf",
    ".cpp",
    ".cs",
    ".css",
    ".go",
    ".gradle",
    ".groovy",
    ".h",
    ".html",
    ".java",
    ".js",
    ".json",
    ".jsx",
    ".kt",
    ".kts",
    ".md",
    ".php",
    ".properties",
    ".py",
    ".rb",
    ".rs",
    ".scala",
    ".scss",
    ".sh",
    ".sql",
    ".swift",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".vue",
    ".xml",
    ".yaml",
    ".yml",
}
STOPWORDS = {
    "about",
    "after",
    "and",
    "are",
    "can",
    "code",
    "does",
    "for",
    "from",
    "how",
    "into",
    "is",
    "of",
    "on",
    "or",
    "the",
    "this",
    "to",
    "what",
    "when",
    "where",
    "why",
    "with",
}
HTTPS_URL_PATTERN = re.compile(r"^https://[^/\s]+/.+\.git$")
IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{2,}")
CLASS_DEF_PATTERN = re.compile(r"\b(class|interface|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\b")
PY_DEF_PATTERN = re.compile(r"^\s*(class|def)\s+([A-Za-z_][A-Za-z0-9_]*)\b")
JS_DEF_PATTERN = re.compile(r"^\s*(?:export\s+)?(?:async\s+)?function\s+([A-Za-z_][A-Za-z0-9_]*)\b|^\s*(?:export\s+)?(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:async\s*)?\(")
JAVA_METHOD_DEF_PATTERN = re.compile(
    r"^\s*(?:public|private|protected)?\s*(?:static\s+)?(?:final\s+)?"
    r"[A-Za-z_][A-Za-z0-9_<>, ?\[\]]+\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
SETTER_CALL_PATTERN = re.compile(r"\.set([A-Z][A-Za-z0-9_]*)\s*\(([^)]{1,240})\)")
BUILDER_FIELD_PATTERN = re.compile(r"\.([a-z][A-Za-z0-9_]*)\s*\(([^)]{1,240})\)")
ASSIGNMENT_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*([^;]{2,240})")
ANNOTATION_ROUTE_PATTERN = re.compile(r"@(RequestMapping|GetMapping|PostMapping|PutMapping|DeleteMapping|PatchMapping)\s*(?:\(([^)]*)\))?")
FEIGN_CLIENT_PATTERN = re.compile(r"@FeignClient\s*\(([^)]*)\)")
MYBATIS_NAMESPACE_PATTERN = re.compile(r"<mapper\b[^>]*\bnamespace\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
MYBATIS_STATEMENT_PATTERN = re.compile(r"<(select|insert|update|delete)\b[^>]*\bid\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
HTTP_LITERAL_PATTERN = re.compile(r"[\"'](https?://[^\"']+|/[A-Za-z0-9_./{}:-]{2,})[\"']")
SQL_TABLE_PATTERN = re.compile(r"\b(?:from|join|update|into)\s+([A-Za-z_][A-Za-z0-9_.$]*)", re.IGNORECASE)
PROPERTIES_KEY_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.-]{3,})\s*[:=]")
FTS_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_./:-]{2,}")
DECLARATION_HINT_PATTERN = re.compile(
    r"^\s*(class|def|function|func|interface|type|enum|const|let|var|public|private|protected|static|final)\b",
    re.IGNORECASE,
)
PATHISH_PATTERN = re.compile(r"/[A-Za-z0-9_./:-]{3,}")
CALL_SYMBOL_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
CLASS_CONSTRUCTION_PATTERN = re.compile(r"\bnew\s+([A-Za-z_][A-Za-z0-9_]*)\b")
FIELD_OR_PARAM_TYPE_PATTERN = re.compile(
    r"\b(?:private|protected|public|final|static|@Autowired|@Resource)?\s*"
    r"([A-Z][A-Za-z0-9_]*(?:Service|Client|Integration|Repository|Dao|DAO|Mapper|Adapter|Gateway|Provider|Facade))\b"
)
DEPENDENCY_QUESTION_TERMS = {
    "data", "source", "sources", "integration", "integrations", "upstream", "dependency",
    "dependencies", "call", "chain", "table", "tables", "api", "apis", "client", "clients",
    "service", "services", "fetch", "screening", "provider",
}
DEPENDENCY_PATH_HINTS = (
    "service", "client", "integration", "repository", "dao", "mapper", "adapter", "gateway", "provider", "strategy", "processor",
)
DEPENDENCY_SYMBOL_SUFFIXES = (
    "service", "client", "integration", "repository", "dao", "mapper", "adapter", "gateway", "provider", "facade", "strategy", "processor",
)
LOW_VALUE_CALL_SYMBOLS = {
    "add", "append", "build", "builder", "collect", "contains", "equals", "filter",
    "foreach", "get", "hashcode", "isempty", "list", "log", "map", "of", "orelse",
    "println", "put", "remove", "set", "size", "stream", "string", "tostring",
    "trim", "valueof",
}
LOW_VALUE_FOCUS_TERMS = {
    "term", "loan", "precheck", "pre", "check", "data", "source", "sources",
}
DATA_SOURCE_HINTS = (
    "datasource", "data source", "source", "sources", "upstream", "table", "jdbc",
    "queryfor", "select", " from ", "repository", "mapper", "dao", "client",
    "integration", "provider", "gateway", "api", "userinfo", "customerinfo",
)
CONCRETE_SOURCE_HINTS = (
    "repository", "mapper", "dao", "jdbc", "queryfor", "select", " from ",
    "table", "client", "integration", "gateway", "api", "http", "endpoint",
    "feign", "resttemplate", "webclient",
)
ANSWER_CONCRETE_SOURCE_HINTS = (
    "repository", "mapper", "dao", "jdbc", "queryfor", "select", "table",
    "client", "integration", "gateway", "api", "http", "endpoint", "feign",
    "resttemplate", "webclient",
)
API_HINTS = ("api", "endpoint", "route", "controller", "requestmapping", "getmapping", "postmapping", "http", "url", "path")
CONFIG_HINTS = ("config", "configuration", "property", "properties", "yaml", "yml", "env", "setting", "feature", "flag")
ERROR_HINTS = ("error", "exception", "failed", "failure", "stacktrace", "status", "code", "timeout")
RULE_HINTS = ("rule", "condition", "logic", "validate", "validation", "permission", "access", "approval", "eligible")
FIELD_POPULATION_HINTS = (
    "set", "get", "build", "populate", "provider", "factory", "converter",
    "assembler", "initiation", "underwritingbasicinfo", "customerinfo",
    "loaninfo", "creditriskinfo", "underwritinginitiationdto",
)
DATA_CARRIER_SUFFIXES = (
    "dto", "input", "context", "record", "result", "request", "response", "body",
    "do", "entity", "model", "profile", "info", "wrap",
)
QUALITY_GATE_TRACE_STAGE = "quality_gate"
TOOL_LOOP_TRACE_PREFIX = "tool_loop_"


@dataclass(frozen=True)
class RepositoryEntry:
    display_name: str
    url: str


class SourceCodeQAService:
    def __init__(
        self,
        *,
        data_root: Path,
        team_profiles: dict[str, dict[str, Any]],
        gitlab_token: str | None = None,
        gitlab_username: str = "oauth2",
        gemini_api_key: str | None = None,
        gemini_model: str = "gemini-2.5-flash",
        gemini_fast_model: str = "gemini-2.5-flash-lite",
        gemini_deep_model: str = "gemini-2.5-flash",
        gemini_fallback_model: str = "gemini-2.5-flash-lite",
        llm_cache_ttl_seconds: int = 1800,
        git_timeout_seconds: int = 90,
        max_file_bytes: int = 500_000,
    ) -> None:
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
        self.team_profiles = team_profiles
        self.gitlab_token = str(gitlab_token or "").strip()
        self.gitlab_username = str(gitlab_username or "oauth2").strip() or "oauth2"
        self.gemini_api_key = str(gemini_api_key or "").strip()
        self.gemini_model = str(gemini_model or "gemini-2.5-flash").strip() or "gemini-2.5-flash"
        self.gemini_fast_model = str(gemini_fast_model or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
        self.gemini_deep_model = str(gemini_deep_model or self.gemini_model).strip() or self.gemini_model
        self.gemini_fallback_model = str(gemini_fallback_model or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
        self.llm_budgets = self._build_llm_budgets()
        self.llm_cache_ttl_seconds = max(60, int(llm_cache_ttl_seconds or 1800))
        self.git_timeout_seconds = max(5, int(git_timeout_seconds or 90))
        self.max_file_bytes = max(20_000, int(max_file_bytes or 500_000))

    def options_payload(self) -> dict[str, Any]:
        return {
            "pm_teams": [
                {"code": code, "label": str(profile.get("label") or code)}
                for code, profile in self.team_profiles.items()
            ],
            "countries": list(CRMS_COUNTRIES),
            "all_country": ALL_COUNTRY,
            "answer_modes": [
                {"value": ANSWER_MODE_AUTO, "label": "Auto"},
                {"value": ANSWER_MODE, "label": "Retrieval Only"},
                {"value": ANSWER_MODE_GEMINI, "label": "LLM"},
            ],
            "llm_budget_modes": [
                {"value": "auto", "label": "Auto"},
                {"value": "cheap", "label": "Cheap"},
                {"value": "balanced", "label": "Balanced"},
                {"value": "deep", "label": "Deep"},
            ],
        }

    def _build_llm_budgets(self) -> dict[str, dict[str, Any]]:
        budgets = json.loads(json.dumps(DEFAULT_LLM_BUDGETS))
        budgets["cheap"]["model"] = self.gemini_fast_model
        budgets["balanced"]["model"] = self.gemini_model
        budgets["deep"]["model"] = self.gemini_deep_model
        return budgets

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
        return merged

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
        if intent.get("data_source"):
            profile_terms.extend(self._profile_terms(profile, "data_carriers", "source_terms", "field_population_terms"))
        if intent.get("api"):
            profile_terms.extend(self._profile_terms(profile, "api_terms"))
        if intent.get("config"):
            profile_terms.extend(self._profile_terms(profile, "config_terms"))
        if intent.get("rule_logic") or intent.get("error"):
            profile_terms.extend(self._profile_terms(profile, "logic_terms"))
        expanded = list(tokens)
        for term in profile_terms:
            for token in self._question_tokens(term):
                if token not in expanded:
                    expanded.append(token)
        return expanded[:40]

    def _apply_conversation_context(
        self,
        question: str,
        conversation_context: dict[str, Any] | None,
    ) -> tuple[str, dict[str, Any]]:
        if not isinstance(conversation_context, dict):
            return question, {"used": False}
        lowered = question.lower()
        followup_markers = (
            "this", "that", "it", "them", "above", "previous", "same", "continue",
            "这个", "那个", "上面", "继续", "它", "他们", "这个方法", "这个表",
        )
        is_followup = len(self._question_tokens(question)) <= 8 or any(marker in lowered for marker in followup_markers)
        if not is_followup:
            return question, {"used": False}
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
        deduped: list[str] = []
        for term in terms:
            lowered_term = term.lower()
            if len(lowered_term) < 4 or lowered_term in STOPWORDS or lowered_term in LOW_VALUE_CALL_SYMBOLS:
                continue
            if lowered_term not in deduped:
                deduped.append(lowered_term)
        context_terms = deduped[:16]
        if not context_terms:
            return question, {"used": False}
        augmented = f"{question}\n\nPrevious Source Code Q&A context terms: {' '.join(context_terms)}"
        return augmented, {"used": True, "terms": context_terms, "previous_question": str(conversation_context.get("question") or "")[:180]}

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

    def query(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        limit: int = 12,
        answer_mode: str = ANSWER_MODE,
        llm_budget_mode: str = "cheap",
        conversation_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        key = self.mapping_key(pm_team, country)
        question = str(question or "").strip()
        started_at = time.time()
        if not question:
            raise ToolError("Please enter a source-code question.")
        original_question = question
        question, followup_context = self._apply_conversation_context(question, conversation_context)
        entries = self._load_entries_for_key(key)
        if not entries:
            payload = self._empty_query_payload(
                key,
                status="empty_config",
                summary="No repositories are configured for this PM Team and country yet.",
            )
            self._record_query_telemetry(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                payload=payload,
                started_at=started_at,
            )
            return payload
        tokens = self._question_tokens(question)
        if not tokens:
            payload = self._empty_query_payload(
                key,
                status="weak_question",
                summary="No confident match. Try adding exact class, API, table, field, or function names.",
            )
            self._record_query_telemetry(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                payload=payload,
                started_at=started_at,
            )
            return payload
        domain_profile = self._domain_profile(pm_team, country)
        tokens = self._expand_tokens_with_domain_profile(tokens, question, domain_profile)
        query_plan = self._build_query_decomposition(question, domain_profile)
        matches: list[dict[str, Any]] = []
        repo_status = self.repo_status(key)
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            matches.extend(self._search_repo(entry, repo_path, tokens, question=question))
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
                    )
                )
        matches = self._rank_matches(question, matches)
        top_matches = matches[: max(1, min(int(limit or 12), 30))]
        if top_matches and self._is_dependency_question(question):
            dependency_matches = self._expand_dependency_matches(
                entries=entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
            )
            if dependency_matches:
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in dependency_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)))
        if top_matches:
            trace_matches = self._expand_two_hop_matches(
                entries=entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
            )
            if trace_matches:
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in trace_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)))
        if top_matches:
            agent_matches = self._expand_agent_trace_matches(
                entries=entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
            )
            if agent_matches:
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in agent_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)))
        if top_matches:
            tool_loop_matches = self._run_planner_tool_loop(
                entries=entries,
                key=key,
                question=question,
                base_matches=top_matches,
                limit=limit,
            )
            if tool_loop_matches:
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in tool_loop_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)))
        if top_matches:
            evidence_summary = self._compress_evidence(question, top_matches)
            quality_gate = self._quality_gate(question, evidence_summary)
            agent_plan = self._build_agent_plan(question, evidence_summary, quality_gate)
            top_matches = self._run_agent_plan(
                entries=entries,
                key=key,
                question=question,
                matches=top_matches,
                evidence_summary=evidence_summary,
                quality_gate=quality_gate,
                agent_plan=agent_plan,
                limit=limit,
            )
        if top_matches:
            evidence_summary = self._compress_evidence(question, top_matches)
            quality_gate = self._quality_gate(question, evidence_summary)
            agent_plan = self._build_agent_plan(question, evidence_summary, quality_gate)
            if quality_gate.get("status") != "sufficient" and agent_plan.get("steps"):
                top_matches = self._run_agent_plan(
                    entries=entries,
                    key=key,
                    question=question,
                    matches=top_matches,
                    evidence_summary=evidence_summary,
                    quality_gate=quality_gate,
                    agent_plan=agent_plan,
                    limit=limit,
                )
        if not top_matches:
            payload = self._empty_query_payload(
                key,
                repo_status=repo_status,
                status="no_match",
                summary="No confident match. Try exact symbols, route paths, table names, or error codes.",
            )
            self._record_query_telemetry(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                payload=payload,
                started_at=started_at,
            )
            return payload
        evidence_summary = self._compress_evidence(question, top_matches)
        quality_gate = self._quality_gate(question, evidence_summary)
        trace_paths = self._build_trace_paths(entries=entries, key=key, matches=top_matches, question=question)
        if trace_paths:
            evidence_summary["trace_paths"] = trace_paths
        repo_graph = self._build_repo_dependency_graph(key=key, entries=entries)
        payload = {
            "status": "ok",
            "answer_mode": ANSWER_MODE,
            "summary": self._build_summary(top_matches),
            "matches": top_matches,
            "citations": self._build_citations(top_matches),
            "trace_paths": trace_paths,
            "repo_graph": repo_graph,
            "repo_status": repo_status,
            "answer_quality": quality_gate,
            "agent_plan": self._build_agent_plan(question, evidence_summary, quality_gate),
            "query_plan": query_plan,
            "followup_context": followup_context,
            "original_question": original_question,
        }
        normalized_answer_mode = str(answer_mode or ANSWER_MODE).strip() or ANSWER_MODE
        if normalized_answer_mode in {ANSWER_MODE_GEMINI, ANSWER_MODE_AUTO}:
            if normalized_answer_mode == ANSWER_MODE_AUTO and not self.llm_ready():
                payload["fallback_notice"] = {
                    "title": "Auto LLM unavailable",
                    "message": "Auto mode is using retrieval-only results because Source Code Q&A LLM credentials are not configured.",
                    "fallback_mode": ANSWER_MODE,
                }
                self._record_query_telemetry(
                    key=key,
                    question=question,
                    answer_mode=answer_mode,
                    llm_budget_mode=llm_budget_mode,
                    payload=payload,
                    started_at=started_at,
                )
                return payload
            try:
                llm_payload = self._build_gemini_answer(
                    entries=entries,
                    key=key,
                    pm_team=pm_team,
                    country=country,
                    question=question,
                    matches=top_matches,
                    llm_budget_mode=llm_budget_mode,
                    requested_answer_mode=normalized_answer_mode,
                )
                payload.update(llm_payload)
                payload["answer_mode"] = normalized_answer_mode
            except ToolError as error:
                payload["fallback_notice"] = {
                    "title": "Gemini unavailable",
                    "message": f"{error} Showing retrieval-only results instead.",
                    "fallback_mode": ANSWER_MODE,
                }
        self._record_query_telemetry(
            key=key,
            question=question,
            answer_mode=answer_mode,
            llm_budget_mode=llm_budget_mode,
            payload=payload,
            started_at=started_at,
        )
        return payload

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

    def save_feedback(self, *, user_email: str, payload: dict[str, Any]) -> dict[str, Any]:
        rating = str(payload.get("rating") or "").strip().lower()
        if rating not in {"useful", "not_useful", "wrong_file", "too_vague", "hallucinated", "missing_repo", "needs_deeper_trace"}:
            raise ToolError("Unknown feedback rating.")
        question = str(payload.get("question") or "").strip()
        record = {
            "timestamp": self._now_iso(),
            "user_email": str(user_email or "").strip().lower(),
            "rating": rating,
            "pm_team": str(payload.get("pm_team") or "").strip().upper(),
            "country": str(payload.get("country") or "").strip(),
            "question_sha1": hashlib.sha1(question.encode("utf-8")).hexdigest() if question else "",
            "question_preview": question[:180],
            "top_paths": [str(path) for path in payload.get("top_paths") or []][:8],
            "comment": str(payload.get("comment") or "").strip()[:1000],
            "answer_quality": payload.get("answer_quality") if isinstance(payload.get("answer_quality"), dict) else {},
        }
        self.feedback_path.parent.mkdir(parents=True, exist_ok=True)
        with self.feedback_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        return {"status": "ok", "message": "Feedback saved."}

    def _load_entries_for_key(self, key: str) -> list[RepositoryEntry]:
        raw_entries = self.load_config().get("mappings", {}).get(key, [])
        return [self._normalize_entry(entry) for entry in raw_entries]

    def llm_ready(self) -> bool:
        return bool(self.gemini_api_key)

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
        except (OSError, sqlite3.Error) as error:
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
            payload = json.loads(self.sync_jobs_path.read_text(encoding="utf-8")) if self.sync_jobs_path.exists() else {}
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
        try:
            self.sync_jobs_path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.loads(self.sync_jobs_path.read_text(encoding="utf-8")) if self.sync_jobs_path.exists() else {}
            if not isinstance(payload, dict):
                payload = {}
            payload[key] = job
            self.sync_jobs_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except OSError:
            return

    def _index_path(self, repo_path: Path) -> Path:
        digest = hashlib.sha1(str(repo_path).encode("utf-8")).hexdigest()[:16]
        return self.index_root / f"{digest}.sqlite3"

    def _index_lock_path(self, repo_path: Path) -> Path:
        digest = hashlib.sha1(str(repo_path).encode("utf-8")).hexdigest()[:16]
        return self.lock_root / f"{digest}.lock"

    def _acquire_index_lock(self, repo_path: Path) -> Path:
        self.lock_root.mkdir(parents=True, exist_ok=True)
        lock_path = self._index_lock_path(repo_path)
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as error:
            raise ToolError("This repository is already being indexed. Please wait for the current sync to finish.") from error
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(self._now_iso())
        return lock_path

    def _ensure_repo_index(
        self,
        *,
        key: str | None,
        entry: RepositoryEntry,
        repo_path: Path,
    ) -> dict[str, Any]:
        info = self._repo_index_info(key, entry, repo_path)
        if info.get("state") == "ready":
            return info
        return self._build_repo_index(key or "", entry, repo_path)

    def _repo_index_info(self, key: str | None, entry: RepositoryEntry, repo_path: Path) -> dict[str, Any]:
        del key, entry
        index_path = self._index_path(repo_path)
        if not index_path.exists():
            return {"state": "missing", "path": str(index_path)}
        fingerprint = self._repo_fingerprint(repo_path)
        try:
            with sqlite3.connect(index_path) as connection:
                metadata = dict(connection.execute("select key, value from metadata").fetchall())
        except sqlite3.Error:
            return {"state": "stale", "path": str(index_path)}
        expected = {
            "version": str(CODE_INDEX_VERSION),
            "file_count": str(fingerprint["file_count"]),
            "latest_mtime_ns": str(fingerprint["latest_mtime_ns"]),
            "total_size": str(fingerprint["total_size"]),
        }
        state = "ready" if all(metadata.get(key) == value for key, value in expected.items()) else "stale"
        return {
            "state": state,
            "path": str(index_path),
            "files": int(metadata.get("indexed_files") or 0),
            "lines": int(metadata.get("indexed_lines") or 0),
            "definitions": int(metadata.get("indexed_definitions") or 0),
            "references": int(metadata.get("indexed_references") or 0),
            "entities": int(metadata.get("indexed_entities") or 0),
            "entity_edges": int(metadata.get("indexed_entity_edges") or 0),
            "edges": int(metadata.get("indexed_edges") or 0),
            "flow_edges": int(metadata.get("indexed_flow_edges") or 0),
            "fts_enabled": metadata.get("fts_enabled") == "1",
            "updated_at": metadata.get("updated_at"),
        }

    def _repo_fingerprint(self, repo_path: Path) -> dict[str, int]:
        file_count = 0
        latest_mtime_ns = 0
        total_size = 0
        for file_path in self._iter_text_files(repo_path):
            try:
                stat = file_path.stat()
            except OSError:
                continue
            file_count += 1
            latest_mtime_ns = max(latest_mtime_ns, stat.st_mtime_ns)
            total_size += stat.st_size
        return {
            "file_count": file_count,
            "latest_mtime_ns": latest_mtime_ns,
            "total_size": total_size,
        }

    def _build_repo_index(self, key: str, entry: RepositoryEntry, repo_path: Path) -> dict[str, Any]:
        del key, entry
        lock_path = self._acquire_index_lock(repo_path)
        self.index_root.mkdir(parents=True, exist_ok=True)
        index_path = self._index_path(repo_path)
        tmp_path = index_path.with_suffix(".tmp")
        tmp_path.unlink(missing_ok=True)
        fingerprint = self._repo_fingerprint(repo_path)
        indexed_files = 0
        indexed_lines = 0
        indexed_definitions = 0
        indexed_references = 0
        indexed_entities = 0
        indexed_entity_edges = 0
        try:
            with sqlite3.connect(tmp_path) as connection:
                connection.execute("pragma journal_mode=off")
                connection.execute("pragma synchronous=off")
                connection.executescript(
                    """
                    create table metadata (key text primary key, value text not null);
                    create table files (
                        path text primary key,
                        lower_path text not null,
                        size integer not null,
                        mtime_ns integer not null,
                        line_count integer not null,
                        symbols text not null
                    );
                    create table lines (
                        file_path text not null,
                        line_no integer not null,
                        line_text text not null,
                        lower_text text not null,
                        symbols text not null,
                        is_declaration integer not null,
                        has_pathish integer not null,
                        primary key (file_path, line_no)
                    );
                    create index idx_lines_file_path on lines(file_path);
                    create table definitions (
                        name text not null,
                        lower_name text not null,
                        kind text not null,
                        file_path text not null,
                        line_no integer not null,
                        signature text not null
                    );
                    create index idx_definitions_lower_name on definitions(lower_name);
                    create index idx_definitions_file_path on definitions(file_path);
                    create table references_index (
                        target text not null,
                        lower_target text not null,
                        kind text not null,
                        file_path text not null,
                        line_no integer not null,
                        context text not null
                    );
                    create index idx_references_lower_target on references_index(lower_target);
                    create index idx_references_file_path on references_index(file_path);
                    create table code_entities (
                        entity_id text primary key,
                        name text not null,
                        lower_name text not null,
                        kind text not null,
                        language text not null,
                        file_path text not null,
                        line_no integer not null,
                        parent text not null,
                        signature text not null
                    );
                    create index idx_entities_lower_name on code_entities(lower_name);
                    create index idx_entities_file_path on code_entities(file_path);
                    create index idx_entities_kind on code_entities(kind);
                    create table entity_edges (
                        from_entity_id text not null,
                        from_file text not null,
                        from_line integer not null,
                        edge_kind text not null,
                        to_name text not null,
                        lower_to_name text not null,
                        to_entity_id text not null,
                        to_file text not null,
                        to_line integer not null,
                        evidence text not null
                    );
                    create index idx_entity_edges_from on entity_edges(from_entity_id);
                    create index idx_entity_edges_from_file on entity_edges(from_file);
                    create index idx_entity_edges_lower_to_name on entity_edges(lower_to_name);
                    create index idx_entity_edges_to_file on entity_edges(to_file);
                    create index idx_entity_edges_kind on entity_edges(edge_kind);
                    create table graph_edges (
                        from_file text not null,
                        from_line integer not null,
                        symbol text not null,
                        lower_symbol text not null,
                        edge_kind text not null,
                        to_file text not null,
                        to_line integer not null
                    );
                    create index idx_graph_from_file on graph_edges(from_file);
                    create index idx_graph_lower_symbol on graph_edges(lower_symbol);
                    create index idx_graph_to_file on graph_edges(to_file);
                    create table flow_edges (
                        from_file text not null,
                        from_line integer not null,
                        from_kind text not null,
                        from_name text not null,
                        edge_kind text not null,
                        to_name text not null,
                        to_file text not null,
                        to_line integer not null,
                        evidence text not null
                    );
                    create index idx_flow_from_file on flow_edges(from_file);
                    create index idx_flow_to_file on flow_edges(to_file);
                    create index idx_flow_to_name on flow_edges(to_name);
                    create index idx_flow_edge_kind on flow_edges(edge_kind);
                    """
                )
                fts_enabled = self._try_create_fts(connection)
                for file_path in self._iter_text_files(repo_path):
                    relative_path = str(file_path.relative_to(repo_path))
                    try:
                        stat = file_path.stat()
                        lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
                    except OSError:
                        continue
                    file_symbols = self._collect_symbols(lines)
                    connection.execute(
                        "insert into files(path, lower_path, size, mtime_ns, line_count, symbols) values (?, ?, ?, ?, ?, ?)",
                        (
                            relative_path,
                            relative_path.lower(),
                            stat.st_size,
                            stat.st_mtime_ns,
                            len(lines),
                            json.dumps(sorted(file_symbols), separators=(",", ":")),
                        ),
                    )
                    line_rows = []
                    for index, line in enumerate(lines, start=1):
                        lowered = line.lower()
                        line_rows.append(
                            (
                                relative_path,
                                index,
                                line,
                                lowered,
                                json.dumps(sorted(self._line_symbols(lowered)), separators=(",", ":")),
                                1 if self._is_declaration_line(line) else 0,
                                1 if PATHISH_PATTERN.search(line) else 0,
                            )
                        )
                    connection.executemany(
                        """
                        insert into lines(file_path, line_no, line_text, lower_text, symbols, is_declaration, has_pathish)
                        values (?, ?, ?, ?, ?, ?, ?)
                        """,
                        line_rows,
                    )
                    if fts_enabled:
                        connection.executemany(
                            "insert into lines_fts(file_path, line_no, content) values (?, ?, ?)",
                            [(relative_path, row[1], row[2]) for row in line_rows],
                        )
                    structure = self._extract_structure_rows(relative_path, lines)
                    connection.executemany(
                        """
                        insert into definitions(name, lower_name, kind, file_path, line_no, signature)
                        values (?, ?, ?, ?, ?, ?)
                        """,
                        structure["definitions"],
                    )
                    connection.executemany(
                        """
                        insert into references_index(target, lower_target, kind, file_path, line_no, context)
                        values (?, ?, ?, ?, ?, ?)
                        """,
                        structure["references"],
                    )
                    connection.executemany(
                        """
                        insert or ignore into code_entities(entity_id, name, lower_name, kind, language, file_path, line_no, parent, signature)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        structure["entities"],
                    )
                    connection.executemany(
                        """
                        insert into entity_edges(from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        structure["entity_edges"],
                    )
                    indexed_files += 1
                    indexed_lines += len(lines)
                    indexed_definitions += len(structure["definitions"])
                    indexed_references += len(structure["references"])
                    indexed_entities += len(structure["entities"])
                    indexed_entity_edges += len(structure["entity_edges"])
                indexed_edges = self._build_graph_edges(connection)
                resolved_entity_edges = self._resolve_entity_edges(connection)
                indexed_entity_edges += resolved_entity_edges
                indexed_flow_edges = self._build_flow_edges(connection)
                metadata = {
                    "version": str(CODE_INDEX_VERSION),
                    "file_count": str(fingerprint["file_count"]),
                    "latest_mtime_ns": str(fingerprint["latest_mtime_ns"]),
                    "total_size": str(fingerprint["total_size"]),
                    "indexed_files": str(indexed_files),
                    "indexed_lines": str(indexed_lines),
                    "indexed_definitions": str(indexed_definitions),
                    "indexed_references": str(indexed_references),
                    "indexed_entities": str(indexed_entities),
                    "indexed_entity_edges": str(indexed_entity_edges),
                    "indexed_edges": str(indexed_edges),
                    "indexed_flow_edges": str(indexed_flow_edges),
                    "fts_enabled": "1" if fts_enabled else "0",
                    "updated_at": self._now_iso(),
                }
                connection.executemany("insert into metadata(key, value) values (?, ?)", metadata.items())
            tmp_path.replace(index_path)
            return {
                "state": "ready",
                "path": str(index_path),
                "files": indexed_files,
                "lines": indexed_lines,
                "definitions": indexed_definitions,
                "references": indexed_references,
                "entities": indexed_entities,
                "entity_edges": indexed_entity_edges,
                "edges": indexed_edges,
                "flow_edges": indexed_flow_edges,
                "fts_enabled": fts_enabled,
                "updated_at": metadata["updated_at"],
            }
        finally:
            lock_path.unlink(missing_ok=True)

    @staticmethod
    def _try_create_fts(connection: sqlite3.Connection) -> bool:
        try:
            connection.execute(
                "create virtual table lines_fts using fts5(file_path unindexed, line_no unindexed, content)"
            )
            return True
        except sqlite3.Error:
            return False

    @staticmethod
    def _build_graph_edges(connection: sqlite3.Connection) -> int:
        definitions = connection.execute(
            "select lower_name, file_path, line_no from definitions"
        ).fetchall()
        definition_by_name: dict[str, list[tuple[str, int]]] = {}
        for lower_name, file_path, line_no in definitions:
            definition_by_name.setdefault(str(lower_name), []).append((str(file_path), int(line_no)))
        rows = []
        for target, lower_target, kind, file_path, line_no, _context in connection.execute(
            "select target, lower_target, kind, file_path, line_no, context from references_index"
        ):
            for to_file, to_line in definition_by_name.get(str(lower_target), [])[:8]:
                if to_file == file_path:
                    continue
                rows.append((file_path, int(line_no), target, lower_target, kind, to_file, to_line))
        rows = list(dict.fromkeys(rows))
        connection.executemany(
            """
            insert into graph_edges(from_file, from_line, symbol, lower_symbol, edge_kind, to_file, to_line)
            values (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)

    @staticmethod
    def _resolve_entity_edges(connection: sqlite3.Connection) -> int:
        entities = connection.execute(
            "select entity_id, lower_name, file_path, line_no from code_entities"
        ).fetchall()
        by_name: dict[str, list[tuple[str, str, int]]] = {}
        for entity_id, lower_name, file_path, line_no in entities:
            by_name.setdefault(str(lower_name), []).append((str(entity_id), str(file_path), int(line_no)))
        updates: list[tuple[str, str, int, int]] = []
        for rowid, lower_to_name, from_file in connection.execute(
            """
            select rowid, lower_to_name, from_file
            from entity_edges
            where to_file = ''
            """
        ):
            candidates = by_name.get(str(lower_to_name)) or []
            if not candidates:
                short_name = str(lower_to_name).rsplit(".", 1)[-1]
                candidates = by_name.get(short_name) or []
            if not candidates:
                continue
            candidates = sorted(candidates, key=lambda item: 0 if item[1] != str(from_file) else 1)
            to_entity_id, to_file, to_line = candidates[0]
            updates.append((to_entity_id, to_file, to_line, int(rowid)))
        connection.executemany(
            """
            update entity_edges
            set to_entity_id = ?, to_file = ?, to_line = ?
            where rowid = ?
            """,
            updates,
        )
        return len(updates)

    @staticmethod
    def _build_flow_edges(connection: sqlite3.Connection) -> int:
        rows: list[tuple[str, int, str, str, str, str, str, int, str]] = []

        for target, kind, file_path, line_no, context in connection.execute(
            """
            select target, kind, file_path, line_no, context
            from references_index
            where kind in ('route', 'sql_table')
            """
        ):
            edge_kind = "route" if str(kind) == "route" else "sql_table"
            rows.append(
                (
                    str(file_path),
                    int(line_no),
                    SourceCodeQAService._flow_role_for_path(str(file_path)),
                    SourceCodeQAService._flow_name_for_path(str(file_path)),
                    edge_kind,
                    str(target),
                    "",
                    0,
                    str(context or "")[:500],
                )
            )

        for from_file, from_line, symbol, edge_kind, to_file, to_line in connection.execute(
            """
            select from_file, from_line, symbol, edge_kind, to_file, to_line
            from graph_edges
            """
        ):
            classified = SourceCodeQAService._classify_flow_edge(
                str(edge_kind),
                str(from_file),
                str(to_file),
                str(symbol),
            )
            rows.append(
                (
                    str(from_file),
                    int(from_line),
                    SourceCodeQAService._flow_role_for_path(str(from_file)),
                    SourceCodeQAService._flow_name_for_path(str(from_file)),
                    classified,
                    str(symbol),
                    str(to_file),
                    int(to_line),
                    f"{edge_kind} {symbol}".strip()[:500],
                )
            )

        for from_file, from_line, edge_kind, to_name, to_file, to_line, evidence in connection.execute(
            """
            select from_file, from_line, edge_kind, to_name, to_file, to_line, evidence
            from entity_edges
            where edge_kind in (
                'route', 'sql_table', 'injects', 'call', 'import', 'symbol_reference',
                'mapper_statement', 'downstream_api', 'http_endpoint', 'framework_binding',
                'data_flow'
            )
            """
        ):
            rows.append(
                (
                    str(from_file),
                    int(from_line),
                    SourceCodeQAService._flow_role_for_path(str(from_file)),
                    SourceCodeQAService._flow_name_for_path(str(from_file)),
                    SourceCodeQAService._classify_flow_edge(str(edge_kind), str(from_file), str(to_file), str(to_name)),
                    str(to_name),
                    str(to_file),
                    int(to_line or 0),
                    str(evidence or "")[:500],
                )
            )

        rows = list(dict.fromkeys(rows))
        connection.executemany(
            """
            insert into flow_edges(from_file, from_line, from_kind, from_name, edge_kind, to_name, to_file, to_line, evidence)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)

    @staticmethod
    def _flow_name_for_path(path: str) -> str:
        return Path(str(path or "unknown")).stem or "unknown"

    @staticmethod
    def _flow_role_for_path(path: str) -> str:
        lowered = str(path or "").lower()
        if any(term in lowered for term in ("controller", "/api/", "/web/")):
            return "controller"
        if "service" in lowered:
            return "service"
        if "repository" in lowered:
            return "repository"
        if "mapper" in lowered:
            return "mapper"
        if "dao" in lowered:
            return "dao"
        if any(term in lowered for term in ("client", "integration", "gateway", "adapter")):
            return "client"
        if any(term in lowered for term in ("config", "properties", ".yml", ".yaml")):
            return "config"
        return "code"

    @staticmethod
    def _classify_flow_edge(reference_kind: str, from_file: str, to_file: str, symbol: str) -> str:
        del from_file
        lowered_symbol = str(symbol or "").lower()
        to_role = SourceCodeQAService._flow_role_for_path(to_file)
        if str(reference_kind) == "route":
            return "route"
        if str(reference_kind) == "sql_table":
            return "sql_table"
        if str(reference_kind) == "mapper_statement":
            return "mapper"
        if str(reference_kind) in {"downstream_api", "http_endpoint"}:
            return "client"
        if str(reference_kind) == "framework_binding":
            return "framework"
        if str(reference_kind) == "data_flow":
            return "field_population"
        if to_role in {"repository", "mapper", "dao"}:
            return to_role
        if to_role == "service":
            return "service"
        if to_role == "controller":
            return "controller"
        if to_role == "client" or any(suffix in lowered_symbol for suffix in ("client", "integration", "gateway")):
            return "client"
        if to_role == "config":
            return "config"
        return "call"

    @staticmethod
    def _entity_id(file_path: str, kind: str, name: str, line_no: int) -> str:
        raw = f"{file_path}:{kind}:{name}:{line_no}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]

    def _extract_structure_rows(self, relative_path: str, lines: list[str]) -> dict[str, list[tuple[Any, ...]]]:
        definitions: list[tuple[Any, ...]] = []
        references: list[tuple[Any, ...]] = []
        entities: list[tuple[Any, ...]] = []
        entity_edges: list[tuple[Any, ...]] = []
        suffix = Path(relative_path).suffix.lower()
        language = self._language_for_suffix(suffix)
        file_entity_id = self._entity_id(relative_path, "file", relative_path, 1)
        entities.append((file_entity_id, relative_path, relative_path.lower(), "file", language, relative_path, 1, "", relative_path))

        def add_definition(name: str, kind: str, line_no: int, signature: str) -> None:
            name = str(name or "").strip()
            if not name:
                return
            definitions.append((name, name.lower(), kind, relative_path, line_no, signature.strip()[:500]))
            add_entity(name, kind, line_no, signature)

        def add_reference(target: str, kind: str, line_no: int, context: str) -> None:
            target = str(target or "").strip().strip("\"'")
            if len(target) < 2:
                return
            references.append((target, target.lower(), kind, relative_path, line_no, context.strip()[:500]))

        def add_entity(name: str, kind: str, line_no: int, signature: str, parent: str = "") -> str:
            normalized = str(name or "").strip()
            if not normalized:
                return file_entity_id
            entity_id = self._entity_id(relative_path, kind, normalized, line_no)
            entities.append(
                (
                    entity_id,
                    normalized,
                    normalized.lower(),
                    kind,
                    language,
                    relative_path,
                    int(line_no),
                    str(parent or ""),
                    str(signature or "").strip()[:500],
                )
            )
            return entity_id

        def add_entity_edge(
            from_entity_id: str,
            edge_kind: str,
            to_name: str,
            line_no: int,
            evidence: str,
        ) -> None:
            target = str(to_name or "").strip().strip("\"'")
            if len(target) < 2:
                return
            entity_edges.append(
                (
                    from_entity_id or file_entity_id,
                    relative_path,
                    int(line_no),
                    str(edge_kind or "reference"),
                    target,
                    target.lower(),
                    "",
                    "",
                    0,
                    str(evidence or "").strip()[:500],
                )
            )

        if suffix == ".py":
            self._extract_python_ast_structure(
                relative_path=relative_path,
                lines=lines,
                add_definition=add_definition,
                add_reference=add_reference,
                add_entity=add_entity,
                add_entity_edge=add_entity_edge,
                file_entity_id=file_entity_id,
            )

        current_class = ""
        current_class_id = file_entity_id
        current_method = ""
        current_method_id = file_entity_id
        pending_routes: list[tuple[str, int, str]] = []
        mapper_namespace = ""
        mapper_namespace_id = file_entity_id
        for line_no, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            namespace_match = MYBATIS_NAMESPACE_PATTERN.search(line)
            if namespace_match:
                mapper_namespace = namespace_match.group(1)
                add_definition(mapper_namespace, "mybatis_mapper_namespace", line_no, stripped)
                mapper_namespace_id = self._entity_id(relative_path, "mybatis_mapper_namespace", mapper_namespace, line_no)
            statement_match = MYBATIS_STATEMENT_PATTERN.search(line)
            if statement_match:
                statement_name = f"{mapper_namespace}.{statement_match.group(2)}" if mapper_namespace else statement_match.group(2)
                add_definition(statement_name, f"mybatis_{statement_match.group(1).lower()}", line_no, stripped)
                statement_id = self._entity_id(relative_path, f"mybatis_{statement_match.group(1).lower()}", statement_name, line_no)
                add_entity_edge(mapper_namespace_id, "mapper_statement", statement_name, line_no, stripped)
                current_method = statement_name
                current_method_id = statement_id
            feign_match = FEIGN_CLIENT_PATTERN.search(line)
            if feign_match:
                for value in re.findall(r'"([^"]+)"', feign_match.group(1)):
                    add_reference(value, "downstream_api", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, "downstream_api", value, line_no, stripped)
            for match in CLASS_DEF_PATTERN.finditer(line):
                add_definition(match.group(2), match.group(1).lower(), line_no, stripped)
                current_class = match.group(2)
                current_class_id = self._entity_id(relative_path, match.group(1).lower(), current_class, line_no)
                current_method = ""
                current_method_id = current_class_id
            py_match = PY_DEF_PATTERN.search(line)
            if py_match and suffix != ".py":
                add_definition(py_match.group(2), "python_" + py_match.group(1).lower(), line_no, stripped)
            js_match = JS_DEF_PATTERN.search(line)
            if js_match:
                function_name = js_match.group(1) or js_match.group(2)
                add_definition(function_name, "javascript_function", line_no, stripped)
                current_method = function_name
                current_method_id = self._entity_id(relative_path, "javascript_function", function_name, line_no)
            java_method = JAVA_METHOD_DEF_PATTERN.search(line)
            if java_method and not stripped.startswith(("if ", "for ", "while ", "switch ", "catch ")):
                method_name = java_method.group(1)
                add_definition(method_name, "java_method", line_no, stripped)
                current_method = method_name
                current_method_id = self._entity_id(relative_path, "java_method", method_name, line_no)
                for route, route_line, route_context in pending_routes:
                    add_reference(route, "route", route_line, route_context)
                    add_entity_edge(current_method_id, "route", route, route_line, route_context)
                pending_routes = []
            for annotation in ANNOTATION_ROUTE_PATTERN.finditer(line):
                add_definition(annotation.group(1), "route_annotation", line_no, stripped)
                for route in re.findall(r'"([^"]+)"', annotation.group(2) or ""):
                    add_reference(route, "route", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id, "route", route, line_no, stripped)
                    pending_routes.append((route, line_no, stripped))
            for table in SQL_TABLE_PATTERN.findall(line):
                add_reference(table, "sql_table", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id, "sql_table", table, line_no, stripped)
            for endpoint in HTTP_LITERAL_PATTERN.findall(line):
                if endpoint.startswith("http") or any(client in stripped.lower() for client in ("resttemplate", "webclient", "feign", "exchange", "postfor", "getfor", "request")):
                    add_reference(endpoint, "http_endpoint", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "http_endpoint", endpoint, line_no, stripped)
            if suffix in {".properties", ".yaml", ".yml", ".conf", ".toml"}:
                key_match = PROPERTIES_KEY_PATTERN.search(line)
                if key_match:
                    add_definition(key_match.group(1), "config_key", line_no, stripped)
                    add_entity_edge(file_entity_id, "config", key_match.group(1), line_no, stripped)
            field_match = FIELD_OR_PARAM_TYPE_PATTERN.search(line)
            if field_match:
                add_entity_edge(current_class_id or file_entity_id, "injects", field_match.group(1), line_no, stripped)
            for target in self._extract_data_flow_targets(stripped):
                add_reference(target, "data_flow", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "data_flow", target, line_no, stripped)
            for symbol in self._extract_downstream_symbols(line):
                add_reference(symbol, "symbol_reference", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "symbol_reference", symbol, line_no, stripped)
            for call in CALL_SYMBOL_PATTERN.findall(line):
                lowered = call.lower()
                if lowered not in LOW_VALUE_CALL_SYMBOLS and lowered not in STOPWORDS:
                    add_reference(call, "call", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", call, line_no, stripped)

        return {
            "definitions": list(dict.fromkeys(definitions)),
            "references": list(dict.fromkeys(references)),
            "entities": list(dict.fromkeys(entities)),
            "entity_edges": list(dict.fromkeys(entity_edges)),
        }

    @staticmethod
    def _language_for_suffix(suffix: str) -> str:
        return {
            ".java": "java",
            ".py": "python",
            ".js": "javascript",
            ".jsx": "javascript",
            ".ts": "typescript",
            ".tsx": "typescript",
            ".xml": "xml",
            ".yaml": "yaml",
            ".yml": "yaml",
            ".properties": "properties",
            ".sql": "sql",
        }.get(str(suffix or "").lower(), "text")

    def _extract_python_ast_structure(
        self,
        *,
        relative_path: str,
        lines: list[str],
        add_definition,
        add_reference,
        add_entity,
        add_entity_edge,
        file_entity_id: str,
    ) -> None:
        try:
            tree = ast.parse("\n".join(lines))
        except SyntaxError:
            return

        class Visitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.stack: list[tuple[str, str]] = []

            def _current_entity(self) -> str:
                return self.stack[-1][1] if self.stack else file_entity_id

            def visit_ClassDef(self, node: ast.ClassDef) -> Any:
                signature = lines[node.lineno - 1].strip() if 0 < node.lineno <= len(lines) else node.name
                add_definition(node.name, "python_class", node.lineno, signature)
                entity_id = SourceCodeQAService._entity_id(relative_path, "python_class", node.name, node.lineno)
                self.stack.append((node.name, entity_id))
                self.generic_visit(node)
                self.stack.pop()

            def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
                self._visit_function(node, "python_function")

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
                self._visit_function(node, "python_async_function")

            def _visit_function(self, node: ast.AST, kind: str) -> None:
                name = getattr(node, "name", "")
                line_no = int(getattr(node, "lineno", 1) or 1)
                signature = lines[line_no - 1].strip() if 0 < line_no <= len(lines) else name
                parent = self.stack[-1][0] if self.stack else ""
                add_definition(name, kind, line_no, signature)
                entity_id = add_entity(name, kind, line_no, signature, parent=parent)
                self.stack.append((name, entity_id))
                self.generic_visit(node)
                self.stack.pop()

            def visit_Import(self, node: ast.Import) -> Any:
                line_no = int(getattr(node, "lineno", 1) or 1)
                evidence = lines[line_no - 1].strip() if 0 < line_no <= len(lines) else "import"
                for alias in node.names:
                    add_reference(alias.name, "import", line_no, evidence)
                    add_entity_edge(self._current_entity(), "import", alias.name, line_no, evidence)

            def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
                line_no = int(getattr(node, "lineno", 1) or 1)
                evidence = lines[line_no - 1].strip() if 0 < line_no <= len(lines) else "import"
                module = str(node.module or "")
                for alias in node.names:
                    target = f"{module}.{alias.name}" if module else alias.name
                    add_reference(target, "import", line_no, evidence)
                    add_entity_edge(self._current_entity(), "import", target, line_no, evidence)

            def visit_Call(self, node: ast.Call) -> Any:
                line_no = int(getattr(node, "lineno", 1) or 1)
                evidence = lines[line_no - 1].strip() if 0 < line_no <= len(lines) else "call"
                target = self._call_name(node.func)
                if target:
                    add_reference(target, "call", line_no, evidence)
                    add_entity_edge(self._current_entity(), "call", target, line_no, evidence)
                self.generic_visit(node)

            @staticmethod
            def _call_name(node: ast.AST) -> str:
                if isinstance(node, ast.Name):
                    return node.id
                if isinstance(node, ast.Attribute):
                    parts = [node.attr]
                    value = node.value
                    while isinstance(value, ast.Attribute):
                        parts.append(value.attr)
                        value = value.value
                    if isinstance(value, ast.Name):
                        parts.append(value.id)
                    return ".".join(reversed(parts))
                return ""

        Visitor().visit(tree)

    @staticmethod
    def _extract_data_flow_targets(line: str) -> list[str]:
        stripped = str(line or "").strip()
        if not stripped or stripped.startswith(("import ", "package ", "//", "*")):
            return []
        targets: list[str] = []

        def add_tokens(value: str) -> None:
            for token in IDENTIFIER_PATTERN.findall(str(value or "")):
                lowered = token.lower()
                if lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
                    continue
                if len(lowered) < 4:
                    continue
                targets.append(token)

        for match in SETTER_CALL_PATTERN.finditer(stripped):
            field_name = match.group(1)
            argument = match.group(2)
            targets.append(f"set{field_name}")
            add_tokens(argument)
        for match in BUILDER_FIELD_PATTERN.finditer(stripped):
            field_name = match.group(1)
            argument = match.group(2)
            if field_name.lower() not in LOW_VALUE_CALL_SYMBOLS:
                targets.append(field_name)
                add_tokens(argument)
        assignment = ASSIGNMENT_PATTERN.search(stripped)
        if assignment and "==" not in stripped:
            add_tokens(assignment.group(1))
            add_tokens(assignment.group(2))
        return list(dict.fromkeys(targets))[:12]

    def _search_repo_index(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        tokens: list[str],
        *,
        question: str,
        focus_terms: list[str] | None = None,
        trace_stage: str = "direct",
    ) -> list[dict[str, Any]]:
        index_path = self._index_path(repo_path)
        matches: list[dict[str, Any]] = []
        repo_score = self._repo_match_score(entry, tokens)
        trace_stage_bonus = 90 if trace_stage == "two_hop" or trace_stage == "query_decomposition" or trace_stage.startswith(TOOL_LOOP_TRACE_PREFIX) or trace_stage.startswith("agent_trace") or trace_stage.startswith("agent_plan") or trace_stage == QUALITY_GATE_TRACE_STAGE else 0
        normalized_focus_terms = [term.lower() for term in (focus_terms or []) if term]
        query_terms = list(dict.fromkeys([*tokens, *normalized_focus_terms]))
        file_hits: dict[str, dict[str, Any]] = {}
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            file_rows = connection.execute("select * from files").fetchall()
            for file_row in file_rows:
                path_text = str(file_row["lower_path"] or "")
                file_symbols = set(json.loads(file_row["symbols"] or "[]"))
                path_score = sum(10 for token in tokens if token in path_text)
                if normalized_focus_terms and any(hint in path_text for hint in DEPENDENCY_PATH_HINTS):
                    path_score += 18
                symbol_score = sum(16 for token in tokens if token in file_symbols)
                symbol_score += sum(20 for term in normalized_focus_terms if term in file_symbols)
                if path_score or symbol_score or repo_score:
                    file_hits[file_row["path"]] = {
                        "path_text": path_text,
                        "file_symbols": file_symbols,
                        "path_score": path_score,
                        "symbol_score": symbol_score,
                        "best_line": 1,
                        "best_score": path_score + symbol_score + repo_score + trace_stage_bonus,
                        "structure_hits": [],
                    }
            for term in query_terms:
                if len(term) < 3:
                    continue
                like_term = f"%{term}%"
                for row in connection.execute(
                    "select * from definitions where lower_name like ? limit 40",
                    (like_term,),
                ):
                    file_path = str(row["file_path"])
                    hit = file_hits.setdefault(
                        file_path,
                        {
                            "path_text": file_path.lower(),
                            "file_symbols": set(),
                            "path_score": 0,
                            "symbol_score": 0,
                            "best_line": int(row["line_no"]),
                            "best_score": 0,
                            "structure_hits": [],
                        },
                    )
                    boost = 72 if str(row["lower_name"]) == term else 42
                    score = boost + repo_score + trace_stage_bonus
                    if score > hit.get("best_score", 0):
                        hit["best_line"] = int(row["line_no"])
                        hit["best_score"] = score
                    hit["structure_hits"].append(f"{row['kind']} definition {row['name']}")
                for row in connection.execute(
                    "select * from references_index where lower_target like ? limit 60",
                    (like_term,),
                ):
                    file_path = str(row["file_path"])
                    hit = file_hits.setdefault(
                        file_path,
                        {
                            "path_text": file_path.lower(),
                            "file_symbols": set(),
                            "path_score": 0,
                            "symbol_score": 0,
                            "best_line": int(row["line_no"]),
                            "best_score": 0,
                            "structure_hits": [],
                        },
                    )
                    boost = 58 if str(row["lower_target"]) == term else 32
                    if str(row["kind"]) in {"sql_table", "route"}:
                        boost += 18
                    score = boost + repo_score + trace_stage_bonus
                    if score > hit.get("best_score", 0):
                        hit["best_line"] = int(row["line_no"])
                        hit["best_score"] = score
                    hit["structure_hits"].append(f"{row['kind']} reference {row['target']}")
            for row in self._fts_search_rows(connection, tokens, normalized_focus_terms):
                file_path = str(row["file_path"])
                hit = file_hits.setdefault(
                    file_path,
                    {
                        "path_text": file_path.lower(),
                        "file_symbols": set(),
                        "path_score": 0,
                        "symbol_score": 0,
                        "best_line": int(row["line_no"]),
                        "best_score": 0,
                        "structure_hits": [],
                    },
                )
                score = max(12, int(80 - min(float(row["rank"]), 60))) + repo_score + trace_stage_bonus
                if score > hit.get("best_score", 0):
                    hit["best_line"] = int(row["line_no"])
                    hit["best_score"] = score
                hit["structure_hits"].append("bm25 content match")
            for row in connection.execute("select * from lines"):
                lower_text = str(row["lower_text"] or "")
                line_symbols = set(json.loads(row["symbols"] or "[]"))
                file_path = str(row["file_path"])
                file_hit = file_hits.get(file_path)
                if file_hit is None:
                    file_row = next((item for item in file_rows if item["path"] == file_path), None)
                    if file_row is None:
                        continue
                    file_hit = {
                        "path_text": str(file_row["lower_path"] or ""),
                        "file_symbols": set(json.loads(file_row["symbols"] or "[]")),
                        "path_score": 0,
                        "symbol_score": 0,
                        "best_line": 1,
                        "best_score": 0,
                        "structure_hits": [],
                    }
                score = sum(3 + min(lower_text.count(token), 3) for token in tokens if token in lower_text)
                score += sum(12 for token in tokens if token in line_symbols)
                score += sum(16 for term in normalized_focus_terms if term in line_symbols)
                score += sum(45 for term in normalized_focus_terms if term in lower_text)
                if int(row["is_declaration"] or 0):
                    score += sum(8 for token in tokens if token in lower_text or token in line_symbols)
                    score += sum(10 for term in normalized_focus_terms if term in lower_text or term in line_symbols)
                if int(row["has_pathish"] or 0):
                    score += sum(6 for token in tokens if token in lower_text)
                if score:
                    score += (
                        file_hit["path_score"]
                        + file_hit["symbol_score"]
                        + repo_score
                        + self._keyword_proximity_bonus(lower_text, tokens)
                        + self._hybrid_query_bonus(question, lower_text, line_symbols)
                        + trace_stage_bonus
                    )
                    if score > file_hit.get("best_score", 0):
                        file_hit.update(
                            {
                                "best_line": int(row["line_no"]),
                                "best_score": score,
                            }
                        )
                    file_hits[file_path] = file_hit
            for relative_path, hit in file_hits.items():
                if not hit.get("best_score"):
                    continue
                lines = [
                    str(row["line_text"])
                    for row in connection.execute(
                        "select line_text from lines where file_path = ? order by line_no",
                        (relative_path,),
                    )
                ]
                if not lines:
                    continue
                start, end = self._best_snippet_window(lines, int(hit["best_line"]))
                snippet = "\n".join(lines[start - 1 : end]).strip()
                reason = self._match_reason(
                    tokens,
                    hit["path_text"],
                    snippet,
                    file_symbols=hit["file_symbols"],
                    question=question,
                    focus_terms=normalized_focus_terms,
                    trace_stage=trace_stage,
                )
                structure_hits = list(dict.fromkeys(hit.get("structure_hits") or []))
                if structure_hits:
                    reason = f"{reason}; structure matched: {', '.join(structure_hits[:4])}"
                matches.append(
                    {
                        "repo": entry.display_name,
                        "path": relative_path,
                        "line_start": start,
                        "line_end": end,
                        "score": hit["best_score"],
                        "snippet": snippet[:2400],
                        "reason": reason,
                        "trace_stage": trace_stage,
                        "retrieval": "persistent_index",
                    }
                )
        return matches

    def _fts_search_rows(
        self,
        connection: sqlite3.Connection,
        tokens: list[str],
        focus_terms: list[str],
    ) -> list[sqlite3.Row]:
        terms = []
        for term in [*tokens, *focus_terms]:
            normalized = str(term or "").strip().lower()
            if len(normalized) < 3 or normalized in STOPWORDS:
                continue
            if FTS_TOKEN_PATTERN.fullmatch(normalized):
                terms.append(normalized.replace('"', ""))
        terms = list(dict.fromkeys(terms))[:12]
        if not terms:
            return []
        query = " OR ".join(f'"{term}"' for term in terms)
        try:
            return list(
                connection.execute(
                    """
                    select file_path, line_no, bm25(lines_fts) as rank
                    from lines_fts
                    where lines_fts match ?
                    order by rank
                    limit 80
                    """,
                    (query,),
                )
            )
        except sqlite3.Error:
            return []

    def _search_repo(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        tokens: list[str],
        *,
        question: str,
        focus_terms: list[str] | None = None,
        trace_stage: str = "direct",
    ) -> list[dict[str, Any]]:
        try:
            self._ensure_repo_index(key=None, entry=entry, repo_path=repo_path)
            return self._search_repo_index(
                entry,
                repo_path,
                tokens,
                question=question,
                focus_terms=focus_terms,
                trace_stage=trace_stage,
            )
        except (OSError, sqlite3.Error):
            return self._search_repo_files(
                entry,
                repo_path,
                tokens,
                question=question,
                focus_terms=focus_terms,
                trace_stage=trace_stage,
            )

    def _search_repo_files(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        tokens: list[str],
        *,
        question: str,
        focus_terms: list[str] | None = None,
        trace_stage: str = "direct",
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        repo_score = self._repo_match_score(entry, tokens)
        trace_stage_bonus = 90 if trace_stage == "two_hop" or trace_stage == "query_decomposition" or trace_stage.startswith(TOOL_LOOP_TRACE_PREFIX) or trace_stage.startswith("agent_trace") or trace_stage.startswith("agent_plan") or trace_stage == QUALITY_GATE_TRACE_STAGE else 0
        normalized_focus_terms = [term.lower() for term in (focus_terms or []) if term]
        for file_path in self._iter_text_files(repo_path):
            relative_path = file_path.relative_to(repo_path)
            path_text = str(relative_path).lower()
            path_score = sum(10 for token in tokens if token in path_text)
            if normalized_focus_terms and any(hint in path_text for hint in DEPENDENCY_PATH_HINTS):
                path_score += 18
            try:
                lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            except OSError:
                continue
            file_symbols = self._collect_symbols(lines)
            symbol_score = sum(16 for token in tokens if token in file_symbols)
            symbol_score += sum(20 for term in normalized_focus_terms if term in file_symbols)
            scored_lines: list[tuple[int, int]] = []
            for index, line in enumerate(lines, start=1):
                lowered = line.lower()
                score = sum(3 + min(lowered.count(token), 3) for token in tokens if token in lowered)
                line_symbols = self._line_symbols(lowered)
                score += sum(12 for token in tokens if token in line_symbols)
                score += sum(16 for term in normalized_focus_terms if term in line_symbols)
                score += sum(45 for term in normalized_focus_terms if term in lowered)
                if self._is_declaration_line(line):
                    score += sum(8 for token in tokens if token in lowered or token in line_symbols)
                    score += sum(10 for term in normalized_focus_terms if term in lowered or term in line_symbols)
                if PATHISH_PATTERN.search(line):
                    score += sum(6 for token in tokens if token in lowered)
                if score:
                    proximity_bonus = self._keyword_proximity_bonus(lowered, tokens)
                    scored_lines.append((index, score + path_score + symbol_score + repo_score + proximity_bonus + trace_stage_bonus))
            if not scored_lines and (path_score or symbol_score or repo_score):
                scored_lines.append((1, path_score + symbol_score + repo_score + trace_stage_bonus))
            if not scored_lines:
                continue
            best_line, best_score = max(scored_lines, key=lambda item: item[1])
            start, end = self._best_snippet_window(lines, best_line)
            snippet = "\n".join(lines[start - 1 : end]).strip()
            matches.append(
                {
                    "repo": entry.display_name,
                    "path": str(relative_path),
                    "line_start": start,
                    "line_end": end,
                    "score": best_score,
                    "snippet": snippet[:2400],
                    "reason": self._match_reason(
                        tokens,
                        path_text,
                        snippet,
                        file_symbols=file_symbols,
                        question=question,
                        focus_terms=normalized_focus_terms,
                        trace_stage=trace_stage,
                    ),
                    "trace_stage": trace_stage,
                }
            )
        return matches

    def _iter_text_files(self, repo_path: Path):
        for path in repo_path.rglob("*"):
            if not path.is_file():
                continue
            if any(part in SKIP_DIRS for part in path.relative_to(repo_path).parts):
                continue
            if path.stat().st_size > self.max_file_bytes:
                continue
            if path.suffix.lower() not in TEXT_SUFFIXES and path.name not in {"Dockerfile", "Makefile"}:
                continue
            yield path

    def _repo_path(self, key: str, entry: RepositoryEntry) -> Path:
        digest = hashlib.sha1(f"{key}:{entry.url}".encode("utf-8")).hexdigest()[:12]
        slug = re.sub(r"[^a-zA-Z0-9_.-]+", "-", entry.display_name).strip("-")[:48] or "repo"
        return self.repo_root / self._safe_key(key) / f"{slug}-{digest}"

    @staticmethod
    def _safe_key(key: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_.-]+", "-", key).strip("-")

    def _normalize_entry(self, raw: dict[str, Any]) -> RepositoryEntry:
        if not isinstance(raw, dict):
            raise ToolError("Each repository entry must include a display name and HTTPS URL.")
        url = str(raw.get("url") or "").strip()
        if not url:
            raise ToolError("Repository HTTPS URL cannot be empty.")
        if not HTTPS_URL_PATTERN.match(url):
            raise ToolError("Only HTTPS clone URLs are supported, for example https://gitlab.example.com/group/repo.git.")
        display_name = str(raw.get("display_name") or "").strip() or self._derive_display_name(url)
        return RepositoryEntry(display_name=display_name[:80], url=url)

    @staticmethod
    def _entry_to_dict(entry: RepositoryEntry | dict[str, Any]) -> dict[str, str]:
        if isinstance(entry, RepositoryEntry):
            return {"display_name": entry.display_name, "url": entry.url}
        display_name = str(entry.get("display_name") or "").strip()
        url = str(entry.get("url") or "").strip()
        return {"display_name": display_name or SourceCodeQAService._derive_display_name(url), "url": url}

    @staticmethod
    def _derive_display_name(url: str) -> str:
        tail = url.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]
        return tail.removesuffix(".git") or "Repository"

    def _authenticated_git_url(self, url: str) -> str:
        parts = urlsplit(url)
        netloc = f"{self.gitlab_username}:{self.gitlab_token}@{parts.netloc}"
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))

    def _sanitize_error_detail(self, detail: str) -> str:
        sanitized = str(detail or "")
        if self.gitlab_token:
            sanitized = sanitized.replace(self.gitlab_token, "***")
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", sanitized)

    @staticmethod
    def _remove_incomplete_repo_dir(repo_path: Path) -> None:
        shutil.rmtree(repo_path, ignore_errors=True)

    @staticmethod
    def _question_tokens(question: str) -> list[str]:
        lowered_question = question.lower()
        raw_tokens = re.findall(r"[a-zA-Z0-9_./:-]{1,}", lowered_question)
        tokens = []
        for token in raw_tokens:
            token = token.strip("./:-_")
            if (len(token) < 2 and not token.isdigit()) or token in STOPWORDS:
                continue
            if token not in tokens:
                tokens.append(token)

        # Business questions often use spaced phrases while code uses camelCase or
        # suffix numbers, e.g. "Term Loan Pre Check 1" -> "termloanprecheck1".
        words = [token for token in tokens if re.fullmatch(r"[a-z0-9]+", token)]
        phrase_variants: list[str] = []
        for size in (2, 3, 4, 5):
            for index in range(0, max(0, len(words) - size + 1)):
                compact = "".join(words[index : index + size])
                if len(compact) >= 4:
                    phrase_variants.append(compact)
        if words:
            compact_all = "".join(words[:8])
            if len(compact_all) >= 4:
                phrase_variants.append(compact_all)
        for variant in phrase_variants:
            if variant not in tokens:
                tokens.append(variant)
        return tokens[:28]

    @staticmethod
    def _match_reason(
        tokens: list[str],
        path_text: str,
        snippet: str,
        *,
        file_symbols: set[str],
        question: str,
        focus_terms: list[str] | None = None,
        trace_stage: str = "direct",
    ) -> str:
        snippet_text = snippet.lower()
        path_hits = [token for token in tokens if token in path_text]
        content_hits = [token for token in tokens if token in snippet_text]
        symbol_hits = [token for token in tokens if token in file_symbols]
        focus_hits = [term for term in (focus_terms or []) if term in snippet_text or term in path_text or term in file_symbols]
        parts = []
        if trace_stage == "dependency":
            parts.append("dependency trace")
        elif trace_stage == "two_hop":
            parts.append("two-hop trace")
        elif trace_stage == "query_decomposition":
            parts.append("query decomposition")
        elif trace_stage.startswith(TOOL_LOOP_TRACE_PREFIX):
            parts.append("planner tool trace")
        elif trace_stage.startswith("agent_trace"):
            parts.append("agent trace")
        elif trace_stage.startswith("agent_plan"):
            parts.append("agent plan trace")
        elif trace_stage == QUALITY_GATE_TRACE_STAGE:
            parts.append("quality-gate trace")
        if path_hits:
            parts.append(f"path matched: {', '.join(path_hits[:4])}")
        if symbol_hits:
            parts.append(f"symbol matched: {', '.join(symbol_hits[:4])}")
        if focus_hits:
            parts.append(f"downstream hit: {', '.join(focus_hits[:4])}")
        if content_hits:
            parts.append(f"content matched: {', '.join(content_hits[:4])}")
        if not parts and question:
            parts.append("best semantic filename/content similarity")
        return "; ".join(parts) or "filename/content similarity"

    @staticmethod
    def _is_dependency_question(question: str) -> bool:
        lowered = question.lower()
        return any(term in lowered for term in DEPENDENCY_QUESTION_TERMS)

    def _expand_dependency_matches(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        base_matches: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        focus_terms = self._dependency_focus_terms(question, base_matches)
        if not focus_terms:
            return []
        matches: list[dict[str, Any]] = []
        expanded_tokens = []
        for term in focus_terms:
            expanded_tokens.extend(self._question_tokens(term))
        expanded_tokens = list(dict.fromkeys(expanded_tokens))[:16]
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            matches.extend(
                self._search_repo(
                    entry,
                    repo_path,
                    expanded_tokens,
                    question=question,
                    focus_terms=focus_terms,
                    trace_stage="dependency",
                )
            )
        matches.sort(key=lambda item: item["score"], reverse=True)
        return matches[: max(4, min(int(limit or 12), 16))]

    def _expand_two_hop_matches(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        base_matches: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        trace_terms = self._two_hop_trace_terms(question, base_matches)
        if not trace_terms:
            return []
        expanded_tokens: list[str] = []
        for term in trace_terms:
            expanded_tokens.extend(self._question_tokens(term))
        expanded_tokens = list(dict.fromkeys(expanded_tokens))[:24]

        matches: list[dict[str, Any]] = []
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            matches.extend(
                self._search_repo(
                    entry,
                    repo_path,
                    expanded_tokens,
                    question=question,
                    focus_terms=trace_terms,
                    trace_stage="two_hop",
                )
            )
        matches.sort(key=lambda item: item["score"], reverse=True)
        return matches[: max(4, min(int(limit or 12), 18))]

    def _expand_agent_trace_matches(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        base_matches: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        all_matches: list[dict[str, Any]] = []
        frontier = list(base_matches[:10])
        seen_terms: set[str] = set()
        seen_match_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in base_matches}
        max_rounds = 3
        for round_index in range(1, max_rounds + 1):
            trace_terms = self._agent_trace_terms(question, frontier, seen_terms)
            if not trace_terms:
                break
            seen_terms.update(trace_terms)
            expanded_tokens: list[str] = []
            for term in trace_terms:
                expanded_tokens.extend(self._question_tokens(term))
            expanded_tokens = list(dict.fromkeys(expanded_tokens))[:24]
            round_matches: list[dict[str, Any]] = []
            for entry in entries:
                repo_path = self._repo_path(key, entry)
                if not (repo_path / ".git").exists():
                    continue
                round_matches.extend(
                    self._search_repo(
                        entry,
                        repo_path,
                        expanded_tokens,
                        question=question,
                        focus_terms=trace_terms,
                        trace_stage=f"agent_trace_{round_index}",
                    )
                )
            round_matches.sort(key=lambda item: item["score"], reverse=True)
            next_frontier: list[dict[str, Any]] = []
            for item in round_matches[: max(6, min(int(limit or 12), 18))]:
                item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                if item_key in seen_match_keys:
                    continue
                all_matches.append(item)
                next_frontier.append(item)
                seen_match_keys.add(item_key)
            frontier = next_frontier
            if not frontier:
                break
        all_matches.sort(key=lambda item: item["score"], reverse=True)
        return all_matches[: max(6, min(int(limit or 12) * 2, 24))]

    def _expand_quality_gate_matches(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        base_matches: list[dict[str, Any]],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        limit: int,
    ) -> list[dict[str, Any]]:
        trace_terms = self._quality_gate_trace_terms(question, evidence_summary, quality_gate, base_matches)
        if not trace_terms:
            return []
        expanded_tokens: list[str] = []
        for term in trace_terms:
            expanded_tokens.extend(self._question_tokens(term))
        expanded_tokens = list(dict.fromkeys(expanded_tokens))[:28]

        matches: list[dict[str, Any]] = []
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            matches.extend(
                self._search_repo(
                    entry,
                    repo_path,
                    expanded_tokens,
                    question=question,
                    focus_terms=trace_terms,
                    trace_stage=QUALITY_GATE_TRACE_STAGE,
                )
            )
        matches.sort(key=lambda item: item["score"], reverse=True)
        return matches[: max(6, min(int(limit or 12), 18))]

    def _run_planner_tool_loop(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        base_matches: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        current_matches = list(base_matches)
        seen = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in current_matches}
        executed_steps: set[str] = set()
        empty_rounds = 0
        max_rounds = 5
        for step_index in range(1, max_rounds + 1):
            evidence_summary = self._compress_evidence(question, current_matches)
            quality_gate = self._quality_gate(question, evidence_summary)
            step = self._choose_next_tool_step(
                question=question,
                matches=current_matches,
                evidence_summary=evidence_summary,
                quality_gate=quality_gate,
                executed_steps=executed_steps,
            )
            if not step:
                break
            step_signature = self._tool_step_signature(step, current_matches)
            executed_steps.add(step_signature)
            step_matches = self._execute_tool_loop_step(
                entries=entries,
                key=key,
                question=question,
                matches=current_matches,
                step=step,
                step_index=step_index,
            )
            step_matches.sort(key=lambda item: item["score"], reverse=True)
            added = 0
            for item in step_matches[: max(5, min(int(limit or 12), 18))]:
                item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                if item_key in seen:
                    self._annotate_duplicate_tool_match(current_matches, item)
                    continue
                collected.append(item)
                current_matches.append(item)
                seen.add(item_key)
                added += 1
            current_matches.sort(key=lambda item: item["score"], reverse=True)
            current_matches = self._select_result_matches(current_matches, max(1, min(int(limit or 12), 30)))
            empty_rounds = empty_rounds + 1 if added == 0 else 0
            if self._should_stop_tool_loop(question, current_matches, step_index, empty_rounds):
                break
        collected.sort(key=lambda item: item["score"], reverse=True)
        return collected[: max(6, min(int(limit or 12) * 2, 24))]

    @staticmethod
    def _annotate_duplicate_tool_match(existing_matches: list[dict[str, Any]], duplicate: dict[str, Any]) -> None:
        duplicate_key = (
            duplicate.get("repo"),
            duplicate.get("path"),
            duplicate.get("line_start"),
            duplicate.get("line_end"),
        )
        for existing in existing_matches:
            existing_key = (
                existing.get("repo"),
                existing.get("path"),
                existing.get("line_start"),
                existing.get("line_end"),
            )
            if existing_key != duplicate_key:
                continue
            duplicate_retrieval = str(duplicate.get("retrieval") or "")
            existing_retrieval = str(existing.get("retrieval") or "file_scan")
            if duplicate_retrieval and duplicate_retrieval != existing_retrieval:
                chain = existing.setdefault("retrieval_chain", [])
                for retrieval in (existing_retrieval, duplicate_retrieval):
                    if retrieval and retrieval not in chain:
                        chain.append(retrieval)
                if duplicate_retrieval in {"flow_graph", "code_graph", "entity_graph"}:
                    existing["retrieval"] = duplicate_retrieval
            duplicate_reason = str(duplicate.get("reason") or "")
            if duplicate_reason and duplicate_reason not in str(existing.get("reason") or ""):
                existing["reason"] = f"{existing.get('reason')}; corroborated by {duplicate_reason}"
            duplicate_stage = str(duplicate.get("trace_stage") or "")
            existing_stage = str(existing.get("trace_stage") or "")
            if duplicate_stage and duplicate_stage not in {"direct", "query_decomposition"} and existing_stage in {"direct", "query_decomposition"}:
                existing["trace_stage"] = duplicate_stage
            existing["score"] = max(int(existing.get("score") or 0), int(duplicate.get("score") or 0))
            return

    def _choose_next_tool_step(
        self,
        *,
        question: str,
        matches: list[dict[str, Any]],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        executed_steps: set[str],
    ) -> dict[str, Any] | None:
        terms = self._tool_loop_terms(question, matches)
        quality_terms = self._quality_gate_trace_terms(question, evidence_summary, quality_gate, matches)
        intent = evidence_summary.get("intent") or self._question_intent(question)
        candidates: list[dict[str, Any]] = []

        if intent.get("data_source"):
            candidates.extend(
                [
                    {"tool": "trace_entity", "terms": terms[:12]},
                    {"tool": "trace_flow", "terms": terms[:12]},
                    {"tool": "trace_graph", "terms": terms[:12]},
                    {"tool": "search_code", "terms": [*quality_terms, "repository", "mapper", "dao", "select", "from", "client"]},
                ]
            )
        if intent.get("api"):
            candidates.extend(
                [
                    {"tool": "trace_entity", "terms": terms[:12]},
                    {"tool": "trace_flow", "terms": terms[:12]},
                    {"tool": "find_references", "terms": [*terms[:10], "RequestMapping", "PostMapping", "GetMapping"]},
                    {"tool": "search_code", "terms": [*terms[:8], "controller", "endpoint", "client"]},
                ]
            )
        if intent.get("config"):
            candidates.append({"tool": "search_code", "terms": [*terms[:8], *quality_terms, "properties", "yaml", "configuration"]})
        if intent.get("rule_logic") or intent.get("error"):
            candidates.append({"tool": "search_code", "terms": [*terms[:8], *quality_terms, "validate", "rule", "exception"]})

        candidates.extend(self._build_tool_loop_plan(question, matches))
        if terms:
            candidates.append({"tool": "trace_flow", "terms": terms[:12]})

        for candidate in candidates:
            normalized_terms = list(
                dict.fromkeys(str(term).strip() for term in candidate.get("terms") or [] if str(term).strip())
            )
            tool = str(candidate.get("tool") or "")
            if tool not in {"find_definition", "find_references", "trace_graph", "trace_flow", "trace_entity", "search_code"}:
                continue
            if tool in {"find_definition", "find_references", "search_code"} and not normalized_terms:
                continue
            step = {"tool": tool, "terms": normalized_terms[:18]}
            signature = self._tool_step_signature(step, matches)
            if signature not in executed_steps:
                return step
        return None

    @staticmethod
    def _tool_step_signature(step: dict[str, Any], matches: list[dict[str, Any]]) -> str:
        tool = str(step.get("tool") or "")
        terms = ",".join(str(term).lower() for term in (step.get("terms") or [])[:10])
        if tool in {"trace_flow", "trace_graph", "trace_entity"}:
            seed_paths = ",".join(str(match.get("path") or "") for match in matches[:10])
            return f"{tool}:{seed_paths}:{terms}"
        return f"{tool}:{terms}"

    def _execute_tool_loop_step(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        matches: list[dict[str, Any]],
        step: dict[str, Any],
        step_index: int,
    ) -> list[dict[str, Any]]:
        tool = str(step.get("tool") or "")
        terms = [str(term) for term in (step.get("terms") or []) if str(term)]
        step_matches: list[dict[str, Any]] = []
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            if tool == "find_definition":
                step_matches.extend(self._tool_find_definition(entry, repo_path, terms, question, step_index))
            elif tool == "find_references":
                step_matches.extend(self._tool_find_references(entry, repo_path, terms, question, step_index))
            elif tool == "trace_graph":
                step_matches.extend(self._tool_trace_graph(entry, repo_path, matches, question, step_index))
            elif tool == "trace_flow":
                step_matches.extend(self._tool_trace_flow(entry, repo_path, matches, question, step_index))
            elif tool == "trace_entity":
                step_matches.extend(self._tool_trace_entity(entry, repo_path, matches, question, step_index))
            elif tool == "search_code":
                expanded_tokens: list[str] = []
                for term in terms:
                    expanded_tokens.extend(self._question_tokens(term))
                step_matches.extend(
                    self._search_repo(
                        entry,
                        repo_path,
                        list(dict.fromkeys(expanded_tokens))[:30],
                        question=question,
                        focus_terms=terms,
                        trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                    )
                )
        return step_matches

    def _should_stop_tool_loop(
        self,
        question: str,
        matches: list[dict[str, Any]],
        step_index: int,
        empty_rounds: int,
    ) -> bool:
        if empty_rounds >= 2:
            return True
        evidence_summary = self._compress_evidence(question, matches)
        quality_gate = self._quality_gate(question, evidence_summary)
        if quality_gate.get("status") != "sufficient" or step_index < 2:
            return False
        if evidence_summary.get("intent", {}).get("data_source"):
            return bool(evidence_summary.get("data_sources"))
        return True

    def _build_tool_loop_plan(self, question: str, base_matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        intent = self._question_intent(question)
        terms = self._tool_loop_terms(question, base_matches)
        plan: list[dict[str, Any]] = []
        if terms:
            plan.append({"tool": "find_definition", "terms": terms[:12]})
            plan.append({"tool": "find_references", "terms": terms[:12]})
        if intent.get("data_source") or intent.get("api") or intent.get("rule_logic"):
            plan.append({"tool": "trace_entity", "terms": terms[:12]})
            plan.append({"tool": "trace_flow", "terms": terms[:12]})
            plan.append({"tool": "trace_graph", "terms": terms[:12]})
        if intent.get("config"):
            plan.append({"tool": "search_code", "terms": [*terms[:8], "properties", "configuration", "yaml", "feature"]})
        if intent.get("data_source"):
            plan.append({"tool": "search_code", "terms": [*terms[:8], "repository", "mapper", "select", "from", "client"]})
        return plan[:5]

    def _tool_loop_terms(self, question: str, base_matches: list[dict[str, Any]]) -> list[str]:
        terms = list(self._question_tokens(question))
        for match in base_matches[:8]:
            terms.extend(IDENTIFIER_PATTERN.findall(str(match.get("path") or "")))
            terms.extend(self._extract_downstream_symbols(str(match.get("snippet") or "")))
            terms.extend(self._extract_assignment_sources(str(match.get("snippet") or "")))
        deduped = []
        for term in terms:
            lowered = str(term or "").strip().lower()
            if len(lowered) < 3 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
                continue
            if lowered not in deduped:
                deduped.append(lowered)
        return deduped[:20]

    def _tool_find_definition(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        question: str,
        step_index: int,
    ) -> list[dict[str, Any]]:
        return self._tool_lookup_structure(
            entry,
            repo_path,
            terms,
            question=question,
            table="definitions",
            name_column="name",
            lower_column="lower_name",
            line_column="line_no",
            kind_column="kind",
            trace_stage=f"tool_loop_{step_index}",
            retrieval="planner_definition",
        )

    def _tool_find_references(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        question: str,
        step_index: int,
    ) -> list[dict[str, Any]]:
        return self._tool_lookup_structure(
            entry,
            repo_path,
            terms,
            question=question,
            table="references_index",
            name_column="target",
            lower_column="lower_target",
            line_column="line_no",
            kind_column="kind",
            trace_stage=f"tool_loop_{step_index}",
            retrieval="planner_reference",
        )

    def _tool_lookup_structure(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        *,
        question: str,
        table: str,
        name_column: str,
        lower_column: str,
        line_column: str,
        kind_column: str,
        trace_stage: str,
        retrieval: str,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        index_path = self._index_path(repo_path)
        try:
            self._ensure_repo_index(key=None, entry=entry, repo_path=repo_path)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                for term in terms[:16]:
                    lowered = str(term).lower()
                    if len(lowered) < 3:
                        continue
                    for row in connection.execute(
                        f"select * from {table} where {lower_column} like ? limit 20",
                        (f"%{lowered}%",),
                    ):
                        matches.append(
                            self._match_from_index_location(
                                entry,
                                connection,
                                str(row["file_path"]),
                                int(row[line_column]),
                                score=170 if str(row[lower_column]) == lowered else 132,
                                reason=f"planner {retrieval}: {row[kind_column]} {row[name_column]}",
                                question=question,
                                trace_stage=trace_stage,
                                retrieval=retrieval,
                            )
                        )
        except (OSError, sqlite3.Error):
            return []
        return [match for match in matches if match]

    def _tool_trace_graph(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        base_matches: list[dict[str, Any]],
        question: str,
        step_index: int,
    ) -> list[dict[str, Any]]:
        index_path = self._index_path(repo_path)
        seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:8]
        matches: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index(key=None, entry=entry, repo_path=repo_path)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                for path in seed_paths:
                    for row in connection.execute(
                        """
                        select * from graph_edges
                        where from_file = ? or to_file = ?
                        limit 30
                        """,
                        (path, path),
                    ):
                        target_path = str(row["to_file"] if row["from_file"] == path else row["from_file"])
                        target_line = int(row["to_line"] if row["from_file"] == path else row["from_line"])
                        matches.append(
                            self._match_from_index_location(
                                entry,
                                connection,
                                target_path,
                                target_line,
                                score=150,
                                reason=f"planner graph trace: {row['edge_kind']} {row['symbol']}",
                                question=question,
                                trace_stage=f"tool_loop_{step_index}",
                                retrieval="code_graph",
                            )
                        )
        except (OSError, sqlite3.Error):
            return []
        return [match for match in matches if match]

    def _tool_trace_flow(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        base_matches: list[dict[str, Any]],
        question: str,
        step_index: int,
    ) -> list[dict[str, Any]]:
        index_path = self._index_path(repo_path)
        seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
        matches: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index(key=None, entry=entry, repo_path=repo_path)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                for path in seed_paths:
                    for row in connection.execute(
                        """
                        select * from flow_edges
                        where from_file = ? or to_file = ?
                        order by
                            case edge_kind
                                when 'sql_table' then 0
                                when 'repository' then 1
                                when 'mapper' then 2
                                when 'dao' then 3
                                when 'field_population' then 4
                                when 'client' then 5
                                when 'service' then 6
                                else 6
                            end,
                            from_line
                        limit 40
                        """,
                        (path, path),
                    ):
                        target_path = str(row["to_file"] or "")
                        target_line = int(row["to_line"] or 0)
                        if target_path and target_path != path:
                            matches.append(
                                self._match_from_index_location(
                                    entry,
                                    connection,
                                    target_path,
                                    target_line,
                                    score=172 if row["edge_kind"] in {"repository", "mapper", "dao", "sql_table"} else 158,
                                    reason=f"planner flow trace: {row['edge_kind']} {row['to_name']}",
                                    question=question,
                                    trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                                    retrieval="flow_graph",
                                )
                            )
                        else:
                            matches.append(
                                self._match_from_index_location(
                                    entry,
                                    connection,
                                    str(row["from_file"]),
                                    int(row["from_line"]),
                                    score=166 if row["edge_kind"] == "sql_table" else 148,
                                    reason=f"planner flow trace: {row['edge_kind']} {row['to_name']}",
                                    question=question,
                                    trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                                    retrieval="flow_graph",
                                )
                            )
        except (OSError, sqlite3.Error):
            return []
        return [match for match in matches if match]

    def _tool_trace_entity(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        base_matches: list[dict[str, Any]],
        question: str,
        step_index: int,
    ) -> list[dict[str, Any]]:
        index_path = self._index_path(repo_path)
        seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
        matches: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index(key=None, entry=entry, repo_path=repo_path)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                for path in seed_paths:
                    for row in connection.execute(
                        """
                        select * from entity_edges
                        where from_file = ? or to_file = ?
                        order by
                            case edge_kind
                                when 'sql_table' then 0
                                when 'route' then 1
                                when 'data_flow' then 2
                                when 'injects' then 3
                                when 'call' then 4
                                else 4
                            end,
                            from_line
                        limit 50
                        """,
                        (path, path),
                    ):
                        target_path = str(row["to_file"] or "")
                        target_line = int(row["to_line"] or 0)
                        if target_path and target_path != path:
                            matches.append(
                                self._match_from_index_location(
                                    entry,
                                    connection,
                                    target_path,
                                    target_line,
                                    score=176 if row["edge_kind"] in {"sql_table", "injects", "call"} else 154,
                                    reason=f"planner entity trace: {row['edge_kind']} {row['to_name']}",
                                    question=question,
                                    trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                                    retrieval="entity_graph",
                                )
                            )
                        else:
                            matches.append(
                                self._match_from_index_location(
                                    entry,
                                    connection,
                                    str(row["from_file"]),
                                    int(row["from_line"]),
                                    score=156,
                                    reason=f"planner entity trace: {row['edge_kind']} {row['to_name']}",
                                    question=question,
                                    trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                                    retrieval="entity_graph",
                                )
                            )
        except (OSError, sqlite3.Error):
            return []
        return [match for match in matches if match]

    def _match_from_index_location(
        self,
        entry: RepositoryEntry,
        connection: sqlite3.Connection,
        file_path: str,
        line_no: int,
        *,
        score: int,
        reason: str,
        question: str,
        trace_stage: str,
        retrieval: str,
    ) -> dict[str, Any] | None:
        rows = connection.execute(
            "select line_text from lines where file_path = ? order by line_no",
            (file_path,),
        ).fetchall()
        lines = [str(row["line_text"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows]
        if not lines:
            return None
        start, end = self._best_snippet_window(lines, max(1, min(line_no, len(lines))))
        return {
            "repo": entry.display_name,
            "path": file_path,
            "line_start": start,
            "line_end": end,
            "score": score,
            "snippet": "\n".join(lines[start - 1 : end]).strip()[:2400],
            "reason": reason,
            "trace_stage": trace_stage,
            "retrieval": retrieval,
        }

    def _build_trace_paths(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        matches: list[dict[str, Any]],
        question: str,
        limit: int = 6,
    ) -> list[dict[str, Any]]:
        del question
        if not matches:
            return []
        paths: list[dict[str, Any]] = []
        seen_signatures: set[str] = set()
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            seed_paths = [str(match.get("path") or "") for match in matches if match.get("repo") == entry.display_name][:10]
            if not seed_paths:
                continue
            try:
                self._ensure_repo_index(key=None, entry=entry, repo_path=repo_path)
                with sqlite3.connect(self._index_path(repo_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    for seed in seed_paths:
                        first_hops = self._trace_path_edges_for_seed(connection, seed)
                        for first in first_hops[:10]:
                            path = self._trace_path_from_edges(entry.display_name, seed, [first])
                            signature = json.dumps(path.get("edges") or [], sort_keys=True)
                            if signature not in seen_signatures:
                                paths.append(path)
                                seen_signatures.add(signature)
                            next_seed = str(first.get("to_file") or "")
                            if not next_seed:
                                continue
                            for second in self._trace_path_edges_for_seed(connection, next_seed)[:6]:
                                if second.get("from_file") == first.get("from_file") and second.get("to_file") == first.get("to_file"):
                                    continue
                                extended = self._trace_path_from_edges(entry.display_name, seed, [first, second])
                                signature = json.dumps(extended.get("edges") or [], sort_keys=True)
                                if signature not in seen_signatures:
                                    paths.append(extended)
                                    seen_signatures.add(signature)
            except (OSError, sqlite3.Error):
                continue
        paths.sort(key=lambda item: item.get("confidence", 0), reverse=True)
        return paths[: max(1, int(limit or 6))]

    def _build_repo_dependency_graph(self, *, key: str, entries: list[RepositoryEntry]) -> dict[str, Any]:
        nodes = [{"name": entry.display_name, "url": entry.url} for entry in entries]
        edge_rows: list[dict[str, Any]] = []
        for source in entries:
            source_path = self._repo_path(key, source)
            if not (source_path / ".git").exists():
                continue
            try:
                self._ensure_repo_index(key=None, entry=source, repo_path=source_path)
                with sqlite3.connect(self._index_path(source_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    rows = connection.execute(
                        """
                        select edge_kind, to_name, evidence, from_file, from_line
                        from flow_edges
                        where edge_kind in ('client', 'route', 'framework')
                        limit 300
                        """
                    ).fetchall()
            except (OSError, sqlite3.Error):
                continue
            for row in rows:
                target = self._match_repo_dependency_target(str(row["to_name"] or row["evidence"] or ""), entries, source.display_name)
                if not target:
                    continue
                edge_rows.append(
                    {
                        "from_repo": source.display_name,
                        "to_repo": target.display_name,
                        "edge_kind": str(row["edge_kind"] or "dependency"),
                        "evidence": str(row["evidence"] or row["to_name"] or "")[:300],
                        "from_file": str(row["from_file"] or ""),
                        "from_line": int(row["from_line"] or 0),
                    }
                )
        deduped = list({json.dumps(edge, sort_keys=True): edge for edge in edge_rows}.values())
        return {"nodes": nodes, "edges": deduped[:80]}

    @staticmethod
    def _match_repo_dependency_target(value: str, entries: list[RepositoryEntry], source_name: str) -> RepositoryEntry | None:
        normalized_value = re.sub(r"[^a-z0-9]+", "", str(value or "").lower())
        if not normalized_value:
            return None
        for entry in entries:
            if entry.display_name == source_name:
                continue
            candidates = {
                re.sub(r"[^a-z0-9]+", "", entry.display_name.lower()),
                re.sub(r"[^a-z0-9]+", "", SourceCodeQAService._derive_display_name(entry.url).lower()),
            }
            for candidate in candidates:
                if len(candidate) >= 4 and (candidate in normalized_value or normalized_value in candidate):
                    return entry
        return None

    @staticmethod
    def _trace_path_edges_for_seed(connection: sqlite3.Connection, seed_path: str) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            select * from flow_edges
            where from_file = ? or to_file = ?
            order by
                case edge_kind
                    when 'sql_table' then 0
                    when 'client' then 1
                    when 'mapper' then 2
                    when 'dao' then 3
                    when 'repository' then 4
                    when 'field_population' then 5
                    when 'service' then 6
                    when 'route' then 7
                    else 7
                end,
                from_line
            limit 30
            """,
            (seed_path, seed_path),
        ).fetchall()
        edges: list[dict[str, Any]] = []
        for row in rows:
            edge = dict(row)
            if edge.get("to_file") == seed_path and edge.get("from_file") != seed_path:
                edge = {
                    **edge,
                    "from_file": edge.get("to_file"),
                    "from_line": edge.get("to_line"),
                    "to_file": edge.get("from_file"),
                    "to_line": edge.get("from_line"),
                    "to_name": edge.get("from_name"),
                    "evidence": f"reverse trace: {edge.get('evidence')}",
                }
            edges.append(edge)
        return edges

    @staticmethod
    def _trace_path_from_edges(repo_name: str, seed_path: str, edges: list[dict[str, Any]]) -> dict[str, Any]:
        nodes: list[dict[str, Any]] = [{"path": seed_path, "kind": SourceCodeQAService._flow_role_for_path(seed_path), "name": SourceCodeQAService._flow_name_for_path(seed_path)}]
        normalized_edges: list[dict[str, Any]] = []
        confidence = 0
        for edge in edges:
            edge_kind = str(edge.get("edge_kind") or "call")
            to_file = str(edge.get("to_file") or "")
            to_name = str(edge.get("to_name") or "")
            node = {
                "path": to_file,
                "line": int(edge.get("to_line") or 0),
                "kind": SourceCodeQAService._flow_role_for_path(to_file) if to_file else edge_kind,
                "name": to_name or SourceCodeQAService._flow_name_for_path(to_file),
            }
            nodes.append(node)
            normalized_edges.append(
                {
                    "from_file": edge.get("from_file"),
                    "from_line": edge.get("from_line"),
                    "edge_kind": edge_kind,
                    "to_name": to_name,
                    "to_file": to_file,
                    "to_line": edge.get("to_line"),
                    "evidence": edge.get("evidence"),
                }
            )
            confidence += {
                "route": 20,
                "service": 18,
                "repository": 24,
                "mapper": 24,
                "dao": 24,
                "sql_table": 70,
                "client": 55,
                "field_population": 26,
                "framework": 16,
            }.get(edge_kind, 10)
        return {
            "repo": repo_name,
            "seed_path": seed_path,
            "nodes": nodes,
            "edges": normalized_edges,
            "confidence": confidence,
            "missing_hop": "" if edges else "no graph edge found from seed",
        }

    def _build_agent_plan(
        self,
        question: str,
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
    ) -> dict[str, Any]:
        intent = evidence_summary.get("intent") or self._question_intent(question)
        steps: list[dict[str, Any]] = []
        if intent.get("data_source"):
            steps.extend(
                [
                    {
                        "name": "trace_data_carriers",
                        "purpose": "Find DTO/context/result objects that carry the requested data.",
                        "terms": ["DataSourceResult", "UnderwritingContext", "UnderwritingInitiationDTO", "Input", "DTO", "Record", "Result"],
                    },
                    {
                        "name": "trace_field_population",
                        "purpose": "Trace DTO fields backward to provider, builder, converter, and setter/getter code.",
                        "terms": [
                            "UnderwritingInitiationProvider", "UnderwritingInitiationDTO",
                            "UnderwritingBasicInfo", "CustomerInfo", "LoanInfo", "CreditRiskInfo",
                            "setCustomerInfo", "setLoanInfo", "setCreditRiskInfo",
                            "getCustomerInfo", "getLoanInfo", "getCreditRiskInfo",
                            "populate", "build", "provider", "converter",
                        ],
                    },
                    {
                        "name": "trace_downstream_sources",
                        "purpose": "Follow services into repository, mapper, integration, client, SQL, or API calls.",
                        "terms": ["Repository", "Mapper", "Dao", "DAO", "jdbcTemplate", "queryForObject", "select", "Integration", "Client"],
                    },
                    {
                        "name": "trace_dao_mapper_methods",
                        "purpose": "Open DAO/Mapper classes and XML/SQL mappings to find concrete table, query, or upstream API source.",
                        "terms": [
                            "CustomerInfoDAO", "CustomerInfoDao", "CustomerInfoMapper",
                            "LoanInfoDAO", "LoanInfoDao", "LoanInfoMapper",
                            "CreditRiskInfoDAO", "CreditRiskInfoDao", "CreditRiskInfoMapper",
                            "UnderwritingBasicInfoDAO", "UnderwritingBasicInfoMapper",
                            "select", "from", "resultMap", "namespace", "queryForObject",
                        ],
                    },
                ]
            )
        if intent.get("api"):
            steps.append(
                {
                    "name": "trace_api_flow",
                    "purpose": "Find controllers, API clients, request mappings, and endpoint calls.",
                    "terms": ["Controller", "RequestMapping", "PostMapping", "GetMapping", "Client", "Endpoint"],
                }
            )
        if intent.get("config"):
            steps.append(
                {
                    "name": "trace_config",
                    "purpose": "Find properties, YAML, feature flags, and configuration classes.",
                    "terms": ["Configuration", "Properties", "yaml", "feature", "config"],
                }
            )
        if intent.get("rule_logic") or intent.get("error"):
            steps.append(
                {
                    "name": "trace_decision_logic",
                    "purpose": "Find validations, rule branches, error handling, or approval logic.",
                    "terms": ["validate", "rule", "condition", "exception", "status", "approval"],
                }
            )
        if quality_gate.get("status") != "sufficient":
            steps.append(
                {
                    "name": "fill_quality_gap",
                    "purpose": "Search for the missing evidence reported by the quality gate.",
                    "terms": self._quality_gate_trace_terms(question, evidence_summary, quality_gate, []),
                }
            )
        deduped_steps: list[dict[str, Any]] = []
        seen: set[str] = set()
        for step in steps:
            name = str(step.get("name") or "")
            if not name or name in seen:
                continue
            terms = list(dict.fromkeys(str(term).strip() for term in step.get("terms") or [] if str(term).strip()))
            if not terms:
                continue
            deduped_steps.append({**step, "terms": terms[:16]})
            seen.add(name)
        return {
            "mode": "local_agentic_retrieval",
            "status": "planned" if deduped_steps else "not_needed",
            "steps": deduped_steps[:5],
        }

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
                    )
                )
            step_matches.sort(key=lambda item: item["score"], reverse=True)
            for item in step_matches[: max(6, min(int(limit or 12), 18))]:
                item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                if item_key in seen_keys:
                    continue
                collected.append(item)
                seen_keys.add(item_key)
            collected.sort(key=lambda item: item["score"], reverse=True)
            collected = self._select_result_matches(collected, max(1, min(int(limit or 12), 30)))
            current_summary = self._compress_evidence(question, collected)
            current_gate = self._quality_gate(question, current_summary)
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
                    if symbol in {"CustomerInfo", "LoanInfo", "CreditRiskInfo", "UnderwritingBasicInfo"}:
                        terms.extend([f"set{symbol}", f"get{symbol}"])
        terms.extend(
            [
                "UnderwritingInitiationProvider",
                "UnderwritingInitiationDTO",
                "CustomerInfoDAO",
                "CustomerInfoDao",
                "CustomerInfoMapper",
                "LoanInfoDAO",
                "LoanInfoDao",
                "LoanInfoMapper",
                "CreditRiskInfoDAO",
                "CreditRiskInfoDao",
                "CreditRiskInfoMapper",
                "UnderwritingBasicInfoDAO",
                "UnderwritingBasicInfoMapper",
                "UnderwritingBasicInfo",
                "CustomerInfo",
                "LoanInfo",
                "CreditRiskInfo",
                "setCustomerInfo",
                "setLoanInfo",
                "setCreditRiskInfo",
                "getCustomerInfo",
                "getLoanInfo",
                "getCreditRiskInfo",
                "populate",
                "build",
                "provider",
                "converter",
                "assembler",
            ]
        )
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
                    "underwritingcontext",
                    "underwritingrecord",
                    "customerinfodo",
                    "customerextrainfodto",
                    "userinfo",
                    "featuredata",
                    "integration",
                    "repository",
                    "mapper",
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
                    "underwritingcontext",
                    "underwritinginitiationdto",
                    "customerinfodo",
                    "customerextrainfodto",
                    "userinfo",
                    "featuredata",
                    "dpdunderwriting",
                    "buildcommon",
                    "commonenginestrategy",
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
        return {
            "data_source": any(term in lowered for term in DATA_SOURCE_HINTS),
            "api": any(term in lowered for term in API_HINTS),
            "config": any(term in lowered for term in CONFIG_HINTS),
            "error": any(term in lowered for term in ERROR_HINTS),
            "rule_logic": any(term in lowered for term in RULE_HINTS),
        }

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
            add("api_surface", ["requestmapping", "postmapping", "getmapping", "route", "endpoint", "client", *question_terms])
        if intent.get("data_source"):
            add("source_trace", ["repository", "mapper", "dao", "select", "from", "jdbcTemplate", "client", "integration", *question_terms])
            add("carrier_backtrace", ["provider", "builder", "converter", "assembler", *profile_terms[:16], *question_terms])
        if intent.get("config"):
            add("configuration", ["properties", "yaml", "configuration", "feature", "config", *question_terms])
        if intent.get("rule_logic") or intent.get("error"):
            add("decision_logic", ["validate", "rule", "condition", "exception", "status", "approval", *question_terms])

        return {
            "mode": "query_decomposition",
            "intent": intent,
            "components": components[:5],
        }

    def _rank_matches(self, question: str, matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not matches:
            return []
        ranked = []
        for match in matches:
            enriched = dict(match)
            enriched["rerank_score"] = self._rerank_score(question, enriched)
            ranked.append(enriched)
        ranked.sort(key=lambda item: item.get("rerank_score", item.get("score", 0)), reverse=True)
        return ranked

    def _rerank_score(self, question: str, match: dict[str, Any]) -> int:
        score = int(match.get("score") or 0)
        intent = self._question_intent(question)
        path = str(match.get("path") or "").lower()
        snippet = str(match.get("snippet") or "").lower()
        retrieval = str(match.get("retrieval") or "")
        trace_stage = str(match.get("trace_stage") or "")
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
        if intent.get("api"):
            if any(term in path for term in ("controller", "client", "api")):
                score += 24
            if any(term in snippet for term in ("requestmapping", "postmapping", "getmapping", "endpoint")):
                score += 24
        if intent.get("config"):
            if path.endswith((".properties", ".yaml", ".yml", ".toml", ".conf")):
                score += 30
        if intent.get("rule_logic") or intent.get("error"):
            if any(term in snippet for term in ("validate", "condition", "exception", "approval", "permission")):
                score += 20
        return score

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
            "rule_or_error_logic": [],
            "source_count": len(selected),
        }
        adders = {key: self._limited_fact_adder(summary[key], 12) for key in summary if isinstance(summary.get(key), list)}

        for match in selected:
            label = self._evidence_label(match)
            path = str(match.get("path") or "")
            path_lower = path.lower()
            snippet = str(match.get("snippet") or "")
            snippet_lower = snippet.lower()
            symbols = IDENTIFIER_PATTERN.findall(snippet)

            if match.get("trace_stage") == "direct" or any(hint in path_lower for hint in ("controller", "consumer", "scene", "strategy", "service", "engine")):
                adders["entry_points"](f"{label}: {self._compact_path(path)}")

            for symbol in symbols:
                lowered = symbol.lower()
                if any(lowered.endswith(suffix) for suffix in DATA_CARRIER_SUFFIXES):
                    adders["data_carriers"](f"{symbol} ({label})")
                if any(lowered.endswith(suffix) for suffix in DEPENDENCY_SYMBOL_SUFFIXES):
                    adders["downstream_components"](f"{symbol} ({label})")

            for line in self._interesting_lines(snippet, DATA_SOURCE_HINTS):
                if self._is_concrete_source_line(line):
                    adders["data_sources"](f"{label}: {line}")
            for line in self._interesting_lines(snippet, FIELD_POPULATION_HINTS):
                adders["field_population"](f"{label}: {line}")
            if re.search(r"\bselect\b.+\bfrom\b", snippet_lower, flags=re.IGNORECASE):
                for line in self._interesting_lines(snippet, ("select", " from ")):
                    adders["data_sources"](f"{label}: {line}")
            for line in self._interesting_lines(snippet, API_HINTS + CONFIG_HINTS):
                adders["api_or_config"](f"{label}: {line}")
            for line in self._interesting_lines(snippet, ERROR_HINTS + RULE_HINTS):
                adders["rule_or_error_logic"](f"{label}: {line}")

        return summary

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
    def _is_concrete_source_line(line: str) -> bool:
        lowered = str(line or "").strip().lower()
        if not lowered:
            return False
        if lowered.startswith(("import ", "package ")):
            return False
        if re.match(r"^\s*(private|protected|public)?\s*(final\s+)?[A-Za-z0-9_<>, ?]+\s+[A-Za-z0-9_]*(dao|mapper|repository|client|integration)\s*;?$", str(line or ""), re.IGNORECASE):
            return False
        return any(hint in f" {lowered} " for hint in CONCRETE_SOURCE_HINTS)

    def _quality_gate(self, question: str, evidence_summary: dict[str, Any]) -> dict[str, Any]:
        intent = evidence_summary.get("intent") or self._question_intent(question)
        missing: list[str] = []
        checks: list[str] = []
        score = 0

        if intent.get("data_source"):
            checks.append("data_source")
            if self._has_concrete_source_evidence(evidence_summary):
                score += 3
            else:
                missing.append("concrete upstream source/table/API/repository evidence beyond DTO fields")
            if evidence_summary.get("data_carriers"):
                score += 1
            if evidence_summary.get("field_population"):
                score += 1
            if evidence_summary.get("downstream_components"):
                score += 1

        if intent.get("api"):
            checks.append("api")
            if evidence_summary.get("api_or_config") or evidence_summary.get("downstream_components"):
                score += 2
            else:
                missing.append("endpoint/client/API evidence")

        if intent.get("config"):
            checks.append("config")
            if evidence_summary.get("api_or_config"):
                score += 2
            else:
                missing.append("config/property evidence")

        if intent.get("error") or intent.get("rule_logic"):
            checks.append("logic")
            if evidence_summary.get("rule_or_error_logic") or evidence_summary.get("entry_points"):
                score += 2
            else:
                missing.append("rule/error handling evidence")

        if not checks:
            checks.append("general")
            if evidence_summary.get("entry_points") or evidence_summary.get("downstream_components") or evidence_summary.get("data_sources"):
                score += 2
            else:
                missing.append("specific code evidence")

        status = "sufficient" if not missing and score >= 2 else "needs_more_trace"
        confidence = "high" if status == "sufficient" and score >= 4 else "medium" if status == "sufficient" else "low"
        return {
            "status": status,
            "confidence": confidence,
            "checks": checks,
            "missing": missing,
            "score": score,
        }

    @staticmethod
    def _has_concrete_source_evidence(evidence_summary: dict[str, Any]) -> bool:
        combined = " ".join(str(value) for value in evidence_summary.get("data_sources") or []).lower()
        return any(hint in combined for hint in CONCRETE_SOURCE_HINTS)

    def _quality_gate_trace_terms(
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
                    "provider", "CustomerInfo", "UserInfo", "DataSource",
                    "UnderwritingInitiationProvider", "UnderwritingInitiationDTO",
                    "UnderwritingBasicInfo", "LoanInfo", "CreditRiskInfo",
                    "setCustomerInfo", "setLoanInfo", "setCreditRiskInfo",
                    "getCustomerInfo", "getLoanInfo", "getCreditRiskInfo",
                ]
            )
        if "api" in (quality_gate.get("checks") or []):
            terms.extend(["controller", "requestmapping", "postmapping", "getmapping", "client", "endpoint"])
        if "config" in (quality_gate.get("checks") or []):
            terms.extend(["properties", "yaml", "configuration", "config", "feature"])

        terms.extend(
            self._all_profile_terms(
                "data_carriers",
                "source_terms",
                "field_population_terms",
                "api_terms",
                "config_terms",
                "logic_terms",
            )
        )
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

    def _hybrid_query_bonus(self, question: str, line: str, line_symbols: set[str]) -> int:
        intent = self._question_intent(question)
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
    def _select_result_matches(matches: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        limit = max(1, int(limit or 1))
        buckets = {
            "direct": [match for match in matches if match.get("trace_stage") == "direct"],
            "query_decomposition": [match for match in matches if match.get("trace_stage") == "query_decomposition"],
            "dependency": [match for match in matches if match.get("trace_stage") == "dependency"],
            "two_hop": [match for match in matches if match.get("trace_stage") == "two_hop"],
            "tool_loop": [match for match in matches if str(match.get("trace_stage") or "").startswith(TOOL_LOOP_TRACE_PREFIX)],
            "agent_trace": [match for match in matches if str(match.get("trace_stage") or "").startswith("agent_trace")],
            "agent_plan": [match for match in matches if str(match.get("trace_stage") or "").startswith("agent_plan")],
            "quality_gate": [match for match in matches if match.get("trace_stage") == QUALITY_GATE_TRACE_STAGE],
        }
        for bucket in buckets.values():
            bucket.sort(key=lambda item: item["score"], reverse=True)
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

        for stage, stage_limit in (
            ("direct", 3),
            ("query_decomposition", 3),
            ("dependency", 3),
            ("two_hop", 3),
            ("tool_loop", 5),
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
        selected.sort(key=lambda item: item["score"], reverse=True)
        return selected

    def _build_gemini_answer(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        pm_team: str,
        country: str,
        question: str,
        matches: list[dict[str, Any]],
        llm_budget_mode: str,
        requested_answer_mode: str = ANSWER_MODE_GEMINI,
    ) -> dict[str, Any]:
        if not self.llm_ready():
            raise ToolError("LLM mode is not configured yet. Set SOURCE_CODE_QA_GEMINI_API_KEY or GEMINI_API_KEY on the server first.")
        routed_budget_mode, budget, llm_route = self._resolve_llm_budget(llm_budget_mode, question, matches)
        selected_model = str(budget.get("model") or self.gemini_model).strip() or self.gemini_model
        selected_matches = self._select_llm_matches(matches, int(budget["match_limit"]), question=question)
        evidence_summary = self._compress_evidence(question, selected_matches)
        trace_paths = self._build_trace_paths(entries=entries, key=key, matches=selected_matches, question=question)
        if trace_paths:
            evidence_summary["trace_paths"] = trace_paths
        quality_gate = self._quality_gate(question, evidence_summary)
        prompt_context = self._build_compressed_llm_context(
            evidence_summary,
            quality_gate,
            selected_matches,
            snippet_line_budget=budget["snippet_line_budget"],
            snippet_char_budget=budget["snippet_char_budget"],
        )
        cache_key = self._answer_cache_key(
            model=selected_model,
            question=question,
            answer_mode=requested_answer_mode,
            llm_budget_mode=routed_budget_mode,
            context=prompt_context,
        )
        cached = self._load_cached_answer(cache_key)
        if cached is not None:
            cached_answer = str(cached["answer"])
            cached_structured = self._parse_structured_answer(cached_answer)
            cached_claim_check = self._verify_answer_claims(cached_answer, evidence_summary, selected_matches)
            cached_final = self._finalize_llm_answer(
                question=question,
                answer=cached_answer,
                structured_answer=cached_structured,
                evidence_summary=evidence_summary,
                quality_gate=cached.get("answer_quality") or quality_gate,
                claim_check=cached_claim_check,
                selected_matches=selected_matches,
            )
            return {
                "llm_answer": cached_final["answer"],
                "llm_budget_mode": routed_budget_mode,
                "llm_requested_budget_mode": llm_budget_mode,
                "llm_route": llm_route,
                "llm_cached": True,
                "llm_usage": cached.get("usage") or {},
                "answer_quality": cached.get("answer_quality") or quality_gate,
                "answer_claim_check": cached_claim_check,
                "structured_answer": cached_final["structured_answer"],
                "answer_contract": cached_final["answer_contract"],
            }
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": self._llm_user_prompt(
                                pm_team=pm_team,
                                country=country,
                                question=question,
                                context=prompt_context,
                            )
                        }
                    ]
                }
            ],
            "systemInstruction": {
                "parts": [
                    {
                        "text": (
                            "You are a codebase analyst for an internal portal. "
                            "Answer only from the provided retrieval evidence. "
                            "Treat the compressed evidence facts as the primary signal, and snippets as secondary grounding. "
                            "Never upgrade DTO/carrier evidence into a final data source. "
                            "Avoid speculative language such as likely, suggests, or appears unless explicitly marking missing evidence. "
                            "Prioritize answering the user's actual question directly and accurately. "
                            "Do not dump ranked references, but do cite concrete claims with provided citation ids. "
                            "If the evidence is insufficient for a confident answer, say what is missing and give the best next question to ask. "
                            "Keep the answer concise, practical, and business-readable."
                        )
                    }
                ]
            },
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": budget["max_output_tokens"],
                "responseMimeType": "application/json",
                "responseSchema": self._llm_answer_response_schema(),
                "thinkingConfig": {
                    "thinkingBudget": budget["thinking_budget"],
                },
            },
        }
        result, usage, effective_model, attempts = self._generate_gemini_with_retry(
            payload=payload,
            primary_model=selected_model,
            fallback_model=self.gemini_fallback_model,
        )
        answer = self._extract_gemini_text(result)
        structured_answer = self._parse_structured_answer(answer)
        usage = usage or result.get("usageMetadata") or {}
        answer_check = self._answer_self_check(question, answer, evidence_summary, quality_gate)
        claim_check = self._verify_answer_claims(answer, evidence_summary, selected_matches)
        if claim_check.get("status") != "ok" and answer_check.get("status") == "retry":
            issues = list(answer_check.get("issues") or [])
            issues.extend(claim_check.get("issues") or [])
            answer_check = {"status": "retry", "issues": list(dict.fromkeys(issues))}
        if answer_check.get("status") == "retry":
            retry_matches = self._expand_answer_retry_matches(
                entries=entries,
                key=key,
                question=question,
                matches=matches,
                evidence_summary=evidence_summary,
                quality_gate=quality_gate,
                limit=int(budget["match_limit"]) + 6,
            )
            if retry_matches:
                retry_selected_matches = self._select_llm_matches(retry_matches, int(budget["match_limit"]) + 4, question=question)
                retry_evidence_summary = self._compress_evidence(question, retry_selected_matches)
                retry_trace_paths = self._build_trace_paths(entries=entries, key=key, matches=retry_selected_matches, question=question)
                if retry_trace_paths:
                    retry_evidence_summary["trace_paths"] = retry_trace_paths
                retry_quality_gate = self._quality_gate(question, retry_evidence_summary)
                retry_context = self._build_compressed_llm_context(
                    retry_evidence_summary,
                    retry_quality_gate,
                    retry_selected_matches,
                    snippet_line_budget=min(int(budget["snippet_line_budget"]) + 60, 180),
                    snippet_char_budget=min(int(budget["snippet_char_budget"]) + 8000, 28_000),
                )
                retry_payload = dict(payload)
                retry_payload["generationConfig"] = {
                    **payload["generationConfig"],
                    "maxOutputTokens": min(max(int(budget["max_output_tokens"]) + 500, 900), 1_600),
                    "thinkingConfig": {
                        "thinkingBudget": min(max(int(budget["thinking_budget"]), 256), 1_024),
                    },
                }
                retry_payload["contents"] = [
                    {
                        "parts": [
                            {
                                "text": self._llm_user_prompt(
                                    pm_team=pm_team,
                                    country=country,
                                    question=question,
                                    context=retry_context,
                                    self_check=answer_check,
                                )
                            }
                        ]
                    }
                ]
                retry_result, retry_usage, retry_model, retry_attempts = self._generate_gemini_with_retry(
                    payload=retry_payload,
                    primary_model=effective_model,
                    fallback_model=self.gemini_fallback_model,
                )
                retry_answer = self._extract_gemini_text(retry_result)
                retry_structured_answer = self._parse_structured_answer(retry_answer)
                retry_check = self._answer_self_check(question, retry_answer, retry_evidence_summary, retry_quality_gate)
                retry_claim_check = self._verify_answer_claims(retry_answer, retry_evidence_summary, retry_selected_matches)
                if retry_claim_check.get("status") != "ok":
                    issues = list(retry_check.get("issues") or [])
                    issues.extend(retry_claim_check.get("issues") or [])
                    retry_check = {"status": "retry", "issues": list(dict.fromkeys(issues))}
                answer = retry_answer
                structured_answer = retry_structured_answer
                usage = retry_usage or retry_result.get("usageMetadata") or {}
                effective_model = retry_model
                attempts += retry_attempts
                evidence_summary = retry_evidence_summary
                quality_gate = retry_quality_gate
                answer_check = retry_check
                claim_check = retry_claim_check
                selected_matches = retry_selected_matches
                prompt_context = retry_context
        final = self._finalize_llm_answer(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            claim_check=claim_check,
            selected_matches=selected_matches,
        )
        answer = final["answer"]
        structured_answer = final["structured_answer"]
        answer_contract = final["answer_contract"]
        self._store_cached_answer(cache_key, answer=answer, usage=usage, answer_quality=quality_gate)
        return {
            "llm_answer": answer,
            "llm_budget_mode": routed_budget_mode,
            "llm_requested_budget_mode": llm_budget_mode,
            "llm_route": llm_route,
            "llm_cached": False,
            "llm_usage": usage,
            "llm_model": effective_model,
            "llm_attempts": attempts,
            "answer_quality": quality_gate,
            "answer_self_check": answer_check,
            "answer_claim_check": claim_check,
            "structured_answer": structured_answer,
            "answer_contract": answer_contract,
        }

    def _generate_gemini_with_retry(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> tuple[dict[str, Any], dict[str, Any], str, int]:
        models = [primary_model]
        if fallback_model and fallback_model not in models:
            models.append(fallback_model)
        retryable_statuses = {429, 500, 502, 503, 504}
        last_error: str | None = None
        attempts = 0
        for model in models:
            for _delay in (0.0, 1.0, 2.0):
                attempts += 1
                if _delay:
                    import time
                    time.sleep(_delay)
                try:
                    response = requests.post(
                        f"{GEMINI_API_BASE_URL}/models/{model}:generateContent",
                        params={"key": self.gemini_api_key},
                        headers={"Content-Type": "application/json"},
                        json=payload,
                        timeout=90,
                    )
                except requests.RequestException as error:
                    last_error = self._sanitize_error_detail(str(error))
                    continue
                response_ok = getattr(response, "ok", None)
                if response_ok is None:
                    try:
                        response.raise_for_status()
                        response_ok = True
                    except requests.HTTPError:
                        response_ok = False
                if response_ok:
                    result = response.json()
                    usage = result.get("usageMetadata") or {}
                    return result, usage, model, attempts
                status = int(getattr(response, "status_code", 500) or 500)
                detail = self._sanitize_error_detail(response.text)
                last_error = detail
                if status not in retryable_statuses:
                    raise ToolError(f"Gemini answer generation failed. {detail[:500]}")
            # try next fallback model after retries on the current model
        raise ToolError(f"Gemini answer generation failed. {str(last_error or 'Model unavailable.')[:500]}")

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
        deep_reasons: list[str] = []
        if intent.get("data_source"):
            deep_reasons.append("data_source_trace")
        if intent.get("error") or "root cause" in lowered_question or "why" in lowered_question:
            deep_reasons.append("root_cause_or_error")
        if any(stage.startswith(("agent_trace", "agent_plan", TOOL_LOOP_TRACE_PREFIX)) for stage in trace_stages):
            deep_reasons.append("agentic_trace_used")
        if {"flow_graph", "entity_graph", "code_graph"} & retrievals and len(matches) >= 8:
            deep_reasons.append("graph_evidence_bundle")
        if deep_reasons:
            mode = "deep"
        elif any(intent.get(key) for key in ("api", "config", "rule_logic")) or len(matches) >= 5:
            mode = "balanced"
            deep_reasons.append("moderate_code_reasoning")
        else:
            mode = "cheap"
            deep_reasons.append("simple_lookup")
        return mode, self.llm_budgets[mode], {"mode": "auto", "requested": requested, "selected": mode, "reason": ",".join(deep_reasons)}

    @staticmethod
    def _llm_answer_response_schema() -> dict[str, Any]:
        string_array = {"type": "ARRAY", "items": {"type": "STRING"}}
        return {
            "type": "OBJECT",
            "properties": {
                "direct_answer": {"type": "STRING"},
                "claims": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "text": {"type": "STRING"},
                            "citations": string_array,
                        },
                        "required": ["text", "citations"],
                        "propertyOrdering": ["text", "citations"],
                    },
                },
                "missing_evidence": string_array,
                "confidence": {"type": "STRING", "enum": ["high", "medium", "low"]},
            },
            "required": ["direct_answer", "claims", "missing_evidence", "confidence"],
            "propertyOrdering": ["direct_answer", "claims", "missing_evidence", "confidence"],
        }

    def _build_llm_context(self, matches: list[dict[str, Any]], *, snippet_line_budget: int, snippet_char_budget: int) -> str:
        chunks: list[str] = []
        remaining_chars = snippet_char_budget
        for index, match in enumerate(matches, start=1):
            snippet_lines = str(match.get("snippet") or "").splitlines()
            snippet = "\n".join(snippet_lines[:snippet_line_budget]).strip()
            chunk = (
                f"Citation: [S{index}]\n"
                f"Repo: {match.get('repo')}\n"
                f"File: {match.get('path')}\n"
                f"Lines: {match.get('line_start')}-{match.get('line_end')}\n"
                f"Trace stage: {match.get('trace_stage') or 'direct'}\n"
                f"Retrieval: {match.get('retrieval') or 'file_scan'}\n"
                f"Reason: {match.get('reason')}\n"
                f"Snippet:\n{snippet}\n"
            )
            if len(chunk) > remaining_chars:
                chunk = chunk[:remaining_chars]
            if not chunk:
                break
            chunks.append(chunk)
            remaining_chars -= len(chunk)
            if remaining_chars <= 0:
                break
        return "\n\n---\n\n".join(chunks)

    def _build_compressed_llm_context(
        self,
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        matches: list[dict[str, Any]],
        *,
        snippet_line_budget: int,
        snippet_char_budget: int,
    ) -> str:
        sections = [
            "Compressed evidence facts:",
            f"- Quality gate: {quality_gate.get('status')} / confidence={quality_gate.get('confidence')} / missing={', '.join(quality_gate.get('missing') or []) or 'none'}",
        ]
        for label, key in (
            ("Entry points", "entry_points"),
            ("Data carriers", "data_carriers"),
            ("Field population trail", "field_population"),
            ("Downstream components", "downstream_components"),
            ("Concrete data sources", "data_sources"),
            ("API or config evidence", "api_or_config"),
            ("Rule or error logic", "rule_or_error_logic"),
        ):
            values = evidence_summary.get(key) or []
            if values:
                sections.append(f"- {label}:")
                sections.extend(f"  - {value}" for value in values[:10])
        trace_paths = evidence_summary.get("trace_paths") or []
        if trace_paths:
            sections.append("- Trace paths:")
            for path in trace_paths[:5]:
                edge_text = " -> ".join(
                    f"{edge.get('edge_kind')}:{edge.get('to_name') or edge.get('to_file')}"
                    for edge in path.get("edges") or []
                )
                sections.append(f"  - {path.get('repo')}: {edge_text}")
        snippet_context = self._build_llm_context(
            matches,
            snippet_line_budget=max(8, min(int(snippet_line_budget or 40), 180)),
            snippet_char_budget=max(1200, min(int(snippet_char_budget or 5000), 28_000)),
        )
        if snippet_context:
            sections.append("\nSecondary raw snippets for grounding:")
            sections.append(snippet_context)
        return "\n".join(sections)

    @staticmethod
    def _llm_user_prompt(
        *,
        pm_team: str,
        country: str,
        question: str,
        context: str,
        self_check: dict[str, Any] | None = None,
    ) -> str:
        retry_note = ""
        if self_check:
            retry_note = (
                "\nPrevious draft self-check failed:\n"
                f"- Issues: {', '.join(self_check.get('issues') or []) or 'unknown'}\n"
                "- Regenerate a stronger answer from the updated context. Do not repeat the weak answer pattern.\n"
            )
        return (
            f"PM Team: {pm_team}\n"
            f"Country: {country}\n"
            f"Question: {question}\n\n"
            "Internal retrieval evidence for grounding only:\n"
            f"{context}\n\n"
            f"{retry_note}"
            "Answer requirements:\n"
            "- Return this JSON shape whenever possible: "
            "{\"direct_answer\":\"...\",\"claims\":[{\"text\":\"...\",\"citations\":[\"S1\"]}],"
            "\"missing_evidence\":[],\"confidence\":\"high|medium|low\"}. "
            "If a short prose answer is more appropriate, still keep citation tags on concrete claims.\n"
            "- Start with the direct answer.\n"
            "- Prefer agent trace and two-hop trace evidence when it clarifies downstream service, integration, repository, mapper, API, or table usage.\n"
            "- Use compressed facts first; only use raw snippets to verify or disambiguate.\n"
            "- For data-source questions, a DTO/Input/Info class is not a final data source. Trace backward to the provider/builder/setter and then to repository/mapper/client/API/table when evidence exists.\n"
            "- A DAO/Mapper import or field declaration is not enough; prefer method bodies, SQL, mapper XML, API client calls, or table names.\n"
            "- If only DTO fields are known, clearly say that these are carriers, not the upstream source.\n"
            "- If a quality gate says evidence is missing, do not pretend certainty. Say the closest known flow and the exact missing link.\n"
            "- Do not use likely/suggests/appears for final data-source claims. Put uncertainty in missing_evidence instead.\n"
            "- Summarize the relevant logic, data sources, APIs, tables, or classes in plain language when applicable.\n"
            "- Add short citation tags like [S1] next to concrete code-backed claims.\n"
            "- Avoid listing file paths or line ranges unless the user asks for code locations.\n"
            "- If unsure, explain the uncertainty instead of inventing details.\n"
        )

    def _finalize_llm_answer(
        self,
        *,
        question: str,
        answer: str,
        structured_answer: dict[str, Any],
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        claim_check: dict[str, Any],
        selected_matches: list[dict[str, Any]],
    ) -> dict[str, Any]:
        intent = evidence_summary.get("intent") or self._question_intent(question)
        confirmed_sources = [
            self._append_fact_citation(fact, selected_matches)
            for fact in evidence_summary.get("data_sources") or []
            if self._is_concrete_source_line(str(fact))
        ]
        data_carriers = [
            self._append_fact_citation(fact, selected_matches)
            for fact in evidence_summary.get("data_carriers") or []
        ][:8]
        field_population = [
            self._append_fact_citation(fact, selected_matches)
            for fact in evidence_summary.get("field_population") or []
        ][:8]
        missing_links = list(dict.fromkeys([*(quality_gate.get("missing") or []), *(structured_answer.get("missing_evidence") or [])]))
        blocked = bool(intent.get("data_source") and not confirmed_sources)
        weak_answer = any(phrase in str(answer or "").lower() for phrase in ANSWER_SELF_CHECK_WEAK_PHRASES)
        uncited_claims = claim_check.get("status") not in {None, "ok"}
        contract = {
            "intent": intent,
            "status": "blocked_missing_source" if blocked else "grounded",
            "confirmed_sources": confirmed_sources[:8],
            "data_carriers": data_carriers,
            "field_population": field_population,
            "missing_links": missing_links[:8],
            "confidence": "low" if blocked else str(structured_answer.get("confidence") or quality_gate.get("confidence") or "medium").lower(),
            "claim_check": claim_check,
        }
        final_answer = str(answer or "").strip()
        if blocked:
            final_answer = self._build_missing_source_answer(contract)
        elif structured_answer.get("format") == "json" and structured_answer.get("direct_answer"):
            final_answer = self._render_structured_answer(structured_answer, contract)
        elif uncited_claims and intent.get("data_source"):
            final_answer = self._render_structured_answer(structured_answer, contract)
        elif weak_answer and confirmed_sources:
            final_answer = self._render_structured_answer(structured_answer, contract)
        final_structured = self._parse_structured_answer(final_answer)
        return {
            "answer": final_answer,
            "structured_answer": final_structured,
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
    def _build_missing_source_answer(contract: dict[str, Any]) -> str:
        lines = [
            "I cannot confirm the final upstream data source from the current indexed evidence.",
            "",
        ]
        carriers = [item for item in contract.get("data_carriers") or [] if item]
        population = [item for item in contract.get("field_population") or [] if item]
        if carriers or population:
            lines.append("Confirmed so far:")
            for item in carriers[:5]:
                lines.append(f"- Carrier/processing evidence: {item}")
            for item in population[:5]:
                lines.append(f"- Population trail: {item}")
            lines.append("")
        lines.append("Missing link:")
        missing = contract.get("missing_links") or ["concrete upstream source/table/API/repository evidence beyond DTO fields"]
        for item in missing[:4]:
            lines.append(f"- {item}")
        lines.append("")
        lines.append("Next trace target:")
        lines.append("- Follow the provider/builder/setter into a repository, mapper, client/API, or SQL table method.")
        return "\n".join(lines)

    @staticmethod
    def _render_structured_answer(structured_answer: dict[str, Any], contract: dict[str, Any]) -> str:
        lines = [str(structured_answer.get("direct_answer") or "").strip()]
        claims = [claim for claim in structured_answer.get("claims") or [] if isinstance(claim, dict) and str(claim.get("text") or "").strip()]
        if claims:
            lines.append("")
            for claim in claims[:6]:
                text = str(claim.get("text") or "").strip()
                citation_tags = []
                for item in claim.get("citations") or []:
                    tag = str(item).strip()
                    tag = tag if tag.startswith("[") else f"[{tag}]"
                    if tag not in text:
                        citation_tags.append(tag)
                suffix = f" {' '.join(citation_tags)}" if citation_tags else ""
                lines.append(f"- {text}{suffix}")
        missing = contract.get("missing_links") or structured_answer.get("missing_evidence") or []
        if missing:
            lines.append("")
            lines.append("Missing evidence:")
            for item in missing[:4]:
                lines.append(f"- {item}")
        return "\n".join(line for line in lines if line is not None).strip()

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
        return {
            "direct_answer": str(payload.get("direct_answer") or payload.get("answer") or text).strip(),
            "claims": normalized_claims,
            "missing_evidence": [str(item) for item in payload.get("missing_evidence") or []] if isinstance(payload.get("missing_evidence"), list) else [],
            "confidence": str(payload.get("confidence") or "medium").strip().lower(),
            "format": "json",
        }

    def _verify_answer_claims(
        self,
        answer: str,
        evidence_summary: dict[str, Any],
        selected_matches: list[dict[str, Any]],
    ) -> dict[str, Any]:
        evidence_text = json.dumps(evidence_summary, ensure_ascii=False).lower()
        valid_citations = {f"s{index}" for index in range(1, len(selected_matches) + 1)}
        issues: list[str] = []
        checked_claims = 0
        unsupported_claims: list[str] = []
        for claim in self._split_answer_claims(answer):
            lowered = claim.lower()
            concrete = any(term in lowered for term in ANSWER_CONCRETE_SOURCE_HINTS + API_HINTS + CONFIG_HINTS + RULE_HINTS)
            if not concrete:
                continue
            checked_claims += 1
            citations = {token.lower() for token in re.findall(r"\[S(\d+)\]", claim)}
            if citations and not citations <= {item.removeprefix("s") for item in valid_citations}:
                issues.append("answer cites evidence ids outside the provided context")
            if not citations and not any(phrase in lowered for phrase in ANSWER_SELF_CHECK_WEAK_PHRASES):
                unsupported_claims.append(claim[:220])
                continue
            expected_terms = self._answer_expected_terms(evidence_summary, "data_sources")
            expected_terms.extend(self._answer_expected_terms(evidence_summary, "api_or_config"))
            if expected_terms and not any(term in lowered for term in expected_terms) and not any(term in evidence_text for term in expected_terms):
                unsupported_claims.append(claim[:220])
        if unsupported_claims:
            issues.append("concrete answer claims need citation-backed evidence")
        return {
            "status": "ok" if not issues else "needs_citation",
            "checked_claims": checked_claims,
            "unsupported_claims": unsupported_claims[:5],
            "issues": list(dict.fromkeys(issues)),
        }

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

    def _answer_self_check(
        self,
        question: str,
        answer: str,
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
    ) -> dict[str, Any]:
        lowered_answer = str(answer or "").lower()
        intent = evidence_summary.get("intent") or self._question_intent(question)
        issues: list[str] = []
        if any(phrase in lowered_answer for phrase in ANSWER_SELF_CHECK_WEAK_PHRASES):
            issues.append("answer sounds inconclusive")
        if intent.get("data_source") and quality_gate.get("status") == "sufficient":
            source_markers = self._answer_expected_terms(evidence_summary, "data_sources")
            if source_markers and not any(marker in lowered_answer for marker in source_markers):
                issues.append("answer omits concrete data source terms found in evidence")
        if intent.get("data_source"):
            if self._looks_like_dto_only_data_source_answer(answer):
                issues.append("answer stops at DTO/carrier layer instead of tracing upstream source")
            if self._has_concrete_source_evidence(evidence_summary) and not self._answer_has_concrete_source_marker(answer):
                issues.append("answer lacks repository/mapper/client/API/table source marker")
        if intent.get("api"):
            api_markers = self._answer_expected_terms(evidence_summary, "api_or_config")
            if api_markers and not any(marker in lowered_answer for marker in api_markers):
                issues.append("answer omits concrete API/config terms found in evidence")
        if len(str(answer or "").strip()) < 80 and evidence_summary.get("source_count", 0) >= 2:
            issues.append("answer is too thin for available evidence")
        retryable = bool(issues) and quality_gate.get("status") != "needs_more_trace" or bool(issues and evidence_summary.get("source_count", 0))
        return {
            "status": "retry" if retryable else "ok",
            "issues": issues,
        }

    @staticmethod
    def _looks_like_dto_only_data_source_answer(answer: str) -> bool:
        lowered = str(answer or "").lower()
        carrier_terms = ("dto", "input", "info", "context")
        concrete_terms = ANSWER_CONCRETE_SOURCE_HINTS
        return any(term in lowered for term in carrier_terms) and not any(term in lowered for term in concrete_terms)

    @staticmethod
    def _answer_has_concrete_source_marker(answer: str) -> bool:
        lowered = str(answer or "").lower()
        return any(term in lowered for term in ANSWER_CONCRETE_SOURCE_HINTS)

    @staticmethod
    def _answer_expected_terms(evidence_summary: dict[str, Any], bucket: str) -> list[str]:
        terms: list[str] = []
        for fact in evidence_summary.get(bucket) or []:
            for token in re.findall(r"[A-Za-z][A-Za-z0-9_]{4,}", str(fact)):
                lowered = token.lower()
                if lowered not in STOPWORDS and lowered not in terms:
                    terms.append(lowered)
        return terms[:12]

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
    ) -> list[dict[str, Any]]:
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
        )

    @staticmethod
    def _extract_gemini_text(payload: dict[str, Any]) -> str:
        candidates = payload.get("candidates") or []
        for candidate in candidates:
            content = candidate.get("content") or {}
            parts = content.get("parts") or []
            texts = [str(part.get("text") or "").strip() for part in parts if str(part.get("text") or "").strip()]
            if texts:
                return "\n".join(texts).strip()
        raise ToolError("Gemini returned no readable answer.")

    def _answer_cache_key(self, *, model: str, question: str, answer_mode: str, llm_budget_mode: str, context: str) -> str:
        digest = hashlib.sha1(
            json.dumps(
                {
                    "model": model,
                    "question": question,
                    "answer_mode": answer_mode,
                    "llm_budget_mode": llm_budget_mode,
                    "context": context,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        return digest

    def _load_cached_answer(self, key: str) -> dict[str, Any] | None:
        cache_path = self.answer_cache_root / f"{key}.json"
        if not cache_path.exists():
            return None
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        expire_at = float(payload.get("expire_at") or 0)
        if expire_at < datetime.now(timezone.utc).timestamp():
            cache_path.unlink(missing_ok=True)
            return None
        return payload

    def _store_cached_answer(self, key: str, *, answer: str, usage: dict[str, Any], answer_quality: dict[str, Any] | None = None) -> None:
        self.answer_cache_root.mkdir(parents=True, exist_ok=True)
        payload = {
            "answer": answer,
            "usage": usage,
            "answer_quality": answer_quality or {},
            "expire_at": datetime.now(timezone.utc).timestamp() + self.llm_cache_ttl_seconds,
        }
        (self.answer_cache_root / f"{key}.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def _record_query_telemetry(
        self,
        *,
        key: str,
        question: str,
        answer_mode: str,
        llm_budget_mode: str,
        payload: dict[str, Any],
        started_at: float,
    ) -> None:
        try:
            matches = payload.get("matches") or []
            stage_counts: dict[str, int] = {}
            retrieval_counts: dict[str, int] = {}
            for match in matches:
                stage = str(match.get("trace_stage") or "direct")
                retrieval = str(match.get("retrieval") or "file_scan")
                stage_counts[stage] = stage_counts.get(stage, 0) + 1
                retrieval_counts[retrieval] = retrieval_counts.get(retrieval, 0) + 1
            record = {
                "timestamp": self._now_iso(),
                "key": key,
                "question_sha1": hashlib.sha1(question.encode("utf-8")).hexdigest(),
                "question_preview": question[:180],
                "requested_answer_mode": answer_mode,
                "answer_mode": payload.get("answer_mode"),
                "llm_budget_mode": llm_budget_mode,
                "status": payload.get("status"),
                "latency_ms": int((time.time() - started_at) * 1000),
                "match_count": len(matches),
                "top_paths": [str(match.get("path") or "") for match in matches[:5]],
                "trace_stage_counts": stage_counts,
                "retrieval_counts": retrieval_counts,
                "answer_quality": payload.get("answer_quality") or {},
                "llm_usage": payload.get("llm_usage") or {},
                "fallback": bool(payload.get("fallback_notice")),
            }
            self.telemetry_path.parent.mkdir(parents=True, exist_ok=True)
            with self.telemetry_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        except OSError:
            return

    def _select_llm_matches(self, matches: list[dict[str, Any]], limit: int, *, question: str = "") -> list[dict[str, Any]]:
        if not matches:
            return []
        limit = max(1, int(limit or 1))
        intent = self._question_intent(question) if question else {}
        buckets = {
            "direct": [match for match in matches if match.get("trace_stage") == "direct"],
            "query_decomposition": [match for match in matches if match.get("trace_stage") == "query_decomposition"],
            "dependency": [match for match in matches if match.get("trace_stage") == "dependency"],
            "two_hop": [match for match in matches if match.get("trace_stage") == "two_hop"],
            "tool_loop": [match for match in matches if str(match.get("trace_stage") or "").startswith(TOOL_LOOP_TRACE_PREFIX)],
            "agent_trace": [match for match in matches if str(match.get("trace_stage") or "").startswith("agent_trace")],
            "agent_plan": [match for match in matches if str(match.get("trace_stage") or "").startswith("agent_plan")],
            "quality_gate": [match for match in matches if match.get("trace_stage") == QUALITY_GATE_TRACE_STAGE],
        }
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
        stage_order = ("direct", "query_decomposition", "dependency", "two_hop", "tool_loop", "agent_trace", "agent_plan", "quality_gate")
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
    ) -> dict[str, Any]:
        return {
            "status": status,
            "answer_mode": ANSWER_MODE,
            "summary": summary,
            "matches": [],
            "repo_status": repo_status or [],
            "key": key,
        }

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
