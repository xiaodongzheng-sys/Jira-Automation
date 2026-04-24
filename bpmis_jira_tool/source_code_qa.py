from __future__ import annotations

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
ANSWER_MODE = "retrieval_only"
ANSWER_MODE_GEMINI = "gemini_flash"
CONFIG_VERSION = 1
CODE_INDEX_VERSION = 2
GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_DOMAIN_PROFILE_PATH = Path(__file__).resolve().parent.parent / "config" / "source_code_qa_domain_profiles.json"
LLM_BUDGETS = {
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
ANNOTATION_ROUTE_PATTERN = re.compile(r"@(RequestMapping|GetMapping|PostMapping|PutMapping|DeleteMapping|PatchMapping)\s*(?:\(([^)]*)\))?")
SQL_TABLE_PATTERN = re.compile(r"\b(?:from|join|update|into)\s+([A-Za-z_][A-Za-z0-9_.$]*)", re.IGNORECASE)
PROPERTIES_KEY_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.-]{3,})\s*[:=]")
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
        gemini_model: str = "gemini-2.5-flash-lite",
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
        self.domain_profile_path = Path(os.getenv("SOURCE_CODE_QA_DOMAIN_PROFILES", str(DEFAULT_DOMAIN_PROFILE_PATH)))
        self.team_profiles = team_profiles
        self.gitlab_token = str(gitlab_token or "").strip()
        self.gitlab_username = str(gitlab_username or "oauth2").strip() or "oauth2"
        self.gemini_api_key = str(gemini_api_key or "").strip()
        self.gemini_model = str(gemini_model or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
        self.gemini_fallback_model = str(gemini_fallback_model or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
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
                {"value": ANSWER_MODE, "label": "Retrieval Only"},
                {"value": ANSWER_MODE_GEMINI, "label": "Gemini"},
            ],
            "llm_budget_modes": [
                {"value": "cheap", "label": "Cheap"},
                {"value": "balanced", "label": "Balanced"},
                {"value": "deep", "label": "Deep"},
            ],
        }

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
        results = [self._sync_entry(key, entry) for entry in entries]
        status = "ok" if all(result["state"] == "ok" for result in results) else "partial"
        return {"status": status, "key": key, "results": results, "repo_status": self.repo_status(key)}

    def query(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        limit: int = 12,
        answer_mode: str = ANSWER_MODE,
        llm_budget_mode: str = "cheap",
    ) -> dict[str, Any]:
        key = self.mapping_key(pm_team, country)
        question = str(question or "").strip()
        started_at = time.time()
        if not question:
            raise ToolError("Please enter a source-code question.")
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
        matches: list[dict[str, Any]] = []
        repo_status = self.repo_status(key)
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            matches.extend(self._search_repo(entry, repo_path, tokens, question=question))
        matches.sort(key=lambda item: item["score"], reverse=True)
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
                top_matches.sort(key=lambda item: item["score"], reverse=True)
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
                top_matches.sort(key=lambda item: item["score"], reverse=True)
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
                top_matches.sort(key=lambda item: item["score"], reverse=True)
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
        payload = {
            "status": "ok",
            "answer_mode": ANSWER_MODE,
            "summary": self._build_summary(top_matches),
            "matches": top_matches,
            "citations": self._build_citations(top_matches),
            "repo_status": repo_status,
            "answer_quality": quality_gate,
            "agent_plan": self._build_agent_plan(question, evidence_summary, quality_gate),
        }
        normalized_answer_mode = str(answer_mode or ANSWER_MODE).strip() or ANSWER_MODE
        if normalized_answer_mode == ANSWER_MODE_GEMINI:
            try:
                llm_payload = self._build_gemini_answer(
                    entries=entries,
                    key=key,
                    pm_team=pm_team,
                    country=country,
                    question=question,
                    matches=top_matches,
                    llm_budget_mode=llm_budget_mode,
                )
                payload.update(llm_payload)
                payload["answer_mode"] = ANSWER_MODE_GEMINI
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
                }
            )
        return statuses

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

    def _index_path(self, repo_path: Path) -> Path:
        digest = hashlib.sha1(str(repo_path).encode("utf-8")).hexdigest()[:16]
        return self.index_root / f"{digest}.sqlite3"

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
        self.index_root.mkdir(parents=True, exist_ok=True)
        index_path = self._index_path(repo_path)
        tmp_path = index_path.with_suffix(".tmp")
        tmp_path.unlink(missing_ok=True)
        fingerprint = self._repo_fingerprint(repo_path)
        indexed_files = 0
        indexed_lines = 0
        indexed_definitions = 0
        indexed_references = 0
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
                """
            )
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
                indexed_files += 1
                indexed_lines += len(lines)
                indexed_definitions += len(structure["definitions"])
                indexed_references += len(structure["references"])
            metadata = {
                "version": str(CODE_INDEX_VERSION),
                "file_count": str(fingerprint["file_count"]),
                "latest_mtime_ns": str(fingerprint["latest_mtime_ns"]),
                "total_size": str(fingerprint["total_size"]),
                "indexed_files": str(indexed_files),
                "indexed_lines": str(indexed_lines),
                "indexed_definitions": str(indexed_definitions),
                "indexed_references": str(indexed_references),
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
            "updated_at": metadata["updated_at"],
        }

    def _extract_structure_rows(self, relative_path: str, lines: list[str]) -> dict[str, list[tuple[Any, ...]]]:
        definitions: list[tuple[Any, ...]] = []
        references: list[tuple[Any, ...]] = []
        suffix = Path(relative_path).suffix.lower()

        def add_definition(name: str, kind: str, line_no: int, signature: str) -> None:
            name = str(name or "").strip()
            if not name:
                return
            definitions.append((name, name.lower(), kind, relative_path, line_no, signature.strip()[:500]))

        def add_reference(target: str, kind: str, line_no: int, context: str) -> None:
            target = str(target or "").strip().strip("\"'")
            if len(target) < 2:
                return
            references.append((target, target.lower(), kind, relative_path, line_no, context.strip()[:500]))

        for line_no, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            for match in CLASS_DEF_PATTERN.finditer(line):
                add_definition(match.group(2), match.group(1).lower(), line_no, stripped)
            py_match = PY_DEF_PATTERN.search(line)
            if py_match:
                add_definition(py_match.group(2), "python_" + py_match.group(1).lower(), line_no, stripped)
            js_match = JS_DEF_PATTERN.search(line)
            if js_match:
                add_definition(js_match.group(1) or js_match.group(2), "javascript_function", line_no, stripped)
            java_method = JAVA_METHOD_DEF_PATTERN.search(line)
            if java_method and not stripped.startswith(("if ", "for ", "while ", "switch ", "catch ")):
                add_definition(java_method.group(1), "java_method", line_no, stripped)
            for annotation in ANNOTATION_ROUTE_PATTERN.finditer(line):
                add_definition(annotation.group(1), "route_annotation", line_no, stripped)
                for route in re.findall(r'"([^"]+)"', annotation.group(2) or ""):
                    add_reference(route, "route", line_no, stripped)
            for table in SQL_TABLE_PATTERN.findall(line):
                add_reference(table, "sql_table", line_no, stripped)
            if suffix in {".properties", ".yaml", ".yml", ".conf", ".toml"}:
                key_match = PROPERTIES_KEY_PATTERN.search(line)
                if key_match:
                    add_definition(key_match.group(1), "config_key", line_no, stripped)
            for symbol in self._extract_downstream_symbols(line):
                add_reference(symbol, "symbol_reference", line_no, stripped)
            for call in CALL_SYMBOL_PATTERN.findall(line):
                lowered = call.lower()
                if lowered not in LOW_VALUE_CALL_SYMBOLS and lowered not in STOPWORDS:
                    add_reference(call, "call", line_no, stripped)

        return {
            "definitions": list(dict.fromkeys(definitions)),
            "references": list(dict.fromkeys(references)),
        }

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
        trace_stage_bonus = 90 if trace_stage == "two_hop" or trace_stage.startswith("agent_trace") or trace_stage.startswith("agent_plan") or trace_stage == QUALITY_GATE_TRACE_STAGE else 0
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
        trace_stage_bonus = 90 if trace_stage == "two_hop" or trace_stage.startswith("agent_trace") or trace_stage.startswith("agent_plan") or trace_stage == QUALITY_GATE_TRACE_STAGE else 0
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

    def _compress_evidence(self, question: str, matches: list[dict[str, Any]]) -> dict[str, Any]:
        selected = self._select_llm_matches(matches, 12)
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
            "dependency": [match for match in matches if match.get("trace_stage") == "dependency"],
            "two_hop": [match for match in matches if match.get("trace_stage") == "two_hop"],
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

        for stage, stage_limit in (("direct", 3), ("dependency", 3), ("two_hop", 3), ("agent_trace", 8), ("agent_plan", 6), ("quality_gate", 4)):
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
    ) -> dict[str, Any]:
        if not self.llm_ready():
            raise ToolError("Gemini mode is not configured yet. Set SOURCE_CODE_QA_GEMINI_API_KEY on the server first.")
        budget = LLM_BUDGETS.get(str(llm_budget_mode or "cheap").strip().lower(), LLM_BUDGETS["cheap"])
        selected_model = str(budget.get("model") or self.gemini_model).strip() or self.gemini_model
        selected_matches = self._select_llm_matches(matches, int(budget["match_limit"]))
        evidence_summary = self._compress_evidence(question, selected_matches)
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
            answer_mode=ANSWER_MODE_GEMINI,
            llm_budget_mode=llm_budget_mode,
            context=prompt_context,
        )
        cached = self._load_cached_answer(cache_key)
        if cached is not None:
            return {
                "llm_answer": cached["answer"],
                "llm_budget_mode": llm_budget_mode,
                "llm_cached": True,
                "llm_usage": cached.get("usage") or {},
                "answer_quality": cached.get("answer_quality") or quality_gate,
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
                            "Prioritize answering the user's actual question directly and accurately. "
                            "Do not dump ranked references or evidence bullets unless the user explicitly asks where in code. "
                            "If the evidence is insufficient for a confident answer, say what is missing and give the best next question to ask. "
                            "Keep the answer concise, practical, and business-readable."
                        )
                    }
                ]
            },
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": budget["max_output_tokens"],
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
        usage = usage or result.get("usageMetadata") or {}
        answer_check = self._answer_self_check(question, answer, evidence_summary, quality_gate)
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
                retry_selected_matches = self._select_llm_matches(retry_matches, int(budget["match_limit"]) + 4)
                retry_evidence_summary = self._compress_evidence(question, retry_selected_matches)
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
                retry_check = self._answer_self_check(question, retry_answer, retry_evidence_summary, retry_quality_gate)
                answer = retry_answer
                usage = retry_usage or retry_result.get("usageMetadata") or {}
                effective_model = retry_model
                attempts += retry_attempts
                evidence_summary = retry_evidence_summary
                quality_gate = retry_quality_gate
                answer_check = retry_check
                selected_matches = retry_selected_matches
                prompt_context = retry_context
        self._store_cached_answer(cache_key, answer=answer, usage=usage, answer_quality=quality_gate)
        return {
            "llm_answer": answer,
            "llm_budget_mode": llm_budget_mode,
            "llm_cached": False,
            "llm_usage": usage,
            "llm_model": effective_model,
            "llm_attempts": attempts,
            "answer_quality": quality_gate,
            "answer_self_check": answer_check,
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
            "- Start with the direct answer.\n"
            "- Prefer agent trace and two-hop trace evidence when it clarifies downstream service, integration, repository, mapper, API, or table usage.\n"
            "- Use compressed facts first; only use raw snippets to verify or disambiguate.\n"
            "- For data-source questions, a DTO/Input/Info class is not a final data source. Trace backward to the provider/builder/setter and then to repository/mapper/client/API/table when evidence exists.\n"
            "- A DAO/Mapper import or field declaration is not enough; prefer method bodies, SQL, mapper XML, API client calls, or table names.\n"
            "- If only DTO fields are known, clearly say that these are carriers, not the upstream source.\n"
            "- If a quality gate says evidence is missing, do not pretend certainty. Say the closest known flow and the exact missing link.\n"
            "- Summarize the relevant logic, data sources, APIs, tables, or classes in plain language when applicable.\n"
            "- Add short citation tags like [S1] next to concrete code-backed claims.\n"
            "- Avoid listing file paths or line ranges unless the user asks for code locations.\n"
            "- If unsure, explain the uncertainty instead of inventing details.\n"
        )

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

    @staticmethod
    def _select_llm_matches(matches: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        if not matches:
            return []
        limit = max(1, int(limit or 1))
        buckets = {
            "direct": [match for match in matches if match.get("trace_stage") == "direct"],
            "dependency": [match for match in matches if match.get("trace_stage") == "dependency"],
            "two_hop": [match for match in matches if match.get("trace_stage") == "two_hop"],
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
        for stage in ("direct", "dependency", "two_hop", "agent_trace", "agent_plan", "quality_gate"):
            for match in buckets[stage][:2]:
                add(match)
        for match in matches:
            add(match)
            if len(selected) >= limit:
                break
        return selected

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
