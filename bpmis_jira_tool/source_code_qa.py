from __future__ import annotations

import ast
import base64
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import functools
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
from typing import Any
from urllib.parse import urlsplit, urlunsplit
import uuid

import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account

from bpmis_jira_tool.errors import ToolError


ALL_COUNTRY = "All"
CRMS_COUNTRIES = ("SG", "ID", "PH")
ANSWER_MODE_AUTO = "auto"
ANSWER_MODE = "retrieval_only"
ANSWER_MODE_GEMINI = "gemini_flash"
CONFIG_VERSION = 1
CODE_INDEX_VERSION = 29
LLM_PROVIDER_GEMINI = "gemini"
LLM_PROVIDER_OPENAI_COMPATIBLE = "openai_compatible"
LLM_PROVIDER_CODEX_CLI_BRIDGE = "codex_cli_bridge"
LLM_PROVIDER_VERTEX_AI = "vertex_ai"
LLM_PROVIDER_ALLOWED_QUERY_CHOICES = {LLM_PROVIDER_GEMINI, LLM_PROVIDER_CODEX_CLI_BRIDGE, LLM_PROVIDER_VERTEX_AI}
LLM_PROMPT_VERSION = 10
LLM_RESPONSE_SCHEMA_VERSION = 5
LLM_ROUTER_VERSION = 7
LLM_CACHE_VERSION = 15
LLM_RUNTIME_VERSION = 2
PLANNER_TOOL_DSL_VERSION = 1
GEMINI_MIN_THINKING_BUDGET = 512
GEMINI_MAX_THINKING_BUDGET = 24576
COMPACT_DEEP_BUDGET_MODE = "compact_deep"
LLM_PROMPT_COMPACT_THRESHOLD_TOKENS = 18_000
LLM_PROMPT_TIGHT_THRESHOLD_TOKENS = 24_000
VERTEX_PROMPT_COMPACT_THRESHOLD_TOKENS = 72_000
VERTEX_PROMPT_TIGHT_THRESHOLD_TOKENS = 96_000
LLM_TOKEN_ESTIMATE_CHARS_PER_TOKEN = 3.0
GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
VERTEX_AI_GLOBAL_API_BASE_URL = "https://aiplatform.googleapis.com/v1"
OPENAI_COMPATIBLE_API_BASE_URL = "https://api.openai.com/v1"
DEFAULT_SEMANTIC_INDEX_MODEL = "local-token-hybrid-v1"
DEFAULT_VERTEX_EMBEDDING_MODEL = "gemini-embedding-001"
VERTEX_EMBEDDING_DOCUMENT_TASK = "RETRIEVAL_DOCUMENT"
VERTEX_EMBEDDING_QUERY_TASK = "CODE_RETRIEVAL_QUERY"
DEFAULT_DOMAIN_PROFILE_PATH = Path(__file__).resolve().parent.parent / "config" / "source_code_qa_domain_profiles.json"
DEFAULT_DOMAIN_KNOWLEDGE_PACK_PATH = Path(__file__).resolve().parent.parent / "config" / "source_code_qa_domain_knowledge_packs.json"
DEFAULT_LLM_TIMEOUT_SECONDS = 90
DEFAULT_LLM_MAX_RETRIES = 2
DEFAULT_LLM_BACKOFF_SECONDS = 1.0
DEFAULT_LLM_MAX_BACKOFF_SECONDS = 8.0
DEFAULT_CODEX_CLI_MODEL = "codex-cli"
DEFAULT_CODEX_TIMEOUT_SECONDS = 240
DEFAULT_CODEX_TOP_PATH_LIMIT = 30
CODEX_INVESTIGATION_PROMPT_MODE = "codex_investigation_brief_v4"
CODEX_SESSION_MODE_EPHEMERAL = "ephemeral"
CODEX_SESSION_MODE_RESUME = "resume"
DEFAULT_INDEX_LOCK_STALE_SECONDS = 15 * 60
DEFAULT_AUTO_SYNC_START_DATE = date(2026, 5, 8)
DEFAULT_AUTO_SYNC_INTERVAL_DAYS = 14
MAX_CACHED_INDEX_LINES = 5_000
MAX_CACHED_SEMANTIC_CHUNKS = 2_000
MAX_TARGETED_INDEX_FILES = 220
MAX_TARGETED_INDEX_LINES = 1_200
MAX_TARGETED_SEMANTIC_CHUNKS = 320
SYNC_JOB_LOCK_TIMEOUT_SECONDS = 5.0
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
        "thinking_budget": 512,
        "max_output_tokens": 700,
        "model": "gemini-2.5-flash",
    },
    "deep": {
        "match_limit": 12,
        "snippet_line_budget": 160,
        "snippet_char_budget": 24_000,
        "thinking_budget": 1024,
        "max_output_tokens": 1_400,
        "model": "gemini-2.5-flash",
    },
    COMPACT_DEEP_BUDGET_MODE: {
        "match_limit": 8,
        "snippet_line_budget": 55,
        "snippet_char_budget": 8_000,
        "thinking_budget": 512,
        "max_output_tokens": 2_000,
        "model": "gemini-2.5-flash",
    },
}
VERTEX_QUALITY_LLM_BUDGETS = {
    "cheap": {
        "match_limit": 10,
        "snippet_line_budget": 90,
        "snippet_char_budget": 24_000,
        "thinking_budget": 512,
        "max_output_tokens": 1_800,
    },
    "balanced": {
        "match_limit": 18,
        "snippet_line_budget": 180,
        "snippet_char_budget": 48_000,
        "thinking_budget": 2048,
        "max_output_tokens": 2_800,
    },
    "deep": {
        "match_limit": 28,
        "snippet_line_budget": 260,
        "snippet_char_budget": 86_000,
        "thinking_budget": 4096,
        "max_output_tokens": 4_800,
    },
    COMPACT_DEEP_BUDGET_MODE: {
        "match_limit": 18,
        "snippet_line_budget": 140,
        "snippet_char_budget": 44_000,
        "thinking_budget": 2048,
        "max_output_tokens": 4_000,
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

PRODUCTION_EVIDENCE_TIERS = {"production", "config"}
WEAK_EVIDENCE_TIERS = {"test", "docs", "generated"}

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
    ".jsonl",
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
JAVA_PACKAGE_PATTERN = re.compile(r"^\s*package\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;")
JAVA_IMPORT_PATTERN = re.compile(r"^\s*import\s+(?:static\s+)?([A-Za-z_][A-Za-z0-9_.*]*)\s*;")
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
MYBATIS_RESULT_MAP_PATTERN = re.compile(r"<resultMap\b[^>]*\bid\s*=\s*[\"']([^\"']+)[\"'][^>]*", re.IGNORECASE)
MYBATIS_INCLUDE_PATTERN = re.compile(r"<include\b[^>]*\brefid\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
MYBATIS_ATTR_REFERENCE_PATTERN = re.compile(r"\b(parameterType|resultType|resultMap|type|javaType|ofType)\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
HTTP_LITERAL_PATTERN = re.compile(r"[\"'](https?://[^\"']+|/[A-Za-z0-9_./{}:-]{2,})[\"']")
SQL_TABLE_PATTERN = re.compile(r"\b(?:from|join|update|into)\s+([A-Za-z_][A-Za-z0-9_.$]*)", re.IGNORECASE)
SQL_READ_TABLE_PATTERN = re.compile(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_.$]*)", re.IGNORECASE)
SQL_WRITE_TABLE_PATTERN = re.compile(r"\b(?:insert\s+into|update|delete\s+from)\s+([A-Za-z_][A-Za-z0-9_.$]*)", re.IGNORECASE)
EXACT_LOOKUP_TERM_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:[./:$-][A-Za-z0-9_][A-Za-z0-9_.:$-]*)+")
CODE_USAGE_SUFFIXES = {".java", ".kt", ".kts", ".py", ".js", ".jsx", ".ts", ".tsx", ".go", ".rb", ".php", ".cs", ".scala"}
NON_FUNCTION_USAGE_SUFFIXES = {".md", ".txt", ".properties", ".yaml", ".yml", ".json", ".jsonl", ".xml", ".toml", ".conf", ".sql"}
USAGE_QUERY_HINTS = (
    " used ",
    " usage ",
    " reference ",
    " references ",
    " referenced ",
    " called ",
    " caller ",
    " call ",
    " where is ",
    " where used ",
    "用到",
    "使用",
    "引用",
    "调用",
    "在哪里",
    "在哪",
)
FUNCTION_USAGE_QUERY_HINTS = (" function", " method", "函数", "方法")
COMPLEX_REASONING_QUERY_HINTS = (
    "logic",
    "calculate",
    "calculation",
    "relationship",
    "relation",
    "between",
    "flow",
    "source",
    "upstream",
    "downstream",
    "impact",
    "test",
    "config",
    "why",
    "how",
    "逻辑",
    "计算",
    "关系",
    "链路",
    "来源",
    "上下游",
    "影响",
    "测试",
    "配置",
    "为什么",
    "如何",
)
PROPERTIES_KEY_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.-]{3,})\s*[:=]")
CONFIG_ASSIGNMENT_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.-]{3,})\s*[:=]\s*(.+?)\s*$")
CONFIG_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Za-z0-9_.-]+)(?::[^}]*)?\}")
SPRING_VALUE_PATTERN = re.compile(r"@Value\s*\(\s*[\"']\$\{([^}:]+)(?::[^}]*)?\}[\"']\s*\)")
SPRING_QUALIFIER_PATTERN = re.compile(r"@(Qualifier|Resource)\s*(?:\(\s*(?:name\s*=\s*)?[\"']([^\"']+)[\"']\s*\))?")
SPRING_QUALIFIED_VARIABLE_PATTERN = re.compile(
    r"@(?:Qualifier|Resource)\s*(?:\(\s*(?:name\s*=\s*)?[\"']([^\"']+)[\"']\s*\))\s*"
    r"(?:@[A-Za-z_][A-Za-z0-9_]*(?:\([^)]*\))?\s*)*"
    r"(?:final\s+)?[A-Z][A-Za-z0-9_]*(?:<[^;(){}]*>)?(?:\s*\[\])?\s+([a-z][A-Za-z0-9_]*)"
)
SPRING_PROFILE_PATTERN = re.compile(r"@Profile\s*\(([^)]*)\)")
SPRING_CONDITIONAL_ON_PROPERTY_PATTERN = re.compile(r"@ConditionalOnProperty\s*\(([^)]*)\)")
SPRING_BEAN_NAME_PATTERN = re.compile(r"@(Service|Component|Repository|Controller|RestController|Bean)\s*(?:\(\s*(?:value\s*=\s*|name\s*=\s*)?[\"']([^\"']+)[\"']\s*\))?")
SPRING_PRIMARY_PATTERN = re.compile(r"@Primary\b")
SPRING_AOP_PATTERN = re.compile(r"@(Around|Before|After|AfterReturning|AfterThrowing|Pointcut)\s*(?:\(([^)]*)\))?")
SPRING_SCHEDULED_PATTERN = re.compile(r"@Scheduled\s*(?:\(([^)]*)\))?")
SPRING_ASPECT_PATTERN = re.compile(r"@Aspect\b")
SPRING_INTERCEPTOR_PATTERN = re.compile(r"\b(?:implements\s+)?(?:HandlerInterceptor|AsyncHandlerInterceptor)\b")
MESSAGE_LISTENER_PATTERN = re.compile(r"@(KafkaListener|RabbitListener|JmsListener)\s*\(([^)]*)\)")
MESSAGE_SEND_PATTERN = re.compile(r"\b(?:kafkaTemplate|rabbitTemplate|jmsTemplate|streamBridge)\.(?:send|convertAndSend|sendMessage)\s*\(([^)]*)\)", re.IGNORECASE)
EVENT_PUBLISH_PATTERN = re.compile(r"\b(?:publishEvent|eventBus\.post|applicationEventPublisher\.publishEvent)\s*\(([^)]*)\)")
MAVEN_DEPENDENCY_BLOCK_PATTERN = re.compile(r"<dependency\b[^>]*>(.*?)</dependency>", re.IGNORECASE | re.DOTALL)
MAVEN_TAG_PATTERN = re.compile(r"<([A-Za-z0-9_.-]+)>\s*([^<]+?)\s*</\1>", re.IGNORECASE)
GRADLE_COORDINATE_PATTERN = re.compile(
    r"\b(?:implementation|api|compileOnly|runtimeOnly|testImplementation|classpath)\s*(?:\(|\s)\s*[\"']([A-Za-z0-9_.-]+):([A-Za-z0-9_.-]+)(?::[^\"']+)?[\"']"
)
GRADLE_PROJECT_DEPENDENCY_PATTERN = re.compile(r"\b(?:implementation|api|compileOnly|runtimeOnly|testImplementation)\s*(?:\(|\s).*?project\([\"'](:?[^\"')]+)[\"']\)")
GRADLE_INCLUDE_PATTERN = re.compile(r"\binclude\s+(.+)")
RUNTIME_TRACE_FILENAMES = {
    "source-code-qa-runtime-traces.jsonl",
    "source_code_qa_runtime_traces.jsonl",
    "runtime-traces.jsonl",
    "runtime_traces.jsonl",
}
TEST_PATH_MARKERS = (
    "/test/",
    "/tests/",
    "__tests__/",
    ".spec.",
    ".test.",
    "_test.",
    "test_",
    "spec_",
)
TEST_ANNOTATION_PATTERN = re.compile(
    r"@(?:Test|ParameterizedTest|RepeatedTest|SpringBootTest|WebMvcTest|DataJpaTest|ExtendWith|RunWith)\b"
)
TEST_ASSERTION_PATTERN = re.compile(
    r"\b(?:assert[A-Z][A-Za-z0-9_]*|assertThat|expect|verify|when|given|then|should|self\.assert[A-Z][A-Za-z0-9_]*)\s*\("
)
OPERATIONAL_BOUNDARY_PATTERN = re.compile(
    r"@(Transactional|Cacheable|CacheEvict|CachePut|Async|Retryable|Recover|CircuitBreaker|RateLimiter|Bulkhead|TimeLimiter|SchedulerLock|PreAuthorize|PostAuthorize)\b"
    r"(?:\(([^)]*)\))?"
)
FTS_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_./:-]{2,}")
DECLARATION_HINT_PATTERN = re.compile(
    r"^\s*(class|def|function|func|interface|type|enum|const|let|var|public|private|protected|static|final)\b",
    re.IGNORECASE,
)
PATHISH_PATTERN = re.compile(r"/[A-Za-z0-9_./:-]{3,}")
CALL_SYMBOL_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
MEMBER_CALL_PATTERN = re.compile(r"\b([a-z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\(")
CLASS_CONSTRUCTION_PATTERN = re.compile(r"\bnew\s+([A-Za-z_][A-Za-z0-9_]*)\b")
FIELD_OR_PARAM_TYPE_PATTERN = re.compile(
    r"\b(?:private|protected|public|final|static|@Autowired|@Resource)?\s*"
    r"([A-Z][A-Za-z0-9_]*(?:Service|Client|Integration|Repository|Dao|DAO|Mapper|Adapter|Gateway|Provider|Facade))\b"
)
FIELD_VAR_TYPE_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9_]*(?:Service|Client|Integration|Repository|Dao|DAO|Mapper|Adapter|Gateway|Provider|Facade))\s+([a-z][A-Za-z0-9_]*)\b"
)
GENERIC_FIELD_VAR_TYPE_PATTERN = re.compile(
    r"\b(?:private|protected|public|final|static|\s)*"
    r"[A-Z][A-Za-z0-9_]*(?:<([^;=(){}]+)>)\s+([a-z][A-Za-z0-9_]*)\b"
)
SERVICE_LIKE_TYPE_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9_]*(?:Service|Client|Integration|Repository|Dao|DAO|Mapper|Adapter|Gateway|Provider|Facade))\b"
)
STREAM_LAMBDA_PATTERN = re.compile(
    r"\b([a-z][A-Za-z0-9_]*)\s*(?:\.values\s*\(\s*\))?(?:\.stream\s*\(\s*\))?"
    r"\.(?:forEach|map|flatMap|filter|anyMatch|allMatch|noneMatch|peek)\s*\(\s*\(?\s*([a-z][A-Za-z0-9_]*)\s*\)?\s*->"
)
PROVIDER_CHAIN_CALL_PATTERN = re.compile(
    r"\b([a-z][A-Za-z0-9_]*)\.(?:getObject|getIfAvailable|getIfUnique|get)\s*\(\s*\)\.([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
THIS_FIELD_ASSIGNMENT_PATTERN = re.compile(r"\bthis\.([a-z][A-Za-z0-9_]*)\s*=\s*([a-z][A-Za-z0-9_]*)\s*;")
STATIC_QA_RULES: tuple[dict[str, Any], ...] = (
    {
        "kind": "hardcoded_secret",
        "severity": "high",
        "score": 214,
        "pattern": re.compile(r"\b(password|passwd|secret|token|api[_-]?key|access[_-]?key)\b\s*[:=]\s*[\"'][^\"'${}]{4,}[\"']", re.IGNORECASE),
        "reason": "hardcoded credential-like value",
    },
    {
        "kind": "sql_string_concatenation",
        "severity": "high",
        "score": 208,
        "pattern": re.compile(r"\b(select|insert|update|delete)\b[^;\n]{0,180}(?:\+|\.format\s*\(|%s|\$\{)", re.IGNORECASE),
        "reason": "SQL appears to be assembled with string interpolation/concatenation",
    },
    {
        "kind": "command_execution",
        "severity": "high",
        "score": 204,
        "pattern": re.compile(r"\b(Runtime\.getRuntime\(\)\.exec|ProcessBuilder|subprocess\.(?:Popen|call|run)|os\.system)\s*\(", re.IGNORECASE),
        "reason": "command execution path needs input validation review",
    },
    {
        "kind": "unsafe_eval_exec",
        "severity": "high",
        "score": 204,
        "pattern": re.compile(r"\b(eval|exec)\s*\(", re.IGNORECASE),
        "reason": "dynamic code execution is risky",
    },
    {
        "kind": "unsafe_deserialization",
        "severity": "high",
        "score": 200,
        "pattern": re.compile(r"\b(ObjectInputStream|pickle\.loads|yaml\.load)\s*\(", re.IGNORECASE),
        "reason": "unsafe deserialization needs trust-boundary review",
    },
    {
        "kind": "broad_exception",
        "severity": "medium",
        "score": 176,
        "pattern": re.compile(r"\b(?:catch\s*\(\s*(?:Exception|Throwable)\b|except\s+(?:Exception|BaseException)\b)", re.IGNORECASE),
        "reason": "broad exception handling can hide specific failures",
    },
    {
        "kind": "swallowed_exception",
        "severity": "medium",
        "score": 174,
        "pattern": re.compile(r"\b(?:printStackTrace\s*\(|except\s+[^:]+:\s*pass\b|catch\s*\([^)]*\)\s*\{\s*\})", re.IGNORECASE),
        "reason": "exception appears logged weakly or swallowed",
    },
    {
        "kind": "debug_output",
        "severity": "low",
        "score": 140,
        "pattern": re.compile(r"\b(System\.out\.print(?:ln)?|console\.log|print)\s*\(", re.IGNORECASE),
        "reason": "debug output may leak operational details or noisy logs",
    },
    {
        "kind": "todo_fixme",
        "severity": "low",
        "score": 132,
        "pattern": re.compile(r"\b(TODO|FIXME|XXX)\b", re.IGNORECASE),
        "reason": "unfinished implementation marker",
    },
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
    "credit", "risk", "sg", "ph", "id", "used", "uses", "using",
}
DATA_SOURCE_HINTS = (
    "datasource", "data source", "source", "sources", "upstream", "table", "jdbc",
    "queryfor", "select", "repository", "mapper", "dao", "client",
    "integration", "provider", "gateway", "api", "userinfo", "customerinfo",
    " read from ", " write to ", " written to ", " comes from ",
    " loaded from ", " fetched from ", " persisted to ",
    "数据源", "来源", "上游", "表", "数据库", "查哪张表", "从哪里来", "哪里取数",
    "读取", "写入",
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
API_HINTS = (*API_HINTS, "接口", "端点", "路由", "入口", "请求", "调用")
CONFIG_HINTS = ("config", "configuration", "property", "properties", "yaml", "yml", "env", "setting", "feature", "flag")
CONFIG_HINTS = (*CONFIG_HINTS, "配置", "开关", "参数", "环境变量", "属性", "在哪里配", "怎么配置")
MODULE_DEPENDENCY_HINTS = (
    "dependency", "dependencies", "depend on", "module", "maven", "gradle", "pom",
    "artifact", "artifactid", "groupid", "package.json", "npm", "yarn", "pnpm",
    "依赖", "模块", "哪个包", "哪个模块",
)
ERROR_HINTS = ("error", "exception", "failed", "failure", "stacktrace", "status", "code", "timeout", "报错", "异常", "失败", "超时", "错误")
RULE_HINTS = ("rule", "condition", "logic", "validate", "validation", "permission", "access", "approval", "eligible", "规则", "条件", "逻辑", "校验", "权限", "审批", "准入")
STATIC_QA_HINTS = (
    "static qa", "static analysis", "code quality", "code smell", "smell", "bug", "bugs",
    "risk", "risks", "security", "vulnerability", "vulnerabilities", "unsafe",
    "hardcoded", "secret", "password", "token", "sql injection", "injection",
    "empty catch", "swallow", "broad exception", "todo", "fixme",
    "静态", "代码质量", "风险", "安全", "漏洞", "硬编码", "密码", "令牌", "注入",
)
IMPACT_ANALYSIS_HINTS = (
    "impact", "impacted", "affect", "affected", "blast radius", "blast-radius",
    "change impact", "if change", "if changed", "who calls", "callers",
    "callees", "upstream", "downstream", "usage", "usages", "dependents",
    "depends on", "what breaks", "regression", "side effect", "side effects",
    "影响", "影响面", "改了会", "谁调用", "调用方", "被谁用", "上游", "下游",
    "依赖方", "会坏", "回归", "副作用",
)
TEST_COVERAGE_HINTS = (
    "test", "tests", "tested", "testing", "coverage", "covered", "unit test",
    "integration test", "spec", "specs", "junit", "pytest", "jest", "mocha",
    "assert", "mockito", "mock", "verify",
    "测试", "覆盖", "单测", "集成测试", "断言", "mock", "有没有测",
)
OPERATIONAL_BOUNDARY_HINTS = (
    "transaction", "transactional", "rollback", "commit", "cache", "cached",
    "cacheable", "cacheevict", "async", "asynchronous", "retry", "retryable",
    "circuit breaker", "circuitbreaker", "rate limit", "ratelimiter",
    "bulkhead", "timeout", "timelimiter", "lock", "schedulerlock",
    "preauthorize", "postauthorize", "authorization", "permission boundary",
    "事务", "回滚", "提交", "缓存", "异步", "重试", "熔断", "限流", "超时",
    "锁", "鉴权", "授权", "权限边界",
)
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
ANSWER_POLICY_REGISTRY = {
    "data_source": {
        "label": "Data source evidence",
        "required_any": ["data_sources"],
        "supporting_any": ["field_population", "data_carriers", "downstream_components"],
        "missing": "concrete upstream source/table/API/repository evidence beyond DTO fields",
    },
    "api": {
        "label": "API surface evidence",
        "required_any": ["api_or_config", "entry_points"],
        "supporting_any": ["downstream_components"],
        "missing": "endpoint/client/API evidence",
    },
    "config": {
        "label": "Configuration evidence",
        "required_any": ["api_or_config"],
        "supporting_any": ["entry_points"],
        "missing": "config/property evidence",
    },
    "module_dependency": {
        "label": "Module dependency evidence",
        "required_any": ["module_dependencies", "api_or_config"],
        "supporting_any": ["external_dependencies", "downstream_components", "entry_points"],
        "missing": "build-file dependency evidence such as Maven, Gradle, npm, or module artifact coordinates",
    },
    "message_flow": {
        "label": "Message flow evidence",
        "required_any": ["message_flows"],
        "supporting_any": ["api_or_config", "entry_points", "downstream_components"],
        "missing": "message producer/consumer evidence such as Kafka topic, queue, publisher, or listener",
    },
    "logic": {
        "label": "Rule or error logic evidence",
        "required_any": ["rule_or_error_logic", "entry_points"],
        "supporting_any": ["downstream_components"],
        "missing": "rule/error handling evidence",
    },
    "general": {
        "label": "Specific code evidence",
        "required_any": ["entry_points", "downstream_components", "data_sources", "api_or_config", "rule_or_error_logic"],
        "supporting_any": [],
        "missing": "specific code evidence",
    },
    "static_qa": {
        "label": "Static QA evidence",
        "required_any": ["static_findings"],
        "supporting_any": ["entry_points", "rule_or_error_logic"],
        "missing": "static QA finding evidence such as risky exception handling, hardcoded secret, unsafe SQL, command execution, or TODO/FIXME",
    },
    "impact_analysis": {
        "label": "Impact analysis evidence",
        "required_any": ["impact_surfaces"],
        "supporting_any": ["entry_points", "downstream_components", "api_or_config", "data_sources"],
        "missing": "caller/callee or graph evidence showing upstream and downstream impact surfaces",
    },
    "test_coverage": {
        "label": "Test coverage evidence",
        "required_any": ["test_coverage"],
        "supporting_any": ["entry_points", "downstream_components"],
        "missing": "test file, test case, assertion, mock, or verification evidence covering the target symbol",
    },
    "operational_boundary": {
        "label": "Operational boundary evidence",
        "required_any": ["operational_boundaries"],
        "supporting_any": ["entry_points", "api_or_config", "downstream_components"],
        "missing": "transaction/cache/async/retry/circuit-breaker/security boundary annotation evidence",
    },
}


@dataclass(frozen=True)
class RepositoryEntry:
    display_name: str
    url: str


class SourceCodeQAIndexUnavailable(sqlite3.OperationalError):
    """Raised when a user query cannot use a ready, prebuilt code index."""


@dataclass(frozen=True)
class LLMGenerateResult:
    payload: dict[str, Any]
    usage: dict[str, Any]
    model: str
    attempts: int
    latency_ms: int = 0
    attempt_log: tuple[dict[str, Any], ...] = ()


class SourceCodeQALLMError(ToolError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        provider_status: str = "",
        retryable: bool = False,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.provider_status = provider_status
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds


class SourceCodeQAEmbeddingProvider:
    name = "local_token_hybrid"

    def ready(self) -> bool:
        return True

    def embed_texts(self, texts: list[str], *, task_type: str | None = None) -> list[list[float]]:
        del task_type
        del texts
        return []

    def public_config(self) -> dict[str, Any]:
        return {"provider": self.name, "ready": self.ready()}


class OpenAICompatibleEmbeddingProvider(SourceCodeQAEmbeddingProvider):
    name = "openai_compatible"

    def __init__(self, *, api_key: str, api_base_url: str, model: str) -> None:
        self.api_key = str(api_key or "").strip()
        self.api_base_url = str(api_base_url or OPENAI_COMPATIBLE_API_BASE_URL).rstrip("/")
        self.model = str(model or "").strip()

    def ready(self) -> bool:
        return bool(self.api_key and self.model and not self.model.startswith("local-"))

    def embed_texts(self, texts: list[str], *, task_type: str | None = None) -> list[list[float]]:
        del task_type
        if not texts:
            return []
        if not self.ready():
            raise ToolError("Source Code Q&A embedding provider is not configured.")
        response = requests.post(
            f"{self.api_base_url}/embeddings",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={"model": self.model, "input": texts},
            timeout=90,
        )
        if not response.ok:
            detail = self._sanitize_error_detail(response.text)
            raise ToolError(f"Source Code Q&A embedding generation failed. {detail[:500]}")
        payload = response.json()
        rows = payload.get("data") or []
        rows.sort(key=lambda item: int(item.get("index") or 0))
        return [[float(value) for value in row.get("embedding") or []] for row in rows]

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "model": self.model,
            "api_base_url": self.api_base_url,
        }

    def _sanitize_error_detail(self, detail: str) -> str:
        sanitized = str(detail or "")
        if self.api_key:
            sanitized = sanitized.replace(self.api_key, "***")
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", sanitized)


class VertexAIEmbeddingProvider(SourceCodeQAEmbeddingProvider):
    name = LLM_PROVIDER_VERTEX_AI
    _SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)

    def __init__(
        self,
        *,
        credentials_file: str | None = None,
        project_id: str | None = None,
        location: str = "global",
        model: str = DEFAULT_VERTEX_EMBEDDING_MODEL,
        output_dimensionality: int = 768,
    ) -> None:
        self.credentials_file = str(credentials_file or "").strip()
        self.project_id = str(project_id or "").strip()
        self.location = str(location or "global").strip() or "global"
        self.model = str(model or DEFAULT_VERTEX_EMBEDDING_MODEL).strip() or DEFAULT_VERTEX_EMBEDDING_MODEL
        self.output_dimensionality = max(0, int(output_dimensionality or 0))

    def ready(self) -> bool:
        return bool(self._credentials_path() and self._resolved_project_id() and self.model)

    def embed_texts(self, texts: list[str], *, task_type: str | None = None) -> list[list[float]]:
        if not texts:
            return []
        if not self.ready():
            raise ToolError(
                "Vertex AI embedding provider is not configured. Set SOURCE_CODE_QA_VERTEX_CREDENTIALS_FILE "
                "or GOOGLE_APPLICATION_CREDENTIALS, plus SOURCE_CODE_QA_VERTEX_PROJECT_ID when needed."
            )
        task = str(task_type or VERTEX_EMBEDDING_DOCUMENT_TASK).strip() or VERTEX_EMBEDDING_DOCUMENT_TASK
        embeddings: list[list[float]] = []
        for text in texts:
            response = requests.post(
                self._predict_url(),
                headers={
                    "Authorization": f"Bearer {self._access_token()}",
                    "Content-Type": "application/json",
                },
                json=self._predict_payload(str(text or "")[:8000], task),
                timeout=90,
            )
            if not response.ok:
                detail = self._sanitize_error_detail(response.text)
                raise ToolError(f"Vertex AI embedding generation failed. {detail[:500]}")
            embeddings.append(self._embedding_from_response(response.json()))
        return embeddings

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "model": self.model,
            "project_id": self._resolved_project_id(),
            "location": self.location,
            "credentials_configured": bool(self._credentials_path()),
            "output_dimensionality": self.output_dimensionality,
        }

    def _credentials_path(self) -> Path | None:
        if not self.credentials_file:
            return None
        path = Path(self.credentials_file).expanduser()
        return path if path.exists() and path.is_file() else None

    def _resolved_project_id(self) -> str:
        if self.project_id:
            return self.project_id
        path = self._credentials_path()
        if path is None:
            return ""
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""
        return str(payload.get("project_id") or "").strip()

    def _access_token(self) -> str:
        path = self._credentials_path()
        if path is None:
            raise ToolError("Vertex AI credentials file is missing or unreadable.")
        credentials = service_account.Credentials.from_service_account_file(
            str(path),
            scopes=list(self._SCOPES),
        )
        credentials.refresh(GoogleAuthRequest())
        token = str(getattr(credentials, "token", "") or "").strip()
        if not token:
            raise ToolError("Vertex AI service account did not return an OAuth access token.")
        return token

    def _predict_url(self) -> str:
        location = self.location
        base_url = VERTEX_AI_GLOBAL_API_BASE_URL if location == "global" else f"https://{location}-aiplatform.googleapis.com/v1"
        return (
            f"{base_url}/projects/{self._resolved_project_id()}/locations/{location}"
            f"/publishers/google/models/{self.model}:predict"
        )

    def _predict_payload(self, text: str, task_type: str) -> dict[str, Any]:
        instance: dict[str, Any] = {"content": text, "task_type": task_type}
        parameters: dict[str, Any] = {}
        if self.output_dimensionality > 0:
            parameters["outputDimensionality"] = self.output_dimensionality
        return {"instances": [instance], "parameters": parameters}

    @staticmethod
    def _embedding_from_response(payload: dict[str, Any]) -> list[float]:
        predictions = payload.get("predictions") or []
        if not predictions:
            raise ToolError("Vertex AI embedding provider returned no predictions.")
        first = predictions[0] or {}
        candidates = [
            ((first.get("embeddings") or {}).get("values") if isinstance(first, dict) else None),
            ((first.get("embedding") or {}).get("values") if isinstance(first, dict) else None),
            first.get("values") if isinstance(first, dict) else None,
        ]
        values = next((item for item in candidates if isinstance(item, list)), None)
        if values is None:
            raise ToolError("Vertex AI embedding provider returned an unreadable embedding.")
        return [float(value) for value in values]

    @staticmethod
    def _sanitize_error_detail(detail: str) -> str:
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", str(detail or ""))


class SourceCodeQALLMProvider:
    name = "unknown"

    def ready(self) -> bool:
        return False

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        raise ToolError("The configured Source Code Q&A LLM provider is not supported yet.")

    def extract_text(self, payload: dict[str, Any]) -> str:
        raise ToolError("The configured Source Code Q&A LLM provider returned an unreadable answer.")

    def public_config(self) -> dict[str, Any]:
        return {"provider": self.name, "ready": self.ready()}


class CodexCliBridgeSourceCodeQALLMProvider(SourceCodeQALLMProvider):
    name = LLM_PROVIDER_CODEX_CLI_BRIDGE
    _semaphore_lock = threading.Lock()
    _run_semaphore = threading.BoundedSemaphore(1)
    _semaphore_limit = 1

    def __init__(
        self,
        *,
        workspace_root: Path,
        timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        concurrency_limit: int = 1,
        session_mode: str = CODEX_SESSION_MODE_EPHEMERAL,
        codex_binary: str | None = None,
    ) -> None:
        self.workspace_root = Path(workspace_root)
        self.timeout_seconds = max(10, int(timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.concurrency_limit = max(1, min(int(concurrency_limit or 1), 4))
        normalized_session_mode = str(session_mode or CODEX_SESSION_MODE_EPHEMERAL).strip().lower()
        self.session_mode = normalized_session_mode if normalized_session_mode in {CODEX_SESSION_MODE_EPHEMERAL, CODEX_SESSION_MODE_RESUME} else CODEX_SESSION_MODE_EPHEMERAL
        self.codex_binary = str(codex_binary or os.getenv("SOURCE_CODE_QA_CODEX_BINARY") or "codex").strip() or "codex"

    @classmethod
    def _semaphore_for_limit(cls, limit: int) -> threading.BoundedSemaphore:
        normalized_limit = max(1, min(int(limit or 1), 4))
        with cls._semaphore_lock:
            if cls._semaphore_limit != normalized_limit:
                cls._run_semaphore = threading.BoundedSemaphore(normalized_limit)
                cls._semaphore_limit = normalized_limit
            return cls._run_semaphore

    def ready(self) -> bool:
        if not shutil.which(self.codex_binary):
            return False
        try:
            result = subprocess.run(
                [self.codex_binary, "login", "status"],
                cwd=str(self.workspace_root),
                text=True,
                capture_output=True,
                env=self._codex_env(),
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        output = f"{result.stdout}\n{result.stderr}"
        return result.returncode == 0 and "Logged in using ChatGPT" in output

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        del fallback_model
        if not self.ready():
            raise ToolError("Codex is unavailable. Run `codex login` with ChatGPT on this server before using Codex mode.")
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        progress_callback = payload.get("_progress_callback") if callable(payload.get("_progress_callback")) else None
        prompt = self._prompt_from_gemini_payload(payload)
        image_paths = [
            str(path or "").strip()
            for path in (payload.get("_codex_image_paths") or [])
            if str(path or "").strip()
        ]
        for image_path in image_paths:
            path = Path(image_path)
            if not path.exists() or not path.is_file():
                raise ToolError(f"Codex image attachment is missing or unreadable: {image_path}")
        started_at = time.time()
        attempt_started = time.time()
        model = str(primary_model or "codex-cli").strip() or "codex-cli"
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as output_file:
            prompt_mode = str(payload.get("codex_prompt_mode") or "").strip()
            codex_cli_session_id = str(payload.get("codex_cli_session_id") or "").strip()
            command, command_mode = self._build_codex_command(
                output_file=output_file.name,
                model=model,
                session_id=codex_cli_session_id,
                image_paths=image_paths,
            )
            queue_started = time.time()
            queue_wait_ms = 0
            try:
                semaphore = self._semaphore_for_limit(self.concurrency_limit)
                if progress_callback:
                    progress_callback(
                        "codex_queue",
                        f"Waiting for Codex slot ({self.concurrency_limit} max concurrent).",
                        0,
                        1,
                    )
                semaphore.acquire()
                queue_wait_ms = int((time.time() - queue_started) * 1000)
                try:
                    if progress_callback and queue_wait_ms > 250:
                        progress_callback(
                            "codex_queue",
                            f"Codex slot acquired after {queue_wait_ms / 1000:.1f}s.",
                            1,
                            1,
                        )
                    if progress_callback:
                        result = self._run_codex_streaming(
                            command=command,
                            prompt=prompt,
                            progress_callback=progress_callback,
                        )
                    else:
                        result = subprocess.run(
                            command,
                            input=prompt,
                            cwd=str(self.workspace_root),
                            text=True,
                            capture_output=True,
                            env=self._codex_env(),
                            timeout=self.timeout_seconds,
                            check=False,
                        )
                finally:
                    semaphore.release()
            except subprocess.TimeoutExpired as error:
                raise ToolError(f"Codex unavailable; used code search fallback. Codex CLI timed out after {self.timeout_seconds}s.") from error
            except OSError as error:
                raise ToolError(f"Codex unavailable; used code search fallback. {error}") from error
            output_file.seek(0)
            answer = output_file.read().strip()
        if result.returncode != 0:
            detail = self._sanitize_cli_output(f"{result.stderr}\n{result.stdout}")
            raise ToolError(f"Codex unavailable; used code search fallback. Codex CLI exited with {result.returncode}. {detail[:500]}")
        if not answer:
            answer = self._extract_last_json_event_message(result.stdout)
        if not answer:
            raise ToolError("Codex unavailable; used code search fallback. Codex CLI returned no readable answer.")
        latency_ms = int((time.time() - started_at) * 1000)
        trace = self._extract_codex_trace(result.stdout, result.stderr)
        trace.update(
            {
                "session_mode": self.session_mode,
                "command_mode": command_mode,
                "session_id": trace.get("session_id") or codex_cli_session_id,
                "exit_code": result.returncode,
                "latency_ms": latency_ms,
                "timeout": False,
            }
        )
        return LLMGenerateResult(
            payload={
                "text": answer,
                "finish_reason": "codex_cli_completed",
                "codex_cli_trace": trace,
            },
            usage={},
            model=model,
            attempts=1,
            latency_ms=latency_ms,
            attempt_log=(
                {
                    "model": model,
                    "attempt": 1,
                    "status": "ok",
                    "retryable": False,
                    "latency_ms": int((time.time() - attempt_started) * 1000),
                    "provider": self.name,
                    "exit_code": result.returncode,
                    "timeout": False,
                    "workspace_root": str(self.workspace_root),
                    "prompt_mode": prompt_mode,
                    "concurrency_limit": self.concurrency_limit,
                    "queue_wait_ms": queue_wait_ms,
                    "session_mode": self.session_mode,
                    "command_mode": command_mode,
                    "codex_cli_session_id": trace.get("session_id") or "",
                    "command": self._command_summary(command),
                },
            ),
        )

    def _build_codex_command(
        self,
        *,
        output_file: str,
        model: str,
        session_id: str = "",
        image_paths: list[str] | None = None,
    ) -> tuple[list[str], str]:
        image_args: list[str] = []
        for image_path in image_paths or []:
            image_args.extend(["--image", str(image_path)])
        if self.session_mode == CODEX_SESSION_MODE_RESUME:
            command = [self.codex_binary, "exec"]
            if model not in {"codex-cli", "codex"}:
                command.extend(["--model", model])
            command.extend(image_args)
            if session_id:
                command.extend(
                    [
                        "resume",
                        "--skip-git-repo-check",
                        "--json",
                        "--output-last-message",
                        output_file,
                        session_id,
                        "-",
                    ]
                )
                return command, "resume"
            command.extend(
                [
                    "--cd",
                    str(self.workspace_root),
                    "--skip-git-repo-check",
                    "--sandbox",
                    "read-only",
                    "--json",
                    "--output-last-message",
                    output_file,
                    "-",
                ]
            )
            return command, "new_persistent"
        command = [
            self.codex_binary,
            "exec",
            "--cd",
            str(self.workspace_root),
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--ephemeral",
            "--json",
            "--output-last-message",
            output_file,
            "-",
        ]
        if model not in {"codex-cli", "codex"}:
            command[2:2] = ["--model", model]
        if image_args:
            insert_at = 2
            if model not in {"codex-cli", "codex"}:
                insert_at = 4
            command[insert_at:insert_at] = image_args
        return command, "ephemeral"

    def _run_codex_streaming(self, *, command: list[str], prompt: str, progress_callback: Any) -> Any:
        process = subprocess.Popen(
            command,
            cwd=str(self.workspace_root),
            env=self._codex_env(),
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
        )
        output_queue: queue.Queue[tuple[str, str]] = queue.Queue()

        def read_pipe(name: str, pipe: Any) -> None:
            try:
                for line in iter(pipe.readline, ""):
                    output_queue.put((name, line))
            finally:
                try:
                    pipe.close()
                except Exception:
                    pass

        stdout_thread = threading.Thread(target=read_pipe, args=("stdout", process.stdout), daemon=True)
        stderr_thread = threading.Thread(target=read_pipe, args=("stderr", process.stderr), daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        if process.stdin is not None:
            process.stdin.write(prompt)
            process.stdin.close()

        started_at = time.time()
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []
        last_message = ""
        while process.poll() is None or not output_queue.empty():
            if time.time() - started_at > self.timeout_seconds:
                process.kill()
                raise subprocess.TimeoutExpired(command, self.timeout_seconds)
            try:
                stream_name, line = output_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if stream_name == "stdout":
                stdout_lines.append(line)
                message = self._extract_progress_json_event_message(line)
                if message and message != last_message:
                    last_message = message
                    try:
                        progress_callback("codex_stream", message[-900:], 0, 0)
                    except Exception:
                        pass
            else:
                stderr_lines.append(line)
        stdout_thread.join(timeout=0.2)
        stderr_thread.join(timeout=0.2)
        return subprocess.CompletedProcess(
            command,
            process.returncode,
            stdout="".join(stdout_lines),
            stderr="".join(stderr_lines),
        )

    def _codex_env(self) -> dict[str, str]:
        env = dict(os.environ)
        path_parts = [part for part in str(env.get("PATH") or "").split(os.pathsep) if part]
        for tool_dir in self._codex_tool_path_dirs():
            if tool_dir not in path_parts:
                path_parts.insert(0, tool_dir)
        if path_parts:
            env["PATH"] = os.pathsep.join(path_parts)
        return env

    @staticmethod
    def _codex_tool_path_dirs() -> list[str]:
        dirs: list[str] = []
        detected = shutil.which("rg")
        if detected:
            dirs.append(str(Path(detected).parent))
        for candidate in (
            "/Applications/Codex.app/Contents/Resources/rg",
            "/opt/homebrew/bin/rg",
            "/usr/local/bin/rg",
            "/usr/bin/rg",
        ):
            if Path(candidate).exists():
                tool_dir = str(Path(candidate).parent)
                if tool_dir not in dirs:
                    dirs.append(tool_dir)
        return dirs

    @staticmethod
    def _codex_rg_hint() -> str:
        detected = shutil.which("rg")
        if detected:
            return str(detected)
        for candidate in (
            "/Applications/Codex.app/Contents/Resources/rg",
            "/opt/homebrew/bin/rg",
            "/usr/local/bin/rg",
            "/usr/bin/rg",
        ):
            if Path(candidate).exists():
                return candidate
        return ""

    def extract_text(self, payload: dict[str, Any]) -> str:
        text = str(payload.get("text") or "").strip()
        if text:
            return text
        raise ToolError("Codex CLI returned no readable answer.")

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "workspace_root": str(self.workspace_root),
            "runtime": {
                "timeout_seconds": self.timeout_seconds,
                "concurrency": self.concurrency_limit,
                "sandbox": "read-only",
                "session_mode": self.session_mode,
            },
        }

    @staticmethod
    def _prompt_from_gemini_payload(payload: dict[str, Any]) -> str:
        system_text = "\n".join(
            str(part.get("text") or "").strip()
            for part in (payload.get("systemInstruction") or {}).get("parts") or []
            if str(part.get("text") or "").strip()
        )
        user_parts: list[str] = []
        for content in payload.get("contents") or []:
            for part in content.get("parts") or []:
                text = str(part.get("text") or "").strip()
                if text:
                    user_parts.append(text)
        user_text = "\n\n".join(user_parts)
        return (
            f"{system_text}\n\n"
            "Codex CLI bridge policy:\n"
            "- Read only from the provided repository workspace and retrieval evidence.\n"
            "- Do not modify files, create commits, deploy, install dependencies, or run write commands.\n"
            f"- Tool availability: `rg` is expected on PATH. If not, call it by absolute path: {CodexCliBridgeSourceCodeQALLMProvider._codex_rg_hint() or 'not detected; use grep -R/find fallback'}.\n"
            "- Return a concise answer in the requested JSON shape when possible.\n\n"
            f"{user_text}"
        ).strip()

    @staticmethod
    def _extract_last_json_event_message(output: str) -> str:
        answer = ""
        for raw_line in str(output or "").splitlines():
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            for key in ("message", "text", "output_text"):
                value = event.get(key)
                if isinstance(value, str) and value.strip():
                    answer = value.strip()
            item = event.get("item")
            if isinstance(item, dict):
                content = item.get("content")
                if isinstance(content, str) and content.strip():
                    answer = content.strip()
        return answer

    @staticmethod
    def _extract_progress_json_event_message(output: str) -> str:
        try:
            event = json.loads(str(output or ""))
        except json.JSONDecodeError:
            return ""
        if not isinstance(event, dict):
            return ""
        candidates: list[str] = []
        for key in ("message", "text", "output_text", "delta"):
            value = event.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())
        item = event.get("item")
        if isinstance(item, dict):
            for key in ("text", "message", "output_text", "delta", "content"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    candidates.append(value.strip())
            content = item.get("content")
            if isinstance(content, list):
                for part in content:
                    if isinstance(part, dict):
                        value = part.get("text") or part.get("output_text")
                        if isinstance(value, str) and value.strip():
                            candidates.append(value.strip())
        return candidates[-1] if candidates else ""

    @classmethod
    def _extract_codex_trace(cls, stdout: str, stderr: str) -> dict[str, Any]:
        stream_messages: list[str] = []
        command_summaries: list[str] = []
        inspected_paths: list[str] = []
        session_id = ""
        path_pattern = re.compile(
            r"([A-Za-z0-9_.@/$-]+/(?:src|config|spec|app|web|test|tests|resources|pages|components|mapper)/"
            r"[A-Za-z0-9_./$@-]+\.(?:java|xml|kt|groovy|md|sql|yml|yaml|properties|json|ts|tsx|js|py))"
        )
        command_pattern = re.compile(r"\b(rg|grep|find|sed|nl|cat|ls)\b(?:\s+[^`'\n]{0,220})?")
        seen_messages: set[str] = set()
        seen_commands: set[str] = set()
        seen_paths: set[str] = set()
        for raw_line in f"{stdout or ''}\n{stderr or ''}".splitlines():
            message = cls._extract_progress_json_event_message(raw_line)
            if message and message not in seen_messages:
                seen_messages.add(message)
                stream_messages.append(message[:1200])
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError:
                event = {}
            if isinstance(event, dict):
                for key in ("session_id", "conversation_id"):
                    value = str(event.get(key) or "").strip()
                    if value:
                        session_id = value
                item = event.get("item")
                if isinstance(item, dict):
                    for key in ("session_id", "conversation_id", "id"):
                        value = str(item.get(key) or "").strip()
                        if value and ("session" in key or str(item.get("type") or "").lower().find("session") >= 0):
                            session_id = value
                    for key in ("command", "cmd"):
                        value = item.get(key)
                        if isinstance(value, str) and value.strip() and value not in seen_commands:
                            seen_commands.add(value)
                            command_summaries.append(value[:240])
                for key in ("command", "cmd"):
                    value = event.get(key)
                    if isinstance(value, str) and value.strip() and value not in seen_commands:
                        seen_commands.add(value)
                        command_summaries.append(value[:240])
            for match in command_pattern.finditer(raw_line):
                command = match.group(0).strip()
                if command and command not in seen_commands:
                    seen_commands.add(command)
                    command_summaries.append(command[:240])
            for match in path_pattern.finditer(raw_line):
                path = match.group(1).strip()
                if path and path not in seen_paths:
                    seen_paths.add(path)
                    inspected_paths.append(path[:300])
        return {
            "stream_messages": stream_messages[-40:],
            "command_summaries": command_summaries[-30:],
            "probable_inspected_files": inspected_paths[-40:],
            "session_id": session_id,
        }

    @staticmethod
    def _sanitize_cli_output(output: str) -> str:
        return re.sub(r"\s+", " ", str(output or "").strip())

    @staticmethod
    def _command_summary(command: list[str]) -> list[str]:
        summarized = list(command)
        if "--output-last-message" in summarized:
            index = summarized.index("--output-last-message")
            if index + 1 < len(summarized):
                summarized[index + 1] = "<output-file>"
        return summarized


class UnsupportedSourceCodeQALLMProvider(SourceCodeQALLMProvider):
    def __init__(self, name: str) -> None:
        self.name = str(name or "unknown")

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        raise ToolError(f"Source Code Q&A LLM provider {self.name!r} is not supported yet.")


class GeminiSourceCodeQALLMProvider(SourceCodeQALLMProvider):
    name = LLM_PROVIDER_GEMINI

    def __init__(
        self,
        *,
        api_key: str,
        api_base_url: str = GEMINI_API_BASE_URL,
        timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_LLM_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_LLM_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_LLM_MAX_BACKOFF_SECONDS,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.api_base_url = str(api_base_url or GEMINI_API_BASE_URL).rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.max_retries = max(0, int(max_retries or 0))
        self.backoff_seconds = max(0.0, float(backoff_seconds or 0.0))
        self.max_backoff_seconds = max(self.backoff_seconds, float(max_backoff_seconds or self.backoff_seconds or DEFAULT_LLM_MAX_BACKOFF_SECONDS))

    def ready(self) -> bool:
        return bool(self.api_key)

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        if not self.ready():
            raise ToolError("LLM mode is not configured yet. Set SOURCE_CODE_QA_GEMINI_API_KEY or GEMINI_API_KEY on the server first.")
        models = [primary_model]
        if fallback_model and fallback_model not in models:
            models.append(fallback_model)
        retryable_statuses = {429, 500, 502, 503, 504}
        last_error: str | None = None
        last_status_code: int | None = None
        last_provider_status = ""
        last_retry_after: float | None = None
        attempts = 0
        attempt_log: list[dict[str, Any]] = []
        started_at = time.time()
        delays = self._retry_delays()
        for model in models:
            model_delays = list(delays)
            for attempt_index, delay in enumerate(model_delays):
                attempts += 1
                if delay:
                    time.sleep(delay)
                attempt_started = time.time()
                try:
                    response = requests.post(
                        f"{self.api_base_url}/models/{model}:generateContent",
                        params={"key": self.api_key},
                        headers={"Content-Type": "application/json"},
                        json=payload,
                        timeout=self.timeout_seconds,
                    )
                except requests.Timeout as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "timeout",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                except requests.RequestException as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "request_error",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
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
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "ok",
                            "retryable": False,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    return LLMGenerateResult(
                        payload=result,
                        usage=usage,
                        model=model,
                        attempts=attempts,
                        latency_ms=int((time.time() - started_at) * 1000),
                        attempt_log=tuple(attempt_log),
                    )
                status = int(getattr(response, "status_code", 500) or 500)
                detail = self._sanitize_error_detail(response.text)
                last_error = detail
                last_status_code = status
                last_provider_status = self._provider_error_status(detail)
                retryable = status in retryable_statuses
                attempt_log.append(
                    {
                        "model": model,
                        "attempt": attempt_index + 1,
                        "status": status,
                        "retryable": retryable,
                        "latency_ms": int((time.time() - attempt_started) * 1000),
                    }
                )
                if not retryable:
                    raise ToolError(f"Gemini answer generation failed. {detail[:500]}")
                if attempt_index + 1 < len(model_delays):
                    retry_after = self._retry_after_seconds(response)
                    if retry_after is not None:
                        last_retry_after = retry_after
                        model_delays[attempt_index + 1] = retry_after
        raise SourceCodeQALLMError(
            f"Gemini answer generation failed. {str(last_error or 'Model unavailable.')[:500]}",
            status_code=last_status_code,
            provider_status=last_provider_status,
            retryable=bool(last_status_code in retryable_statuses),
            retry_after_seconds=last_retry_after,
        )

    def extract_text(self, payload: dict[str, Any]) -> str:
        candidates = payload.get("candidates") or []
        for candidate in candidates:
            content = candidate.get("content") or {}
            parts = content.get("parts") or []
            texts = [str(part.get("text") or "").strip() for part in parts if str(part.get("text") or "").strip()]
            if texts:
                return "\n".join(texts).strip()
        raise ToolError("Gemini returned no readable answer.")

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "api_base_url": self.api_base_url,
            "runtime": self.runtime_config(),
        }

    def _sanitize_error_detail(self, detail: str) -> str:
        sanitized = str(detail or "")
        if self.api_key:
            sanitized = sanitized.replace(self.api_key, "***")
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", sanitized)

    def runtime_config(self) -> dict[str, Any]:
        return {
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "backoff_seconds": self.backoff_seconds,
            "max_backoff_seconds": self.max_backoff_seconds,
            "retryable_statuses": [429, 500, 502, 503, 504],
        }

    def _retry_delays(self) -> list[float]:
        delays = [0.0]
        for index in range(self.max_retries):
            delay = self.backoff_seconds * (2**index)
            delays.append(min(self.max_backoff_seconds, delay))
        return delays

    def _retry_after_seconds(self, response: Any) -> float | None:
        headers = getattr(response, "headers", {}) or {}
        raw_value = headers.get("Retry-After") if hasattr(headers, "get") else None
        if raw_value is None:
            return None
        try:
            delay = float(str(raw_value).strip())
        except ValueError:
            return None
        return max(0.0, min(self.max_backoff_seconds, delay))

    @staticmethod
    def _provider_error_status(detail: str) -> str:
        try:
            payload = json.loads(str(detail or ""))
        except json.JSONDecodeError:
            return ""
        error = payload.get("error") if isinstance(payload, dict) else {}
        if isinstance(error, dict):
            return str(error.get("status") or "").strip()
        return ""


class VertexAISourceCodeQALLMProvider(GeminiSourceCodeQALLMProvider):
    name = LLM_PROVIDER_VERTEX_AI
    _SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)

    def __init__(
        self,
        *,
        credentials_file: str | None = None,
        project_id: str | None = None,
        location: str = "global",
        timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_LLM_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_LLM_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_LLM_MAX_BACKOFF_SECONDS,
    ) -> None:
        self.credentials_file = str(credentials_file or "").strip()
        self.project_id = str(project_id or "").strip()
        self.location = str(location or "global").strip() or "global"
        self.timeout_seconds = max(5, int(timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.max_retries = max(0, int(max_retries or 0))
        self.backoff_seconds = max(0.0, float(backoff_seconds or 0.0))
        self.max_backoff_seconds = max(self.backoff_seconds, float(max_backoff_seconds or self.backoff_seconds or DEFAULT_LLM_MAX_BACKOFF_SECONDS))

    def ready(self) -> bool:
        return bool(self._credentials_path() and self._resolved_project_id() and self.location)

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        if not self.ready():
            raise ToolError(
                "Vertex AI mode is not configured yet. Set SOURCE_CODE_QA_VERTEX_CREDENTIALS_FILE "
                "or GOOGLE_APPLICATION_CREDENTIALS, plus SOURCE_CODE_QA_VERTEX_PROJECT_ID when the JSON has no project_id."
            )
        access_token = self._access_token()
        models = [primary_model]
        if fallback_model and fallback_model not in models:
            models.append(fallback_model)
        retryable_statuses = {429, 500, 502, 503, 504}
        last_error: str | None = None
        last_status_code: int | None = None
        last_provider_status = ""
        last_retry_after: float | None = None
        attempts = 0
        attempt_log: list[dict[str, Any]] = []
        started_at = time.time()
        delays = self._retry_delays()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        for model in models:
            model_delays = list(delays)
            for attempt_index, delay in enumerate(model_delays):
                attempts += 1
                if delay:
                    time.sleep(delay)
                attempt_started = time.time()
                try:
                    response = requests.post(
                        self._generate_content_url(model),
                        headers=headers,
                        json=self._payload_for_generate_content(payload),
                        timeout=self.timeout_seconds,
                    )
                except requests.Timeout as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "timeout",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                except requests.RequestException as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "request_error",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
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
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "ok",
                            "retryable": False,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    return LLMGenerateResult(
                        payload=result,
                        usage=usage,
                        model=model,
                        attempts=attempts,
                        latency_ms=int((time.time() - started_at) * 1000),
                        attempt_log=tuple(attempt_log),
                    )
                status = int(getattr(response, "status_code", 500) or 500)
                detail = self._sanitize_error_detail(response.text)
                last_error = detail
                last_status_code = status
                last_provider_status = self._provider_error_status(detail)
                retryable = status in retryable_statuses
                attempt_log.append(
                    {
                        "model": model,
                        "attempt": attempt_index + 1,
                        "status": status,
                        "retryable": retryable,
                        "latency_ms": int((time.time() - attempt_started) * 1000),
                    }
                )
                if not retryable:
                    raise ToolError(f"Vertex AI answer generation failed. {detail[:500]}")
                if attempt_index + 1 < len(model_delays):
                    retry_after = self._retry_after_seconds(response)
                    if retry_after is not None:
                        last_retry_after = retry_after
                        model_delays[attempt_index + 1] = retry_after
        raise SourceCodeQALLMError(
            f"Vertex AI answer generation failed. {str(last_error or 'Model unavailable.')[:500]}",
            status_code=last_status_code,
            provider_status=last_provider_status,
            retryable=bool(last_status_code in retryable_statuses),
            retry_after_seconds=last_retry_after,
        )

    @staticmethod
    def _payload_for_generate_content(payload: dict[str, Any]) -> dict[str, Any]:
        normalized = json.loads(json.dumps(payload or {}, ensure_ascii=False))
        for content in normalized.get("contents") or []:
            if isinstance(content, dict) and not str(content.get("role") or "").strip():
                content["role"] = "user"
        return normalized

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "project_id": self._resolved_project_id(),
            "location": self.location,
            "credentials_configured": bool(self._credentials_path()),
            "runtime": self.runtime_config(),
        }

    def _credentials_path(self) -> Path | None:
        if not self.credentials_file:
            return None
        path = Path(self.credentials_file).expanduser()
        return path if path.exists() and path.is_file() else None

    def _resolved_project_id(self) -> str:
        if self.project_id:
            return self.project_id
        path = self._credentials_path()
        if path is None:
            return ""
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""
        return str(payload.get("project_id") or "").strip()

    def _access_token(self) -> str:
        path = self._credentials_path()
        if path is None:
            raise ToolError("Vertex AI credentials file is missing or unreadable.")
        credentials = service_account.Credentials.from_service_account_file(
            str(path),
            scopes=list(self._SCOPES),
        )
        credentials.refresh(GoogleAuthRequest())
        token = str(getattr(credentials, "token", "") or "").strip()
        if not token:
            raise ToolError("Vertex AI service account did not return an OAuth access token.")
        return token

    def _generate_content_url(self, model: str) -> str:
        location = self.location
        base_url = VERTEX_AI_GLOBAL_API_BASE_URL if location == "global" else f"https://{location}-aiplatform.googleapis.com/v1"
        project_id = self._resolved_project_id()
        return (
            f"{base_url}/projects/{project_id}/locations/{location}"
            f"/publishers/google/models/{model}:generateContent"
        )

    def _sanitize_error_detail(self, detail: str) -> str:
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", str(detail or ""))


class OpenAICompatibleSourceCodeQALLMProvider(SourceCodeQALLMProvider):
    name = LLM_PROVIDER_OPENAI_COMPATIBLE

    def __init__(
        self,
        *,
        api_key: str,
        api_base_url: str = OPENAI_COMPATIBLE_API_BASE_URL,
        timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_LLM_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_LLM_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_LLM_MAX_BACKOFF_SECONDS,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.api_base_url = str(api_base_url or OPENAI_COMPATIBLE_API_BASE_URL).rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.max_retries = max(0, int(max_retries or 0))
        self.backoff_seconds = max(0.0, float(backoff_seconds or 0.0))
        self.max_backoff_seconds = max(self.backoff_seconds, float(max_backoff_seconds or self.backoff_seconds or DEFAULT_LLM_MAX_BACKOFF_SECONDS))

    def ready(self) -> bool:
        return bool(self.api_key)

    def generate(
        self,
        *,
        payload: dict[str, Any],
        primary_model: str,
        fallback_model: str,
    ) -> LLMGenerateResult:
        if not self.ready():
            raise ToolError("LLM mode is not configured yet. Set SOURCE_CODE_QA_OPENAI_API_KEY or OPENAI_API_KEY on the server first.")
        if self._has_inline_image_part(payload):
            raise ToolError("Current Source Code Q&A provider does not support image attachments. Use Codex or Vertex for image-based questions.")
        messages = self._messages_from_gemini_payload(payload)
        generation_config = payload.get("generationConfig") or {}
        models = [primary_model]
        if fallback_model and fallback_model not in models:
            models.append(fallback_model)
        retryable_statuses = {429, 500, 502, 503, 504}
        last_error: str | None = None
        last_status_code: int | None = None
        last_provider_status = ""
        last_retry_after: float | None = None
        attempts = 0
        attempt_log: list[dict[str, Any]] = []
        started_at = time.time()
        delays = self._retry_delays()
        for model in models:
            model_delays = list(delays)
            for attempt_index, delay in enumerate(model_delays):
                attempts += 1
                if delay:
                    time.sleep(delay)
                attempt_started = time.time()
                request_payload = {
                    "model": model,
                    "messages": messages,
                    "temperature": generation_config.get("temperature", 0.2),
                    "max_tokens": generation_config.get("maxOutputTokens", 900),
                    "response_format": {"type": "json_object"},
                }
                try:
                    response = requests.post(
                        f"{self.api_base_url}/chat/completions",
                        headers={
                            "Authorization": f"Bearer {self.api_key}",
                            "Content-Type": "application/json",
                        },
                        json=request_payload,
                        timeout=self.timeout_seconds,
                    )
                except requests.Timeout as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "timeout",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                except requests.RequestException as error:
                    last_error = self._sanitize_error_detail(str(error))
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "request_error",
                            "retryable": True,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    continue
                if response.ok:
                    result = response.json()
                    usage = result.get("usage") or {}
                    attempt_log.append(
                        {
                            "model": model,
                            "attempt": attempt_index + 1,
                            "status": "ok",
                            "retryable": False,
                            "latency_ms": int((time.time() - attempt_started) * 1000),
                        }
                    )
                    return LLMGenerateResult(
                        payload=result,
                        usage=usage,
                        model=model,
                        attempts=attempts,
                        latency_ms=int((time.time() - started_at) * 1000),
                        attempt_log=tuple(attempt_log),
                    )
                detail = self._sanitize_error_detail(response.text)
                last_error = detail
                status = int(getattr(response, "status_code", 500) or 500)
                last_status_code = status
                last_provider_status = GeminiSourceCodeQALLMProvider._provider_error_status(detail)
                retryable = status in retryable_statuses
                attempt_log.append(
                    {
                        "model": model,
                        "attempt": attempt_index + 1,
                        "status": status,
                        "retryable": retryable,
                        "latency_ms": int((time.time() - attempt_started) * 1000),
                    }
                )
                if not retryable:
                    raise ToolError(f"OpenAI-compatible answer generation failed. {detail[:500]}")
                if attempt_index + 1 < len(model_delays):
                    retry_after = self._retry_after_seconds(response)
                    if retry_after is not None:
                        last_retry_after = retry_after
                        model_delays[attempt_index + 1] = retry_after
        raise SourceCodeQALLMError(
            f"OpenAI-compatible answer generation failed. {str(last_error or 'Model unavailable.')[:500]}",
            status_code=last_status_code,
            provider_status=last_provider_status,
            retryable=bool(last_status_code in retryable_statuses),
            retry_after_seconds=last_retry_after,
        )

    def extract_text(self, payload: dict[str, Any]) -> str:
        choices = payload.get("choices") or []
        for choice in choices:
            message = choice.get("message") or {}
            content = message.get("content")
            if isinstance(content, list):
                texts = [str(item.get("text") or "").strip() for item in content if isinstance(item, dict)]
                text = "\n".join(item for item in texts if item).strip()
            else:
                text = str(content or "").strip()
            if text:
                return text
        raise ToolError("OpenAI-compatible provider returned no readable answer.")

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "api_base_url": self.api_base_url,
            "runtime": self.runtime_config(),
        }

    @staticmethod
    def _has_inline_image_part(payload: dict[str, Any]) -> bool:
        for content in payload.get("contents") or []:
            for part in content.get("parts") or []:
                if isinstance(part, dict) and (part.get("inlineData") or part.get("inline_data")):
                    return True
        return False

    @staticmethod
    def _messages_from_gemini_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
        system_text = "\n".join(
            str(part.get("text") or "").strip()
            for part in (payload.get("systemInstruction") or {}).get("parts") or []
            if str(part.get("text") or "").strip()
        )
        user_parts: list[str] = []
        for content in payload.get("contents") or []:
            for part in content.get("parts") or []:
                text = str(part.get("text") or "").strip()
                if text:
                    user_parts.append(text)
        messages = []
        if system_text:
            messages.append({"role": "system", "content": system_text})
        messages.append({"role": "user", "content": "\n\n".join(user_parts)})
        return messages

    def _sanitize_error_detail(self, detail: str) -> str:
        sanitized = str(detail or "")
        if self.api_key:
            sanitized = sanitized.replace(self.api_key, "***")
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", sanitized)

    def runtime_config(self) -> dict[str, Any]:
        return {
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "backoff_seconds": self.backoff_seconds,
            "max_backoff_seconds": self.max_backoff_seconds,
            "retryable_statuses": [429, 500, 502, 503, 504],
        }

    def _retry_delays(self) -> list[float]:
        delays = [0.0]
        for index in range(self.max_retries):
            delay = self.backoff_seconds * (2**index)
            delays.append(min(self.max_backoff_seconds, delay))
        return delays

    def _retry_after_seconds(self, response: Any) -> float | None:
        headers = getattr(response, "headers", {}) or {}
        raw_value = headers.get("Retry-After") if hasattr(headers, "get") else None
        if raw_value is None:
            return None
        try:
            delay = float(str(raw_value).strip())
        except ValueError:
            return None
        return max(0.0, min(self.max_backoff_seconds, delay))


class SourceCodeQAService:
    def __init__(
        self,
        *,
        data_root: Path,
        team_profiles: dict[str, dict[str, Any]],
        gitlab_token: str | None = None,
        gitlab_username: str = "oauth2",
        llm_provider: str = LLM_PROVIDER_GEMINI,
        gemini_api_key: str | None = None,
        gemini_api_base_url: str = GEMINI_API_BASE_URL,
        openai_api_key: str | None = None,
        openai_api_base_url: str = OPENAI_COMPATIBLE_API_BASE_URL,
        openai_model: str = "gpt-4.1-mini",
        openai_fast_model: str = "gpt-4.1-mini",
        openai_deep_model: str = "gpt-4.1",
        openai_fallback_model: str = "gpt-4.1-mini",
        gemini_model: str = "gemini-2.5-flash",
        gemini_fast_model: str = "gemini-2.5-flash-lite",
        gemini_deep_model: str = "gemini-2.5-flash",
        gemini_fallback_model: str = "gemini-2.5-flash-lite",
        vertex_credentials_file: str | None = None,
        vertex_project_id: str | None = None,
        vertex_location: str = "global",
        vertex_model: str = "gemini-2.5-flash",
        vertex_fast_model: str = "gemini-2.5-flash-lite",
        vertex_deep_model: str = "gemini-2.5-flash",
        vertex_fallback_model: str = "gemini-2.5-flash-lite",
        query_rewrite_model: str | None = None,
        planner_model: str | None = None,
        answer_model: str | None = None,
        judge_model: str | None = None,
        repair_model: str | None = None,
        llm_judge_enabled: bool = False,
        semantic_index_model: str = DEFAULT_SEMANTIC_INDEX_MODEL,
        semantic_index_enabled: bool = True,
        embedding_provider: str = "local_token_hybrid",
        embedding_api_key: str | None = None,
        embedding_api_base_url: str = OPENAI_COMPATIBLE_API_BASE_URL,
        llm_cache_ttl_seconds: int = 1800,
        llm_timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS,
        codex_timeout_seconds: int | None = None,
        codex_concurrency: int = 1,
        codex_top_path_limit: int = DEFAULT_CODEX_TOP_PATH_LIMIT,
        codex_repair_enabled: bool = True,
        codex_session_mode: str = CODEX_SESSION_MODE_EPHEMERAL,
        codex_session_max_turns: int = 8,
        codex_fast_path_enabled: bool = True,
        codex_cache_followups: bool = False,
        llm_max_retries: int = DEFAULT_LLM_MAX_RETRIES,
        llm_backoff_seconds: float = DEFAULT_LLM_BACKOFF_SECONDS,
        llm_max_backoff_seconds: float = DEFAULT_LLM_MAX_BACKOFF_SECONDS,
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
        self.llm_provider_name = str(llm_provider or LLM_PROVIDER_GEMINI).strip().lower() or LLM_PROVIDER_GEMINI
        self.gemini_api_key = str(gemini_api_key or "").strip()
        self.gemini_api_base_url = str(gemini_api_base_url or GEMINI_API_BASE_URL).strip() or GEMINI_API_BASE_URL
        self.openai_api_key = str(openai_api_key or "").strip()
        self.openai_api_base_url = str(openai_api_base_url or OPENAI_COMPATIBLE_API_BASE_URL).strip() or OPENAI_COMPATIBLE_API_BASE_URL
        self.openai_model = str(openai_model or "gpt-4.1-mini").strip() or "gpt-4.1-mini"
        self.openai_fast_model = str(openai_fast_model or self.openai_model).strip() or self.openai_model
        self.openai_deep_model = str(openai_deep_model or self.openai_model).strip() or self.openai_model
        self.openai_fallback_model = str(openai_fallback_model or self.openai_fast_model).strip() or self.openai_fast_model
        self.gemini_model = str(gemini_model or "gemini-2.5-flash").strip() or "gemini-2.5-flash"
        self.gemini_fast_model = str(gemini_fast_model or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
        self.gemini_deep_model = str(gemini_deep_model or self.gemini_model).strip() or self.gemini_model
        self.gemini_fallback_model = str(gemini_fallback_model or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
        self.vertex_credentials_file = str(vertex_credentials_file or "").strip()
        self.vertex_project_id = str(vertex_project_id or "").strip()
        self.vertex_location = str(vertex_location or "global").strip() or "global"
        self.vertex_model = str(vertex_model or "gemini-2.5-flash").strip() or "gemini-2.5-flash"
        self.vertex_fast_model = str(vertex_fast_model or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
        self.vertex_deep_model = str(vertex_deep_model or self.vertex_model).strip() or self.vertex_model
        self.vertex_fallback_model = str(vertex_fallback_model or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
        self.query_rewrite_model = str(query_rewrite_model or "").strip()
        self.planner_model = str(planner_model or "").strip()
        self.answer_model = str(answer_model or "").strip()
        self.judge_model = str(judge_model or "").strip()
        self.repair_model = str(repair_model or "").strip()
        self.llm_judge_enabled = bool(llm_judge_enabled)
        self.embedding_provider_name = str(embedding_provider or "local_token_hybrid").strip().lower() or "local_token_hybrid"
        self.semantic_index_model = str(semantic_index_model or DEFAULT_SEMANTIC_INDEX_MODEL).strip() or DEFAULT_SEMANTIC_INDEX_MODEL
        if self.embedding_provider_name == LLM_PROVIDER_VERTEX_AI and self.semantic_index_model.startswith("local-"):
            self.semantic_index_model = DEFAULT_VERTEX_EMBEDDING_MODEL
        self.semantic_index_enabled = bool(semantic_index_enabled)
        self.embedding_api_key = str(embedding_api_key or "").strip()
        self.embedding_api_base_url = str(embedding_api_base_url or OPENAI_COMPATIBLE_API_BASE_URL).strip() or OPENAI_COMPATIBLE_API_BASE_URL
        self.llm_timeout_seconds = max(5, int(llm_timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        self.codex_timeout_seconds = max(
            10,
            int(codex_timeout_seconds if codex_timeout_seconds is not None else DEFAULT_CODEX_TIMEOUT_SECONDS),
        )
        self.codex_concurrency = max(1, min(int(codex_concurrency or 1), 4))
        self.codex_top_path_limit = max(5, min(int(codex_top_path_limit or DEFAULT_CODEX_TOP_PATH_LIMIT), 80))
        self.codex_repair_enabled = bool(codex_repair_enabled)
        normalized_codex_session_mode = str(codex_session_mode or CODEX_SESSION_MODE_EPHEMERAL).strip().lower()
        self.codex_session_mode = normalized_codex_session_mode if normalized_codex_session_mode in {CODEX_SESSION_MODE_EPHEMERAL, CODEX_SESSION_MODE_RESUME} else CODEX_SESSION_MODE_EPHEMERAL
        self.codex_session_max_turns = max(1, min(int(codex_session_max_turns or 8), 30))
        self.codex_fast_path_enabled = bool(codex_fast_path_enabled)
        self.codex_cache_followups = bool(codex_cache_followups)
        self.llm_max_retries = max(0, int(llm_max_retries or 0))
        self.llm_backoff_seconds = max(0.0, float(llm_backoff_seconds or 0.0))
        self.llm_max_backoff_seconds = max(
            self.llm_backoff_seconds,
            float(llm_max_backoff_seconds or self.llm_backoff_seconds or DEFAULT_LLM_MAX_BACKOFF_SECONDS),
        )
        self.llm_provider = self._build_llm_provider()
        self.embedding_provider = self._build_embedding_provider()
        self.llm_budgets = self._build_llm_budgets()
        self.model_policy = self._build_model_policy_matrix()
        self._tree_sitter_parsers: dict[str, Any | None] = {}
        self._tree_sitter_load_errors: dict[str, str] = {}
        self.llm_cache_ttl_seconds = max(60, int(llm_cache_ttl_seconds or 1800))
        self.git_timeout_seconds = max(5, int(git_timeout_seconds or 90))
        self.max_file_bytes = max(20_000, int(max_file_bytes or 500_000))

    def with_llm_provider(self, llm_provider: str) -> "SourceCodeQAService":
        normalized = self.normalize_query_llm_provider(llm_provider)
        if normalized == self.llm_provider_name:
            return self
        return SourceCodeQAService(
            data_root=self.base_data_root,
            team_profiles=self.team_profiles,
            gitlab_token=self.gitlab_token,
            gitlab_username=self.gitlab_username,
            llm_provider=normalized,
            gemini_api_key=self.gemini_api_key,
            gemini_api_base_url=self.gemini_api_base_url,
            openai_api_key=self.openai_api_key,
            openai_api_base_url=self.openai_api_base_url,
            openai_model=self.openai_model,
            openai_fast_model=self.openai_fast_model,
            openai_deep_model=self.openai_deep_model,
            openai_fallback_model=self.openai_fallback_model,
            gemini_model=self.gemini_model,
            gemini_fast_model=self.gemini_fast_model,
            gemini_deep_model=self.gemini_deep_model,
            gemini_fallback_model=self.gemini_fallback_model,
            vertex_credentials_file=self.vertex_credentials_file,
            vertex_project_id=self.vertex_project_id,
            vertex_location=self.vertex_location,
            vertex_model=self.vertex_model,
            vertex_fast_model=self.vertex_fast_model,
            vertex_deep_model=self.vertex_deep_model,
            vertex_fallback_model=self.vertex_fallback_model,
            query_rewrite_model=self.query_rewrite_model,
            planner_model=self.planner_model,
            answer_model=self.answer_model,
            judge_model=self.judge_model,
            repair_model=self.repair_model,
            llm_judge_enabled=self.llm_judge_enabled,
            semantic_index_model=self.semantic_index_model,
            semantic_index_enabled=self.semantic_index_enabled,
            embedding_provider=self.embedding_provider_name,
            embedding_api_key=self.embedding_api_key,
            embedding_api_base_url=self.embedding_api_base_url,
            llm_cache_ttl_seconds=self.llm_cache_ttl_seconds,
            llm_timeout_seconds=self.llm_timeout_seconds,
            codex_timeout_seconds=self.codex_timeout_seconds,
            codex_concurrency=self.codex_concurrency,
            codex_top_path_limit=self.codex_top_path_limit,
            codex_repair_enabled=self.codex_repair_enabled,
            codex_session_mode=self.codex_session_mode,
            codex_session_max_turns=self.codex_session_max_turns,
            codex_fast_path_enabled=self.codex_fast_path_enabled,
            codex_cache_followups=self.codex_cache_followups,
            llm_max_retries=self.llm_max_retries,
            llm_backoff_seconds=self.llm_backoff_seconds,
            llm_max_backoff_seconds=self.llm_max_backoff_seconds,
            git_timeout_seconds=self.git_timeout_seconds,
            max_file_bytes=self.max_file_bytes,
        )

    @staticmethod
    def normalize_query_llm_provider(llm_provider: str | None) -> str:
        provider = str(llm_provider or LLM_PROVIDER_CODEX_CLI_BRIDGE).strip().lower() or LLM_PROVIDER_CODEX_CLI_BRIDGE
        return provider if provider in LLM_PROVIDER_ALLOWED_QUERY_CHOICES else LLM_PROVIDER_CODEX_CLI_BRIDGE

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
            "llm_providers": [
                {"value": LLM_PROVIDER_CODEX_CLI_BRIDGE, "label": "Codex"},
                {"value": LLM_PROVIDER_GEMINI, "label": "Gemini (Unavailable)", "disabled": True},
                {"value": LLM_PROVIDER_VERTEX_AI, "label": "Vertex AI"},
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
                "gemini_only_policy": {
                    "cheap": "Flash-Lite for simple lookup and lightweight judge/rewrite work.",
                    "balanced": "Flash with moderate thinking for API, config, rule, or multi-match questions.",
                    "deep": "Flash with higher thinking for data-source, cross-repo, root-cause, and repair work.",
                    COMPACT_DEEP_BUDGET_MODE: "Flash with compact evidence and low thinking for token-heavy code questions.",
                },
                "token_pressure": {
                    "compact_threshold": LLM_PROMPT_COMPACT_THRESHOLD_TOKENS,
                    "tight_threshold": LLM_PROMPT_TIGHT_THRESHOLD_TOKENS,
                    "vertex_compact_threshold": VERTEX_PROMPT_COMPACT_THRESHOLD_TOKENS,
                    "vertex_tight_threshold": VERTEX_PROMPT_TIGHT_THRESHOLD_TOKENS,
                    "strategy": "switch balanced/deep answers to compact_deep before the model call when estimated prompt tokens are high",
                },
                "vertex_quality_profile": {
                    "enabled": self.llm_provider_name == LLM_PROVIDER_VERTEX_AI,
                    "quality_floor": "cheap requests are routed to balanced",
                    "context_policy": "raw code snippets are primary evidence; compressed facts are navigation hints",
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
                "max_retries": self.llm_max_retries,
                "backoff_seconds": self.llm_backoff_seconds,
                "max_backoff_seconds": self.llm_max_backoff_seconds,
                "retry_after": "honored",
                "fallback_model": self._llm_fallback_model(),
            },
            "semantic_retrieval": {
                "enabled": self.semantic_index_enabled,
                "model": self.semantic_index_model,
                "index_version": CODE_INDEX_VERSION,
                "embedding_provider": self.embedding_provider.public_config(),
            },
            "judge": {
                "enabled": self.llm_judge_enabled,
                "mode": "llm_evidence_judge" if self.llm_judge_enabled else "deterministic_evidence_judge",
                "cache": "enabled",
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
        if self.llm_provider_name == LLM_PROVIDER_OPENAI_COMPATIBLE:
            return "LLM mode is not configured yet. Set SOURCE_CODE_QA_OPENAI_API_KEY or OPENAI_API_KEY on the server first."
        if self.llm_provider_name == LLM_PROVIDER_GEMINI:
            return "LLM mode is not configured yet. Set SOURCE_CODE_QA_GEMINI_API_KEY or GEMINI_API_KEY on the server first."
        if self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
            return "Codex is unavailable. Run `codex login` with ChatGPT on this server before using Codex mode."
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            return (
                "Vertex AI mode is not configured yet. Set SOURCE_CODE_QA_VERTEX_CREDENTIALS_FILE "
                "or GOOGLE_APPLICATION_CREDENTIALS, and set SOURCE_CODE_QA_VERTEX_PROJECT_ID / SOURCE_CODE_QA_VERTEX_LOCATION if needed."
            )
        return f"LLM mode is not configured yet. Provider {self.llm_provider_name!r} is unsupported or missing credentials."

    def _build_llm_provider(self) -> SourceCodeQALLMProvider:
        if self.llm_provider_name == LLM_PROVIDER_GEMINI:
            return GeminiSourceCodeQALLMProvider(
                api_key=self.gemini_api_key,
                api_base_url=self.gemini_api_base_url,
                timeout_seconds=self.llm_timeout_seconds,
                max_retries=self.llm_max_retries,
                backoff_seconds=self.llm_backoff_seconds,
                max_backoff_seconds=self.llm_max_backoff_seconds,
            )
        if self.llm_provider_name == LLM_PROVIDER_OPENAI_COMPATIBLE:
            return OpenAICompatibleSourceCodeQALLMProvider(
                api_key=self.openai_api_key,
                api_base_url=self.openai_api_base_url,
                timeout_seconds=self.llm_timeout_seconds,
                max_retries=self.llm_max_retries,
                backoff_seconds=self.llm_backoff_seconds,
                max_backoff_seconds=self.llm_max_backoff_seconds,
            )
        if self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
            return CodexCliBridgeSourceCodeQALLMProvider(
                workspace_root=self.repo_root,
                timeout_seconds=self.codex_timeout_seconds,
                concurrency_limit=self.codex_concurrency,
                session_mode=self.codex_session_mode,
            )
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            return VertexAISourceCodeQALLMProvider(
                credentials_file=self.vertex_credentials_file,
                project_id=self.vertex_project_id,
                location=self.vertex_location,
                timeout_seconds=self.llm_timeout_seconds,
                max_retries=self.llm_max_retries,
                backoff_seconds=self.llm_backoff_seconds,
                max_backoff_seconds=self.llm_max_backoff_seconds,
            )
        return UnsupportedSourceCodeQALLMProvider(self.llm_provider_name)

    def _build_embedding_provider(self) -> SourceCodeQAEmbeddingProvider:
        if self.embedding_provider_name == LLM_PROVIDER_VERTEX_AI:
            model = self.semantic_index_model
            if model.startswith("local-"):
                model = DEFAULT_VERTEX_EMBEDDING_MODEL
            return VertexAIEmbeddingProvider(
                credentials_file=self.vertex_credentials_file,
                project_id=self.vertex_project_id,
                location=self.vertex_location,
                model=model,
            )
        if self.embedding_provider_name == "openai_compatible" or not self.semantic_index_model.startswith("local-"):
            return OpenAICompatibleEmbeddingProvider(
                api_key=self.embedding_api_key,
                api_base_url=self.embedding_api_base_url,
                model=self.semantic_index_model,
            )
        return SourceCodeQAEmbeddingProvider()

    def _build_llm_budgets(self) -> dict[str, dict[str, Any]]:
        budgets = json.loads(json.dumps(DEFAULT_LLM_BUDGETS))
        if self.llm_provider_name == LLM_PROVIDER_OPENAI_COMPATIBLE:
            budgets["cheap"]["model"] = self.openai_fast_model
            budgets["balanced"]["model"] = self.openai_model
            budgets["deep"]["model"] = self.openai_deep_model
            budgets[COMPACT_DEEP_BUDGET_MODE]["model"] = self.openai_deep_model
        elif self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
            codex_model = self._codex_cli_model()
            budgets["cheap"]["model"] = codex_model
            budgets["balanced"]["model"] = codex_model
            budgets["deep"]["model"] = codex_model
            budgets[COMPACT_DEEP_BUDGET_MODE]["model"] = codex_model
        elif self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            for budget_name, overrides in VERTEX_QUALITY_LLM_BUDGETS.items():
                budgets[budget_name].update(overrides)
            budgets["cheap"]["model"] = self.vertex_fast_model
            budgets["balanced"]["model"] = self.vertex_model
            budgets["deep"]["model"] = self.vertex_deep_model
            budgets[COMPACT_DEEP_BUDGET_MODE]["model"] = self.vertex_deep_model
        else:
            budgets["cheap"]["model"] = self.gemini_fast_model
            budgets["balanced"]["model"] = self.gemini_model
            budgets["deep"]["model"] = self.gemini_deep_model
            budgets[COMPACT_DEEP_BUDGET_MODE]["model"] = self.gemini_deep_model
        return budgets

    def _build_model_policy_matrix(self) -> dict[str, dict[str, Any]]:
        role_defaults = {
            "query_rewrite": ("cheap", self.query_rewrite_model, "Normalize follow-up and fuzzy user wording before retrieval."),
            "planner": ("cheap", self.planner_model, "Choose deterministic retrieval tools and trace expansion steps."),
            "answer": ("balanced", self.answer_model, "Generate the user-facing evidence-grounded answer."),
            "judge": ("cheap", self.judge_model, "Check whether answer claims are supported by the evidence pack."),
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
                "enabled": role != "judge" or self.llm_judge_enabled,
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

    def _thinking_budget_for_call(
        self,
        *,
        role: str,
        budget_mode: str,
        budget: dict[str, Any],
        quality_gate: dict[str, Any] | None = None,
        retry: bool = False,
    ) -> int:
        if self.llm_provider_name not in {LLM_PROVIDER_GEMINI, LLM_PROVIDER_VERTEX_AI}:
            return int((budget or {}).get("thinking_budget") or 0)
        role = str(role or "").strip().lower()
        mode = str(budget_mode or "").strip().lower()
        base = int((budget or {}).get("thinking_budget") or 0)
        if role in {"judge", "query_rewrite", "planner"} or mode == "cheap":
            return 0
        if retry or role == "repair":
            return 2048
        gate_status = str((quality_gate or {}).get("status") or "").strip().lower()
        if gate_status and gate_status != "sufficient":
            return max(base, 1024)
        if mode == "deep":
            return max(base, 1024)
        if mode == COMPACT_DEEP_BUDGET_MODE:
            return max(base, 512)
        if mode == "balanced":
            return max(base, 512)
        return base

    def _normalize_thinking_budget_for_provider(self, budget: int | None) -> int:
        value = int(budget or 0)
        if self.llm_provider_name not in {LLM_PROVIDER_GEMINI, LLM_PROVIDER_VERTEX_AI}:
            return value
        if value <= 0:
            return 0
        return max(GEMINI_MIN_THINKING_BUDGET, min(value, GEMINI_MAX_THINKING_BUDGET))

    @staticmethod
    def _is_gemini_3_model(model: str | None) -> bool:
        return str(model or "").strip().lower().startswith("gemini-3")

    @staticmethod
    def _gemini_3_thinking_level(*, role: str, budget_mode: str) -> str:
        normalized_role = str(role or "").strip().lower()
        normalized_mode = str(budget_mode or "").strip().lower()
        if normalized_role in {"repair", "judge"}:
            return "HIGH"
        if normalized_mode in {"deep", COMPACT_DEEP_BUDGET_MODE, "balanced"}:
            return "HIGH"
        return "MEDIUM"

    def _thinking_config_for_provider(
        self,
        budget: int | None,
        *,
        model: str | None = None,
        role: str = "",
        budget_mode: str = "",
    ) -> dict[str, Any]:
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI and self._is_gemini_3_model(model):
            return {"thinkingLevel": self._gemini_3_thinking_level(role=role, budget_mode=budget_mode)}
        return {"thinkingBudget": self._normalize_thinking_budget_for_provider(budget)}

    @staticmethod
    def _finish_reason_needs_generation_repair(finish_reason: str | None) -> bool:
        reason = str(finish_reason or "").strip()
        return reason.upper() in {"MAX_TOKENS", "SAFETY", "RECITATION"} or reason.lower() in {"length", "content_filter"}

    @staticmethod
    def _finish_reason_is_token_limited(finish_reason: str | None) -> bool:
        reason = str(finish_reason or "").strip()
        return reason.upper() == "MAX_TOKENS" or reason.lower() == "length"

    @staticmethod
    def _estimate_llm_tokens(text: str) -> int:
        return max(1, int((len(str(text or "")) / LLM_TOKEN_ESTIMATE_CHARS_PER_TOKEN) + 0.999))

    @staticmethod
    def _llm_prompt_pressure(estimated_prompt_tokens: int) -> str:
        if estimated_prompt_tokens >= LLM_PROMPT_TIGHT_THRESHOLD_TOKENS:
            return "tight"
        if estimated_prompt_tokens >= LLM_PROMPT_COMPACT_THRESHOLD_TOKENS:
            return "compact"
        return "normal"

    def _llm_prompt_pressure_for_provider(self, estimated_prompt_tokens: int) -> str:
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            if estimated_prompt_tokens >= VERTEX_PROMPT_TIGHT_THRESHOLD_TOKENS:
                return "tight"
            if estimated_prompt_tokens >= VERTEX_PROMPT_COMPACT_THRESHOLD_TOKENS:
                return "compact"
            return "normal"
        return self._llm_prompt_pressure(estimated_prompt_tokens)

    def _llm_fallback_model(self) -> str:
        if self.llm_provider_name == LLM_PROVIDER_OPENAI_COMPATIBLE:
            return self.openai_fallback_model
        if self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
            return self._codex_cli_model()
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            return self.vertex_fallback_model
        return self.gemini_fallback_model

    def _llm_default_model(self) -> str:
        if self.llm_provider_name == LLM_PROVIDER_OPENAI_COMPATIBLE:
            return self.openai_model
        if self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
            return self._codex_cli_model()
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            return self.vertex_model
        return self.gemini_model

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
            "Answer in one concise direct paragraph, then add evidence-backed bullets only when useful.",
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
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
        progress_callback: Any | None = None,
    ) -> dict[str, Any]:
        key = self.mapping_key(pm_team, country)
        question = str(question or "").strip()
        started_at = time.time()
        trace_id = uuid.uuid4().hex

        def report(stage: str, message: str, current: int = 0, total: int = 0) -> None:
            if not progress_callback:
                return
            try:
                progress_callback(stage, message, current, total)
            except Exception:
                return

        report("validating", "Validating question and repository scope.", 0, 0)
        if not question:
            raise ToolError("Please enter a source-code question.")
        original_question = question
        entries = self._load_entries_for_key(key)
        if not entries:
            payload = self._empty_query_payload(
                key,
                status="empty_config",
                summary="No repositories are configured for this PM Team and country yet.",
                trace_id=trace_id,
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
        query_entries, repository_scope = self._filter_entries_for_question_repository_scope(original_question, entries)
        question, followup_context = self._apply_conversation_context(
            question,
            conversation_context,
            current_key=key,
            current_repositories=entries,
        )
        tokens = self._question_tokens(question)
        if not tokens:
            payload = self._empty_query_payload(
                key,
                status="weak_question",
                summary="No confident match. Try adding exact class, API, table, field, or function names.",
                trace_id=trace_id,
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
        tool_trace: list[dict[str, Any]] = []
        matches: list[dict[str, Any]] = []
        request_cache = self._new_retrieval_request_cache()
        result_limit = max(1, min(int(limit or 12), 30))
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
        exact_lookup_terms = self._extract_exact_lookup_terms(question)
        question_specific_terms = self._question_specific_retrieval_terms(question)
        if question_specific_terms:
            specific_exact_terms = [
                str(term or "").strip().lower()
                for term in question_specific_terms
                if "_" in str(term or "") and any(marker in str(term or "").lower() for marker in ("_tab", "_table", "process_info", "response_body"))
            ]
            exact_lookup_terms = list(dict.fromkeys([*exact_lookup_terms, *specific_exact_terms]))[:12]
        exact_matches: list[dict[str, Any]] = []
        latency_guarded_query_expansion = False
        synced_entries = [(entry, self._repo_path(key, entry)) for entry in query_entries if (self._repo_path(key, entry) / ".git").exists()]
        if repository_scope.get("active"):
            self._increment_retrieval_stat(request_cache, "repository_scope_filters")
            report(
                "repository_scope",
                "Question names specific repositories; limiting retrieval scope.",
                len(query_entries),
                len(entries),
            )
        if not synced_entries:
            payload = self._empty_query_payload(
                key,
                repo_status=repo_status,
                index_freshness=index_freshness,
                status="not_synced",
                summary="No synced repositories are available in the selected scope. Run Sync / Refresh before asking code questions.",
                trace_id=trace_id,
            )
            payload["repository_scope"] = repository_scope
            payload["retrieval_runtime"] = self._retrieval_cache_stats(request_cache)
            self._record_query_telemetry(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                payload=payload,
                started_at=started_at,
            )
            return payload
        queryable_entries = [
            (entry, repo_path)
            for entry, repo_path in synced_entries
            if (
                (index_info := self._repo_index_info(key, entry, repo_path)).get("state") == "ready"
                or (index_info.get("state") == "stale" and index_info.get("queryable"))
            )
        ]
        if not queryable_entries:
            self._increment_retrieval_stat(request_cache, "index_not_ready_scopes")
            payload = self._empty_query_payload(
                key,
                repo_status=repo_status,
                index_freshness=index_freshness,
                status="index_not_ready",
                summary="Synced repositories exist in the selected scope, but none has a ready queryable index. Run Sync / Refresh before trusting code answers.",
                trace_id=trace_id,
            )
            payload["repository_scope"] = repository_scope
            payload["retrieval_runtime"] = self._retrieval_cache_stats(request_cache)
            self._record_query_telemetry(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                payload=payload,
                started_at=started_at,
            )
            return payload
        intent = query_plan.get("intent") if isinstance(query_plan.get("intent"), dict) else {}
        simple_quality_trace = (
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
            payload = self._empty_query_payload(
                key,
                repo_status=repo_status,
                index_freshness=index_freshness,
                status="no_match",
                summary="No exact table/path references were found in the indexed repositories.",
                trace_id=trace_id,
            )
            payload["exact_lookup_terms"] = exact_lookup_terms
            payload["repository_scope"] = repository_scope
            self._record_query_telemetry(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                payload=payload,
                started_at=started_at,
            )
            return payload
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
            report("direct_search", f"Searching direct matches across {len(synced_entries)} repos.", 0, len(synced_entries))
            skip_broad_query_decomposition = False
            for index, (entry, repo_path) in enumerate(synced_entries, start=1):
                matches.extend(self._search_repo(entry, repo_path, tokens, question=question, request_cache=request_cache))
                report("direct_search", f"Searching direct matches in {entry.display_name}.", index, len(synced_entries))
                if matches and simple_quality_trace and index >= 3 and (len(matches) >= 80 or time.time() - started_at >= 4.0):
                    direct_ranked = self._rank_matches(question, matches, request_cache=request_cache)
                    direct_top = self._select_result_matches(direct_ranked, max(1, min(int(limit or 12), 30)), question=question)
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
                direct_top = self._select_result_matches(direct_ranked, max(1, min(int(limit or 12), 30)), question=question)
                direct_evidence_summary = self._compress_evidence_cached(question, direct_top, request_cache=request_cache)
                direct_quality_gate = self._quality_gate_cached(question, direct_evidence_summary, request_cache=request_cache)
                if (
                    direct_quality_gate.get("status") == "sufficient"
                    and direct_quality_gate.get("confidence") in {"medium", "high"}
                    and (len(synced_entries) >= 5 or time.time() - started_at >= 6.0)
                ):
                    skip_broad_query_decomposition = True
                    self._increment_retrieval_stat(request_cache, "direct_quality_short_circuits")
                    report("quality_gate", "Direct matches are sufficient; skipping broader evidence expansion.", len(synced_entries), len(synced_entries))
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
        report("ranking", "Ranking matched files and snippets.", 0, 0)
        matches = self._rank_matches(question, matches, request_cache=request_cache)
        top_matches = (
            self._select_result_matches(matches, result_limit, question=question)
            if exact_matches
            else matches[:result_limit]
        )
        should_expand_matches = not exact_lookup_sufficient
        if top_matches and should_expand_matches and simple_quality_trace:
            early_evidence_summary = self._compress_evidence_cached(question, top_matches, request_cache=request_cache)
            early_quality_gate = self._quality_gate_cached(question, early_evidence_summary, request_cache=request_cache)
            if (
                early_quality_gate.get("status") == "sufficient"
                and early_quality_gate.get("confidence") in {"medium", "high"}
                and (len(synced_entries) >= 5 or time.time() - started_at >= 6.0)
            ):
                should_expand_matches = False
                self._increment_retrieval_stat(request_cache, "early_quality_short_circuits")
                report("quality_gate", "Ranked evidence is sufficient; skipping deeper local expansion.", 0, 0)
        if top_matches and should_expand_matches and latency_guarded_query_expansion:
            should_expand_matches = False
            self._increment_retrieval_stat(request_cache, "deep_expansion_latency_guards")
            report("quality_gate", "Skipping deeper expansion because retrieval already hit the latency guard.", 0, 0)
        if top_matches and should_expand_matches and time.time() - started_at >= 8.0:
            should_expand_matches = False
            self._increment_retrieval_stat(request_cache, "deep_expansion_latency_guards")
            report("quality_gate", "Skipping deeper expansion because retrieval already has enough evidence and hit the latency guard.", 0, 0)
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
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in dependency_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches, request_cache=request_cache)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)), question=question)
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
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in trace_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches, request_cache=request_cache)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)), question=question)
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
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in agent_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches, request_cache=request_cache)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)), question=question)
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
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in tool_loop_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches, request_cache=request_cache)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)), question=question)
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
                existing_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in top_matches}
                for item in impact_matches:
                    item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
                    if item_key not in existing_keys:
                        top_matches.append(item)
                        existing_keys.add(item_key)
                    else:
                        self._annotate_duplicate_tool_match(top_matches, item)
                top_matches = self._rank_matches(question, top_matches, request_cache=request_cache)
                top_matches = self._select_result_matches(top_matches, max(1, min(int(limit or 12), 30)), question=question)
        if index_freshness.get("status") != "fresh":
            repo_status = self.repo_status(key)
            index_freshness = self._index_freshness_payload(repo_status)
        if not top_matches:
            payload = self._empty_query_payload(
                key,
                repo_status=repo_status,
                index_freshness=index_freshness,
                status="no_match",
                summary="No confident match. Try exact symbols, route paths, table names, or error codes.",
                trace_id=trace_id,
            )
            payload["repository_scope"] = repository_scope
            self._record_query_telemetry(
                key=key,
                question=question,
                answer_mode=answer_mode,
                llm_budget_mode=llm_budget_mode,
                payload=payload,
                started_at=started_at,
            )
            return payload
        normalized_answer_mode = str(answer_mode or ANSWER_MODE).strip() or ANSWER_MODE
        if normalized_answer_mode not in {ANSWER_MODE, ANSWER_MODE_GEMINI, ANSWER_MODE_AUTO}:
            normalized_answer_mode = ANSWER_MODE_AUTO
        codex_fast_path = self._codex_fast_path_active(normalized_answer_mode)
        report(
            "evidence_pack",
            "Preparing Codex navigation hints." if codex_fast_path else "Building evidence pack and answer context.",
            0,
            0,
        )
        if codex_fast_path:
            evidence_summary = self._codex_fast_evidence_summary(question, top_matches)
            quality_gate = self._codex_fast_quality_gate(top_matches)
        else:
            evidence_summary = self._compress_evidence_cached(question, top_matches, request_cache=request_cache)
            quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
        trace_paths = [] if exact_lookup_sufficient or not should_expand_matches else self._build_trace_paths(entries=query_entries, key=key, matches=top_matches, question=question, request_cache=request_cache)
        if trace_paths:
            evidence_summary["trace_paths"] = trace_paths
        repo_graph = (
            {
                "version": 2,
                "nodes": [{"name": entry.display_name, "url": entry.url} for entry in query_entries],
                "edges": [],
                "skipped": "exact_lookup_sufficient",
            }
            if exact_lookup_sufficient or not should_expand_matches
            else self._build_repo_dependency_graph(key=key, entries=query_entries, request_cache=request_cache)
        )
        evidence_pack = (
            self._codex_fast_evidence_pack(top_matches, trace_paths)
            if codex_fast_path
            else self._build_evidence_pack(
                question=question,
                evidence_summary=evidence_summary,
                matches=top_matches,
                trace_paths=trace_paths,
                quality_gate=quality_gate,
            )
        )
        payload = {
            "status": "ok",
            "answer_mode": normalized_answer_mode,
            "summary": self._build_summary(top_matches),
            "matches": top_matches,
            "citations": self._build_citations(top_matches),
            "trace_paths": trace_paths,
            "repo_graph": repo_graph,
            "evidence_pack": evidence_pack,
            "evidence_outline": self._build_evidence_outline(evidence_pack, top_matches),
            "repo_status": repo_status,
            "index_freshness": index_freshness,
            "answer_quality": quality_gate,
            "agent_plan": self._build_agent_plan(question, evidence_summary, quality_gate),
            "query_plan": query_plan,
            "exact_lookup": {
                "terms": exact_lookup_terms,
                "matched_terms": exact_lookup_matched_terms,
                "match_count": len(exact_matches),
                "sufficient": exact_lookup_sufficient,
            },
            "tool_trace": tool_trace,
            "retrieval_runtime": self._retrieval_cache_stats(request_cache),
            "followup_context": followup_context,
            "repository_scope": repository_scope,
            "original_question": original_question,
            "trace_id": trace_id,
        }
        if normalized_answer_mode in {ANSWER_MODE_GEMINI, ANSWER_MODE_AUTO}:
            payload["llm_provider"] = self.llm_provider.name
            report("llm_generation", "Calling LLM with retrieved evidence.", 0, 0)
            llm_payload = self._build_llm_answer(
                entries=query_entries,
                key=key,
                pm_team=pm_team,
                country=country,
                question=question,
                matches=top_matches,
                llm_budget_mode=llm_budget_mode,
                followup_context=followup_context,
                requested_answer_mode=normalized_answer_mode,
                request_cache=request_cache,
                progress_callback=progress_callback,
                attachments=attachments or [],
                runtime_evidence=runtime_evidence or [],
            )
            payload.update(llm_payload)
            payload["evidence_outline"] = self._build_evidence_outline(payload.get("evidence_pack") or evidence_pack, top_matches)
            payload["answer_mode"] = normalized_answer_mode
            report("completed", "LLM answer generated.", 0, 0)
        if not (normalized_answer_mode in {ANSWER_MODE_GEMINI, ANSWER_MODE_AUTO} and payload.get("llm_answer")):
            report("completed", "Code evidence retrieval completed.", 0, 0)
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

    def _codex_fast_path_active(self, answer_mode: str | None = None) -> bool:
        normalized_answer_mode = str(answer_mode or "").strip() or ANSWER_MODE
        return (
            self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE
            and self.codex_fast_path_enabled
            and normalized_answer_mode in {ANSWER_MODE_AUTO, ANSWER_MODE_GEMINI}
        )

    @staticmethod
    def _codex_fast_quality_gate(matches: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "status": "codex_fast_path_skipped",
            "confidence": "unknown",
            "reason": "Codex reads the candidate files directly; Gemini-style answer quality gate is skipped.",
            "missing": [] if matches else ["candidate paths"],
        }

    @staticmethod
    def _codex_fast_evidence_summary(question: str, matches: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "version": 1,
            "mode": "codex_fast_path",
            "question": str(question or "")[:500],
            "supporting_count": len(matches),
            "top_paths": [
                {
                    "source_id": f"S{index}",
                    "repo": match.get("repo"),
                    "path": match.get("path"),
                    "line_start": match.get("line_start"),
                    "line_end": match.get("line_end"),
                    "retrieval": match.get("retrieval"),
                    "trace_stage": match.get("trace_stage"),
                    "reason": match.get("reason"),
                }
                for index, match in enumerate(matches[:30], start=1)
                if isinstance(match, dict)
            ],
        }

    @staticmethod
    def _codex_fast_evidence_pack(matches: list[dict[str, Any]], trace_paths: list[dict[str, Any]]) -> dict[str, Any]:
        top_paths = [
            f"{match.get('repo')}:{match.get('path')}:{match.get('line_start')}-{match.get('line_end')} [S{index}]"
            for index, match in enumerate(matches[:12], start=1)
            if isinstance(match, dict)
        ]
        return {
            "version": 2,
            "mode": "codex_navigation_hints",
            "entry_points": top_paths[:6],
            "call_chain": top_paths[6:10],
            "read_write_points": [],
            "tables": [],
            "apis": [],
            "configs": [],
            "missing_hops": [],
            "trace_paths": trace_paths[:5],
        }

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
        if rating not in {"useful", "not_useful", "wrong_file", "too_vague", "hallucinated", "missing_repo", "needs_deeper_trace"}:
            raise ToolError("Unknown feedback rating.")
        question = str(payload.get("question") or "").strip()
        replay_context = self._feedback_replay_context(payload)
        record = {
            "timestamp": self._now_iso(),
            "user_email": str(user_email or "").strip().lower(),
            "rating": rating,
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

    @staticmethod
    def _is_retryable_llm_rate_limit(error: ToolError) -> bool:
        status_code = getattr(error, "status_code", None)
        provider_status = str(getattr(error, "provider_status", "") or "").upper()
        text = str(error).upper()
        return status_code == 429 or provider_status == "RESOURCE_EXHAUSTED" or "RESOURCE_EXHAUSTED" in text

    @staticmethod
    def _llm_retry_after_seconds(error: ToolError) -> float | None:
        value = getattr(error, "retry_after_seconds", None)
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            return None

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

    def _index_path(self, repo_path: Path) -> Path:
        digest = hashlib.sha1(str(repo_path).encode("utf-8")).hexdigest()[:16]
        return self.index_root / f"{digest}.sqlite3"

    def _index_lock_path(self, repo_path: Path) -> Path:
        digest = hashlib.sha1(str(repo_path).encode("utf-8")).hexdigest()[:16]
        return self.lock_root / f"{digest}.lock"

    def _index_lock_is_stale(self, lock_path: Path) -> bool:
        stale_seconds = float(os.getenv("SOURCE_CODE_QA_INDEX_LOCK_STALE_SECONDS", str(DEFAULT_INDEX_LOCK_STALE_SECONDS)))
        if stale_seconds <= 0:
            return False
        timestamp = 0.0
        try:
            raw_timestamp = lock_path.read_text(encoding="utf-8").splitlines()[0].strip()
            timestamp = datetime.fromisoformat(raw_timestamp).timestamp()
        except (IndexError, OSError, ValueError):
            try:
                timestamp = lock_path.stat().st_mtime
            except OSError:
                return True
        return (time.time() - timestamp) > stale_seconds

    def _acquire_index_lock(self, repo_path: Path) -> Path:
        self.lock_root.mkdir(parents=True, exist_ok=True)
        lock_path = self._index_lock_path(repo_path)
        for attempt in range(2):
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                break
            except FileExistsError as error:
                if attempt == 0 and self._index_lock_is_stale(lock_path):
                    lock_path.unlink(missing_ok=True)
                    continue
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

    def _require_ready_repo_index(
        self,
        *,
        key: str | None,
        entry: RepositoryEntry,
        repo_path: Path,
    ) -> dict[str, Any]:
        info = self._repo_index_info(key, entry, repo_path)
        if info.get("state") == "ready" or (info.get("state") == "stale" and info.get("queryable")):
            return info
        raise SourceCodeQAIndexUnavailable(
            f"Code index for {entry.display_name} is {info.get('state') or 'unavailable'}; run Sync / Refresh first."
        )

    def _repo_index_info(self, key: str | None, entry: RepositoryEntry, repo_path: Path) -> dict[str, Any]:
        del key, entry
        index_path = self._index_path(repo_path)
        if not index_path.exists():
            return {"state": "missing", "path": str(index_path), "schema_compatible": False, "queryable": False}
        git_revision = self._repo_git_revision(repo_path)
        try:
            with sqlite3.connect(index_path) as connection:
                metadata = dict(connection.execute("select key, value from metadata").fetchall())
        except sqlite3.Error:
            return {"state": "stale", "path": str(index_path), "schema_compatible": False, "queryable": False}
        index_version = int(str(metadata.get("version") or "0")) if str(metadata.get("version") or "0").isdigit() else 0
        schema_compatible = index_version == CODE_INDEX_VERSION
        queryable = index_version >= 28
        state = "stale"
        if (
            schema_compatible
            and git_revision
            and metadata.get("git_revision") == git_revision
            and self._repo_worktree_clean(repo_path)
        ):
            state = "ready"
        else:
            fingerprint = self._repo_fingerprint(repo_path)
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
            "schema_compatible": schema_compatible,
            "queryable": queryable,
            "index_version": index_version,
            "files": int(metadata.get("indexed_files") or 0),
            "lines": int(metadata.get("indexed_lines") or 0),
            "definitions": int(metadata.get("indexed_definitions") or 0),
            "references": int(metadata.get("indexed_references") or 0),
            "entities": int(metadata.get("indexed_entities") or 0),
            "entity_edges": int(metadata.get("indexed_entity_edges") or 0),
            "edges": int(metadata.get("indexed_edges") or 0),
            "flow_edges": int(metadata.get("indexed_flow_edges") or 0),
            "semantic_chunks": int(metadata.get("indexed_semantic_chunks") or 0),
            "reused_files": int(metadata.get("reused_files") or 0),
            "reparsed_files": int(metadata.get("reparsed_files") or 0),
            "index_refresh_strategy": metadata.get("index_refresh_strategy") or "",
            "parser_backend": metadata.get("parser_backend") or "regex",
            "parser_languages": [
                item
                for item in str(metadata.get("parser_languages") or "").split(",")
                if item
            ],
            "tree_sitter_files": int(metadata.get("tree_sitter_files") or 0),
            "tree_sitter_errors": int(metadata.get("tree_sitter_errors") or 0),
            "semantic_index_model": metadata.get("semantic_index_model") or DEFAULT_SEMANTIC_INDEX_MODEL,
            "git_revision": metadata.get("git_revision") or git_revision,
            "file_fts_enabled": metadata.get("file_fts_enabled") == "1",
            "fts_enabled": metadata.get("fts_enabled") == "1",
            "semantic_fts_enabled": metadata.get("semantic_fts_enabled") == "1",
            "updated_at": metadata.get("updated_at"),
        }

    @staticmethod
    def _repo_worktree_clean(repo_path: Path) -> bool:
        try:
            completed = subprocess.run(
                ["git", "-C", str(repo_path), "status", "--porcelain", "--untracked-files=all"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return completed.returncode == 0 and not (completed.stdout or "").strip()

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

    def _repo_git_revision(self, repo_path: Path) -> str:
        try:
            completed = subprocess.run(
                ["git", "-C", str(repo_path), "rev-parse", "--short=12", "HEAD"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            return ""
        if completed.returncode != 0:
            return ""
        return (completed.stdout or "").strip()

    def _build_repo_index(self, key: str, entry: RepositoryEntry, repo_path: Path) -> dict[str, Any]:
        del key, entry
        lock_path = self._acquire_index_lock(repo_path)
        self.index_root.mkdir(parents=True, exist_ok=True)
        index_path = self._index_path(repo_path)
        tmp_path = index_path.with_suffix(".tmp")
        tmp_path.unlink(missing_ok=True)
        fingerprint = self._repo_fingerprint(repo_path)
        git_revision = self._repo_git_revision(repo_path)
        indexed_files = 0
        indexed_lines = 0
        indexed_definitions = 0
        indexed_references = 0
        indexed_entities = 0
        indexed_entity_edges = 0
        indexed_semantic_chunks = 0
        tree_sitter_files = 0
        tree_sitter_errors = 0
        reused_files = 0
        reparsed_files = 0
        parser_languages: set[str] = set()
        reusable_index = self._open_reusable_index(index_path)
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
                    create table file_tokens (
                        token text not null,
                        file_path text not null,
                        primary key (token, file_path)
                    );
                    create index idx_file_tokens_path on file_tokens(file_path);
                    create table line_tokens (
                        token text not null,
                        file_path text not null,
                        line_no integer not null,
                        primary key (token, file_path, line_no)
                    );
                    create index idx_line_tokens_location on line_tokens(file_path, line_no);
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
                    create table semantic_chunks (
                        chunk_id text primary key,
                        file_path text not null,
                        start_line integer not null,
                        end_line integer not null,
                        chunk_text text not null,
                        lower_text text not null,
                        tokens text not null,
                        symbols text not null,
                        embedding text not null
                    );
                    create index idx_semantic_chunks_file_path on semantic_chunks(file_path);
                    create table semantic_chunk_tokens (
                        token text not null,
                        chunk_id text not null,
                        file_path text not null,
                        primary key (token, chunk_id)
                    );
                    create index idx_semantic_chunk_tokens_chunk on semantic_chunk_tokens(chunk_id);
                    create index idx_semantic_chunk_tokens_file on semantic_chunk_tokens(file_path);
                    """
                )
                file_fts_enabled = self._try_create_file_fts(connection)
                fts_enabled = self._try_create_fts(connection)
                semantic_fts_enabled = self._try_create_semantic_fts(connection)
                for file_path in self._iter_text_files(repo_path):
                    relative_path = str(file_path.relative_to(repo_path))
                    try:
                        stat = file_path.stat()
                    except OSError:
                        continue
                    copied_counts = self._copy_unchanged_index_file(
                        reusable_index,
                        connection,
                        relative_path,
                        stat,
                        file_fts_enabled=file_fts_enabled,
                        fts_enabled=fts_enabled,
                        semantic_fts_enabled=semantic_fts_enabled,
                    )
                    if copied_counts is not None:
                        indexed_files += 1
                        indexed_lines += copied_counts["lines"]
                        indexed_definitions += copied_counts["definitions"]
                        indexed_references += copied_counts["references"]
                        indexed_entities += copied_counts["entities"]
                        indexed_entity_edges += copied_counts["entity_edges"]
                        indexed_semantic_chunks += copied_counts["semantic_chunks"]
                        reused_files += 1
                        reused_language = self._tree_sitter_language_for_suffix(Path(relative_path).suffix.lower())
                        if reused_language:
                            parser_languages.add(reused_language)
                            tree_sitter_files += 1
                        continue
                    try:
                        lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
                    except OSError:
                        continue
                    reparsed_files += 1
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
                    if file_fts_enabled:
                        connection.execute(
                            "insert into files_fts(path, content) values (?, ?)",
                            (
                                relative_path,
                                "\n".join(
                                    [
                                        relative_path,
                                        relative_path.replace("/", " ").replace(".", " "),
                                        " ".join(sorted(file_symbols)),
                                    ]
                                ),
                            ),
                        )
                    self._insert_file_tokens(connection, relative_path, relative_path, file_symbols)
                    line_rows = []
                    line_token_rows: list[tuple[str, str, int]] = []
                    for index, line in enumerate(lines, start=1):
                        lowered = line.lower()
                        line_symbols = self._line_symbols(lowered)
                        line_rows.append(
                            (
                                relative_path,
                                index,
                                line,
                                lowered,
                                json.dumps(sorted(line_symbols), separators=(",", ":")),
                                1 if self._is_declaration_line(line) else 0,
                                1 if PATHISH_PATTERN.search(line) else 0,
                            )
                        )
                        line_token_rows.extend(
                            (token, relative_path, index)
                            for token in self._index_tokens_for_text(line, line_symbols)
                        )
                    connection.executemany(
                        """
                        insert into lines(file_path, line_no, line_text, lower_text, symbols, is_declaration, has_pathish)
                        values (?, ?, ?, ?, ?, ?, ?)
                        """,
                        line_rows,
                    )
                    connection.executemany(
                        "insert or ignore into line_tokens(token, file_path, line_no) values (?, ?, ?)",
                        line_token_rows,
                    )
                    if fts_enabled:
                        connection.executemany(
                            "insert into lines_fts(file_path, line_no, content) values (?, ?, ?)",
                            [(relative_path, row[1], row[2]) for row in line_rows],
                        )
                    structure = self._extract_structure_rows(relative_path, lines)
                    if structure.get("tree_sitter_used"):
                        tree_sitter_files += 1
                        parser_language = str(structure.get("tree_sitter_language") or "")
                        if parser_language:
                            parser_languages.add(parser_language)
                    if structure.get("tree_sitter_error"):
                        tree_sitter_errors += 1
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
                    semantic_chunks = self._build_semantic_chunks(relative_path, lines) if self.semantic_index_enabled else []
                    semantic_chunks = self._attach_semantic_embeddings(semantic_chunks) if semantic_chunks else []
                    connection.executemany(
                        """
                        insert into semantic_chunks(chunk_id, file_path, start_line, end_line, chunk_text, lower_text, tokens, symbols, embedding)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        semantic_chunks,
                    )
                    if semantic_fts_enabled and semantic_chunks:
                        connection.executemany(
                            "insert into semantic_chunks_fts(chunk_id, file_path, content) values (?, ?, ?)",
                            [(chunk[0], chunk[1], f"{chunk[4]}\n{chunk[6]}\n{chunk[7]}") for chunk in semantic_chunks],
                        )
                    self._insert_semantic_chunk_tokens(connection, semantic_chunks)
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
                    indexed_semantic_chunks += len(semantic_chunks)
                indexed_edges = self._build_graph_edges(connection)
                resolved_entity_edges = self._resolve_entity_edges(connection)
                indexed_entity_edges += resolved_entity_edges
                implementation_edges = self._build_implementation_edges(connection)
                indexed_entity_edges += implementation_edges
                aop_edges = self._build_aop_edges(connection)
                indexed_entity_edges += aop_edges
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
                    "indexed_semantic_chunks": str(indexed_semantic_chunks),
                    "reused_files": str(reused_files),
                    "reparsed_files": str(reparsed_files),
                    "index_refresh_strategy": "delta_row_reuse" if reusable_index is not None else "full_rebuild",
                    "parser_backend": "tree_sitter+regex" if tree_sitter_files else "regex",
                    "parser_languages": ",".join(sorted(parser_languages)),
                    "tree_sitter_files": str(tree_sitter_files),
                    "tree_sitter_errors": str(tree_sitter_errors),
                    "semantic_index_model": self.semantic_index_model,
                    "git_revision": git_revision,
                    "file_fts_enabled": "1" if file_fts_enabled else "0",
                    "fts_enabled": "1" if fts_enabled else "0",
                    "semantic_fts_enabled": "1" if semantic_fts_enabled else "0",
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
                "semantic_chunks": indexed_semantic_chunks,
                "reused_files": reused_files,
                "reparsed_files": reparsed_files,
                "index_refresh_strategy": metadata["index_refresh_strategy"],
                "parser_backend": metadata["parser_backend"],
                "parser_languages": sorted(parser_languages),
                "tree_sitter_files": tree_sitter_files,
                "tree_sitter_errors": tree_sitter_errors,
                "semantic_index_model": self.semantic_index_model,
                "git_revision": git_revision,
                "file_fts_enabled": file_fts_enabled,
                "fts_enabled": fts_enabled,
                "semantic_fts_enabled": semantic_fts_enabled,
                "updated_at": metadata["updated_at"],
            }
        finally:
            if reusable_index is not None:
                reusable_index.close()
            lock_path.unlink(missing_ok=True)

    def _open_reusable_index(self, index_path: Path) -> sqlite3.Connection | None:
        if not index_path.exists():
            return None
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(index_path)
            connection.row_factory = sqlite3.Row
            metadata = dict(connection.execute("select key, value from metadata").fetchall())
            if metadata.get("version") != str(CODE_INDEX_VERSION):
                connection.close()
                return None
            return connection
        except sqlite3.Error:
            if connection is not None:
                connection.close()
            return None

    @staticmethod
    def _copy_unchanged_index_file(
        old_connection: sqlite3.Connection | None,
        new_connection: sqlite3.Connection,
        relative_path: str,
        stat: os.stat_result,
        *,
        file_fts_enabled: bool,
        fts_enabled: bool,
        semantic_fts_enabled: bool,
    ) -> dict[str, int] | None:
        if old_connection is None:
            return None
        try:
            file_row = old_connection.execute("select * from files where path = ?", (relative_path,)).fetchone()
            if file_row is None:
                return None
            if int(file_row["size"]) != int(stat.st_size) or int(file_row["mtime_ns"]) != int(stat.st_mtime_ns):
                return None

            new_connection.execute("savepoint reuse_index_file")
            new_connection.execute(
                "insert into files(path, lower_path, size, mtime_ns, line_count, symbols) values (?, ?, ?, ?, ?, ?)",
                (
                    file_row["path"],
                    file_row["lower_path"],
                    file_row["size"],
                    file_row["mtime_ns"],
                    file_row["line_count"],
                    file_row["symbols"],
                ),
            )
            if file_fts_enabled:
                try:
                    symbols = " ".join(json.loads(file_row["symbols"] or "[]"))
                except (TypeError, json.JSONDecodeError):
                    symbols = ""
                new_connection.execute(
                    "insert into files_fts(path, content) values (?, ?)",
                    (
                        relative_path,
                        "\n".join(
                            [
                                relative_path,
                                relative_path.replace("/", " ").replace(".", " "),
                                symbols,
                            ]
                        ),
                    ),
                )

            line_rows = old_connection.execute(
                """
                select file_path, line_no, line_text, lower_text, symbols, is_declaration, has_pathish
                from lines
                where file_path = ?
                order by line_no
                """,
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                """
                insert into lines(file_path, line_no, line_text, lower_text, symbols, is_declaration, has_pathish)
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                [tuple(row) for row in line_rows],
            )
            if fts_enabled and line_rows:
                new_connection.executemany(
                    "insert into lines_fts(file_path, line_no, content) values (?, ?, ?)",
                    [(row["file_path"], row["line_no"], row["line_text"]) for row in line_rows],
                )
            file_token_rows = old_connection.execute(
                "select token, file_path from file_tokens where file_path = ?",
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                "insert or ignore into file_tokens(token, file_path) values (?, ?)",
                [tuple(row) for row in file_token_rows],
            )
            line_token_rows = old_connection.execute(
                "select token, file_path, line_no from line_tokens where file_path = ?",
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                "insert or ignore into line_tokens(token, file_path, line_no) values (?, ?, ?)",
                [tuple(row) for row in line_token_rows],
            )

            definition_rows = old_connection.execute(
                "select name, lower_name, kind, file_path, line_no, signature from definitions where file_path = ?",
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                "insert into definitions(name, lower_name, kind, file_path, line_no, signature) values (?, ?, ?, ?, ?, ?)",
                [tuple(row) for row in definition_rows],
            )

            reference_rows = old_connection.execute(
                """
                select target, lower_target, kind, file_path, line_no, context
                from references_index
                where file_path = ?
                """,
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                """
                insert into references_index(target, lower_target, kind, file_path, line_no, context)
                values (?, ?, ?, ?, ?, ?)
                """,
                [tuple(row) for row in reference_rows],
            )

            entity_rows = old_connection.execute(
                """
                select entity_id, name, lower_name, kind, language, file_path, line_no, parent, signature
                from code_entities
                where file_path = ?
                """,
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                """
                insert or ignore into code_entities(entity_id, name, lower_name, kind, language, file_path, line_no, parent, signature)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [tuple(row) for row in entity_rows],
            )

            raw_edge_rows = old_connection.execute(
                """
                select from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence
                from entity_edges
                where from_file = ? and (to_entity_id = '' or to_file = '')
                """,
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                """
                insert into entity_edges(from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [tuple(row) for row in raw_edge_rows],
            )

            semantic_rows = old_connection.execute(
                """
                select chunk_id, file_path, start_line, end_line, chunk_text, lower_text, tokens, symbols, embedding
                from semantic_chunks
                where file_path = ?
                """,
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                """
                insert into semantic_chunks(chunk_id, file_path, start_line, end_line, chunk_text, lower_text, tokens, symbols, embedding)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [tuple(row) for row in semantic_rows],
            )
            if semantic_fts_enabled and semantic_rows:
                new_connection.executemany(
                    "insert into semantic_chunks_fts(chunk_id, file_path, content) values (?, ?, ?)",
                    [
                        (
                            row["chunk_id"],
                            row["file_path"],
                            f"{row['chunk_text']}\n{row['tokens']}\n{row['symbols']}",
                        )
                        for row in semantic_rows
                    ],
                )
            semantic_token_rows = old_connection.execute(
                "select token, chunk_id, file_path from semantic_chunk_tokens where file_path = ?",
                (relative_path,),
            ).fetchall()
            new_connection.executemany(
                "insert or ignore into semantic_chunk_tokens(token, chunk_id, file_path) values (?, ?, ?)",
                [tuple(row) for row in semantic_token_rows],
            )

            new_connection.execute("release savepoint reuse_index_file")
            return {
                "lines": len(line_rows),
                "definitions": len(definition_rows),
                "references": len(reference_rows),
                "entities": len(entity_rows),
                "entity_edges": len(raw_edge_rows),
                "semantic_chunks": len(semantic_rows),
            }
        except (sqlite3.Error, KeyError, TypeError, ValueError):
            try:
                new_connection.execute("rollback to savepoint reuse_index_file")
                new_connection.execute("release savepoint reuse_index_file")
            except sqlite3.Error:
                pass
            return None

    @staticmethod
    def _index_tokens_for_text(text: str, symbols: set[str] | None = None) -> list[str]:
        tokens: set[str] = set(symbols or set())
        lowered = str(text or "").lower()
        tokens.update(match.group(0).lower() for match in FTS_TOKEN_PATTERN.finditer(lowered))
        tokens.update(match.group(0).lower() for match in IDENTIFIER_PATTERN.finditer(lowered))
        cleaned = {
            token.strip("._/-:")
            for token in tokens
            if len(token.strip("._/-:")) >= 3
            and token.strip("._/-:") not in STOPWORDS
            and token.strip("._/-:") not in LOW_VALUE_CALL_SYMBOLS
        }
        return sorted(cleaned)[:160]

    @classmethod
    def _insert_file_tokens(
        cls,
        connection: sqlite3.Connection,
        relative_path: str,
        content: str,
        symbols: set[str],
    ) -> None:
        rows = [
            (token, relative_path)
            for token in cls._index_tokens_for_text(f"{relative_path}\n{content}", symbols)
        ]
        connection.executemany(
            "insert or ignore into file_tokens(token, file_path) values (?, ?)",
            rows,
        )

    @classmethod
    def _insert_semantic_chunk_tokens(
        cls,
        connection: sqlite3.Connection,
        chunks: list[tuple[str, str, int, int, str, str, str, str, str]],
    ) -> None:
        rows: list[tuple[str, str, str]] = []
        for chunk in chunks:
            chunk_id, file_path, _start, _end, chunk_text, _lower, tokens_json, symbols_json, _embedding = chunk
            symbols: set[str] = set()
            try:
                symbols.update(str(item).lower() for item in json.loads(tokens_json or "[]"))
                symbols.update(str(item).lower() for item in json.loads(symbols_json or "[]"))
            except (TypeError, json.JSONDecodeError):
                pass
            rows.extend(
                (token, chunk_id, file_path)
                for token in cls._index_tokens_for_text(chunk_text, symbols)
            )
        connection.executemany(
            "insert or ignore into semantic_chunk_tokens(token, chunk_id, file_path) values (?, ?, ?)",
            rows,
        )

    @staticmethod
    def _build_semantic_chunks(relative_path: str, lines: list[str]) -> list[tuple[str, str, int, int, str, str, str, str, str]]:
        chunks: list[tuple[str, str, int, int, str, str, str, str, str]] = []
        if not lines:
            return chunks
        window = 32
        overlap = 8
        step = max(1, window - overlap)
        for start_index in range(0, len(lines), step):
            window_lines = lines[start_index : start_index + window]
            if not window_lines:
                continue
            chunk_text = "\n".join(window_lines).strip()
            if not chunk_text:
                continue
            lower_text = chunk_text.lower()
            tokens = SourceCodeQAService._semantic_tokens(f"{relative_path}\n{chunk_text}")
            symbols = sorted(SourceCodeQAService._line_symbols(lower_text))
            start_line = start_index + 1
            end_line = min(len(lines), start_index + len(window_lines))
            chunk_id = hashlib.sha1(f"{relative_path}:{start_line}:{end_line}:{chunk_text[:120]}".encode("utf-8")).hexdigest()[:16]
            chunks.append(
                (
                    chunk_id,
                    relative_path,
                    start_line,
                    end_line,
                    chunk_text[:6000],
                    lower_text[:6000],
                    json.dumps(tokens[:160], separators=(",", ":")),
                    json.dumps(symbols[:160], separators=(",", ":")),
                    "",
                )
            )
            if end_line >= len(lines):
                break
        return chunks

    def _attach_semantic_embeddings(
        self,
        chunks: list[tuple[str, str, int, int, str, str, str, str, str]],
    ) -> list[tuple[str, str, int, int, str, str, str, str, str]]:
        if not self.embedding_provider.ready() or self.embedding_provider.name == "local_token_hybrid":
            return chunks
        texts = [chunk[4] for chunk in chunks]
        try:
            embeddings = self.embedding_provider.embed_texts(texts, task_type=VERTEX_EMBEDDING_DOCUMENT_TASK)
        except ToolError:
            return chunks
        enriched = []
        for chunk, embedding in zip(chunks, embeddings):
            enriched.append((*chunk[:8], json.dumps(embedding[:2048], separators=(",", ":"))))
        if len(enriched) < len(chunks):
            enriched.extend(chunks[len(enriched):])
        return enriched

    @staticmethod
    def _semantic_tokens(text: str) -> list[str]:
        raw_tokens = re.findall(r"[A-Za-z0-9_./:-]{2,}", str(text or "").lower())
        tokens: list[str] = []
        for token in raw_tokens:
            token = token.strip("./:-_")
            if len(token) < 3 or token in STOPWORDS or token in LOW_VALUE_CALL_SYMBOLS:
                continue
            for part in re.split(r"[/_.:-]+", token):
                if len(part) >= 3 and part not in STOPWORDS and part not in tokens:
                    tokens.append(part)
            if token not in tokens:
                tokens.append(token)
        return tokens[:220]

    @staticmethod
    def _try_create_file_fts(connection: sqlite3.Connection) -> bool:
        try:
            connection.execute(
                "create virtual table files_fts using fts5(path unindexed, content)"
            )
            return True
        except sqlite3.Error:
            return False

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
    def _try_create_semantic_fts(connection: sqlite3.Connection) -> bool:
        try:
            connection.execute(
                "create virtual table semantic_chunks_fts using fts5(chunk_id unindexed, file_path unindexed, content)"
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
    def _build_implementation_edges(connection: sqlite3.Connection) -> int:
        bean_names_by_class: dict[str, set[str]] = {}
        for class_name, bean_name in connection.execute(
            """
            select c.name, e.to_name
            from entity_edges e
            join code_entities c on c.entity_id = e.from_entity_id
            where e.edge_kind = 'bean_name'
            """
        ):
            for key in SourceCodeQAService._symbol_lookup_keys(str(class_name)):
                bean_names_by_class.setdefault(key, set()).add(str(bean_name))

        primary_classes: set[str] = set()
        for class_name in connection.execute(
            """
            select c.name
            from entity_edges e
            join code_entities c on c.entity_id = e.from_entity_id
            where e.edge_kind = 'bean_primary'
            """
        ):
            for key in SourceCodeQAService._symbol_lookup_keys(str(class_name[0])):
                primary_classes.add(key)

        profiles_by_class: dict[str, set[str]] = {}
        for class_name, profile_name in connection.execute(
            """
            select c.name, e.to_name
            from entity_edges e
            join code_entities c on c.entity_id = e.from_entity_id
            where e.edge_kind = 'spring_profile'
            """
        ):
            for key in SourceCodeQAService._symbol_lookup_keys(str(class_name)):
                profiles_by_class.setdefault(key, set()).add(str(profile_name).strip().lower())

        active_profiles = SourceCodeQAService._active_spring_profiles(connection)
        config_values = SourceCodeQAService._spring_config_values(connection)

        conditions_by_class: dict[str, set[str]] = {}
        for class_name, condition in connection.execute(
            """
            select c.name, e.to_name
            from entity_edges e
            join code_entities c on c.entity_id = e.from_entity_id
            where e.edge_kind = 'bean_condition'
            """
        ):
            for key in SourceCodeQAService._symbol_lookup_keys(str(class_name)):
                conditions_by_class.setdefault(key, set()).add(str(condition).strip())

        qualifiers_by_file: dict[str, set[str]] = {}
        for from_file, qualifier in connection.execute(
            "select from_file, to_name from entity_edges where edge_kind = 'bean_qualifier'"
        ):
            qualifiers_by_file.setdefault(str(from_file), set()).add(str(qualifier))
        qualifiers_by_variable: dict[tuple[str, str], set[str]] = {}
        for from_file, target in connection.execute(
            "select from_file, to_name from entity_edges where edge_kind = 'bean_qualifier_target'"
        ):
            variable_name, separator, qualifier = str(target).partition("=")
            if separator and variable_name and qualifier:
                qualifiers_by_variable.setdefault((str(from_file), variable_name), set()).add(qualifier)

        implementors: dict[str, list[dict[str, Any]]] = {}
        for impl_name, from_file, from_line, interface_name in connection.execute(
            """
            select c.name, e.from_file, e.from_line, e.to_name
            from entity_edges e
            join code_entities c on c.entity_id = e.from_entity_id
            where e.edge_kind in ('implements', 'extends')
            """
        ):
            for key in SourceCodeQAService._symbol_lookup_keys(str(interface_name)):
                impl_keys = SourceCodeQAService._symbol_lookup_keys(str(impl_name))
                bean_names: set[str] = set()
                for impl_key in impl_keys:
                    bean_names.update(bean_names_by_class.get(impl_key) or set())
                profiles: set[str] = set()
                for impl_key in impl_keys:
                    profiles.update(profiles_by_class.get(impl_key) or set())
                conditions: set[str] = set()
                for impl_key in impl_keys:
                    conditions.update(conditions_by_class.get(impl_key) or set())
                implementors.setdefault(key, []).append(
                    {
                        "name": str(impl_name),
                        "file": str(from_file),
                        "line": int(from_line),
                        "bean_names": bean_names,
                        "profiles": profiles,
                        "conditions": conditions,
                        "primary": any(impl_key in primary_classes for impl_key in impl_keys),
                    }
                )

        definitions: dict[str, list[tuple[str, int]]] = {}
        for lower_name, file_path, line_no in connection.execute(
            "select lower_name, file_path, line_no from definitions"
        ):
            definitions.setdefault(str(lower_name), []).append((str(file_path), int(line_no)))

        rows: list[tuple[str, str, int, str, str, str, str, str, int, str]] = []
        for from_entity_id, from_file, from_line, to_name, evidence in connection.execute(
            """
            select from_entity_id, from_file, from_line, to_name, evidence
            from entity_edges
            where edge_kind = 'call' and instr(to_name, '.') > 0
            """
        ):
            owner, method_name = str(to_name).rsplit(".", 1)
            call_variable = SourceCodeQAService._member_call_variable(str(evidence or ""), method_name)
            for owner_key in SourceCodeQAService._symbol_lookup_keys(owner):
                candidates = implementors.get(owner_key, [])[:8]
                qualifiers = qualifiers_by_variable.get((str(from_file), call_variable), set()) if call_variable else set()
                if not qualifiers:
                    qualifiers = qualifiers_by_file.get(str(from_file)) or set()
                qualified_candidates = [
                    item
                    for item in candidates
                    if qualifiers and (item.get("bean_names") or set()) & qualifiers
                ]
                profile_candidates = [
                    item
                    for item in candidates
                    if active_profiles and (item.get("profiles") or set()) & active_profiles
                ]
                condition_candidates = [
                    item
                    for item in candidates
                    if any(
                        SourceCodeQAService._spring_condition_matches(str(condition), config_values)
                        for condition in (item.get("conditions") or set())
                    )
                ]
                primary_candidates = [item for item in candidates if item.get("primary")]
                selected_candidates = qualified_candidates or profile_candidates or condition_candidates or primary_candidates or candidates
                for item in selected_candidates:
                    impl_name = str(item.get("name") or "")
                    impl_call = f"{impl_name}.{method_name}"
                    targets = definitions.get(impl_call.lower()) or []
                    if not targets:
                        continue
                    to_file, to_line = targets[0]
                    matched_qualifiers = sorted((item.get("bean_names") or set()) & qualifiers)
                    qualifier_note = f" qualifier={matched_qualifiers[0]};" if matched_qualifiers else ""
                    matched_profiles = sorted((item.get("profiles") or set()) & active_profiles)
                    profile_note = f" profile={matched_profiles[0]};" if matched_profiles else ""
                    matched_conditions = sorted(
                        str(condition)
                        for condition in (item.get("conditions") or set())
                        if SourceCodeQAService._spring_condition_matches(str(condition), config_values)
                    )
                    condition_note = f" condition={matched_conditions[0]};" if matched_conditions else ""
                    primary_note = " primary=true;" if item.get("primary") else ""
                    rows.append(
                        (
                            str(from_entity_id),
                            str(from_file),
                            int(from_line),
                            "implementation_call",
                            impl_call,
                            impl_call.lower(),
                            "",
                            str(to_file),
                            int(to_line),
                            f"resolved implementation for {to_name}: {impl_call};{qualifier_note}{profile_note}{condition_note}{primary_note} {evidence or ''}"[:500],
                        )
                    )
        rows = list(dict.fromkeys(rows))
        connection.executemany(
            """
            insert into entity_edges(from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)

    @staticmethod
    def _symbol_lookup_keys(name: str) -> list[str]:
        value = str(name or "").strip().lower()
        if not value:
            return []
        keys = [value]
        short = value.rsplit(".", 1)[-1]
        if short and short not in keys:
            keys.append(short)
        return keys

    @staticmethod
    def _build_aop_edges(connection: sqlite3.Connection) -> int:
        definitions: dict[str, list[tuple[str, int, str]]] = {}
        for name, lower_name, file_path, line_no in connection.execute(
            "select name, lower_name, file_path, line_no from definitions"
        ):
            definitions.setdefault(str(lower_name), []).append((str(file_path), int(line_no), str(name)))

        implementors: dict[str, set[str]] = {}
        for impl_name, interface_name in connection.execute(
            """
            select c.name, e.to_name
            from entity_edges e
            join code_entities c on c.entity_id = e.from_entity_id
            where e.edge_kind in ('implements', 'extends')
            """
        ):
            for key in SourceCodeQAService._symbol_lookup_keys(str(interface_name)):
                implementors.setdefault(key, set()).add(str(impl_name))

        raw_aop_edges = list(connection.execute(
            """
            select c.entity_id, c.name, e.from_file, e.from_line, e.edge_kind, e.to_name, e.evidence
            from entity_edges e
            join code_entities c on c.entity_id = e.from_entity_id
            where e.edge_kind in ('aop_pointcut', 'aop_advice')
            """
        ))
        pointcuts_by_name: dict[str, str] = {}
        for _entity_id, entity_name, _from_file, _from_line, edge_kind, to_name, _evidence in raw_aop_edges:
            if str(edge_kind) == "aop_pointcut" and str(entity_name):
                pointcuts_by_name[str(entity_name).lower()] = SourceCodeQAService._aop_pointcut_expression(str(to_name), {})

        aop_edges: list[tuple[str, str, int, str, str, str]] = []
        for entity_id, _entity_name, from_file, from_line, _edge_kind, to_name, evidence in raw_aop_edges:
            pointcut_expression = SourceCodeQAService._aop_pointcut_expression(str(to_name), pointcuts_by_name)
            aop_edges.append((str(entity_id), str(from_file), int(from_line), str(to_name), pointcut_expression, str(evidence or "")))

        rows: list[tuple[str, str, int, str, str, str, str, str, int, str]] = []
        for from_entity_id, from_file, from_line, source_name, pointcut_expression, evidence in aop_edges:
            for target_name in SourceCodeQAService._aop_execution_target_names(pointcut_expression, implementors):
                for to_file, to_line, definition_name in SourceCodeQAService._definition_matches_for_aop_target(definitions, target_name):
                    rows.append(
                        (
                            from_entity_id,
                            from_file,
                            int(from_line),
                            "aop_applies_to",
                            definition_name,
                            definition_name.lower(),
                            "",
                            to_file,
                            int(to_line),
                            f"resolved AOP pointcut {source_name} -> {definition_name}; {evidence}"[:500],
                        )
                    )

        rows = list(dict.fromkeys(rows))
        connection.executemany(
            """
            insert into entity_edges(from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)

    @staticmethod
    def _aop_pointcut_expression(to_name: str, pointcuts_by_name: dict[str, str]) -> str:
        raw = str(to_name or "")
        _kind, separator, payload = raw.partition(":")
        value = payload if separator else raw
        normalized_reference = re.sub(r"\(\s*\)$", "", value.strip()).lower()
        return pointcuts_by_name.get(normalized_reference, value.strip())

    @staticmethod
    def _aop_execution_target_names(pointcut_expression: str, implementors: dict[str, set[str]]) -> list[str]:
        targets: list[str] = []
        text = str(pointcut_expression or "")
        for owner_name, method_name in re.findall(
            r"(?:execution|within|call)\s*\([^)]*?([A-Z][A-Za-z0-9_.$]*|\*)\.([A-Za-z_][A-Za-z0-9_]+)\s*\(",
            text,
        ):
            if owner_name == "*":
                targets.append(method_name)
                continue
            owner = owner_name.rsplit(".", 1)[-1]
            targets.append(f"{owner}.{method_name}")
            for owner_key in SourceCodeQAService._symbol_lookup_keys(owner):
                for impl_name in sorted(implementors.get(owner_key, set())):
                    targets.append(f"{impl_name}.{method_name}")
        for owner_name, method_name in re.findall(r"\*\s+\*+\.\.([A-Z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]+)\s*\(", text):
            targets.append(f"{owner_name}.{method_name}")
            for owner_key in SourceCodeQAService._symbol_lookup_keys(owner_name):
                for impl_name in sorted(implementors.get(owner_key, set())):
                    targets.append(f"{impl_name}.{method_name}")
        return list(dict.fromkeys(targets))

    @staticmethod
    def _definition_matches_for_aop_target(
        definitions: dict[str, list[tuple[str, int, str]]],
        target_name: str,
    ) -> list[tuple[str, int, str]]:
        normalized = str(target_name or "").strip().lower()
        if not normalized:
            return []
        matches: list[tuple[str, int, str]] = []
        if "." in normalized:
            candidates = definitions.get(normalized, [])
            suffix = f".{normalized}"
            for lower_name, rows in definitions.items():
                if lower_name.endswith(suffix):
                    candidates.extend(rows)
            matches.extend(candidates)
        else:
            matches.extend(definitions.get(normalized, []))
        return list(dict.fromkeys(matches))[:12]

    @staticmethod
    def _member_call_variable(context: str, method_name: str) -> str:
        for variable_name, called_method in MEMBER_CALL_PATTERN.findall(str(context or "")):
            if called_method == method_name:
                return variable_name
        return ""

    @staticmethod
    def _qualified_variable_targets(line: str) -> dict[str, list[str]]:
        targets: dict[str, list[str]] = {}
        for qualifier, variable_name in SPRING_QUALIFIED_VARIABLE_PATTERN.findall(str(line or "")):
            if qualifier and variable_name:
                targets.setdefault(variable_name, []).append(qualifier)
        return {variable: list(dict.fromkeys(qualifiers)) for variable, qualifiers in targets.items()}

    @staticmethod
    def _service_like_types_from_generic(generic_text: str) -> list[str]:
        return list(dict.fromkeys(SERVICE_LIKE_TYPE_PATTERN.findall(str(generic_text or ""))))

    @staticmethod
    def _active_spring_profiles(connection: sqlite3.Connection) -> set[str]:
        return SourceCodeQAService._active_spring_profiles_from_rows(
            SourceCodeQAService._spring_config_rows(connection)
        )

    @staticmethod
    def _active_spring_profiles_from_rows(rows: list[tuple[str, str, str, str]]) -> set[str]:
        profiles: set[str] = set()
        for file_path, key, value, doc_profile in rows:
            if SourceCodeQAService._spring_profile_from_config_path(file_path) or doc_profile:
                continue
            if str(key).lower() in {"spring.profiles.active", "spring.profiles.include"}:
                for profile in re.split(r"[,;\s]+", str(value or "")):
                    normalized = profile.strip().lower()
                    if normalized:
                        profiles.add(normalized)
        return profiles

    @staticmethod
    def _spring_config_values(connection: sqlite3.Connection) -> dict[str, set[str]]:
        values: dict[str, set[str]] = {}
        rows = SourceCodeQAService._spring_config_rows(connection)
        active_profiles = SourceCodeQAService._active_spring_profiles_from_rows(rows)
        profile_overrides: set[str] = set()
        for file_path, key, value, doc_profile in rows:
            normalized_key = str(key or "").strip().lower()
            normalized_value = str(value or "").strip().strip("\"'").lower()
            if not normalized_key or not normalized_value:
                continue
            file_profile = doc_profile or SourceCodeQAService._spring_profile_from_config_path(file_path)
            if file_profile and not SourceCodeQAService._spring_profile_matches(file_profile, active_profiles):
                continue
            if file_profile:
                if normalized_key not in profile_overrides:
                    values[normalized_key] = set()
                    profile_overrides.add(normalized_key)
            elif normalized_key in profile_overrides:
                continue
            values.setdefault(normalized_key, set()).add(normalized_value)
        return values

    @staticmethod
    def _spring_config_rows(connection: sqlite3.Connection) -> list[tuple[str, str, str, str]]:
        rows_out: list[tuple[str, str, str, str]] = []
        try:
            rows = connection.execute(
                """
                select file_path, line_text
                from lines
                where lower(file_path) glob '*.properties'
                   or lower(file_path) glob '*.yml'
                   or lower(file_path) glob '*.yaml'
                order by file_path, line_no
                """
            ).fetchall()
        except sqlite3.Error:
            return rows_out

        current_yaml_file = ""
        yaml_stack: list[tuple[int, str]] = []
        yaml_doc_rows: list[tuple[str, str]] = []
        yaml_doc_profile = ""

        def flush_yaml_doc() -> None:
            nonlocal yaml_doc_rows, yaml_doc_profile
            if not current_yaml_file:
                yaml_doc_rows = []
                yaml_doc_profile = ""
                return
            for key, value in yaml_doc_rows:
                rows_out.append((current_yaml_file, key, value, yaml_doc_profile))
            yaml_doc_rows = []
            yaml_doc_profile = ""

        for file_path, line_text in rows:
            file_path_str = str(file_path)
            suffix = Path(file_path_str).suffix.lower()
            if suffix in {".yaml", ".yml"}:
                stripped = str(line_text or "").strip()
                if current_yaml_file != file_path_str:
                    flush_yaml_doc()
                    current_yaml_file = file_path_str
                    yaml_stack = []
                if stripped.startswith("---"):
                    flush_yaml_doc()
                    yaml_stack = []
                    continue
                pair = SourceCodeQAService._extract_yaml_config_assignment(str(line_text or ""), yaml_stack)
            else:
                if current_yaml_file:
                    flush_yaml_doc()
                    current_yaml_file = ""
                    yaml_stack = []
                pair = SourceCodeQAService._extract_config_assignment(str(line_text or ""))
            if not pair:
                continue
            key, value = pair
            normalized_key = str(key or "").strip().lower()
            normalized_value = str(value or "").strip().strip("\"'")
            if normalized_key and normalized_value:
                if suffix in {".yaml", ".yml"}:
                    if normalized_key in {"spring.config.activate.on-profile", "spring.profiles"}:
                        yaml_doc_profile = normalized_value.strip().lower()
                    yaml_doc_rows.append((normalized_key, normalized_value))
                else:
                    rows_out.append((file_path_str, normalized_key, normalized_value, ""))
        flush_yaml_doc()
        return rows_out

    @staticmethod
    def _spring_profile_from_config_path(file_path: str) -> str:
        name = Path(str(file_path or "")).name.lower()
        match = re.match(r"(?:application|bootstrap)-([a-z0-9_.-]+)\.(?:properties|ya?ml)$", name)
        return match.group(1) if match else ""

    @staticmethod
    def _spring_profile_matches(profile_spec: str, active_profiles: set[str]) -> bool:
        spec = str(profile_spec or "").strip().lower()
        if not spec:
            return False
        candidates = [item.strip().lstrip("!") for item in re.split(r"[,;|\s&()]+", spec) if item.strip()]
        return any(candidate in active_profiles for candidate in candidates)

    @staticmethod
    def _spring_condition_matches(condition: str, config_values: dict[str, set[str]]) -> bool:
        if "=" not in str(condition or ""):
            return False
        key, expected_value = str(condition).split("=", 1)
        normalized_key = key.strip().lower()
        normalized_expected = expected_value.strip().strip("\"'").lower()
        values = config_values.get(normalized_key) or set()
        if normalized_expected == "<missing:true>":
            return not values
        if normalized_expected == "<present>":
            return bool(values) and "false" not in values
        return normalized_expected in values

    @staticmethod
    def _build_flow_edges(connection: sqlite3.Connection) -> int:
        rows: list[tuple[str, int, str, str, str, str, str, int, str]] = []

        for target, kind, file_path, line_no, context in connection.execute(
            """
            select target, kind, file_path, line_no, context
            from references_index
            where kind in ('route', 'sql_table', 'db_read', 'db_write', 'message_publish', 'message_consume', 'event_publish', 'event_consume')
            """
        ):
            edge_kind = str(kind)
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
                'data_flow', 'mapper_interface', 'implements', 'extends', 'implementation_call', 'route_prefix',
                'config_value', 'package', 'module_dependency', 'module_artifact', 'gradle_module', 'gradle_project_dependency',
                'db_read', 'db_write', 'message_publish', 'message_consume',
                'event_publish', 'event_consume', 'bean_qualifier', 'bean_qualifier_target', 'bean_name',
                'bean_primary', 'spring_profile', 'bean_condition',
                'aop_advice', 'aop_pointcut', 'aop_applies_to', 'scheduled_job', 'web_interceptor',
                'runtime_call', 'runtime_route', 'runtime_sql', 'runtime_message', 'runtime_config'
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
        if str(reference_kind) == "runtime_route":
            return "route"
        if str(reference_kind) == "runtime_sql":
            return "db_runtime"
        if str(reference_kind) == "runtime_message":
            return "message_runtime"
        if str(reference_kind) == "runtime_config":
            return "config"
        if str(reference_kind) == "runtime_call":
            return "runtime"
        if str(reference_kind) == "sql_table":
            return "sql_table"
        if str(reference_kind) in {"db_read", "db_write"}:
            return str(reference_kind)
        if str(reference_kind) in {"message_publish", "message_consume", "event_publish", "event_consume"}:
            return str(reference_kind)
        if str(reference_kind) == "mapper_statement":
            return "mapper"
        if str(reference_kind) == "mapper_interface":
            return "mapper"
        if str(reference_kind) in {"implements", "extends"}:
            return "type_hierarchy"
        if str(reference_kind) == "implementation_call":
            if to_role in {"service", "repository", "mapper", "dao", "controller", "client"}:
                return to_role
            return "implementation"
        if str(reference_kind) in {"downstream_api", "http_endpoint"}:
            return "client"
        if str(reference_kind) == "aop_applies_to":
            if to_role in {"service", "repository", "mapper", "dao", "controller", "client"}:
                return to_role
            return "framework"
        if str(reference_kind) in {"framework_binding", "aop_advice", "aop_pointcut", "scheduled_job", "web_interceptor"}:
            return "framework"
        if str(reference_kind) == "data_flow":
            return "field_population"
        if str(reference_kind) == "config_value":
            return "config"
        if str(reference_kind) == "package":
            return "type_hierarchy"
        if str(reference_kind) in {"module_dependency", "module_artifact", "gradle_module", "gradle_project_dependency"}:
            return "module_dependency"
        if str(reference_kind) in {"bean_qualifier", "bean_qualifier_target", "bean_name", "bean_primary", "spring_profile", "bean_condition"}:
            return "framework"
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

    def _tree_sitter_parser_for_language(self, language: str) -> Any | None:
        language = str(language or "").strip().lower()
        if not language:
            return None
        if language in self._tree_sitter_parsers:
            return self._tree_sitter_parsers[language]
        try:
            from tree_sitter import Language, Parser

            if language == "java":
                import tree_sitter_java as grammar

                tree_language = Language(grammar.language())
            elif language == "python":
                import tree_sitter_python as grammar

                tree_language = Language(grammar.language())
            elif language == "javascript":
                import tree_sitter_javascript as grammar

                tree_language = Language(grammar.language())
            elif language == "typescript":
                import tree_sitter_typescript as grammar

                tree_language = Language(grammar.language_typescript())
            elif language == "tsx":
                import tree_sitter_typescript as grammar

                tree_language = Language(grammar.language_tsx())
            else:
                self._tree_sitter_parsers[language] = None
                return None
            parser = Parser(tree_language)
            self._tree_sitter_parsers[language] = parser
            return parser
        except Exception as error:
            self._tree_sitter_load_errors[language] = str(error)[:240]
            self._tree_sitter_parsers[language] = None
            return None

    @staticmethod
    def _tree_sitter_language_for_suffix(suffix: str) -> str:
        return {
            ".java": "java",
            ".py": "python",
            ".js": "javascript",
            ".jsx": "javascript",
            ".ts": "typescript",
            ".tsx": "tsx",
        }.get(str(suffix or "").lower(), "")

    @staticmethod
    def _node_text(source: bytes, node: Any) -> str:
        try:
            return source[int(node.start_byte) : int(node.end_byte)].decode("utf-8", errors="ignore")
        except Exception:
            return ""

    @staticmethod
    def _node_line(lines: list[str], node: Any) -> str:
        try:
            line_no = int(node.start_point[0]) + 1
        except Exception:
            line_no = 1
        if 1 <= line_no <= len(lines):
            return lines[line_no - 1].strip()
        return ""

    def _extract_tree_sitter_structure(
        self,
        *,
        relative_path: str,
        lines: list[str],
        language: str,
        add_definition,
        add_reference,
        add_entity,
        add_entity_edge,
        file_entity_id: str,
    ) -> tuple[bool, str]:
        parser = self._tree_sitter_parser_for_language(language)
        if parser is None:
            return False, self._tree_sitter_load_errors.get(language, "parser unavailable")
        source = "\n".join(lines).encode("utf-8", errors="ignore")
        try:
            tree = parser.parse(source)
        except Exception as error:
            return False, str(error)[:240]
        root = tree.root_node
        if getattr(root, "has_error", False):
            return False, "parse error"

        def line_no(node: Any) -> int:
            try:
                return int(node.start_point[0]) + 1
            except Exception:
                return 1

        def first_named_child_text(node: Any, types: set[str]) -> str:
            for child in getattr(node, "named_children", []) or []:
                if str(child.type) in types:
                    return self._node_text(source, child).strip()
            return ""

        def name_for_node(node: Any) -> str:
            name_node = None
            try:
                name_node = node.child_by_field_name("name")
            except Exception:
                name_node = None
            if name_node is not None:
                value = self._node_text(source, name_node).strip()
                if value:
                    return value
            return first_named_child_text(node, {"identifier", "type_identifier", "property_identifier"})

        def call_target(node: Any) -> str:
            target_node = None
            for field in ("function", "name"):
                try:
                    target_node = node.child_by_field_name(field)
                except Exception:
                    target_node = None
                if target_node is not None:
                    break
            target = self._node_text(source, target_node).strip() if target_node is not None else ""
            if not target:
                raw = self._node_text(source, node)
                target = raw.split("(", 1)[0].strip()
            target = target.replace("this.", "").strip()
            return target[-180:]

        def type_text_for_node(node: Any) -> str:
            type_node = None
            try:
                type_node = node.child_by_field_name("type")
            except Exception:
                type_node = None
            if type_node is not None:
                value = self._node_text(source, type_node).strip()
                if value:
                    return value
            return first_named_child_text(node, {"type_identifier", "generic_type"})

        def string_values(node: Any) -> list[str]:
            values: list[str] = []
            raw = self._node_text(source, node)
            for value in re.findall(r"[\"']([^\"']+)[\"']", raw):
                if value and value not in values:
                    values.append(value)
            return values

        def add_route_edges(owner_id: str, node: Any, evidence: str) -> None:
            for route in re.findall(r"[\"']([^\"']+)[\"']", evidence):
                if route.startswith("/") or route.startswith("http"):
                    add_reference(route, "route", line_no(node), evidence)
                    add_entity_edge(owner_id, "route", route, line_no(node), evidence)

        def visit(node: Any, class_id: str, method_id: str, class_name: str = "") -> None:
            node_type = str(getattr(node, "type", ""))
            current_class_id = class_id
            current_method_id = method_id
            node_text_lines = self._node_text(source, node).splitlines()
            signature = self._node_line(lines, node) or (node_text_lines[0][:500] if node_text_lines else node_type)
            node_line = line_no(node)

            if node_type in {"class_declaration", "interface_declaration", "enum_declaration"}:
                name = name_for_node(node)
                if name:
                    kind = f"{language}_{node_type.replace('_declaration', '')}"
                    add_definition(name, kind, node_line, signature)
                    current_class_id = add_entity(name, kind, node_line, signature, parent=Path(relative_path).name)
                    current_method_id = current_class_id
                    class_name = name
                    raw = self._node_text(source, node)[:500]
                    for inherited in re.findall(r"\b(?:implements|extends)\s+([A-Z][A-Za-z0-9_]*(?:\s*,\s*[A-Z][A-Za-z0-9_]*)*)", raw):
                        for inherited_name in re.findall(r"[A-Z][A-Za-z0-9_]*", inherited):
                            add_reference(inherited_name, "type_hierarchy", node_line, signature)
                            add_entity_edge(current_class_id, "implements" if "implements" in raw else "extends", inherited_name, node_line, signature)
            elif node_type in {"method_declaration", "method_definition", "function_declaration", "function_definition"}:
                name = name_for_node(node)
                if name:
                    kind = f"{language}_{'method' if 'method' in node_type else 'function'}"
                    add_definition(name, kind, node_line, signature)
                    current_method_id = add_entity(name, kind, node_line, signature, parent=class_name)
                    if class_name and "method" in kind:
                        qualified_name = f"{class_name}.{name}"
                        add_definition(qualified_name, kind, node_line, signature)
                        add_entity(qualified_name, kind, node_line, signature, parent=class_name)
            elif node_type in {"field_declaration", "public_field_definition", "variable_declarator"}:
                raw = self._node_text(source, node)
                type_name = type_text_for_node(node) or first_named_child_text(node, {"type_identifier", "generic_type", "identifier"})
                variable_names = [
                    value
                    for value in IDENTIFIER_PATTERN.findall(raw)
                    if value not in {"private", "public", "protected", "static", "final", type_name}
                ]
                if variable_names:
                    add_definition(variable_names[-1], f"{language}_field", node_line, signature)
                if type_name and type_name not in variable_names and len(type_name) >= 3:
                    add_reference(type_name, "field_type", node_line, signature)
                    add_entity_edge(current_class_id or file_entity_id, "injects", type_name, node_line, signature)
            elif node_type in {"import_statement", "import_from_statement", "import_declaration"}:
                for value in string_values(node):
                    add_reference(value, "import", node_line, signature)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "import", value, node_line, signature)
                for dotted in re.findall(r"(?:import|from)\s+([A-Za-z0-9_.*{} ,/.-]+)", signature):
                    add_reference(dotted.strip(), "import", node_line, signature)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "import", dotted.strip(), node_line, signature)
            elif node_type in {"method_invocation", "call", "call_expression"}:
                target = call_target(node)
                if target and target.lower() not in LOW_VALUE_CALL_SYMBOLS:
                    add_reference(target, "call", node_line, signature)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", target, node_line, signature)

            if node_type in {"annotation", "marker_annotation", "decorator"} or "@" in signature:
                if "FeignClient" in signature:
                    for value in re.findall(r"[\"']([^\"']+)[\"']", signature):
                        add_reference(value, "downstream_api", node_line, signature)
                        add_entity_edge(current_class_id or file_entity_id, "downstream_api", value, node_line, signature)
                if any(marker in signature for marker in ("RestController", "Controller", "Service", "Repository", "Component")):
                    for marker in re.findall(r"@([A-Za-z0-9_]+)", signature):
                        add_reference(marker, "framework_binding", node_line, signature)
                        add_entity_edge(current_class_id or file_entity_id, "framework_binding", marker, node_line, signature)
                add_route_edges(current_method_id or current_class_id or file_entity_id, node, signature)

            raw_text = self._node_text(source, node)
            if node_type in {"string", "string_literal"} or any(client in signature.lower() for client in ("fetch", "axios", "resttemplate", "webclient")):
                for endpoint in HTTP_LITERAL_PATTERN.findall(raw_text or signature):
                    add_reference(endpoint, "http_endpoint", node_line, signature)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "http_endpoint", endpoint, node_line, signature)

            for child in getattr(node, "named_children", []) or []:
                visit(child, current_class_id, current_method_id, class_name)

        visit(root, file_entity_id, file_entity_id)
        return True, ""

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

        self._extract_build_file_structure(
            relative_path=relative_path,
            lines=lines,
            add_definition=add_definition,
            add_reference=add_reference,
            add_entity_edge=add_entity_edge,
            file_entity_id=file_entity_id,
        )
        self._extract_runtime_trace_structure(
            relative_path=relative_path,
            lines=lines,
            add_reference=add_reference,
            add_entity_edge=add_entity_edge,
            file_entity_id=file_entity_id,
        )

        tree_sitter_language = self._tree_sitter_language_for_suffix(suffix)
        tree_sitter_used = False
        tree_sitter_error = ""
        if tree_sitter_language:
            tree_sitter_used, tree_sitter_error = self._extract_tree_sitter_structure(
                relative_path=relative_path,
                lines=lines,
                language=tree_sitter_language,
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
        class_routes: list[str] = []
        pending_routes: list[tuple[str, int, str, str]] = []
        mapper_namespace = ""
        mapper_namespace_id = file_entity_id
        variable_types: dict[str, str] = {}
        collection_element_types: dict[str, str] = {}
        java_package = ""
        java_imports: dict[str, str] = {}
        pending_bean_names: list[tuple[str, int, str]] = []
        pending_profiles: list[tuple[str, int, str]] = []
        pending_primary: list[tuple[int, str]] = []
        pending_conditions: list[tuple[str, int, str]] = []
        pending_qualifiers: list[tuple[str, int, str]] = []
        pending_class_framework_edges: list[tuple[str, str, int, str]] = []
        pending_method_framework_edges: list[tuple[str, str, int, str]] = []
        variable_qualifiers: dict[str, set[str]] = {}
        yaml_config_stack: list[tuple[int, str]] = []
        is_test_file = self._is_test_file_path(relative_path)
        for line_no, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            if is_test_file or TEST_ANNOTATION_PATTERN.search(stripped):
                if TEST_ANNOTATION_PATTERN.search(stripped) or re.search(r"\b(?:test|should)[A-Za-z0-9_]*\s*\(", stripped):
                    add_reference("test_case", "test_case", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "test_case", "test_case", line_no, stripped)
                if TEST_ASSERTION_PATTERN.search(stripped):
                    add_reference("assertion", "test_assertion", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "test_assertion", "assertion", line_no, stripped)
                for call_name in CALL_SYMBOL_PATTERN.findall(stripped):
                    if call_name.lower() not in LOW_VALUE_CALL_SYMBOLS and len(call_name) >= 3:
                        add_reference(call_name, "test_reference", line_no, stripped)
                        add_entity_edge(current_method_id or current_class_id or file_entity_id, "test_reference", call_name, line_no, stripped)
                for subject in re.findall(r"\b([A-Z][A-Za-z0-9_]{3,})\b", stripped):
                    if subject in {"Test", "BeforeEach", "AfterEach", "Autowired", "MockBean", "Mockito", "Assertions", "Assert"}:
                        continue
                    if subject.endswith(("Test", "Tests", "Spec")):
                        continue
                    add_reference(subject, "test_subject", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "test_subject", subject, line_no, stripped)
            package_match = JAVA_PACKAGE_PATTERN.search(stripped)
            if package_match:
                java_package = package_match.group(1)
                add_definition(java_package, "java_package", line_no, stripped)
                add_entity_edge(file_entity_id, "package", java_package, line_no, stripped)
            import_match = JAVA_IMPORT_PATTERN.search(stripped)
            if import_match:
                imported_name = import_match.group(1)
                short_import = imported_name.rsplit(".", 1)[-1]
                if short_import and short_import != "*":
                    java_imports[short_import] = imported_name
                add_reference(imported_name, "import", line_no, stripped)
                add_entity_edge(file_entity_id, "import", imported_name, line_no, stripped)
            namespace_match = MYBATIS_NAMESPACE_PATTERN.search(line)
            if namespace_match:
                mapper_namespace = namespace_match.group(1)
                add_definition(mapper_namespace, "mybatis_mapper_namespace", line_no, stripped)
                mapper_namespace_id = self._entity_id(relative_path, "mybatis_mapper_namespace", mapper_namespace, line_no)
                mapper_short_name = mapper_namespace.rsplit(".", 1)[-1]
                if mapper_short_name:
                    add_reference(mapper_short_name, "mapper_interface", line_no, stripped)
                    add_entity_edge(mapper_namespace_id, "mapper_interface", mapper_short_name, line_no, stripped)
            result_map_match = MYBATIS_RESULT_MAP_PATTERN.search(line)
            if result_map_match:
                result_map_name = f"{mapper_namespace}.{result_map_match.group(1)}" if mapper_namespace else result_map_match.group(1)
                add_definition(result_map_name, "mybatis_result_map", line_no, stripped)
                result_map_id = self._entity_id(relative_path, "mybatis_result_map", result_map_name, line_no)
                add_entity_edge(mapper_namespace_id, "result_map", result_map_name, line_no, stripped)
                if mapper_namespace:
                    mapper_short_name = mapper_namespace.rsplit(".", 1)[-1]
                    short_result_map = f"{mapper_short_name}.{result_map_match.group(1)}"
                    add_definition(short_result_map, "mybatis_result_map", line_no, stripped)
                    add_entity_edge(result_map_id, "result_map_alias", short_result_map, line_no, stripped)
            include_match = MYBATIS_INCLUDE_PATTERN.search(line)
            if include_match:
                include_ref = include_match.group(1)
                add_reference(include_ref, "mybatis_include_refid", line_no, stripped)
                add_entity_edge(current_method_id or mapper_namespace_id, "mybatis_include_refid", include_ref, line_no, stripped)
            for attr_name, attr_value in MYBATIS_ATTR_REFERENCE_PATTERN.findall(line):
                if not attr_value:
                    continue
                attr_kind = "mybatis_result_map_ref" if attr_name.lower() == "resultmap" else "mybatis_type_ref"
                add_reference(attr_value, attr_kind, line_no, stripped)
                add_entity_edge(current_method_id or mapper_namespace_id, attr_kind, attr_value, line_no, stripped)
            statement_match = MYBATIS_STATEMENT_PATTERN.search(line)
            if statement_match:
                statement_name = f"{mapper_namespace}.{statement_match.group(2)}" if mapper_namespace else statement_match.group(2)
                add_definition(statement_name, f"mybatis_{statement_match.group(1).lower()}", line_no, stripped)
                statement_id = self._entity_id(relative_path, f"mybatis_{statement_match.group(1).lower()}", statement_name, line_no)
                add_entity_edge(mapper_namespace_id, "mapper_statement", statement_name, line_no, stripped)
                if mapper_namespace:
                    mapper_short_name = mapper_namespace.rsplit(".", 1)[-1]
                    qualified_statement = f"{mapper_short_name}.{statement_match.group(2)}"
                    add_definition(qualified_statement, f"mybatis_{statement_match.group(1).lower()}", line_no, stripped)
                    add_entity_edge(mapper_namespace_id, "mapper_statement", qualified_statement, line_no, stripped)
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
                if java_package:
                    add_definition(f"{java_package}.{current_class}", match.group(1).lower(), line_no, stripped)
                current_class_id = self._entity_id(relative_path, match.group(1).lower(), current_class, line_no)
                for bean_name, bean_line, bean_context in pending_bean_names:
                    add_reference(bean_name, "bean_name", bean_line, bean_context)
                    add_entity_edge(current_class_id, "bean_name", bean_name, bean_line, bean_context)
                pending_bean_names = []
                for profile_name, profile_line, profile_context in pending_profiles:
                    add_reference(profile_name, "spring_profile", profile_line, profile_context)
                    add_entity_edge(current_class_id, "spring_profile", profile_name, profile_line, profile_context)
                pending_profiles = []
                for primary_line, primary_context in pending_primary:
                    add_reference(current_class, "bean_primary", primary_line, primary_context)
                    add_entity_edge(current_class_id, "bean_primary", current_class, primary_line, primary_context)
                pending_primary = []
                for condition, condition_line, condition_context in pending_conditions:
                    add_reference(condition, "bean_condition", condition_line, condition_context)
                    add_entity_edge(current_class_id, "bean_condition", condition, condition_line, condition_context)
                pending_conditions = []
                for edge_kind, target, edge_line, edge_context in pending_class_framework_edges:
                    add_reference(target, edge_kind, edge_line, edge_context)
                    add_entity_edge(current_class_id, edge_kind, target, edge_line, edge_context)
                pending_class_framework_edges = []
                current_method = ""
                current_method_id = current_class_id
                class_routes = [route for route, _, _, annotation_name in pending_routes if annotation_name == "RequestMapping"]
                for route, route_line, route_context, annotation_name in pending_routes:
                    if annotation_name == "RequestMapping":
                        add_reference(route, "route", route_line, route_context)
                        add_entity_edge(current_class_id, "route", route, route_line, route_context)
                pending_routes = [item for item in pending_routes if item[3] != "RequestMapping"]
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
                if current_class:
                    add_definition(f"{current_class}.{method_name}", "java_method", line_no, stripped)
                    if java_package:
                        add_definition(f"{java_package}.{current_class}.{method_name}", "java_method", line_no, stripped)
                current_method = method_name
                current_method_id = self._entity_id(relative_path, "java_method", method_name, line_no)
                for edge_kind, target, edge_line, edge_context in pending_method_framework_edges:
                    add_reference(target, edge_kind, edge_line, edge_context)
                    add_entity_edge(current_method_id, edge_kind, target, edge_line, edge_context)
                pending_method_framework_edges = []
                for route, route_line, route_context, _annotation_name in pending_routes:
                    add_reference(route, "route", route_line, route_context)
                    add_entity_edge(current_method_id, "route", route, route_line, route_context)
                    for class_route in class_routes:
                        joined_route = self._join_routes(class_route, route)
                        if joined_route and joined_route != route:
                            add_reference(joined_route, "route", route_line, route_context)
                            add_entity_edge(current_method_id, "route", joined_route, route_line, route_context)
                            add_entity_edge(current_class_id or file_entity_id, "route_prefix", joined_route, route_line, route_context)
                if current_class and method_name == current_class:
                    for parameter_type in re.findall(r"\b([A-Z][A-Za-z0-9_]*(?:Service|Repository|Mapper|Client|Gateway|Adapter|Dao))\s+[a-z][A-Za-z0-9_]*", stripped):
                        add_reference(parameter_type, "field_type", line_no, stripped)
                        add_entity_edge(current_class_id or file_entity_id, "injects", parameter_type, line_no, stripped)
                pending_routes = []
            for annotation in ANNOTATION_ROUTE_PATTERN.finditer(line):
                add_definition(annotation.group(1), "route_annotation", line_no, stripped)
                for route in re.findall(r'"([^"]+)"', annotation.group(2) or ""):
                    add_reference(route, "route", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id, "route", route, line_no, stripped)
                    pending_routes.append((route, line_no, stripped, annotation.group(1)))
            spring_value_match = SPRING_VALUE_PATTERN.search(line)
            if spring_value_match:
                config_key = spring_value_match.group(1)
                add_reference(config_key, "config_key", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "config", config_key, line_no, stripped)
            qualifier_match = SPRING_QUALIFIER_PATTERN.search(line)
            line_qualifiers: list[tuple[str, int, str]] = []
            for qualifier_match in SPRING_QUALIFIER_PATTERN.finditer(line):
                qualifier = qualifier_match.group(2) or ""
                if qualifier:
                    add_reference(qualifier, "bean_qualifier", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "bean_qualifier", qualifier, line_no, stripped)
                    line_qualifiers.append((qualifier, line_no, stripped))
                    if stripped.startswith("@") and not FIELD_VAR_TYPE_PATTERN.search(stripped):
                        pending_qualifiers.append((qualifier, line_no, stripped))
            bean_match = SPRING_BEAN_NAME_PATTERN.search(line)
            if bean_match:
                bean_name = bean_match.group(2) or ""
                if bean_name:
                    target_id = current_class_id if current_class and not stripped.startswith("@") else file_entity_id
                    if target_id == file_entity_id:
                        pending_bean_names.append((bean_name, line_no, stripped))
                    else:
                        add_reference(bean_name, "bean_name", line_no, stripped)
                        add_entity_edge(target_id, "bean_name", bean_name, line_no, stripped)
            if SPRING_PRIMARY_PATTERN.search(line):
                if current_class and not stripped.startswith("@"):
                    add_reference(current_class, "bean_primary", line_no, stripped)
                    add_entity_edge(current_class_id, "bean_primary", current_class, line_no, stripped)
                else:
                    pending_primary.append((line_no, stripped))
            if SPRING_ASPECT_PATTERN.search(line):
                if current_class and not stripped.startswith("@"):
                    add_reference("Aspect", "framework_binding", line_no, stripped)
                    add_entity_edge(current_class_id, "framework_binding", "Aspect", line_no, stripped)
                else:
                    pending_class_framework_edges.append(("framework_binding", "Aspect", line_no, stripped))
            if SPRING_INTERCEPTOR_PATTERN.search(line):
                target = current_class or "HandlerInterceptor"
                add_reference(target, "web_interceptor", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "web_interceptor", target, line_no, stripped)
            for boundary_match in OPERATIONAL_BOUNDARY_PATTERN.finditer(line):
                boundary_name = boundary_match.group(1)
                boundary_args = self._annotation_target_text(boundary_match.group(2) or "")
                boundary_target = f"{boundary_name}:{boundary_args}" if boundary_args else boundary_name
                add_reference(boundary_target, "operational_boundary", line_no, stripped)
                if current_method and not stripped.startswith("@"):
                    add_entity_edge(current_method_id, "operational_boundary", boundary_target, line_no, stripped)
                else:
                    pending_method_framework_edges.append(("operational_boundary", boundary_target, line_no, stripped))
            for aop_match in SPRING_AOP_PATTERN.finditer(line):
                advice_kind = aop_match.group(1)
                pointcut_text = self._annotation_target_text(aop_match.group(2) or stripped) or advice_kind
                pointcut_target = f"{advice_kind}:{pointcut_text}"
                edge_kind = "aop_pointcut" if advice_kind == "Pointcut" else "aop_advice"
                add_reference(pointcut_target, edge_kind, line_no, stripped)
                if current_method and not stripped.startswith("@"):
                    add_entity_edge(current_method_id, edge_kind, pointcut_target, line_no, stripped)
                else:
                    pending_method_framework_edges.append((edge_kind, pointcut_target, line_no, stripped))
            scheduled_match = SPRING_SCHEDULED_PATTERN.search(line)
            if scheduled_match:
                schedule_target = self._scheduled_target_text(scheduled_match.group(1) or "") or "scheduled"
                add_reference(schedule_target, "scheduled_job", line_no, stripped)
                if current_method and not stripped.startswith("@"):
                    add_entity_edge(current_method_id, "scheduled_job", schedule_target, line_no, stripped)
                else:
                    pending_method_framework_edges.append(("scheduled_job", schedule_target, line_no, stripped))
            profile_match = SPRING_PROFILE_PATTERN.search(line)
            if profile_match:
                for profile in re.findall(r'"([^"]+)"|\'([^\']+)\'', profile_match.group(1)):
                    profile_name = next((item for item in profile if item), "")
                    if profile_name:
                        if current_class and not stripped.startswith("@"):
                            add_reference(profile_name, "spring_profile", line_no, stripped)
                            add_entity_edge(current_class_id, "spring_profile", profile_name, line_no, stripped)
                        else:
                            pending_profiles.append((profile_name, line_no, stripped))
            conditional_match = SPRING_CONDITIONAL_ON_PROPERTY_PATTERN.search(line)
            if conditional_match:
                condition_entries = self._spring_conditional_on_property_entries(conditional_match.group(1))
                for condition in condition_entries:
                    add_reference(condition, "bean_condition", line_no, stripped)
                    if current_class and not stripped.startswith("@"):
                        add_entity_edge(current_class_id, "bean_condition", condition, line_no, stripped)
                    else:
                        pending_conditions.append((condition, line_no, stripped))
                for property_name in (
                    self._spring_annotation_arg_values(conditional_match.group(1), "name")
                    or self._spring_annotation_arg_values(conditional_match.group(1), "value")
                ):
                    add_reference(property_name, "config_key", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, "config", property_name, line_no, stripped)
                for property_name in self._spring_annotation_arg_values(conditional_match.group(1), "prefix"):
                    add_reference(property_name, "config_key", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, "config", property_name, line_no, stripped)
            listener_match = MESSAGE_LISTENER_PATTERN.search(line)
            if listener_match:
                for topic in self._extract_message_names(listener_match.group(2)):
                    add_reference(topic, "message_consume", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "message_consume", topic, line_no, stripped)
            for send_match in MESSAGE_SEND_PATTERN.finditer(line):
                for topic in self._extract_message_names(send_match.group(1)):
                    add_reference(topic, "message_publish", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "message_publish", topic, line_no, stripped)
            for event_match in EVENT_PUBLISH_PATTERN.finditer(line):
                for event_name in self._extract_event_names(event_match.group(1)):
                    add_reference(event_name, "event_publish", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "event_publish", event_name, line_no, stripped)
            if "@EventListener" in stripped or "@TransactionalEventListener" in stripped:
                for event_name in re.findall(r"\b([A-Z][A-Za-z0-9_]*(?:Event|Message|Command))\b", stripped):
                    add_reference(event_name, "event_consume", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "event_consume", event_name, line_no, stripped)
            for table in SQL_TABLE_PATTERN.findall(line):
                add_reference(table, "sql_table", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id, "sql_table", table, line_no, stripped)
            for table in SQL_READ_TABLE_PATTERN.findall(line):
                add_reference(table, "db_read", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id, "db_read", table, line_no, stripped)
            for table in SQL_WRITE_TABLE_PATTERN.findall(line):
                add_reference(table, "db_write", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id, "db_write", table, line_no, stripped)
            for endpoint in HTTP_LITERAL_PATTERN.findall(line):
                if endpoint.startswith("http") or any(client in stripped.lower() for client in ("resttemplate", "webclient", "feign", "exchange", "postfor", "getfor", "request")):
                    add_reference(endpoint, "http_endpoint", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "http_endpoint", endpoint, line_no, stripped)
            if suffix in {".properties", ".yaml", ".yml", ".conf", ".toml"}:
                config_pair = (
                    self._extract_yaml_config_assignment(line, yaml_config_stack)
                    if suffix in {".yaml", ".yml"}
                    else self._extract_config_assignment(stripped)
                )
                key_match = PROPERTIES_KEY_PATTERN.search(line)
                if config_pair:
                    config_key, config_value = config_pair
                    add_definition(config_key, "config_key", line_no, stripped)
                    add_entity_edge(file_entity_id, "config", config_key, line_no, stripped)
                    if config_value:
                        add_reference(config_value, "config_value", line_no, stripped)
                        add_entity_edge(file_entity_id, "config_value", config_value, line_no, stripped)
                    for endpoint in HTTP_LITERAL_PATTERN.findall(f"'{config_value}'"):
                        add_reference(endpoint, "http_endpoint", line_no, stripped)
                        add_entity_edge(file_entity_id, "http_endpoint", endpoint, line_no, stripped)
                    if re.search(r"\b[a-z0-9-]+-service\b", config_value, re.IGNORECASE):
                        add_reference(config_value, "downstream_api", line_no, stripped)
                        add_entity_edge(file_entity_id, "downstream_api", config_value, line_no, stripped)
                elif key_match:
                    add_definition(key_match.group(1), "config_key", line_no, stripped)
                    add_entity_edge(file_entity_id, "config", key_match.group(1), line_no, stripped)
            field_match = FIELD_OR_PARAM_TYPE_PATTERN.search(line)
            if field_match:
                add_entity_edge(current_class_id or file_entity_id, "injects", field_match.group(1), line_no, stripped)
            qualified_variable_targets = self._qualified_variable_targets(stripped)
            typed_variables = FIELD_VAR_TYPE_PATTERN.findall(stripped)
            for type_name, variable_name in typed_variables:
                variable_types[variable_name] = type_name
                add_reference(type_name, "field_type", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "injects", type_name, line_no, stripped)
                targeted_qualifiers = [
                    (qualifier, line_no, stripped)
                    for qualifier in qualified_variable_targets.get(variable_name, [])
                ]
                fallback_line_qualifiers = line_qualifiers if not targeted_qualifiers and len(typed_variables) == 1 else []
                for qualifier, qualifier_line, qualifier_context in pending_qualifiers + targeted_qualifiers + fallback_line_qualifiers:
                    add_reference(f"{variable_name}={qualifier}", "bean_qualifier_target", qualifier_line, qualifier_context)
                    add_entity_edge(current_class_id or file_entity_id, "bean_qualifier_target", f"{variable_name}={qualifier}", qualifier_line, qualifier_context)
                    variable_qualifiers.setdefault(variable_name, set()).add(qualifier)
                if pending_qualifiers:
                    pending_qualifiers = []
                imported_type = java_imports.get(type_name)
                if imported_type:
                    add_reference(imported_type, "field_type", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, "injects", imported_type, line_no, stripped)
            for field_name, source_variable in THIS_FIELD_ASSIGNMENT_PATTERN.findall(stripped):
                for qualifier in sorted(variable_qualifiers.get(source_variable, set())):
                    add_reference(f"{field_name}={qualifier}", "bean_qualifier_target", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, "bean_qualifier_target", f"{field_name}={qualifier}", line_no, stripped)
                    variable_qualifiers.setdefault(field_name, set()).add(qualifier)
            simple_variable_names = {variable_name for _type_name, variable_name in typed_variables}
            for generic_text, variable_name in GENERIC_FIELD_VAR_TYPE_PATTERN.findall(stripped):
                if variable_name in simple_variable_names:
                    continue
                inner_types = self._service_like_types_from_generic(generic_text)
                if not inner_types:
                    continue
                element_type = inner_types[-1]
                collection_element_types[variable_name] = element_type
                add_reference(element_type, "field_type", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "injects", element_type, line_no, stripped)
                targeted_qualifiers = [
                    (qualifier, line_no, stripped)
                    for qualifier in qualified_variable_targets.get(variable_name, [])
                ]
                fallback_line_qualifiers = line_qualifiers if not targeted_qualifiers else []
                for qualifier, qualifier_line, qualifier_context in pending_qualifiers + targeted_qualifiers + fallback_line_qualifiers:
                    add_reference(f"{variable_name}={qualifier}", "bean_qualifier_target", qualifier_line, qualifier_context)
                    add_entity_edge(current_class_id or file_entity_id, "bean_qualifier_target", f"{variable_name}={qualifier}", qualifier_line, qualifier_context)
                    variable_qualifiers.setdefault(variable_name, set()).add(qualifier)
                if pending_qualifiers:
                    pending_qualifiers = []
                imported_type = java_imports.get(element_type)
                if imported_type:
                    add_reference(imported_type, "field_type", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, "injects", imported_type, line_no, stripped)
            for collection_variable, lambda_variable in STREAM_LAMBDA_PATTERN.findall(stripped):
                element_type = collection_element_types.get(collection_variable) or variable_types.get(collection_variable)
                if element_type:
                    variable_types[lambda_variable] = element_type
                    for qualifier in sorted(variable_qualifiers.get(collection_variable, set())):
                        add_reference(f"{lambda_variable}={qualifier}", "bean_qualifier_target", line_no, stripped)
                        add_entity_edge(current_class_id or file_entity_id, "bean_qualifier_target", f"{lambda_variable}={qualifier}", line_no, stripped)
                        variable_qualifiers.setdefault(lambda_variable, set()).add(qualifier)
            for provider_variable, method_name in PROVIDER_CHAIN_CALL_PATTERN.findall(stripped):
                owner_type = collection_element_types.get(provider_variable) or variable_types.get(provider_variable)
                if owner_type:
                    qualified_call = f"{owner_type}.{method_name}"
                    add_reference(qualified_call, "call", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", qualified_call, line_no, stripped)
                    imported_type = java_imports.get(owner_type)
                    if imported_type:
                        imported_call = f"{imported_type}.{method_name}"
                        add_reference(imported_call, "call", line_no, stripped)
                        add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", imported_call, line_no, stripped)
            for variable_name, method_name in MEMBER_CALL_PATTERN.findall(stripped):
                owner_type = variable_types.get(variable_name)
                if owner_type:
                    qualified_call = f"{owner_type}.{method_name}"
                    add_reference(qualified_call, "call", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", qualified_call, line_no, stripped)
                    imported_type = java_imports.get(owner_type)
                    if imported_type:
                        imported_call = f"{imported_type}.{method_name}"
                        add_reference(imported_call, "call", line_no, stripped)
                        add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", imported_call, line_no, stripped)
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
            "tree_sitter_used": tree_sitter_used,
            "tree_sitter_language": tree_sitter_language if tree_sitter_used else "",
            "tree_sitter_error": tree_sitter_error,
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

    @staticmethod
    def _extract_config_assignment(line: str) -> tuple[str, str] | None:
        stripped = str(line or "").strip()
        if not stripped or stripped.startswith(("#", "//", "- ")):
            return None
        match = CONFIG_ASSIGNMENT_PATTERN.search(stripped)
        if not match:
            return None
        key = match.group(1).strip()
        value = match.group(2).strip().strip("\"'")
        if not key or not value:
            return None
        return key, value

    def _extract_runtime_trace_structure(
        self,
        *,
        relative_path: str,
        lines: list[str],
        add_reference: Any,
        add_entity_edge: Any,
        file_entity_id: str,
    ) -> None:
        if not self._is_runtime_trace_file(relative_path):
            return
        for line_no, raw_line in enumerate(lines, start=1):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            edge_kind, target = self._runtime_trace_edge(payload)
            if not edge_kind or not target:
                continue
            evidence = self._runtime_trace_evidence(payload)
            add_reference(target, edge_kind, line_no, evidence)
            add_entity_edge(file_entity_id, edge_kind, target, line_no, evidence)

    @staticmethod
    def _is_runtime_trace_file(relative_path: str) -> bool:
        path = Path(str(relative_path or ""))
        if path.suffix.lower() != ".jsonl":
            return False
        lowered_name = path.name.lower()
        lowered_parts = {part.lower() for part in path.parts}
        runtime_dirs = {"runtime-traces", "runtime_traces", "source-code-qa-traces", "source_code_qa_traces"}
        return lowered_name in RUNTIME_TRACE_FILENAMES or bool(runtime_dirs & lowered_parts)

    def _runtime_trace_edge(self, payload: dict[str, Any]) -> tuple[str, str]:
        kind_text = self._runtime_trace_string(payload, ("kind", "type", "event", "span_kind")).lower()
        if any(token in kind_text for token in ("route", "http", "request", "endpoint")):
            return "runtime_route", self._runtime_trace_target(
                payload, ("route", "path", "url", "http_path", "endpoint", "target", "to", "handler")
            )
        if any(token in kind_text for token in ("sql", "db", "database", "table")):
            return "runtime_sql", self._runtime_trace_sql_target(payload)
        if any(token in kind_text for token in ("message", "kafka", "rabbit", "jms", "topic", "queue", "event")):
            return "runtime_message", self._runtime_trace_target(
                payload, ("topic", "queue", "channel", "message", "event_name", "target", "to")
            )
        if any(token in kind_text for token in ("config", "feature", "flag", "property")):
            return "runtime_config", self._runtime_trace_target(
                payload, ("key", "config", "property", "feature_flag", "flag", "name", "target", "to")
            )
        if any(token in kind_text for token in ("call", "method", "function", "span")):
            return "runtime_call", self._runtime_trace_target(
                payload, ("to", "target", "callee", "method", "function", "operation", "handler")
            )
        if self._runtime_trace_target(payload, ("route", "path", "url", "http_path", "endpoint")):
            return "runtime_route", self._runtime_trace_target(payload, ("route", "path", "url", "http_path", "endpoint"))
        if self._runtime_trace_target(payload, ("table", "sql", "statement", "query")):
            return "runtime_sql", self._runtime_trace_sql_target(payload)
        if self._runtime_trace_target(payload, ("topic", "queue", "channel")):
            return "runtime_message", self._runtime_trace_target(payload, ("topic", "queue", "channel"))
        if self._runtime_trace_target(payload, ("key", "config", "property", "feature_flag", "flag")):
            return "runtime_config", self._runtime_trace_target(
                payload, ("key", "config", "property", "feature_flag", "flag")
            )
        return "runtime_call", self._runtime_trace_target(payload, ("to", "target", "callee", "operation", "handler"))

    def _runtime_trace_sql_target(self, payload: dict[str, Any]) -> str:
        table = self._runtime_trace_target(payload, ("table", "db_table", "entity"))
        if table:
            return table
        sql = self._runtime_trace_target(payload, ("sql", "statement", "query"))
        for pattern in (SQL_READ_TABLE_PATTERN, SQL_WRITE_TABLE_PATTERN, SQL_TABLE_PATTERN):
            match = pattern.search(sql)
            if match:
                return match.group(1)
        return sql[:160]

    @staticmethod
    def _runtime_trace_target(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
        for key in keys:
            value = SourceCodeQAService._runtime_trace_string(payload, (key,))
            if value:
                return value
        return ""

    @staticmethod
    def _runtime_trace_string(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
        for key in keys:
            if key not in payload:
                continue
            value = payload.get(key)
            if value is None:
                continue
            if isinstance(value, (str, int, float, bool)):
                normalized = str(value).strip()
            else:
                normalized = json.dumps(value, ensure_ascii=False, sort_keys=True)
            if normalized:
                return normalized
        return ""

    @staticmethod
    def _runtime_trace_evidence(payload: dict[str, Any]) -> str:
        source = SourceCodeQAService._runtime_trace_target(payload, ("from", "source", "caller", "handler", "span"))
        target = SourceCodeQAService._runtime_trace_target(
            payload, ("to", "target", "callee", "route", "path", "url", "table", "topic", "queue", "key")
        )
        evidence = SourceCodeQAService._runtime_trace_string(payload, ("evidence", "summary", "trace_id", "span_id"))
        parts = []
        if source:
            parts.append(f"from={source}")
        if target:
            parts.append(f"to={target}")
        if evidence:
            parts.append(evidence)
        if not parts:
            parts.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return " | ".join(parts)[:500]

    @staticmethod
    def _extract_yaml_config_assignment(line: str, stack: list[tuple[int, str]]) -> tuple[str, str] | None:
        raw = str(line or "").rstrip()
        stripped = raw.strip()
        if not stripped or stripped.startswith(("#", "---", "...", "- ")):
            return None
        match = re.match(r"^(\s*)([A-Za-z0-9_.-]{2,})\s*:\s*(.*?)\s*$", raw)
        if not match:
            return None
        indent = len(match.group(1).replace("\t", "    "))
        key = match.group(2).strip()
        value = match.group(3).strip()
        while stack and stack[-1][0] >= indent:
            stack.pop()
        full_key = ".".join([item[1] for item in stack] + [key])
        if not value or value in {"|", ">"}:
            stack.append((indent, key))
            return full_key, ""
        value = re.sub(r"\s+#.*$", "", value).strip().strip("\"'")
        if not full_key:
            return None
        return full_key, value

    @staticmethod
    def _spring_annotation_arg_values(annotation_args: str, key: str) -> list[str]:
        values: list[str] = []
        pattern = re.compile(
            rf"\b{re.escape(key)}\s*=\s*(\{{[^}}]*\}}|\"[^\"]*\"|'[^']*'|[A-Za-z0-9_.-]+)"
        )
        for match in pattern.finditer(str(annotation_args or "")):
            raw_value = match.group(1).strip()
            quoted_values = re.findall(r"\"([^\"]+)\"|'([^']+)'", raw_value)
            if quoted_values:
                values.extend(next((item for item in group if item), "") for group in quoted_values)
            else:
                values.extend(item.strip() for item in raw_value.strip("{}").split(","))
        return list(dict.fromkeys(value.strip() for value in values if value and value.strip()))

    @staticmethod
    def _spring_conditional_on_property_entries(annotation_args: str) -> list[str]:
        prefix_values = SourceCodeQAService._spring_annotation_arg_values(annotation_args, "prefix")
        prefix = prefix_values[0].strip(".") if prefix_values else ""
        property_names = (
            SourceCodeQAService._spring_annotation_arg_values(annotation_args, "name")
            or SourceCodeQAService._spring_annotation_arg_values(annotation_args, "value")
        )
        having_values = SourceCodeQAService._spring_annotation_arg_values(annotation_args, "havingValue")
        having_value = having_values[0] if having_values else "<present>"
        match_if_missing = any(
            value.lower() == "true"
            for value in SourceCodeQAService._spring_annotation_arg_values(annotation_args, "matchIfMissing")
        )
        conditions: list[str] = []
        for property_name in property_names:
            normalized_name = property_name.strip(".")
            if not normalized_name:
                continue
            full_key = (
                normalized_name
                if not prefix or normalized_name.startswith(f"{prefix}.")
                else f"{prefix}.{normalized_name}"
            )
            conditions.append(f"{full_key}={having_value}")
            if match_if_missing:
                conditions.append(f"{full_key}=<missing:true>")
        return list(dict.fromkeys(conditions))

    @staticmethod
    def _annotation_target_text(annotation_args: str) -> str:
        text = str(annotation_args or "").strip()
        quoted_values = re.findall(r"\"([^\"]+)\"|'([^']+)'", text)
        values = [next((item for item in group if item), "") for group in quoted_values]
        values = [value.strip() for value in values if value and value.strip()]
        if values:
            return values[0]
        cleaned = re.sub(r"^\s*(?:value|pointcut)\s*=\s*", "", text).strip()
        return cleaned[:200]

    @staticmethod
    def _scheduled_target_text(annotation_args: str) -> str:
        text = str(annotation_args or "").strip()
        if not text:
            return "scheduled"
        entries: list[str] = []
        for key in ("cron", "fixedRateString", "fixedDelayString", "initialDelayString"):
            for value in SourceCodeQAService._spring_annotation_arg_values(text, key):
                entries.append(f"{key}={value}")
        for key in ("fixedRate", "fixedDelay", "initialDelay"):
            match = re.search(rf"\b{re.escape(key)}\s*=\s*([0-9]+)", text)
            if match:
                entries.append(f"{key}={match.group(1)}")
        return ";".join(entries[:4]) or SourceCodeQAService._annotation_target_text(text) or "scheduled"

    @staticmethod
    def _extract_message_names(argument_text: str) -> list[str]:
        names: list[str] = []
        text = str(argument_text or "")
        for value in re.findall(r"[\"']([^\"']{3,120})[\"']", text):
            lowered = value.lower()
            if any(marker in lowered for marker in ("topic", "queue", "exchange", "event", "issue", "command", ".", "-", "_")):
                names.append(value)
        for value in re.findall(r"\$\{([^}:]+)(?::[^}]*)?\}", text):
            names.append(value)
        return list(dict.fromkeys(name.strip() for name in names if name.strip()))[:8]

    @staticmethod
    def _extract_event_names(argument_text: str) -> list[str]:
        text = str(argument_text or "")
        names = []
        for value in re.findall(r"\bnew\s+([A-Z][A-Za-z0-9_]*(?:Event|Message|Command))\b", text):
            names.append(value)
        for value in re.findall(r"\b([A-Z][A-Za-z0-9_]*(?:Event|Message|Command))\.class\b", text):
            names.append(value)
        for value in re.findall(r"[\"']([^\"']*(?:event|message|command)[^\"']*)[\"']", text, re.IGNORECASE):
            names.append(value)
        return list(dict.fromkeys(name.strip() for name in names if name.strip()))[:8]

    def _extract_build_file_structure(
        self,
        *,
        relative_path: str,
        lines: list[str],
        add_definition: Any,
        add_reference: Any,
        add_entity_edge: Any,
        file_entity_id: str,
    ) -> None:
        lowered_path = str(relative_path or "").lower()
        filename = Path(relative_path).name.lower()
        if filename == "package.json":
            try:
                payload = json.loads("\n".join(lines))
            except json.JSONDecodeError:
                payload = {}
            package_name = str(payload.get("name") or "").strip() if isinstance(payload, dict) else ""
            if package_name:
                line_no = self._first_line_number_containing(lines, package_name)
                add_definition(package_name, "npm_package", line_no, package_name)
                add_entity_edge(file_entity_id, "module_artifact", package_name, line_no, package_name)
            for section in ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies"):
                dependencies = payload.get(section) if isinstance(payload, dict) else {}
                if not isinstance(dependencies, dict):
                    continue
                for dependency_name in dependencies:
                    dependency = str(dependency_name or "").strip()
                    if not dependency:
                        continue
                    line_no = self._first_line_number_containing(lines, dependency)
                    add_reference(dependency, "module_dependency", line_no, dependency)
                    add_entity_edge(file_entity_id, "module_dependency", dependency, line_no, dependency)
            return
        if filename == "pom.xml":
            full_text = "\n".join(lines)
            project_header = full_text.split("<dependencies>", 1)[0]
            project_tags = dict(MAVEN_TAG_PATTERN.findall(project_header))
            project_group = str(project_tags.get("groupId") or "").strip()
            project_artifact = str(project_tags.get("artifactId") or "").strip()
            if project_artifact:
                line_no = self._first_line_number_containing(lines, project_artifact)
                add_definition(project_artifact, "maven_artifact", line_no, project_artifact)
                add_entity_edge(file_entity_id, "module_artifact", project_artifact, line_no, project_artifact)
                if project_group:
                    coordinate = f"{project_group}:{project_artifact}"
                    add_definition(coordinate, "maven_coordinate", line_no, coordinate)
                    add_entity_edge(file_entity_id, "module_artifact", coordinate, line_no, coordinate)
            for block in MAVEN_DEPENDENCY_BLOCK_PATTERN.findall(full_text):
                tags = dict(MAVEN_TAG_PATTERN.findall(block))
                group_id = str(tags.get("groupId") or "").strip()
                artifact_id = str(tags.get("artifactId") or "").strip()
                if not artifact_id or "$" in artifact_id:
                    continue
                coordinate = f"{group_id}:{artifact_id}" if group_id and "$" not in group_id else artifact_id
                line_no = self._first_line_number_containing(lines, artifact_id)
                add_reference(coordinate, "module_dependency", line_no, block)
                add_entity_edge(file_entity_id, "module_dependency", coordinate, line_no, block)
                if coordinate != artifact_id:
                    add_reference(artifact_id, "module_dependency", line_no, block)
                    add_entity_edge(file_entity_id, "module_dependency", artifact_id, line_no, block)
            return
        if filename in {"build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts"} or lowered_path.endswith(".gradle"):
            for line_no, line in enumerate(lines, start=1):
                stripped = line.strip()
                if not stripped or stripped.startswith(("//", "#")):
                    continue
                for group_id, artifact_id in GRADLE_COORDINATE_PATTERN.findall(stripped):
                    coordinate = f"{group_id}:{artifact_id}"
                    add_reference(coordinate, "module_dependency", line_no, stripped)
                    add_entity_edge(file_entity_id, "module_dependency", coordinate, line_no, stripped)
                    add_reference(artifact_id, "module_dependency", line_no, stripped)
                    add_entity_edge(file_entity_id, "module_dependency", artifact_id, line_no, stripped)
                for module_name in GRADLE_PROJECT_DEPENDENCY_PATTERN.findall(stripped):
                    raw_module = module_name.strip()
                    normalized_module = self._normalize_gradle_module_name(raw_module)
                    if normalized_module:
                        add_reference(normalized_module, "module_dependency", line_no, stripped)
                        add_entity_edge(file_entity_id, "module_dependency", normalized_module, line_no, stripped)
                        add_reference(normalized_module, "gradle_project_dependency", line_no, stripped)
                        add_entity_edge(file_entity_id, "gradle_project_dependency", normalized_module, line_no, stripped)
                        if raw_module and raw_module != normalized_module:
                            add_reference(raw_module, "gradle_project_dependency", line_no, stripped)
                            add_entity_edge(file_entity_id, "gradle_project_dependency", raw_module, line_no, stripped)
                include_match = GRADLE_INCLUDE_PATTERN.search(stripped)
                if include_match:
                    for module_name in re.findall(r"[\"']:([^\"']+)[\"']", include_match.group(1)):
                        raw_module = f":{module_name.strip().strip(':')}"
                        normalized_module = self._normalize_gradle_module_name(raw_module)
                        if normalized_module:
                            add_definition(normalized_module, "gradle_module", line_no, stripped)
                            add_entity_edge(file_entity_id, "module_artifact", normalized_module, line_no, stripped)
                            add_entity_edge(file_entity_id, "gradle_module", normalized_module, line_no, stripped)
                            add_definition(raw_module, "gradle_module", line_no, stripped)
                            add_entity_edge(file_entity_id, "gradle_module", raw_module, line_no, stripped)

    @staticmethod
    def _normalize_gradle_module_name(module_name: str) -> str:
        return str(module_name or "").strip().strip(":").replace(":", "-")

    @staticmethod
    def _first_line_number_containing(lines: list[str], needle: str) -> int:
        value = str(needle or "")
        if value:
            for index, line in enumerate(lines, start=1):
                if value in line:
                    return index
        return 1

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
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        index_path = self._index_path(repo_path)
        matches: list[dict[str, Any]] = []
        repo_score = self._repo_match_score(entry, tokens)
        trace_stage_bonus = 90 if trace_stage == "two_hop" or trace_stage == "query_decomposition" or trace_stage.startswith(TOOL_LOOP_TRACE_PREFIX) or trace_stage.startswith("agent_trace") or trace_stage.startswith("agent_plan") or trace_stage == QUALITY_GATE_TRACE_STAGE else 0
        normalized_focus_terms = [term.lower() for term in (focus_terms or []) if term]
        query_terms = list(dict.fromkeys([*tokens, *normalized_focus_terms]))
        large_index = self._is_large_index_file(index_path)
        structure_term_limit = 8 if trace_stage == "direct" or not large_index else 2
        structure_query_terms = self._structure_lookup_query_terms(
            tokens,
            normalized_focus_terms,
            large_index=large_index,
            limit=structure_term_limit,
        )
        intent = self._question_retrieval_features(question, request_cache=request_cache).get("intent") or {}
        simple_intent = (
            any((intent or {}).get(key) for key in ("rule_logic", "api", "config"))
            and not any(
                (intent or {}).get(key)
                for key in (
                    "data_source",
                    "module_dependency",
                    "static_qa",
                    "impact_analysis",
                    "test_coverage",
                    "operational_boundary",
                )
            )
        )
        file_hits: dict[str, dict[str, Any]] = {}
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            index_rows = self._targeted_index_rows(
                connection,
                index_path,
                tokens=tokens,
                focus_terms=normalized_focus_terms,
                intent=intent,
                request_cache=request_cache,
                structure_term_limit=structure_term_limit,
            )
            file_rows = index_rows["files"]
            file_rows_by_path = index_rows["files_by_path"]
            line_rows = index_rows["lines"]
            file_symbols_by_path = index_rows.get("file_symbols_by_path") or {}
            line_symbols_by_key = index_rows.get("line_symbols_by_key") or {}
            for file_row in file_rows:
                path_text = str(file_row["lower_path"] or "")
                file_symbols = file_symbols_by_path.get(str(file_row["path"]))
                if file_symbols is None:
                    file_symbols = set(json.loads(file_row["symbols"] or "[]"))
                path_score = sum(10 for token in tokens if token in path_text)
                if intent.get("config") and path_text.endswith((".properties", ".yaml", ".yml", ".conf", ".toml")):
                    path_score += 70
                if intent.get("module_dependency") and (
                    path_text.endswith(("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "package.json"))
                    or ".gradle" in path_text
                ):
                    path_score += 76
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
            for term in structure_query_terms:
                if len(term) < 3:
                    continue
                for row in self._cached_structure_like_rows(
                    connection,
                    index_path,
                    table="definitions",
                    lower_column="lower_name",
                    term=term,
                    limit=40,
                    request_cache=request_cache,
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
                for row in self._cached_structure_like_rows(
                    connection,
                    index_path,
                    table="references_index",
                    lower_column="lower_target",
                    term=term,
                    limit=60,
                    request_cache=request_cache,
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
                    if intent.get("config") and str(row["kind"]) in {"config_value", "http_endpoint", "downstream_api"}:
                        boost += 34
                    if intent.get("module_dependency") and str(row["kind"]) == "module_dependency":
                        boost += 36
                    score = boost + repo_score + trace_stage_bonus
                    if score > hit.get("best_score", 0):
                        hit["best_line"] = int(row["line_no"])
                        hit["best_score"] = score
                    hit["structure_hits"].append(f"{row['kind']} reference {row['target']}")
            for row in self._fts_search_rows(
                connection,
                tokens,
                normalized_focus_terms,
                index_path=index_path,
                request_cache=request_cache,
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
                score = max(12, int(80 - min(float(row["rank"]), 60))) + repo_score + trace_stage_bonus
                if score > hit.get("best_score", 0):
                    hit["best_line"] = int(row["line_no"])
                    hit["best_score"] = score
                hit["structure_hits"].append("bm25 content match")
            for row in line_rows:
                lower_text = str(row["lower_text"] or "")
                file_path = str(row["file_path"])
                file_hit = file_hits.get(file_path)
                if file_hit is None:
                    file_row = file_rows_by_path.get(file_path)
                    if file_row is None:
                        continue
                    file_symbols = file_symbols_by_path.get(file_path)
                    if file_symbols is None:
                        file_symbols = set(json.loads(file_row["symbols"] or "[]"))
                    file_hit = {
                        "path_text": str(file_row["lower_path"] or ""),
                        "file_symbols": file_symbols,
                        "path_score": 0,
                        "symbol_score": 0,
                        "best_line": 1,
                        "best_score": 0,
                        "structure_hits": [],
                    }
                line_no = int(row["line_no"])
                line_symbols = line_symbols_by_key.get((file_path, line_no))
                if line_symbols is None:
                    line_symbols = set(json.loads(row["symbols"] or "[]"))
                score = sum(3 + min(lower_text.count(token), 3) for token in tokens if token in lower_text)
                score += sum(12 for token in tokens if token in line_symbols)
                score += sum(16 for term in normalized_focus_terms if term in line_symbols)
                score += sum(45 for term in normalized_focus_terms if term in lower_text)
                if int(row["is_declaration"] or 0):
                    score += sum(8 for token in tokens if token in lower_text or token in line_symbols)
                    score += sum(10 for term in normalized_focus_terms if term in lower_text or term in line_symbols)
                if int(row["has_pathish"] or 0):
                    score += sum(6 for token in tokens if token in lower_text)
                if intent.get("config") and file_path.lower().endswith((".properties", ".yaml", ".yml", ".conf", ".toml")):
                    score += 45
                if intent.get("module_dependency") and file_path.lower().endswith(("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "package.json")):
                    score += 48
                if score:
                    score += (
                        file_hit["path_score"]
                        + file_hit["symbol_score"]
                        + repo_score
                        + self._keyword_proximity_bonus(lower_text, tokens)
                        + self._hybrid_query_bonus(question, lower_text, line_symbols, intent=intent)
                        + trace_stage_bonus
                    )
                    if score > file_hit.get("best_score", 0):
                        file_hit.update(
                            {
                                "best_line": line_no,
                                "best_score": score,
                            }
                        )
                    file_hits[file_path] = file_hit
            matches.extend(
                self._semantic_chunk_matches(
                    connection,
                    entry=entry,
                    tokens=tokens,
                    question=question,
                    focus_terms=normalized_focus_terms,
                    trace_stage=trace_stage,
                    repo_score=repo_score,
                    trace_stage_bonus=trace_stage_bonus,
                    rows=index_rows.get("semantic_chunks"),
                    intent=intent,
                )
            )
            if simple_intent and trace_stage == "direct" and len(file_hits) > 60:
                file_hits = dict(
                    sorted(
                        file_hits.items(),
                        key=lambda item: int(item[1].get("best_score") or 0),
                        reverse=True,
                    )[:60]
                )
                self._increment_retrieval_stat(request_cache, "simple_file_hit_prunes")
            for relative_path, hit in file_hits.items():
                if not hit.get("best_score"):
                    continue
                lines = self._cached_file_lines(
                    connection,
                    index_path,
                    str(relative_path),
                    request_cache=request_cache,
                )
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

    def _cached_index_rows(
        self,
        connection: sqlite3.Connection,
        index_path: Path,
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        cache_key = self._index_fingerprint(index_path)
        if request_cache is not None:
            rows_cache = request_cache.setdefault("index_rows", {})
            cached = rows_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "index_rows_hits")
                return cached
        self._increment_retrieval_stat(request_cache, "index_rows_misses")
        file_rows = connection.execute("select * from files").fetchall()
        line_rows = connection.execute(
            "select * from lines order by file_path, line_no limit ?",
            (MAX_CACHED_INDEX_LINES,),
        ).fetchall()
        files_by_path = {str(row["path"]): row for row in file_rows}
        file_symbols_by_path = {
            str(row["path"]): set(json.loads(row["symbols"] or "[]"))
            for row in file_rows
        }
        lines_by_path: dict[str, list[tuple[int, str]]] = {}
        line_symbols_by_key: dict[tuple[str, int], set[str]] = {}
        for row in line_rows:
            file_path = str(row["file_path"])
            line_no = int(row["line_no"])
            lines_by_path.setdefault(file_path, []).append((line_no, str(row["line_text"])))
            line_symbols_by_key[(file_path, line_no)] = set(json.loads(row["symbols"] or "[]"))
        normalized_lines_by_path = {
            file_path: [line_text for _, line_text in sorted(rows, key=lambda item: item[0])]
            for file_path, rows in lines_by_path.items()
        }
        semantic_rows: list[sqlite3.Row] | None = None
        semantic_chunks: list[dict[str, Any]] | None = None
        if self.semantic_index_enabled:
            try:
                semantic_rows = connection.execute(
                    "select * from semantic_chunks order by file_path, start_line limit ?",
                    (MAX_CACHED_SEMANTIC_CHUNKS,),
                ).fetchall()
                semantic_chunks = [
                    {
                        "chunk_id": str(row["chunk_id"] if "chunk_id" in row.keys() else f"{row['file_path']}:{row['start_line']}"),
                        "file_path": str(row["file_path"] or ""),
                        "start_line": int(row["start_line"] or 1),
                        "end_line": int(row["end_line"] or row["start_line"] or 1),
                        "chunk_text": str(row["chunk_text"] or ""),
                        "lower_text": str(row["lower_text"] or ""),
                        "tokens_set": set(json.loads(row["tokens"] or "[]")),
                        "symbols_set": set(json.loads(row["symbols"] or "[]")),
                        "embedding_values": self._parse_embedding(row["embedding"] if "embedding" in row.keys() else ""),
                    }
                    for row in semantic_rows
                ]
            except sqlite3.Error:
                semantic_rows = []
                semantic_chunks = []
        payload = {
            "files": file_rows,
            "files_by_path": files_by_path,
            "file_symbols_by_path": file_symbols_by_path,
            "lines": line_rows,
            "line_symbols_by_key": line_symbols_by_key,
            "lines_by_path": normalized_lines_by_path,
            "semantic_chunks": semantic_chunks if semantic_chunks is not None else semantic_rows,
            "bounded": {
                "max_lines": MAX_CACHED_INDEX_LINES,
                "max_semantic_chunks": MAX_CACHED_SEMANTIC_CHUNKS,
            },
        }
        if request_cache is not None:
            rows_cache[cache_key] = payload
        return payload

    @staticmethod
    def _is_large_index_file(index_path: Path) -> bool:
        try:
            return index_path.stat().st_size >= 100_000_000
        except OSError:
            return False

    @staticmethod
    def _structure_lookup_query_terms(
        tokens: list[str],
        focus_terms: list[str] | None,
        *,
        large_index: bool,
        limit: int,
    ) -> list[str]:
        normalized_focus_terms = [str(term or "").strip().lower() for term in (focus_terms or []) if str(term or "").strip()]
        focus_term_set = set(normalized_focus_terms)
        ordered_terms = list(dict.fromkeys([*normalized_focus_terms, *(str(token or "").strip().lower() for token in tokens)]))
        terms: list[str] = []
        plain_terms_kept = 0
        for term in ordered_terms:
            normalized = str(term or "").strip().lower()
            if not normalized or normalized in terms:
                continue
            if len(normalized) < 3 or normalized in STOPWORDS or normalized in LOW_VALUE_CALL_SYMBOLS:
                continue
            if not large_index:
                terms.append(normalized)
            else:
                has_separator = any(separator in normalized for separator in ("_", ".", "/", "-"))
                is_focus_term = normalized in focus_term_set
                meaningful_plain_term = 4 <= len(normalized) <= 18 and normalized not in LOW_VALUE_FOCUS_TERMS
                if has_separator or is_focus_term:
                    terms.append(normalized)
                elif meaningful_plain_term and plain_terms_kept < 4:
                    terms.append(normalized)
                    plain_terms_kept += 1
            if len(terms) >= max(1, int(limit or 16)):
                break
        return terms

    def _targeted_index_rows(
        self,
        connection: sqlite3.Connection,
        index_path: Path,
        *,
        tokens: list[str],
        focus_terms: list[str],
        intent: dict[str, Any],
        request_cache: dict[str, Any] | None = None,
        structure_term_limit: int = 16,
    ) -> dict[str, Any]:
        try:
            indexed_file_count = int(connection.execute("select count(*) from files").fetchone()[0] or 0)
        except sqlite3.Error:
            indexed_file_count = 0
        large_index = indexed_file_count >= 2000 or self._is_large_index_file(index_path)
        focus_term_set = {str(term).lower() for term in focus_terms}
        query_terms = []
        for term in list(dict.fromkeys([*(str(token).lower() for token in tokens), *focus_term_set])):
            if len(term) < 3 or term in STOPWORDS:
                continue
            if large_index and term in LOW_VALUE_FOCUS_TERMS:
                continue
            if large_index and "_" not in term and "/" not in term and "." not in term:
                if len(term) > 28:
                    continue
                if len(term) > 14 and term not in focus_term_set:
                    continue
            query_terms.append(term)
            if len(query_terms) >= 24:
                break
        structure_query_terms = self._structure_lookup_query_terms(
            query_terms,
            focus_terms,
            large_index=large_index,
            limit=structure_term_limit,
        )
        simple_intent = (
            any((intent or {}).get(key) for key in ("rule_logic", "api", "config"))
            and not any(
                (intent or {}).get(key)
                for key in (
                    "data_source",
                    "module_dependency",
                    "message_flow",
                    "static_qa",
                    "impact_analysis",
                    "test_coverage",
                    "operational_boundary",
                )
            )
        )
        max_target_files = 48 if simple_intent else MAX_TARGETED_INDEX_FILES
        max_target_lines = 160 if simple_intent else MAX_TARGETED_INDEX_LINES
        max_target_semantic_chunks = 64 if simple_intent else MAX_TARGETED_SEMANTIC_CHUNKS
        intent_key = ",".join(sorted(key for key, enabled in (intent or {}).items() if enabled))
        cache_key = f"{self._index_fingerprint(index_path)}:targeted:{'|'.join(query_terms)}:{intent_key}"
        if request_cache is not None:
            rows_cache = request_cache.setdefault("targeted_index_rows", {})
            cached = rows_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "targeted_index_rows_hits")
                return cached
            self._increment_retrieval_stat(request_cache, "targeted_index_rows_misses")
        file_rows_by_path: dict[str, sqlite3.Row] = {}
        line_rows_by_key: dict[tuple[str, int], sqlite3.Row] = {}
        semantic_rows_by_id: dict[str, sqlite3.Row] = {}

        def add_file_rows(rows: list[sqlite3.Row]) -> None:
            for row in rows:
                if len(file_rows_by_path) >= max_target_files:
                    break
                file_rows_by_path.setdefault(str(row["path"]), row)

        def add_line_rows(rows: list[sqlite3.Row]) -> None:
            for row in rows:
                if len(line_rows_by_key) >= max_target_lines:
                    break
                line_rows_by_key.setdefault((str(row["file_path"]), int(row["line_no"])), row)

        def placeholders(values: list[str]) -> str:
            return ",".join("?" for _ in values)

        file_fts_rows = self._file_fts_search_rows(
            connection,
            tokens,
            focus_terms,
            index_path=index_path,
            request_cache=request_cache,
        )
        for row in file_fts_rows:
            file_path = str(row.get("path") if isinstance(row, dict) else row["path"])
            try:
                file_row = connection.execute("select * from files where path = ?", (file_path,)).fetchone()
            except sqlite3.Error:
                file_row = None
            if file_row is not None:
                add_file_rows([file_row])
        if not file_rows_by_path and query_terms:
            token_lookup_supported = True
            try:
                add_file_rows(
                    connection.execute(
                        f"""
                        select files.*
                        from file_tokens
                        join files on files.path = file_tokens.file_path
                        where file_tokens.token in ({placeholders(query_terms)})
                        group by files.path
                        order by count(*) desc, files.path
                        limit ?
                        """,
                        (*query_terms, max_target_files),
                    ).fetchall()
                )
            except sqlite3.Error:
                token_lookup_supported = False
            if not file_rows_by_path and not token_lookup_supported:
                for term in query_terms[:12]:
                    try:
                        add_file_rows(
                            connection.execute(
                                "select * from files where lower_path like ? order by path limit ?",
                                (f"%{term}%", max_target_files),
                            ).fetchall()
                        )
                    except sqlite3.Error:
                        continue
        for row in self._fts_search_rows(
            connection,
            tokens,
            focus_terms,
            index_path=index_path,
            request_cache=request_cache,
        ):
            file_path = str(row.get("file_path") if isinstance(row, dict) else row["file_path"])
            line_no = int(row.get("line_no") if isinstance(row, dict) else row["line_no"])
            try:
                file_row = connection.execute("select * from files where path = ?", (file_path,)).fetchone()
                if file_row is not None:
                    add_file_rows([file_row])
                line_row = connection.execute("select * from lines where file_path = ? and line_no = ?", (file_path, line_no)).fetchone()
                if line_row is not None:
                    add_line_rows([line_row])
            except sqlite3.Error:
                continue
        if not line_rows_by_key and query_terms:
            token_lookup_supported = True
            try:
                add_line_rows(
                    connection.execute(
                        f"""
                        select lines.*
                        from line_tokens
                        join lines on lines.file_path = line_tokens.file_path and lines.line_no = line_tokens.line_no
                        where line_tokens.token in ({placeholders(query_terms[:12])})
                        group by lines.file_path, lines.line_no
                        order by count(*) desc, lines.file_path, lines.line_no
                        limit ?
                        """,
                        (*query_terms[:12], max_target_lines),
                    ).fetchall()
                )
            except sqlite3.Error:
                token_lookup_supported = False
            if not line_rows_by_key and not token_lookup_supported:
                for term in query_terms[:12]:
                    try:
                        add_line_rows(
                            connection.execute(
                                "select * from lines where lower_text like ? order by file_path, line_no limit ?",
                                (f"%{term}%", max_target_lines),
                            ).fetchall()
                        )
                    except sqlite3.Error:
                        continue
        line_file_paths = list(dict.fromkeys(file_path for file_path, _line_no in line_rows_by_key))
        for file_path in line_file_paths:
            if file_path in file_rows_by_path:
                continue
            try:
                file_row = connection.execute("select * from files where path = ?", (file_path,)).fetchone()
            except sqlite3.Error:
                file_row = None
            if file_row is not None:
                add_file_rows([file_row])
        intent_file_patterns: list[str] = []
        if intent.get("config"):
            intent_file_patterns.extend(["%.properties", "%.yaml", "%.yml", "%.conf", "%.toml"])
        if intent.get("module_dependency"):
            intent_file_patterns.extend(["%pom.xml", "%build.gradle", "%build.gradle.kts", "%settings.gradle", "%settings.gradle.kts", "%package.json"])
        for pattern in intent_file_patterns:
            try:
                add_file_rows(connection.execute("select * from files where lower_path like ? order by path limit 40", (pattern,)).fetchall())
            except sqlite3.Error:
                continue
        for term in structure_query_terms:
            for table, lower_column in (("definitions", "lower_name"), ("references_index", "lower_target")):
                for row in self._cached_structure_like_rows(
                    connection,
                    index_path,
                    table=table,
                    lower_column=lower_column,
                    term=term,
                    limit=40,
                    request_cache=request_cache,
                ):
                    file_path = str(row.get("file_path") or "")
                    line_no = int(row.get("line_no") or 1)
                    try:
                        file_row = connection.execute("select * from files where path = ?", (file_path,)).fetchone()
                        if file_row is not None:
                            add_file_rows([file_row])
                        line_row = connection.execute("select * from lines where file_path = ? and line_no = ?", (file_path, line_no)).fetchone()
                        if line_row is not None:
                            add_line_rows([line_row])
                    except sqlite3.Error:
                        continue
        if file_rows_by_path and len(line_rows_by_key) < max_target_lines:
            paths = list(file_rows_by_path)[: (10 if simple_intent else 80)]
            line_fill_limit = min(max_target_lines - len(line_rows_by_key), 80 if simple_intent else max_target_lines)
            try:
                add_line_rows(
                    connection.execute(
                        f"select * from lines where file_path in ({placeholders(paths)}) order by file_path, line_no limit ?",
                        (*paths, line_fill_limit),
                    ).fetchall()
                )
            except sqlite3.Error:
                pass
        if not file_rows_by_path:
            try:
                add_file_rows(connection.execute("select * from files order by path limit ?", (min(max_target_files, 80),)).fetchall())
            except sqlite3.Error:
                pass
        if self.semantic_index_enabled:
            for row in self._semantic_fts_search_rows(
                connection,
                tokens,
                focus_terms,
                index_path=index_path,
                request_cache=request_cache,
            ):
                chunk_id = str(row.get("chunk_id") if isinstance(row, dict) else row["chunk_id"])
                try:
                    chunk_row = connection.execute("select * from semantic_chunks where chunk_id = ?", (chunk_id,)).fetchone()
                except sqlite3.Error:
                    chunk_row = None
                if chunk_row is not None:
                    semantic_rows_by_id.setdefault(chunk_id, chunk_row)
                if len(semantic_rows_by_id) >= max_target_semantic_chunks:
                    break
            if not semantic_rows_by_id and query_terms:
                token_lookup_supported = True
                try:
                    rows = connection.execute(
                        f"""
                        select semantic_chunks.*
                        from semantic_chunk_tokens
                        join semantic_chunks on semantic_chunks.chunk_id = semantic_chunk_tokens.chunk_id
                        where semantic_chunk_tokens.token in ({placeholders(query_terms[:12])})
                        group by semantic_chunks.chunk_id
                        order by count(*) desc, semantic_chunks.file_path, semantic_chunks.start_line
                        limit ?
                        """,
                        (*query_terms[:12], max_target_semantic_chunks),
                    ).fetchall()
                except sqlite3.Error:
                    token_lookup_supported = False
                    rows = []
                if not rows and not token_lookup_supported:
                    for term in query_terms[:12]:
                        try:
                            rows.extend(
                                connection.execute(
                                    "select * from semantic_chunks where lower_text like ? order by file_path, start_line limit ?",
                                    (f"%{term}%", max_target_semantic_chunks),
                                ).fetchall()
                            )
                        except sqlite3.Error:
                            continue
                for row in rows:
                    if len(semantic_rows_by_id) >= max_target_semantic_chunks:
                        break
                    chunk_id = str(row["chunk_id"] if "chunk_id" in row.keys() else f"{row['file_path']}:{row['start_line']}")
                    semantic_rows_by_id.setdefault(chunk_id, row)
            if file_rows_by_path and len(semantic_rows_by_id) < max_target_semantic_chunks:
                paths = list(file_rows_by_path)[: (10 if simple_intent else 80)]
                semantic_fill_limit = min(max_target_semantic_chunks - len(semantic_rows_by_id), 32 if simple_intent else max_target_semantic_chunks)
                try:
                    rows = connection.execute(
                        f"select * from semantic_chunks where file_path in ({placeholders(paths)}) order by file_path, start_line limit ?",
                        (*paths, semantic_fill_limit),
                    ).fetchall()
                except sqlite3.Error:
                    rows = []
                for row in rows:
                    chunk_id = str(row["chunk_id"] if "chunk_id" in row.keys() else f"{row['file_path']}:{row['start_line']}")
                    semantic_rows_by_id.setdefault(chunk_id, row)
        file_rows = list(file_rows_by_path.values())
        line_rows = sorted(line_rows_by_key.values(), key=lambda row: (str(row["file_path"]), int(row["line_no"])))
        files_by_path = {str(row["path"]): row for row in file_rows}
        file_symbols_by_path = {str(row["path"]): set(json.loads(row["symbols"] or "[]")) for row in file_rows}
        line_symbols_by_key: dict[tuple[str, int], set[str]] = {}
        lines_by_path: dict[str, list[tuple[int, str]]] = {}
        for row in line_rows:
            file_path = str(row["file_path"])
            line_no = int(row["line_no"])
            line_symbols_by_key[(file_path, line_no)] = set(json.loads(row["symbols"] or "[]"))
            lines_by_path.setdefault(file_path, []).append((line_no, str(row["line_text"])))
        semantic_chunks = [
            {
                "chunk_id": str(row["chunk_id"] if "chunk_id" in row.keys() else f"{row['file_path']}:{row['start_line']}"),
                "file_path": str(row["file_path"] or ""),
                "start_line": int(row["start_line"] or 1),
                "end_line": int(row["end_line"] or row["start_line"] or 1),
                "chunk_text": str(row["chunk_text"] or ""),
                "lower_text": str(row["lower_text"] or ""),
                "tokens_set": set(json.loads(row["tokens"] or "[]")),
                "symbols_set": set(json.loads(row["symbols"] or "[]")),
                "embedding_values": self._parse_embedding(row["embedding"] if "embedding" in row.keys() else ""),
            }
            for row in semantic_rows_by_id.values()
        ] if self.semantic_index_enabled else []
        payload = {
            "files": file_rows,
            "files_by_path": files_by_path,
            "file_symbols_by_path": file_symbols_by_path,
            "lines": line_rows,
            "line_symbols_by_key": line_symbols_by_key,
            "lines_by_path": {
                file_path: [line_text for _, line_text in sorted(rows, key=lambda item: item[0])]
                for file_path, rows in lines_by_path.items()
            },
            "semantic_chunks": semantic_chunks,
            "bounded": {
                "max_files": max_target_files,
                "max_lines": max_target_lines,
                "max_semantic_chunks": max_target_semantic_chunks,
                "targeted": True,
            },
        }
        if request_cache is not None:
            rows_cache[cache_key] = payload
        return payload

    def _cached_file_lines(
        self,
        connection: sqlite3.Connection,
        index_path: Path,
        file_path: str,
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> list[str]:
        normalized_path = str(file_path or "")
        if not normalized_path:
            return []
        if request_cache is not None:
            cache_key = f"{self._index_fingerprint(index_path)}:{normalized_path}"
            file_cache = request_cache.setdefault("file_lines", {})
            cached = file_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "file_lines_hits")
                return list(cached)
            self._increment_retrieval_stat(request_cache, "file_lines_misses")
        rows = connection.execute(
            "select line_text from lines where file_path = ? order by line_no",
            (normalized_path,),
        ).fetchall()
        lines = [str(row["line_text"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows]
        if request_cache is not None:
            request_cache.setdefault("file_lines", {})[cache_key] = list(lines)
        return lines

    def _file_fts_search_rows(
        self,
        connection: sqlite3.Connection,
        tokens: list[str],
        focus_terms: list[str],
        *,
        index_path: Path | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
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
        cache_key = ""
        if request_cache is not None and index_path is not None:
            cache_key = f"{self._index_fingerprint(index_path)}:{query}"
            file_fts_cache = request_cache.setdefault("file_fts", {})
            cached = file_fts_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "file_fts_hits")
                return cached
            self._increment_retrieval_stat(request_cache, "file_fts_misses")
        try:
            rows = list(
                connection.execute(
                    """
                    select path, bm25(files_fts) as rank
                    from files_fts
                    where files_fts match ?
                    order by rank
                    limit 80
                    """,
                    (query,),
                )
            )
        except sqlite3.Error:
            return []
        payload = [dict(row) for row in rows]
        if request_cache is not None and cache_key:
            request_cache.setdefault("file_fts", {})[cache_key] = payload
        return payload

    def _fts_search_rows(
        self,
        connection: sqlite3.Connection,
        tokens: list[str],
        focus_terms: list[str],
        *,
        index_path: Path | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> list[sqlite3.Row | dict[str, Any]]:
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
        cache_key = ""
        if request_cache is not None and index_path is not None:
            cache_key = f"{self._index_fingerprint(index_path)}:{query}"
            fts_cache = request_cache.setdefault("fts", {})
            cached = fts_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "fts_hits")
                return cached
            self._increment_retrieval_stat(request_cache, "fts_misses")
        try:
            rows = list(
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
        payload = [dict(row) for row in rows]
        if request_cache is not None and cache_key:
            request_cache.setdefault("fts", {})[cache_key] = payload
        return payload

    def _semantic_fts_search_rows(
        self,
        connection: sqlite3.Connection,
        tokens: list[str],
        focus_terms: list[str],
        *,
        index_path: Path | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
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
        cache_key = ""
        if request_cache is not None and index_path is not None:
            cache_key = f"{self._index_fingerprint(index_path)}:{query}"
            semantic_fts_cache = request_cache.setdefault("semantic_fts", {})
            cached = semantic_fts_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "semantic_fts_hits")
                return cached
            self._increment_retrieval_stat(request_cache, "semantic_fts_misses")
        try:
            rows = list(
                connection.execute(
                    """
                    select chunk_id, file_path, bm25(semantic_chunks_fts) as rank
                    from semantic_chunks_fts
                    where semantic_chunks_fts match ?
                    order by rank
                    limit 120
                    """,
                    (query,),
                )
            )
        except sqlite3.Error:
            return []
        payload = [dict(row) for row in rows]
        if request_cache is not None and cache_key:
            request_cache.setdefault("semantic_fts", {})[cache_key] = payload
        return payload

    def _cached_structure_like_rows(
        self,
        connection: sqlite3.Connection,
        index_path: Path,
        *,
        table: str,
        lower_column: str,
        term: str,
        limit: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        safe_table_columns = {
            "definitions": {"lower_name"},
            "references_index": {"lower_target"},
        }
        if table not in safe_table_columns or lower_column not in safe_table_columns[table]:
            return []
        normalized = str(term or "").strip().lower()
        if len(normalized) < 3:
            return []
        cache_key = f"{self._index_fingerprint(index_path)}:{table}:{lower_column}:{normalized}:{int(limit or 0)}"
        if request_cache is not None:
            structure_cache = request_cache.setdefault("structure_like", {})
            cached = structure_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "structure_like_hits")
                return cached
            self._increment_retrieval_stat(request_cache, "structure_like_misses")
        try:
            rows = connection.execute(
                f"select * from {table} where {lower_column} = ? limit ?",
                (normalized, int(limit or 40)),
            ).fetchall()
            if len(rows) < int(limit or 40):
                seen = {(str(row["file_path"]), int(row["line_no"]), str(row[lower_column])) for row in rows}
                prefix_rows = connection.execute(
                    f"select * from {table} where {lower_column} like ? limit ?",
                    (f"{normalized}%", max(0, int(limit or 40) - len(rows))),
                ).fetchall()
                for row in prefix_rows:
                    key = (str(row["file_path"]), int(row["line_no"]), str(row[lower_column]))
                    if key not in seen:
                        rows.append(row)
                        seen.add(key)
            large_index = self._is_large_index_file(index_path)
            allow_contains_lookup = (
                not large_index
                or (
                    len(normalized) >= 12
                    and any(separator in normalized for separator in ("_", ".", "/", "-"))
                )
            )
            if allow_contains_lookup and len(rows) < max(8, int(limit or 40) // 3):
                seen = {(str(row["file_path"]), int(row["line_no"]), str(row[lower_column])) for row in rows}
                contains_rows = connection.execute(
                    f"select * from {table} where {lower_column} like ? limit ?",
                    (f"%{normalized}%", max(0, int(limit or 40) - len(rows))),
                ).fetchall()
                for row in contains_rows:
                    key = (str(row["file_path"]), int(row["line_no"]), str(row[lower_column]))
                    if key not in seen:
                        rows.append(row)
                        seen.add(key)
        except sqlite3.Error:
            rows = []
        payload = [dict(row) for row in rows]
        if request_cache is not None:
            request_cache.setdefault("structure_like", {})[cache_key] = payload
        return payload

    def _semantic_chunk_matches(
        self,
        connection: sqlite3.Connection,
        *,
        entry: RepositoryEntry,
        tokens: list[str],
        question: str,
        focus_terms: list[str],
        trace_stage: str,
        repo_score: int,
        trace_stage_bonus: int,
        rows: list[sqlite3.Row] | None = None,
        intent: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if not self.semantic_index_enabled:
            return []
        intent = intent or self._question_intent(question)
        query_terms = self._semantic_query_terms(question, tokens, focus_terms, intent=intent)
        if not query_terms:
            return []
        matches: list[dict[str, Any]] = []
        query_set = set(query_terms)
        query_embedding = self._query_embedding(question) if self.embedding_provider.ready() and self.embedding_provider.name != "local_token_hybrid" else []
        if rows is None:
            try:
                rows = connection.execute("select * from semantic_chunks").fetchall()
            except sqlite3.Error:
                return []
        for row in rows:
            if isinstance(row, dict):
                chunk_tokens = set(row.get("tokens_set") or set())
                chunk_symbols = set(row.get("symbols_set") or set())
                lower_text = str(row.get("lower_text") or "")
                file_path = str(row.get("file_path") or "")
                chunk_embedding = list(row.get("embedding_values") or [])
                start_line = int(row.get("start_line") or 1)
                end_line = int(row.get("end_line") or start_line)
                chunk_text = str(row.get("chunk_text") or "")
            else:
                chunk_tokens = set(json.loads(row["tokens"] or "[]"))
                chunk_symbols = set(json.loads(row["symbols"] or "[]"))
                lower_text = str(row["lower_text"] or "")
                file_path = str(row["file_path"] or "")
                chunk_embedding = self._parse_embedding(row["embedding"])
                start_line = int(row["start_line"])
                end_line = int(row["end_line"])
                chunk_text = str(row["chunk_text"] or "")
            overlap = query_set & (chunk_tokens | chunk_symbols)
            phrase_hits = [term for term in query_terms if len(term) >= 5 and term in lower_text]
            embedding_score = self._embedding_similarity(query_embedding, chunk_embedding) if query_embedding and chunk_embedding else 0.0
            if not overlap and not phrase_hits and embedding_score < 0.2:
                continue
            score = 35 + repo_score + trace_stage_bonus
            score += min(len(overlap), 8) * 9
            score += min(len(phrase_hits), 6) * 12
            score += int(max(0.0, embedding_score) * 80)
            if intent.get("data_source") and any(term in lower_text for term in CONCRETE_SOURCE_HINTS):
                score += 26
            if intent.get("api") and any(term in lower_text for term in API_HINTS):
                score += 20
            if intent.get("config") and any(term in lower_text for term in CONFIG_HINTS):
                score += 20
            if intent.get("module_dependency") and any(term in lower_text for term in MODULE_DEPENDENCY_HINTS):
                score += 22
            if (intent.get("error") or intent.get("rule_logic")) and any(term in lower_text for term in ERROR_HINTS + RULE_HINTS):
                score += 18
            matched_terms = list(dict.fromkeys([*sorted(overlap), *phrase_hits]))[:6]
            reason = f"semantic chunk matched: {', '.join(matched_terms)}" if matched_terms else f"semantic embedding matched: {embedding_score:.2f}"
            if trace_stage == "dependency":
                reason = f"dependency trace; {reason}"
            elif trace_stage == "two_hop":
                reason = f"two-hop trace; {reason}"
            elif trace_stage == "query_decomposition":
                reason = f"query decomposition; {reason}"
            matches.append(
                {
                    "repo": entry.display_name,
                    "path": file_path,
                    "line_start": start_line,
                    "line_end": end_line,
                    "score": score,
                    "snippet": chunk_text[:2400],
                    "reason": reason,
                    "trace_stage": trace_stage,
                    "retrieval": "semantic_chunk",
                }
            )
        matches.sort(key=lambda item: item["score"], reverse=True)
        return matches[:24]

    def _semantic_query_terms(
        self,
        question: str,
        tokens: list[str],
        focus_terms: list[str],
        *,
        intent: dict[str, Any] | None = None,
    ) -> list[str]:
        terms = [*tokens, *focus_terms, *self._semantic_tokens(question)]
        intent = intent or self._question_intent(question)
        if intent.get("data_source"):
            terms.extend(["repository", "mapper", "dao", "jdbc", "jdbctemplate", "select", "from", "client", "integration", "provider", "source"])
        if intent.get("api"):
            terms.extend(["controller", "requestmapping", "postmapping", "getmapping", "endpoint", "api", "client"])
        if intent.get("config"):
            terms.extend(["config", "configuration", "properties", "yaml", "yml", "value"])
        if intent.get("module_dependency"):
            terms.extend(["dependency", "dependencies", "maven", "gradle", "pom", "artifactid", "groupid", "implementation", "package.json", "npm"])
        if intent.get("error") or intent.get("rule_logic"):
            terms.extend(["validate", "validation", "condition", "exception", "rule", "approval", "permission"])
        deduped: list[str] = []
        for term in terms:
            lowered = str(term or "").strip().lower()
            if len(lowered) < 3 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
                continue
            if lowered not in deduped:
                deduped.append(lowered)
        return deduped[:64]

    def _query_embedding(self, question: str) -> list[float]:
        try:
            rows = self.embedding_provider.embed_texts([question[:6000]], task_type=VERTEX_EMBEDDING_QUERY_TASK)
        except ToolError:
            return []
        return rows[0] if rows else []

    @staticmethod
    def _extract_exact_lookup_terms(question: str) -> list[str]:
        terms: list[str] = []
        text = str(question or "")
        for match in EXACT_LOOKUP_TERM_PATTERN.finditer(text):
            term = match.group(0).strip("`'\".,;()[]{}<>").lower()
            if not term or term.startswith(("http://", "https://")):
                continue
            if len(term) < 8:
                continue
            if not any(separator in term for separator in (".", "/", ":")):
                continue
            if term not in terms:
                terms.append(term)
        for match in IDENTIFIER_PATTERN.finditer(text):
            term = match.group(0).strip("`'\".,;()[]{}<>").lower()
            if len(term) >= 8 and "_" in term and any(marker in term for marker in ("table", "dwd", "dim", "tmp", "ads", "ods", "snapshot")):
                if any(existing.endswith(f".{term}") for existing in terms):
                    continue
                if term not in terms:
                    terms.append(term)
                continue
            if len(term) < 24 or term.count("_") < 3:
                continue
            if any(existing.endswith(f".{term}") for existing in terms):
                continue
            if term not in terms:
                terms.append(term)
        return terms[:8]

    def _llm_cost_skip_for_local_no_hit(
        self,
        *,
        question: str,
        matches: list[dict[str, Any]],
        evidence_summary: dict[str, Any],
        evidence_pack: dict[str, Any],
        quality_gate: dict[str, Any],
    ) -> dict[str, Any] | None:
        symbols = self._simple_function_usage_symbols(question)
        if not symbols:
            return None
        exact_hit_count = sum(
            1
            for match in matches
            if self._match_contains_any_symbol(match, symbols)
        )
        if exact_hit_count and any(self._match_has_function_usage_evidence(match, symbols) for match in matches):
            return None

        display_symbols = ", ".join(f"`{symbol}`" for symbol in symbols[:3])
        if exact_hit_count:
            direct_answer = (
                f"I found references to {display_symbols}, but not in a function or method usage context in the current local index."
            )
            missing = [f"Function or method body usage evidence for {display_symbols}."]
            claims = [
                {
                    "text": f"Local retrieval found {exact_hit_count} exact symbol reference{'s' if exact_hit_count != 1 else ''}, but none looked like function/method usage.",
                    "citations": ["S1"] if matches else [],
                }
            ]
            confidence = "medium"
        else:
            direct_answer = f"I did not find exact local index references to {display_symbols}, so there is no retrieved evidence that it is used in a function."
            missing = [f"Any exact code reference to {display_symbols}."]
            claims = []
            confidence = "medium"

        closest = []
        for index, match in enumerate(matches[:4], start=1):
            if not self._match_contains_any_symbol(match, symbols):
                continue
            closest.append(
                {
                    "text": f"Closest reference: {self._evidence_label(match)}.",
                    "citations": [f"S{index}"],
                }
            )
        claims.extend(closest[:3])
        answer = direct_answer
        structured_answer = {
            "direct_answer": direct_answer,
            "claims": claims,
            "missing_evidence": missing,
            "confidence": confidence,
            "format": "deterministic_local_gate",
        }
        local_quality = dict(quality_gate or {})
        local_quality.update(
            {
                "status": "local_no_function_usage_hit",
                "confidence": confidence,
                "missing": list(dict.fromkeys([*(quality_gate or {}).get("missing", []), *missing]))[:8],
            }
        )
        return {
            "llm_answer": answer,
            "llm_budget_mode": "skipped",
            "llm_requested_budget_mode": "skipped",
            "llm_route": {"mode": "local_cost_gate", "reason": "simple_symbol_usage_no_function_hit"},
            "llm_provider": "local",
            "llm_model": "deterministic-no-hit-gate",
            "llm_cached": False,
            "llm_usage": {},
            "llm_thinking_budget": 0,
            "llm_latency_ms": 0,
            "llm_attempt_log": [],
            "llm_finish_reason": "llm_skipped_local_no_hit",
            "llm_cost_skip": {
                "skipped": True,
                "reason": "simple_symbol_usage_no_function_hit",
                "symbols": symbols,
                "exact_hit_count": exact_hit_count,
            },
            "answer_quality": local_quality,
            "answer_claim_check": {"status": "skipped", "issues": []},
            "answer_judge": {"status": "skipped", "reason": "local deterministic no-hit gate"},
            "structured_answer": structured_answer,
            "answer_contract": {
                "confidence": confidence,
                "status": "local_no_function_usage_hit",
                "missing_evidence": missing,
                "evidence_mode": "deterministic_local_retrieval",
            },
            "evidence_pack": {
                **(evidence_pack if isinstance(evidence_pack, dict) else {}),
                "local_cost_gate": {
                    "reason": "simple_symbol_usage_no_function_hit",
                    "symbols": symbols,
                    "exact_hit_count": exact_hit_count,
                },
            },
        }

    @staticmethod
    def _simple_function_usage_symbols(question: str) -> list[str]:
        text = str(question or "")
        lowered = f" {text.lower()} "
        has_usage_hint = any(hint in lowered for hint in USAGE_QUERY_HINTS)
        has_function_hint = any(hint in lowered for hint in FUNCTION_USAGE_QUERY_HINTS)
        if not (has_usage_hint and has_function_hint):
            return []
        if any(hint in lowered for hint in COMPLEX_REASONING_QUERY_HINTS):
            return []
        noise = {
            "any",
            "function",
            "functions",
            "method",
            "methods",
            "used",
            "usage",
            "referenced",
            "called",
            "check",
        }
        symbols: list[str] = []
        for raw in IDENTIFIER_PATTERN.findall(text):
            candidate = raw.strip("`'\".,;()[]{}<>")
            lowered_candidate = candidate.lower()
            if len(candidate) < 4 or lowered_candidate in STOPWORDS or lowered_candidate in LOW_VALUE_CALL_SYMBOLS or lowered_candidate in noise:
                continue
            if candidate not in symbols:
                symbols.append(candidate)
        return symbols[:4]

    @staticmethod
    def _match_contains_any_symbol(match: dict[str, Any], symbols: list[str]) -> bool:
        haystack = f"{match.get('path') or ''}\n{match.get('snippet') or ''}".lower()
        return any(symbol.lower() in haystack for symbol in symbols)

    @classmethod
    def _match_has_function_usage_evidence(cls, match: dict[str, Any], symbols: list[str]) -> bool:
        path = str(match.get("path") or "")
        suffix = Path(path).suffix.lower()
        if suffix in NON_FUNCTION_USAGE_SUFFIXES or suffix not in CODE_USAGE_SUFFIXES:
            return False
        lines = str(match.get("snippet") or "").splitlines()
        for index, line in enumerate(lines):
            line_lower = line.lower()
            if not any(symbol.lower() in line_lower for symbol in symbols):
                continue
            if cls._line_is_ui_or_config_literal(line):
                continue
            window = lines[max(0, index - 6) : min(len(lines), index + 4)]
            window_text = "\n".join(window)
            window_lower = window_text.lower()
            for symbol in symbols:
                lowered_symbol = symbol.lower()
                accessor = cls._accessor_suffix(symbol).lower()
                if re.search(rf"\b(?:get|set|is){re.escape(accessor)}\s*\(", window_lower):
                    return True
                if re.search(rf"(?<![A-Za-z0-9_]){re.escape(lowered_symbol)}\s*\(", line_lower):
                    return True
                if re.search(rf"\.\s*{re.escape(lowered_symbol)}\b", line_lower):
                    return True
            if cls._window_has_function_boundary(window) and cls._line_looks_like_executable_code(line):
                return True
        return False

    @staticmethod
    def _accessor_suffix(symbol: str) -> str:
        parts = [part for part in re.split(r"[_\W]+", str(symbol or "")) if part]
        if len(parts) > 1:
            return "".join(part[:1].upper() + part[1:] for part in parts)
        value = str(symbol or "")
        return value[:1].upper() + value[1:]

    @staticmethod
    def _line_is_ui_or_config_literal(line: str) -> bool:
        lowered = str(line or "").lower()
        return any(
            marker in lowered
            for marker in (
                "label",
                "placeholder",
                "tooltip",
                "title",
                "rules",
                "columns",
                "field:",
                "name:",
                "prop:",
                "key:",
                "value:",
            )
        )

    @staticmethod
    def _line_looks_like_executable_code(line: str) -> bool:
        stripped = str(line or "").strip()
        lowered = stripped.lower()
        return bool(
            re.search(r"\b(if|for|while|switch|case|return|throw|new)\b", lowered)
            or "=" in stripped
            or "." in stripped
            or "->" in stripped
            or "=>" in stripped
        )

    @staticmethod
    def _window_has_function_boundary(lines: list[str]) -> bool:
        for line in lines:
            if JAVA_METHOD_DEF_PATTERN.search(line) or PY_DEF_PATTERN.search(line) or JS_DEF_PATTERN.search(line):
                return True
            if re.search(r"\b(?:public|private|protected)\s+[A-Za-z0-9_<>, ?\[\]]+\s+[A-Za-z_][A-Za-z0-9_]*\s*\(", line):
                return True
            if re.search(r"\b(?:async\s+)?[A-Za-z_][A-Za-z0-9_]*\s*=\s*(?:async\s*)?\([^)]*\)\s*=>", line):
                return True
        return False

    @staticmethod
    def _exact_lookup_is_sufficient(terms: list[str], matches: list[dict[str, Any]]) -> bool:
        if not terms or not matches:
            return False
        covered_terms = {
            str((match.get("exact_lookup") or {}).get("term") or "").lower()
            for match in matches
            if (match.get("exact_lookup") or {}).get("term")
        }
        required_terms = {str(term).lower() for term in terms if term}
        return bool(required_terms) and required_terms.issubset(covered_terms)

    @classmethod
    def _exact_lookup_miss_should_stop(cls, terms: list[str]) -> bool:
        strong_terms = [term for term in terms if cls._is_strict_exact_lookup_term(term)]
        return bool(strong_terms) and len(strong_terms) == len([term for term in terms if str(term or "").strip()])

    @staticmethod
    def _is_strict_exact_lookup_term(term: str) -> bool:
        value = str(term or "").strip().lower()
        if not value:
            return False
        if "/" in value or value.endswith((".java", ".kt", ".py", ".xml", ".sql", ".yaml", ".yml", ".properties", ".ts", ".tsx", ".js", ".jsx")):
            return True
        if "_" not in value:
            return False
        table_markers = ("_tab", "_table", "dwd", "dim", "tmp", "ads", "ods", "snapshot", "process_info", "response_body")
        return any(marker in value for marker in table_markers)

    def _exact_table_path_lookup_repo(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        *,
        question: str,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        seen: set[tuple[str, int, str, str]] = set()
        index_path = self._index_path(repo_path)
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            if request_cache is not None:
                self._increment_retrieval_stat(request_cache, "exact_lookup_repos")
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                for term in terms[:8]:
                    normalized = str(term or "").strip().lower()
                    if not normalized:
                        continue
                    term_matches = 0
                    lookup_values = [normalized]
                    if "." in normalized:
                        suffix = normalized.rsplit(".", 1)[-1].strip()
                        if len(suffix) >= 8 and suffix.count("_") >= 2 and suffix not in lookup_values:
                            lookup_values.append(suffix)

                    def add_match(file_path: str, line_no: int, score: int, reason: str, source: str, lookup_value: str) -> None:
                        nonlocal term_matches
                        key = (str(file_path), int(line_no or 1), normalized, source)
                        if key in seen:
                            return
                        seen.add(key)
                        match = self._match_from_index_location(
                            entry,
                            connection,
                            str(file_path),
                            int(line_no or 1),
                            score=score,
                            reason=reason,
                            question=question,
                            trace_stage="exact_lookup",
                            retrieval="exact_table_path_lookup",
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                        if not match:
                            return
                        match["exact_lookup"] = {"term": normalized, "lookup_value": lookup_value, "source": source}
                        matches.append(match)
                        term_matches += 1

                    for lookup_value in lookup_values:
                        for row in connection.execute(
                            """
                            select * from references_index
                            where lower_target = ?
                            order by
                                case kind
                                    when 'sql_table' then 0
                                    when 'db_read' then 1
                                    when 'db_write' then 2
                                    when 'runtime_sql' then 3
                                    else 4
                                end,
                                file_path,
                                line_no
                            limit 80
                            """,
                            (lookup_value,),
                        ).fetchall():
                            add_match(
                                str(row["file_path"]),
                                int(row["line_no"] or 1),
                                282 if str(row["kind"]) == "sql_table" else 268,
                                f"exact table/path lookup: {row['kind']} {row['target']}",
                                "references_index",
                                lookup_value,
                            )
                        if "/" in lookup_value or lookup_value.endswith((".java", ".py", ".xml", ".sql", ".yaml", ".yml", ".properties", ".ts", ".tsx", ".js", ".jsx")):
                            for row in connection.execute(
                                """
                                select path from files
                                where lower_path = ? or lower_path like ?
                                order by case when lower_path = ? then 0 else 1 end, path
                                limit 40
                                """,
                                (lookup_value, f"%{lookup_value}%", lookup_value),
                            ).fetchall():
                                add_match(
                                    str(row["path"]),
                                    1,
                                    260,
                                    f"exact path lookup: {lookup_value}",
                                    "files",
                                    lookup_value,
                                )
                        try:
                            line_rows = connection.execute(
                                """
                                select lines.file_path, lines.line_no
                                from line_tokens
                                join lines on lines.file_path = line_tokens.file_path and lines.line_no = line_tokens.line_no
                                where line_tokens.token = ?
                                order by lines.file_path, lines.line_no
                                limit 100
                                """,
                                (lookup_value,),
                            ).fetchall()
                        except sqlite3.Error:
                            line_rows = []
                        for row in line_rows:
                            add_match(
                                str(row["file_path"]),
                                int(row["line_no"] or 1),
                                248,
                                f"exact line lookup: {lookup_value}",
                                "line_tokens",
                                lookup_value,
                            )
                    if term_matches == 0:
                        for lookup_value in lookup_values:
                            fts_query = f'"{lookup_value.replace(chr(34), chr(34) + chr(34))}"'
                            try:
                                fallback_rows = connection.execute(
                                    """
                                    select file_path, line_no from lines_fts
                                    where lines_fts match ?
                                    order by file_path, line_no
                                    limit 80
                                    """,
                                    (fts_query,),
                                ).fetchall()
                            except sqlite3.Error:
                                fallback_rows = []
                            for row in fallback_rows:
                                add_match(
                                    str(row["file_path"]),
                                    int(row["line_no"] or 1),
                                    236,
                                    f"exact indexed text lookup: {lookup_value}",
                                    "lines_fts",
                                    lookup_value,
                                )
                    if request_cache is not None:
                        stat_key = "exact_lookup_hits" if term_matches else "exact_lookup_misses"
                        self._increment_retrieval_stat(request_cache, stat_key)
        except (OSError, sqlite3.Error):
            return []
        matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        return matches[:160]

    @staticmethod
    def _parse_embedding(raw_value: Any) -> list[float]:
        if not raw_value:
            return []
        try:
            values = json.loads(str(raw_value))
        except json.JSONDecodeError:
            return []
        if not isinstance(values, list):
            return []
        parsed = []
        for value in values[:2048]:
            try:
                parsed.append(float(value))
            except (TypeError, ValueError):
                continue
        return parsed

    @staticmethod
    def _embedding_similarity(left: list[float], right: list[float]) -> float:
        if not left or not right:
            return 0.0
        size = min(len(left), len(right))
        if size <= 0:
            return 0.0
        dot = sum(left[index] * right[index] for index in range(size))
        left_norm = sum(left[index] * left[index] for index in range(size)) ** 0.5
        right_norm = sum(right[index] * right[index] for index in range(size)) ** 0.5
        if not left_norm or not right_norm:
            return 0.0
        return dot / (left_norm * right_norm)

    @staticmethod
    def _new_retrieval_request_cache() -> dict[str, Any]:
        return {
            "started_at": time.perf_counter(),
            "search": {},
            "ensured_indexes": set(),
            "index_rows": {},
            "targeted_index_rows": {},
            "structure_like": {},
            "file_fts": {},
            "fts": {},
            "semantic_fts": {},
            "trace_paths": {},
            "evidence": {},
            "quality": {},
            "rerank": {},
            "question_features": {},
            "stats": {
                "search_hits": 0,
                "search_misses": 0,
                "index_ensure_hits": 0,
                "index_ensure_misses": 0,
                "index_rows_hits": 0,
                "index_rows_misses": 0,
                "targeted_index_rows_hits": 0,
                "targeted_index_rows_misses": 0,
                "file_lines_hits": 0,
                "file_lines_misses": 0,
                "structure_like_hits": 0,
                "structure_like_misses": 0,
                "file_fts_hits": 0,
                "file_fts_misses": 0,
                "fts_hits": 0,
                "fts_misses": 0,
                "semantic_fts_hits": 0,
                "semantic_fts_misses": 0,
                "trace_paths_hits": 0,
                "trace_paths_misses": 0,
                "evidence_hits": 0,
                "evidence_misses": 0,
                "quality_hits": 0,
                "quality_misses": 0,
                "rerank_hits": 0,
                "rerank_misses": 0,
                "question_feature_hits": 0,
                "question_feature_misses": 0,
                "repository_scope_filters": 0,
                "index_not_ready_scopes": 0,
                "exact_lookup_repos": 0,
                "exact_lookup_hits": 0,
                "exact_lookup_misses": 0,
                "exact_lookup_soft_misses": 0,
                "direct_quality_short_circuits": 0,
                "early_quality_short_circuits": 0,
                "simple_file_hit_prunes": 0,
                "direct_scan_early_stops": 0,
                "simple_latency_guards": 0,
                "query_decomposition_latency_guards": 0,
                "deep_expansion_latency_guards": 0,
            },
        }

    @staticmethod
    def _increment_retrieval_stat(request_cache: dict[str, Any] | None, key: str) -> None:
        if request_cache is None:
            return
        stats = request_cache.setdefault("stats", {})
        stats[key] = int(stats.get(key) or 0) + 1

    @staticmethod
    def _retrieval_cache_stats(request_cache: dict[str, Any]) -> dict[str, Any]:
        stats = dict(request_cache.get("stats") or {})
        started_at = float(request_cache.get("started_at") or 0)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000) if started_at else 0
        return {
            **stats,
            "elapsed_ms": elapsed_ms,
            "search_entries": len(request_cache.get("search") or {}),
            "index_row_entries": len(request_cache.get("index_rows") or {}),
            "targeted_index_row_entries": len(request_cache.get("targeted_index_rows") or {}),
            "file_line_entries": len(request_cache.get("file_lines") or {}),
            "structure_like_entries": len(request_cache.get("structure_like") or {}),
            "file_fts_entries": len(request_cache.get("file_fts") or {}),
            "fts_entries": len(request_cache.get("fts") or {}),
            "semantic_fts_entries": len(request_cache.get("semantic_fts") or {}),
            "trace_path_entries": len(request_cache.get("trace_paths") or {}),
            "evidence_entries": len(request_cache.get("evidence") or {}),
            "quality_entries": len(request_cache.get("quality") or {}),
            "rerank_entries": len(request_cache.get("rerank") or {}),
            "question_feature_entries": len(request_cache.get("question_features") or {}),
        }

    @staticmethod
    def _clone_jsonish(payload: Any) -> Any:
        try:
            return json.loads(json.dumps(payload, ensure_ascii=False))
        except (TypeError, ValueError):
            if isinstance(payload, list):
                return [dict(item) if isinstance(item, dict) else item for item in payload]
            if isinstance(payload, dict):
                return dict(payload)
            return payload

    def _index_fingerprint(self, index_path: Path) -> str:
        try:
            stat = index_path.stat()
        except OSError:
            return f"{index_path}:missing:{CODE_INDEX_VERSION}"
        return f"{index_path}:{stat.st_mtime_ns}:{stat.st_size}:{CODE_INDEX_VERSION}"

    def _search_cache_key(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        tokens: list[str],
        *,
        question: str,
        focus_terms: list[str] | None,
        trace_stage: str,
    ) -> str:
        index_path = self._index_path(repo_path)
        payload = {
            "repo": entry.display_name,
            "url": entry.url,
            "repo_path": str(repo_path),
            "index": self._index_fingerprint(index_path),
            "tokens": list(tokens),
            "question": question,
            "focus_terms": list(focus_terms or []),
            "trace_stage": trace_stage,
        }
        return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    def _ensure_repo_index_cached(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> None:
        if request_cache is None:
            self._require_ready_repo_index(key=None, entry=entry, repo_path=repo_path)
            return
        ensured_indexes = request_cache.setdefault("ensured_indexes", set())
        ensured_key = str(repo_path)
        if ensured_key in ensured_indexes:
            self._increment_retrieval_stat(request_cache, "index_ensure_hits")
            return
        self._increment_retrieval_stat(request_cache, "index_ensure_misses")
        self._require_ready_repo_index(key=None, entry=entry, repo_path=repo_path)
        ensured_indexes.add(ensured_key)

    def _search_repo(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        tokens: list[str],
        *,
        question: str,
        focus_terms: list[str] | None = None,
        trace_stage: str = "direct",
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            cache_key = self._search_cache_key(
                entry,
                repo_path,
                tokens,
                question=question,
                focus_terms=focus_terms,
                trace_stage=trace_stage,
            )
            if request_cache is not None:
                search_cache = request_cache.setdefault("search", {})
                cached = search_cache.get(cache_key)
                if cached is not None:
                    self._increment_retrieval_stat(request_cache, "search_hits")
                    return self._clone_jsonish(cached)
                self._increment_retrieval_stat(request_cache, "search_misses")
            matches = self._search_repo_index(
                entry,
                repo_path,
                tokens,
                question=question,
                focus_terms=focus_terms,
                trace_stage=trace_stage,
                request_cache=request_cache,
            )
            if request_cache is not None:
                request_cache.setdefault("search", {})[cache_key] = self._clone_jsonish(matches)
            return matches
        except (OSError, sqlite3.Error):
            return []

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
        return list(SourceCodeQAService._question_tokens_cached(str(question or "")))

    @staticmethod
    @functools.lru_cache(maxsize=4096)
    def _question_tokens_cached(question: str) -> tuple[str, ...]:
        lowered_question = question.lower()
        raw_tokens = re.findall(r"[a-zA-Z0-9_./:-]{1,}", lowered_question)
        raw_tokens.extend(re.findall(r"[\u4e00-\u9fff]{2,}", lowered_question))
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
        return tuple(tokens[:28])

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

    @staticmethod
    def _requires_cross_repo_context(question: str) -> bool:
        lowered = f" {str(question or '').lower()} "
        return any(
            term in lowered
            for term in (
                " which repo",
                " which repository",
                " which service",
                " cross repo",
                " cross-repo",
                " downstream",
                " upstream",
                " consume",
                " consumes",
                " producer",
                " publisher",
                " after it is written",
                " after written",
                " read ",
                " written ",
                " v2 ",
                " report ",
                " failed ",
                " failure ",
                " root cause ",
                " 调用链",
                " 链路",
                " 上游",
                " 下游",
                " 失败",
                " 报错",
                " 为什么",
                " 什么意思",
                " 区别",
            )
        )

    @staticmethod
    def _is_test_file_path(path: str) -> bool:
        normalized = str(path or "").replace("\\", "/").lower()
        return any(marker in normalized for marker in TEST_PATH_MARKERS) or normalized.endswith(
            ("test.java", "tests.java", "spec.java", "test.py", "spec.py", "test.ts", "spec.ts", "test.js", "spec.js", "test.tsx", "spec.tsx", "test.jsx", "spec.jsx")
        )

    def _expand_dependency_matches(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        base_matches: list[dict[str, Any]],
        limit: int,
        request_cache: dict[str, Any] | None = None,
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
                    request_cache=request_cache,
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
        request_cache: dict[str, Any] | None = None,
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
                    request_cache=request_cache,
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
        request_cache: dict[str, Any] | None = None,
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
                        request_cache=request_cache,
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
        request_cache: dict[str, Any] | None = None,
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
                    request_cache=request_cache,
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
        tool_trace: list[dict[str, Any]] | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        current_matches = list(base_matches)
        seen = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in current_matches}
        executed_steps: set[str] = set()
        empty_rounds = 0
        max_rounds = 5
        for step_index in range(1, max_rounds + 1):
            evidence_summary = self._compress_evidence_cached(question, current_matches, request_cache=request_cache)
            quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
            step = self._choose_next_tool_step(
                question=question,
                matches=current_matches,
                evidence_summary=evidence_summary,
                quality_gate=quality_gate,
                executed_steps=executed_steps,
            )
            if not step:
                if tool_trace is not None:
                    tool_trace.append(
                        {
                            "phase": "tool_loop",
                            "round": step_index,
                            "tool": "stop",
                            "reason": "no_next_tool",
                            "matches_before": len(current_matches),
                        }
                    )
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
                request_cache=request_cache,
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
            current_matches = self._select_result_matches(current_matches, max(1, min(int(limit or 12), 30)), question=question)
            empty_rounds = empty_rounds + 1 if added == 0 else 0
            should_stop = self._should_stop_tool_loop(question, current_matches, step_index, empty_rounds, request_cache=request_cache)
            if tool_trace is not None:
                tool_trace.append(
                    {
                        "phase": "tool_loop",
                        "round": step_index,
                        "tool": str(step.get("tool") or "search_code"),
                        "terms": [str(term) for term in step.get("terms") or []][:10],
                        "matches_found": len(step_matches),
                        "matches_added": added,
                        "matches_after": len(current_matches),
                        "stop_reason": "quality_sufficient" if should_stop else "",
                    }
                )
            if should_stop:
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
                if duplicate_retrieval in {
                    "flow_graph",
                    "code_graph",
                    "entity_graph",
                    "static_qa",
                    "test_coverage",
                    "operational_boundary",
                }:
                    existing["retrieval"] = duplicate_retrieval
                for payload_key in ("static_qa", "test_coverage", "operational_boundary"):
                    if duplicate.get(payload_key):
                        existing[payload_key] = duplicate.get(payload_key)
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
                    {"tool": "find_tables", "terms": [*terms[:10], *quality_terms[:8]]},
                    {"tool": "trace_entity", "terms": terms[:12]},
                    {"tool": "trace_flow", "terms": terms[:12]},
                    {"tool": "trace_graph", "terms": terms[:12]},
                    {"tool": "find_callees", "terms": terms[:12]},
                    {"tool": "find_callers", "terms": terms[:12]},
                    {"tool": "search_code", "terms": [*quality_terms, "repository", "mapper", "dao", "select", "from", "client"]},
                ]
            )
        if intent.get("api"):
            candidates.extend(
                [
                    {"tool": "find_api_routes", "terms": [*terms[:10], *quality_terms[:8]]},
                    {"tool": "trace_entity", "terms": terms[:12]},
                    {"tool": "trace_flow", "terms": terms[:12]},
                    {"tool": "find_references", "terms": [*terms[:10], "RequestMapping", "PostMapping", "GetMapping"]},
                    {"tool": "find_callees", "terms": terms[:12]},
                    {"tool": "search_code", "terms": [*terms[:8], "controller", "endpoint", "client"]},
                ]
            )
        if intent.get("config"):
            candidates.append({"tool": "search_code", "terms": [*terms[:8], *quality_terms, "properties", "yaml", "configuration"]})
        if intent.get("module_dependency"):
            candidates.extend(
                [
                    {"tool": "trace_flow", "terms": [*terms[:10], "module_dependency", "maven", "gradle"]},
                    {"tool": "search_code", "terms": [*terms[:8], *quality_terms, "pom.xml", "build.gradle", "package.json", "artifactId", "dependency"]},
                ]
            )
        if intent.get("rule_logic") or intent.get("error"):
            candidates.append({"tool": "search_code", "terms": [*terms[:8], *quality_terms, "validate", "rule", "exception"]})
        if intent.get("static_qa"):
            candidates.extend(
                [
                    {"tool": "find_static_findings", "terms": [*terms[:10], *quality_terms[:8]]},
                    {"tool": "search_code", "terms": [*terms[:8], *quality_terms, "TODO", "FIXME", "secret", "catch", "Exception"]},
                ]
            )
        if intent.get("impact_analysis"):
            candidates.extend(
                [
                    {"tool": "find_references", "terms": [*terms[:12], *quality_terms[:6]]},
                    {"tool": "find_callers", "terms": terms[:12]},
                    {"tool": "find_callees", "terms": terms[:12]},
                    {"tool": "trace_flow", "terms": terms[:12]},
                    {"tool": "trace_entity", "terms": terms[:12]},
                    {"tool": "search_code", "terms": [*terms[:8], "controller", "service", "repository", "client", "handler"]},
                ]
            )
        if intent.get("test_coverage"):
            candidates.extend(
                [
                    {"tool": "find_test_coverage", "terms": [*terms[:12], *quality_terms[:6]]},
                    {"tool": "search_code", "terms": [*terms[:8], "test", "assert", "verify", "mock"]},
                ]
            )
        if intent.get("operational_boundary"):
            candidates.extend(
                [
                    {"tool": "find_operational_boundaries", "terms": [*terms[:12], *quality_terms[:8]]},
                    {"tool": "trace_entity", "terms": [*terms[:12], "operational_boundary"]},
                    {"tool": "search_code", "terms": [*terms[:8], "Transactional", "Cacheable", "Async", "Retryable", "CircuitBreaker"]},
                ]
            )

        candidates.extend(self._build_tool_loop_plan(question, matches))
        if matches:
            candidates.append({"tool": "open_file_window", "terms": terms[:8]})
        if terms:
            candidates.append({"tool": "trace_flow", "terms": terms[:12]})

        for candidate in candidates:
            normalized_terms = list(
                dict.fromkeys(str(term).strip() for term in candidate.get("terms") or [] if str(term).strip())
            )
            tool = str(candidate.get("tool") or "")
            if tool not in {
                "find_definition",
                "find_references",
                "find_callers",
                "find_callees",
                "open_file_window",
                "find_tables",
                "find_api_routes",
                "trace_graph",
                "trace_flow",
                "trace_entity",
                "find_static_findings",
                "find_test_coverage",
                "find_operational_boundaries",
                "search_code",
            }:
                continue
            if tool in {"find_definition", "find_references", "find_callers", "find_tables", "find_api_routes", "search_code"} and not normalized_terms:
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
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        tool = str(step.get("tool") or "")
        terms = [str(term) for term in (step.get("terms") or []) if str(term)]
        step_matches: list[dict[str, Any]] = []
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            if tool == "find_definition":
                step_matches.extend(self._tool_find_definition(entry, repo_path, terms, question, step_index, request_cache=request_cache))
            elif tool == "find_references":
                step_matches.extend(self._tool_find_references(entry, repo_path, terms, question, step_index, request_cache=request_cache))
            elif tool == "find_callers":
                step_matches.extend(self._tool_find_callers(entry, repo_path, matches, terms, question, step_index, request_cache=request_cache))
            elif tool == "find_callees":
                step_matches.extend(self._tool_find_callees(entry, repo_path, matches, terms, question, step_index, request_cache=request_cache))
            elif tool == "open_file_window":
                step_matches.extend(self._tool_open_file_window(entry, repo_path, matches, question, step_index, request_cache=request_cache))
            elif tool == "find_tables":
                step_matches.extend(self._tool_find_tables(entry, repo_path, terms, question, step_index, request_cache=request_cache))
            elif tool == "find_api_routes":
                step_matches.extend(self._tool_find_api_routes(entry, repo_path, terms, question, step_index, request_cache=request_cache))
            elif tool == "trace_graph":
                step_matches.extend(self._tool_trace_graph(entry, repo_path, matches, question, step_index, request_cache=request_cache))
            elif tool == "trace_flow":
                step_matches.extend(self._tool_trace_flow(entry, repo_path, matches, question, step_index, request_cache=request_cache))
            elif tool == "trace_entity":
                step_matches.extend(self._tool_trace_entity(entry, repo_path, matches, question, step_index, request_cache=request_cache))
            elif tool == "find_static_findings":
                step_matches.extend(self._tool_find_static_findings(entry, repo_path, terms, question, step_index, request_cache=request_cache))
            elif tool == "find_test_coverage":
                step_matches.extend(self._tool_find_test_coverage(entry, repo_path, terms, question, step_index, request_cache=request_cache))
            elif tool == "find_operational_boundaries":
                step_matches.extend(self._tool_find_operational_boundaries(entry, repo_path, terms, question, step_index, request_cache=request_cache))
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
                        request_cache=request_cache,
                    )
                )
        return step_matches

    def _should_stop_tool_loop(
        self,
        question: str,
        matches: list[dict[str, Any]],
        step_index: int,
        empty_rounds: int,
        request_cache: dict[str, Any] | None = None,
    ) -> bool:
        if empty_rounds >= 2:
            return True
        evidence_summary = self._compress_evidence_cached(question, matches, request_cache=request_cache)
        quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
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
            if intent.get("data_source"):
                plan.append({"tool": "find_tables", "terms": terms[:12]})
            if intent.get("api"):
                plan.append({"tool": "find_api_routes", "terms": terms[:12]})
            plan.append({"tool": "trace_entity", "terms": terms[:12]})
            plan.append({"tool": "trace_flow", "terms": terms[:12]})
            plan.append({"tool": "trace_graph", "terms": terms[:12]})
            plan.append({"tool": "find_callees", "terms": terms[:12]})
        if intent.get("config"):
            plan.append({"tool": "search_code", "terms": [*terms[:8], "properties", "configuration", "yaml", "feature"]})
        if intent.get("data_source"):
            plan.append({"tool": "search_code", "terms": [*terms[:8], "repository", "mapper", "select", "from", "client"]})
        if intent.get("static_qa"):
            plan.append({"tool": "find_static_findings", "terms": [*terms[:8], "secret", "catch", "exception", "todo", "sql"]})
        if intent.get("test_coverage"):
            plan.append({"tool": "find_test_coverage", "terms": [*terms[:8], "test", "assert", "verify", "mock"]})
        if intent.get("operational_boundary"):
            plan.append({"tool": "find_operational_boundaries", "terms": [*terms[:8], "Transactional", "Cacheable", "Async", "Retryable", "CircuitBreaker"]})
        if intent.get("impact_analysis"):
            plan.extend(
                [
                    {"tool": "find_references", "terms": terms[:12]},
                    {"tool": "find_callers", "terms": terms[:12]},
                    {"tool": "find_callees", "terms": terms[:12]},
                    {"tool": "trace_flow", "terms": terms[:12]},
                ]
            )
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
        request_cache: dict[str, Any] | None = None,
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
            request_cache=request_cache,
        )

    def _tool_find_references(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
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
            request_cache=request_cache,
        )

    def _tool_find_tables(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        lookup_terms = list(dict.fromkeys([*terms, "select", "from", "join", "update", "insert"]))
        return self._tool_lookup_references_by_kind(
            entry,
            repo_path,
            lookup_terms,
            kinds={"sql_table"},
            question=question,
            trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
            retrieval="planner_table",
            score=184,
            request_cache=request_cache,
        )

    def _tool_find_api_routes(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        lookup_terms = list(dict.fromkeys([*terms, "requestmapping", "postmapping", "getmapping", "endpoint", "api", "http"]))
        return self._tool_lookup_references_by_kind(
            entry,
            repo_path,
            lookup_terms,
            kinds={"route", "http_endpoint", "downstream_api"},
            question=question,
            trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
            retrieval="planner_api_route",
            score=178,
            request_cache=request_cache,
        )

    def _tool_find_static_findings(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        index_path = self._index_path(repo_path)
        lowered_terms = [str(term).lower() for term in terms if len(str(term).strip()) >= 3]
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                index_rows = self._cached_index_rows(connection, index_path, request_cache=request_cache)
                lines_by_path = index_rows.get("lines_by_path") or {}
                seen: set[tuple[str, int, str]] = set()
                for file_path, lines in lines_by_path.items():
                    path_lower = str(file_path).lower()
                    if any(part in path_lower for part in ("/node_modules/", "/dist/", "/build/", "/target/", ".min.js")):
                        continue
                    for line_index, line_text in enumerate(lines, start=1):
                        findings = self._static_qa_findings_for_line(str(line_text))
                        if not findings:
                            continue
                        haystack = f"{file_path} {line_text}".lower()
                        for finding in findings:
                            key = (str(file_path), line_index, str(finding["kind"]))
                            if key in seen:
                                continue
                            seen.add(key)
                            term_boost = 18 if lowered_terms and any(term in haystack or term in str(finding["kind"]).lower() for term in lowered_terms) else 0
                            match = self._match_from_index_location(
                                entry,
                                connection,
                                str(file_path),
                                line_index,
                                score=int(finding["score"]) + term_boost,
                                reason=f"static QA finding: {finding['severity']} {finding['kind']} - {finding['reason']}",
                                question=question,
                                trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}" if step_index else "static_qa",
                                retrieval="static_qa",
                                index_path=index_path,
                                request_cache=request_cache,
                            )
                            if match:
                                match["static_qa"] = {
                                    "kind": finding["kind"],
                                    "severity": finding["severity"],
                                    "reason": finding["reason"],
                                }
                                matches.append(match)
                matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        except (OSError, sqlite3.Error):
            return []
        return matches[:80]

    def _tool_find_test_coverage(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        noise_terms = set(self._question_tokens(" ".join(TEST_COVERAGE_HINTS))) | {"covered", "coverage", "unit", "integration", "junit", "pytest", "jest"}
        lookup_terms = [
            term.lower()
            for term in [*terms, *self._question_tokens(question)]
            if len(str(term).strip()) >= 3 and term.lower() not in noise_terms and term.lower() not in STOPWORDS
        ]
        lookup_terms = list(dict.fromkeys(lookup_terms))[:18]
        if not lookup_terms:
            return []
        matches: list[dict[str, Any]] = []
        seen: set[tuple[str, int, str]] = set()
        index_path = self._index_path(repo_path)
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                index_rows = self._cached_index_rows(connection, index_path, request_cache=request_cache)
                lines_by_path = index_rows.get("lines_by_path") or {}
                for file_path, lines in lines_by_path.items():
                    if not self._is_test_file_path(str(file_path)):
                        continue
                    path_lower = str(file_path).lower()
                    file_term_hit = any(term in path_lower for term in lookup_terms)
                    best_line = 1
                    best_score = 0
                    best_reasons: list[str] = []
                    for line_index, line_text in enumerate(lines, start=1):
                        lowered_line = str(line_text).lower()
                        term_hits = [term for term in lookup_terms if term in lowered_line]
                        if not term_hits and not file_term_hit:
                            continue
                        score = 172 + (28 if term_hits else 0) + (16 if file_term_hit else 0)
                        if TEST_ASSERTION_PATTERN.search(str(line_text)):
                            score += 24
                            best_reasons.append("assertion/verify present")
                        if TEST_ANNOTATION_PATTERN.search(str(line_text)) or re.search(r"\b(?:test|should)[A-Za-z0-9_]*\s*\(", str(line_text)):
                            score += 20
                            best_reasons.append("test case present")
                        if term_hits:
                            best_reasons.append(f"target terms: {', '.join(term_hits[:4])}")
                        if score > best_score:
                            best_score = score
                            best_line = line_index
                    if best_score:
                        key = (str(file_path), best_line, "test_coverage")
                        if key in seen:
                            continue
                        seen.add(key)
                        match = self._match_from_index_location(
                            entry,
                            connection,
                            str(file_path),
                            best_line,
                            score=best_score,
                            reason="test coverage evidence: " + "; ".join(list(dict.fromkeys(best_reasons))[:5]),
                            question=question,
                            trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}" if step_index else "test_coverage",
                            retrieval="test_coverage",
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                        if match:
                            match["test_coverage"] = {
                                "terms": lookup_terms[:8],
                                "has_assertion": "assertion/verify present" in best_reasons,
                                "has_test_case": "test case present" in best_reasons,
                            }
                            matches.append(match)
                for term in lookup_terms[:10]:
                    like_term = f"%{term}%"
                    rows = connection.execute(
                        """
                        select * from references_index
                        where kind in ('test_subject', 'test_reference', 'test_assertion', 'test_case', 'call', 'import')
                          and lower_target like ?
                        limit 80
                        """,
                        (like_term,),
                    ).fetchall()
                    for row in rows:
                        file_path = str(row["file_path"])
                        if not self._is_test_file_path(file_path):
                            continue
                        line_no = int(row["line_no"] or 1)
                        key = (file_path, line_no, str(row["kind"] or ""))
                        if key in seen:
                            continue
                        seen.add(key)
                        kind = str(row["kind"] or "")
                        score = 218 if kind in {"test_assertion", "test_subject", "test_reference"} else 196
                        if str(row["lower_target"] or "") == term:
                            score += 24
                        match = self._match_from_index_location(
                            entry,
                            connection,
                            file_path,
                            line_no,
                            score=score,
                            reason=f"test coverage evidence: {kind} {row['target']}",
                            question=question,
                            trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}" if step_index else "test_coverage",
                            retrieval="test_coverage",
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                        if match:
                            match["test_coverage"] = {"kind": kind, "target": str(row["target"] or term)}
                            matches.append(match)
        except (OSError, sqlite3.Error):
            return []
        matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        return matches[:80]

    def _tool_find_operational_boundaries(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        boundary_terms = [
            "transactional",
            "transaction",
            "rollback",
            "cacheable",
            "cacheevict",
            "cacheput",
            "cache",
            "async",
            "retryable",
            "retry",
            "circuitbreaker",
            "ratelimiter",
            "bulkhead",
            "timelimiter",
            "schedulerlock",
            "lock",
            "preauthorize",
            "postauthorize",
        ]
        lowered_terms = list(
            dict.fromkeys(
                term.lower()
                for term in [*terms, *self._question_tokens(question), *boundary_terms]
                if len(str(term).strip()) >= 3 and str(term).lower() not in STOPWORDS
            )
        )[:32]
        matches: list[dict[str, Any]] = []
        seen: set[tuple[str, int, str]] = set()
        index_path = self._index_path(repo_path)
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                rows = connection.execute(
                    """
                    select * from references_index
                    where kind in ('operational_boundary', 'framework_binding', 'scheduled_job', 'web_interceptor', 'bean_condition')
                    limit 240
                    """
                ).fetchall()
                for row in rows:
                    target = str(row["target"] or "")
                    context = str(row["context"] or "")
                    haystack = f"{target} {context} {row['file_path']}".lower()
                    if lowered_terms and not any(term in haystack for term in lowered_terms):
                        continue
                    line_no = int(row["line_no"] or 1)
                    key = (str(row["file_path"]), line_no, str(row["kind"] or ""))
                    if key in seen:
                        continue
                    seen.add(key)
                    kind = str(row["kind"] or "")
                    score = 226 if kind == "operational_boundary" else 186
                    if any(term in target.lower() for term in ("transactional", "cache", "async", "retry", "circuit", "rate", "lock", "authorize")):
                        score += 24
                    match = self._match_from_index_location(
                        entry,
                        connection,
                        str(row["file_path"]),
                        line_no,
                        score=score,
                        reason=f"operational boundary evidence: {kind} {target}",
                        question=question,
                        trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}" if step_index else "operational_boundary",
                        retrieval="operational_boundary",
                        index_path=index_path,
                        request_cache=request_cache,
                    )
                    if match:
                        match["operational_boundary"] = {"kind": kind, "target": target}
                        matches.append(match)
        except (OSError, sqlite3.Error):
            return []
        matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        return matches[:80]

    def _expand_impact_matches(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        question: str,
        base_matches: list[dict[str, Any]],
        limit: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        question_tokens = set(self._question_tokens(question))
        upstream_matches: list[dict[str, Any]] = []
        downstream_matches: list[dict[str, Any]] = []
        seen: set[tuple[str, str, int, str, str]] = set()
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            repo_seed_matches = [match for match in base_matches if match.get("repo") == entry.display_name and match.get("path")]
            repo_seed_matches.sort(
                key=lambda item: (
                    Path(str(item.get("path") or "")).stem.lower() in question_tokens,
                    int(item.get("score") or 0),
                ),
                reverse=True,
            )
            seed_paths = list(dict.fromkeys(str(match.get("path") or "") for match in repo_seed_matches))[:8]
            if not seed_paths:
                continue
            index_path = self._index_path(repo_path)
            try:
                self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
                with sqlite3.connect(index_path) as connection:
                    connection.row_factory = sqlite3.Row
                    def resolve_target_location(target_name: str) -> tuple[str, int] | None:
                        normalized = str(target_name or "").strip()
                        if not normalized:
                            return None
                        candidates = list(dict.fromkeys(
                            item.lower()
                            for item in (
                                normalized,
                                normalized.split(".")[-1],
                                re.sub(r"\b(get|set|find|create|update|delete|read|write)\b", "", normalized, flags=re.IGNORECASE).strip("."),
                            )
                            if item
                        ))
                        for candidate in candidates:
                            row = connection.execute(
                                """
                                select file_path, line_no from definitions
                                where lower_name = ?
                                order by
                                    case
                                        when kind like '%class%' or kind in ('class', 'interface') then 0
                                        when kind like '%method%' then 1
                                        else 2
                                    end,
                                    line_no
                                limit 1
                                """,
                                (candidate,),
                            ).fetchone()
                            if row:
                                return str(row["file_path"] or ""), int(row["line_no"] or 1)
                            row = connection.execute(
                                """
                                select file_path, line_no from code_entities
                                where lower_name = ?
                                order by
                                    case
                                        when kind like '%class%' or kind in ('class', 'interface') then 0
                                        when kind like '%method%' then 1
                                        else 2
                                    end,
                                    line_no
                                limit 1
                                """,
                                (candidate,),
                            ).fetchone()
                            if row:
                                return str(row["file_path"] or ""), int(row["line_no"] or 1)
                        return None

                    for seed_path in seed_paths:
                        seed_exact = Path(seed_path).stem.lower() in question_tokens
                        rows = connection.execute(
                            """
                            select * from flow_edges
                            where from_file = ? or to_file = ?
                            order by
                                case
                                    when to_file = ? and from_kind in ('controller', 'handler', 'consumer') then 0
                                    when to_file = ? then 1
                                    when from_file = ? and edge_kind in ('repository', 'mapper', 'dao', 'sql_table', 'db_read', 'db_write', 'client', 'route') then 2
                                    when from_file = ? then 3
                                    else 4
                                end,
                                from_line
                            limit 80
                            """,
                            (seed_path, seed_path, seed_path, seed_path, seed_path, seed_path),
                        ).fetchall()
                        for row in rows:
                            raw_upstream = str(row["to_file"] or "") == seed_path and str(row["from_file"] or "") != seed_path
                            from_kind = str(row["from_kind"] or "").lower()
                            upstream = raw_upstream and from_kind not in {
                                "repository",
                                "mapper",
                                "dao",
                                "client",
                                "integration",
                                "gateway",
                            }
                            if raw_upstream and not upstream:
                                file_path = str(row["from_file"] or "")
                            else:
                                file_path = str(row["from_file"] if upstream else row["to_file"] or row["from_file"])
                            if not file_path:
                                continue
                            line_no = int(row["from_line"] if upstream or (raw_upstream and not upstream) else row["to_line"] or row["from_line"] or 1)
                            if not upstream and not str(row["to_file"] or "").strip():
                                resolved = resolve_target_location(str(row["to_name"] or ""))
                                if resolved and resolved[0]:
                                    file_path, line_no = resolved
                            edge_key = (
                                entry.display_name,
                                file_path,
                                line_no,
                                str(row["edge_kind"] or ""),
                                str(row["to_name"] or ""),
                            )
                            if edge_key in seen:
                                continue
                            seen.add(edge_key)
                            role = "upstream caller" if upstream else "downstream dependency"
                            path_lower = file_path.lower()
                            score = 214 if seed_exact else 190
                            if upstream and any(marker in path_lower for marker in ("controller", "handler", "consumer", "job")):
                                score += 20
                            if not upstream and any(marker in path_lower for marker in ("repository", "mapper", "dao", "client")):
                                score += 20
                            match = self._match_from_index_location(
                                entry,
                                connection,
                                file_path,
                                line_no,
                                score=score,
                                reason=f"impact {role}: {row['edge_kind']} {row['from_name']} -> {row['to_name']}",
                                question=question,
                                trace_stage="impact_analysis",
                                retrieval="planner_caller" if upstream else "planner_callee",
                                index_path=index_path,
                                request_cache=request_cache,
                            )
                            if match:
                                if upstream:
                                    upstream_matches.append(match)
                                else:
                                    downstream_matches.append(match)
            except (OSError, sqlite3.Error):
                continue
        upstream_matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        downstream_matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        max_items = max(6, min(int(limit or 12), 24))
        balanced: list[dict[str, Any]] = []
        seen_result: set[tuple[Any, Any, Any, Any]] = set()

        def add_result(match: dict[str, Any]) -> None:
            if len(balanced) >= max_items:
                return
            key_value = (match.get("repo"), match.get("path"), match.get("line_start"), match.get("line_end"))
            if key_value in seen_result:
                return
            balanced.append(match)
            seen_result.add(key_value)

        for bucket_limit, bucket in ((max_items // 2, upstream_matches), (max_items // 2, downstream_matches)):
            added = 0
            for match in bucket:
                before = len(balanced)
                add_result(match)
                if len(balanced) > before:
                    added += 1
                if added >= max(1, bucket_limit):
                    break
        for match in [*upstream_matches, *downstream_matches]:
            add_result(match)
            if len(balanced) >= max_items:
                break
        balanced.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
        return balanced

    def _tool_lookup_references_by_kind(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        terms: list[str],
        *,
        kinds: set[str],
        question: str,
        trace_stage: str,
        retrieval: str,
        score: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        index_path = self._index_path(repo_path)
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                placeholders = ",".join("?" for _ in kinds)
                kind_values = tuple(sorted(kinds))
                rows = connection.execute(
                    f"select * from references_index where kind in ({placeholders}) limit 120",
                    kind_values,
                ).fetchall()
                lowered_terms = [str(term).lower() for term in terms if len(str(term).strip()) >= 3]
                for row in rows:
                    haystack = f"{row['target']} {row['context']} {row['file_path']}".lower()
                    if lowered_terms and not any(term in haystack for term in lowered_terms):
                        if str(row["kind"]) == "sql_table" and not any(keyword in haystack for keyword in ("select", "from", "join", "update", "insert")):
                            continue
                    matches.append(
                        self._match_from_index_location(
                            entry,
                            connection,
                            str(row["file_path"]),
                            int(row["line_no"]),
                            score=score,
                            reason=f"planner {retrieval}: {row['kind']} {row['target']}",
                            question=question,
                            trace_stage=trace_stage,
                            retrieval=retrieval,
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                    )
        except (OSError, sqlite3.Error):
            return []
        return [match for match in matches if match]

    def _tool_find_callers(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        base_matches: list[dict[str, Any]],
        terms: list[str],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
        return self._tool_lookup_flow_edges(
            entry,
            repo_path,
            terms=terms,
            seed_paths=seed_paths,
            direction="callers",
            question=question,
            step_index=step_index,
            request_cache=request_cache,
        )

    def _tool_find_callees(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        base_matches: list[dict[str, Any]],
        terms: list[str],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
        return self._tool_lookup_flow_edges(
            entry,
            repo_path,
            terms=terms,
            seed_paths=seed_paths,
            direction="callees",
            question=question,
            step_index=step_index,
            request_cache=request_cache,
        )

    def _tool_lookup_flow_edges(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        *,
        terms: list[str],
        seed_paths: list[str],
        direction: str,
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        index_path = self._index_path(repo_path)
        lowered_terms = [str(term).lower() for term in terms if len(str(term).strip()) >= 3]
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                rows: list[sqlite3.Row] = []
                if direction == "callers" and seed_paths:
                    for path in seed_paths:
                        rows.extend(
                            connection.execute(
                                """
                                select * from flow_edges
                                where to_file = ?
                                order by
                                    case edge_kind
                                        when 'route' then 0
                                        when 'controller' then 1
                                        when 'service' then 2
                                        when 'client' then 3
                                        when 'runtime_call' then 4
                                        else 5
                                    end,
                                    from_line
                                limit 80
                                """,
                                (path,),
                            ).fetchall()
                        )
                if direction == "callees" and seed_paths:
                    for path in seed_paths:
                        rows.extend(
                            connection.execute(
                                """
                                select * from flow_edges
                                where from_file = ?
                                order by
                                    case edge_kind
                                        when 'sql_table' then 0
                                        when 'repository' then 1
                                        when 'mapper' then 2
                                        when 'dao' then 3
                                        when 'client' then 4
                                        else 5
                                    end,
                                    from_line
                                limit 60
                                """,
                                (path,),
                            ).fetchall()
                        )
                if lowered_terms:
                    for term in lowered_terms[:16]:
                        rows.extend(
                            connection.execute(
                                """
                                select * from flow_edges
                                where lower(to_name) like ? or lower(from_name) like ? or lower(evidence) like ?
                                limit 40
                                """,
                                (f"%{term}%", f"%{term}%", f"%{term}%"),
                            ).fetchall()
                        )
                seen_rows: set[tuple[Any, ...]] = set()
                for row in rows:
                    row_key = (row["from_file"], row["from_line"], row["edge_kind"], row["to_name"], row["to_file"], row["to_line"])
                    if row_key in seen_rows:
                        continue
                    seen_rows.add(row_key)
                    if direction == "callers":
                        file_path = str(row["from_file"])
                        line_no = int(row["from_line"] or 1)
                        retrieval = "planner_caller"
                        score = 192 if str(row["from_kind"] or "").lower() in {"controller", "handler", "consumer"} else 184 if any(
                            marker in file_path.lower() for marker in ("controller", "handler", "consumer", "job", "scheduler")
                        ) else 176
                    else:
                        file_path = str(row["to_file"] or row["from_file"])
                        line_no = int(row["to_line"] or row["from_line"] or 1)
                        retrieval = "planner_callee"
                        score = 176 if row["edge_kind"] in {"sql_table", "repository", "mapper", "dao", "client"} else 150
                    matches.append(
                        self._match_from_index_location(
                            entry,
                            connection,
                            file_path,
                            line_no,
                            score=score,
                            reason=f"planner {direction}: {row['edge_kind']} {row['from_name']} -> {row['to_name']}",
                            question=question,
                            trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                            retrieval=retrieval,
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                    )
        except (OSError, sqlite3.Error):
            return []
        return [match for match in matches if match]

    def _tool_open_file_window(
        self,
        entry: RepositoryEntry,
        repo_path: Path,
        base_matches: list[dict[str, Any]],
        question: str,
        step_index: int,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        index_path = self._index_path(repo_path)
        seeds = [match for match in base_matches if match.get("repo") == entry.display_name][:6]
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                for seed in seeds:
                    line_no = int(seed.get("line_start") or 1)
                    match = self._match_from_index_location(
                        entry,
                        connection,
                        str(seed.get("path") or ""),
                        line_no,
                        score=max(120, int(seed.get("score") or 0) - 5),
                        reason=f"planner open file window: {seed.get('reason') or 'seed evidence'}",
                        question=question,
                        trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                        retrieval="open_file_window",
                        index_path=index_path,
                        request_cache=request_cache,
                    )
                    if match:
                        matches.append(match)
        except (OSError, sqlite3.Error):
            return []
        return matches

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
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        index_path = self._index_path(repo_path)
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
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
                                index_path=index_path,
                                request_cache=request_cache,
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
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        index_path = self._index_path(repo_path)
        seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:8]
        matches: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
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
                                index_path=index_path,
                                request_cache=request_cache,
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
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        index_path = self._index_path(repo_path)
        seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
        matches: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
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
                                    index_path=index_path,
                                    request_cache=request_cache,
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
                                    index_path=index_path,
                                    request_cache=request_cache,
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
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        index_path = self._index_path(repo_path)
        seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
        matches: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
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
                                    index_path=index_path,
                                    request_cache=request_cache,
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
                                    index_path=index_path,
                                    request_cache=request_cache,
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
        index_path: Path | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        lines: list[str] = []
        if index_path is not None and request_cache is not None:
            lines = self._cached_file_lines(
                connection,
                index_path,
                file_path,
                request_cache=request_cache,
            )
        if not lines:
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

    def _trace_paths_cache_key(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        matches: list[dict[str, Any]],
        limit: int,
    ) -> str:
        repo_fingerprints = []
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            repo_fingerprints.append(
                {
                    "name": entry.display_name,
                    "url": entry.url,
                    "path": str(repo_path),
                    "index": self._index_fingerprint(self._index_path(repo_path)),
                }
            )
        payload = {
            "repos": repo_fingerprints,
            "matches": self._match_cache_signature(matches),
            "limit": max(1, int(limit or 6)),
        }
        return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    def _build_trace_paths(
        self,
        *,
        entries: list[RepositoryEntry],
        key: str,
        matches: list[dict[str, Any]],
        question: str,
        limit: int = 6,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        del question
        if not matches:
            return []
        cache_key = self._trace_paths_cache_key(entries=entries, key=key, matches=matches, limit=limit)
        if request_cache is not None:
            trace_cache = request_cache.setdefault("trace_paths", {})
            cached = trace_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "trace_paths_hits")
                return self._clone_jsonish(cached)
            self._increment_retrieval_stat(request_cache, "trace_paths_misses")
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
                self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
                with sqlite3.connect(self._index_path(repo_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    edge_cache: dict[str, list[dict[str, Any]]] = {}

                    def edges_for(seed_path: str) -> list[dict[str, Any]]:
                        cached_edges = edge_cache.get(seed_path)
                        if cached_edges is not None:
                            return cached_edges
                        edge_cache[seed_path] = self._trace_path_edges_for_seed(connection, seed_path)
                        return edge_cache[seed_path]

                    for seed in seed_paths:
                        first_hops = edges_for(seed)
                        for first in first_hops[:10]:
                            path = self._trace_path_from_edges(entry.display_name, seed, [first])
                            signature = json.dumps(path.get("edges") or [], sort_keys=True)
                            if signature not in seen_signatures:
                                paths.append(path)
                                seen_signatures.add(signature)
                            next_seed = str(first.get("to_file") or "")
                            if not next_seed:
                                continue
                            for second in edges_for(next_seed)[:6]:
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
        result = paths[: max(1, int(limit or 6))]
        if request_cache is not None:
            request_cache.setdefault("trace_paths", {})[cache_key] = self._clone_jsonish(result)
        return result

    def _build_repo_dependency_graph(
        self,
        *,
        key: str,
        entries: list[RepositoryEntry],
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        nodes = [{"name": entry.display_name, "url": entry.url} for entry in entries]
        edge_rows: list[dict[str, Any]] = []
        route_index = self._repo_route_index(key=key, entries=entries, request_cache=request_cache)
        config_index = self._repo_config_index(key=key, entries=entries, request_cache=request_cache)
        message_index = self._repo_message_index(key=key, entries=entries, config_index=config_index, request_cache=request_cache)
        artifact_index = self._repo_artifact_index(key=key, entries=entries, request_cache=request_cache)
        table_index = self._repo_table_index(key=key, entries=entries, request_cache=request_cache)
        for source in entries:
            source_path = self._repo_path(key, source)
            if not (source_path / ".git").exists():
                continue
            try:
                self._ensure_repo_index_cached(source, source_path, request_cache=request_cache)
                with sqlite3.connect(self._index_path(source_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    rows = connection.execute(
                        """
                        select edge_kind, to_name, evidence, from_file, from_line, to_file, to_line
                        from flow_edges
                        where edge_kind in (
                            'client', 'route', 'framework', 'call', 'import', 'module_dependency',
                            'message_publish', 'event_publish', 'db_write'
                        )
                        limit 300
                        """
                    ).fetchall()
            except (OSError, sqlite3.Error):
                continue
            for row in rows:
                candidate = self._match_repo_dependency_candidate(
                    row=dict(row),
                    entries=entries,
                    source_name=source.display_name,
                    route_index=route_index,
                    message_index=message_index,
                    artifact_index=artifact_index,
                    table_index=table_index,
                    source_config=config_index.get(source.display_name) or {},
                )
                if not candidate:
                    continue
                edge_rows.append(
                    {
                        "from_repo": source.display_name,
                        "to_repo": candidate["target"].display_name,
                        "edge_kind": candidate.get("edge_kind") or str(row["edge_kind"] or "dependency"),
                        "confidence": candidate["confidence"],
                        "match_reason": candidate["match_reason"],
                        "evidence": str(row["evidence"] or row["to_name"] or "")[:300],
                        "from_file": str(row["from_file"] or ""),
                        "from_line": int(row["from_line"] or 0),
                        "to_file": str(candidate.get("to_file") or row["to_file"] or ""),
                        "to_line": int(candidate.get("to_line") or row["to_line"] or 0),
                    }
                )
        by_signature: dict[str, dict[str, Any]] = {}
        for edge in edge_rows:
            signature = json.dumps(
                {
                    "from_repo": edge.get("from_repo"),
                    "to_repo": edge.get("to_repo"),
                    "edge_kind": edge.get("edge_kind"),
                    "from_file": edge.get("from_file"),
                    "from_line": edge.get("from_line"),
                    "to_file": edge.get("to_file"),
                    "to_line": edge.get("to_line"),
                },
                sort_keys=True,
            )
            existing = by_signature.get(signature)
            if existing is None or float(edge.get("confidence") or 0) > float(existing.get("confidence") or 0):
                by_signature[signature] = edge
        deduped = sorted(by_signature.values(), key=lambda item: float(item.get("confidence") or 0), reverse=True)
        return {"version": 2, "nodes": nodes, "edges": deduped[:80]}

    def _repo_message_index(
        self,
        *,
        key: str,
        entries: list[RepositoryEntry],
        config_index: dict[str, dict[str, list[str]]] | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        message_index: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            messages: list[dict[str, Any]] = []
            repo_config = (config_index or {}).get(entry.display_name) or {}
            try:
                self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
                with sqlite3.connect(self._index_path(repo_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    for row in connection.execute(
                        """
                        select edge_kind, to_name, evidence, from_file, from_line
                        from flow_edges
                        where edge_kind in ('message_consume', 'event_consume')
                        limit 300
                        """
                    ):
                        for message_name in self._message_values_from_config(str(row["to_name"] or ""), repo_config):
                            messages.append(
                                {
                                    "message": message_name,
                                    "edge_kind": str(row["edge_kind"] or ""),
                                    "file": str(row["from_file"] or ""),
                                    "line": int(row["from_line"] or 0),
                                    "evidence": str(row["evidence"] or ""),
                                }
                            )
            except (OSError, sqlite3.Error):
                messages = []
            message_index[entry.display_name] = messages
        return message_index

    def _repo_artifact_index(
        self,
        *,
        key: str,
        entries: list[RepositoryEntry],
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        artifact_index: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            artifacts: list[dict[str, Any]] = []
            try:
                self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
                with sqlite3.connect(self._index_path(repo_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    for row in connection.execute(
                        """
                        select edge_kind, to_name, evidence, from_file, from_line
                        from flow_edges
                        where edge_kind = 'module_dependency'
                          and (evidence = to_name or from_file like '%pom.xml' or from_file like '%package.json')
                        limit 300
                        """
                    ):
                        artifacts.append(
                            {
                                "artifact": str(row["to_name"] or ""),
                                "file": str(row["from_file"] or ""),
                                "line": int(row["from_line"] or 0),
                                "evidence": str(row["evidence"] or ""),
                            }
                        )
                    for row in connection.execute(
                        """
                        select edge_kind, to_name, evidence, from_file, from_line
                        from flow_edges
                        where edge_kind = 'module_dependency'
                        limit 300
                        """
                    ):
                        if str(row["evidence"] or "") != str(row["to_name"] or ""):
                            continue
                        artifacts.append(
                            {
                                "artifact": str(row["to_name"] or ""),
                                "file": str(row["from_file"] or ""),
                                "line": int(row["from_line"] or 0),
                                "evidence": str(row["evidence"] or ""),
                            }
                        )
                    for row in connection.execute(
                        """
                        select edge_kind, to_name, evidence, from_file, from_line
                        from flow_edges
                        where edge_kind = 'module_dependency'
                           or edge_kind = 'module_artifact'
                        limit 300
                        """
                    ):
                        artifacts.append(
                            {
                                "artifact": str(row["to_name"] or ""),
                                "file": str(row["from_file"] or ""),
                                "line": int(row["from_line"] or 0),
                                "evidence": str(row["evidence"] or ""),
                            }
                        )
            except (OSError, sqlite3.Error):
                artifacts = []
            by_key: dict[str, dict[str, Any]] = {}
            for item in artifacts:
                normalized = self._normalize_artifact_name(str(item.get("artifact") or ""))
                if normalized and normalized not in by_key:
                    by_key[normalized] = item
            artifact_index[entry.display_name] = list(by_key.values())
        return artifact_index

    def _repo_table_index(
        self,
        *,
        key: str,
        entries: list[RepositoryEntry],
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        table_index: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            tables: list[dict[str, Any]] = []
            try:
                self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
                with sqlite3.connect(self._index_path(repo_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    for row in connection.execute(
                        """
                        select edge_kind, to_name, evidence, from_file, from_line
                        from flow_edges
                        where edge_kind in ('db_read', 'db_write')
                        limit 300
                        """
                    ):
                        tables.append(
                            {
                                "table": str(row["to_name"] or ""),
                                "edge_kind": str(row["edge_kind"] or ""),
                                "file": str(row["from_file"] or ""),
                                "line": int(row["from_line"] or 0),
                                "evidence": str(row["evidence"] or ""),
                            }
                        )
            except (OSError, sqlite3.Error):
                tables = []
            table_index[entry.display_name] = tables
        return table_index

    def _repo_route_index(
        self,
        *,
        key: str,
        entries: list[RepositoryEntry],
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        route_index: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            routes: list[dict[str, Any]] = []
            try:
                self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
                with sqlite3.connect(self._index_path(repo_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    for row in connection.execute(
                        """
                        select edge_kind, to_name, evidence, from_file, from_line
                        from flow_edges
                        where edge_kind = 'route'
                        limit 300
                        """
                    ):
                        routes.append(
                            {
                                "route": str(row["to_name"] or ""),
                                "file": str(row["from_file"] or ""),
                                "line": int(row["from_line"] or 0),
                                "evidence": str(row["evidence"] or ""),
                            }
                        )
            except (OSError, sqlite3.Error):
                routes = []
            route_index[entry.display_name] = self._prefer_specific_routes(routes)
        return route_index

    def _repo_config_index(
        self,
        *,
        key: str,
        entries: list[RepositoryEntry],
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, list[str]]]:
        config_index: dict[str, dict[str, list[str]]] = {}
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            values: dict[str, list[str]] = {}
            yaml_stacks: dict[str, list[tuple[int, str]]] = {}
            try:
                self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
                with sqlite3.connect(self._index_path(repo_path)) as connection:
                    connection.row_factory = sqlite3.Row
                    for row in connection.execute(
                        """
                        select file_path, line_no, line_text
                        from lines
                        where lower(file_path) glob '*.properties'
                           or lower(file_path) glob '*.yml'
                           or lower(file_path) glob '*.yaml'
                           or lower(file_path) glob '*.conf'
                           or lower(file_path) glob '*.toml'
                        order by file_path, line_no
                        limit 1000
                        """
                    ):
                        file_path = str(row["file_path"] or "")
                        suffix = Path(file_path).suffix.lower()
                        yaml_stack = yaml_stacks.setdefault(file_path, [])
                        pair = (
                            self._extract_yaml_config_assignment(str(row["line_text"] or ""), yaml_stack)
                            if suffix in {".yaml", ".yml"}
                            else self._extract_config_assignment(str(row["line_text"] or ""))
                        )
                        if not pair:
                            continue
                        key_name, value = pair
                        if not value:
                            continue
                        values.setdefault(key_name, [])
                        if value not in values[key_name]:
                            values[key_name].append(value)
            except (OSError, sqlite3.Error):
                values = {}
            config_index[entry.display_name] = values
        return config_index

    def _match_repo_dependency_candidate(
        self,
        *,
        row: dict[str, Any],
        entries: list[RepositoryEntry],
        source_name: str,
        route_index: dict[str, list[dict[str, Any]]],
        message_index: dict[str, list[dict[str, Any]]] | None = None,
        artifact_index: dict[str, list[dict[str, Any]]] | None = None,
        table_index: dict[str, list[dict[str, Any]]] | None = None,
        source_config: dict[str, list[str]] | None = None,
    ) -> dict[str, Any] | None:
        value = str(row.get("to_name") or row.get("evidence") or "")
        evidence = str(row.get("evidence") or "")
        search_text = f"{value} {evidence}"
        best: dict[str, Any] | None = None
        source_role = self._flow_role_for_path(str(row.get("from_file") or ""))
        row_kind = str(row.get("edge_kind") or "")
        lowered_search_text = search_text.lower()

        http_client_like = source_role == "client" or any(
            marker in lowered_search_text for marker in ("feignclient", "fetch", "axios", "resttemplate", "webclient")
        )
        resolved_config_values = self._resolve_config_placeholders(search_text, source_config or {})
        if http_client_like:
            resolved_config_values.extend(self._candidate_dependency_config_values(source_config or {}))
            resolved_config_values = list(dict.fromkeys(resolved_config_values))[:20]
        if resolved_config_values:
            search_text = " ".join([search_text, *resolved_config_values, *self._join_config_routes(search_text, resolved_config_values)])
            lowered_search_text = search_text.lower()
        if http_client_like:
            for route in self._extract_route_literals(search_text):
                for entry in entries:
                    if entry.display_name == source_name:
                        continue
                    for target_route in route_index.get(entry.display_name) or []:
                        score = self._route_overlap_score(route, str(target_route.get("route") or ""))
                        if score <= 0:
                            continue
                        candidate = {
                            "target": entry,
                            "edge_kind": "http_path",
                            "confidence": score,
                            "match_reason": f"http path overlap: {route} -> {target_route.get('route')}",
                            "from_route": route,
                            "target_route": target_route.get("route") or "",
                            "to_file": target_route.get("file") or "",
                            "to_line": int(target_route.get("line") or 0),
                        }
                        best = self._better_repo_dependency_candidate(best, candidate)

        if row_kind in {"module_dependency"}:
            source_artifacts = [self._normalize_artifact_name(item) for item in self._artifact_values_from_text(search_text)]
            source_artifacts = [item for item in dict.fromkeys(source_artifacts) if item]
            for source_artifact in source_artifacts:
                for entry in entries:
                    if entry.display_name == source_name:
                        continue
                    for target_artifact in (artifact_index or {}).get(entry.display_name) or []:
                        target_name = str(target_artifact.get("artifact") or "")
                        if source_artifact != self._normalize_artifact_name(target_name):
                            continue
                        candidate = {
                            "target": entry,
                            "edge_kind": "module_dependency",
                            "confidence": 0.97,
                            "match_reason": f"exact build artifact match: {source_artifact}",
                            "to_file": target_artifact.get("file") or "",
                            "to_line": int(target_artifact.get("line") or 0),
                        }
                        best = self._better_repo_dependency_candidate(best, candidate)

        if row_kind in {"db_write"}:
            source_table = self._normalize_table_name(value)
            if source_table:
                for entry in entries:
                    if entry.display_name == source_name:
                        continue
                    for target_table in (table_index or {}).get(entry.display_name) or []:
                        if str(target_table.get("edge_kind") or "") != "db_read":
                            continue
                        target_name = str(target_table.get("table") or "")
                        if source_table != self._normalize_table_name(target_name):
                            continue
                        candidate = {
                            "target": entry,
                            "edge_kind": "shared_table",
                            "confidence": 0.86,
                            "match_reason": f"db write/read table overlap: {value} -> {target_name}",
                            "to_file": target_table.get("file") or "",
                            "to_line": int(target_table.get("line") or 0),
                        }
                        best = self._better_repo_dependency_candidate(best, candidate)

        if row_kind in {"message_publish", "event_publish"}:
            source_messages = self._message_values_from_config(value, source_config or {})
            for source_message_value in source_messages:
                source_message = self._normalize_message_name(source_message_value)
                if not source_message:
                    continue
                for entry in entries:
                    if entry.display_name == source_name:
                        continue
                    for target_message in (message_index or {}).get(entry.display_name) or []:
                        target_name = str(target_message.get("message") or "")
                        if source_message != self._normalize_message_name(target_name):
                            continue
                        candidate = {
                            "target": entry,
                            "edge_kind": "message_topic" if row_kind == "message_publish" else "event_flow",
                            "confidence": 0.93,
                            "match_reason": f"{row_kind} matches consumer: {source_message_value} -> {target_name}",
                            "to_file": target_message.get("file") or "",
                            "to_line": int(target_message.get("line") or 0),
                        }
                        best = self._better_repo_dependency_candidate(best, candidate)

        alias_client_like = http_client_like or row_kind in {"import", "module_dependency"}
        if alias_client_like:
            for entry in entries:
                if entry.display_name == source_name:
                    continue
                alias_score = self._repo_alias_match_score(search_text, entry)
                if alias_score <= 0:
                    continue
                candidate = {
                    "target": entry,
                    "edge_kind": str(row.get("edge_kind") or "dependency"),
                    "confidence": alias_score,
                    "match_reason": "build dependency alias match" if row_kind == "module_dependency" else "service/import alias match",
                    "to_file": "",
                    "to_line": 0,
                }
                best = self._better_repo_dependency_candidate(best, candidate)

        return best

    @staticmethod
    def _normalize_message_name(value: str) -> str:
        normalized = str(value or "").strip().lower()
        normalized = re.sub(r"^\$\{([^}:]+).*$", r"\1", normalized)
        return re.sub(r"[^a-z0-9_.:-]+", "", normalized)

    @staticmethod
    def _message_values_from_config(value: str, config_values: dict[str, list[str]]) -> list[str]:
        raw_value = str(value or "").strip()
        values: list[str] = []
        for key in CONFIG_PLACEHOLDER_PATTERN.findall(str(value or "")):
            values.extend(config_values.get(key, [])[:5])
        values.extend(config_values.get(raw_value, [])[:5])
        values.append(raw_value)
        return list(dict.fromkeys(item for item in values if item))[:8]

    @staticmethod
    def _artifact_values_from_text(value: str) -> list[str]:
        text = str(value or "")
        artifacts: list[str] = []
        for coordinate in re.findall(r"([A-Za-z0-9_.@/-]+:[A-Za-z0-9_.@/-]+)", text):
            artifacts.append(coordinate)
            artifacts.append(coordinate.rsplit(":", 1)[-1])
        for package_name in re.findall(r"@[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", text):
            artifacts.append(package_name)
        for token in re.findall(r"\b[A-Za-z0-9_.-]+(?:-api|-sdk|-client|-service)\b", text):
            artifacts.append(token)
        return list(dict.fromkeys(item.strip() for item in artifacts if item.strip()))[:12]

    @staticmethod
    def _normalize_artifact_name(value: str) -> str:
        normalized = str(value or "").strip().lower()
        if ":" in normalized:
            normalized = normalized.rsplit(":", 1)[-1]
        if "/" in normalized:
            normalized = normalized.rsplit("/", 1)[-1]
        return re.sub(r"[^a-z0-9_.-]+", "", normalized)

    @staticmethod
    def _normalize_table_name(value: str) -> str:
        normalized = str(value or "").strip().lower()
        normalized = normalized.rsplit(".", 1)[-1]
        return re.sub(r"[^a-z0-9_]+", "", normalized)

    @staticmethod
    def _better_repo_dependency_candidate(current: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
        if current is None:
            return candidate
        candidate_confidence = float(candidate.get("confidence") or 0)
        current_confidence = float(current.get("confidence") or 0)
        if candidate_confidence > current_confidence:
            return candidate
        if candidate_confidence == current_confidence:
            candidate_specificity = SourceCodeQAService._route_segment_count(str(candidate.get("from_route") or "")) + SourceCodeQAService._route_segment_count(str(candidate.get("target_route") or ""))
            current_specificity = SourceCodeQAService._route_segment_count(str(current.get("from_route") or "")) + SourceCodeQAService._route_segment_count(str(current.get("target_route") or ""))
            if candidate_specificity > current_specificity:
                return candidate
        return current

    @staticmethod
    def _extract_route_literals(value: str) -> list[str]:
        routes = []
        for route in re.findall(r"https?://[^\s\"'<>),]+", str(value or "")):
            if route not in routes:
                routes.append(route)
        for route in HTTP_LITERAL_PATTERN.findall(str(value or "")):
            if route not in routes:
                routes.append(route)
        for route in re.findall(r"(?<![A-Za-z0-9_])/[A-Za-z0-9_./{}:-]{2,}", str(value or "")):
            if route not in routes:
                routes.append(route)
        return routes[:12]

    @staticmethod
    def _prefer_specific_routes(routes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        preferred: list[dict[str, Any]] = []
        route_values = [str(route.get("route") or "") for route in routes]
        for route in routes:
            value = str(route.get("route") or "")
            normalized = "/" + value.strip("/")
            if SourceCodeQAService._route_segment_count(normalized) <= 1 and any(
                SourceCodeQAService._route_segment_count(other) > 1
                and ("/" + str(other).strip("/")).endswith(normalized)
                for other in route_values
                if str(other or "") != value
            ):
                continue
            preferred.append(route)
        return preferred

    @staticmethod
    def _route_segment_count(route: str) -> int:
        return len([part for part in SourceCodeQAService._route_path(route).split("/") if part])

    @staticmethod
    def _join_config_routes(search_text: str, config_values: list[str]) -> list[str]:
        joined: list[str] = []
        relative_routes = [
            route
            for route in SourceCodeQAService._extract_route_literals(search_text)
            if route.startswith("/") and SourceCodeQAService._route_segment_count(route) <= 2
        ]
        for value in config_values:
            parsed = urlsplit(str(value or ""))
            base_path = parsed.path if parsed.scheme else str(value or "")
            if not base_path:
                continue
            for route in relative_routes:
                combined = SourceCodeQAService._join_routes(base_path, route)
                if combined and combined not in joined:
                    joined.append(combined)
        return joined[:12]

    @staticmethod
    def _resolve_config_placeholders(value: str, config_values: dict[str, list[str]]) -> list[str]:
        resolved: list[str] = []
        for key in CONFIG_PLACEHOLDER_PATTERN.findall(str(value or "")):
            for candidate in config_values.get(key, [])[:5]:
                if candidate and candidate not in resolved:
                    resolved.append(candidate)
        return resolved[:12]

    @staticmethod
    def _candidate_dependency_config_values(config_values: dict[str, list[str]]) -> list[str]:
        values: list[str] = []
        for key, candidates in config_values.items():
            lowered_key = str(key or "").lower()
            if not any(marker in lowered_key for marker in ("url", "uri", "endpoint", "host", "service", "client")):
                continue
            for candidate in candidates[:5]:
                lowered = str(candidate or "").lower()
                if candidate and ("http" in lowered or "/" in lowered or "-service" in lowered):
                    values.append(candidate)
        return values[:20]

    @staticmethod
    def _route_overlap_score(left: str, right: str) -> float:
        left_norm = SourceCodeQAService._route_path(left)
        right_norm = SourceCodeQAService._route_path(right)
        if len(left_norm) < 3 or len(right_norm) < 3:
            return 0.0
        if left_norm == right_norm:
            return 0.96
        if left_norm.endswith(right_norm) or right_norm.endswith(left_norm):
            return 0.88
        left_parts = {part for part in left_norm.lower().split("/") if part and not part.startswith("{")}
        right_parts = {part for part in right_norm.lower().split("/") if part and not part.startswith("{")}
        if not left_parts or not right_parts:
            return 0.0
        overlap = left_parts & right_parts
        if len(overlap) >= 2:
            return 0.78
        return 0.0

    @staticmethod
    def _route_path(route: str) -> str:
        value = str(route or "").split("?", 1)[0].strip()
        parsed = urlsplit(value)
        if parsed.scheme and parsed.path:
            value = parsed.path
        return "/" + value.strip("/")

    @staticmethod
    def _join_routes(prefix: str, suffix: str) -> str:
        prefix = str(prefix or "").split("?", 1)[0].strip()
        suffix = str(suffix or "").split("?", 1)[0].strip()
        if not prefix:
            return suffix
        if not suffix:
            return prefix
        if prefix.startswith("http") or suffix.startswith("http"):
            return suffix if suffix.startswith("http") else prefix.rstrip("/") + "/" + suffix.lstrip("/")
        return "/" + "/".join(part.strip("/") for part in (prefix, suffix) if part.strip("/"))

    @staticmethod
    def _repo_alias_match_score(value: str, entry: RepositoryEntry) -> float:
        normalized_value = re.sub(r"[^a-z0-9]+", "", str(value or "").lower())
        if not normalized_value:
            return 0.0
        aliases = {
            re.sub(r"[^a-z0-9]+", "", entry.display_name.lower()),
            re.sub(r"[^a-z0-9]+", "", SourceCodeQAService._derive_display_name(entry.url).lower()),
        }
        ignored_parts = {"service", "repo", "repository", "portal", "client", "api", "team"}
        for raw_part in re.split(r"[^A-Za-z0-9]+", entry.display_name):
            normalized_part = raw_part.lower()
            if len(normalized_part) >= 8 and normalized_part not in ignored_parts:
                aliases.add(normalized_part)
        for candidate in aliases:
            if len(candidate) >= 4 and candidate in normalized_value:
                return 0.84
            if len(candidate) >= 6 and normalized_value in candidate:
                return 0.72
        return 0.0

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
        edge_rank = {
            "sql_table": 0,
            "client": 1,
            "mapper": 2,
            "dao": 3,
            "repository": 4,
            "field_population": 5,
            "service": 6,
            "route": 7,
        }
        rows: list[sqlite3.Row] = []
        seen: set[tuple[str, int, str, int, str]] = set()
        for clause, params in (
            ("from_file = ?", (seed_path,)),
            ("to_file = ? and from_file <> ?", (seed_path, seed_path)),
        ):
            for row in connection.execute(
                f"select * from flow_edges where {clause} limit 30",
                params,
            ).fetchall():
                key = (
                    str(row["from_file"] or ""),
                    int(row["from_line"] or 0),
                    str(row["to_file"] or ""),
                    int(row["to_line"] or 0),
                    str(row["edge_kind"] or ""),
                )
                if key in seen:
                    continue
                rows.append(row)
                seen.add(key)
        rows.sort(key=lambda row: (edge_rank.get(str(row["edge_kind"] or ""), 7), int(row["from_line"] or 0)))
        rows = rows[:30]
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
        return {
            "mode": "local_agentic_retrieval",
            "recipe_version": 3,
            "status": "planned" if deduped_steps else "not_needed",
            "intent": intent,
            "steps": deduped_steps[:6],
        }

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
            collected = self._select_result_matches(collected, max(1, min(int(limit or 12), 30)), question=question)
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
        return {
            "data_source": data_source_intent,
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
        pack: dict[str, Any] = {
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

    def _select_result_matches(self, matches: list[dict[str, Any]], limit: int, *, question: str = "") -> list[dict[str, Any]]:
        limit = max(1, int(limit or 1))
        intent = self._question_intent(question) if question else {}
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

        def result_sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
            retrieval = str(item.get("retrieval") or "")
            trace_stage = str(item.get("trace_stage") or "")
            path = str(item.get("path") or "").lower()
            snippet = str(item.get("snippet") or "").lower()
            lowered_question = str(question or "").lower()
            priority = 0
            if trace_stage == "exact_lookup" or retrieval == "exact_table_path_lookup":
                priority = max(priority, 70)
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

        if any(intent.get(key) for key in ("data_source", "static_qa", "test_coverage", "operational_boundary", "module_dependency", "message_flow")) and not intent.get("impact_analysis"):
            selected.sort(key=result_sort_key, reverse=True)
        else:
            selected.sort(key=lambda item: item["score"], reverse=True)
        return selected

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
        followup_context: dict[str, Any] | None = None,
        requested_answer_mode: str = ANSWER_MODE_GEMINI,
        request_cache: dict[str, Any] | None = None,
        progress_callback: Any | None = None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if not self.llm_ready():
            raise ToolError(self.llm_unavailable_message())
        routed_budget_mode, budget, llm_route = self._resolve_llm_budget(llm_budget_mode, question, matches)
        selected_model = self._model_for_role_or_budget("answer", budget)
        selected_matches = self._select_llm_matches(matches, int(budget["match_limit"]), question=question)
        evidence_summary = self._compress_evidence_cached(question, selected_matches, request_cache=request_cache)
        trace_paths = self._build_trace_paths(entries=entries, key=key, matches=selected_matches, question=question, request_cache=request_cache)
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
        if self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
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
            )
        attachment_section = self._context_attachment_section(attachments or [], runtime_evidence or [])
        domain_context = self._llm_domain_context(
            pm_team=pm_team,
            country=country,
            question=question,
            evidence_summary=evidence_summary,
        )
        answer_thinking_budget = self._thinking_budget_for_call(
            role="answer",
            budget_mode=routed_budget_mode,
            budget=budget,
            quality_gate=quality_gate,
        )
        answer_thinking_budget = self._normalize_thinking_budget_for_provider(answer_thinking_budget)
        answer_thinking_config = self._thinking_config_for_provider(
            answer_thinking_budget,
            model=selected_model,
            role="answer",
            budget_mode=routed_budget_mode,
        )
        llm_route = {
            **llm_route,
            "answer_model": selected_model,
            "thinking_budget": answer_thinking_budget,
            "thinking_config": answer_thinking_config,
        }
        prompt_context = self._build_compressed_llm_context(
            evidence_summary,
            quality_gate,
            evidence_pack,
            selected_matches,
            domain_context=domain_context,
            snippet_line_budget=budget["snippet_line_budget"],
            snippet_char_budget=budget["snippet_char_budget"],
        )
        initial_prompt_tokens = self._estimate_llm_tokens(
            self._llm_user_prompt(pm_team=pm_team, country=country, question=question, context=prompt_context, attachment_section=attachment_section)
        )
        token_pressure = self._llm_prompt_pressure_for_provider(initial_prompt_tokens)
        if token_pressure != "normal" and routed_budget_mode in {"balanced", "deep"}:
            original_budget_mode = routed_budget_mode
            routed_budget_mode = COMPACT_DEEP_BUDGET_MODE
            budget = self.llm_budgets[COMPACT_DEEP_BUDGET_MODE]
            selected_model = self._model_for_role_or_budget("answer", budget)
            selected_matches = self._select_llm_matches(matches, int(budget["match_limit"]), question=question)
            evidence_summary = self._compress_evidence_cached(question, selected_matches, request_cache=request_cache)
            trace_paths = self._build_trace_paths(entries=entries, key=key, matches=selected_matches, question=question, request_cache=request_cache)
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
            answer_thinking_budget = self._thinking_budget_for_call(
                role="answer",
                budget_mode=routed_budget_mode,
                budget=budget,
                quality_gate=quality_gate,
            )
            if token_pressure == "tight":
                answer_thinking_budget = 0
            answer_thinking_budget = self._normalize_thinking_budget_for_provider(answer_thinking_budget)
            answer_thinking_config = self._thinking_config_for_provider(
                answer_thinking_budget,
                model=selected_model,
                role="answer",
                budget_mode=routed_budget_mode,
            )
            llm_route = {
                **llm_route,
                "selected": routed_budget_mode,
                "reason": f"{llm_route.get('reason') or ''},token_pressure_{token_pressure}".strip(","),
                "original_budget": original_budget_mode,
                "answer_model": selected_model,
                "thinking_budget": answer_thinking_budget,
                "thinking_config": answer_thinking_config,
            }
            prompt_context = self._build_compressed_llm_context(
                evidence_summary,
                quality_gate,
                evidence_pack,
                selected_matches,
                domain_context=domain_context,
                snippet_line_budget=budget["snippet_line_budget"],
                snippet_char_budget=budget["snippet_char_budget"],
                compact=True,
            )
            final_prompt_tokens = self._estimate_llm_tokens(
                self._llm_user_prompt(pm_team=pm_team, country=country, question=question, context=prompt_context, attachment_section=attachment_section)
            )
        else:
            final_prompt_tokens = initial_prompt_tokens
        if token_pressure != "normal":
            llm_route = {
                **llm_route,
                "token_pressure": {
                    "status": token_pressure,
                    "initial_estimated_prompt_tokens": initial_prompt_tokens,
                    "final_estimated_prompt_tokens": final_prompt_tokens,
                    "compact_threshold": LLM_PROMPT_COMPACT_THRESHOLD_TOKENS,
                    "tight_threshold": LLM_PROMPT_TIGHT_THRESHOLD_TOKENS,
                },
            }
        answer_max_output_tokens = int(budget["max_output_tokens"])
        if routed_budget_mode == COMPACT_DEEP_BUDGET_MODE and token_pressure == "tight":
            answer_max_output_tokens = max(answer_max_output_tokens, 2_400)
        vertex_two_pass = self.llm_provider_name == LLM_PROVIDER_VERTEX_AI
        cache_context = f"{prompt_context}\n\n{attachment_section}" if attachment_section else prompt_context
        cache_key = self._answer_cache_key(
            provider=self.llm_provider.name,
            model=selected_model,
            question=question,
            answer_mode=requested_answer_mode,
            llm_budget_mode=routed_budget_mode,
            context=cache_context,
        )
        cached = None if vertex_two_pass else self._load_cached_answer(cache_key)
        if cached is not None:
            cached_answer = str(cached["answer"])
            cached_structured = self._parse_structured_answer(cached_answer)
            if self._trust_provider_final_answer():
                cached_claim_check = self._trusted_provider_check()
                cached_judge = self._trusted_provider_judge()
                cached_final = self._finalize_trusted_model_answer(
                    question=question,
                    answer=cached_answer,
                    structured_answer=cached_structured,
                    evidence_summary=evidence_summary,
                    quality_gate=cached.get("answer_quality") or quality_gate,
                    claim_check=cached_claim_check,
                    answer_judge=cached_judge,
                    finish_reason=cached.get("finish_reason") or "cache_hit",
                    selected_matches=selected_matches,
                )
            else:
                cached_claim_check = self._verify_answer_claims(cached_answer, evidence_summary, selected_matches)
                cached_judge = self._run_answer_judge(question, cached_answer, evidence_pack, cached_claim_check)
                cached_final = self._finalize_llm_answer(
                    question=question,
                    answer=cached_answer,
                    structured_answer=cached_structured,
                    evidence_summary=evidence_summary,
                    quality_gate=cached.get("answer_quality") or quality_gate,
                    claim_check=cached_claim_check,
                    answer_judge=cached_judge,
                    finish_reason=cached.get("finish_reason") or "cache_hit",
                    selected_matches=selected_matches,
                )
            return {
                "llm_answer": cached_final["answer"],
                "llm_budget_mode": routed_budget_mode,
                "llm_requested_budget_mode": llm_budget_mode,
                "llm_route": llm_route,
                "llm_provider": cached.get("provider") or self.llm_provider.name,
                "llm_model": cached.get("model") or selected_model,
                "llm_cached": True,
                "llm_usage": self._normalize_llm_usage(cached.get("usage") or {}),
                "llm_thinking_budget": cached.get("thinking_budget", answer_thinking_budget),
                "llm_latency_ms": 0,
                "llm_attempt_log": [],
                "llm_finish_reason": cached.get("finish_reason") or "cache_hit",
                "answer_quality": cached.get("answer_quality") or quality_gate,
                "answer_claim_check": cached_claim_check,
                "answer_judge": cached_judge,
                "structured_answer": cached_final["structured_answer"],
                "answer_contract": cached_final["answer_contract"],
                "evidence_pack": evidence_pack,
            }
        draft_answer = ""
        draft_usage: dict[str, Any] = {}
        draft_attempts = 0
        draft_latency_ms = 0
        draft_attempt_log: list[dict[str, Any]] = []
        vertex_draft_check: dict[str, Any] | None = None
        vertex_draft_claim_check: dict[str, Any] | None = None
        vertex_draft_judge: dict[str, Any] | None = None
        if vertex_two_pass:
            draft_payload = {
                "contents": [
                    {
                        "parts": self._llm_payload_parts(
                            self._vertex_draft_prompt(
                                pm_team=pm_team,
                                country=country,
                                question=question,
                                context=prompt_context,
                                attachment_section=attachment_section,
                            ),
                            attachments or [],
                        )
                    }
                ],
                "systemInstruction": {
                    "parts": [
                        {
                            "text": self._llm_system_instruction()
                        }
                    ]
                },
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": max(1_800, min(answer_max_output_tokens, 3_600)),
                    "thinkingConfig": answer_thinking_config,
                },
            }
            draft_result = self.llm_provider.generate(
                payload=draft_payload,
                primary_model=selected_model,
                fallback_model=self._llm_fallback_model(),
            )
            draft_answer = self.llm_provider.extract_text(draft_result.payload)
            draft_usage = self._normalize_llm_usage(draft_result.usage or draft_result.payload.get("usageMetadata") or {})
            draft_attempts = draft_result.attempts
            draft_latency_ms = int(draft_result.latency_ms or 0)
            draft_attempt_log = [dict(item) for item in draft_result.attempt_log]
            vertex_draft_check = self._answer_self_check(question, draft_answer, evidence_summary, quality_gate)
            vertex_draft_claim_check = self._verify_answer_claims(draft_answer, evidence_summary, selected_matches)
            vertex_draft_judge = self._run_answer_judge(question, draft_answer, evidence_pack, vertex_draft_claim_check)
            vertex_retry_matches = self._expand_answer_retry_matches(
                entries=entries,
                key=key,
                question=question,
                matches=matches,
                draft_answer=draft_answer,
                answer_check=vertex_draft_check,
                claim_check=vertex_draft_claim_check,
                answer_judge=vertex_draft_judge,
                evidence_summary=evidence_summary,
                quality_gate=quality_gate,
                limit=int(budget["match_limit"]) + 10,
                request_cache=request_cache,
            )
            if vertex_retry_matches:
                retry_match_limit = int(budget["match_limit"]) + 6
                selected_matches = self._select_llm_matches(vertex_retry_matches, retry_match_limit, question=question)
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
                prompt_context = self._build_compressed_llm_context(
                    evidence_summary,
                    quality_gate,
                    evidence_pack,
                    selected_matches,
                    domain_context=domain_context,
                    snippet_line_budget=min(int(budget["snippet_line_budget"]) + 120, 360),
                    snippet_char_budget=min(int(budget["snippet_char_budget"]) + 24_000, 120_000),
                )
                llm_route = {
                    **llm_route,
                    "vertex_second_pass": True,
                    "vertex_second_pass_match_count": len(selected_matches),
                }
            llm_route = {
                **llm_route,
                "vertex_two_pass": True,
                "vertex_draft_model": draft_result.model,
                "vertex_final_schema": True,
            }
            cache_key = self._answer_cache_key(
                provider=self.llm_provider.name,
                model=selected_model,
                question=question,
                answer_mode=requested_answer_mode,
                llm_budget_mode=routed_budget_mode,
                context=f"{prompt_context}\n\n{attachment_section}" if attachment_section else prompt_context,
            )
        final_prompt = self._llm_user_prompt(
            pm_team=pm_team,
            country=country,
            question=question,
            context=prompt_context,
            self_check=vertex_draft_check if vertex_two_pass else None,
            attachment_section=attachment_section,
        )
        if vertex_two_pass:
            final_prompt = self._vertex_final_prompt(final_prompt=final_prompt, draft_answer=draft_answer)
        payload = {
            "contents": [
                {
                    "parts": self._llm_payload_parts(final_prompt, attachments or [])
                }
            ],
            "systemInstruction": {
                "parts": [
                    {
                        "text": self._llm_system_instruction()
                    }
                ]
            },
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": answer_max_output_tokens,
                "responseMimeType": "application/json",
                "responseSchema": self._llm_answer_response_schema(),
                "thinkingConfig": answer_thinking_config,
            },
        }
        result = self.llm_provider.generate(
            payload=payload,
            primary_model=selected_model,
            fallback_model=self._llm_fallback_model(),
        )
        answer = self.llm_provider.extract_text(result.payload)
        structured_answer = self._parse_structured_answer(answer)
        result_usage = self._normalize_llm_usage(result.usage or result.payload.get("usageMetadata") or {})
        usage = self._merge_llm_usage(draft_usage, result_usage) if vertex_two_pass else result_usage
        effective_model = result.model
        attempts = draft_attempts + result.attempts
        llm_latency_ms = draft_latency_ms + int(result.latency_ms or 0)
        llm_attempt_log = [*draft_attempt_log, *[dict(item) for item in result.attempt_log]]
        finish_reason = self._llm_finish_reason(result.payload)
        answer_check = self._answer_self_check(question, answer, evidence_summary, quality_gate)
        token_limited_generation = self._finish_reason_is_token_limited(finish_reason)
        if self._trust_provider_final_answer():
            claim_check = self._trusted_provider_check()
            answer_judge = self._trusted_provider_judge()
            final = self._finalize_trusted_model_answer(
                question=question,
                answer=answer,
                structured_answer=structured_answer,
                evidence_summary=evidence_summary,
                quality_gate=quality_gate,
                claim_check=claim_check,
                answer_judge=answer_judge,
                finish_reason=finish_reason,
                selected_matches=selected_matches,
            )
            answer = final["answer"]
            structured_answer = final["structured_answer"]
            answer_contract = final["answer_contract"]
            self._store_cached_answer(
                cache_key,
                answer=answer,
                usage=usage,
                answer_quality=quality_gate,
                provider=self.llm_provider.name,
                model=effective_model,
                thinking_budget=llm_route.get("repair_thinking_budget", answer_thinking_budget),
                finish_reason=finish_reason,
            )
            return {
                "llm_answer": answer,
                "llm_budget_mode": routed_budget_mode,
                "llm_requested_budget_mode": llm_budget_mode,
                "llm_route": llm_route,
                "llm_provider": self.llm_provider.name,
                "llm_cached": False,
                "llm_usage": usage,
                "llm_model": effective_model,
                "llm_thinking_budget": llm_route.get("repair_thinking_budget", answer_thinking_budget),
                "llm_attempts": attempts,
                "llm_latency_ms": llm_latency_ms,
                "llm_attempt_log": llm_attempt_log,
                "llm_finish_reason": finish_reason,
                "answer_quality": quality_gate,
                "answer_self_check": answer_check,
                "answer_claim_check": claim_check,
                "answer_judge": answer_judge,
                "structured_answer": structured_answer,
                "answer_contract": answer_contract,
                "evidence_pack": evidence_pack,
            }
        if self._finish_reason_needs_generation_repair(finish_reason):
            issues = list(answer_check.get("issues") or [])
            issues.append(f"model finish reason requires repair: {finish_reason}")
            answer_check = {"status": "retry", "issues": list(dict.fromkeys(issues))}
        claim_check = self._verify_answer_claims(answer, evidence_summary, selected_matches)
        answer_judge = self._run_answer_judge(question, answer, evidence_pack, claim_check)
        if claim_check.get("status") != "ok" and answer_check.get("status") == "retry":
            issues = list(answer_check.get("issues") or [])
            issues.extend(claim_check.get("issues") or [])
            answer_check = {"status": "retry", "issues": list(dict.fromkeys(issues))}
        if answer_judge.get("status") == "repair":
            issues = list(answer_check.get("issues") or [])
            issues.extend(answer_judge.get("issues") or [])
            answer_check = {"status": "retry", "issues": list(dict.fromkeys(issues))}
        if answer_check.get("status") == "retry":
            if token_limited_generation:
                retry_matches = selected_matches[: max(4, min(int(budget["match_limit"]), 8))]
            else:
                retry_matches = self._expand_answer_retry_matches(
                    entries=entries,
                    key=key,
                    question=question,
                    matches=matches,
                    draft_answer=answer,
                    answer_check=answer_check,
                    claim_check=claim_check,
                    answer_judge=answer_judge,
                    evidence_summary=evidence_summary,
                    quality_gate=quality_gate,
                    limit=int(budget["match_limit"]) + 6,
                    request_cache=request_cache,
                )
            if retry_matches:
                retry_match_limit = max(4, min(int(budget["match_limit"]), 8)) if token_limited_generation else int(budget["match_limit"]) + 4
                retry_selected_matches = self._select_llm_matches(retry_matches, retry_match_limit, question=question)
                retry_evidence_summary = self._compress_evidence_cached(question, retry_selected_matches, request_cache=request_cache)
                retry_trace_paths = self._build_trace_paths(
                    entries=entries,
                    key=key,
                    matches=retry_selected_matches,
                    question=question,
                    request_cache=request_cache,
                )
                if retry_trace_paths:
                    retry_evidence_summary["trace_paths"] = retry_trace_paths
                retry_quality_gate = self._quality_gate_cached(question, retry_evidence_summary, request_cache=request_cache)
                retry_evidence_pack = self._build_evidence_pack(
                    question=question,
                    evidence_summary=retry_evidence_summary,
                    matches=retry_selected_matches,
                    trace_paths=retry_trace_paths,
                    quality_gate=retry_quality_gate,
                )
                retry_domain_context = self._llm_domain_context(
                    pm_team=pm_team,
                    country=country,
                    question=question,
                    evidence_summary=retry_evidence_summary,
                )
                if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI and not token_limited_generation:
                    retry_snippet_line_budget = min(int(budget["snippet_line_budget"]) + 100, 320)
                    retry_snippet_char_budget = min(int(budget["snippet_char_budget"]) + 16_000, 100_000)
                else:
                    retry_snippet_line_budget = 45 if token_limited_generation else min(int(budget["snippet_line_budget"]) + 60, 180)
                    retry_snippet_char_budget = 6_000 if token_limited_generation else min(int(budget["snippet_char_budget"]) + 8000, 28_000)
                retry_context = self._build_compressed_llm_context(
                    retry_evidence_summary,
                    retry_quality_gate,
                    retry_evidence_pack,
                    retry_selected_matches,
                    domain_context=retry_domain_context,
                    snippet_line_budget=retry_snippet_line_budget,
                    snippet_char_budget=retry_snippet_char_budget,
                    compact=token_limited_generation,
                )
                retry_payload = dict(payload)
                retry_thinking_budget = self._thinking_budget_for_call(
                    role="repair",
                    budget_mode="deep",
                    budget=self.llm_budgets.get("deep") or budget,
                    quality_gate=retry_quality_gate,
                    retry=True,
                )
                retry_thinking_budget = 0 if token_limited_generation else self._normalize_thinking_budget_for_provider(retry_thinking_budget)
                retry_model = self._model_for_role_or_budget("repair", self.llm_budgets.get("deep") or budget)
                if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
                    retry_max_output_tokens = 2_400 if token_limited_generation else min(max(int(budget["max_output_tokens"]) + 800, 3_200), 5_600)
                else:
                    retry_max_output_tokens = 2_400 if token_limited_generation else min(max(int(budget["max_output_tokens"]) + 500, 900), 1_600)
                retry_payload["generationConfig"] = {
                    **payload["generationConfig"],
                    "maxOutputTokens": retry_max_output_tokens,
                    "thinkingConfig": self._thinking_config_for_provider(
                        retry_thinking_budget,
                        model=retry_model,
                        role="repair",
                        budget_mode="deep",
                    ),
                }
                retry_payload["contents"] = [
                    {
                        "parts": self._llm_payload_parts(
                            self._llm_user_prompt(
                                pm_team=pm_team,
                                country=country,
                                question=question,
                                context=retry_context,
                                self_check=answer_check,
                                attachment_section=attachment_section,
                            ),
                            attachments or [],
                        )
                    }
                ]
                retry_result = self.llm_provider.generate(
                    payload=retry_payload,
                    primary_model=retry_model,
                    fallback_model=self._llm_fallback_model(),
                )
                retry_answer = self.llm_provider.extract_text(retry_result.payload)
                retry_structured_answer = self._parse_structured_answer(retry_answer)
                retry_finish_reason = self._llm_finish_reason(retry_result.payload)
                retry_check = self._answer_self_check(question, retry_answer, retry_evidence_summary, retry_quality_gate)
                if self._finish_reason_needs_generation_repair(retry_finish_reason):
                    issues = list(retry_check.get("issues") or [])
                    issues.append(f"model finish reason requires caution: {retry_finish_reason}")
                    retry_check = {"status": "retry", "issues": list(dict.fromkeys(issues))}
                retry_claim_check = self._verify_answer_claims(retry_answer, retry_evidence_summary, retry_selected_matches)
                retry_judge = self._run_answer_judge(question, retry_answer, retry_evidence_pack, retry_claim_check)
                if retry_claim_check.get("status") != "ok":
                    issues = list(retry_check.get("issues") or [])
                    issues.extend(retry_claim_check.get("issues") or [])
                    retry_check = {"status": "retry", "issues": list(dict.fromkeys(issues))}
                if retry_judge.get("status") == "repair":
                    issues = list(retry_check.get("issues") or [])
                    issues.extend(retry_judge.get("issues") or [])
                    retry_check = {"status": "retry", "issues": list(dict.fromkeys(issues))}
                answer = retry_answer
                structured_answer = retry_structured_answer
                retry_usage = self._normalize_llm_usage(retry_result.usage or retry_result.payload.get("usageMetadata") or {})
                usage = self._merge_llm_usage(usage, retry_usage)
                effective_model = retry_result.model
                attempts += retry_result.attempts
                llm_latency_ms += int(retry_result.latency_ms or 0)
                llm_attempt_log.extend(dict(item) for item in retry_result.attempt_log)
                finish_reason = retry_finish_reason
                evidence_summary = retry_evidence_summary
                quality_gate = retry_quality_gate
                evidence_pack = retry_evidence_pack
                answer_check = retry_check
                claim_check = retry_claim_check
                answer_judge = retry_judge
                selected_matches = retry_selected_matches
                prompt_context = retry_context
                llm_route = {
                    **llm_route,
                    "repair_model": effective_model,
                    "repair_thinking_budget": retry_thinking_budget,
                }
        final = self._finalize_llm_answer(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            claim_check=claim_check,
            answer_judge=answer_judge,
            finish_reason=finish_reason,
            selected_matches=selected_matches,
        )
        answer = final["answer"]
        structured_answer = final["structured_answer"]
        answer_contract = final["answer_contract"]
        self._store_cached_answer(
            cache_key,
            answer=answer,
            usage=usage,
            answer_quality=quality_gate,
            provider=self.llm_provider.name,
            model=effective_model,
            thinking_budget=llm_route.get("repair_thinking_budget", answer_thinking_budget),
            finish_reason=finish_reason,
        )
        return {
            "llm_answer": answer,
            "llm_budget_mode": routed_budget_mode,
            "llm_requested_budget_mode": llm_budget_mode,
            "llm_route": llm_route,
            "llm_provider": self.llm_provider.name,
            "llm_cached": False,
            "llm_usage": usage,
            "llm_model": effective_model,
            "llm_thinking_budget": llm_route.get("repair_thinking_budget", answer_thinking_budget),
            "llm_attempts": attempts,
            "llm_latency_ms": llm_latency_ms,
            "llm_attempt_log": llm_attempt_log,
            "llm_finish_reason": finish_reason,
            "answer_quality": quality_gate,
            "answer_self_check": answer_check,
            "answer_claim_check": claim_check,
            "answer_judge": answer_judge,
            "structured_answer": structured_answer,
            "answer_contract": answer_contract,
            "evidence_pack": evidence_pack,
        }

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
        request_cache: dict[str, Any] | None = None,
        progress_callback: Any | None = None,
        attachments: list[dict[str, Any]] | None = None,
        runtime_evidence: list[dict[str, Any]] | None = None,
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
        llm_route = {
            **llm_route,
            "answer_model": selected_model,
            "prompt_mode": CODEX_INVESTIGATION_PROMPT_MODE,
            "candidate_paths": candidate_paths,
            "candidate_path_layers": candidate_path_layers,
            "candidate_repo_count": len({item.get("repo") for item in candidate_paths}),
            "candidate_path_count": len(candidate_paths),
            "codex_repair_enabled": self.codex_repair_enabled,
            "codex_fast_path_enabled": self.codex_fast_path_enabled,
            "codex_session_mode": self.codex_session_mode,
            "codex_session_max_turns": self.codex_session_max_turns,
            "codex_cache_followups": self.codex_cache_followups,
        }
        prompt_context = self._codex_investigation_brief(
            pm_team=pm_team,
            country=country,
            question=question,
            candidate_paths=candidate_paths,
            evidence_pack=evidence_pack,
            quality_gate=quality_gate,
            followup_context=followup_context,
            attachments=attachments or [],
            runtime_evidence=runtime_evidence or [],
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
        cached = None if (is_followup and not self.codex_cache_followups) else self._load_cached_answer(cache_key)
        if cached is not None:
            cached_answer = str(cached["answer"])
            cached_structured = self._parse_structured_answer(cached_answer)
            cached_validation = self._validate_codex_citations(cached_answer, candidate_paths, candidate_paths)
            cached_claim_check = self._merge_codex_validation(self._trusted_provider_check(), cached_validation)
            cached_judge = self._run_answer_judge(question, cached_answer, evidence_pack, cached_claim_check)
            cached_needs_refresh = self.codex_repair_enabled and self._codex_deep_investigation_needed(
                question=question,
                answer=cached_answer,
                structured_answer=cached_structured,
                quality_gate=cached.get("answer_quality") or quality_gate,
                answer_judge=cached_judge,
                codex_validation=cached_validation,
            )
            cached_needs_refresh = cached_needs_refresh or (
                self.codex_repair_enabled
                and (
                    cached_validation.get("status") not in {"ok", "skipped"}
                    or str(cached_judge.get("status") or "").lower() in {"repair", "warn", "insufficient_evidence"}
                )
            )
            if cached_needs_refresh:
                cached = None
        if cached is not None:
            cached_answer = str(cached["answer"])
            cached_structured = self._parse_structured_answer(cached_answer)
            cached_validation = self._validate_codex_citations(cached_answer, candidate_paths, candidate_paths)
            cached_claim_check = self._merge_codex_validation(self._trusted_provider_check(), cached_validation)
            cached_judge = self._run_answer_judge(question, cached_answer, evidence_pack, cached_claim_check)
            cached_final = self._finalize_trusted_model_answer(
                question=question,
                answer=cached_answer,
                structured_answer=cached_structured,
                evidence_summary=evidence_summary,
                quality_gate=cached.get("answer_quality") or quality_gate,
                claim_check=cached_claim_check,
                answer_judge=cached_judge,
                finish_reason=cached.get("finish_reason") or "cache_hit",
                selected_matches=candidate_matches,
            )
            answer_contract = cached_final["answer_contract"]
            cached_summary = {
                "prompt_mode": llm_route.get("prompt_mode"),
                "candidate_repo_count": llm_route.get("candidate_repo_count"),
                "candidate_path_count": llm_route.get("candidate_path_count"),
                "cited_path_count": 0,
                "citation_validation_status": cached_validation.get("status"),
                "repair_attempted": False,
                "cli_latency_ms": 0,
                "exit_codes": [],
                "timeout": False,
            }
            return {
                "llm_answer": cached_final["answer"],
                "llm_budget_mode": routed_budget_mode,
                "llm_requested_budget_mode": llm_budget_mode,
                "llm_route": llm_route,
                "llm_provider": cached.get("provider") or self.llm_provider.name,
                "llm_model": cached.get("model") or selected_model,
                "llm_cached": True,
                "llm_usage": self._normalize_llm_usage(cached.get("usage") or {}),
                "llm_thinking_budget": 0,
                "llm_latency_ms": 0,
                "llm_attempts": 0,
                "llm_attempt_log": [],
                "llm_finish_reason": cached.get("finish_reason") or "cache_hit",
                "answer_quality": cached.get("answer_quality") or quality_gate,
                "answer_claim_check": cached_claim_check,
                "answer_judge": cached_judge,
                "structured_answer": cached_final["structured_answer"],
                "answer_contract": answer_contract,
                "evidence_pack": evidence_pack,
                "codex_cli_summary": cached_summary,
            }
        codex_cli_session_id = ""
        if self.codex_session_mode == CODEX_SESSION_MODE_RESUME and isinstance(followup_context, dict):
            session_meta = followup_context.get("codex_cli_session") if isinstance(followup_context.get("codex_cli_session"), dict) else {}
            codex_cli_session_id = str(session_meta.get("session_id") or "").strip()
        payload = self._codex_payload(
            prompt_context,
            progress_callback=progress_callback,
            codex_cli_session_id=codex_cli_session_id,
            image_paths=self._attachment_image_paths(attachments or []),
        )
        result = self.llm_provider.generate(
            payload=payload,
            primary_model=selected_model,
            fallback_model=self._llm_fallback_model(),
        )
        answer = self.llm_provider.extract_text(result.payload)
        structured_answer = self._parse_structured_answer(answer)
        usage = self._normalize_llm_usage(result.usage or result.payload.get("usageMetadata") or {})
        effective_model = result.model
        attempts = result.attempts
        llm_latency_ms = int(result.latency_ms or 0)
        llm_attempt_log = [dict(item) for item in result.attempt_log]
        finish_reason = self._llm_finish_reason(result.payload)
        codex_cli_trace = result.payload.get("codex_cli_trace") if isinstance(result.payload.get("codex_cli_trace"), dict) else {}
        codex_validation = self._validate_codex_citations(answer, candidate_paths, candidate_paths)
        claim_check = self._merge_codex_validation(self._trusted_provider_check(), codex_validation)
        answer_judge = self._run_answer_judge(question, answer, evidence_pack, claim_check)
        repair_attempted = False
        deep_investigation_rounds = 0
        deep_investigation_terms: list[str] = []
        deep_investigation_added = 0
        repair_issues = list(codex_validation.get("issues") or [])
        repair_issues.extend(answer_judge.get("issues") or [])
        deep_needed = self._codex_deep_investigation_needed(
            question=question,
            answer=answer,
            structured_answer=structured_answer,
            quality_gate=quality_gate,
            answer_judge=answer_judge,
            codex_validation=codex_validation,
        )
        if self.codex_repair_enabled and (repair_issues or deep_needed):
            repair_attempted = True
            if deep_needed:
                if progress_callback:
                    try:
                        progress_callback("codex_deep_investigation", "Expanding investigation from Codex gaps.", 0, 0)
                    except Exception:
                        pass
                before_keys = {(item.get("repo"), item.get("path"), item.get("line_start"), item.get("line_end")) for item in candidate_matches}
                deep_investigation_terms = self._codex_deep_investigation_terms(
                    question=question,
                    answer=answer,
                    structured_answer=structured_answer,
                    answer_judge=answer_judge,
                    codex_validation=codex_validation,
                )
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
                if expanded_matches:
                    candidate_matches = self._select_llm_matches(expanded_matches, self.codex_top_path_limit, question=question)
                    candidate_paths = self._codex_candidate_paths(entries=entries, key=key, matches=candidate_matches)
                    candidate_paths = self._merge_codex_followup_candidate_paths(candidate_paths, followup_context)
                    candidate_path_layers = self._codex_candidate_path_layers(candidate_paths, followup_context)
                    llm_route = {
                        **llm_route,
                        "candidate_paths": candidate_paths,
                        "candidate_path_layers": candidate_path_layers,
                        "candidate_repo_count": len({item.get("repo") for item in candidate_paths}),
                        "candidate_path_count": len(candidate_paths),
                    }
                    evidence_summary = self._compress_evidence_cached(question, candidate_matches, request_cache=request_cache)
                    trace_paths = self._build_trace_paths(entries=entries, key=key, matches=candidate_matches, question=question, request_cache=request_cache)
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
                    after_keys = {(item.get("repo"), item.get("path"), item.get("line_start"), item.get("line_end")) for item in candidate_matches}
                    deep_investigation_added = len(after_keys - before_keys)
                deep_investigation_rounds = 1
            repair_context = self._codex_investigation_brief(
                pm_team=pm_team,
                country=country,
                question=question,
                candidate_paths=candidate_paths,
                evidence_pack=evidence_pack,
                quality_gate=quality_gate,
                followup_context=followup_context,
                attachments=attachments or [],
                runtime_evidence=runtime_evidence or [],
                repair_issues=list(dict.fromkeys([
                    *[str(issue) for issue in repair_issues if issue],
                    *(["Deep investigation: use the expanded candidate paths and explicitly resolve business ambiguity, caller/callee gaps, and missing source hops before finalizing."] if deep_needed else []),
                ])),
            )
            repair_payload = self._codex_payload(
                repair_context,
                progress_callback=progress_callback,
                codex_cli_session_id=codex_cli_session_id,
                image_paths=self._attachment_image_paths(attachments or []),
            )
            repair_result = self.llm_provider.generate(
                payload=repair_payload,
                primary_model=selected_model,
                fallback_model=self._llm_fallback_model(),
            )
            repair_answer = self.llm_provider.extract_text(repair_result.payload)
            repair_structured = self._parse_structured_answer(repair_answer)
            repair_validation = self._validate_codex_citations(repair_answer, candidate_paths, candidate_paths)
            repair_claim_check = self._merge_codex_validation(self._trusted_provider_check(), repair_validation)
            repair_judge = self._run_answer_judge(question, repair_answer, evidence_pack, repair_claim_check)
            answer = repair_answer
            structured_answer = repair_structured
            codex_validation = repair_validation
            claim_check = repair_claim_check
            answer_judge = repair_judge
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
        answer_contract = final["answer_contract"]
        llm_route = {
            **llm_route,
            "codex_citation_validation_status": codex_validation.get("status"),
            "codex_repair_attempted": repair_attempted,
            "codex_cited_path_count": codex_validation.get("cited_path_count", 0),
            "codex_deep_investigation_rounds": deep_investigation_rounds,
            "codex_deep_investigation_terms": deep_investigation_terms[:12],
            "codex_deep_investigation_added": deep_investigation_added,
        }
        if not (is_followup and not self.codex_cache_followups):
            self._store_cached_answer(
                cache_key,
                answer=final["answer"],
                usage=usage,
                answer_quality=quality_gate,
                provider=self.llm_provider.name,
                model=effective_model,
                thinking_budget=0,
                finish_reason=finish_reason,
            )
        codex_cli_summary = {
            "prompt_mode": llm_route.get("prompt_mode"),
            "candidate_repo_count": llm_route.get("candidate_repo_count"),
            "candidate_path_count": llm_route.get("candidate_path_count"),
            "cited_path_count": codex_validation.get("cited_path_count", 0),
            "citation_validation_status": codex_validation.get("status"),
            "repair_attempted": repair_attempted,
            "cli_latency_ms": llm_latency_ms,
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
            "answer_contract": answer_contract,
            "evidence_pack": evidence_pack,
            "codex_cli_summary": codex_cli_summary,
            "codex_cli_trace": codex_cli_trace,
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

    def _codex_payload(
        self,
        prompt: str,
        *,
        progress_callback: Any | None = None,
        codex_cli_session_id: str = "",
        image_paths: list[str] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "codex_prompt_mode": CODEX_INVESTIGATION_PROMPT_MODE,
            "contents": [{"parts": [{"text": prompt}]}],
            "systemInstruction": {"parts": [{"text": self._codex_system_instruction()}]},
            "generationConfig": {"temperature": 0.1, "responseMimeType": "application/json"},
        }
        if codex_cli_session_id:
            payload["codex_cli_session_id"] = codex_cli_session_id
        if image_paths:
            payload["_codex_image_paths"] = list(image_paths)
        if progress_callback:
            payload["_progress_callback"] = progress_callback
        return payload

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
            "- If runtime evidence conflicts with source code, describe the conflict instead of silently choosing one.",
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
                if len(text) > 6000:
                    text = f"{text[:6000]}\n...[runtime evidence text truncated]"
                lines.append(f"  Extracted text/summary:\n{text}")
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
    def _llm_payload_parts(prompt: str, attachments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        parts: list[dict[str, Any]] = [{"text": prompt}]
        for item in attachments or []:
            if str(item.get("kind") or "") != "image":
                continue
            path = Path(str(item.get("path") or ""))
            if not path.exists() or not path.is_file():
                raise ToolError(f"Image attachment is missing or unreadable: {item.get('filename') or item.get('id') or path}")
            try:
                encoded = base64.b64encode(path.read_bytes()).decode("ascii")
            except OSError as error:
                raise ToolError(f"Image attachment is unreadable: {item.get('filename') or path}") from error
            parts.append(
                {
                    "inlineData": {
                        "mimeType": str(item.get("mime_type") or "application/octet-stream"),
                        "data": encoded,
                    }
                }
            )
        return parts

    @staticmethod
    def _codex_system_instruction() -> str:
        return (
            "You are Codex running as a read-only code investigator for Source Code Q&A. "
            "Use shell/file inspection to verify the answer from the synced repository workspace. "
            "Do not edit files, install dependencies, create commits, deploy, or run mutating commands. "
            "Prefer rg, sed, nl, and direct file reads. "
            "Always follow the three-stage investigation contract: first discover candidate evidence, then verify gaps/absence with targeted searches, then answer with explicit certainty levels. "
            "Return concise JSON with direct_answer, investigation_steps, attachment_facts, screenshot_evidence, source_code_evidence, confirmed_from_code, inferred_from_code, not_found, missing_production_evidence, next_checks, claims, missing_evidence, and confidence. "
            "Put only verified production/config code facts in confirmed_from_code; put weaker deductions in inferred_from_code. "
            "For screenshot-driven questions, extract visible screenshot facts first, then tie them to code paths/functions/fields; never present screenshot content as repository fact. "
            "When source evidence is incomplete, put the exact missing repository/table/config/log/export in not_found and missing_evidence instead of filling the gap from naming or prior assumptions. "
            "Every concrete code claim must cite either an evidence id like S1 or a real file reference like path/to/File.java:10-20."
        )

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

    def _repo_relative_root(self, repo_root: Path) -> str:
        try:
            return str(Path(repo_root).resolve().relative_to(self.repo_root.resolve()))
        except ValueError:
            return ""

    def _codex_candidate_path_layers(
        self,
        candidate_paths: list[dict[str, Any]],
        followup_context: dict[str, Any] | None,
    ) -> dict[str, list[dict[str, Any]]]:
        confirmed_keys: set[tuple[str, str]] = set()
        for item in (followup_context or {}).get("codex_inspected_paths") or []:
            if isinstance(item, dict):
                confirmed_keys.add((str(item.get("repo") or ""), str(item.get("path") or "")))
        for item in (followup_context or {}).get("codex_candidate_paths") or []:
            if isinstance(item, dict) and str(item.get("trace_stage") or "") == "followup_memory":
                confirmed_keys.add((str(item.get("repo") or ""), str(item.get("path") or "")))
        layers = {
            "confirmed_previous_paths": [],
            "current_high_confidence_paths": [],
            "current_supporting_paths": [],
            "maybe_relevant_paths": [],
        }
        for item in candidate_paths:
            key = (str(item.get("repo") or ""), str(item.get("path") or ""))
            stage = str(item.get("trace_stage") or "").lower()
            retrieval = str(item.get("retrieval") or "").lower()
            exists = bool(item.get("file_exists"))
            if key in confirmed_keys or stage == "followup_memory" or retrieval == "previous_codex_context":
                layers["confirmed_previous_paths"].append(item)
            elif exists and stage in {"direct", "call_chain", "read_write", "semantic", "token"}:
                layers["current_high_confidence_paths"].append(item)
            elif exists:
                layers["current_supporting_paths"].append(item)
            else:
                layers["maybe_relevant_paths"].append(item)
        return layers

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
        repair_issues: list[str] | None = None,
    ) -> str:
        candidate_path_layers = self._codex_candidate_path_layers(candidate_paths, followup_context)
        lines = [
            f"Prompt mode: {CODEX_INVESTIGATION_PROMPT_MODE}",
            f"PM Team: {pm_team}",
            f"Country: {country}",
            f"Question: {question}",
            "",
            "Execution policy:",
            "- Use only read-only shell/file inspection inside the synced repository workspace.",
            "- Local retrieval has only narrowed the candidate repo/path range; verify by opening files yourself.",
            "- Do not write files, run formatters, install packages, commit, push, deploy, or mutate runtime state.",
            "- Prefer `rg` when available; if it is unavailable, use `grep -R`, `find`, `sed -n`, `nl -ba`, and direct file reads.",
            f"- Codex starts in the synced repos parent directory: {self.repo_root}.",
            "- Candidate `root` values are absolute synced repo roots. Candidate `relative_root` values are relative to the current repos parent.",
            "- Use either `cd relative_root` from the repos parent or use the absolute `root`; do not use only the final directory basename.",
            "- Candidate `path` values are relative to that repo root; inspect files as root/path.",
            "- First confirm the cwd/repo root, then read at least one candidate file or run one repository search before answering unless there are no candidate paths.",
            "- Prioritize confirmed_previous_paths, then current_high_confidence_paths, then current_supporting_paths; use maybe_relevant_paths only to redirect a search.",
            "",
            "Three-stage investigation required:",
            "- Stage 1 candidate evidence: identify the most relevant files/configs/SQL/tests/routes from candidate paths and direct searches. Record this in investigation_steps.candidate_evidence.",
            "- Stage 2 gap verification: run targeted searches for expected missing links before answering. Search for full definitions, INSERT rows, rollback scripts, mapper/client/repository/table/API hops, enums, value mappings, and relevant tests when the question implies them. Record this in investigation_steps.gap_verification.",
            "- Stage 3 certainty split: answer by separating confirmed_from_code, inferred_from_code, and not_found/missing_evidence. Do not promote a high-confidence inference into confirmed_from_code.",
            "- For rule/config questions, explicitly distinguish full rule/config definitions from status-only migration updates. If only status updates are found, say the full rule row/expression is missing.",
            "- For data-source questions, explicitly distinguish DTO/carrier fields from upstream source tables/APIs/repos. If the upstream hop is absent, list that hop in missing_evidence.",
            "- For ambiguous business wording, map each possible meaning to concrete code surfaces before answering. Example: distinguish admin query endpoints from report ingestion endpoints, field aliases from true schema fields, and synchronous caller failures from async processing failures.",
            "- If a developer phrase sounds like a business shorthand rather than an exact class/API name, say which source-backed interpretation is confirmed and which interpretation still needs logs, traceId, config export, or the caller repo.",
            "- For screenshot-driven incident questions, first write down visible IDs/statuses/timestamps/expected-vs-actual facts from the screenshot, then use those exact terms to steer source searches.",
            "- Do not call a screenshot-based hypothesis an RCA unless DB/log/runtime evidence for the visible case IDs is present.",
            "",
            "Candidate path layers:",
        ]
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
                "- Use S ids from candidate paths when they support the claim, or direct file citations like src/Foo.java:10-20 after you verify the file exists.",
                "- source_code_evidence must name concrete files/functions/classes/fields/tables or APIs when available; do not use generic phrases like 'the admin code'.",
                "- Put only file-verified production facts in confirmed_from_code/source_code_evidence; put carrier/call-chain deductions in inferred_from_code; put missing source hops in not_found.",
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
        if validation.get("status") == "ok" and str(merged.get("status") or "") == "skipped":
            merged["status"] = "ok"
            merged["reason"] = "codex_citation_validation"
        elif validation.get("status") != "ok":
            issues = [*(merged.get("issues") or []), *(validation.get("issues") or [])]
            merged["issues"] = list(dict.fromkeys(str(issue) for issue in issues if issue))
            merged["status"] = "needs_citation"
            unsupported = list(merged.get("unsupported_claims") or [])
            unsupported.extend(validation.get("unsupported_claims") or [])
            merged["unsupported_claims"] = list(dict.fromkeys(unsupported))[:8]
        return merged

    @staticmethod
    def _mark_codex_answer_unreliable(answer_contract: dict[str, Any], validation: dict[str, Any]) -> dict[str, Any]:
        contract = dict(answer_contract or {})
        contract["status"] = "unreliable_llm_answer"
        contract["confidence"] = "low"
        contract["codex_citation_validation"] = validation
        return contract

    def _validate_codex_citations(
        self,
        answer: str,
        candidate_paths: list[dict[str, Any]],
        selected_matches: list[dict[str, Any]],
    ) -> dict[str, Any]:
        structured = self._parse_structured_answer(answer)
        claims = [
            claim for claim in structured.get("claims") or []
            if isinstance(claim, dict) and str(claim.get("text") or "").strip()
        ]
        valid_s_ids = {str(index) for index in range(1, len(selected_matches) + 1)}
        issues: list[str] = []
        unsupported_claims: list[str] = []
        direct_refs: list[dict[str, Any]] = []
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
                issues.append("Codex cited evidence ids outside the candidate path list")
            valid_s = s_ids & valid_s_ids
            valid_direct = []
            invalid_direct = []
            for raw_ref in direct_candidates:
                resolved = self._resolve_codex_file_ref(raw_ref, candidate_paths)
                if resolved.get("status") == "ok":
                    valid_direct.append(resolved)
                else:
                    invalid_direct.append(raw_ref)
            direct_refs.extend(valid_direct)
            if invalid_direct:
                issues.append(f"Codex returned invalid file references: {', '.join(invalid_direct[:4])}")
            if not valid_s and not valid_direct:
                unsupported_claims.append(claim_text[:220])
        if unsupported_claims:
            issues.append("Codex concrete claims need valid S-id or file:line citations")
        return {
            "status": "ok" if not issues else "needs_citation",
            "checked_claims": checked_claims,
            "issues": list(dict.fromkeys(issues)),
            "unsupported_claims": unsupported_claims[:6],
            "cited_path_count": len({item.get("absolute_path") for item in direct_refs if item.get("absolute_path")}),
            "direct_file_refs": direct_refs[:12],
        }

    @staticmethod
    def _extract_direct_file_refs(text: str) -> list[str]:
        refs = []
        for match in re.finditer(r"([A-Za-z0-9_./$@-]+\.[A-Za-z0-9_]+:\d+(?:-\d+)?)", str(text or "")):
            refs.append(match.group(1))
        return refs

    def _resolve_codex_file_ref(self, raw_ref: str, candidate_paths: list[dict[str, Any]]) -> dict[str, Any]:
        ref = str(raw_ref or "").strip().strip("[]`'\"")
        match = re.match(r"^(?:(?P<repo>[^:]+):)?(?P<path>.+):(?P<start>\d+)(?:-(?P<end>\d+))?$", ref)
        if not match:
            return {"status": "invalid", "reason": "missing file line range", "ref": ref}
        path = match.group("path").strip()
        start = int(match.group("start"))
        end = int(match.group("end") or start)
        if start <= 0 or end < start:
            return {"status": "invalid", "reason": "invalid line range", "ref": ref}
        if path.startswith("/") or ".." in Path(path).parts:
            return {"status": "invalid", "reason": "unsafe path", "ref": ref}
        repo_hint = str(match.group("repo") or "").strip()
        repo_roots = []
        for item in candidate_paths:
            if repo_hint and repo_hint not in {str(item.get("repo") or ""), Path(str(item.get("repo_root") or "")).name}:
                continue
            root = str(item.get("repo_root") or "").strip()
            if root and root not in repo_roots:
                repo_roots.append(root)
        for root in repo_roots:
            candidate = (Path(root) / path).resolve()
            try:
                candidate.relative_to(Path(root).resolve())
            except ValueError:
                continue
            if not candidate.exists() or not candidate.is_file():
                continue
            try:
                line_count = len(candidate.read_text(encoding="utf-8", errors="ignore").splitlines())
            except OSError:
                continue
            if end <= line_count:
                return {
                    "status": "ok",
                    "ref": ref,
                    "path": path,
                    "absolute_path": str(candidate),
                    "line_start": start,
                    "line_end": end,
                }
        return {"status": "invalid", "reason": "file or line range not found", "ref": ref}

    def _resolve_llm_budget(
        self,
        requested_budget_mode: str,
        question: str,
        matches: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        requested = str(requested_budget_mode or "auto").strip().lower() or "auto"
        if requested in self.llm_budgets:
            if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI and requested == "cheap":
                return "balanced", self.llm_budgets["balanced"], {
                    "mode": "provider_quality_floor",
                    "requested": requested,
                    "selected": "balanced",
                    "reason": "vertex_quality_floor",
                }
            return requested, self.llm_budgets[requested], {"mode": "manual", "requested": requested, "reason": "user_selected"}
        intent = self._question_intent(question)
        trace_stages = {str(match.get("trace_stage") or "") for match in matches}
        retrievals = {str(match.get("retrieval") or "") for match in matches}
        lowered_question = str(question or "").lower()
        simple_lookup = self._is_simple_symbol_lookup_question(question)
        deep_reasons: list[str] = []
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            if intent.get("data_source"):
                deep_reasons.append("data_source_trace")
            if any(intent.get(key) for key in ("error", "impact_analysis", "module_dependency", "message_flow", "test_coverage", "operational_boundary", "static_qa")):
                deep_reasons.append("vertex_complex_code_reasoning")
            if "root cause" in lowered_question or "why" in lowered_question or "cross-repo" in lowered_question:
                deep_reasons.append("root_cause_or_cross_repo")
            if not simple_lookup and (len(matches) >= 8 or {"flow_graph", "entity_graph", "code_graph"} & retrievals):
                deep_reasons.append("vertex_large_evidence_bundle")
            if deep_reasons:
                mode = "deep"
            else:
                mode = "balanced"
                deep_reasons.append("vertex_quality_default")
            return mode, self.llm_budgets[mode], {"mode": "auto", "requested": requested, "selected": mode, "reason": ",".join(deep_reasons)}
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
    def _llm_answer_response_schema() -> dict[str, Any]:
        string_array = {"type": "ARRAY", "items": {"type": "STRING"}}
        return {
            "type": "OBJECT",
            "properties": {
                "direct_answer": {"type": "STRING"},
                "investigation_steps": {
                    "type": "OBJECT",
                    "properties": {
                        "candidate_evidence": string_array,
                        "gap_verification": string_array,
                        "certainty_split": string_array,
                    },
                    "propertyOrdering": ["candidate_evidence", "gap_verification", "certainty_split"],
                },
                "confirmed_from_code": string_array,
                "inferred_from_code": string_array,
                "not_found": string_array,
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
            "propertyOrdering": [
                "direct_answer",
                "investigation_steps",
                "confirmed_from_code",
                "inferred_from_code",
                "not_found",
                "claims",
                "missing_evidence",
                "confidence",
            ],
        }

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
        evidence_pack: dict[str, Any],
        matches: list[dict[str, Any]],
        *,
        domain_context: str = "",
        snippet_line_budget: int,
        snippet_char_budget: int,
        compact: bool = False,
    ) -> str:
        vertex_native = self.llm_provider_name == LLM_PROVIDER_VERTEX_AI
        sections = [
            "Evidence summary and guardrails:",
            f"- Quality gate: {quality_gate.get('status')} / confidence={quality_gate.get('confidence')} / missing={', '.join(quality_gate.get('missing') or []) or 'none'}",
        ]
        if domain_context:
            sections.append("\nDomain and answer-shape guidance:")
            sections.append(str(domain_context).strip())
        policies = quality_gate.get("policies") or []
        if policies:
            sections.append("- Evidence policies:")
            for policy in policies[:6]:
                sections.append(
                    f"  - {policy.get('name')}: {policy.get('status')} / required_any={', '.join(policy.get('required_any') or [])}"
                )
        snippet_context = self._build_llm_context(
            matches,
            snippet_line_budget=max(8, min(int(snippet_line_budget or 40), 320 if vertex_native else 180)),
            snippet_char_budget=max(1200, min(int(snippet_char_budget or 5000), 100_000 if vertex_native else 28_000)),
        )
        if snippet_context and vertex_native:
            sections.append("\nPrimary raw code evidence:")
            sections.append(
                "Use these snippets as the source of truth. The structured facts below are navigation hints, not a substitute for the code."
            )
            sections.append(snippet_context)
        if evidence_pack:
            sections.append(f"- Evidence pack v{evidence_pack.get('version')}:")
            typed_items = evidence_pack.get("items") or []
            if typed_items:
                sections.append("  - Typed evidence items:")
                typed_limit = 6 if compact else 12
                for item in typed_items[:typed_limit]:
                    location = ""
                    if item.get("source_id"):
                        location = f" [{item.get('source_id')}]"
                    sections.append(
                        f"    - {item.get('type')} / confidence={item.get('confidence')} / hop={item.get('hop')}{location}: {item.get('claim')}"
                    )
            for label, key in (
                ("Entry points", "entry_points"),
                ("Call chain", "call_chain"),
                ("Read/write points", "read_write_points"),
                ("External dependencies", "external_dependencies"),
                ("Tables", "tables"),
                ("APIs", "apis"),
                ("Configs", "configs"),
                ("Static QA findings", "static_findings"),
                ("Impact surfaces", "impact_surfaces"),
                ("Test coverage", "test_coverage"),
                ("Operational boundaries", "operational_boundaries"),
                ("Source tiers", "source_tiers"),
                ("Source conflicts", "source_conflicts"),
                ("Missing hops", "missing_hops"),
            ):
                values = evidence_pack.get(key) or []
                if values:
                    sections.append(f"  - {label}:")
                    value_limit = 4 if compact else 8
                    for value in values[:value_limit]:
                        sections.append(f"    - {value}")
        for label, key in (
            ("Entry points", "entry_points"),
            ("Data carriers", "data_carriers"),
            ("Field population trail", "field_population"),
            ("Downstream components", "downstream_components"),
            ("Concrete data sources", "data_sources"),
            ("API or config evidence", "api_or_config"),
            ("Rule or error logic", "rule_or_error_logic"),
            ("Static QA findings", "static_findings"),
            ("Impact surfaces", "impact_surfaces"),
            ("Test coverage", "test_coverage"),
            ("Operational boundaries", "operational_boundaries"),
            ("Source tiers", "source_tiers"),
            ("Source conflicts", "source_conflicts"),
        ):
            values = evidence_summary.get(key) or []
            if values:
                sections.append(f"- {label}:")
                value_limit = 4 if compact else 10
                sections.extend(f"  - {value}" for value in values[:value_limit])
        trace_paths = evidence_summary.get("trace_paths") or []
        if trace_paths:
            sections.append("- Trace paths:")
            path_limit = 2 if compact else 5
            for path in trace_paths[:path_limit]:
                edge_text = " -> ".join(
                    f"{edge.get('edge_kind')}:{edge.get('to_name') or edge.get('to_file')}"
                    for edge in path.get("edges") or []
                )
                sections.append(f"  - {path.get('repo')}: {edge_text}")
        if snippet_context and not vertex_native:
            sections.append("\nSecondary raw snippets for grounding:")
            sections.append(snippet_context)
        return "\n".join(sections)

    def _llm_system_instruction(self) -> str:
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            return (
                "You are a senior codebase analyst for an internal portal. "
                "Answer only from the provided retrieval evidence. "
                "Use primary raw code evidence as the source of truth, and use structured facts only to navigate, group, and verify the code. "
                "Do not let compressed facts override a raw snippet. "
                "Reason through call flow, data flow, and configuration links when the evidence supports it. "
                "Never upgrade DTO/carrier evidence into a final data source. "
                "Separate confirmed_from_code, inferred_from_code, and not_found/missing evidence instead of blending certainty levels. "
                "Avoid speculative language such as likely, suggests, or appears unless explicitly marking missing evidence. "
                "Prioritize the user's actual question, give the direct answer first, and keep the final response concise. "
                "Use short citation tags for concrete code-backed claims. "
                "Keep the final answer compact, but do not omit evidence that is necessary to answer the question. "
                "If the evidence is insufficient for a confident answer, say exactly what is missing and the closest confirmed flow."
            )
        return (
            "You are a codebase analyst for an internal portal. "
            "Answer only from the provided retrieval evidence. "
            "Treat the compressed evidence facts as the primary signal, and snippets as secondary grounding. "
            "Follow the supplied domain guidance and answer blueprint, but never let domain hints override code evidence. "
            "Never upgrade DTO/carrier evidence into a final data source. "
            "Separate confirmed_from_code, inferred_from_code, and not_found/missing evidence instead of blending certainty levels. "
            "Avoid speculative language such as likely, suggests, or appears unless explicitly marking missing evidence. "
            "Prioritize answering the user's actual question directly and accurately. "
            "Do not dump ranked references, but do cite concrete claims with provided citation ids. "
            "If the evidence is insufficient for a confident answer, say what is missing and give the best next question to ask. "
            "Keep the answer concise, practical, and business-readable."
        )

    @staticmethod
    def _llm_user_prompt(
        *,
        pm_team: str,
        country: str,
        question: str,
        context: str,
        self_check: dict[str, Any] | None = None,
        attachment_section: str = "",
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
            f"{(attachment_section + chr(10) + chr(10)) if attachment_section else ''}"
            f"{retry_note}"
            "Answer requirements:\n"
            "- Return this JSON shape whenever possible: "
            "{\"direct_answer\":\"...\",\"investigation_steps\":{\"candidate_evidence\":[\"...\"],"
            "\"gap_verification\":[\"...\"],\"certainty_split\":[\"...\"]},"
            "\"attachment_facts\":[\"...\"],\"screenshot_evidence\":[\"...\"],"
            "\"source_code_evidence\":[\"file/function/field evidence...\"],"
            "\"confirmed_from_code\":[\"...\"],\"inferred_from_code\":[\"...\"],"
            "\"not_found\":[\"...\"],\"missing_production_evidence\":[\"...\"],"
            "\"next_checks\":[\"...\"],\"claims\":[{\"text\":\"...\",\"citations\":[\"S1\"]}],"
            "\"missing_evidence\":[],\"confidence\":\"high|medium|low\"}. "
            "If a short prose answer is more appropriate, still keep citation tags on concrete claims.\n"
            "- Start with the direct answer.\n"
            "- Use investigation_steps to show the three-stage investigation at a compact level: candidate evidence checked, gap verification performed, and certainty split used.\n"
            "- For image/screenshot attachments, first extract visible facts exactly into attachment_facts/screenshot_evidence: IDs, trace IDs, timestamps, status values, field names, expected-vs-actual behavior, and business impact.\n"
            "- For screenshot-driven incident questions, answer with these visible sections: Conclusion, Screenshot Evidence, Source-code Evidence, Missing Production Evidence, Next Checks. Keep them concise.\n"
            "- source_code_evidence must include concrete file/function/class/field/table/API names when available. If you cannot name them, say the source-code evidence is incomplete.\n"
            "- Put production-code, mapper, client, SQL, route, config, and directly opened file facts in confirmed_from_code.\n"
            "- Put carrier DTOs, call-chain deductions, and relation hypotheses in inferred_from_code unless a raw snippet directly proves the claim.\n"
            "- Put absent repository/mapper/client/table hops, missing tests, and evidence-tier conflicts in not_found and missing_evidence.\n"
            "- Put missing DB rows, trace logs, production config exports, and case-specific runtime checks in missing_production_evidence.\n"
            "- Put concrete follow-up actions in next_checks, not vague advice.\n"
            "- Prefer agent trace and two-hop trace evidence when it clarifies downstream service, integration, repository, mapper, API, or table usage.\n"
            "- Treat raw code snippets as the source of truth when they are provided as primary evidence; use compressed facts as navigation hints and consistency checks.\n"
            "- If user attachments are present, label their contribution as attachment evidence and do not present it as source-code evidence.\n"
            "- If uploaded runtime evidence is present, label DB/Apollo/config facts as runtime evidence, keep country scope explicit, and do not present them as source-code evidence.\n"
            "- Treat uploaded Apollo config as UAT/non-Live reference only unless the user explicitly provides Live evidence; do not conclude current production behavior from UAT Apollo uploads alone.\n"
            "- Follow the domain-specific evidence rules and answer blueprint when present.\n"
            "- Apply evidence priority: production code and mapper/client/SQL evidence beat config snapshots, tests, and docs/spec/generated files.\n"
            "- For data-source questions, a DTO/Input/Info class is not a final data source. Trace backward to the provider/builder/setter and then to repository/mapper/client/API/table when evidence exists.\n"
            "- A DAO/Mapper import or field declaration is not enough; prefer method bodies, SQL, mapper XML, API client calls, or table names.\n"
            "- If only DTO fields are known, clearly say that these are carriers, not the upstream source.\n"
            "- For static QA questions, rank concrete findings by severity and explain why each code line is risky without inventing runtime impact.\n"
            "- For impact-analysis questions, separate upstream callers/users from downstream dependencies/tables/APIs/configs.\n"
            "- For test-coverage questions, distinguish direct tests/assertions/mocks from nearby production code; call out missing test evidence explicitly.\n"
            "- For operational-boundary questions, call out transaction, cache, async, retry, circuit breaker, lock, rate-limit, and authorization annotations as runtime behavior constraints.\n"
            "- If a quality gate says evidence is missing, do not pretend certainty. Say the closest known flow and the exact missing link.\n"
            "- Do not use likely/suggests/appears for final data-source claims. Put uncertainty in missing_evidence instead.\n"
            "- Summarize the relevant logic, data sources, APIs, tables, or classes in plain language when applicable.\n"
            "- Add short citation tags like [S1] next to concrete code-backed claims.\n"
            "- Avoid listing file paths or line ranges unless the user asks for code locations.\n"
            "- If unsure, explain the uncertainty instead of inventing details.\n"
        )

    def _vertex_draft_prompt(
        self,
        *,
        pm_team: str,
        country: str,
        question: str,
        context: str,
        attachment_section: str = "",
    ) -> str:
        return (
            f"PM Team: {pm_team}\n"
            f"Country: {country}\n"
            f"Question: {question}\n\n"
            "Internal retrieval evidence for grounding only:\n"
            f"{context}\n\n"
            f"{(attachment_section + chr(10) + chr(10)) if attachment_section else ''}"
            "First-pass task for Vertex AI:\n"
            "- Write a concise prose draft answer only; do not return JSON in this pass.\n"
            "- Use raw code snippets as the source of truth and cite concrete claims with tags like [S1].\n"
            "- Identify any missing upstream/downstream/source evidence explicitly.\n"
            "- Do not turn DTO, request, input, or info classes into final data sources.\n"
            "- This draft will be used to run a second retrieval pass before the final structured answer.\n"
        )

    @staticmethod
    def _vertex_final_prompt(*, final_prompt: str, draft_answer: str) -> str:
        draft = str(draft_answer or "").strip()
        if len(draft) > 5000:
            draft = f"{draft[:5000]}\n...[draft truncated]"
        return (
            f"{final_prompt}\n"
            "Vertex first-pass draft for verification only:\n"
            f"{draft or '(empty draft)'}\n\n"
            "Final pass instruction:\n"
            "- Return the required JSON object now.\n"
            "- Use the first-pass draft only as a checklist; keep only claims supported by the updated evidence.\n"
            "- Prefer the updated second-pass evidence over the draft when they differ.\n"
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
        answer_judge: dict[str, Any] | None = None,
        finish_reason: str | None = None,
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
        missing_links = list(
            dict.fromkeys(
                [
                    *(quality_gate.get("missing") or []),
                    *(structured_answer.get("not_found") or []),
                    *(structured_answer.get("missing_evidence") or []),
                ]
            )
        )
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
            "investigation_steps": structured_answer.get("investigation_steps") or {},
            "confirmed_from_code": list(dict.fromkeys([*(structured_answer.get("confirmed_from_code") or []), *confirmed_sources]))[:8],
            "inferred_from_code": list(dict.fromkeys([*(structured_answer.get("inferred_from_code") or []), *data_carriers, *field_population]))[:8],
            "not_found": list(dict.fromkeys([*(structured_answer.get("not_found") or []), *missing_links]))[:8],
            "confidence": "low" if blocked else str(structured_answer.get("confidence") or quality_gate.get("confidence") or "medium").lower(),
            "claim_check": claim_check,
            "policies": quality_gate.get("policies") or [],
            "source_tiers": evidence_summary.get("source_tiers") or [],
            "source_conflicts": evidence_summary.get("source_conflicts") or [],
        }
        final_answer = str(answer or "").strip()
        unreliable_llm_output = self._unreliable_llm_output(
            answer=answer,
            structured_answer=structured_answer,
            claim_check=claim_check,
            answer_judge=answer_judge or {},
            finish_reason=finish_reason,
        )
        if unreliable_llm_output:
            contract["status"] = "unreliable_llm_answer"
            contract["confidence"] = "low"
            final_answer = self._build_unreliable_llm_answer(
                contract=contract,
                answer_judge=answer_judge or {},
                claim_check=claim_check,
                finish_reason=finish_reason,
            )
        elif blocked:
            final_answer = self._build_missing_source_answer(contract)
        elif structured_answer.get("format") == "json" and structured_answer.get("direct_answer"):
            final_answer = self._render_structured_answer(structured_answer, contract)
        elif uncited_claims and intent.get("data_source"):
            final_answer = self._render_structured_answer(structured_answer, contract)
        elif weak_answer and confirmed_sources:
            final_answer = self._render_structured_answer(structured_answer, contract)
        if structured_answer.get("format") == "json" and structured_answer.get("direct_answer") and not blocked and not unreliable_llm_output:
            final_structured = {
                **structured_answer,
                "confirmed_from_code": contract["confirmed_from_code"] or structured_answer.get("confirmed_from_code") or [],
                "inferred_from_code": contract["inferred_from_code"] or structured_answer.get("inferred_from_code") or [],
                "not_found": contract["not_found"] or structured_answer.get("not_found") or [],
                "missing_evidence": missing_links[:8] or structured_answer.get("missing_evidence") or [],
            }
        else:
            final_structured = self._parse_structured_answer(final_answer)
            if unreliable_llm_output:
                final_structured = {**final_structured, "confidence": "low"}
        return {
            "answer": final_answer,
            "structured_answer": final_structured,
            "answer_contract": contract,
        }

    def _trust_provider_final_answer(self) -> bool:
        return self.llm_provider_name in {LLM_PROVIDER_CODEX_CLI_BRIDGE, LLM_PROVIDER_VERTEX_AI}

    @staticmethod
    def _trusted_provider_check() -> dict[str, Any]:
        return {
            "status": "skipped",
            "reason": "trusted_provider_passthrough",
            "issues": [],
            "unsupported_claims": [],
        }

    @staticmethod
    def _trusted_provider_judge() -> dict[str, Any]:
        return {
            "status": "skipped",
            "mode": "trusted_provider_passthrough",
            "issues": [],
            "repair_targets": [],
        }

    @staticmethod
    def _skipped_codex_validation() -> dict[str, Any]:
        return {
            "status": "skipped",
            "reason": "trusted_provider_passthrough",
            "checked_claims": 0,
            "cited_path_count": 0,
            "direct_file_refs": [],
            "issues": [],
            "unsupported_claims": [],
        }

    @staticmethod
    def _skipped_codex_answer_check() -> dict[str, Any]:
        return {
            "status": "skipped",
            "reason": "codex_fast_path_passthrough",
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
        if structured_answer.get("format") == "json" and structured_answer.get("direct_answer"):
            final_answer = self._render_structured_answer(structured_answer, contract)
        return {
            "answer": final_answer,
            "structured_answer": structured_answer,
            "answer_contract": contract,
        }

    @staticmethod
    def _unreliable_llm_output(
        *,
        answer: str,
        structured_answer: dict[str, Any],
        claim_check: dict[str, Any],
        answer_judge: dict[str, Any],
        finish_reason: str | None,
    ) -> bool:
        reason = str(finish_reason or "").strip()
        broken_jsonish = structured_answer.get("format") == "prose_fallback" and str(answer or "").lstrip().startswith("{")
        capped = reason.upper() in {"MAX_TOKENS", "SAFETY", "RECITATION"} or reason.lower() in {"length", "content_filter"}
        judge_status = str((answer_judge or {}).get("status") or "").lower()
        unsupported = str((claim_check or {}).get("status") or "").lower() not in {"", "ok"}
        return bool(broken_jsonish or capped or (judge_status in {"repair", "insufficient_evidence"} and unsupported))

    @staticmethod
    def _build_unreliable_llm_answer(
        *,
        contract: dict[str, Any],
        answer_judge: dict[str, Any],
        claim_check: dict[str, Any],
        finish_reason: str | None,
    ) -> str:
        lines = [
            "I could not produce a reliable final answer from this LLM attempt.",
            "",
        ]
        reason = str(finish_reason or "").strip()
        if reason:
            lines.append("Why:")
            lines.append(f"- Model finish reason: {reason}.")
        for issue in list(answer_judge.get("issues") or [])[:3]:
            if "Why:" not in lines:
                lines.append("Why:")
            lines.append(f"- Evidence judge: {issue}")
        for issue in list(claim_check.get("issues") or [])[:2]:
            if "Why:" not in lines:
                lines.append("Why:")
            lines.append(f"- Claim check: {issue}")
        missing = contract.get("missing_links") or []
        if missing:
            lines.append("")
            lines.append("Missing evidence:")
            for item in missing[:4]:
                lines.append(f"- {item}")
        lines.append("")
        lines.append("Please retry after the index is refreshed or ask with the exact table/class names; the portal should not treat the current LLM output as a confirmed answer.")
        return "\n".join(lines)

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
        screenshot_evidence = [
            str(item).strip()
            for item in [
                *(structured_answer.get("screenshot_evidence") or []),
                *(structured_answer.get("attachment_facts") or []),
            ]
            if str(item).strip()
        ]
        source_code_evidence = [
            str(item).strip()
            for item in [
                *(structured_answer.get("source_code_evidence") or []),
                *(contract.get("confirmed_from_code") or structured_answer.get("confirmed_from_code") or []),
            ]
            if str(item).strip()
        ]
        missing_production = [
            str(item).strip()
            for item in [
                *(structured_answer.get("missing_production_evidence") or []),
                *(structured_answer.get("missing_evidence") or []),
            ]
            if str(item).strip()
        ]
        next_checks = [str(item).strip() for item in structured_answer.get("next_checks") or [] if str(item).strip()]
        if screenshot_evidence or source_code_evidence or missing_production or next_checks:
            lines = ["Conclusion", str(structured_answer.get("direct_answer") or "").strip()]
            sections = [
                ("Screenshot Evidence", screenshot_evidence),
                ("Source-code Evidence", list(dict.fromkeys(source_code_evidence))[:6]),
            ]
            inferred = [str(item).strip() for item in contract.get("inferred_from_code") or structured_answer.get("inferred_from_code") or [] if str(item).strip()]
            if inferred:
                sections.append(("Inferred / Hypothesis", inferred[:4]))
            not_found = [str(item).strip() for item in contract.get("not_found") or structured_answer.get("not_found") or [] if str(item).strip()]
            missing_combined = list(dict.fromkeys([*missing_production, *not_found, *(contract.get("missing_links") or [])]))[:6]
            sections.extend(
                [
                    ("Missing Production Evidence", missing_combined),
                    ("Next Checks", next_checks[:6]),
                ]
            )
            confidence = str(structured_answer.get("confidence") or contract.get("confidence") or "").strip()
            for title, items in sections:
                if not items:
                    continue
                lines.extend(["", title])
                for item in items:
                    lines.append(f"- {item}")
            if confidence:
                lines.extend(["", "Confidence", f"- {confidence}"])
            return "\n".join(line for line in lines if line is not None).strip()

        lines = [str(structured_answer.get("direct_answer") or "").strip()]
        confirmed = [str(item).strip() for item in contract.get("confirmed_from_code") or structured_answer.get("confirmed_from_code") or [] if str(item).strip()]
        inferred = [str(item).strip() for item in contract.get("inferred_from_code") or structured_answer.get("inferred_from_code") or [] if str(item).strip()]
        not_found = [str(item).strip() for item in contract.get("not_found") or structured_answer.get("not_found") or [] if str(item).strip()]
        if confirmed:
            lines.append("")
            lines.append("Confirmed from code:")
            for item in confirmed[:5]:
                lines.append(f"- {item}")
        if inferred:
            lines.append("")
            lines.append("Inferred:")
            for item in inferred[:4]:
                lines.append(f"- {item}")
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
        missing = not_found or contract.get("missing_links") or structured_answer.get("missing_evidence") or []
        if missing:
            lines.append("")
            lines.append("Not found / missing evidence:")
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

    def _verify_answer_claims(
        self,
        answer: str,
        evidence_summary: dict[str, Any],
        selected_matches: list[dict[str, Any]],
    ) -> dict[str, Any]:
        evidence_text = json.dumps(evidence_summary, ensure_ascii=False).lower()
        valid_citation_numbers = {str(index) for index in range(1, len(selected_matches) + 1)}
        issues: list[str] = []
        checked_claims = 0
        unsupported_claims: list[str] = []
        structured = self._parse_structured_answer(answer)
        parsed_claims = [
            item
            for item in structured.get("claims") or []
            if isinstance(item, dict) and str(item.get("text") or "").strip()
        ]
        if not parsed_claims:
            parsed_claims = [
                {"text": claim, "citations": [f"S{number}" for number in re.findall(r"\[S(\d+)\]", claim)]}
                for claim in self._split_answer_claims(answer)
            ]
        for parsed_claim in parsed_claims:
            claim = str(parsed_claim.get("text") or "").strip()
            lowered = claim.lower()
            concrete = any(term in lowered for term in ANSWER_CONCRETE_SOURCE_HINTS + API_HINTS + CONFIG_HINTS + RULE_HINTS)
            if not concrete:
                continue
            checked_claims += 1
            citations = self._claim_citation_numbers(claim, parsed_claim)
            if citations and not citations <= valid_citation_numbers:
                issues.append("answer cites evidence ids outside the provided context")
            valid_citations = citations & valid_citation_numbers
            if not valid_citations and not any(phrase in lowered for phrase in ANSWER_SELF_CHECK_WEAK_PHRASES):
                unsupported_claims.append(claim[:220])
                continue
            if valid_citations and not self._claim_supported_by_citations(claim, valid_citations, selected_matches):
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
    def _claim_citation_numbers(claim: str, parsed_claim: dict[str, Any]) -> set[str]:
        numbers = {token for token in re.findall(r"\[S(\d+)\]", str(claim or ""))}
        for raw_item in parsed_claim.get("citations") or []:
            item = str(raw_item or "").strip()
            match = re.fullmatch(r"\[?S?(\d+)\]?", item, flags=re.IGNORECASE)
            if match:
                numbers.add(match.group(1))
        return numbers

    @staticmethod
    def _claim_supported_by_citations(claim: str, citation_numbers: set[str], selected_matches: list[dict[str, Any]]) -> bool:
        claim_terms = [
            term.lower()
            for term in IDENTIFIER_PATTERN.findall(str(claim or ""))
            if len(term) >= 4 and term.lower() not in STOPWORDS and term.lower() not in LOW_VALUE_CALL_SYMBOLS
        ]
        if not claim_terms:
            return True
        for number in citation_numbers:
            try:
                match = selected_matches[int(number) - 1]
            except (IndexError, ValueError):
                continue
            evidence_text = " ".join(
                str(match.get(key) or "")
                for key in ("path", "snippet", "reason", "retrieval", "trace_stage")
            ).lower()
            overlap = {term for term in claim_terms if term in evidence_text}
            if len(overlap) >= 1:
                return True
        return False

    def _judge_answer(
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
        if len(answer_text) < 80 and len(supported_items) >= 2:
            issues.append("answer is too thin for the available typed evidence")
            repair_targets.append("summarize the strongest typed evidence")
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
        deterministic = self._judge_answer(question, answer, evidence_pack, claim_check)
        if self.llm_provider_name == LLM_PROVIDER_CODEX_CLI_BRIDGE:
            return deterministic
        if not self.llm_judge_enabled or not self.llm_ready():
            return deterministic
        cache_key = self._judge_cache_key(question, answer, evidence_pack, claim_check)
        cached = self._load_judge_cache(cache_key)
        if cached is not None:
            cached["cached"] = True
            return cached
        try:
            judge = self._llm_judge_answer(question, answer, evidence_pack, claim_check, deterministic)
            self._store_judge_cache(cache_key, judge)
            return judge
        except (OSError, ValueError, requests.RequestException, ToolError) as error:
            fallback = dict(deterministic)
            fallback["llm_judge_error"] = self._sanitize_error_detail(str(error))
            return fallback

    def _judge_cache_key(
        self,
        question: str,
        answer: str,
        evidence_pack: dict[str, Any],
        claim_check: dict[str, Any],
    ) -> str:
        digest = hashlib.sha1(
            json.dumps(
                {
                    "provider": self.llm_provider.name,
                    "model": self._model_for_role("judge", fallback=str(self.llm_budgets["cheap"]["model"])),
                    "question": question,
                    "answer": answer,
                    "evidence_pack": evidence_pack,
                    "claim_check": claim_check,
                    "versions": self._llm_versions(),
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        return digest

    def _load_judge_cache(self, key: str) -> dict[str, Any] | None:
        cache_path = self.answer_cache_root / "judge" / f"{key}.json"
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
        if payload.get("versions") != self._llm_versions():
            return None
        judge = payload.get("judge")
        return judge if isinstance(judge, dict) else None

    def _store_judge_cache(self, key: str, judge: dict[str, Any]) -> None:
        cache_dir = self.answer_cache_root / "judge"
        cache_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "versions": self._llm_versions(),
            "judge": judge,
            "expire_at": datetime.now(timezone.utc).timestamp() + self.llm_cache_ttl_seconds,
        }
        self._atomic_write_json(cache_dir / f"{key}.json", payload)

    @staticmethod
    def _llm_judge_response_schema() -> dict[str, Any]:
        string_array = {"type": "ARRAY", "items": {"type": "STRING"}}
        return {
            "type": "OBJECT",
            "properties": {
                "status": {"type": "STRING", "enum": ["ok", "warn", "repair", "insufficient_evidence"]},
                "confidence": {"type": "STRING", "enum": ["high", "medium", "low"]},
                "issues": string_array,
                "repair_targets": string_array,
            },
            "required": ["status", "confidence", "issues", "repair_targets"],
            "propertyOrdering": ["status", "confidence", "issues", "repair_targets"],
        }

    def _llm_judge_answer(
        self,
        question: str,
        answer: str,
        evidence_pack: dict[str, Any],
        claim_check: dict[str, Any],
        deterministic: dict[str, Any],
    ) -> dict[str, Any]:
        typed_items = [
            {
                "type": item.get("type"),
                "claim": item.get("claim"),
                "source_id": item.get("source_id"),
                "confidence": item.get("confidence"),
                "supports_answer": item.get("supports_answer"),
            }
            for item in evidence_pack.get("items") or []
            if isinstance(item, dict)
        ][:18]
        judge_context = {
            "question": question,
            "answer": answer,
            "claim_check": claim_check,
            "deterministic_judge": deterministic,
            "evidence": {
                "version": evidence_pack.get("version"),
                "intent": evidence_pack.get("intent"),
                "items": typed_items,
                "tables": evidence_pack.get("tables") or [],
                "apis": evidence_pack.get("apis") or [],
                "missing_hops": evidence_pack.get("missing_hops") or [],
            },
        }
        judge_model = self._model_for_role("judge", fallback=str(self.llm_budgets["cheap"]["model"]))
        judge_thinking_config = self._thinking_config_for_provider(
            0,
            model=judge_model,
            role="judge",
            budget_mode="cheap",
        )
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": (
                                "Judge whether the answer is fully supported by the evidence. "
                                "Return JSON only. Use repair when the answer invents, overstates, or misses stronger typed evidence. "
                                "Use insufficient_evidence when evidence itself cannot answer the question.\n\n"
                                f"{json.dumps(judge_context, ensure_ascii=False)}"
                            )
                        }
                    ]
                }
            ],
            "systemInstruction": {
                "parts": [
                    {
                        "text": (
                            "You are an evidence judge for source-code QA. "
                            "Do not answer the user's question. Judge only support, missing evidence, and repair targets."
                        )
                    }
                ]
            },
            "generationConfig": {
                "temperature": 0,
                "maxOutputTokens": 500,
                "responseMimeType": "application/json",
                "responseSchema": self._llm_judge_response_schema(),
                "thinkingConfig": judge_thinking_config,
            },
        }
        result = self.llm_provider.generate(
            payload=payload,
            primary_model=judge_model,
            fallback_model=self._llm_fallback_model(),
        )
        parsed = self._parse_judge_payload(self.llm_provider.extract_text(result.payload))
        status = str(parsed.get("status") or deterministic.get("status") or "warn").strip().lower()
        if status == "insufficient_evidence":
            status = "repair"
        if status not in {"ok", "warn", "repair"}:
            status = deterministic.get("status") or "warn"
        issues = [str(item).strip() for item in parsed.get("issues") or [] if str(item).strip()]
        repair_targets = [str(item).strip() for item in parsed.get("repair_targets") or [] if str(item).strip()]
        if deterministic.get("status") == "repair" and status == "ok":
            status = "warn"
            issues.extend(deterministic.get("issues") or [])
        return {
            "status": status,
            "mode": "llm_evidence_judge",
            "model": result.model,
            "attempts": result.attempts,
            "usage": self._normalize_llm_usage(result.usage or result.payload.get("usageMetadata") or {}),
            "deterministic_status": deterministic.get("status"),
            "checked_items": deterministic.get("checked_items", len(typed_items)),
            "supporting_items": deterministic.get("supporting_items"),
            "confidence": str(parsed.get("confidence") or "medium"),
            "issues": list(dict.fromkeys(issues))[:6],
            "repair_targets": list(dict.fromkeys(repair_targets))[:6],
        }

    @staticmethod
    def _parse_judge_payload(text: str) -> dict[str, Any]:
        raw = str(text or "").strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            if not match:
                return {"status": "warn", "issues": ["judge returned non-json output"], "repair_targets": []}
            try:
                payload = json.loads(match.group(0))
            except json.JSONDecodeError:
                return {"status": "warn", "issues": ["judge returned invalid json"], "repair_targets": []}
        return payload if isinstance(payload, dict) else {}

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
        if intent.get("static_qa"):
            static_markers = self._answer_expected_terms(evidence_summary, "static_findings")
            if static_markers and not any(marker in lowered_answer for marker in static_markers):
                issues.append("answer omits static QA finding terms found in evidence")
        if intent.get("impact_analysis"):
            impact_markers = self._answer_expected_terms(evidence_summary, "impact_surfaces")
            if impact_markers and not any(marker in lowered_answer for marker in impact_markers):
                issues.append("answer omits impact surface terms found in evidence")
        if intent.get("test_coverage"):
            test_markers = self._answer_expected_terms(evidence_summary, "test_coverage")
            if test_markers and not any(marker in lowered_answer for marker in test_markers):
                issues.append("answer omits test coverage terms found in evidence")
        if intent.get("operational_boundary"):
            boundary_markers = self._answer_expected_terms(evidence_summary, "operational_boundaries")
            if boundary_markers and not any(marker in lowered_answer for marker in boundary_markers):
                issues.append("answer omits operational boundary terms found in evidence")
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
        draft_answer: str = "",
        answer_check: dict[str, Any] | None = None,
        claim_check: dict[str, Any] | None = None,
        answer_judge: dict[str, Any] | None = None,
        request_cache: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        agent_plan = self._build_agent_plan(question, evidence_summary, quality_gate)
        if self.llm_provider_name == LLM_PROVIDER_VERTEX_AI:
            vertex_terms = self._vertex_second_pass_terms(
                question=question,
                draft_answer=draft_answer,
                evidence_summary=evidence_summary,
                quality_gate=quality_gate,
                answer_check=answer_check or {},
                claim_check=claim_check or {},
                answer_judge=answer_judge or {},
                matches=matches,
            )
            if vertex_terms:
                agent_plan = {
                    "steps": [
                        {
                            "name": "vertex_answer_guided_retrieval",
                            "purpose": "Use the first Vertex draft and evidence checks to retrieve missing concrete code evidence.",
                            "terms": vertex_terms,
                        }
                    ],
                    "provider": "vertex_answer_guided",
                }
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

    def _vertex_second_pass_terms(
        self,
        *,
        question: str,
        draft_answer: str,
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        answer_check: dict[str, Any],
        claim_check: dict[str, Any],
        answer_judge: dict[str, Any],
        matches: list[dict[str, Any]],
    ) -> list[str]:
        if self.llm_provider_name != LLM_PROVIDER_VERTEX_AI:
            return []
        texts = [
            question,
            draft_answer,
            " ".join(str(item) for item in quality_gate.get("missing") or []),
            " ".join(str(item) for item in answer_check.get("issues") or []),
            " ".join(str(item) for item in claim_check.get("unsupported_claims") or []),
            " ".join(str(item) for item in answer_judge.get("repair_targets") or []),
        ]
        for bucket in (
            "entry_points",
            "data_carriers",
            "field_population",
            "downstream_components",
            "data_sources",
            "api_or_config",
            "rule_or_error_logic",
            "impact_surfaces",
            "test_coverage",
            "operational_boundaries",
        ):
            texts.extend(str(item) for item in evidence_summary.get(bucket) or [])
        for match in matches[:12]:
            texts.append(str(match.get("path") or ""))
            texts.append(str(match.get("reason") or ""))
        terms: list[str] = []
        for text in texts:
            for token in re.findall(r"[A-Za-z][A-Za-z0-9_./:-]{2,}", str(text or "")):
                cleaned = token.strip("._/-:").lower()
                if len(cleaned) < 3 or cleaned in STOPWORDS or cleaned in LOW_VALUE_CALL_SYMBOLS:
                    continue
                if cleaned in {"answer", "evidence", "missing", "claim", "claims", "source", "sources"}:
                    continue
                if cleaned not in terms:
                    terms.append(cleaned)
        intent = evidence_summary.get("intent") or self._question_intent(question)
        if intent.get("data_source"):
            terms.extend(["repository", "mapper", "dao", "jdbc", "select", "from", "client", "provider"])
        if intent.get("api"):
            terms.extend(["controller", "requestmapping", "postmapping", "client", "endpoint"])
        deduped: list[str] = []
        for term in terms:
            if term not in deduped:
                deduped.append(term)
        return deduped[:36]

    def _answer_cache_key(self, *, provider: str, model: str, question: str, answer_mode: str, llm_budget_mode: str, context: str) -> str:
        digest = hashlib.sha1(
            json.dumps(
                {
                    "provider": provider,
                    "model": model,
                    "question": question,
                    "answer_mode": answer_mode,
                    "llm_budget_mode": llm_budget_mode,
                    "versions": self._llm_versions(),
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
        if payload.get("versions") != self._llm_versions():
            return None
        return payload

    def _store_cached_answer(
        self,
        key: str,
        *,
        answer: str,
        usage: dict[str, Any],
        answer_quality: dict[str, Any] | None = None,
        provider: str,
        model: str,
        thinking_budget: int | None = None,
        finish_reason: str | None = None,
    ) -> None:
        self.answer_cache_root.mkdir(parents=True, exist_ok=True)
        payload = {
            "versions": self._llm_versions(),
            "provider": provider,
            "model": model,
            "thinking_budget": int(thinking_budget or 0),
            "finish_reason": str(finish_reason or ""),
            "answer": answer,
            "usage": usage,
            "answer_quality": answer_quality or {},
            "expire_at": datetime.now(timezone.utc).timestamp() + self.llm_cache_ttl_seconds,
        }
        self._atomic_write_json(self.answer_cache_root / f"{key}.json", payload)

    @staticmethod
    def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(temp_path, path)

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
            answer_contract = payload.get("answer_contract") or {}
            evidence_pack = payload.get("evidence_pack") or {}
            tool_trace = payload.get("tool_trace") or []
            policies = answer_contract.get("policies") or (payload.get("answer_quality") or {}).get("policies") or []
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
                "trace_id": payload.get("trace_id"),
                "question_sha1": hashlib.sha1(question.encode("utf-8")).hexdigest(),
                "question_preview": question[:180],
                "requested_answer_mode": answer_mode,
                "answer_mode": payload.get("answer_mode"),
                "requested_llm_budget_mode": llm_budget_mode,
                "llm_budget_mode": payload.get("llm_budget_mode") or llm_budget_mode,
                "llm_requested_budget_mode": payload.get("llm_requested_budget_mode") or llm_budget_mode,
                "llm_route": payload.get("llm_route") or {},
                "llm_provider": payload.get("llm_provider") or self.llm_provider.name,
                "llm_model": payload.get("llm_model"),
                "llm_thinking_budget": payload.get("llm_thinking_budget"),
                "llm_cached": bool(payload.get("llm_cached")),
                "llm_attempts": payload.get("llm_attempts"),
                "llm_latency_ms": payload.get("llm_latency_ms"),
                "llm_finish_reason": payload.get("llm_finish_reason"),
                "llm_attempt_log": payload.get("llm_attempt_log") or [],
                "status": payload.get("status"),
                "latency_ms": int((time.time() - started_at) * 1000),
                "match_count": len(matches),
                "top_paths": [str(match.get("path") or "") for match in matches[:5]],
                "index_freshness": payload.get("index_freshness") or {},
                "trace_stage_counts": stage_counts,
                "retrieval_counts": retrieval_counts,
                "retrieval_runtime": payload.get("retrieval_runtime") or {},
                "answer_quality": payload.get("answer_quality") or {},
                "answer_claim_check": payload.get("answer_claim_check") or {},
                "answer_judge": payload.get("answer_judge") or {},
                "codex_cli_summary": self._codex_telemetry_summary(payload),
                "tool_trace_summary": {
                    "steps": len(tool_trace),
                    "phases": sorted({str(step.get("phase") or "unknown") for step in tool_trace if isinstance(step, dict)}),
                    "tools": sorted({str(step.get("tool") or "unknown") for step in tool_trace if isinstance(step, dict)})[:20],
                    "matches_added": sum(int(step.get("matches_added") or 0) for step in tool_trace if isinstance(step, dict)),
                },
                "answer_contract": answer_contract,
                "evidence_pack_summary": {
                    "version": evidence_pack.get("version"),
                    "items": len(evidence_pack.get("items") or []),
                    "entry_points": len(evidence_pack.get("entry_points") or []),
                    "call_chain": len(evidence_pack.get("call_chain") or []),
                    "read_write_points": len(evidence_pack.get("read_write_points") or []),
                    "external_dependencies": len(evidence_pack.get("external_dependencies") or []),
                    "tables": len(evidence_pack.get("tables") or []),
                    "apis": len(evidence_pack.get("apis") or []),
                    "static_findings": len(evidence_pack.get("static_findings") or []),
                    "impact_surfaces": len(evidence_pack.get("impact_surfaces") or []),
                    "test_coverage": len(evidence_pack.get("test_coverage") or []),
                    "operational_boundaries": len(evidence_pack.get("operational_boundaries") or []),
                    "missing_hops": len(evidence_pack.get("missing_hops") or []),
                },
                "answer_policy_statuses": {
                    str(policy.get("name") or "unknown"): str(policy.get("status") or "unknown")
                    for policy in policies
                    if isinstance(policy, dict)
                },
                "structured_answer_confidence": (payload.get("structured_answer") or {}).get("confidence"),
                "llm_usage": payload.get("llm_usage") or {},
                "fallback": bool(payload.get("fallback_notice")),
                "versions": self._llm_versions(),
            }
            self.telemetry_path.parent.mkdir(parents=True, exist_ok=True)
            with self.telemetry_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        except OSError:
            return

    @staticmethod
    def _codex_telemetry_summary(payload: dict[str, Any]) -> dict[str, Any]:
        if str(payload.get("llm_provider") or "") != LLM_PROVIDER_CODEX_CLI_BRIDGE:
            return {}
        llm_route = payload.get("llm_route") or {}
        validation = ((payload.get("answer_claim_check") or {}).get("codex_citation_validation") or {})
        attempts = payload.get("llm_attempt_log") or []
        trace = payload.get("codex_cli_trace") if isinstance(payload.get("codex_cli_trace"), dict) else {}
        exit_codes = [
            item.get("exit_code")
            for item in attempts
            if isinstance(item, dict) and item.get("exit_code") is not None
        ]
        return {
            "prompt_mode": llm_route.get("prompt_mode"),
            "candidate_repo_count": llm_route.get("candidate_repo_count"),
            "candidate_path_count": llm_route.get("candidate_path_count"),
            "cited_path_count": validation.get("cited_path_count", llm_route.get("codex_cited_path_count", 0)),
            "citation_validation_status": validation.get("status"),
            "repair_attempted": bool(llm_route.get("codex_repair_attempted")),
            "cli_latency_ms": payload.get("llm_latency_ms"),
            "exit_codes": exit_codes,
            "timeout": any(bool(item.get("timeout")) for item in attempts if isinstance(item, dict)),
            "session_mode": trace.get("session_mode") or llm_route.get("codex_session_mode"),
            "command_mode": trace.get("command_mode"),
            "stream_message_count": len(trace.get("stream_messages") or []),
            "command_count": len(trace.get("command_summaries") or []),
            "probable_inspected_file_count": len(trace.get("probable_inspected_files") or []),
        }

    def _select_llm_matches(self, matches: list[dict[str, Any]], limit: int, *, question: str = "") -> list[dict[str, Any]]:
        if not matches:
            return []
        limit = max(1, int(limit or 1))
        intent = self._question_intent(question) if question else {}
        buckets = {
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
        stage_order = ("exact_lookup", "direct", "query_decomposition", "dependency", "two_hop", "tool_loop", "agent_trace", "agent_plan", "quality_gate")
        if intent.get("impact_analysis"):
            stage_order = ("exact_lookup", "direct", "impact_analysis", "tool_loop", "query_decomposition", "dependency", "two_hop", "agent_trace", "agent_plan", "quality_gate")
        if intent.get("test_coverage"):
            stage_order = ("exact_lookup", "direct", "test_coverage", "query_decomposition", "tool_loop", "dependency", "two_hop", "agent_trace", "agent_plan", "quality_gate")
        if intent.get("operational_boundary"):
            stage_order = ("exact_lookup", "direct", "operational_boundary", "query_decomposition", "tool_loop", "dependency", "two_hop", "agent_trace", "agent_plan", "quality_gate")
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
