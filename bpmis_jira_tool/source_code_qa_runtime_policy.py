from __future__ import annotations

from datetime import date
from pathlib import Path


LLM_PROMPT_VERSION = 11
LLM_RESPONSE_SCHEMA_VERSION = 5
LLM_ROUTER_VERSION = 12
LLM_CACHE_VERSION = 21
LLM_RUNTIME_VERSION = 2
PLANNER_TOOL_DSL_VERSION = 1
COMPACT_DEEP_BUDGET_MODE = "compact_deep"
LLM_PROMPT_COMPACT_THRESHOLD_TOKENS = 18_000
LLM_PROMPT_TIGHT_THRESHOLD_TOKENS = 24_000
LLM_TOKEN_ESTIMATE_CHARS_PER_TOKEN = 3.0
DEFAULT_SEMANTIC_INDEX_MODEL = "local-token-hybrid-v1"
DEFAULT_DOMAIN_PROFILE_PATH = Path(__file__).resolve().parent.parent / "config" / "source_code_qa_domain_profiles.json"
DEFAULT_DOMAIN_KNOWLEDGE_PACK_PATH = Path(__file__).resolve().parent.parent / "config" / "source_code_qa_domain_knowledge_packs.json"
DEFAULT_LLM_TIMEOUT_SECONDS = 90
DEFAULT_LLM_MAX_RETRIES = 2
DEFAULT_LLM_BACKOFF_SECONDS = 1.0
DEFAULT_LLM_MAX_BACKOFF_SECONDS = 8.0
DEFAULT_CODEX_CLI_MODEL = "codex-cli"
DEFAULT_CODEX_TIMEOUT_SECONDS = 360
DEFAULT_CODEX_TOP_PATH_LIMIT = 30
DEFAULT_CODEX_REPAIR_TOP_PATH_LIMIT = 16
DEFAULT_CODEX_REPAIR_PROMPT_TOKEN_LIMIT = 11_000
DEFAULT_CODEX_REPAIR_MIN_REMAINING_SECONDS = 90
DEFAULT_CODEX_DEEP_REPAIR_RESERVE_SECONDS = 75
CODEX_INVESTIGATION_PROMPT_MODE = "codex_investigation_brief_v5"
CODEX_SQL_GENERATION_PROMPT_MODE = "codex_sql_generation_brief_v1"
CODEX_SQL_RUNTIME_EVIDENCE_CHAR_LIMIT = 18_000
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
        "model": "codex-cli",
    },
    "balanced": {
        "match_limit": 6,
        "snippet_line_budget": 70,
        "snippet_char_budget": 8_000,
        "thinking_budget": 512,
        "max_output_tokens": 700,
        "model": "codex-cli",
    },
    "deep": {
        "match_limit": 12,
        "snippet_line_budget": 160,
        "snippet_char_budget": 24_000,
        "thinking_budget": 1024,
        "max_output_tokens": 1_400,
        "model": "codex-cli",
    },
    COMPACT_DEEP_BUDGET_MODE: {
        "match_limit": 8,
        "snippet_line_budget": 55,
        "snippet_char_budget": 8_000,
        "thinking_budget": 512,
        "max_output_tokens": 2_000,
        "model": "codex-cli",
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
