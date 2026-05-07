from __future__ import annotations

import json
import re
import time
from typing import Any

import sqlparse

from bpmis_jira_tool.source_code_qa import ALL_COUNTRY


SQL_CODE_BLOCK_PATTERN = re.compile(r"```(?:sql|mysql|postgresql|postgres|sqlite|plsql|tsql)\s*\n(.*?)```", re.IGNORECASE | re.DOTALL)
SQL_INLINE_MARKER_PATTERN = re.compile(
    r"\bSQL\s*:\s*((?:WITH|SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|MERGE)\b.*)",
    re.IGNORECASE | re.DOTALL,
)
SQL_START_PATTERN = re.compile(r"^\s*(?:WITH|SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|MERGE)\b", re.IGNORECASE)
SQL_CTE_PATTERN = re.compile(r"(?:\bWITH|,)\s+([A-Za-z_][\w$]*)\s+AS\s*\(", re.IGNORECASE)
SQL_TABLE_REFERENCE_PATTERN = re.compile(
    r"\b(?:FROM|JOIN|UPDATE|INTO)\s+([`\"\[]?[A-Za-z_][\w$]*(?:[.`\"\]]+[A-Za-z_][\w$]*){0,2}[`\"\]]?)",
    re.IGNORECASE,
)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def extract_source_code_qa_sql_blocks(answer: str) -> list[str]:
    blocks: list[str] = []
    for text in source_code_qa_answer_text_candidates(answer):
        for match in SQL_CODE_BLOCK_PATTERN.finditer(text):
            sql = normalize_source_code_qa_sql_text(match.group(1))
            if sql:
                blocks.append(sql)
        if blocks:
            continue
        inline_sql = extract_source_code_qa_inline_sql(text)
        if inline_sql:
            blocks.append(inline_sql)
    return blocks


def source_code_qa_answer_text_candidates(answer: str) -> list[str]:
    raw = str(answer or "").strip()
    candidates: list[str] = []
    if not raw or not raw.startswith("{"):
        return [raw] if raw else []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return [raw] if raw else []
    if isinstance(parsed, dict):
        for key in ("sql", "query_sql", "generated_sql", "direct_answer", "answer", "summary"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())
    if candidates:
        return candidates
    candidates.append(raw)
    return candidates


def normalize_source_code_qa_sql_text(value: Any) -> str:
    sql = str(value or "").strip()
    if not sql:
        return ""
    sql = re.split(r"\n```", sql, maxsplit=1)[0].strip()
    if ";" in sql:
        sql = sql[: sql.rfind(";") + 1].strip()
    return sql if SQL_START_PATTERN.search(sql) else ""


def extract_source_code_qa_inline_sql(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    match = SQL_INLINE_MARKER_PATTERN.search(raw)
    if match:
        return normalize_source_code_qa_sql_text(match.group(1))
    if SQL_START_PATTERN.search(raw):
        return normalize_source_code_qa_sql_text(raw)
    return ""


def format_source_code_qa_sql_text(sql: str) -> str:
    raw = str(sql or "").strip()
    if not raw:
        return ""
    formatted = sqlparse.format(
        raw,
        keyword_case="upper",
        strip_whitespace=True,
        reindent_aligned=True,
        indent_width=2,
        wrap_after=88,
        use_space_around_operators=True,
    ).strip()
    return formatted or raw


def clean_source_code_qa_sql_identifier(value: str) -> str:
    identifier = str(value or "").strip().strip("`\"[]")
    identifier = re.sub(r"[`\"\[\]]", "", identifier)
    return identifier


def source_code_qa_sql_ctes(sql: str) -> list[str]:
    ctes: list[str] = []
    seen: set[str] = set()
    for match in SQL_CTE_PATTERN.finditer(str(sql or "")):
        cte = clean_source_code_qa_sql_identifier(match.group(1))
        key = cte.lower()
        if cte and key not in seen:
            seen.add(key)
            ctes.append(cte)
    return ctes


def source_code_qa_sql_tables(sql: str) -> list[str]:
    raw = str(sql or "")
    ctes = {cte.lower() for cte in source_code_qa_sql_ctes(raw)}
    tables: list[str] = []
    seen: set[str] = set()
    for match in SQL_TABLE_REFERENCE_PATTERN.finditer(raw):
        table = clean_source_code_qa_sql_identifier(match.group(1))
        key = table.lower()
        if not table or key in ctes or key in {"select", "values"} or key in seen:
            continue
        seen.add(key)
        tables.append(table)
    return tables


def source_code_qa_sql_logic_summary(sql: str) -> list[str]:
    raw = str(sql or "")
    upper = raw.upper()
    ctes = source_code_qa_sql_ctes(raw)
    tables = source_code_qa_sql_tables(raw)
    lines: list[str] = []
    if ctes:
        lines.append(f"- Builds intermediate CTE(s): {', '.join(ctes[:8])}.")
    if tables:
        lines.append(f"- Uses table(s): {', '.join(tables[:12])}.")
    if re.search(r"\bSELECT\b", upper):
        lines.append("- Returns rows from the final SELECT projection.")
    if re.search(r"\bJOIN\b", upper):
        lines.append("- Combines data through JOIN clause(s).")
    if re.search(r"\bWHERE\b", upper):
        lines.append("- Applies WHERE filter condition(s) from the generated SQL.")
    if re.search(r"\bGROUP\s+BY\b", upper) or re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", upper):
        lines.append("- Performs aggregation or grouping logic.")
    if re.search(r"\bORDER\s+BY\b", upper):
        lines.append("- Sorts the result set with ORDER BY.")
    if re.search(r"\bLIMIT\b|\bFETCH\s+FIRST\b", upper):
        lines.append("- Limits the number of returned rows.")
    return lines or ["- No rough SQL logic could be inferred automatically; review `query.sql` directly."]


def build_source_code_qa_sql_readme(
    *,
    pm_team: str,
    country: str,
    question: str,
    sql: str,
    result: dict[str, Any],
    runtime_evidence: list[dict[str, Any]],
) -> str:
    evidence_lines = []
    for item in runtime_evidence[:12]:
        source_type = str(item.get("source_type") or "runtime")
        filename = str(item.get("filename") or item.get("id") or "")
        scope = f"{item.get('pm_team') or ''}:{item.get('country') or ''}"
        evidence_lines.append(f"- {scope} {source_type}: {filename}")
    code_lines = []
    for index, match in enumerate(result.get("matches") or [], start=1):
        if not isinstance(match, dict) or index > 12:
            continue
        location = str(match.get("path") or "")
        if match.get("line_start"):
            location = f"{location}:{match.get('line_start')}"
        repo = str(match.get("repo") or "")
        code_lines.append(f"- S{index}: {repo} {location}".strip())
    tables = source_code_qa_sql_tables(sql)
    logic_lines = source_code_qa_sql_logic_summary(sql)
    return "\n".join(
        [
            "# Source Code Q&A SQL Package",
            "",
            f"- Scope: {str(pm_team or '').strip().upper()}:{str(country or '').strip().upper() or ALL_COUNTRY}",
            f"- Question: {str(question or '').strip()}",
            f"- Generated at: {_now()}",
            "",
            "## Files",
            "- `query.sql`: AI-generated SQL text.",
            "- `README.md`: this context and evidence summary.",
            "",
            "## SQL Rough Logic",
            *logic_lines,
            "",
            "## Tables Used",
            *(f"- `{table}`" for table in tables[:24]),
            *(["- No base table reference could be inferred automatically."] if not tables else []),
            "",
            "## Runtime Evidence",
            *(evidence_lines or ["- No runtime evidence was attached to this package."]),
            "",
            "## Source Code Evidence",
            *(code_lines or ["- No source-code matches were included in the response payload."]),
            "",
            "## Review Notes",
            "- This portal only generates SQL text. It does not connect to any database or execute the SQL.",
            "- Review table names, column names, filters, limits, and environment-specific schema before running it.",
            "- Treat data dictionary files as reference evidence and source-code paths as implementation evidence.",
        ]
    )
