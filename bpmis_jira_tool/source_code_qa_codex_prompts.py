from __future__ import annotations

from typing import Any

from bpmis_jira_tool.source_code_qa_runtime_policy import (
    CODEX_INVESTIGATION_PROMPT_MODE,
    CODEX_SQL_GENERATION_PROMPT_MODE,
)


def codex_system_instruction() -> str:
    return (
        "You are Codex running as a read-only code investigator for Source Code Q&A. "
        "Use shell/file inspection to verify the answer from the synced repository workspace. "
        "Do not edit files, install dependencies, create commits, deploy, or run mutating commands. "
        "Prefer rg, sed, nl, and direct file reads. "
        "Always follow the three-stage investigation contract: first discover candidate evidence, then verify gaps/absence with targeted searches, then answer with explicit certainty levels. "
        "Return JSON with direct_answer, investigation_steps, attachment_facts, screenshot_evidence, source_code_evidence, confirmed_from_code, inferred_from_code, not_found, missing_production_evidence, next_checks, claims, missing_evidence, and confidence. "
        "Put only verified production/config code facts in confirmed_from_code; put weaker deductions in inferred_from_code. "
        "For screenshot-driven questions, extract visible screenshot facts first, then tie them to code paths/functions/fields; never present screenshot content as repository fact. "
        "When source evidence is incomplete, put the exact missing repository/table/config/log/export in not_found and missing_evidence instead of filling the gap from naming or prior assumptions. "
        "Every concrete code claim must cite either an evidence id like S1 or a real file reference like path/to/File.java:10-20."
    )


def build_codex_payload(
    prompt: str,
    *,
    prompt_mode: str,
    system_instruction: str,
    prompt_stats: dict[str, Any],
    progress_callback: Any | None = None,
    codex_cli_session_id: str = "",
    image_paths: list[str] | None = None,
    trace_id: str = "",
    phase: str = "",
    candidate_path_count: int = 0,
    candidate_repo_count: int = 0,
    repair_issue_count: int = 0,
) -> dict[str, Any]:
    payload = {
        "codex_prompt_mode": prompt_mode,
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "generationConfig": {"temperature": 0.1, "responseMimeType": "application/json"},
    }
    stats = prompt_stats if isinstance(prompt_stats, dict) else {}
    payload["_codex_trace_id"] = str(trace_id or "")
    payload["_codex_phase"] = str(phase or "")
    payload["_codex_prompt_chars"] = int(stats.get("prompt_chars") or 0)
    payload["_codex_prompt_bytes"] = int(stats.get("prompt_bytes") or 0)
    payload["_codex_estimated_prompt_tokens"] = int(stats.get("estimated_prompt_tokens") or 0)
    payload["_codex_candidate_path_count"] = int(candidate_path_count or 0)
    payload["_codex_candidate_repo_count"] = int(candidate_repo_count or 0)
    payload["_codex_repair_issue_count"] = int(repair_issue_count or 0)
    if codex_cli_session_id:
        payload["codex_cli_session_id"] = codex_cli_session_id
    if image_paths:
        payload["_codex_image_paths"] = list(image_paths)
    if progress_callback:
        payload["_progress_callback"] = progress_callback
    return payload


def build_codex_repair_brief(
    *,
    pm_team: str,
    country: str,
    question: str,
    initial_answer: str,
    scope_roots: list[dict[str, str]],
    candidate_paths: list[dict[str, Any]],
    attachment_section: str,
    repair_issues: list[str],
) -> str:
    answer = str(initial_answer or "").strip()
    if len(answer) > 7000:
        answer = f"{answer[:7000]}\n...[initial answer truncated]"
    lines = [
        f"Prompt mode: {CODEX_INVESTIGATION_PROMPT_MODE}",
        f"PM Team: {pm_team}",
        f"Country: {country}",
        f"Question: {question}",
        "",
        "Task: repair the previous Codex answer only for the severe issues below.",
        "- Search only the allowed scope roots for this PM team/country.",
        "- Do not cite sibling PM teams, countries, or repos outside the allowed scope roots.",
        "- Use candidate paths only as starting hints; run scoped `rg`/file reads when needed.",
        "- Return the same JSON contract as the initial answer.",
        "",
        "Allowed scope roots:",
    ]
    for item in scope_roots:
        lines.append(
            f"- repo={item.get('repo')} root={item.get('repo_root')} "
            f"relative_root={item.get('repo_relative_root')}"
        )
    lines.extend(["", "Severe repair reasons:"])
    lines.extend(f"- {issue}" for issue in repair_issues[:10])
    if candidate_paths:
        lines.extend(["", "Starting path hints:"])
        for item in candidate_paths[:10]:
            lines.append(
                f"- {item.get('id')} repo={item.get('repo')} root={item.get('repo_root')} "
                f"relative_root={item.get('repo_relative_root')} path={item.get('path')} "
                f"file_exists={item.get('file_exists')} lines={item.get('line_start')}-{item.get('line_end')}"
            )
    if attachment_section:
        lines.extend(["", attachment_section])
    lines.extend(
        [
            "",
            "Previous answer to repair:",
            answer or "(empty)",
            "",
            "Final answer contract:",
            '- Return JSON: {"direct_answer":"...","investigation_steps":{"candidate_evidence":["..."],"gap_verification":["..."],"certainty_split":["..."]},"attachment_facts":["..."],"screenshot_evidence":["..."],"source_code_evidence":["file/function/field evidence..."],"confirmed_from_code":["..."],"inferred_from_code":["..."],"not_found":["..."],"missing_production_evidence":["..."],"next_checks":["..."],"claims":[{"text":"...","citations":["S1 or scoped/file/path.java:10-20"]}],"missing_evidence":[],"confidence":"high|medium|low"}.',
            "- Cite concrete code claims with S ids from starting hints or direct file:line references inside the allowed scope roots.",
            "- If evidence is missing, state the exact missing link instead of guessing.",
        ]
    )
    return "\n".join(lines)


def build_codex_sql_generation_brief(
    *,
    pm_team: str,
    country: str,
    question: str,
    candidate_paths: list[dict[str, Any]],
    evidence_pack: dict[str, Any],
    quality_gate: dict[str, Any],
    followup_context: dict[str, Any] | None,
    scope_roots: list[dict[str, str]] | None,
    attachment_section: str,
    runtime_section: str,
) -> str:
    normalized_scope_roots = [
        item for item in (scope_roots or [])
        if isinstance(item, dict) and str(item.get("repo_root") or "").strip()
    ]
    pm_team_label = str(pm_team or "selected").strip() or "selected"
    lines = [
        f"Prompt mode: {CODEX_SQL_GENERATION_PROMPT_MODE}",
        f"PM Team: {pm_team}",
        f"Country: {country}",
        f"Question: {question}",
        "",
        "Role:",
        f"- You are the {pm_team_label} PM Team's Source Code & Runtime Evidence Assistant.",
        "- Produce a code-backed SQL answer using repository evidence plus uploaded data dictionary/runtime evidence.",
        "",
        "SQL task rules:",
        "- Still inspect source code before answering. Candidate paths and data dictionary rows are hints, not proof.",
        "- Use read-only `rg` and file reads inside the allowed scope roots to verify mapper/XML/DAO/repository/source SQL, table joins, filters, enums, and timestamp assumptions.",
        "- Start direct_answer with the table/row selection logic and business meaning. Do not start with country DB routing, no-country-filter, or execution caveats.",
        "- For AF and GRC, data_dictionary uploads apply to SG, ID, PH, and All; table and field definitions are shared across countries.",
        "- For AF and GRC, the selected country runs against that country's separate DB instance. Mention this as an execution caveat only after the SQL/table logic if relevant.",
        "- For GRC reviewer SQL questions, RC and Compliance are business aliases for the same review stage.",
        "- Put generated SQL in a fenced ```sql block or a `sql` field so the portal can build query.sql.",
        "- If a requested column is not explicitly defined, state the best code-backed approximation and list the missing evidence.",
        "",
        "Allowed scope roots:",
    ]
    if normalized_scope_roots:
        for item in normalized_scope_roots:
            lines.append(
                f"- repo={item.get('repo')} root={item.get('repo_root')} "
                f"relative_root={item.get('repo_relative_root')}"
            )
    else:
        lines.append("- No explicit allowlist was provided; stay within the selected synced repository workspace.")
    if candidate_paths:
        lines.extend(["", "Starting code path hints from retrieval:"])
        for item in candidate_paths[:12]:
            original_path = str(item.get("original_path") or "").strip()
            path_note = f" original_path={original_path}" if original_path else ""
            lines.append(
                f"- {item.get('id')} repo={item.get('repo')} root={item.get('repo_root')} "
                f"relative_root={item.get('repo_relative_root')} path={item.get('path')}{path_note} "
                f"file_exists={item.get('file_exists')} path_status={item.get('path_status')} "
                f"lines={item.get('line_start')}-{item.get('line_end')} reason={item.get('reason')}"
            )
    lines.extend(
        [
            "",
            "Compact evidence pack:",
            f"- Quality gate: {quality_gate.get('status')} / confidence={quality_gate.get('confidence')} / missing={', '.join(quality_gate.get('missing') or []) or 'none'}",
        ]
    )
    for label, key in (
        ("Tables", "tables"),
        ("Read/write points", "read_write_points"),
        ("Entry points", "entry_points"),
        ("Concrete data sources", "data_sources"),
        ("Source tiers", "source_tiers"),
        ("Missing hops", "missing_hops"),
    ):
        values = evidence_pack.get(key) or []
        if values:
            lines.append(f"- {label}:")
            for value in values[:8]:
                lines.append(f"  - {value}")
    typed_items = evidence_pack.get("items") or []
    if typed_items:
        lines.append("- Typed evidence items:")
        for item in typed_items[:10]:
            if not isinstance(item, dict):
                continue
            location = f" [{item.get('source_id')}]" if item.get("source_id") else ""
            lines.append(
                f"  - {item.get('type')} / confidence={item.get('confidence')} / hop={item.get('hop')}{location}: {item.get('claim')}"
            )
    if attachment_section:
        lines.extend(["", attachment_section])
    if runtime_section:
        lines.extend(["", runtime_section])
    if followup_context:
        previous_question = str(followup_context.get("question") or "").strip()
        previous_answer = str(followup_context.get("rendered_answer") or followup_context.get("answer") or "").strip()
        if previous_question or previous_answer:
            lines.extend(["", "Follow-up context:"])
            if previous_question:
                lines.append(f"- Previous question: {previous_question[:500]}")
            if previous_answer:
                lines.append(f"- Previous answer summary: {previous_answer[:900]}")
    lines.extend(
        [
            "",
            "Final answer contract:",
            '- Return JSON: {"direct_answer":"...","sql":"...","sql_logic":["..."],"tables_used":["..."],"source_code_evidence":["file/function/field/table evidence..."],"runtime_evidence":["data dictionary/runtime evidence..."],"confirmed_from_code":["..."],"inferred_from_code":["..."],"missing_evidence":["..."],"claims":[{"text":"...","citations":["S1 or scoped/file/path.java:10-20"]}],"confidence":"high|medium|low"}.',
            "- direct_answer should be concise but include the same useful table strategy as a normal Source Code Q&A answer.",
            "- source_code_evidence must name concrete files, mappers, SQL fragments, functions, classes, fields, tables, or APIs.",
            "- If the SQL depends on a runtime parameter, use a named placeholder such as :incident_id and explain it.",
        ]
    )
    return "\n".join(lines)
