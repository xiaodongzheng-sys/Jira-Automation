from __future__ import annotations

from typing import Any

from bpmis_jira_tool.source_code_qa_runtime_policy import CODEX_INVESTIGATION_PROMPT_MODE


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
