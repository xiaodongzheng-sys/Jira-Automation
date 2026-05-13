"""LLM budget routing and evidence-context assembly for Source Code QA answers."""
from __future__ import annotations

from typing import Any

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa_runtime_policy import (
    COMPACT_DEEP_BUDGET_MODE,
    LLM_PROMPT_COMPACT_THRESHOLD_TOKENS,
)


QUERY_MODE_DEEP = "deep"
ANSWER_MODE_AUTO = "auto"


def build_llm_answer(
    service: Any,
    *,
    entries: list[Any],
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
    if not service.llm_ready():
        raise ToolError(service.llm_unavailable_message())
    query_mode = service.normalize_query_mode(query_mode)
    routed_budget_mode, budget, llm_route = service._resolve_llm_budget(llm_budget_mode, question, matches)
    llm_route = {
        **llm_route,
        "query_mode": query_mode,
        "deadline_seconds": 0,
    }
    if effort_assessment:
        llm_route["task"] = "effort_assessment"
    selected_model = service._model_for_role_or_budget("answer", budget)
    answer_context = service._llm_answer_evidence_context(
        entries=entries,
        key=key,
        question=question,
        matches=matches,
        match_limit=int(budget["match_limit"]),
        pm_team=pm_team,
        country=country,
        request_cache=request_cache,
    )
    if (
        llm_route.get("mode") == "auto"
        and routed_budget_mode in {"balanced", "deep"}
        and service._answer_context_estimated_tokens(answer_context) > LLM_PROMPT_COMPACT_THRESHOLD_TOKENS
    ):
        original_budget_mode = routed_budget_mode
        routed_budget_mode = COMPACT_DEEP_BUDGET_MODE
        budget = service.llm_budgets[routed_budget_mode]
        selected_model = service._model_for_role_or_budget("answer", budget)
        answer_context = service._llm_answer_evidence_context(
            entries=entries,
            key=key,
            question=question,
            matches=matches,
            match_limit=int(budget["match_limit"]),
            pm_team=pm_team,
            country=country,
            request_cache=request_cache,
        )
        compact_estimated_tokens = service._answer_context_estimated_tokens(answer_context)
        llm_route = {
            **llm_route,
            "selected": routed_budget_mode,
            "reason": f"{llm_route.get('reason') or ''},prompt_token_pressure".strip(","),
            "token_pressure": True,
            "original_selected": original_budget_mode,
            "compact_estimated_tokens": compact_estimated_tokens,
        }
    selected_matches = answer_context["selected_matches"]
    evidence_summary = answer_context["evidence_summary"]
    quality_gate = answer_context["quality_gate"]
    evidence_pack = answer_context["evidence_pack"]
    return service._build_codex_llm_answer(
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
