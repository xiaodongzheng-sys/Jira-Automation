from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CodexInitialPlan:
    candidate_matches: list[dict[str, Any]]
    candidate_paths: list[dict[str, Any]]
    candidate_path_layers: list[dict[str, Any]]
    scope_roots: list[str]
    prompt_mode: str
    prompt_runtime_evidence: list[dict[str, Any]]
    prompt_context: dict[str, Any]
    prompt_stats: dict[str, Any]
    llm_route: dict[str, Any]
    candidate_repo_count: int
    reasoning_effort: str


def build_codex_initial_plan(
    service: Any,
    *,
    entries: list[Any],
    key: str,
    pm_team: str,
    country: str,
    question: str,
    matches: list[dict[str, Any]],
    selected_matches: list[dict[str, Any]],
    evidence_pack: dict[str, Any],
    quality_gate: dict[str, Any],
    llm_route: dict[str, Any],
    selected_model: str,
    followup_context: dict[str, Any] | None,
    query_mode: str,
    routed_budget_mode: str,
    attachments: list[dict[str, Any]],
    runtime_evidence: list[dict[str, Any]],
    effort_assessment: bool = False,
) -> CodexInitialPlan:
    candidate_context = service._codex_initial_candidate_context(
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
    next_route = {
        **llm_route,
        **service._codex_initial_route_fields(
            selected_model=selected_model,
            prompt_mode=prompt_mode,
            candidate_paths=candidate_paths,
            candidate_path_layers=candidate_path_layers,
            scope_roots=scope_roots,
            query_mode=query_mode,
        ),
    }
    if effort_assessment:
        next_route["task"] = "effort_assessment"
    prompt_runtime_evidence = service._runtime_evidence_for_budget(runtime_evidence, routed_budget_mode)
    next_route = {
        **next_route,
        "runtime_evidence_count": len(runtime_evidence),
        "prompt_runtime_evidence_count": len(prompt_runtime_evidence),
    }
    prompt_context = service._codex_initial_prompt_context(
        prompt_mode=prompt_mode,
        pm_team=pm_team,
        country=country,
        question=question,
        candidate_paths=candidate_paths,
        evidence_pack=evidence_pack,
        quality_gate=quality_gate,
        followup_context=followup_context,
        attachments=attachments,
        runtime_evidence=prompt_runtime_evidence,
        scope_roots=scope_roots,
    )
    prompt_stats = service._codex_prompt_stats(prompt_context)
    next_route = {
        **next_route,
        "initial_prompt_estimated_tokens": prompt_stats["estimated_prompt_tokens"],
        "initial_prompt_chars": prompt_stats["prompt_chars"],
    }
    return CodexInitialPlan(
        candidate_matches=candidate_matches,
        candidate_paths=candidate_paths,
        candidate_path_layers=candidate_path_layers,
        scope_roots=scope_roots,
        prompt_mode=prompt_mode,
        prompt_runtime_evidence=prompt_runtime_evidence,
        prompt_context=prompt_context,
        prompt_stats=prompt_stats,
        llm_route=next_route,
        candidate_repo_count=len({item.get("repo") for item in candidate_paths}),
        reasoning_effort=service._codex_reasoning_effort_for_route(routed_budget_mode),
    )
