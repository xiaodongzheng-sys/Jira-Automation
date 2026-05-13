"""Codex answer orchestration for Source Code Q&A.

This module keeps the first-pass Codex answer, repair decision, optional deep
investigation, and final payload assembly outside the main service class while
preserving the existing service helper contract.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any


LOGGER = logging.getLogger(__name__)


def _log_source_code_qa_timing(component: str, *, elapsed_ms: int, **fields: Any) -> None:
    payload: dict[str, Any] = {
        "event": "source_code_qa_timing",
        "component": component,
        "elapsed_ms": elapsed_ms,
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


def build_codex_llm_answer(
    service: Any,
    *,
    entries: list[Any],
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
    query_mode: str = "deep",
    trace_id: str = "",
    request_cache: dict[str, Any] | None = None,
    progress_callback: Any | None = None,
    attachments: list[dict[str, Any]] | None = None,
    runtime_evidence: list[dict[str, Any]] | None = None,
    effort_assessment: bool = False,
) -> dict[str, Any]:
    query_mode = service.normalize_query_mode(query_mode)
    timing: dict[str, int] = {}
    codex_started_at = time.time()

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
    llm_route = {
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
        llm_route["task"] = "effort_assessment"
    prompt_context = service._codex_initial_prompt_context(
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
    initial_prompt_stats = service._codex_prompt_stats(prompt_context)
    candidate_repo_count = len({item.get("repo") for item in candidate_paths})
    service._log_codex_prompt_timing(
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
    cache_key = service._answer_cache_key(
        provider=service.llm_provider.name,
        model=selected_model,
        question=question,
        answer_mode=requested_answer_mode,
        llm_budget_mode=routed_budget_mode,
        context=prompt_context,
    )
    cached = service._load_cached_answer(cache_key)
    if cached is not None:
        return service._cached_codex_answer_payload(
            cached=cached,
            question=question,
            structured_answer=service._parse_structured_answer(str(cached.get("answer") or "")),
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
    codex_cli_session_id = service._codex_cli_session_id(followup_context)
    initial_result = service._codex_initial_answer_result(
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
        reasoning_effort=service._codex_reasoning_effort_for_route(routed_budget_mode),
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
    repair_decision = service._codex_repair_decision(
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
    if repair_will_run and int(service.codex_repair_deadline_seconds or 0) > 0:
        elapsed_seconds = time.time() - codex_started_at
        if elapsed_seconds >= int(service.codex_repair_deadline_seconds):
            repair_will_run = False
            repair_skipped_reason = "codex_repair_deadline_after_initial_answer"
            _log_source_code_qa_timing(
                "codex_repair_skip",
                elapsed_ms=0,
                trace_id=trace_id,
                provider=service.llm_provider.name,
                model=selected_model,
                query_mode=query_mode,
                reason=repair_skipped_reason,
                phase="repair",
                deadline_seconds=int(service.codex_repair_deadline_seconds),
                elapsed_seconds=round(elapsed_seconds, 3),
                codex_initial_ms=codex_initial_ms,
            )
    if repair_will_run:
        repair_attempted = True
        repair_reason = "; ".join(severe_repair_reasons[:6])
        if deep_needed:
            deep_context = service._codex_deep_investigation_context(
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
        repair_context_result = service._codex_repair_answer_context(
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
    return service._codex_final_answer_payload(
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
