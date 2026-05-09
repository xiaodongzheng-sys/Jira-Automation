"""Internal components for the Source Code QA service.

These components are intentionally thin.  They give retrieval, answer
generation, and quality judging explicit ownership boundaries while preserving
the existing SourceCodeQAService private-method contract during refactors.
"""
from __future__ import annotations

from typing import Any


class _SourceCodeQAComponent:
    def __init__(self, service: Any) -> None:
        self._service = service


class SourceCodeQARetrievalComponent(_SourceCodeQAComponent):
    """Owns query-time retrieval orchestration."""

    def query_direct_and_decomposed_matches(self, **kwargs: Any) -> dict[str, Any]:
        return self._service._query_direct_and_decomposed_matches_impl(**kwargs)

    def rank_and_expand_query_matches(self, **kwargs: Any) -> tuple[list[dict[str, Any]], bool]:
        return self._service._rank_and_expand_query_matches_impl(**kwargs)

    def build_query_answer_context(self, **kwargs: Any) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        return self._service._build_query_answer_context_impl(**kwargs)


class SourceCodeQAAnswerGenerationComponent(_SourceCodeQAComponent):
    """Owns LLM/Codex answer construction and payload enrichment."""

    def augment_query_payload_with_llm_answer(self, **kwargs: Any) -> None:
        self._service._augment_query_payload_with_llm_answer_impl(**kwargs)

    def llm_answer_evidence_context(self, **kwargs: Any) -> dict[str, Any]:
        return self._service._llm_answer_evidence_context_impl(**kwargs)

    def build_llm_answer(self, **kwargs: Any) -> dict[str, Any]:
        return self._service._build_llm_answer_impl(**kwargs)

    def build_codex_llm_answer(self, **kwargs: Any) -> dict[str, Any]:
        return self._service._build_codex_llm_answer_impl(**kwargs)


class SourceCodeQAQualityJudgeComponent(_SourceCodeQAComponent):
    """Owns evidence sufficiency and deterministic answer judging."""

    def quality_gate_cached(
        self,
        question: str,
        evidence_summary: dict[str, Any],
        *,
        request_cache: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._service._quality_gate_cached_impl(
            question,
            evidence_summary,
            request_cache=request_cache,
        )

    def quality_gate(self, question: str, evidence_summary: dict[str, Any]) -> dict[str, Any]:
        return self._service._quality_gate_impl(question, evidence_summary)

    def quality_gate_trace_terms(
        self,
        question: str,
        evidence_summary: dict[str, Any],
        quality_gate: dict[str, Any],
        matches: list[dict[str, Any]],
    ) -> list[str]:
        return self._service._quality_gate_trace_terms_impl(question, evidence_summary, quality_gate, matches)

    def judge_answer(
        self,
        question: str,
        answer: str,
        evidence_pack: dict[str, Any],
        claim_check: dict[str, Any],
    ) -> dict[str, Any]:
        return self._service._judge_answer_impl(question, answer, evidence_pack, claim_check)

    def run_answer_judge(
        self,
        question: str,
        answer: str,
        evidence_pack: dict[str, Any],
        claim_check: dict[str, Any],
    ) -> dict[str, Any]:
        return self.judge_answer(question, answer, evidence_pack, claim_check)
