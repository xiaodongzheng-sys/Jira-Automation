"""Answer cache and query telemetry helpers for Source Code QA."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bpmis_jira_tool.source_code_qa_llm_providers import LLM_PROVIDER_CODEX_CLI_BRIDGE


LOGGER = logging.getLogger(__name__)
QUERY_MODE_DEEP = "deep"


def _log_source_code_qa_timing(component: str, *, elapsed_ms: int, **fields: Any) -> None:
    payload = {
        "event": "source_code_qa_timing",
        "component": component,
        "elapsed_ms": max(0, int(elapsed_ms)),
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


def _answer_cache_key(self, *, provider: str, model: str, question: str, answer_mode: str, llm_budget_mode: str, context: str) -> str:
    normalized_question = " ".join(str(question or "").strip().lower().split())
    digest = hashlib.sha1(
        json.dumps(
            {
                "provider": provider,
                "model": model,
                "question": normalized_question,
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

def _answer_cache_metadata(key: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    return {
        "cache_key": key,
        "cached_at": payload.get("stored_at") or "",
        "generated_at": payload.get("stored_at") or "",
        "expire_at": payload.get("expire_at") or 0,
        "provider": payload.get("provider") or "",
        "model": payload.get("model") or "",
        "query_mode": payload.get("query_mode") or "",
        "llm_budget_mode": payload.get("llm_budget_mode") or "",
        "source": "answer_cache",
    }

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
    query_mode: str = "",
    llm_budget_mode: str = "",
    trace_id: str = "",
) -> None:
    started = time.perf_counter()
    self.answer_cache_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "versions": self._llm_versions(),
        "provider": provider,
        "model": model,
        "query_mode": self.normalize_query_mode(query_mode) if query_mode else "",
        "llm_budget_mode": str(llm_budget_mode or ""),
        "stored_at": self._now_iso(),
        "thinking_budget": int(thinking_budget or 0),
        "finish_reason": str(finish_reason or ""),
        "answer": answer,
        "usage": usage,
        "answer_quality": answer_quality or {},
        "expire_at": datetime.now(timezone.utc).timestamp() + self.llm_cache_ttl_seconds,
    }
    self._atomic_write_json(self.answer_cache_root / f"{key}.json", payload)
    _log_source_code_qa_timing(
        "cache_write",
        elapsed_ms=int((time.perf_counter() - started) * 1000),
        trace_id=trace_id,
        cache_key=key,
        provider=provider,
        model=model,
        query_mode=query_mode,
        llm_budget_mode=llm_budget_mode,
    )

def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(temp_path, path)

def _query_telemetry_match_counts(
    matches: list[dict[str, Any]],
) -> tuple[dict[str, int], dict[str, int]]:
    stage_counts: dict[str, int] = {}
    retrieval_counts: dict[str, int] = {}
    for match in matches:
        stage = str(match.get("trace_stage") or "direct")
        retrieval = str(match.get("retrieval") or "file_scan")
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
        retrieval_counts[retrieval] = retrieval_counts.get(retrieval, 0) + 1
    return stage_counts, retrieval_counts

def _query_telemetry_tool_trace_summary(tool_trace: list[Any]) -> dict[str, Any]:
    return {
        "steps": len(tool_trace),
        "phases": sorted({str(step.get("phase") or "unknown") for step in tool_trace if isinstance(step, dict)}),
        "tools": sorted({str(step.get("tool") or "unknown") for step in tool_trace if isinstance(step, dict)})[:20],
        "matches_added": sum(int(step.get("matches_added") or 0) for step in tool_trace if isinstance(step, dict)),
    }

def _query_telemetry_evidence_pack_summary(evidence_pack: dict[str, Any]) -> dict[str, Any]:
    return {
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
    }

def _query_telemetry_answer_policy_statuses(policies: list[Any]) -> dict[str, str]:
    return {
        str(policy.get("name") or "unknown"): str(policy.get("status") or "unknown")
        for policy in policies
        if isinstance(policy, dict)
    }

def _record_query_telemetry(
    self,
    *,
    key: str,
    question: str,
    answer_mode: str,
    llm_budget_mode: str,
    query_mode: str = QUERY_MODE_DEEP,
    payload: dict[str, Any],
    started_at: float,
) -> None:
    try:
        matches = payload.get("matches") or []
        answer_contract = payload.get("answer_contract") or {}
        evidence_pack = payload.get("evidence_pack") or {}
        tool_trace = payload.get("tool_trace") or []
        policies = answer_contract.get("policies") or (payload.get("answer_quality") or {}).get("policies") or []
        stage_counts, retrieval_counts = self._query_telemetry_match_counts(matches)
        record = {
            "timestamp": self._now_iso(),
            "key": key,
            "trace_id": payload.get("trace_id"),
            "question_sha1": hashlib.sha1(question.encode("utf-8")).hexdigest(),
            "question_preview": question[:180],
            "requested_answer_mode": answer_mode,
            "answer_mode": payload.get("answer_mode"),
            "query_mode": payload.get("query_mode") or self.normalize_query_mode(query_mode),
            "deadline_seconds": payload.get("deadline_seconds") or 0,
            "deadline_hit": bool(payload.get("deadline_hit")),
            "retrieval_latency_ms": payload.get("retrieval_latency_ms"),
            "codex_latency_ms": payload.get("codex_latency_ms"),
            "fallback_used": bool(payload.get("fallback_used")),
            "fallback_answer_quality": payload.get("fallback_answer_quality") or "",
            "fallback_evidence_count": payload.get("fallback_evidence_count") or 0,
            "fallback_claim_count": payload.get("fallback_claim_count") or 0,
            "deadline_fallback_reason": payload.get("deadline_fallback_reason") or "",
            "background_deep_job_id": payload.get("background_deep_job_id") or "",
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
            "latency_ms": int(payload.get("latency_ms") or int((time.time() - started_at) * 1000)),
            "elapsed_seconds": payload.get("elapsed_seconds"),
            "match_count": len(matches),
            "top_paths": [str(match.get("path") or "") for match in matches[:5]],
            "index_freshness": payload.get("index_freshness") or {},
            "trace_stage_counts": stage_counts,
            "retrieval_counts": retrieval_counts,
            "retrieval_runtime": payload.get("retrieval_runtime") or {},
            "query_timing": payload.get("query_timing") or {},
            "slow_query_attribution": payload.get("slow_query_attribution") or {},
            "cache_preload": payload.get("cache_preload") or {},
            "answer_quality": payload.get("answer_quality") or {},
            "answer_claim_check": payload.get("answer_claim_check") or {},
            "answer_judge": payload.get("answer_judge") or {},
            "codex_cli_summary": self._codex_telemetry_summary(payload),
            "tool_trace_summary": self._query_telemetry_tool_trace_summary(tool_trace),
            "answer_contract": answer_contract,
            "evidence_pack_summary": self._query_telemetry_evidence_pack_summary(evidence_pack),
            "answer_policy_statuses": self._query_telemetry_answer_policy_statuses(policies),
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



def attach_cache_telemetry_helpers(cls: type) -> None:
    cls._answer_cache_key = _answer_cache_key
    cls._load_cached_answer = _load_cached_answer
    cls._answer_cache_metadata = staticmethod(_answer_cache_metadata)
    cls._store_cached_answer = _store_cached_answer
    cls._atomic_write_json = staticmethod(_atomic_write_json)
    cls._query_telemetry_match_counts = staticmethod(_query_telemetry_match_counts)
    cls._query_telemetry_tool_trace_summary = staticmethod(_query_telemetry_tool_trace_summary)
    cls._query_telemetry_evidence_pack_summary = staticmethod(_query_telemetry_evidence_pack_summary)
    cls._query_telemetry_answer_policy_statuses = staticmethod(_query_telemetry_answer_policy_statuses)
    cls._record_query_telemetry = _record_query_telemetry
    cls._codex_telemetry_summary = staticmethod(_codex_telemetry_summary)
