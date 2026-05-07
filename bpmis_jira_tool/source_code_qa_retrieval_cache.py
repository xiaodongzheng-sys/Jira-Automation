"""Request-local retrieval cache helpers for Source Code QA."""
from __future__ import annotations

import json
import time
from typing import Any


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

def _increment_retrieval_stat(request_cache: dict[str, Any] | None, key: str) -> None:
    if request_cache is None:
        return
    stats = request_cache.setdefault("stats", {})
    stats[key] = int(stats.get(key) or 0) + 1

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

def _clone_jsonish(payload: Any) -> Any:
    try:
        return json.loads(json.dumps(payload, ensure_ascii=False))
    except (TypeError, ValueError):
        if isinstance(payload, list):
            return [dict(item) if isinstance(item, dict) else item for item in payload]
        if isinstance(payload, dict):
            return dict(payload)
        return payload



def attach_retrieval_cache_helpers(cls: type) -> None:
    cls._new_retrieval_request_cache = staticmethod(_new_retrieval_request_cache)
    cls._increment_retrieval_stat = staticmethod(_increment_retrieval_stat)
    cls._retrieval_cache_stats = staticmethod(_retrieval_cache_stats)
    cls._clone_jsonish = staticmethod(_clone_jsonish)
