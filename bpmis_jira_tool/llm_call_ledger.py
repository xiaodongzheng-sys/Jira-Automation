from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path
import threading
from typing import Any
from zoneinfo import ZoneInfo


LOGGER = logging.getLogger(__name__)
_LEDGER_LOCK = threading.Lock()
_SGT = ZoneInfo("Asia/Singapore")


def llm_call_ledger_path() -> Path:
    explicit = str(os.getenv("LLM_CALL_LEDGER_PATH") or "").strip()
    if explicit:
        return Path(explicit).expanduser()
    data_root = (
        str(os.getenv("LOCAL_AGENT_TEAM_PORTAL_DATA_DIR") or "").strip()
        or str(os.getenv("TEAM_PORTAL_DATA_DIR") or "").strip()
        or ".team-portal"
    )
    return Path(data_root).expanduser() / "llm_call_ledger.jsonl"


def infer_llm_flow(prompt_mode: str) -> str:
    mode = str(prompt_mode or "").strip().lower()
    if mode.startswith("codex_") or "source_code_qa" in mode:
        return "source_code_qa"
    if mode.startswith("monthly_report"):
        return "monthly_report"
    if mode.startswith("seatalk_") or "seatalk" in mode:
        return "seatalk"
    if mode.startswith("productization_"):
        return "productization"
    if mode.startswith("prd_reviewer") or mode.startswith("prd_review"):
        return "prd_reviewer"
    if mode.startswith("meeting_recorder"):
        return "meeting_recorder"
    if mode.startswith("prd_briefing"):
        return "prd_briefing"
    return "unknown"


def estimate_prompt_tokens(prompt: str, explicit_tokens: Any = None) -> int:
    try:
        value = int(explicit_tokens or 0)
    except (TypeError, ValueError):
        value = 0
    if value > 0:
        return value
    text = str(prompt or "")
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def record_llm_call(
    *,
    provider: str,
    flow: str,
    prompt_mode: str,
    route: str,
    model_id: str,
    reasoning_effort: str,
    status: str,
    latency_ms: int,
    estimated_prompt_tokens: int,
    prompt_chars: int,
    prompt_bytes: int,
    prompt_sha256: str,
    cache_hit: bool = False,
    repair_attempted: bool = False,
    error_category: str = "",
    error: str = "",
    trace_id: str = "",
    session_mode: str = "",
    command_mode: str = "",
    queue_wait_ms: int = 0,
    attempt_count: int = 1,
    extra: dict[str, Any] | None = None,
) -> None:
    try:
        now = datetime.now(_SGT)
        record: dict[str, Any] = {
            "timestamp": now.isoformat(),
            "timestamp_sgt": now.strftime("%Y-%m-%d %H:%M:%S SGT"),
            "provider": str(provider or ""),
            "flow": str(flow or infer_llm_flow(prompt_mode)),
            "prompt_mode": str(prompt_mode or ""),
            "route": str(route or ""),
            "model_id": str(model_id or ""),
            "reasoning_effort": str(reasoning_effort or ""),
            "status": str(status or ""),
            "latency_ms": max(0, int(latency_ms or 0)),
            "estimated_prompt_tokens": max(0, int(estimated_prompt_tokens or 0)),
            "prompt_chars": max(0, int(prompt_chars or 0)),
            "prompt_bytes": max(0, int(prompt_bytes or 0)),
            "prompt_sha256": str(prompt_sha256 or ""),
            "cache_hit": bool(cache_hit),
            "repair_attempted": bool(repair_attempted),
            "trace_id": str(trace_id or ""),
            "session_mode": str(session_mode or ""),
            "command_mode": str(command_mode or ""),
            "queue_wait_ms": max(0, int(queue_wait_ms or 0)),
            "attempt_count": max(1, int(attempt_count or 1)),
        }
        if error_category:
            record["error_category"] = str(error_category)
        if error:
            record["error"] = str(error)[:500]
        if extra:
            record["extra"] = {
                str(key): value
                for key, value in extra.items()
                if isinstance(value, (str, int, float, bool))
            }
        path = llm_call_ledger_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with _LEDGER_LOCK:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
    except Exception:
        LOGGER.debug("LLM call ledger write failed.", exc_info=True)


def prompt_sha256(prompt: str) -> str:
    return hashlib.sha256(str(prompt or "").encode("utf-8")).hexdigest()[:16]
