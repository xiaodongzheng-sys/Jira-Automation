from __future__ import annotations

import html
import json
import os
from pathlib import Path
import re
from typing import Any

from bpmis_jira_tool.codex_model_router import CODEX_ROUTE_CHEAP, resolve_codex_model, resolve_codex_reasoning_effort
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa import CodexCliBridgeSourceCodeQALLMProvider


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def generate_productization_detailed_features_with_local_codex(
    prompt_items: list[dict[str, str]],
    *,
    settings: Settings,
) -> list[dict[str, str]]:
    provider = CodexCliBridgeSourceCodeQALLMProvider(
        workspace_root=PROJECT_ROOT,
        timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        concurrency_limit=settings.source_code_qa_codex_concurrency,
        session_mode="ephemeral",
        codex_binary=os.getenv("SOURCE_CODE_QA_CODEX_BINARY") or None,
    )
    prompt = (
        "Generate English Detailed Feature text for each Jira ticket from its Jira Description.\n"
        "Rules:\n"
        "- Output strict JSON only, with shape: {\"items\":[{\"jira_ticket_number\":\"...\",\"detailed_feature\":\"...\"}]}.\n"
        "- Keep one item per input Jira ticket and preserve the jira_ticket_number exactly.\n"
        "- Write in clear product/engineering English.\n"
        "- Summarize the functional change and expected behavior, not implementation chatter.\n"
        "- If the description is empty or not meaningful, use \"-\".\n"
        "- Do not include Markdown fences, citations, explanations, or Chinese text.\n\n"
        f"Input JSON:\n{json.dumps({'items': prompt_items}, ensure_ascii=False)}"
    )
    codex_model = resolve_codex_model(
        CODEX_ROUTE_CHEAP,
        legacy_env_names=("PRODUCTIZATION_CODEX_MODEL", "SOURCE_CODE_QA_CODEX_MODEL"),
    )
    result = provider.generate(
        payload={
            "systemInstruction": {"parts": [{"text": "You are a concise product feature summarizer."}]},
            "contents": [{"parts": [{"text": prompt}]}],
            "codex_prompt_mode": "productization_detailed_feature_v1",
            "_codex_reasoning_effort": resolve_codex_reasoning_effort(CODEX_ROUTE_CHEAP),
        },
        primary_model=codex_model,
        fallback_model=codex_model,
    )
    text = provider.extract_text(result.payload)
    payload = parse_codex_json_object(text)
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise ToolError("Codex returned an invalid Detailed Feature payload.")
    return [
        {
            "jira_ticket_number": str(item.get("jira_ticket_number") or "").strip(),
            "detailed_feature": clean_codex_productization_detailed_feature(str(item.get("detailed_feature") or "")),
        }
        for item in items
        if isinstance(item, dict)
    ]


def parse_codex_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end <= start:
            raise ToolError("Codex returned unreadable Detailed Feature JSON.") from error
        try:
            payload = json.loads(raw[start : end + 1])
        except json.JSONDecodeError as nested_error:
            raise ToolError("Codex returned unreadable Detailed Feature JSON.") from nested_error
    if not isinstance(payload, dict):
        raise ToolError("Codex returned an invalid Detailed Feature payload.")
    return payload


def clean_codex_productization_detailed_feature(value: str) -> str:
    text = format_productization_description_text(value)
    if not text:
        return "-"
    text = re.sub(r"```(?:json)?|```", "", text, flags=re.I).strip()
    return text or "-"


def format_productization_description_text(value: str) -> str:
    if not str(value or "").strip():
        return "-"

    text = html.unescape(value)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\r", "\n")
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.split("\n")]
    chunks = [line for line in lines if line]
    return "\n".join(chunks).strip() if chunks else "-"
