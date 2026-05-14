from __future__ import annotations

import os
from pathlib import Path

from bpmis_jira_tool.codex_model_router import CODEX_ROUTE_BALANCED, resolve_codex_model, resolve_codex_reasoning_effort
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.source_code_qa import CodexCliBridgeSourceCodeQALLMProvider


class CodexTextGenerationClient:
    def __init__(self, *, settings: Settings, workspace_root: Path, prompt_mode: str, codex_model: str | None = None) -> None:
        self.settings = settings
        self.workspace_root = Path(workspace_root)
        self.prompt_mode = str(prompt_mode or "prd_briefing_codex").strip() or "prd_briefing_codex"
        self.codex_model = resolve_codex_model(
            CODEX_ROUTE_BALANCED,
            legacy_env_names=("PRD_BRIEFING_CODEX_MODEL",),
            explicit_model=codex_model,
        )
        self.codex_reasoning_effort = resolve_codex_reasoning_effort(CODEX_ROUTE_BALANCED)
        self.provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=self.workspace_root,
            timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
            concurrency_limit=settings.source_code_qa_codex_concurrency,
            session_mode="ephemeral",
            codex_binary=os.getenv("SOURCE_CODE_QA_CODEX_BINARY") or None,
        )

    @property
    def model_id(self) -> str:
        return f"codex:{self.codex_model}"

    def is_configured(self) -> bool:
        return self.provider.ready()

    def create_answer(self, *, system_prompt: str, user_prompt: str) -> str:
        result = self.provider.generate(
            payload={
                "systemInstruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"parts": [{"text": user_prompt}]}],
                "codex_prompt_mode": self.prompt_mode,
                "_codex_reasoning_effort": self.codex_reasoning_effort,
                "_llm_ledger_flow": self._ledger_flow(),
                "_llm_ledger_route": CODEX_ROUTE_BALANCED,
            },
            primary_model=self.codex_model,
            fallback_model=self.codex_model,
        )
        return self.provider.extract_text(result.payload)

    def _ledger_flow(self) -> str:
        if self.prompt_mode.startswith("meeting_recorder"):
            return "meeting_recorder"
        return "prd_briefing"
