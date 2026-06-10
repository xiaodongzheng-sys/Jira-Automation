from __future__ import annotations

import logging
import os
from pathlib import Path

from bpmis_jira_tool.codex_model_router import CODEX_ROUTE_BALANCED, resolve_codex_model, resolve_codex_reasoning_effort
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa import (
    ClaudeCliBridgeSourceCodeQALLMProvider,
    CodexCliBridgeSourceCodeQALLMProvider,
)
from bpmis_jira_tool.source_code_qa_llm_providers import DEFAULT_CLAUDE_CLI_MODEL

LOGGER = logging.getLogger(__name__)


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

    def _build_payload(self, *, system_prompt: str, user_prompt: str) -> dict:
        return {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "codex_prompt_mode": self.prompt_mode,
            "_codex_reasoning_effort": self.codex_reasoning_effort,
            "_llm_ledger_flow": self._ledger_flow(),
            "_llm_ledger_route": CODEX_ROUTE_BALANCED,
        }

    def create_answer(self, *, system_prompt: str, user_prompt: str) -> str:
        result = self.provider.generate(
            payload=self._build_payload(system_prompt=system_prompt, user_prompt=user_prompt),
            primary_model=self.codex_model,
            fallback_model=self.codex_model,
        )
        return self.provider.extract_text(result.payload)

    def _ledger_flow(self) -> str:
        if self.prompt_mode.startswith("meeting_recorder"):
            return "meeting_recorder"
        return "prd_briefing"


class ClaudeFirstTextGenerationClient(CodexTextGenerationClient):
    """Text generation that prefers the local Claude Code CLI (Opus 4.8) and
    falls back to the Codex bridge on any Claude failure.

    Mirrors the daily brief's provider strategy (see
    ``SeaTalkDashboardService._run_codex_insights_prompt``) so meeting-recorder
    minutes can run on Opus 4.8 instead of the spend-capped Codex backend. The
    ``create_answer`` contract is identical to the parent, so callers need no
    change. On any Claude ``ToolError`` (e.g. CLI not installed / not logged in)
    it degrades gracefully to Codex.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        workspace_root: Path,
        prompt_mode: str,
        codex_model: str | None = None,
        claude_model: str | None = None,
        claude_binary: str | None = None,
    ) -> None:
        super().__init__(
            settings=settings,
            workspace_root=workspace_root,
            prompt_mode=prompt_mode,
            codex_model=codex_model,
        )
        self.claude_model = (
            str(claude_model or "").strip()
            or os.getenv("MEETING_RECORDER_CLAUDE_MODEL")
            or os.getenv("DAILY_BRIEF_CLAUDE_MODEL")
            or DEFAULT_CLAUDE_CLI_MODEL
        ).strip() or DEFAULT_CLAUDE_CLI_MODEL
        self.claude_binary = (
            str(claude_binary or "").strip()
            or os.getenv("MEETING_RECORDER_CLAUDE_BINARY")
            or os.getenv("DAILY_BRIEF_CLAUDE_BINARY")
            or "claude"
        ).strip() or "claude"
        self.claude_provider = ClaudeCliBridgeSourceCodeQALLMProvider(
            workspace_root=self.workspace_root,
            timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
            concurrency_limit=settings.source_code_qa_codex_concurrency,
            model=self.claude_model,
            claude_binary=self.claude_binary,
        )

    @property
    def model_id(self) -> str:
        return f"claude:{self.claude_model}"

    def create_answer(self, *, system_prompt: str, user_prompt: str) -> str:
        payload = self._build_payload(system_prompt=system_prompt, user_prompt=user_prompt)
        try:
            result = self.claude_provider.generate(
                payload=payload,
                primary_model=self.claude_model,
                fallback_model=self.claude_model,
            )
            return self.claude_provider.extract_text(result.payload)
        except ToolError as error:
            LOGGER.warning(
                "meeting_recorder_claude_cli_fallback_to_codex %s",
                str(error)[:300],
            )
        result = self.provider.generate(
            payload=payload,
            primary_model=self.codex_model,
            fallback_model=self.codex_model,
        )
        return self.provider.extract_text(result.payload)
