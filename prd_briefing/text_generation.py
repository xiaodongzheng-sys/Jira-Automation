from __future__ import annotations

import os
from pathlib import Path

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.source_code_qa import CodexCliBridgeSourceCodeQALLMProvider

from .openai_client import OpenAIClient


class TextGenerationClient:
    def __init__(self, *, primary_openai: OpenAIClient) -> None:
        self.primary_openai = primary_openai

    @property
    def model_id(self) -> str:
        return f"openai:{self.primary_openai.text_model}" if self.primary_openai.is_configured() else "openai:none"

    def is_configured(self) -> bool:
        return self.primary_openai.is_configured()

    def create_answer(self, *, system_prompt: str, user_prompt: str) -> str:
        if not self.primary_openai.is_configured():
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        return self.primary_openai.create_answer(system_prompt=system_prompt, user_prompt=user_prompt)


class CodexTextGenerationClient:
    def __init__(self, *, settings: Settings, workspace_root: Path, prompt_mode: str) -> None:
        self.settings = settings
        self.workspace_root = Path(workspace_root)
        self.prompt_mode = str(prompt_mode or "prd_briefing_codex").strip() or "prd_briefing_codex"
        self.codex_model = os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli")
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
            },
            primary_model=self.codex_model,
            fallback_model=self.codex_model,
        )
        return self.provider.extract_text(result.payload)
