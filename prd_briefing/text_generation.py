from __future__ import annotations

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
