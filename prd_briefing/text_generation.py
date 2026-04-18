from __future__ import annotations

import time

import requests

from .openai_client import OpenAIClient


class GeminiClient:
    def __init__(
        self,
        *,
        api_key: str | None,
        base_url: str,
        text_model: str,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.base_url = base_url.rstrip("/")
        self.text_model = text_model

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def create_answer(self, *, system_prompt: str, user_prompt: str) -> str:
        if not self.is_configured():
            raise RuntimeError("GEMINI_API_KEY is not configured.")
        payload = {
            "systemInstruction": {
                "parts": [{"text": system_prompt}],
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": user_prompt}],
                }
            ],
            "generationConfig": {
                "temperature": 0.2,
            },
        }
        last_error: Exception | None = None
        url = f"{self.base_url}/models/{self.text_model}:generateContent?key={self.api_key}"
        for attempt in range(4):
            try:
                response = requests.post(
                    url,
                    json=payload,
                    timeout=90,
                )
                response.raise_for_status()
                data = response.json()
                candidates = data.get("candidates") or []
                if not candidates:
                    raise RuntimeError("Gemini returned no candidates.")
                parts = candidates[0].get("content", {}).get("parts") or []
                text = "".join(str(part.get("text") or "") for part in parts).strip()
                if not text:
                    raise RuntimeError("Gemini returned an empty text response.")
                return text
            except requests.HTTPError as error:
                last_error = error
                status_code = error.response.status_code if error.response is not None else None
                if status_code not in {429, 500, 502, 503, 504} or attempt >= 3:
                    raise
                time.sleep(1.5 * (attempt + 1))
            except requests.RequestException as error:
                last_error = error
                if attempt >= 3:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if last_error:
            raise last_error
        raise RuntimeError("Gemini text generation failed without a response.")


class TextGenerationClient:
    def __init__(
        self,
        *,
        primary_openai: OpenAIClient,
        secondary_gemini: GeminiClient | None = None,
        priority: str = "openai_first",
    ) -> None:
        self.primary_openai = primary_openai
        self.secondary_gemini = secondary_gemini
        self.priority = priority

    @property
    def model_id(self) -> str:
        openai_id = f"openai:{self.primary_openai.text_model}" if self.primary_openai.is_configured() else "openai:none"
        gemini_id = (
            f"gemini:{self.secondary_gemini.text_model}"
            if self.secondary_gemini and self.secondary_gemini.is_configured()
            else "gemini:none"
        )
        return f"{openai_id}|{gemini_id}"

    def is_configured(self) -> bool:
        if self.primary_openai.is_configured():
            return True
        return bool(self.secondary_gemini and self.secondary_gemini.is_configured())

    def create_answer(self, *, system_prompt: str, user_prompt: str) -> str:
        errors: list[str] = []
        providers: list[tuple[str, object]] = []
        if self.priority == "gemini_first":
            if self.secondary_gemini and self.secondary_gemini.is_configured():
                providers.append(("Gemini", self.secondary_gemini))
            if self.primary_openai.is_configured():
                providers.append(("OpenAI", self.primary_openai))
        else:
            if self.primary_openai.is_configured():
                providers.append(("OpenAI", self.primary_openai))
            if self.secondary_gemini and self.secondary_gemini.is_configured():
                providers.append(("Gemini", self.secondary_gemini))

        for name, provider in providers:
            try:
                return provider.create_answer(system_prompt=system_prompt, user_prompt=user_prompt)
            except Exception as error:  # noqa: BLE001
                errors.append(f"{name}: {error}")
        if errors:
            raise RuntimeError(" | ".join(errors))
        raise RuntimeError("No configured text generation provider is available.")
