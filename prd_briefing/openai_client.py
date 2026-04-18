from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests


class OpenAIClient:
    def __init__(
        self,
        *,
        api_key: str | None,
        base_url: str,
        text_model: str,
        embedding_model: str,
        transcription_model: str,
        tts_model: str,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.base_url = base_url.rstrip("/")
        self.text_model = text_model
        self.embedding_model = embedding_model
        self.transcription_model = transcription_model
        self.tts_model = tts_model

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not self.is_configured():
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        payload = {
            "model": self.embedding_model,
            "input": texts,
        }
        response = requests.post(
            f"{self.base_url}/embeddings",
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()["data"]
        return [item["embedding"] for item in data]

    def create_answer(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        if not self.is_configured():
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        payload = {
            "model": self.text_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
        }
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                response = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers=self._headers(),
                    json=payload,
                    timeout=90,
                )
                response.raise_for_status()
                return response.json()["choices"][0]["message"]["content"].strip()
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
        raise RuntimeError("OpenAI text generation failed without a response.")

    def synthesize_speech(
        self,
        *,
        text: str,
        voice: str,
        response_format: str = "mp3",
        instructions: str | None = None,
        speed: float | None = None,
    ) -> tuple[bytes, str]:
        if not self.is_configured():
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        payload = {
            "model": self.tts_model,
            "input": text,
            "voice": voice,
            "format": response_format,
        }
        if instructions:
            payload["instructions"] = instructions
        if speed is not None:
            payload["speed"] = speed
        response = requests.post(
            f"{self.base_url}/audio/speech",
            headers=self._headers(),
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
        return response.content, response_format

    def transcribe_audio(self, audio_path: Path) -> str:
        if not self.is_configured():
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        with audio_path.open("rb") as handle:
            response = requests.post(
                f"{self.base_url}/audio/transcriptions",
                headers={"Authorization": f"Bearer {self.api_key}"},
                data={"model": self.transcription_model},
                files={"file": (audio_path.name, handle, "audio/webm")},
                timeout=120,
            )
        response.raise_for_status()
        payload = response.json()
        return str(payload.get("text") or "").strip()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }


def safe_json_loads(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None
