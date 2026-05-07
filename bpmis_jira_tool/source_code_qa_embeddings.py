from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account

from bpmis_jira_tool.errors import ToolError


VERTEX_AI_GLOBAL_API_BASE_URL = "https://aiplatform.googleapis.com/v1"
OPENAI_COMPATIBLE_API_BASE_URL = "https://api.openai.com/v1"
DEFAULT_VERTEX_EMBEDDING_MODEL = "gemini-embedding-001"
VERTEX_EMBEDDING_DOCUMENT_TASK = "RETRIEVAL_DOCUMENT"
VERTEX_EMBEDDING_QUERY_TASK = "CODE_RETRIEVAL_QUERY"


class SourceCodeQAEmbeddingProvider:
    name = "local_token_hybrid"

    def ready(self) -> bool:
        return True

    def embed_texts(self, texts: list[str], *, task_type: str | None = None) -> list[list[float]]:
        del task_type
        del texts
        return []

    def public_config(self) -> dict[str, Any]:
        return {"provider": self.name, "ready": self.ready()}


class OpenAICompatibleEmbeddingProvider(SourceCodeQAEmbeddingProvider):
    name = "openai_compatible"

    def __init__(self, *, api_key: str, api_base_url: str, model: str) -> None:
        self.api_key = str(api_key or "").strip()
        self.api_base_url = str(api_base_url or OPENAI_COMPATIBLE_API_BASE_URL).rstrip("/")
        self.model = str(model or "").strip()

    def ready(self) -> bool:
        return bool(self.api_key and self.model and not self.model.startswith("local-"))

    def embed_texts(self, texts: list[str], *, task_type: str | None = None) -> list[list[float]]:
        del task_type
        if not texts:
            return []
        if not self.ready():
            raise ToolError("Source Code Q&A embedding provider is not configured.")
        response = requests.post(
            f"{self.api_base_url}/embeddings",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={"model": self.model, "input": texts},
            timeout=90,
        )
        if not response.ok:
            detail = self._sanitize_error_detail(response.text)
            raise ToolError(f"Source Code Q&A embedding generation failed. {detail[:500]}")
        payload = response.json()
        rows = payload.get("data") or []
        rows.sort(key=lambda item: int(item.get("index") or 0))
        return [[float(value) for value in row.get("embedding") or []] for row in rows]

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "model": self.model,
            "api_base_url": self.api_base_url,
        }

    def _sanitize_error_detail(self, detail: str) -> str:
        sanitized = str(detail or "")
        if self.api_key:
            sanitized = sanitized.replace(self.api_key, "***")
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", sanitized)


class VertexAIEmbeddingProvider(SourceCodeQAEmbeddingProvider):
    name = "vertex_ai"
    _SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)

    def __init__(
        self,
        *,
        credentials_file: str | None = None,
        project_id: str | None = None,
        location: str = "global",
        model: str = DEFAULT_VERTEX_EMBEDDING_MODEL,
        output_dimensionality: int = 768,
    ) -> None:
        self.credentials_file = str(credentials_file or "").strip()
        self.project_id = str(project_id or "").strip()
        self.location = str(location or "global").strip() or "global"
        self.model = str(model or DEFAULT_VERTEX_EMBEDDING_MODEL).strip() or DEFAULT_VERTEX_EMBEDDING_MODEL
        self.output_dimensionality = max(0, int(output_dimensionality or 0))

    def ready(self) -> bool:
        return bool(self._credentials_path() and self._resolved_project_id() and self.model)

    def embed_texts(self, texts: list[str], *, task_type: str | None = None) -> list[list[float]]:
        if not texts:
            return []
        if not self.ready():
            raise ToolError(
                "Vertex AI embedding provider is not configured. Set SOURCE_CODE_QA_VERTEX_CREDENTIALS_FILE "
                "or GOOGLE_APPLICATION_CREDENTIALS, plus SOURCE_CODE_QA_VERTEX_PROJECT_ID when needed."
            )
        task = str(task_type or VERTEX_EMBEDDING_DOCUMENT_TASK).strip() or VERTEX_EMBEDDING_DOCUMENT_TASK
        embeddings: list[list[float]] = []
        for text in texts:
            response = requests.post(
                self._predict_url(),
                headers={
                    "Authorization": f"Bearer {self._access_token()}",
                    "Content-Type": "application/json",
                },
                json=self._predict_payload(str(text or "")[:8000], task),
                timeout=90,
            )
            if not response.ok:
                detail = self._sanitize_error_detail(response.text)
                raise ToolError(f"Vertex AI embedding generation failed. {detail[:500]}")
            embeddings.append(self._embedding_from_response(response.json()))
        return embeddings

    def public_config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "ready": self.ready(),
            "model": self.model,
            "project_id": self._resolved_project_id(),
            "location": self.location,
            "credentials_configured": bool(self._credentials_path()),
            "output_dimensionality": self.output_dimensionality,
        }

    def _credentials_path(self) -> Path | None:
        if not self.credentials_file:
            return None
        path = Path(self.credentials_file).expanduser()
        return path if path.exists() and path.is_file() else None

    def _resolved_project_id(self) -> str:
        if self.project_id:
            return self.project_id
        path = self._credentials_path()
        if path is None:
            return ""
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""
        return str(payload.get("project_id") or "").strip()

    def _access_token(self) -> str:
        path = self._credentials_path()
        if path is None:
            raise ToolError("Vertex AI credentials file is missing or unreadable.")
        credentials = service_account.Credentials.from_service_account_file(
            str(path),
            scopes=list(self._SCOPES),
        )
        credentials.refresh(GoogleAuthRequest())
        token = str(getattr(credentials, "token", "") or "").strip()
        if not token:
            raise ToolError("Vertex AI service account did not return an OAuth access token.")
        return token

    def _predict_url(self) -> str:
        location = self.location
        base_url = VERTEX_AI_GLOBAL_API_BASE_URL if location == "global" else f"https://{location}-aiplatform.googleapis.com/v1"
        return (
            f"{base_url}/projects/{self._resolved_project_id()}/locations/{location}"
            f"/publishers/google/models/{self.model}:predict"
        )

    def _predict_payload(self, text: str, task_type: str) -> dict[str, Any]:
        instance: dict[str, Any] = {"content": text, "task_type": task_type}
        parameters: dict[str, Any] = {}
        if self.output_dimensionality > 0:
            parameters["outputDimensionality"] = self.output_dimensionality
        return {"instances": [instance], "parameters": parameters}

    @staticmethod
    def _embedding_from_response(payload: dict[str, Any]) -> list[float]:
        predictions = payload.get("predictions") or []
        if not predictions:
            raise ToolError("Vertex AI embedding provider returned no predictions.")
        first = predictions[0] or {}
        candidates = [
            ((first.get("embeddings") or {}).get("values") if isinstance(first, dict) else None),
            ((first.get("embedding") or {}).get("values") if isinstance(first, dict) else None),
            first.get("values") if isinstance(first, dict) else None,
        ]
        values = next((item for item in candidates if isinstance(item, list)), None)
        if values is None:
            raise ToolError("Vertex AI embedding provider returned an unreadable embedding.")
        return [float(value) for value in values]

    @staticmethod
    def _sanitize_error_detail(detail: str) -> str:
        return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", str(detail or ""))
