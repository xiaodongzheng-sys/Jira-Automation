from __future__ import annotations

from typing import Any


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
