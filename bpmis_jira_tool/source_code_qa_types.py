from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any

from bpmis_jira_tool.errors import ToolError


@dataclass(frozen=True)
class RepositoryEntry:
    display_name: str
    url: str


class SourceCodeQAIndexUnavailable(sqlite3.OperationalError):
    """Raised when a user query cannot use a ready, prebuilt code index."""


@dataclass(frozen=True)
class LLMGenerateResult:
    payload: dict[str, Any]
    usage: dict[str, Any]
    model: str
    attempts: int
    latency_ms: int = 0
    attempt_log: tuple[dict[str, Any], ...] = ()


class SourceCodeQALLMError(ToolError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        provider_status: str = "",
        retryable: bool = False,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.provider_status = provider_status
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
