from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ChunkRecord:
    source_id: int
    owner_key: str
    session_id: str | None
    source_type: str
    title: str
    section_path: str
    content: str
    html_content: str
    image_refs: list[str]
    source_url: str
    updated_at: str
    embedding: list[float] | None = None
    score: float = 0.0


@dataclass
class Citation:
    title: str
    section_path: str
    source_type: str
    source_url: str
    snippet: str


@dataclass
class AnswerPayload:
    answer_text: str
    answer_language: str
    groundedness: str
    citations: list[Citation]
    audio_url: str | None = None
