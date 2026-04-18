from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SourceDocument:
    owner_key: str
    source_type: str
    external_id: str
    title: str
    language: str
    source_url: str
    updated_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


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


@dataclass
class VoiceProfile:
    voice_profile_id: str
    owner_key: str
    provider: str
    consent_status: str
    sample_language: str
    provider_voice_id: str | None
    sample_path: str | None
    created_at: str
    updated_at: str
