from __future__ import annotations

import json
import math
import re
import threading
import asyncio
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from .confluence import ConfluenceConnector, ParsedSection
from .models import AnswerPayload, ChunkRecord, Citation
from .openai_client import OpenAIClient
from .storage import BriefingStore, citation_from_chunk
from .text_generation import TextGenerationClient


DEVELOPER_AUDIENCE = "developer_zh"
DEVELOPER_AUDIENCE_EN = "developer_en"
DEVELOPER_LANGUAGE = "Mandarin Chinese"
DEVELOPER_LANGUAGE_EN = "English"

WALKTHROUGH_SCRIPT_PROMPT_VERSION = "v1_openai_only_pm_briefing"
WALKTHROUGH_BLOCK_PROMPT_VERSION = "v1_pm_briefing_block"
SESSION_BRIEF_PROMPT_VERSION = "v7_two_part_chinese_summary"
WALKTHROUGH_PREWARM_LIMIT = 12

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "be",
    "for",
    "how",
    "in",
    "is",
    "of",
    "or",
    "the",
    "to",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
}


def normalize_walkthrough_language(language: str | None) -> str:
    normalized = str(language or "zh").strip().lower()
    return "en" if normalized in {"en", "english"} else "zh"


def walkthrough_audience(language: str | None) -> str:
    return DEVELOPER_AUDIENCE_EN if normalize_walkthrough_language(language) == "en" else DEVELOPER_AUDIENCE


def walkthrough_language_code(language: str | None) -> str:
    return "en" if normalize_walkthrough_language(language) == "en" else "zh"


def walkthrough_language_name(language: str | None) -> str:
    return DEVELOPER_LANGUAGE_EN if normalize_walkthrough_language(language) == "en" else DEVELOPER_LANGUAGE


def walkthrough_language_from_session(session: dict[str, Any]) -> str:
    return "en" if str(session.get("audience") or "").strip() == DEVELOPER_AUDIENCE_EN else "zh"


def build_walkthrough_section_system_prompt(language: str | None = "zh") -> str:
    language = normalize_walkthrough_language(language)
    base = (
        f"You are a product manager briefing a PRD section to software engineers in {walkthrough_language_name(language)}. "
        "Speak the way PMs normally align requirements with developers during grooming or walkthrough sessions. "
        "Be direct, practical, and structured. First explain the purpose of this section, then the main flow, "
        "then what developers need to build or pay attention to. Call out scope, user actions, system behavior, "
        "validation rules, dependencies, edge cases, and any implementation-sensitive details when present. "
        "Do not sound like a keynote presenter. Do not read the PRD word for word. "
        "Do not mechanically read every field, bullet, or table row. Summarize dense tables into what engineering should understand. "
    )
    if language == "en":
        return base + (
            "Use natural spoken English for a developer walkthrough: frame the goal first, then explain the flow, "
            "what changes on the page, what gets triggered, what should be validated, and what cases developers need to watch."
        )
    return base + (
        "Use spoken PM phrasing that feels normal in a dev sync, for example framing the goal first, then saying what the flow is, "
        "what changes on the page, what gets triggered, what should be validated, and what cases developers need to pay attention to."
    )


def build_walkthrough_section_user_prompt(*, section: dict[str, Any], notes: list[str], language: str | None = "zh") -> str:
    body = (
        f"Section: {section['section_path']}\n\n"
        f"Presenter summary:\n{section.get('briefing_summary', '')}\n\n"
        f"Presenter notes:\n- " + "\n- ".join(notes) + "\n\n"
        f"Source:\n{section['content']}\n\n"
    )
    if normalize_walkthrough_language(language) == "en":
        return body + (
            "Write a natural spoken script of around 5 to 9 sentences in English. "
            "The first sentence should explain why this section matters to implementation. "
            "Then explain the intended flow in order. "
            "After that, highlight the key engineering takeaways, such as important rules, triggers, state changes, "
            "input or output expectations, and any edge cases or risks implied by the source. "
            "If the section is mostly UI fields, summarize the pattern and only name the most important fields. "
            "Make it sound like live PM speech rather than written prose. "
            "Natural phrasing is encouraged, such as: "
            "'This part is mainly about...', 'For implementation, focus on...', 'The actual flow is...', "
            "'The key thing to validate is...', 'There are many fields here, but the core idea is...'. "
            "Do not force all phrases in every answer, but keep the overall tone close to that style."
        )
    return body + (
        "Write a natural spoken script of around 5 to 9 sentences in Mandarin. "
        "The first sentence should explain why this section matters to implementation. "
        "Then explain the intended flow in order. "
        "After that, highlight the key engineering takeaways, such as important rules, triggers, state changes, "
        "input or output expectations, and any edge cases or risks implied by the source. "
        "If the section is mostly UI fields, summarize the pattern and only name the most important fields. "
        "Make it sound like live PM speech rather than written prose. "
        "Natural phrasing is encouraged, such as: "
        "'这一块主要是...', '开发这里重点看...', '实际 flow 是...', '这里需要注意...', "
        "'这个字段很多，但本质上是为了...', '异常情况主要是...'. "
        "Do not force all phrases in every answer, but keep the overall tone close to that style."
    )


def build_walkthrough_block_system_prompt(language: str | None = "zh") -> str:
    return (
        f"You are a product manager briefing related PRD sections to software engineers in {walkthrough_language_name(language)}. "
        "The input is already grouped into one product briefing block, so explain the merged capability instead of reading each Confluence section separately. "
        "Be concrete about user flow, page behavior, field or status rules, backend/system responsibilities, dependencies, and implementation risks. "
        "Do not read the PRD word for word. Keep the tone like a live PM walkthrough for developers."
    )


def build_walkthrough_block_user_prompt(*, block: dict[str, Any], source_lines: list[str], language: str | None = "zh") -> str:
    body = (
        f"Briefing block: {block['title']}\n\n"
        f"Briefing goal:\n{block.get('briefing_goal', '')}\n\n"
        f"Merged summary:\n{block.get('merged_summary', '')}\n\n"
        f"Developer focus:\n- " + "\n- ".join(block.get("developer_focus") or []) + "\n\n"
        f"Related PRD source sections:\n\n" + "\n\n".join(source_lines) + "\n\n"
    )
    if normalize_walkthrough_language(language) == "en":
        return body + (
            "Write a natural spoken script of around 7 to 12 sentences in English. "
            "Start with why this merged module matters. Then explain the user/system flow in order. "
            "Call out the related PRD sections by their product meaning, but do not mechanically enumerate section numbers. "
            "End with what developers should double-check before implementation or QA."
        )
    return body + (
        "Write a natural spoken script of around 7 to 12 sentences in Mandarin. "
        "Start with why this merged module matters. Then explain the user/system flow in order. "
        "Call out the related PRD sections by their product meaning, but do not mechanically enumerate section numbers. "
        "End with what developers should double-check before implementation or QA."
    )


class RetrievalService:
    def __init__(self, openai_client: OpenAIClient) -> None:
        self.openai_client = openai_client

    def rank(self, query: str, chunks: list[ChunkRecord], limit: int = 6) -> list[ChunkRecord]:
        if not chunks:
            return []
        if self.openai_client.is_configured():
            try:
                query_embedding = self.openai_client.embed_texts([query])[0]
                for chunk in chunks:
                    if chunk.embedding:
                        chunk.score = cosine_similarity(query_embedding, chunk.embedding)
                    else:
                        chunk.score = keyword_score(query, chunk.content)
            except Exception:  # noqa: BLE001
                for chunk in chunks:
                    chunk.score = keyword_score(query, chunk.content)
        else:
            for chunk in chunks:
                chunk.score = keyword_score(query, chunk.content)
        return [chunk for chunk in sorted(chunks, key=lambda item: item.score, reverse=True)[:limit] if chunk_has_signal(chunk)]


class VoiceService:
    def __init__(
        self,
        *,
        store: BriefingStore,
        openai_client: OpenAIClient,
        tts_provider: str,
        edge_mandarin_voice: str,
        edge_english_voice: str,
        edge_rate: str,
        openai_mandarin_voice: str,
        openai_voice_speed: float,
        openai_custom_voice_enabled: bool,
        openai_tts_fallback_enabled: bool,
        elevenlabs_api_key: str | None,
        elevenlabs_mandarin_model_id: str,
        elevenlabs_mandarin_voice_id: str | None,
    ) -> None:
        self.store = store
        self.openai_client = openai_client
        self.tts_provider = str(tts_provider or "edge").strip().lower() or "edge"
        self.edge_mandarin_voice = str(edge_mandarin_voice or "zh-CN-XiaoxiaoNeural").strip() or "zh-CN-XiaoxiaoNeural"
        self.edge_english_voice = str(edge_english_voice or "en-US-JennyNeural").strip() or "en-US-JennyNeural"
        self.edge_rate = str(edge_rate or "-8%").strip() or "-8%"
        self.openai_mandarin_voice = openai_mandarin_voice
        self.openai_voice_speed = openai_voice_speed
        self.openai_custom_voice_enabled = openai_custom_voice_enabled
        self.openai_tts_fallback_enabled = openai_tts_fallback_enabled
        self.elevenlabs_api_key = (elevenlabs_api_key or "").strip()
        self.elevenlabs_mandarin_model_id = elevenlabs_mandarin_model_id
        self.elevenlabs_mandarin_voice_id = (elevenlabs_mandarin_voice_id or "").strip()

    def synthesize(self, *, session_id: str, text: str, language_code: str, owner_key: str) -> str | None:
        normalized_text = optimize_tts_text(text, language_code=language_code)
        voice_id = None
        audio_bytes: bytes | None = None
        suffix = "mp3"
        provider = ""
        model_id = ""

        elevenlabs_voice_id = self.elevenlabs_mandarin_voice_id

        if self.tts_provider == "edge":
            voice_id = self._edge_voice_for_language(language_code)
            provider = "edge"
            model_id = f"edge-tts:{self.edge_rate}"
            cached = self.store.get_cached_audio(
                owner_key=owner_key,
                provider=provider,
                voice_id=voice_id,
                language_code=language_code,
                model_id=model_id,
                text=normalized_text,
            )
            if cached:
                return cached
            try:
                audio_bytes = self._synthesize_with_edge_tts(text=normalized_text, voice_id=voice_id)
            except Exception:  # noqa: BLE001
                audio_bytes = None
            suffix = "mp3"
        elif self.tts_provider == "elevenlabs" and self.elevenlabs_api_key and elevenlabs_voice_id:
            provider = "elevenlabs"
            model_id = self.elevenlabs_mandarin_model_id
            cached = self.store.get_cached_audio(
                owner_key=owner_key,
                provider=provider,
                voice_id=elevenlabs_voice_id,
                language_code=language_code,
                model_id=model_id,
                text=normalized_text,
            )
            if cached:
                return cached
            audio_bytes = self._synthesize_with_elevenlabs(
                text=normalized_text,
                voice_id=elevenlabs_voice_id,
                language_code=language_code,
                model_id=model_id,
            )
            suffix = "mp3"
        elif self.tts_provider == "openai" and self.openai_tts_fallback_enabled and self.openai_client.is_configured():
            voice_id = self.openai_mandarin_voice
            provider = "openai"
            model_id = str(self.openai_client.tts_model)
            cached = self.store.get_cached_audio(
                owner_key=owner_key,
                provider=provider,
                voice_id=voice_id,
                language_code=language_code,
                model_id=model_id,
                text=normalized_text,
            )
            if cached:
                return cached
            try:
                audio_bytes, suffix = self.openai_client.synthesize_speech(
                    text=normalized_text,
                    voice=voice_id,
                    instructions=self._build_openai_voice_instructions(language_code=language_code),
                    speed=self.openai_voice_speed,
                )
            except Exception:  # noqa: BLE001
                audio_bytes = None

        if not audio_bytes:
            return None
        asset_path = self.store.save_audio_blob(session_id, suffix, audio_bytes)
        if provider and model_id:
            self.store.cache_audio(
                owner_key=owner_key,
                provider=provider,
                voice_id=voice_id or elevenlabs_voice_id,
                language_code=language_code,
                model_id=model_id,
                text=normalized_text,
                asset_path=asset_path,
            )
        return asset_path

    def get_cached_audio_for_text(self, *, owner_key: str, text: str, language_code: str) -> str | None:
        normalized_text = optimize_tts_text(text, language_code=language_code)
        cache_target = self._resolve_cache_target(language_code=language_code)
        if not cache_target:
            return None
        return self.store.get_cached_audio(
            owner_key=owner_key,
            provider=cache_target["provider"],
            voice_id=cache_target["voice_id"],
            language_code=language_code,
            model_id=cache_target["model_id"],
            text=normalized_text,
        )

    def _resolve_cache_target(self, *, language_code: str) -> dict[str, str] | None:
        elevenlabs_voice_id = self.elevenlabs_mandarin_voice_id
        if self.tts_provider == "edge":
            return {
                "provider": "edge",
                "voice_id": self._edge_voice_for_language(language_code),
                "model_id": f"edge-tts:{self.edge_rate}",
            }
        if self.tts_provider == "elevenlabs" and self.elevenlabs_api_key and elevenlabs_voice_id:
            return {
                "provider": "elevenlabs",
                "voice_id": elevenlabs_voice_id,
                "model_id": self.elevenlabs_mandarin_model_id,
            }
        if self.tts_provider == "openai" and self.openai_tts_fallback_enabled and self.openai_client.is_configured():
            return {
                "provider": "openai",
                "voice_id": self.openai_mandarin_voice,
                "model_id": str(self.openai_client.tts_model),
            }
        return None

    def _build_openai_voice_instructions(self, *, language_code: str = "zh") -> str:
        language_name = "English" if str(language_code or "").lower().startswith("en") else "Mandarin"
        return (
            f"Speak like an experienced product manager walking software engineers through a requirement in {language_name}. "
            "Sound natural, grounded, and practical, as if you are in a normal requirement grooming session. "
            "Use calm confidence, short pauses between ideas, and slightly slower pacing for dense logic. "
            "Emphasize what the flow is, what needs to be built, and what developers need to pay attention to. "
            "Do not sound robotic, theatrical, overly polished, or like you are reading bullet points word for word."
        )

    def _edge_voice_for_language(self, language_code: str) -> str:
        return self.edge_mandarin_voice if str(language_code or "").lower().startswith("zh") else self.edge_english_voice

    def _synthesize_with_edge_tts(self, *, text: str, voice_id: str) -> bytes:
        return self._run_edge_tts_async(self._edge_tts_bytes(text=text, voice_id=voice_id))

    async def _edge_tts_bytes(self, *, text: str, voice_id: str) -> bytes:
        import edge_tts

        communicate = edge_tts.Communicate(text, voice=voice_id, rate=self.edge_rate)
        chunks: list[bytes] = []
        async for chunk in communicate.stream():
            if chunk.get("type") == "audio":
                data = chunk.get("data") or b""
                if data:
                    chunks.append(bytes(data))
        return b"".join(chunks)

    @staticmethod
    def _run_edge_tts_async(coro):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        result: dict[str, Any] = {}

        def run_in_thread() -> None:
            try:
                result["value"] = asyncio.run(coro)
            except Exception as error:  # noqa: BLE001
                result["error"] = error

        worker = threading.Thread(target=run_in_thread, daemon=True)
        worker.start()
        worker.join()
        if "error" in result:
            raise result["error"]
        return result.get("value", b"")

    def _synthesize_with_elevenlabs(self, *, text: str, voice_id: str, language_code: str, model_id: str) -> bytes:
        response = requests.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
            headers={
                "xi-api-key": self.elevenlabs_api_key,
                "Content-Type": "application/json",
            },
            params={"output_format": "mp3_44100_128"},
            json={
                "text": text,
                "model_id": model_id,
                "language_code": language_code,
                "voice_settings": self._elevenlabs_voice_settings(language_code),
            },
            timeout=180,
        )
        response.raise_for_status()
        return response.content

    def _elevenlabs_voice_settings(self, language_code: str) -> dict[str, Any]:
        if language_code.startswith("zh"):
            return {
                "stability": 0.6,
                "similarity_boost": 0.86,
                "style": 0.08,
                "use_speaker_boost": True,
                "speed": 0.92,
            }
        return {
            "stability": 0.46,
            "similarity_boost": 0.8,
            "style": 0.16,
            "use_speaker_boost": True,
            "speed": 0.98,
        }


class PRDBriefingService:
    def __init__(
        self,
        *,
        store: BriefingStore,
        confluence: ConfluenceConnector,
        openai_client: OpenAIClient,
        text_client: TextGenerationClient | None = None,
        voice_service: VoiceService,
        answer_audio_enabled: bool = False,
        walkthrough_prewarm_enabled: bool = True,
    ) -> None:
        self.store = store
        self.confluence = confluence
        self.openai_client = openai_client
        self.text_client = text_client or openai_client
        self.voice_service = voice_service
        self.retrieval = RetrievalService(openai_client)
        self.answer_audio_enabled = answer_audio_enabled
        self.walkthrough_prewarm_enabled = walkthrough_prewarm_enabled

    def create_session(self, *, owner_key: str, page_ref: str, mode: str, language: str = "zh") -> dict[str, Any]:
        walkthrough_language = normalize_walkthrough_language(language)
        page = self.confluence.ingest_page(page_ref, "pending")
        session_id = self.store.create_session(
            owner_key=owner_key,
            confluence_page_id=page.page_id,
            confluence_page_url=page.source_url,
            audience=walkthrough_audience(walkthrough_language),
            mode=mode,
            title=page.title,
        )
        source_id = self.store.upsert_source(
            owner_key=owner_key,
            session_id=session_id,
            source_type="prd",
            external_id=f"{page.page_id}:{session_id}",
            title=page.title,
            language=page.language,
            source_url=page.source_url,
            updated_at=page.updated_at,
            metadata={"page_id": page.page_id},
        )
        chunks = self._sections_to_chunks(
            owner_key=owner_key,
            session_id=session_id,
            source_id=source_id,
            source_type="prd",
            title=page.title,
            source_url=page.source_url,
            updated_at=page.updated_at,
            sections=page.sections,
        )
        self.store.replace_chunks(chunks)
        payload = self.get_session_payload(session_id=session_id, owner_key=owner_key)
        if self.walkthrough_prewarm_enabled:
            self._spawn_prewarm_walkthrough_scripts(
                owner_key=owner_key,
                sections=payload["sections"],
                language=walkthrough_language,
            )
        return payload

    def get_session_payload(self, *, session_id: str, owner_key: str) -> dict[str, Any]:
        session = self.store.get_session(session_id, owner_key)
        if not session:
            raise ValueError("Briefing session was not found.")
        language = walkthrough_language_from_session(session)
        prd_chunks = self.store.list_session_prd_chunks(session_id, owner_key)
        sections = self._build_sections(prd_chunks)
        sections = [self._annotate_section_cache(owner_key=owner_key, section=section, language=language) for section in sections]
        briefing_blocks = self._build_briefing_blocks(sections)
        briefing_blocks = [self._annotate_block_cache(owner_key=owner_key, block=block, language=language) for block in briefing_blocks]
        session_overview = self._build_session_overview(
            owner_key=owner_key,
            session=session,
            sections=sections,
        )
        return {
            "session": session,
            "session_overview": session_overview,
            "sections": sections,
            "briefing_blocks": briefing_blocks,
            "messages": self.store.list_recent_messages(session_id, limit=20),
        }

    def answer_question(self, *, session_id: str, owner_key: str, question: str) -> dict[str, Any]:
        session = self.store.get_session(session_id, owner_key)
        if not session:
            raise ValueError("Briefing session was not found.")
        all_chunks = self.store.list_session_chunks(session_id, owner_key)
        ranked = self.retrieval.rank(question, all_chunks)
        groundedness = "unsupported"
        if ranked:
            if ranked[0].score >= 0.18:
                groundedness = "grounded"
            elif ranked[0].score >= 0.08:
                groundedness = "inference"
        citations = [citation_from_chunk(chunk) for chunk in ranked[:3]]
        answer = self._compose_answer(
            question=question,
            chunks=ranked[:5],
            groundedness=groundedness,
            recent_messages=self.store.list_recent_messages(session_id, limit=6),
        )
        payload = AnswerPayload(
            answer_text=answer,
            answer_language="zh",
            groundedness=groundedness,
            citations=citations,
        )
        if groundedness != "unsupported" and self.answer_audio_enabled:
            audio_path = self.voice_service.synthesize(
                session_id=session_id,
                text=answer,
                language_code=payload.answer_language,
                owner_key=owner_key,
            )
            payload.audio_url = f"/prd-briefing/assets/{audio_path}" if audio_path else None
        self.store.add_message(session_id, "user", question)
        self.store.add_message(session_id, "assistant", payload.answer_text, answer=payload)
        return {
            "answer_text": payload.answer_text,
            "answer_language": payload.answer_language,
            "groundedness": payload.groundedness,
            "citations": [asdict(citation) for citation in payload.citations],
            "audio_url": payload.audio_url,
        }

    def narrate_section(
        self,
        *,
        session_id: str,
        owner_key: str,
        section_index: int = 0,
        briefing_block_id: str | None = None,
        include_audio: bool = True,
    ) -> dict[str, Any]:
        session = self.store.get_session(session_id, owner_key)
        if not session:
            raise ValueError("Briefing session was not found.")
        language = walkthrough_language_from_session(session)
        language_code = walkthrough_language_code(language)
        prd_chunks = self.store.list_session_prd_chunks(session_id, owner_key)
        sections = self._build_sections(prd_chunks)
        if briefing_block_id:
            blocks = self._build_briefing_blocks(sections)
            block = next((item for item in blocks if item.get("block_id") == briefing_block_id), None)
            if not block:
                raise ValueError("Briefing block is out of range.")
            script, cached = self._compose_walkthrough_block(owner_key=owner_key, block=block, language=language)
            audio_cached = bool(
                self.voice_service.get_cached_audio_for_text(
                    owner_key=owner_key,
                    text=script,
                    language_code=language_code,
                )
            )
            audio_path = None
            if include_audio:
                audio_path = self.voice_service.synthesize(
                    session_id=session_id,
                    text=script,
                    language_code=language_code,
                    owner_key=owner_key,
                )
            return {
                "script": script,
                "audio_url": f"/prd-briefing/assets/{audio_path}" if audio_path else None,
                "cached": cached,
                "audio_cached": audio_cached,
                "briefing_block_id": block["block_id"],
                "section_indexes": block["section_indexes"],
                "language": language,
            }
        if section_index < 0 or section_index >= len(sections):
            raise ValueError("Section index is out of range.")
        section = sections[section_index]
        script, cached = self._compose_walkthrough_section(owner_key=owner_key, section=section, language=language)
        audio_cached = bool(
            self.voice_service.get_cached_audio_for_text(
                owner_key=owner_key,
                text=script,
                language_code=language_code,
            )
        )
        audio_path = None
        if include_audio:
            audio_path = self.voice_service.synthesize(
                session_id=session_id,
                text=script,
                language_code=language_code,
                owner_key=owner_key,
            )
        return {
            "script": script,
            "audio_url": f"/prd-briefing/assets/{audio_path}" if audio_path else None,
            "cached": cached,
            "audio_cached": audio_cached,
            "language": language,
        }

    def _sections_to_chunks(
        self,
        *,
        owner_key: str,
        session_id: str | None,
        source_id: int,
        source_type: str,
        title: str,
        source_url: str,
        updated_at: str,
        sections: list[ParsedSection],
    ) -> list[ChunkRecord]:
        contents = [section.content for section in sections]
        embeddings: list[list[float] | None]
        if self.openai_client.is_configured():
            try:
                embeddings = self.openai_client.embed_texts(contents)
            except Exception:  # noqa: BLE001
                embeddings = [None for _ in contents]
        else:
            embeddings = [None for _ in contents]
        return [
            ChunkRecord(
                source_id=source_id,
                owner_key=owner_key,
                session_id=session_id,
                source_type=source_type,
                title=title,
                section_path=section.section_path,
                content=section.content,
                html_content=section.html_content,
                image_refs=section.image_refs,
                source_url=source_url,
                updated_at=updated_at,
                embedding=embeddings[index],
            )
            for index, section in enumerate(sections)
        ]

    def _build_sections(self, prd_chunks: list[ChunkRecord]) -> list[dict[str, Any]]:
        return [
            {
                "section_index": index,
                "section_path": chunk.section_path,
                "content": chunk.content,
                "html_content": chunk.html_content,
                "briefing_notes": build_presenter_notes(chunk.section_path, chunk.content),
                "briefing_summary": build_briefing_summary(chunk.section_path, chunk.content),
                "image_refs": [
                    f"/prd-briefing/image-proxy?src={quote(ref, safe='')}" if ref.startswith("http") else f"/prd-briefing/assets/{ref}"
                    for ref in chunk.image_refs
                ],
            }
            for index, chunk in enumerate(prd_chunks)
        ]

    def _build_briefing_blocks(self, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return build_pm_briefing_blocks(sections)

    def _compose_walkthrough_section(self, *, owner_key: str, section: dict[str, Any], language: str = "zh") -> tuple[str, bool]:
        language = normalize_walkthrough_language(language)
        prompt = build_walkthrough_section_system_prompt(language)
        notes = section.get("briefing_notes") or []
        body = build_walkthrough_section_user_prompt(section=section, notes=notes, language=language)
        if not self.text_client.is_configured():
            raise RuntimeError("Walkthrough script generation now requires a configured OpenAI text model.")
        cache_lookup = self._build_walkthrough_cache_lookup(
            owner_key=owner_key,
            section=section,
            prompt=prompt,
            body=body,
            notes=notes,
            language=language,
        )
        model_id = cache_lookup["model_id"]
        section_payload = cache_lookup["section_payload"]
        cached_script = cache_lookup["cached_script"]
        if cached_script:
            return cached_script, True
        legacy_cached_script = cache_lookup["legacy_cached_script"]
        if legacy_cached_script:
            # Normalize legacy cache entries under the current model id so later
            # requests hit the fast path without needing OpenAI again.
            self.store.cache_script(
                owner_key=owner_key,
                audience=walkthrough_audience(language),
                model_id=model_id,
                prompt_version=WALKTHROUGH_SCRIPT_PROMPT_VERSION,
                section_payload=section_payload,
                script=legacy_cached_script,
            )
            return legacy_cached_script, True
        try:
            script = self.text_client.create_answer(system_prompt=prompt, user_prompt=body)
        except Exception as error:  # noqa: BLE001
            raise RuntimeError(f"Text model could not generate the walkthrough script: {error}") from error
        self.store.cache_script(
            owner_key=owner_key,
            audience=walkthrough_audience(language),
            model_id=model_id,
            prompt_version=WALKTHROUGH_SCRIPT_PROMPT_VERSION,
            section_payload=section_payload,
            script=script,
        )
        return script, False

    def _compose_walkthrough_block(self, *, owner_key: str, block: dict[str, Any], language: str = "zh") -> tuple[str, bool]:
        language = normalize_walkthrough_language(language)
        prompt = build_walkthrough_block_system_prompt(language)
        source_lines = []
        for ref in block.get("source_refs") or []:
            source_lines.append(
                f"[{int(ref.get('section_index', 0)) + 1}] {ref.get('section_path')}\n{ref.get('content_excerpt', '')}"
            )
        body = build_walkthrough_block_user_prompt(block=block, source_lines=source_lines, language=language)
        if not self.text_client.is_configured():
            raise RuntimeError("Walkthrough script generation now requires a configured OpenAI text model.")
        cache_lookup = self._build_walkthrough_block_cache_lookup(
            owner_key=owner_key,
            block=block,
            prompt=prompt,
            body=body,
            language=language,
        )
        model_id = cache_lookup["model_id"]
        block_payload = cache_lookup["block_payload"]
        cached_script = cache_lookup["cached_script"]
        if cached_script:
            return cached_script, True
        legacy_cached_script = cache_lookup["legacy_cached_script"]
        if legacy_cached_script:
            self.store.cache_script(
                owner_key=owner_key,
                audience=walkthrough_audience(language),
                model_id=model_id,
                prompt_version=WALKTHROUGH_BLOCK_PROMPT_VERSION,
                section_payload=block_payload,
                script=legacy_cached_script,
            )
            return legacy_cached_script, True
        try:
            script = self.text_client.create_answer(system_prompt=prompt, user_prompt=body)
        except Exception as error:  # noqa: BLE001
            raise RuntimeError(f"Text model could not generate the walkthrough script: {error}") from error
        self.store.cache_script(
            owner_key=owner_key,
            audience=walkthrough_audience(language),
            model_id=model_id,
            prompt_version=WALKTHROUGH_BLOCK_PROMPT_VERSION,
            section_payload=block_payload,
            script=script,
        )
        return script, False

    def _prewarm_walkthrough_scripts(
        self,
        *,
        owner_key: str,
        sections: list[dict[str, Any]],
        language: str = "zh",
    ) -> None:
        if not self.text_client.is_configured():
            return
        language = normalize_walkthrough_language(language)
        briefing_blocks = self._build_briefing_blocks(sections)
        for block in briefing_blocks[:WALKTHROUGH_PREWARM_LIMIT]:
            try:
                self._compose_walkthrough_block(
                    owner_key=owner_key,
                    block=block,
                    language=language,
                )
            except Exception:  # noqa: BLE001
                continue

    def _spawn_prewarm_walkthrough_scripts(
        self,
        *,
        owner_key: str,
        sections: list[dict[str, Any]],
        language: str = "zh",
    ) -> None:
        if not self.text_client.is_configured():
            return
        language = normalize_walkthrough_language(language)
        section_copies = [
            dict(section, image_refs=list(section.get("image_refs") or []), briefing_notes=list(section.get("briefing_notes") or []))
            for section in sections
        ]
        worker = threading.Thread(
            target=self._prewarm_walkthrough_scripts,
            kwargs={
                "owner_key": owner_key,
                "sections": section_copies,
                "language": language,
            },
            daemon=True,
            name="prd-briefing-prewarm",
        )
        worker.start()

    def _annotate_section_cache(self, *, owner_key: str, section: dict[str, Any], language: str = "zh") -> dict[str, Any]:
        annotated = dict(section)
        language = normalize_walkthrough_language(language)
        try:
            cache_lookup = self._build_walkthrough_cache_lookup(owner_key=owner_key, section=section, language=language)
            cached_script = cache_lookup["cached_script"] or cache_lookup["legacy_cached_script"]
            annotated["walkthrough_cached"] = bool(cached_script)
            annotated["walkthrough_audio_cached"] = bool(
                cached_script
                and self.voice_service.get_cached_audio_for_text(
                    owner_key=owner_key,
                    text=cached_script,
                    language_code=walkthrough_language_code(language),
                )
            )
        except Exception:  # noqa: BLE001
            annotated["walkthrough_cached"] = False
            annotated["walkthrough_audio_cached"] = False
        return annotated

    def _annotate_block_cache(self, *, owner_key: str, block: dict[str, Any], language: str = "zh") -> dict[str, Any]:
        annotated = dict(block)
        language = normalize_walkthrough_language(language)
        try:
            cache_lookup = self._build_walkthrough_block_cache_lookup(owner_key=owner_key, block=block, language=language)
            cached_script = cache_lookup["cached_script"] or cache_lookup["legacy_cached_script"]
            annotated["walkthrough_cached"] = bool(cached_script)
            annotated["walkthrough_audio_cached"] = bool(
                cached_script
                and self.voice_service.get_cached_audio_for_text(
                    owner_key=owner_key,
                    text=cached_script,
                    language_code=walkthrough_language_code(language),
                )
            )
        except Exception:  # noqa: BLE001
            annotated["walkthrough_cached"] = False
            annotated["walkthrough_audio_cached"] = False
        return annotated

    def _build_walkthrough_cache_lookup(
        self,
        *,
        owner_key: str,
        section: dict[str, Any],
        prompt: str | None = None,
        body: str | None = None,
        notes: list[str] | None = None,
        language: str = "zh",
    ) -> dict[str, Any]:
        language = normalize_walkthrough_language(language)
        prompt = prompt or build_walkthrough_section_system_prompt(language)
        notes = notes if notes is not None else (section.get("briefing_notes") or [])
        body = body or build_walkthrough_section_user_prompt(section=section, notes=notes, language=language)
        model_id = str(getattr(self.text_client, "model_id", getattr(self.openai_client, "chat_model", "text_model")))
        section_payload = json.dumps(
            {
                "section_path": section["section_path"],
                "briefing_summary": section.get("briefing_summary", ""),
                "briefing_notes": notes,
                "content": section["content"],
                "prompt": prompt,
                "body": body,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        cached_script = self.store.get_cached_script(
            owner_key=owner_key,
            audience=walkthrough_audience(language),
            model_id=model_id,
            prompt_version=WALKTHROUGH_SCRIPT_PROMPT_VERSION,
            section_payload=section_payload,
        )
        legacy_cached_script = self.store.get_cached_script_any_model(
            owner_key=owner_key,
            audience=walkthrough_audience(language),
            prompt_version=WALKTHROUGH_SCRIPT_PROMPT_VERSION,
            section_payload=section_payload,
        )
        return {
            "model_id": model_id,
            "section_payload": section_payload,
            "cached_script": cached_script,
            "legacy_cached_script": legacy_cached_script,
        }

    def _build_walkthrough_block_cache_lookup(
        self,
        *,
        owner_key: str,
        block: dict[str, Any],
        prompt: str | None = None,
        body: str | None = None,
        language: str = "zh",
    ) -> dict[str, Any]:
        language = normalize_walkthrough_language(language)
        prompt = prompt or build_walkthrough_block_system_prompt(language)
        source_payload = [
            {
                "section_index": ref.get("section_index"),
                "section_path": ref.get("section_path"),
                "content_excerpt": ref.get("content_excerpt", ""),
            }
            for ref in block.get("source_refs") or []
        ]
        body = body or json.dumps(
            {
                "block_id": block.get("block_id"),
                "title": block.get("title"),
                "briefing_goal": block.get("briefing_goal"),
                "merged_summary": block.get("merged_summary"),
                "developer_focus": block.get("developer_focus") or [],
                "source_refs": source_payload,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        model_id = str(getattr(self.text_client, "model_id", getattr(self.openai_client, "chat_model", "text_model")))
        block_payload = json.dumps(
            {
                "payload_type": "briefing_block",
                "block_id": block.get("block_id"),
                "title": block.get("title"),
                "briefing_goal": block.get("briefing_goal"),
                "merged_summary": block.get("merged_summary"),
                "developer_focus": block.get("developer_focus") or [],
                "section_indexes": block.get("section_indexes") or [],
                "source_refs": source_payload,
                "prompt": prompt,
                "body": body,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        cached_script = self.store.get_cached_script(
            owner_key=owner_key,
            audience=walkthrough_audience(language),
            model_id=model_id,
            prompt_version=WALKTHROUGH_BLOCK_PROMPT_VERSION,
            section_payload=block_payload,
        )
        legacy_cached_script = self.store.get_cached_script_any_model(
            owner_key=owner_key,
            audience=walkthrough_audience(language),
            prompt_version=WALKTHROUGH_BLOCK_PROMPT_VERSION,
            section_payload=block_payload,
        )
        return {
            "model_id": model_id,
            "block_payload": block_payload,
            "cached_script": cached_script,
            "legacy_cached_script": legacy_cached_script,
        }

    def _compose_answer(
        self,
        *,
        question: str,
        chunks: list[ChunkRecord],
        groundedness: str,
        recent_messages: list[dict[str, Any]],
    ) -> str:
        if groundedness == "unsupported":
            return "我无法在当前选择的 PRD 或已上传的团队知识库中找到这个答案。"
        excerpts = "\n\n".join(
            f"[{index + 1}] {chunk.title} | {chunk.section_path}\n{chunk.content}"
            for index, chunk in enumerate(chunks)
        )
        history = "\n".join(f"{item['role']}: {item['body']}" for item in recent_messages[-4:])
        system_prompt = (
            f"You answer questions about a product requirements document. Respond in {DEVELOPER_LANGUAGE}. "
            "Use only the provided excerpts. Start with the direct answer. "
            "If the answer is inferred rather than explicit, say that it is an interpretation."
        )
        user_prompt = (
            f"Conversation context:\n{history or '(none)'}\n\n"
            f"Question:\n{question}\n\n"
            f"Retrieved excerpts:\n{excerpts}"
        )
        if self.text_client.is_configured():
            try:
                return self.text_client.create_answer(system_prompt=system_prompt, user_prompt=user_prompt)
            except Exception:  # noqa: BLE001
                pass
        return self._build_fallback_answer(chunks=chunks, groundedness=groundedness)

    def _build_fallback_answer(self, *, chunks: list[ChunkRecord], groundedness: str) -> str:
        if not chunks:
            return "我暂时无法生成完整回答，但当前也没有检索到足够的 PRD 片段。"
        lead = "根据当前检索到的 PRD 内容，我先给你一个简短结论："
        if groundedness == "inference":
            lead = "根据当前检索到的 PRD 内容，这里先给你一个偏保守的推断："
        summary_points = []
        for chunk in chunks[:2]:
            snippet = summarize_chunk_for_fallback(chunk.content)
            if snippet:
                summary_points.append(snippet)
        if not summary_points:
            summary_points.append("当前命中的 section 主要覆盖相关流程和字段说明")
        references = "；".join(
            f"{chunk.title} · {chunk.section_path}"
            for chunk in chunks[:3]
        )
        body = "；".join(summary_points[:2])
        return f"{lead}{body}。可优先查看：{references}。"

    def _build_session_overview(
        self,
        *,
        owner_key: str,
        session: dict[str, Any],
        sections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self._build_developer_chinese_overview(
            owner_key=owner_key,
            session=session,
            sections=sections,
        )

    def _build_developer_chinese_overview(
        self,
        *,
        owner_key: str,
        session: dict[str, Any],
        sections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        prioritized_sections = select_sections_for_overview(sections)
        return build_two_part_fallback_overview(session["title"], prioritized_sections)


def cosine_similarity(a: list[float], b: list[float]) -> float:
    numerator = sum(x * y for x, y in zip(a, b))
    denominator_a = math.sqrt(sum(x * x for x in a))
    denominator_b = math.sqrt(sum(y * y for y in b))
    if not denominator_a or not denominator_b:
        return 0.0
    return numerator / (denominator_a * denominator_b)


def tokenize(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z0-9_]{2,}", value.lower())
        if token not in STOPWORDS
    }


def keyword_score(query: str, content: str) -> float:
    query_tokens = tokenize(query)
    content_tokens = tokenize(content)
    if not query_tokens or not content_tokens:
        return 0.0
    overlap = len(query_tokens & content_tokens)
    return overlap / max(len(query_tokens), 1)


def chunk_has_signal(chunk: ChunkRecord) -> bool:
    return chunk.score > 0.0


def summarize_chunk_for_fallback(content: str, limit: int = 90) -> str:
    normalized = " ".join((content or "").split())
    if not normalized:
        return ""
    parts = re.split(r"(?<=[。！？.!?])\s+|\n+", normalized)
    first = next((part.strip() for part in parts if part.strip()), normalized)
    first = re.sub(r"\s+", " ", first)
    if len(first) <= limit:
        return first
    trimmed = first[:limit].rstrip(" ,;:，；：")
    return f"{trimmed}..."


def safe_filename(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", name.strip()).strip("-")
    return cleaned or "upload.bin"


def extract_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".md", ".txt", ".rst"}:
        return path.read_text(encoding="utf-8", errors="ignore")
    if suffix in {".html", ".htm"}:
        soup = BeautifulSoup(path.read_text(encoding="utf-8", errors="ignore"), "html.parser")
        return soup.get_text("\n", strip=True)
    if suffix == ".json":
        return path.read_text(encoding="utf-8", errors="ignore")
    raise ValueError("Supported knowledge-base files are .md, .txt, .html, and .json.")


def truncate_for_prompt(content: str, limit: int) -> str:
    value = " ".join(content.split())
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def format_source_text(content: str) -> str:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    formatted: list[str] = []
    for line in lines:
        parts = split_text_fragments(line)
        if parts:
            formatted.extend(parts)
        else:
            formatted.append(line)
    return "\n".join(formatted[:18])


def build_briefing_summary(section_title: str, content: str) -> str:
    candidates = split_text_fragments(content)
    for candidate in candidates:
        if len(candidate) >= 40:
            return candidate
    return section_title


def build_presenter_notes(section_title: str, content: str) -> list[str]:
    candidates = split_text_fragments(content)
    notes: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        clean = candidate.strip(" -")
        if not clean:
            continue
        key = clean.casefold()
        if key in seen or key == section_title.casefold():
            continue
        seen.add(key)
        notes.append(clean)
        if len(notes) >= 5:
            break
    if not notes and section_title:
        notes.append(section_title)
    return notes


def build_heuristic_session_overview(title: str, sections: list[dict[str, Any]]) -> dict[str, Any]:
    prioritized_sections = select_sections_for_overview(sections)
    section_titles = [section.get("section_path", "") for section in prioritized_sections if section.get("section_path")]
    modules = infer_impacted_modules_from_sections(prioritized_sections)
    overview = localize_detail_point_zh(build_detail_grounded_overview(title, prioritized_sections))
    return {
        "overview": overview,
        "scope": build_scope_items(prioritized_sections),
        "impacted_modules": modules or ["核心功能流程", "关键页面和字段", "状态流转和操作动作"],
        "developer_focus": [localize_detail_point_zh(item) for item in extract_detail_points(prioritized_sections, category="developer")],
        "frontend_focus": [localize_detail_point_zh(item) for item in extract_detail_points(prioritized_sections, category="frontend")],
        "backend_focus": [localize_detail_point_zh(item) for item in extract_detail_points(prioritized_sections, category="backend")],
        "risks": [localize_detail_point_zh(item) for item in extract_detail_points(prioritized_sections, category="risks")],
        "unclear_rules": [localize_detail_point_zh(item) for item in extract_detail_points(prioritized_sections, category="unclear_rules")],
        "missing_edge_cases": [localize_detail_point_zh(item) for item in extract_detail_points(prioritized_sections, category="missing_edge_cases")],
        "unclear_ownership": [localize_detail_point_zh(item) for item in extract_detail_points(prioritized_sections, category="unclear_ownership")],
        "open_questions": [localize_detail_point_zh(item) for item in extract_detail_points(prioritized_sections, category="open_questions")],
    }


def build_minimal_overview_payload(summary: str) -> dict[str, Any]:
    return {
        "overview": summary.strip(),
        "background_goal": "",
        "implementation_overview": summary.strip(),
        "scope": [],
        "impacted_modules": [],
        "developer_focus": [],
        "frontend_focus": [],
        "backend_focus": [],
        "risks": [],
        "unclear_rules": [],
        "missing_edge_cases": [],
        "unclear_ownership": [],
        "open_questions": [],
    }


def parse_developer_overview_payload(raw: str) -> dict[str, Any]:
    payload = strip_code_fences(raw)
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return build_minimal_overview_payload(payload)
    background_goal = str(data.get("background_goal", "")).strip()
    implementation_overview = str(data.get("implementation_overview", "")).strip()
    combined = " ".join(part for part in (background_goal, implementation_overview) if part).strip()
    return {
        "overview": combined,
        "background_goal": background_goal,
        "implementation_overview": implementation_overview,
        "scope": [],
        "impacted_modules": [],
        "developer_focus": [],
        "frontend_focus": [],
        "backend_focus": [],
        "risks": [],
        "unclear_rules": [],
        "missing_edge_cases": [],
        "unclear_ownership": [],
        "open_questions": [],
    }


def build_chinese_fallback_overview(title: str, sections: list[dict[str, Any]]) -> str:
    titles = [str(section.get("section_path", "")).strip() for section in sections[:6] if str(section.get("section_path", "")).strip()]
    content = " ".join(str(section.get("content", "")) for section in sections[:6])
    flow_bits: list[str] = []
    if any(token in content.lower() for token in ("add new assessment", "submit", "review", "withdraw", "reopen")):
        flow_bits.append("重点围绕新增评估、提交、复核、撤回和重开等操作流程展开")
    if any(token in content.lower() for token in ("layout", "field", "search", "detail", "tab")):
        flow_bits.append("同时覆盖页面布局、字段展示、搜索条件和详情区规则")
    if any(token in content.lower() for token in ("status", "default", "auto populate", "required", "readonly")):
        flow_bits.append("实现时需要特别关注状态流转、默认值、自动回填以及必填或只读条件")
    if not flow_bits:
        flow_bits.append("主要围绕页面操作流程、关键规则和实现约束展开")
    focus_sections = "、".join(titles[:3]) if titles else title
    detail_bits: list[str] = []
    if any(token in content.lower() for token in ("assessment id", "view the assessment detail", "overview tab")):
        detail_bits.append("用户会先从外包管理概览页进入评估详情，很多后续操作都是从详情页继续展开。")
    if any(token in content.lower() for token in ("withdraw comment", "review comment", "submit comment", "details tab")):
        detail_bits.append("详情页里会根据不同状态展示不同的 Tab 和 comment 区域，所以前端显示条件不能写死。")
    if any(token in content.lower() for token in ("closed", "verified", "draft", "pending review", "pending approval")):
        detail_bits.append("状态流转是这份 PRD 的核心之一，不同状态会直接影响按钮是否可点、Tab 是否显示，以及后续流程是否允许继续。")
    if any(token in content.lower() for token in ("auto populate", "default", "required", "readonly")):
        detail_bits.append("另外，默认值、自动回填、必填和只读条件也比较多，这些都需要在实现时逐项对齐。")
    if any(token in content.lower() for token in ("sg", "ph", "id", "regional")):
        detail_bits.append("部分逻辑还会因为地区或模板类型不同而变化，所以实现时不能假设所有 assessment 页面行为完全一致。")
    summary_parts = [
        "这份 PRD 主要是在讲 Outsourcing Management 相关评估流程的页面行为、操作步骤和规则变化。",
        f"{flow_bits[0]}。",
        f"从当前内容看，开发可以优先关注 {focus_sections} 这几部分，因为这里最集中体现了页面动作、字段规则和状态变化。",
    ]
    summary_parts.extend(detail_bits[:5])
    summary_parts.append("实现时建议先把主流程跑通，再重点确认默认值、按钮触发条件、系统自动处理、状态依赖，以及不同页面或场景下的显示和校验逻辑。")
    return "".join(summary_parts)


def build_two_part_fallback_overview(title: str, sections: list[dict[str, Any]]) -> dict[str, Any]:
    titles = [str(section.get("section_path", "")).strip() for section in sections[:6] if str(section.get("section_path", "")).strip()]
    content = " ".join(str(section.get("content", "")) for section in sections[:6])
    background_parts = [
        "这份 PRD 主要围绕 Outsourcing Management 相关评估流程的处理方式、页面行为和规则变化展开。",
    ]
    if any(token in content.lower() for token in ("outsourcing", "assessment", "review", "approval")):
        background_parts.append("它本质上是在把不同类型评估的创建、查看、提交、复核和后续流转逻辑定义清楚，避免人工处理时规则不一致。")
    if any(token in content.lower() for token in ("report", "download", "history", "register")):
        background_parts.append("除了页面操作本身，这份需求也涉及报表、下载、历史记录或监管相关输出，因此不仅是前端页面改动，还会影响后续处理链路。")

    implementation_parts = [build_chinese_fallback_overview(title, sections)]
    if titles:
        implementation_parts.append(f"从当前内容看，最值得优先细读的是 { '、'.join(titles[:4]) }，因为这些 section 最集中体现了页面结构、字段规则和状态依赖。")
    if any(token in content.lower() for token in ("closed", "verified", "draft", "pending review", "pending approval")):
        implementation_parts.append("实现时尤其要注意不同状态下按钮、Tab、评论区和后续动作是否允许继续，因为很多页面行为都是由状态驱动的。")
    if any(token in content.lower() for token in ("default", "auto populate", "required", "readonly", "show", "display")):
        implementation_parts.append("另外，默认值、自动回填、必填或只读条件、显示或隐藏逻辑这些细节也要逐项对齐，否则开发很容易做出和 PRD 不一致的页面行为。")

    background_goal = "".join(background_parts)
    implementation_overview = "".join(implementation_parts)
    return {
        "overview": f"{background_goal} {implementation_overview}".strip(),
        "background_goal": background_goal,
        "implementation_overview": implementation_overview,
        "scope": [],
        "impacted_modules": [],
        "developer_focus": [],
        "frontend_focus": [],
        "backend_focus": [],
        "risks": [],
        "unclear_rules": [],
        "missing_edge_cases": [],
        "unclear_ownership": [],
        "open_questions": [],
    }


def strip_code_fences(value: str) -> str:
    payload = value.strip()
    if payload.startswith("```"):
        payload = re.sub(r"^```(?:json)?\s*", "", payload)
        payload = re.sub(r"\s*```$", "", payload)
    return payload.strip()


def parse_session_overview(raw: str) -> dict[str, Any]:
    payload = raw.strip()
    if payload.startswith("```"):
        payload = re.sub(r"^```(?:json)?\s*", "", payload)
        payload = re.sub(r"\s*```$", "", payload)
    data = json.loads(payload)
    return {
        "overview": str(data.get("overview", "")).strip(),
        "scope": normalize_overview_list(data.get("scope")),
        "impacted_modules": normalize_overview_list(data.get("impacted_modules")),
        "developer_focus": normalize_overview_list(data.get("developer_focus")),
        "frontend_focus": normalize_overview_list(data.get("frontend_focus")),
        "backend_focus": normalize_overview_list(data.get("backend_focus")),
        "risks": normalize_overview_list(data.get("risks")),
        "unclear_rules": normalize_overview_list(data.get("unclear_rules")),
        "missing_edge_cases": normalize_overview_list(data.get("missing_edge_cases")),
        "unclear_ownership": normalize_overview_list(data.get("unclear_ownership")),
        "open_questions": normalize_overview_list(data.get("open_questions")),
    }


def overview_is_low_signal(overview: dict[str, Any]) -> bool:
    overview_text = str(overview.get("overview", "")).strip()
    if not overview_text or looks_like_metadata_noise(overview_text):
        return True
    buckets = [
        overview.get("scope", []),
        overview.get("impacted_modules", []),
        overview.get("developer_focus", []),
        overview.get("frontend_focus", []),
        overview.get("backend_focus", []),
        overview.get("risks", []),
        overview.get("unclear_rules", []),
        overview.get("missing_edge_cases", []),
        overview.get("unclear_ownership", []),
        overview.get("open_questions", []),
    ]
    flattened = [str(item).strip() for bucket in buckets for item in bucket if str(item).strip()]
    if not flattened:
        return True
    developer_focus = [str(item).strip() for item in overview.get("developer_focus", []) if str(item).strip()]
    scope = [str(item).strip() for item in overview.get("scope", []) if str(item).strip()]
    if developer_focus and all(looks_like_metadata_noise(item) for item in developer_focus):
        return True
    if scope and all(looks_like_metadata_noise(item) for item in scope):
        return True
    if all(looks_like_metadata_noise(item) for item in flattened[:4]):
        return True
    generic_hits = sum(
        1
        for item in flattened
        if any(
            phrase in item
            for phrase in (
                "主流程",
                "页面动作",
                "状态变化",
                "核心业务流程",
                "规则约束展开",
                "页面展示",
                "字段显隐",
                "前端展示和交互",
                "后端负责状态计算",
            )
        )
    )
    feature_hits = sum(1 for item in flattened if has_feature_level_signal(item))
    return feature_hits == 0 or generic_hits >= max(4, len(flattened) // 2)


def normalize_overview_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        clean = str(item).strip()
        if not clean:
            continue
        if looks_like_metadata_noise(clean):
            continue
        result.append(clean)
    return result


def looks_like_metadata_noise(value: str) -> bool:
    lowered = value.lower()
    if re.search(r"\b(version|date|pic|owner|requester|approver|people involved|relevant documentations?)\b", lowered):
        return True
    if "@" in lowered:
        return True
    if re.search(r"\b(v\d+(\.\d+)?)\b", lowered):
        return True
    if re.search(r"\b\d{1,2}\s+[a-z]{3,9}\s+\d{4}\b", lowered):
        return True
    return False


def has_feature_level_signal(value: str) -> bool:
    lowered = value.lower()
    return any(
        token in lowered
        for token in (
            "button",
            "tab",
            "field",
            "layout",
            "search",
            "download",
            "report",
            "review",
            "withdraw",
            "reopen",
            "submit",
            "approval",
            "history",
            "detail",
            "assessment",
            "状态",
            "按钮",
            "字段",
            "搜索",
            "下载",
            "提交",
            "审批",
            "撤回",
            "重开",
            "详情",
            "列表",
            "表单",
            "页面",
            "tab",
        )
    )


BRIEFING_CATEGORY_META = {
    "workflow": {
        "title": "主流程和页面跳转",
        "goal": "帮助开发先理解用户从入口到完成操作的主路径，以及页面之间怎么衔接。",
    },
    "ui_rules": {
        "title": "页面布局和字段规则",
        "goal": "把页面结构、字段展示、默认值、显隐、只读和必填规则合并讲清楚。",
    },
    "state_actions": {
        "title": "状态流转和操作动作",
        "goal": "集中说明提交、复核、审批、撤回、重开等动作会如何改变状态和可用操作。",
    },
    "reporting": {
        "title": "报表、下载和历史记录",
        "goal": "说明报表下载、历史记录、审计输出等功能对前后端实现的要求。",
    },
    "permission_edge": {
        "title": "权限、校验和异常边界",
        "goal": "把权限条件、强校验、失败路径和边界 case 单独拎出来，避免实现时漏规则。",
    },
    "feature": {
        "title": "核心功能说明",
        "goal": "合并 PRD 里真正影响开发实现的功能说明，过滤低价值背景和元数据。",
    },
}


MAX_BRIEFING_BLOCK_SECTIONS = 4
MAX_BRIEFING_BLOCK_HTML_CHARS = 180_000


def build_pm_briefing_blocks(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[tuple[int, dict[str, Any], str, int]] = []
    for index, section in enumerate(sections):
        section_path = str(section.get("section_path", "")).strip()
        content = str(section.get("content", "")).strip()
        score = section_overview_score(section_path, content)
        if looks_like_metadata_noise(section_path) and score < 8:
            continue
        if score < 3 and not has_feature_level_signal(f"{section_path} {content}"):
            continue
        candidates.append((index, section, classify_briefing_category(section_path, content), score))

    if not candidates and sections:
        ranked = [
            (index, section, "feature", section_overview_score(str(section.get("section_path", "")), str(section.get("content", ""))))
            for index, section in enumerate(sections)
        ]
        candidates = sorted(ranked, key=lambda item: item[3], reverse=True)[: min(3, len(ranked))]

    grouped: dict[str, list[tuple[int, dict[str, Any], int]]] = {}
    category_order: list[str] = []
    for index, section, category, score in candidates:
        if category not in grouped:
            grouped[category] = []
            category_order.append(category)
        grouped[category].append((index, section, score))

    blocks: list[dict[str, Any]] = []
    for category in category_order:
        entries = sorted(grouped[category], key=lambda item: item[0])
        meta = BRIEFING_CATEGORY_META.get(category, BRIEFING_CATEGORY_META["feature"])
        entry_groups = split_briefing_entries(entries)
        for group_index, group_entries in enumerate(entry_groups, start=1):
            title = meta["title"]
            if len(entry_groups) > 1:
                title = f"{title} {group_index}"
            section_indexes = [index for index, _section, _score in group_entries]
            block_sections = [section for _index, section, _score in group_entries]
            source_refs = [
                {
                    "section_index": index,
                    "section_path": str(section.get("section_path", "")).strip(),
                    "content_excerpt": truncate_for_prompt(str(section.get("content", "")), 700),
                }
                for index, section, _score in group_entries
            ]
            developer_focus = [localize_detail_point_zh(item) for item in extract_detail_points(block_sections, category="developer")]
            merged_summary = build_block_summary(title, block_sections)
            blocks.append(
                {
                    "block_id": (
                        f"block-{len(blocks) + 1}-{category}-{group_index}"
                        if len(entry_groups) > 1
                        else f"block-{len(blocks) + 1}-{category}"
                    ),
                    "title": title,
                    "briefing_goal": meta["goal"],
                    "merged_summary": merged_summary,
                    "section_indexes": section_indexes,
                    "source_refs": source_refs,
                    "developer_focus": developer_focus,
                    "walkthrough_cached": False,
                    "walkthrough_audio_cached": False,
                }
            )
    return blocks


def split_briefing_entries(entries: list[tuple[int, dict[str, Any], int]]) -> list[list[tuple[int, dict[str, Any], int]]]:
    groups: list[list[tuple[int, dict[str, Any], int]]] = []
    current: list[tuple[int, dict[str, Any], int]] = []
    current_size = 0

    for entry in entries:
        _index, section, _score = entry
        section_size = len(str(section.get("html_content", ""))) + len(str(section.get("content", "")))
        would_overflow_count = len(current) >= MAX_BRIEFING_BLOCK_SECTIONS
        would_overflow_size = current and current_size + section_size > MAX_BRIEFING_BLOCK_HTML_CHARS
        if current and (would_overflow_count or would_overflow_size):
            groups.append(current)
            current = []
            current_size = 0
        current.append(entry)
        current_size += section_size

    if current:
        groups.append(current)
    return groups


def classify_briefing_category(section_path: str, content: str) -> str:
    text = f"{section_path} {content}".lower()
    if any(token in text for token in ("report", "download", "export", "history", "audit")):
        return "reporting"
    if any(token in text for token in ("status", "submit", "review", "approval", "approve", "withdraw", "reopen", "closed", "verified", "draft")):
        return "state_actions"
    if any(token in text for token in ("layout", "field", "form", "search", "criteria", "column", "tab", "button", "display", "show", "visible", "hidden", "readonly", "default")):
        return "ui_rules"
    if any(token in text for token in ("permission", "role", "validation", "required", "must", "cannot", "only", "error", "fail", "exception")):
        return "permission_edge"
    if any(token in text for token in ("workflow", "flow", "navigation", "journey", "click", "navigate", "entry")):
        return "workflow"
    return "feature"


def build_block_summary(title: str, sections: list[dict[str, Any]]) -> str:
    focus_sentences = extract_candidate_sentences(sections, limit=3)
    if focus_sentences:
        return f"{title}：{'；'.join(focus_sentences[:3])}。"
    titles = [str(section.get("section_path", "")).strip() for section in sections if str(section.get("section_path", "")).strip()]
    if titles:
        return f"{title}：建议把 {'、'.join(titles[:4])} 作为同一个产品能力一起 briefing。"
    return f"{title}：系统已把相关 PRD 内容合并成一个产品 briefing 模块。"


def select_sections_for_overview(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(
        sections,
        key=lambda section: section_overview_score(
            str(section.get("section_path", "")),
            str(section.get("content", "")),
        ),
        reverse=True,
    )
    return ranked or sections


def section_overview_score(section_path: str, content: str) -> int:
    text = f"{section_path} {content}".lower()
    score = 0

    if re.search(r"\b3(\.|$)|\b4(\.|$)|\b5(\.|$)|\b6(\.|$)", section_path):
        score += 10
    if re.search(r"\b1(\.|$)|\b2(\.|$)", section_path):
        score -= 4

    for token in (
        "workflow",
        "navigation",
        "function list",
        "requirement",
        "layout",
        "field",
        "search",
        "submit",
        "review",
        "withdraw",
        "reopen",
        "download",
        "report",
        "tab",
        "button",
        "assessment",
        "detail",
        "status",
    ):
        if token in text:
            score += 3

    for token in (
        "version control",
        "people involved",
        "relevant documentation",
        "background",
        "objectives",
        "target users",
    ):
        if token in text:
            score -= 6

    if "click" in text or "user can" in text:
        score += 4
    if "system" in text and ("auto" in text or "populate" in text or "default" in text):
        score += 4
    if "validation" in text or "required" in text or "must" in text or "cannot" in text:
        score += 4

    return score


def dedupe_non_empty(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        clean = str(item).strip()
        if not clean:
            continue
        key = clean.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(clean)
    return result


def infer_impacted_modules(section_titles: list[str]) -> list[str]:
    joined = " ".join(section_titles).lower()
    modules: list[str] = []
    if any(token in joined for token in ("workflow", "flow", "navigation")):
        modules.append("流程和页面跳转")
    if any(token in joined for token in ("layout", "field", "form", "search", "detail")):
        modules.append("页面布局和字段交互")
    if any(token in joined for token in ("review", "approval", "submit", "withdraw", "reopen")):
        modules.append("状态流转和操作动作")
    if any(token in joined for token in ("report", "download", "history")):
        modules.append("报表、下载或历史记录")
    return dedupe_non_empty(modules)


def infer_impacted_modules_from_sections(sections: list[dict[str, Any]]) -> list[str]:
    modules: list[str] = []
    for section in sections[:10]:
        title = str(section.get("section_path", "")).strip()
        lowered = title.lower()
        if looks_like_metadata_noise(title):
            continue
        if any(token in lowered for token in ("layout", "field", "form", "search", "detail", "tab")):
            modules.append(title)
        elif any(token in lowered for token in ("workflow", "navigation", "function list", "requirement", "review", "withdraw", "reopen", "download", "report", "status")):
            modules.append(title)
    return dedupe_non_empty(modules[:6]) or infer_impacted_modules(
        [str(section.get("section_path", "")) for section in sections]
    )


def build_detail_grounded_overview(title: str, sections: list[dict[str, Any]]) -> str:
    focus_sentences = extract_candidate_sentences(sections, limit=3)
    if not focus_sentences:
        return f"这个 PRD《{title}》建议开发优先从主流程、页面动作、状态变化和关键规则四个方面理解。"
    return "；".join(focus_sentences[:3]) + "。"


def build_scope_items(sections: list[dict[str, Any]]) -> list[str]:
    items: list[str] = []
    for section in sections[:10]:
        title = str(section.get("section_path", "")).strip()
        if not title or looks_like_metadata_noise(title):
            continue
        lowered = title.lower()
        if any(token in lowered for token in ("layout", "field", "form", "search", "detail", "workflow", "navigation", "review", "withdraw", "reopen", "download", "report", "status", "function list")):
            items.append(title)
    return dedupe_non_empty(items[:5]) or ["主流程", "页面交互", "状态流转", "规则校验"]


def extract_detail_points(sections: list[dict[str, Any]], *, category: str) -> list[str]:
    candidates = extract_candidate_sentences(sections, limit=20)
    filtered: list[str] = []
    for sentence in candidates:
        lowered = sentence.lower()
        if category == "developer" and any(token in lowered for token in ("click", "show", "display", "default", "status", "submit", "review", "withdraw", "reopen", "download", "field", "layout", "search", "tab", "button")):
            filtered.append(sentence)
        elif category == "frontend" and any(token in lowered for token in ("show", "display", "layout", "field", "search", "tab", "button", "expand", "collapse", "readonly", "visible", "hidden")):
            filtered.append(sentence)
        elif category == "backend" and any(token in lowered for token in ("status", "auto", "populate", "submit", "review", "approve", "download", "report", "history", "system")):
            filtered.append(sentence)
        elif category == "risks" and any(token in lowered for token in ("default", "status", "readonly", "required", "only", "must", "cannot")):
            filtered.append(sentence)
        elif category == "unclear_rules" and any(token in lowered for token in ("required", "must", "cannot", "only", "default", "readonly", "validation")):
            filtered.append(sentence)
        elif category == "missing_edge_cases" and any(token in lowered for token in ("if ", "when ", "reopen", "withdraw", "collapse", "expand", "error", "fail", "empty", "duplicate")):
            filtered.append(sentence)
        elif category == "unclear_ownership" and any(token in lowered for token in ("system", "auto", "populate", "api", "backend", "frontend", "download", "report")):
            filtered.append(sentence)
        elif category == "open_questions" and any(token in lowered for token in ("required", "default", "readonly", "status", "api", "permission", "role")):
            filtered.append(sentence)

    result = dedupe_non_empty([to_brief_point(sentence) for sentence in filtered])
    if result:
        return result[:4]

    fallback_map = {
        "developer": ["先对齐关键页面动作、状态变化和系统自动处理逻辑。"],
        "frontend": ["优先确认页面展示、字段显隐和交互顺序。"],
        "backend": ["优先确认状态流转、自动回填和接口处理逻辑。"],
        "risks": ["字段、默认值和状态切换较多时，容易在实现时出现理解偏差。"],
        "unclear_rules": ["需要再确认哪些规则是强校验，哪些只是展示说明。"],
        "missing_edge_cases": ["需要再确认异常路径、回退动作和失败提示是否完整。"],
        "unclear_ownership": ["需要再确认前端展示和后端处理的职责边界。"],
        "open_questions": ["需要再确认默认值、权限条件和异常处理方式。"],
    }
    return fallback_map[category]


def extract_candidate_sentences(sections: list[dict[str, Any]], *, limit: int) -> list[str]:
    candidates: list[str] = []
    for section in sections:
        title = str(section.get("section_path", "")).strip()
        if section_overview_score(title, str(section.get("content", ""))) < 3:
            continue
        fragments = split_text_fragments(str(section.get("content", "")))
        for fragment in fragments:
            clean = normalize_detail_sentence(fragment)
            if not clean or looks_like_metadata_noise(clean):
                continue
            if len(clean) < 18:
                continue
            candidates.append(clean)
            if len(candidates) >= limit:
                return dedupe_non_empty(candidates)
    return dedupe_non_empty(candidates)


def normalize_detail_sentence(value: str) -> str:
    clean = re.sub(r"\s+", " ", value).strip(" -•\t")
    if not clean:
        return ""
    if re.fullmatch(r"[A-Za-z0-9_.:/ -]{1,24}", clean) and not any(ch.isalpha() for ch in clean if ord(ch) > 127):
        return ""
    return clean


def to_brief_point(sentence: str) -> str:
    sentence = sentence.strip()
    if len(sentence) <= 90:
        return sentence
    return sentence[:89].rstrip(" ,;:") + "…"


def localize_detail_point_zh(sentence: str) -> str:
    value = sentence.strip()
    if not value:
        return value
    replacements = [
        (r'User can click on "([^"]+)" button to ([^.]+)', r'用户可点击“\1”按钮来\2'),
        (r'User can click "([^"]+)" and ([^.]+)', r'用户可点击“\1”，并且\2'),
        (r'User can click on "([^"]+)" from ([^.]+)', r'用户可在\2点击“\1”'),
        (r"By default", "默认"),
        (r"system auto populate", "系统自动回填"),
        (r"System populate", "系统回填"),
        (r"System populated", "系统自动带出"),
        (r"only display after", "仅在以下条件后显示"),
        (r"readonly", "只读"),
        (r"required", "必填"),
        (r"submit", "提交"),
        (r"review", "复核"),
        (r"approve", "审批"),
        (r"withdraw", "撤回"),
        (r"reopen", "重开"),
        (r"download", "下载"),
        (r"search criteria", "搜索条件"),
        (r"detail", "详情"),
        (r"tab", "Tab"),
        (r"button", "按钮"),
        (r"status changes to", "状态变为"),
        (r"status =", "状态 = "),
        (r"click", "点击"),
        (r"field", "字段"),
        (r"layout", "布局"),
    ]
    localized = value
    for pattern, replacement in replacements:
        localized = re.sub(pattern, replacement, localized, flags=re.IGNORECASE)
    localized = re.sub(r"\bSSA\b", "SSA", localized)
    localized = re.sub(r"\bDD\b", "DD", localized)
    localized = localized.replace("User ", "用户")
    localized = localized.replace("System ", "系统")
    localized = localized.strip()
    if not localized.endswith(("。", "？", "！", "…")):
        localized += "。"
    return localized


def infer_focus_items(section_titles: list[str], summaries: list[str], *, target: str) -> list[str]:
    text = " ".join(section_titles + summaries).lower()
    if target == "frontend":
        focus = [
            "页面展示、字段显隐、按钮状态和交互顺序要先对齐。",
            "重点确认表单校验、提示文案、只读条件和异常提示。",
        ]
        if any(token in text for token in ("table", "layout", "search", "field", "tab")):
            focus.append("如果页面字段或表格很多，建议先拆清搜索区、结果区和详情区。")
        return dedupe_non_empty(focus)
    focus = [
        "重点确认状态流转、默认值、系统自动回填和接口出参约束。",
        "需要先对齐哪些动作会触发后端处理，哪些只是前端展示变化。",
    ]
    if any(token in text for token in ("download", "report", "history", "approval", "submit")):
        focus.append("涉及下载、提交、审批或审计记录的逻辑，后端规则要先定清楚。")
    return dedupe_non_empty(focus)


def build_developer_zh_fallback_script(section_title: str, content: str, notes: list[str]) -> str:
    topic = describe_section_topic_zh(section_title, content)
    focus_points = infer_engineering_focus_zh(section_title, content)
    source_signals = derive_source_signals_zh(content)

    sentences = [
        f"这一节主要是在讲{topic}，开发这里先对齐整体行为和实现边界。",
        f"从实现角度看，重点会落在{focus_points[0]}。",
    ]

    if len(focus_points) > 1:
        sentences.append(f"实际 flow 可以先按{focus_points[1]}来理解，先把主路径跑通。")

    if source_signals:
        sentences.append(f"这里还要特别注意{source_signals[0]}。")
    else:
        sentences.append("如果原文里字段很多，不需要逐个硬读，先抓住页面动作、系统反馈和校验规则。")

    if len(source_signals) > 1:
        sentences.append(f"另外，{source_signals[1]}，这部分会直接影响开发实现和联调。")
    else:
        sentences.append("另外要把默认值、状态变化和异常处理补齐，不然上线后很容易出边界问题。")

    if re.search(r"\b(field|layout|search|form|criteria|column)\b", content, flags=re.IGNORECASE):
        sentences.append("这类页面通常字段比较多，但本质上还是围绕搜索条件、展示结果和触发动作这几个点来实现。")
    elif re.search(r"\b(click|submit|expand|collapse|navigate|workflow|flow)\b", content, flags=re.IGNORECASE):
        sentences.append("可以先按用户操作顺序把交互链路串起来，再补展开、收起、跳转和状态回写这些细节。")
    else:
        sentences.append("建议开发先把核心规则和主路径实现清楚，再回头补文档里零散的细节说明。")

    if notes:
        sentences.append("如果后面要继续细化，我们再针对关键规则、依赖接口和异常 case 单独展开。")

    return "".join(sentences)


def describe_section_topic_zh(section_title: str, content: str) -> str:
    signals = f"{section_title} {content}".lower()
    if any(token in signals for token in ("workflow", "flow", "journey")):
        return "这段流程和系统流转"
    if any(token in signals for token in ("navigation", "menu", "tab")):
        return "页面入口、导航方式和页面切换"
    if any(token in signals for token in ("layout", "field", "form", "search criteria", "column")):
        return "页面布局、字段规则和展示方式"
    if any(token in signals for token in ("requirement", "rule", "validation", "criteria")):
        return "这部分规则要求和校验逻辑"
    if any(token in signals for token in ("assessment", "review", "approval", "submit")):
        return "业务处理过程里的关键动作和状态变化"
    return "这一段需求说明"


def infer_engineering_focus_zh(section_title: str, content: str) -> list[str]:
    signals = f"{section_title} {content}".lower()
    focus: list[str] = []
    if any(token in signals for token in ("layout", "field", "form", "search criteria", "column")):
        focus.append("页面布局、字段展示和搜索条件的处理规则")
    if any(token in signals for token in ("click", "expand", "collapse", "navigate", "button", "tab")):
        focus.append("用户点击之后页面怎么变化，以及系统要触发哪些动作")
    if any(token in signals for token in ("default", "show", "display", "visible", "hidden")):
        focus.append("默认展示逻辑、可见性控制和初始状态")
    if any(token in signals for token in ("validation", "required", "must", "cannot", "only")):
        focus.append("校验规则、限制条件和允许范围")
    if any(token in signals for token in ("workflow", "flow", "status", "review", "approval", "submit")):
        focus.append("主流程顺序、状态流转和提交后的系统反馈")
    if not focus:
        focus.append("主流程、关键规则和系统需要响应的行为")
    if len(focus) == 1:
        focus.append("用户操作顺序和页面上的关键变化")
    return focus[:3]


def derive_source_signals_zh(content: str) -> list[str]:
    signals: list[str] = []
    normalized = content.lower()
    if any(token in normalized for token in ("default", "by default", "initial")):
        signals.append("默认值和默认展示状态要跟 PRD 对齐")
    if any(token in normalized for token in ("search", "filter", "criteria")):
        signals.append("搜索条件、筛选逻辑和结果列表的对应关系要处理准确")
    if any(token in normalized for token in ("expand", "collapse")):
        signals.append("展开和收起这类交互不能只改 UI，还要确认对应字段和查询条件怎么切换")
    if any(token in normalized for token in ("show", "display", "visible", "hidden")):
        signals.append("哪些字段显示、什么时候显示，要按规则做清楚")
    if any(token in normalized for token in ("submit", "review", "approve", "assessment")):
        signals.append("提交、审核或评估动作后的结果状态要定义清楚")
    return signals[:2]


def split_text_fragments(text: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return []
    raw_parts = re.split(r"(?:;\s+|\.\s+|\n+)", normalized)
    parts: list[str] = []
    for part in raw_parts:
        clean = part.strip(" -")
        if len(clean) < 8:
            continue
        parts.append(clean)
    return parts


def build_sections_from_text(text: str, chunk_size: int = 1400) -> list[ParsedSection]:
    words = text.split()
    sections: list[ParsedSection] = []
    buffer: list[str] = []
    index = 1
    current_length = 0
    for word in words:
        buffer.append(word)
        current_length += len(word) + 1
        if current_length >= chunk_size:
            body = " ".join(buffer).strip()
            sections.append(ParsedSection(title=f"KB Chunk {index}", section_path=f"KB Chunk {index}", content=body, html_content=""))
            buffer = []
            current_length = 0
            index += 1
    if buffer:
        body = " ".join(buffer).strip()
        sections.append(ParsedSection(title=f"KB Chunk {index}", section_path=f"KB Chunk {index}", content=body, html_content=""))
    return sections


def optimize_tts_text(text: str, *, language_code: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return normalized
    max_chars = 420 if language_code.startswith("zh") else 520
    if len(normalized) <= max_chars:
        return normalized
    truncated = normalized[:max_chars]
    for separator in ("。", ".", "！", "!", "？", "?"):
        index = truncated.rfind(separator)
        if index >= max_chars * 0.6:
            return truncated[: index + 1].strip()
    return truncated.rstrip(",;: ") + ("。" if language_code.startswith("zh") else ".")
