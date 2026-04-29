from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .models import AnswerPayload, ChunkRecord, Citation


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class BriefingStore:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root_dir / "prd_briefing.db"
        self.asset_root = self.root_dir / "assets"
        self.audio_root = self.root_dir / "audio"
        self.asset_root.mkdir(parents=True, exist_ok=True)
        self.audio_root.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                create table if not exists briefing_sessions (
                    session_id text primary key,
                    owner_key text not null,
                    confluence_page_id text not null,
                    confluence_page_url text not null,
                    audience text not null,
                    mode text not null,
                    title text not null,
                    status text not null,
                    created_at text not null,
                    updated_at text not null
                );

                create table if not exists briefing_sources (
                    id integer primary key autoincrement,
                    owner_key text not null,
                    session_id text,
                    source_type text not null,
                    external_id text not null,
                    title text not null,
                    language text not null,
                    source_url text not null,
                    updated_at text not null,
                    metadata_json text not null default '{}'
                );

                create unique index if not exists idx_sources_owner_type_external
                    on briefing_sources(owner_key, source_type, external_id);

                create table if not exists briefing_chunks (
                    id integer primary key autoincrement,
                    source_id integer not null,
                    owner_key text not null,
                    session_id text,
                    source_type text not null,
                    title text not null,
                    section_path text not null,
                    content text not null,
                    html_content text not null default '',
                    image_refs_json text not null default '[]',
                    source_url text not null,
                    updated_at text not null,
                    embedding_json text,
                    foreign key(source_id) references briefing_sources(id)
                );

                create table if not exists briefing_messages (
                    id integer primary key autoincrement,
                    session_id text not null,
                    role text not null,
                    body text not null,
                    answer_language text,
                    groundedness text,
                    citations_json text not null default '[]',
                    audio_url text,
                    created_at text not null
                );

                create table if not exists briefing_audio_cache (
                    id integer primary key autoincrement,
                    owner_key text not null,
                    provider text not null,
                    voice_id text not null,
                    language_code text not null,
                    model_id text not null,
                    text_hash text not null,
                    asset_path text not null,
                    created_at text not null
                );

                create unique index if not exists idx_briefing_audio_cache_lookup
                    on briefing_audio_cache(owner_key, provider, voice_id, language_code, model_id, text_hash);

                create table if not exists briefing_script_cache (
                    id integer primary key autoincrement,
                    owner_key text not null,
                    audience text not null,
                    model_id text not null,
                    prompt_version text not null,
                    section_hash text not null,
                    script text not null,
                    created_at text not null
                );

                create unique index if not exists idx_briefing_script_cache_lookup
                    on briefing_script_cache(owner_key, audience, model_id, prompt_version, section_hash);

                create table if not exists prd_review_results (
                    id integer primary key autoincrement,
                    owner_key text not null,
                    jira_id text not null,
                    jira_link text not null default '',
                    prd_url text not null,
                    prd_updated_at text not null,
                    prompt_version text not null,
                    status text not null,
                    result_markdown text not null default '',
                    error text not null default '',
                    model_id text not null default '',
                    trace_json text not null default '{}',
                    created_at text not null,
                    updated_at text not null
                );

                create unique index if not exists idx_prd_review_results_lookup
                    on prd_review_results(owner_key, jira_id, prd_url, prd_updated_at, prompt_version);
                """
            )
            chunk_columns = {
                row["name"]
                for row in conn.execute("pragma table_info(briefing_chunks)").fetchall()
            }
            if "html_content" not in chunk_columns:
                conn.execute(
                    "alter table briefing_chunks add column html_content text not null default ''"
                )

    def create_session(
        self,
        *,
        owner_key: str,
        confluence_page_id: str,
        confluence_page_url: str,
        audience: str,
        mode: str,
        title: str,
    ) -> str:
        session_id = uuid.uuid4().hex
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                insert into briefing_sessions (
                    session_id, owner_key, confluence_page_id, confluence_page_url,
                    audience, mode, title, status, created_at, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?)
                """,
                (session_id, owner_key, confluence_page_id, confluence_page_url, audience, mode, title, now, now),
            )
        return session_id

    def get_session(self, session_id: str, owner_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "select * from briefing_sessions where session_id = ? and owner_key = ?",
                (session_id, owner_key),
            ).fetchone()
            return dict(row) if row else None

    def upsert_source(
        self,
        *,
        owner_key: str,
        session_id: str | None,
        source_type: str,
        external_id: str,
        title: str,
        language: str,
        source_url: str,
        updated_at: str,
        metadata: dict[str, Any],
    ) -> int:
        payload = json.dumps(metadata, ensure_ascii=False)
        with self.connect() as conn:
            existing = conn.execute(
                """
                select id from briefing_sources
                where owner_key = ? and source_type = ? and external_id = ?
                """,
                (owner_key, source_type, external_id),
            ).fetchone()
            if existing:
                source_id = int(existing["id"])
                conn.execute(
                    """
                    update briefing_sources
                    set session_id = ?, title = ?, language = ?, source_url = ?,
                        updated_at = ?, metadata_json = ?
                    where id = ?
                    """,
                    (session_id, title, language, source_url, updated_at, payload, source_id),
                )
                conn.execute("delete from briefing_chunks where source_id = ?", (source_id,))
                return source_id
            cursor = conn.execute(
                """
                insert into briefing_sources (
                    owner_key, session_id, source_type, external_id, title,
                    language, source_url, updated_at, metadata_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (owner_key, session_id, source_type, external_id, title, language, source_url, updated_at, payload),
            )
            return int(cursor.lastrowid)

    def replace_chunks(self, chunks: list[ChunkRecord]) -> None:
        if not chunks:
            return
        with self.connect() as conn:
            conn.executemany(
                """
                insert into briefing_chunks (
                    source_id, owner_key, session_id, source_type, title, section_path,
                    content, html_content, image_refs_json, source_url, updated_at, embedding_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        chunk.source_id,
                        chunk.owner_key,
                        chunk.session_id,
                        chunk.source_type,
                        chunk.title,
                        chunk.section_path,
                        chunk.content,
                        chunk.html_content,
                        json.dumps(chunk.image_refs, ensure_ascii=False),
                        chunk.source_url,
                        chunk.updated_at,
                        json.dumps(chunk.embedding, ensure_ascii=False) if chunk.embedding is not None else None,
                    )
                    for chunk in chunks
                ],
            )

    def list_session_chunks(self, session_id: str, owner_key: str) -> list[ChunkRecord]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select * from briefing_chunks
                where owner_key = ? and session_id = ?
                order by id asc
                """,
                (owner_key, session_id),
            ).fetchall()
        return [self._row_to_chunk(row) for row in rows]

    def list_session_prd_chunks(self, session_id: str, owner_key: str) -> list[ChunkRecord]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select * from briefing_chunks
                where owner_key = ? and session_id = ?
                order by id asc
                """,
                (owner_key, session_id),
            ).fetchall()
        return [self._row_to_chunk(row) for row in rows]

    def add_message(self, session_id: str, role: str, body: str, answer: AnswerPayload | None = None) -> None:
        created_at = utc_now()
        answer_language = answer.answer_language if answer else None
        groundedness = answer.groundedness if answer else None
        citations = json.dumps([citation.__dict__ for citation in (answer.citations if answer else [])], ensure_ascii=False)
        audio_url = answer.audio_url if answer else None
        with self.connect() as conn:
            conn.execute(
                """
                insert into briefing_messages (
                    session_id, role, body, answer_language, groundedness,
                    citations_json, audio_url, created_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, role, body, answer_language, groundedness, citations, audio_url, created_at),
            )

    def list_recent_messages(self, session_id: str, limit: int = 8) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select * from briefing_messages
                where session_id = ?
                order by id desc
                limit ?
                """,
                (session_id, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def save_audio_blob(self, session_id: str, suffix: str, audio_bytes: bytes) -> str:
        session_dir = self.audio_root / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{uuid.uuid4().hex}.{suffix}"
        path = session_dir / filename
        path.write_bytes(audio_bytes)
        return str(path.relative_to(self.root_dir))

    def get_cached_audio(
        self,
        *,
        owner_key: str,
        provider: str,
        voice_id: str,
        language_code: str,
        model_id: str,
        text: str,
    ) -> str | None:
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        with self.connect() as conn:
            row = conn.execute(
                """
                select asset_path from briefing_audio_cache
                where owner_key = ? and provider = ? and voice_id = ? and language_code = ?
                    and model_id = ? and text_hash = ?
                limit 1
                """,
                (owner_key, provider, voice_id, language_code, model_id, text_hash),
            ).fetchone()
        if not row:
            return None
        asset_path = str(row["asset_path"])
        return asset_path if (self.root_dir / asset_path).exists() else None

    def cache_audio(
        self,
        *,
        owner_key: str,
        provider: str,
        voice_id: str,
        language_code: str,
        model_id: str,
        text: str,
        asset_path: str,
    ) -> None:
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        with self.connect() as conn:
            conn.execute(
                """
                insert or replace into briefing_audio_cache (
                    owner_key, provider, voice_id, language_code, model_id, text_hash, asset_path, created_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (owner_key, provider, voice_id, language_code, model_id, text_hash, asset_path, utc_now()),
            )

    def get_cached_script(
        self,
        *,
        owner_key: str,
        audience: str,
        model_id: str,
        prompt_version: str,
        section_payload: str,
    ) -> str | None:
        section_hash = hashlib.sha256(section_payload.encode("utf-8")).hexdigest()
        with self.connect() as conn:
            row = conn.execute(
                """
                select script from briefing_script_cache
                where owner_key = ? and audience = ? and model_id = ? and prompt_version = ? and section_hash = ?
                limit 1
                """,
                (owner_key, audience, model_id, prompt_version, section_hash),
            ).fetchone()
        return str(row["script"]) if row else None

    def get_cached_script_any_model(
        self,
        *,
        owner_key: str,
        audience: str,
        prompt_version: str,
        section_payload: str,
    ) -> str | None:
        section_hash = hashlib.sha256(section_payload.encode("utf-8")).hexdigest()
        with self.connect() as conn:
            row = conn.execute(
                """
                select script from briefing_script_cache
                where owner_key = ? and audience = ? and prompt_version = ? and section_hash = ?
                order by created_at desc
                limit 1
                """,
                (owner_key, audience, prompt_version, section_hash),
            ).fetchone()
        return str(row["script"]) if row else None

    def cache_script(
        self,
        *,
        owner_key: str,
        audience: str,
        model_id: str,
        prompt_version: str,
        section_payload: str,
        script: str,
    ) -> None:
        section_hash = hashlib.sha256(section_payload.encode("utf-8")).hexdigest()
        with self.connect() as conn:
            conn.execute(
                """
                insert or replace into briefing_script_cache (
                    owner_key, audience, model_id, prompt_version, section_hash, script, created_at
                ) values (?, ?, ?, ?, ?, ?, ?)
                """,
                (owner_key, audience, model_id, prompt_version, section_hash, script, utc_now()),
            )

    def get_prd_review_result(
        self,
        *,
        owner_key: str,
        jira_id: str,
        prd_url: str,
        prd_updated_at: str,
        prompt_version: str,
    ) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                select * from prd_review_results
                where owner_key = ? and jira_id = ? and prd_url = ?
                    and prd_updated_at = ? and prompt_version = ?
                limit 1
                """,
                (owner_key, jira_id, prd_url, prd_updated_at, prompt_version),
            ).fetchone()
        return self._row_to_prd_review(row) if row else None

    def save_prd_review_result(
        self,
        *,
        owner_key: str,
        jira_id: str,
        jira_link: str,
        prd_url: str,
        prd_updated_at: str,
        prompt_version: str,
        status: str,
        result_markdown: str = "",
        error: str = "",
        model_id: str = "",
        trace: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        trace_json = json.dumps(trace or {}, ensure_ascii=False, sort_keys=True)
        with self.connect() as conn:
            conn.execute(
                """
                insert into prd_review_results (
                    owner_key, jira_id, jira_link, prd_url, prd_updated_at,
                    prompt_version, status, result_markdown, error, model_id,
                    trace_json, created_at, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(owner_key, jira_id, prd_url, prd_updated_at, prompt_version)
                do update set
                    jira_link = excluded.jira_link,
                    status = excluded.status,
                    result_markdown = excluded.result_markdown,
                    error = excluded.error,
                    model_id = excluded.model_id,
                    trace_json = excluded.trace_json,
                    updated_at = excluded.updated_at
                """,
                (
                    owner_key,
                    jira_id,
                    jira_link,
                    prd_url,
                    prd_updated_at,
                    prompt_version,
                    status,
                    result_markdown,
                    error,
                    model_id,
                    trace_json,
                    now,
                    now,
                ),
            )
        result = self.get_prd_review_result(
            owner_key=owner_key,
            jira_id=jira_id,
            prd_url=prd_url,
            prd_updated_at=prd_updated_at,
            prompt_version=prompt_version,
        )
        return result or {
            "owner_key": owner_key,
            "jira_id": jira_id,
            "jira_link": jira_link,
            "prd_url": prd_url,
            "prd_updated_at": prd_updated_at,
            "prompt_version": prompt_version,
            "status": status,
            "result_markdown": result_markdown,
            "error": error,
            "model_id": model_id,
            "trace": trace or {},
            "created_at": now,
            "updated_at": now,
        }

    def save_asset(self, session_id: str, filename: str, content: bytes) -> str:
        asset_dir = self.asset_root / session_id
        asset_dir.mkdir(parents=True, exist_ok=True)
        path = asset_dir / filename
        path.write_bytes(content)
        return str(path.relative_to(self.root_dir))

    def _row_to_chunk(self, row: sqlite3.Row) -> ChunkRecord:
        return ChunkRecord(
            source_id=int(row["source_id"]),
            owner_key=str(row["owner_key"]),
            session_id=row["session_id"],
            source_type=str(row["source_type"]),
            title=str(row["title"]),
            section_path=str(row["section_path"]),
            content=str(row["content"]),
            html_content=str(row["html_content"] or ""),
            image_refs=json.loads(row["image_refs_json"] or "[]"),
            source_url=str(row["source_url"]),
            updated_at=str(row["updated_at"]),
            embedding=json.loads(row["embedding_json"]) if row["embedding_json"] else None,
        )

    def _row_to_prd_review(self, row: sqlite3.Row) -> dict[str, Any]:
        try:
            trace = json.loads(row["trace_json"] or "{}")
        except json.JSONDecodeError:
            trace = {}
        return {
            "id": int(row["id"]),
            "owner_key": str(row["owner_key"]),
            "jira_id": str(row["jira_id"]),
            "jira_link": str(row["jira_link"] or ""),
            "prd_url": str(row["prd_url"]),
            "prd_updated_at": str(row["prd_updated_at"]),
            "prompt_version": str(row["prompt_version"]),
            "status": str(row["status"]),
            "result_markdown": str(row["result_markdown"] or ""),
            "error": str(row["error"] or ""),
            "model_id": str(row["model_id"] or ""),
            "trace": trace if isinstance(trace, dict) else {},
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

def citation_from_chunk(chunk: ChunkRecord) -> Citation:
    return Citation(
        title=chunk.title,
        section_path=chunk.section_path,
        source_type=chunk.source_type,
        source_url=chunk.source_url,
        snippet=(chunk.content[:240] + "...") if len(chunk.content) > 240 else chunk.content,
    )
