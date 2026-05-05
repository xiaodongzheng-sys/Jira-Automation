from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any


VISIBILITY_PRIVATE = "private_to_owner"
VISIBILITY_TEAM = "team_visible"
VISIBILITY_VALUES = {VISIBILITY_PRIVATE, VISIBILITY_TEAM}

FEEDBACK_ACTIONS = {"accept", "correct", "ignore", "private", "stale", "important", "about_me", "not_about_me"}

SOURCE_PRECEDENCE = {
    "user_feedback": 100,
    "gmail_sent_monthly_report": 80,
    "team_dashboard": 70,
    "bpmis": 65,
    "jira": 65,
    "confluence": 60,
    "meeting_recorder": 50,
    "gmail_attachment": 46,
    "gmail_drive_link": 46,
    "gmail": 40,
    "seatalk": 40,
    "source_code_qa": 20,
}

REVIEW_ITEM_TYPE_PRIORITY = {
    "key_project": 100,
    "risk": 95,
    "blocker": 95,
    "decision": 90,
    "open_loop": 88,
    "todo": 82,
    "stakeholder": 78,
    "personal_preference": 76,
    "owner_speech_candidate": 74,
    "curated_report": 70,
    "project": 60,
}

NON_REVIEWED_ACTIONS = {"accept", "ignore", "stale", "about_me", "not_about_me"}

REVIEW_ITEM_TYPES = {
    "blocker",
    "curated_report",
    "decision",
    "key_project",
    "open_loop",
    "owner_speech_candidate",
    "personal_preference",
    "project",
    "risk",
    "stakeholder",
    "todo",
}


MEETING_ATTRIBUTION_METADATA = {
    "attribution_scope": "meeting",
    "speaker_attribution": "unknown",
    "owner_role": "memory_owner",
    "owner_is_speaker": False,
    "personal_profile_eligible": False,
    "attribution_rule_id": "meeting_facts_are_not_owner_speech_v1",
    "attribution_note": "Meeting Recorder owner controls privacy scope; extracted facts are meeting-level unless a speaker is explicitly known.",
}

OWNER_SPEECH_CANDIDATE_METADATA = {
    "attribution_scope": "owner_speech_candidate",
    "speaker_attribution": "local_microphone_candidate",
    "owner_role": "memory_owner",
    "owner_is_speaker": "candidate",
    "personal_profile_eligible": "candidate_after_review",
    "attribution_rule_id": "owner_speech_candidate_requires_local_microphone_v1",
    "attribution_note": "Only local microphone speech candidates may be used as possible owner-authored signals; they are not diarized proof.",
}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _stable_json(value: Any) -> str:
    try:
        return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return json.dumps({}, ensure_ascii=False, sort_keys=True)


def _load_json(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        values = []
    return [str(item or "").strip() for item in values if str(item or "").strip()]


def normalize_visibility(value: Any, *, default: str = VISIBILITY_PRIVATE) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in VISIBILITY_VALUES else default


def _memory_id(*parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()
    return digest[:32]


def _extract_jira_keys(text: str) -> list[str]:
    return sorted({match.upper() for match in re.findall(r"\b[A-Z][A-Z0-9]+-\d+\b", str(text or ""))})


def _extract_bpmis_ids(text: str) -> list[str]:
    return sorted({match for match in re.findall(r"\b\d{5,}\b", str(text or ""))})


class WorkMemoryStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._ensure_db()

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _ensure_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS work_memory_items (
                    item_id TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    item_type TEXT NOT NULL,
                    owner_email TEXT NOT NULL,
                    visibility TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    content TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    ingestion_status TEXT NOT NULL,
                    weight REAL NOT NULL DEFAULT 1.0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source_type, source_id, item_type, owner_email)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS work_memory_entities (
                    entity_id TEXT PRIMARY KEY,
                    entity_type TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    label TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, entity_key)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS work_memory_item_entities (
                    item_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    relation TEXT NOT NULL,
                    PRIMARY KEY(item_id, entity_id, relation)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS work_memory_feedback (
                    feedback_id TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    correction_text TEXT NOT NULL,
                    visibility_override TEXT NOT NULL,
                    owner_email TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS work_memory_ingestion_runs (
                    run_id TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    owner_email TEXT NOT NULL,
                    cursor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    scanned_count INTEGER NOT NULL DEFAULT 0,
                    matched_count INTEGER NOT NULL DEFAULT 0,
                    recorded_count INTEGER NOT NULL DEFAULT 0,
                    duplicate_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS work_memory_materialized (
                    materialized_id TEXT PRIMARY KEY,
                    owner_email TEXT NOT NULL,
                    graph_type TEXT NOT NULL,
                    graph_key TEXT NOT NULL,
                    visibility TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    content TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(owner_email, graph_type, graph_key)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS work_memory_superagent_audit (
                    audit_id TEXT PRIMARY KEY,
                    owner_email TEXT NOT NULL,
                    user_email TEXT NOT NULL,
                    query TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    visibility_scope TEXT NOT NULL,
                    used_item_ids_json TEXT NOT NULL,
                    used_private_evidence INTEGER NOT NULL DEFAULT 0,
                    answer_confidence TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS work_memory_superagent_eval_cases (
                    case_id TEXT PRIMARY KEY,
                    owner_email TEXT NOT NULL,
                    question TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    expected_source_type TEXT NOT NULL,
                    expected_text TEXT NOT NULL,
                    visibility_scope TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(owner_email, question, task_type)
                )
                """
            )
            try:
                connection.execute(
                    """
                    CREATE VIRTUAL TABLE IF NOT EXISTS work_memory_items_fts
                    USING fts5(item_id UNINDEXED, summary, content, metadata)
                    """
                )
            except sqlite3.Error:
                pass
            connection.execute("CREATE INDEX IF NOT EXISTS idx_work_memory_items_owner ON work_memory_items(owner_email)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_work_memory_items_visibility ON work_memory_items(visibility)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_work_memory_items_type ON work_memory_items(item_type)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_work_memory_ingestion_source ON work_memory_ingestion_runs(source_type, owner_email)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_work_memory_materialized_owner ON work_memory_materialized(owner_email, graph_type)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_work_memory_audit_owner ON work_memory_superagent_audit(owner_email, created_at)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_work_memory_eval_owner ON work_memory_superagent_eval_cases(owner_email, task_type)")
            connection.commit()

    def record_memory_item(
        self,
        *,
        source_type: str,
        source_id: str,
        owner_email: str = "",
        visibility: str = VISIBILITY_PRIVATE,
        item_type: str = "evidence",
        observed_at: str = "",
        summary: str = "",
        content: str = "",
        evidence: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        ingestion_status: str = "ok",
        entities: list[dict[str, Any]] | None = None,
        weight: float = 1.0,
    ) -> dict[str, Any]:
        normalized_source_type = str(source_type or "").strip() or "unknown"
        normalized_source_id = str(source_id or "").strip() or uuid.uuid4().hex
        normalized_item_type = str(item_type or "").strip() or "evidence"
        normalized_owner = _normalize_email(owner_email)
        normalized_visibility = normalize_visibility(visibility)
        timestamp = _now_iso()
        item_id = _memory_id(normalized_source_type, normalized_source_id, normalized_item_type, normalized_owner)
        summary_text = str(summary or "").strip()
        content_text = str(content or "").strip()
        if not summary_text:
            summary_text = content_text[:500].strip() or f"{normalized_source_type} memory"
        evidence_json = _stable_json(evidence or {})
        metadata_json = _stable_json(metadata or {})
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO work_memory_items (
                    item_id, source_type, source_id, item_type, owner_email, visibility, observed_at,
                    summary, content, evidence_json, metadata_json, ingestion_status, weight, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_type, source_id, item_type, owner_email) DO UPDATE SET
                    visibility = excluded.visibility,
                    observed_at = excluded.observed_at,
                    summary = excluded.summary,
                    content = excluded.content,
                    evidence_json = excluded.evidence_json,
                    metadata_json = excluded.metadata_json,
                    ingestion_status = excluded.ingestion_status,
                    weight = excluded.weight,
                    updated_at = excluded.updated_at
                """,
                (
                    item_id,
                    normalized_source_type,
                    normalized_source_id,
                    normalized_item_type,
                    normalized_owner,
                    normalized_visibility,
                    str(observed_at or "").strip() or timestamp,
                    summary_text,
                    content_text,
                    evidence_json,
                    metadata_json,
                    str(ingestion_status or "ok").strip() or "ok",
                    float(weight or 1.0),
                    timestamp,
                    timestamp,
                ),
            )
            self._refresh_fts(connection, item_id, summary_text, content_text, metadata_json)
            for entity in entities or []:
                entity_id = self._upsert_entity(connection, entity)
                if entity_id:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO work_memory_item_entities (item_id, entity_id, relation)
                        VALUES (?, ?, ?)
                        """,
                        (item_id, entity_id, str(entity.get("relation") or "mentions").strip() or "mentions"),
                    )
            connection.commit()
        return self.get_item(item_id) or {"item_id": item_id, "status": "ok"}

    def _refresh_fts(self, connection: sqlite3.Connection, item_id: str, summary: str, content: str, metadata_json: str) -> None:
        try:
            connection.execute("DELETE FROM work_memory_items_fts WHERE item_id = ?", (item_id,))
            connection.execute(
                "INSERT INTO work_memory_items_fts (item_id, summary, content, metadata) VALUES (?, ?, ?, ?)",
                (item_id, summary, content, metadata_json),
            )
        except sqlite3.Error:
            return

    def _upsert_entity(self, connection: sqlite3.Connection, entity: dict[str, Any]) -> str:
        entity_type = str(entity.get("entity_type") or entity.get("type") or "").strip().lower()
        entity_key = str(entity.get("entity_key") or entity.get("key") or "").strip()
        if not entity_type or not entity_key:
            return ""
        label = str(entity.get("label") or entity_key).strip()
        timestamp = _now_iso()
        entity_id = _memory_id(entity_type, entity_key.casefold())
        connection.execute(
            """
            INSERT INTO work_memory_entities (entity_id, entity_type, entity_key, label, metadata_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_type, entity_key) DO UPDATE SET
                label = excluded.label,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (entity_id, entity_type, entity_key, label, _stable_json(entity.get("metadata") or {}), timestamp, timestamp),
        )
        return entity_id

    def get_item(self, item_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM work_memory_items WHERE item_id = ?", (item_id,)).fetchone()
            if not row:
                return None
            return self._public_item(connection, row)

    def existing_source_ids(
        self,
        *,
        source_type: str,
        owner_email: str,
        source_ids: list[str],
        item_type: str = "",
    ) -> set[str]:
        normalized_source_type = str(source_type or "").strip()
        normalized_owner = _normalize_email(owner_email)
        normalized_ids = []
        seen_ids: set[str] = set()
        for source_id in source_ids:
            normalized_id = str(source_id or "").strip()
            if normalized_id and normalized_id not in seen_ids:
                seen_ids.add(normalized_id)
                normalized_ids.append(normalized_id)
        if not normalized_source_type or not normalized_owner or not normalized_ids:
            return set()

        found: set[str] = set()
        with self._connect() as connection:
            for start in range(0, len(normalized_ids), 500):
                chunk = normalized_ids[start:start + 500]
                placeholders = ",".join("?" for _ in chunk)
                params: list[Any] = [normalized_source_type, normalized_owner, *chunk]
                sql = (
                    "SELECT DISTINCT source_id FROM work_memory_items "
                    f"WHERE source_type = ? AND owner_email = ? AND source_id IN ({placeholders})"
                )
                normalized_item_type = str(item_type or "").strip()
                if normalized_item_type:
                    sql += " AND item_type = ?"
                    params.append(normalized_item_type)
                rows = connection.execute(sql, params).fetchall()
                found.update(str(row["source_id"]) for row in rows)
        return found

    def record_memory_feedback(
        self,
        *,
        item_id: str,
        action: str,
        owner_email: str,
        correction_text: str = "",
        visibility_override: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        normalized_action = str(action or "").strip().lower()
        if normalized_action not in FEEDBACK_ACTIONS:
            raise ValueError(f"Unsupported work memory feedback action: {action}")
        normalized_owner = _normalize_email(owner_email)
        normalized_visibility = normalize_visibility(visibility_override, default="") if visibility_override else ""
        timestamp = _now_iso()
        feedback_id = uuid.uuid4().hex
        with self._connect() as connection:
            item = connection.execute("SELECT * FROM work_memory_items WHERE item_id = ?", (item_id,)).fetchone()
            if item is None:
                raise KeyError(f"Unknown work memory item: {item_id}")
            if normalized_action == "private":
                normalized_visibility = VISIBILITY_PRIVATE
            connection.execute(
                """
                INSERT INTO work_memory_feedback (
                    feedback_id, item_id, action, correction_text, visibility_override, owner_email, reason, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    feedback_id,
                    item_id,
                    normalized_action,
                    str(correction_text or "").strip(),
                    normalized_visibility,
                    normalized_owner,
                    str(reason or "").strip(),
                    timestamp,
                ),
            )
            if normalized_visibility:
                connection.execute(
                    "UPDATE work_memory_items SET visibility = ?, updated_at = ? WHERE item_id = ?",
                    (normalized_visibility, timestamp, item_id),
                )
            if normalized_action == "important":
                connection.execute(
                    "UPDATE work_memory_items SET weight = MAX(weight, 1.5), updated_at = ? WHERE item_id = ?",
                    (timestamp, item_id),
                )
            if normalized_action in {"ignore", "stale", "not_about_me"}:
                connection.execute(
                    "UPDATE work_memory_items SET weight = MIN(weight, ?), updated_at = ? WHERE item_id = ?",
                    (0.1 if normalized_action == "not_about_me" else 0.2, timestamp, item_id),
                )
            if normalized_action in {"about_me", "not_about_me"}:
                metadata = _load_json(item["metadata_json"], {})
                if normalized_action == "about_me":
                    metadata["personal_profile_review"] = "confirmed_about_me"
                    metadata["personal_profile_eligible"] = True
                    connection.execute(
                        "UPDATE work_memory_items SET metadata_json = ?, weight = MAX(weight, 1.4), updated_at = ? WHERE item_id = ?",
                        (_stable_json(metadata), timestamp, item_id),
                    )
                else:
                    metadata["personal_profile_review"] = "rejected_not_about_me"
                    metadata["personal_profile_eligible"] = False
                    connection.execute(
                        "UPDATE work_memory_items SET metadata_json = ?, updated_at = ? WHERE item_id = ?",
                        (_stable_json(metadata), timestamp, item_id),
                    )
            if normalized_action == "accept":
                connection.execute(
                    "UPDATE work_memory_items SET weight = MAX(weight, 1.2), updated_at = ? WHERE item_id = ?",
                    (timestamp, item_id),
                )
            connection.commit()
            row = connection.execute("SELECT * FROM work_memory_items WHERE item_id = ?", (item_id,)).fetchone()
            return {"status": "ok", "feedback_id": feedback_id, "item": self._public_item(connection, row)}

    def record_ingestion_run(
        self,
        *,
        source_type: str,
        owner_email: str,
        cursor: str = "",
        status: str = "ok",
        scanned_count: int = 0,
        matched_count: int = 0,
        recorded_count: int = 0,
        duplicate_count: int = 0,
        failed_count: int = 0,
        error: str = "",
        metadata: dict[str, Any] | None = None,
        started_at: str = "",
        completed_at: str = "",
    ) -> dict[str, Any]:
        timestamp = _now_iso()
        run_id = uuid.uuid4().hex
        normalized_source = str(source_type or "").strip() or "unknown"
        normalized_owner = _normalize_email(owner_email)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO work_memory_ingestion_runs (
                    run_id, source_type, owner_email, cursor, status, scanned_count, matched_count,
                    recorded_count, duplicate_count, failed_count, error, metadata_json, started_at, completed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    normalized_source,
                    normalized_owner,
                    str(cursor or ""),
                    str(status or "ok").strip() or "ok",
                    int(scanned_count or 0),
                    int(matched_count or 0),
                    int(recorded_count or 0),
                    int(duplicate_count or 0),
                    int(failed_count or 0),
                    str(error or "").strip(),
                    _stable_json(metadata or {}),
                    started_at or timestamp,
                    completed_at or timestamp,
                ),
            )
            connection.commit()
        return {
            "run_id": run_id,
            "source_type": normalized_source,
            "owner_email": normalized_owner,
            "cursor": str(cursor or ""),
            "status": str(status or "ok").strip() or "ok",
            "scanned_count": int(scanned_count or 0),
            "matched_count": int(matched_count or 0),
            "recorded_count": int(recorded_count or 0),
            "duplicate_count": int(duplicate_count or 0),
            "failed_count": int(failed_count or 0),
            "error": str(error or "").strip(),
            "metadata": metadata or {},
            "started_at": started_at or timestamp,
            "completed_at": completed_at or timestamp,
        }

    def query_work_memory(
        self,
        *,
        owner_email: str,
        visibility_scope: str = "owner",
        query: str = "",
        filters: dict[str, Any] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        normalized_owner = _normalize_email(owner_email)
        filters = filters if isinstance(filters, dict) else {}
        where = []
        params: list[Any] = []
        if visibility_scope == "team":
            where.append("visibility = ?")
            params.append(VISIBILITY_TEAM)
        else:
            where.append("(visibility = ? OR owner_email = ?)")
            params.extend([VISIBILITY_TEAM, normalized_owner])
        if filters.get("item_type"):
            where.append("item_type = ?")
            params.append(str(filters["item_type"]))
        if filters.get("source_type"):
            where.append("source_type = ?")
            params.append(str(filters["source_type"]))
        sql = "SELECT * FROM work_memory_items"
        if where:
            sql += " WHERE " + " AND ".join(where)
        lowered_query = str(query or "").strip().casefold()
        requested_limit = max(1, min(int(limit or 50), 200))
        fetch_limit = 1000 if lowered_query else requested_limit
        sql += " ORDER BY observed_at DESC, updated_at DESC LIMIT ?"
        params.append(fetch_limit)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
            items = [self._public_item(connection, row) for row in rows]
        if lowered_query:
            items = [
                item
                for item in items
                if lowered_query in f"{item.get('summary')} {item.get('content')} {json.dumps(item.get('metadata'), ensure_ascii=False)}".casefold()
            ]
        return items[:requested_limit]

    def review_candidates(self, *, owner_email: str, limit: int = 50) -> list[dict[str, Any]]:
        candidates = self.query_work_memory(
            owner_email=owner_email,
            visibility_scope="owner",
            limit=200,
        )
        filtered = [
            item
            for item in candidates
            if item.get("item_type") in REVIEW_ITEM_TYPES and item.get("latest_feedback_action") not in NON_REVIEWED_ACTIONS
        ]
        filtered.sort(key=self._review_score, reverse=True)
        return filtered[: max(1, min(int(limit or 50), 200))]

    def project_timeline(self, *, project_ref: str, owner_email: str, visibility_scope: str = "owner", limit: int = 100) -> list[dict[str, Any]]:
        ref = str(project_ref or "").strip()
        if not ref:
            return []
        items = self._query_by_resolved_terms(
            query=ref,
            owner_email=owner_email,
            visibility_scope=visibility_scope,
            task_type="project_status",
            limit=max(1, min(int(limit or 100), 200)),
        )
        return self._sort_by_precedence(items)

    def resolve_work_entity(self, *, query: str, owner_email: str, entity_type: str = "") -> dict[str, Any]:
        normalized_query = str(query or "").strip()
        normalized_type = str(entity_type or "").strip().lower()
        detected_type = normalized_type or self._detect_entity_type(normalized_query)
        terms = [normalized_query] if normalized_query else []
        candidates: list[dict[str, Any]] = []
        if not normalized_query:
            return {"status": "ok", "query": "", "entity_type": detected_type or "unknown", "canonical_key": "", "aliases": [], "candidates": []}
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT entity_type, entity_key, label, metadata_json
                FROM work_memory_entities
                WHERE (? = '' OR entity_type = ?)
                ORDER BY updated_at DESC
                LIMIT 1000
                """,
                (detected_type, detected_type),
            ).fetchall()
        lowered_query = normalized_query.casefold()
        for row in rows:
            metadata = _load_json(row["metadata_json"], {})
            alias_values = [row["entity_key"], row["label"]]
            for key in ("email", "name", "seatalk_name", "jira_user", "pm_name", "aliases"):
                value = metadata.get(key) if isinstance(metadata, dict) else None
                if isinstance(value, list):
                    alias_values.extend(str(item or "") for item in value)
                elif value:
                    alias_values.append(str(value))
            normalized_aliases = sorted({str(item or "").strip() for item in alias_values if str(item or "").strip()})
            if any(lowered_query in alias.casefold() or alias.casefold() in lowered_query for alias in normalized_aliases):
                candidates.append(
                    {
                        "entity_type": row["entity_type"],
                        "entity_key": row["entity_key"],
                        "label": row["label"],
                        "aliases": normalized_aliases,
                        "metadata": metadata if isinstance(metadata, dict) else {},
                    }
                )
                terms.extend(normalized_aliases)
        for jira_key in _extract_jira_keys(normalized_query):
            candidates.append({"entity_type": "jira_key", "entity_key": jira_key, "label": jira_key, "aliases": [jira_key], "metadata": {}})
            terms.append(jira_key)
        for bpmis_id in _extract_bpmis_ids(normalized_query):
            candidates.append({"entity_type": "bpmis_id", "entity_key": bpmis_id, "label": bpmis_id, "aliases": [bpmis_id], "metadata": {}})
            terms.insert(0, bpmis_id)
        canonical = self._canonical_key_from_candidates(normalized_query=normalized_query, entity_type=detected_type, candidates=candidates)
        aliases = sorted({term for term in terms if term})
        return {
            "status": "ok",
            "query": normalized_query,
            "entity_type": detected_type or "unknown",
            "canonical_key": canonical,
            "aliases": aliases,
            "candidates": candidates[:20],
        }

    def distill_work_memory(
        self,
        *,
        owner_email: str,
        date_range: str = "90d",
        sources: list[str] | None = None,
        project_refs: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_owner = _normalize_email(owner_email)
        refs = [str(ref or "").strip() for ref in project_refs or [] if str(ref or "").strip()]
        if not refs:
            refs = self._candidate_project_refs(owner_email=normalized_owner, sources=sources)
        materialized_projects = []
        for ref in refs[:30]:
            materialized_projects.append(self.materialize_project_profile(project_key=ref, owner_email=normalized_owner))
        personal_profile = self.materialize_personal_work_profile(owner_email=normalized_owner)
        self.record_ingestion_run(
            source_type="work_memory_distill",
            owner_email=normalized_owner,
            cursor=date_range,
            status="ok",
            scanned_count=len(refs),
            matched_count=len(materialized_projects),
            recorded_count=len(materialized_projects) + (1 if personal_profile.get("materialized") else 0),
            metadata={"sources": sources or [], "date_range": date_range},
        )
        return {
            "status": "ok",
            "project_refs": refs[:30],
            "project_profiles": materialized_projects,
            "personal_work_profile": personal_profile,
        }

    def materialize_project_profile(self, *, project_key: str, owner_email: str) -> dict[str, Any]:
        ref = str(project_key or "").strip()
        if not ref:
            return {"status": "skipped", "reason": "missing_project_key", "materialized": False}
        items = self.project_timeline(project_ref=ref, owner_email=owner_email, visibility_scope="owner", limit=80)
        if not items:
            return {"status": "skipped", "reason": "no_evidence", "project_key": ref, "materialized": False}
        key_items = [item for item in items if item.get("item_type") in {"key_project", "project", "curated_report"}]
        open_loops = [item for item in items if item.get("item_type") in {"todo", "risk", "blocker", "open_loop"} and item.get("latest_feedback_action") not in {"ignore", "stale"}]
        stakeholders = self._stakeholders_from_items(items)
        top = key_items[0] if key_items else items[0]
        visibility = VISIBILITY_TEAM if all(item.get("visibility") == VISIBILITY_TEAM for item in items) else VISIBILITY_PRIVATE
        payload = {
            "project_key": ref,
            "summary": top.get("summary") or ref,
            "timeline": [self._evidence_summary(item, include_private_summary=True) for item in items[:20]],
            "open_loops": [self._evidence_summary(item, include_private_summary=True) for item in open_loops[:20]],
            "stakeholders": stakeholders,
            "source_precedence": SOURCE_PRECEDENCE,
        }
        return self._upsert_materialized(
            owner_email=owner_email,
            graph_type="project_profile",
            graph_key=ref,
            visibility=visibility,
            summary=f"Project profile: {ref}",
            content=json.dumps(payload, ensure_ascii=False, sort_keys=True),
            evidence={"item_ids": [item["item_id"] for item in items[:80]]},
            metadata={
                "project_key": ref,
                "timeline_count": len(items),
                "open_loop_count": len(open_loops),
                "stakeholder_count": len(stakeholders),
                "materialized_layers": ["project_profile", "project_timeline", "stakeholder_map", "open_loops"],
            },
        )

    def materialize_personal_work_profile(self, *, owner_email: str) -> dict[str, Any]:
        items = self.query_work_memory(owner_email=owner_email, visibility_scope="owner", limit=200)
        eligible = []
        for item in items:
            latest = item.get("latest_feedback_action")
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            if latest not in {"accept", "about_me"}:
                continue
            if item.get("item_type") == "owner_speech_candidate" and metadata.get("attribution_scope") == "owner_speech_candidate":
                eligible.append(item)
            elif item.get("item_type") == "personal_preference":
                eligible.append(item)
        rejected = [
            item
            for item in items
            if item.get("latest_feedback_action") == "not_about_me"
        ]
        if not eligible:
            self._delete_materialized(owner_email=owner_email, graph_type="personal_work_profile", graph_key=_normalize_email(owner_email))
            return {"status": "ok", "materialized": False, "eligible_count": 0, "rejected_count": len(rejected)}
        payload = {
            "owner_email": _normalize_email(owner_email),
            "confirmed_signals": [self._evidence_summary(item, include_private_summary=True) for item in self._sort_by_precedence(eligible)[:30]],
            "rejected_not_about_me_count": len(rejected),
            "attribution_rule": "owner_speech_candidate_requires_review_v1",
        }
        return self._upsert_materialized(
            owner_email=owner_email,
            graph_type="personal_work_profile",
            graph_key=_normalize_email(owner_email),
            visibility=VISIBILITY_PRIVATE,
            summary="Personal work profile",
            content=json.dumps(payload, ensure_ascii=False, sort_keys=True),
            evidence={"item_ids": [item["item_id"] for item in eligible[:50]]},
            metadata={
                "eligible_count": len(eligible),
                "rejected_not_about_me_count": len(rejected),
                "requires_review": True,
                "materialized_layers": ["personal_work_profile"],
            },
        )

    def query_superagent_context(
        self,
        *,
        query: str,
        owner_email: str,
        visibility_scope: str = "owner",
        task_type: str = "general",
        limit: int = 12,
    ) -> dict[str, Any]:
        entity_resolution = self.resolve_work_entity(query=query, owner_email=owner_email, entity_type="")
        domain = self._domain_from_query(query)
        items = self._query_by_resolved_terms(
            query=query,
            owner_email=owner_email,
            visibility_scope=visibility_scope,
            task_type=task_type,
            limit=max(1, min(int(limit or 12), 30)),
            resolved=entity_resolution,
        )
        if not items and task_type in {"follow_up", "open_loop_check"}:
            items = self.query_work_memory(
                owner_email=owner_email,
                visibility_scope=visibility_scope,
                filters={"item_type": "todo"},
                limit=max(1, min(int(limit or 12), 30)),
            )
        items = self._rerank_items_for_query(
            [item for item in items if item.get("latest_feedback_action") not in {"ignore", "stale"}],
            query=query,
            task_type=task_type,
            domain=domain,
        )
        materialized = self._query_materialized(owner_email=owner_email, query=query, visibility_scope=visibility_scope, limit=8)
        return {
            "query": str(query or "").strip(),
            "task_type": str(task_type or "general").strip() or "general",
            "visibility_scope": visibility_scope,
            "domain": domain,
            "entity_resolution": entity_resolution,
            "items": items[: max(1, min(int(limit or 12), 30))],
            "materialized": materialized,
        }

    def generate_superagent_answer(self, *, task_type: str, query: str, context: dict[str, Any]) -> dict[str, Any]:
        return self.generate_llm_superagent_answer(task_type=task_type, query=query, context=context)

    def generate_llm_superagent_answer(self, *, task_type: str, query: str, context: dict[str, Any]) -> dict[str, Any]:
        items = context.get("items") if isinstance(context.get("items"), list) else []
        materialized = context.get("materialized") if isinstance(context.get("materialized"), list) else []
        visibility_scope = str(context.get("visibility_scope") or "owner").strip().lower() or "owner"
        owner_view = visibility_scope != "team"
        evidence_items = items[:8]
        evidence = [
            self._evidence_summary(
                item,
                include_private_summary=owner_view,
                include_excerpt=owner_view,
                query=query,
            )
            for item in evidence_items
        ]
        if not evidence_items and not materialized:
            return {
                "status": "ok",
                "answer": "I do not have enough Work Memory evidence to answer this yet.",
                "direct_answer": "I do not have enough Work Memory evidence to answer this yet.",
                "supporting_evidence": [],
                "evidence": [],
                "confidence": "none",
                "unknowns": ["No matching Work Memory evidence was found."],
                "follow_up_candidates": [],
                "readonly": True,
            }
        label = {
            "project_status": "Project status",
            "follow_up": "Follow-up items",
            "meeting_prep": "Meeting prep",
            "stakeholder_brief": "Stakeholder brief",
            "monthly_focus": "Monthly focus",
            "open_loop_check": "Open loops",
        }.get(str(task_type or "").strip(), "Work Memory summary")
        direct_points = []
        for item in evidence_items[:5]:
            excerpt = self._evidence_excerpt(item, query=query, allow_private=owner_view)
            if excerpt:
                direct_points.append(excerpt)
            else:
                direct_points.append(str(item.get("summary") or item.get("content") or item.get("item_id") or "").strip())
        direct_answer = "\n".join(f"- {point}" for point in direct_points if point).strip()
        if not direct_answer:
            direct_answer = "I found related Work Memory evidence, but it does not contain enough detail for a direct answer."
        lines = [f"{label} based on Work Memory evidence:", direct_answer]
        for item in evidence_items[:5]:
            prefix = self._answer_prefix(item)
            evidence_label = item.get("summary") or item.get("item_id")
            lines.append(f"- Evidence: {prefix}{evidence_label}")
        if materialized:
            lines.append(f"- Materialized memory available: {', '.join(str(item.get('graph_type') or '') for item in materialized[:3] if item.get('graph_type'))}.")
        lines.append("This is read-only; no external system was updated.")
        follow_ups = [
            self._evidence_summary(item, include_private_summary=False)
            for item in evidence_items
            if item.get("item_type") in {"todo", "open_loop", "risk", "blocker"} and item.get("latest_feedback_action") not in {"ignore", "stale"}
        ][:5]
        unknowns = []
        if any(item.get("source_type") == "meeting_recorder" and (item.get("metadata") or {}).get("attribution_scope") == "meeting" for item in evidence_items):
            unknowns.append("Some meeting facts have unknown speaker attribution and are not treated as owner-authored decisions.")
        return {
            "status": "ok",
            "answer": "\n".join(lines),
            "direct_answer": direct_answer,
            "supporting_evidence": evidence,
            "evidence": evidence,
            "confidence": "medium" if len(evidence_items) >= 2 or materialized else "low",
            "unknowns": unknowns,
            "follow_up_candidates": follow_ups,
            "readonly": True,
        }

    def record_superagent_audit_log(
        self,
        *,
        owner_email: str,
        user_email: str,
        query: str,
        task_type: str,
        visibility_scope: str,
        context: dict[str, Any],
        answer: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        items = context.get("items") if isinstance(context.get("items"), list) else []
        used_item_ids = [str(item.get("item_id") or "") for item in items if item.get("item_id")]
        used_private = any(item.get("visibility") == VISIBILITY_PRIVATE for item in items)
        timestamp = _now_iso()
        audit_id = uuid.uuid4().hex
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO work_memory_superagent_audit (
                    audit_id, owner_email, user_email, query, task_type, visibility_scope,
                    used_item_ids_json, used_private_evidence, answer_confidence, metadata_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    audit_id,
                    _normalize_email(owner_email),
                    _normalize_email(user_email),
                    str(query or "").strip(),
                    str(task_type or "general").strip() or "general",
                    str(visibility_scope or "owner").strip() or "owner",
                    _stable_json(used_item_ids),
                    1 if used_private else 0,
                    str(answer.get("confidence") or ""),
                    _stable_json(metadata or {}),
                    timestamp,
                ),
            )
            connection.commit()
        return {
            "audit_id": audit_id,
            "owner_email": _normalize_email(owner_email),
            "user_email": _normalize_email(user_email),
            "query": str(query or "").strip(),
            "task_type": str(task_type or "general").strip() or "general",
            "visibility_scope": str(visibility_scope or "owner").strip() or "owner",
            "used_item_ids": used_item_ids,
            "used_private_evidence": used_private,
            "answer_confidence": str(answer.get("confidence") or ""),
            "created_at": timestamp,
        }

    def superagent_audit_log(self, *, owner_email: str, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM work_memory_superagent_audit
                WHERE owner_email = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (_normalize_email(owner_email), max(1, min(int(limit or 50), 200))),
            ).fetchall()
        return [
            {
                "audit_id": row["audit_id"],
                "owner_email": row["owner_email"],
                "user_email": row["user_email"],
                "query": row["query"],
                "task_type": row["task_type"],
                "visibility_scope": row["visibility_scope"],
                "used_item_ids": _load_json(row["used_item_ids_json"], []),
                "used_private_evidence": bool(row["used_private_evidence"]),
                "answer_confidence": row["answer_confidence"],
                "metadata": _load_json(row["metadata_json"], {}),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def explain_superagent_answer(self, *, owner_email: str, query: str, task_type: str = "general", visibility_scope: str = "owner") -> dict[str, Any]:
        context = self.query_superagent_context(
            query=query,
            owner_email=owner_email,
            visibility_scope=visibility_scope,
            task_type=task_type,
            limit=12,
        )
        items = context.get("items") if isinstance(context.get("items"), list) else []
        explanations = []
        for item in items[:8]:
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            explanations.append(
                {
                    "item_id": item.get("item_id"),
                    "source_type": item.get("source_type"),
                    "item_type": item.get("item_type"),
                    "visibility": item.get("visibility"),
                    "summary": self._evidence_summary(item, include_private_summary=False)["summary"],
                    "source_precedence": SOURCE_PRECEDENCE.get(str(item.get("source_type") or ""), 0),
                    "attribution_scope": metadata.get("attribution_scope") or "",
                    "attribution_rule_id": metadata.get("attribution_rule_id") or "",
                    "latest_feedback_action": item.get("latest_feedback_action") or "",
                }
            )
        return {
            "status": "ok",
            "query": str(query or "").strip(),
            "task_type": str(task_type or "general").strip() or "general",
            "entity_resolution": context.get("entity_resolution") or {},
            "source_precedence": SOURCE_PRECEDENCE,
            "evidence": explanations,
            "guardrails": {
                "readonly": True,
                "meeting_level_facts_are_not_owner_decisions": True,
                "team_visible_outputs_hide_private_raw_text": True,
            },
        }

    def upsert_superagent_eval_cases(self, *, owner_email: str, cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
        timestamp = _now_iso()
        stored = []
        with self._connect() as connection:
            for case in cases:
                if not isinstance(case, dict):
                    continue
                question = str(case.get("question") or case.get("query") or "").strip()
                if not question:
                    continue
                task_type = str(case.get("task_type") or "general").strip() or "general"
                metadata = dict(case.get("metadata") or {})
                expected_answer_points = _normalize_string_list(case.get("expected_answer_points") or metadata.get("expected_answer_points") or [])
                expected_sources = _normalize_string_list(case.get("expected_sources") or metadata.get("expected_sources") or [])
                expected_links = _normalize_string_list(case.get("expected_links") or metadata.get("expected_links") or [])
                domain = str(case.get("domain") or metadata.get("domain") or "").strip()
                suite_id = str(case.get("suite_id") or metadata.get("suite_id") or "").strip()
                if expected_answer_points:
                    metadata["expected_answer_points"] = expected_answer_points
                if expected_sources:
                    metadata["expected_sources"] = expected_sources
                if expected_links:
                    metadata["expected_links"] = expected_links
                if domain:
                    metadata["domain"] = domain
                if suite_id:
                    metadata["suite_id"] = suite_id
                case_id = _memory_id(_normalize_email(owner_email), task_type, question)
                connection.execute(
                    """
                    INSERT INTO work_memory_superagent_eval_cases (
                        case_id, owner_email, question, task_type, expected_source_type, expected_text,
                        visibility_scope, metadata_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(owner_email, question, task_type) DO UPDATE SET
                        expected_source_type = excluded.expected_source_type,
                        expected_text = excluded.expected_text,
                        visibility_scope = excluded.visibility_scope,
                        metadata_json = excluded.metadata_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        case_id,
                        _normalize_email(owner_email),
                        question,
                        task_type,
                        str(case.get("expected_source_type") or "").strip() or (expected_sources[0] if expected_sources else ""),
                        str(case.get("expected_text") or "").strip() or "\n".join(expected_answer_points),
                        str(case.get("visibility_scope") or "owner").strip() or "owner",
                        _stable_json(metadata),
                        timestamp,
                        timestamp,
                    ),
                )
                stored.append({"case_id": case_id, "question": question, "task_type": task_type, "suite_id": suite_id})
            connection.commit()
        return stored

    def run_superagent_eval_cases(
        self,
        *,
        owner_email: str,
        cases: list[dict[str, Any]] | None = None,
        limit: int = 30,
        suite_id: str = "",
    ) -> dict[str, Any]:
        if cases:
            self.upsert_superagent_eval_cases(owner_email=owner_email, cases=cases)
        normalized_suite_id = str(suite_id or "").strip()
        row_limit = 200 if normalized_suite_id else max(1, min(int(limit or 30), 100))
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM work_memory_superagent_eval_cases
                WHERE owner_email = ?
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (_normalize_email(owner_email), row_limit),
            ).fetchall()
        results = []
        for row in rows:
            metadata = _load_json(row["metadata_json"], {})
            if normalized_suite_id and str(metadata.get("suite_id") or "") != normalized_suite_id:
                continue
            if len(results) >= max(1, min(int(limit or 30), 100)):
                break
            context = self.query_superagent_context(
                query=row["question"],
                owner_email=owner_email,
                visibility_scope=row["visibility_scope"] or "owner",
                task_type=row["task_type"] or "general",
                limit=12,
            )
            answer = self.generate_llm_superagent_answer(task_type=row["task_type"], query=row["question"], context=context)
            items = context.get("items") if isinstance(context.get("items"), list) else []
            evidence_sources = {str(item.get("source_type") or "") for item in items}
            expected_source = str(row["expected_source_type"] or "").strip()
            expected_sources = _normalize_string_list(metadata.get("expected_sources") or ([expected_source] if expected_source else []))
            normalized_expected_sources = self._normalize_expected_sources(expected_sources)
            expected_text = str(row["expected_text"] or "").strip().casefold()
            expected_answer_points = _normalize_string_list(metadata.get("expected_answer_points") or [])
            if not expected_answer_points and expected_text:
                expected_answer_points = [expected_text]
            answer_text = str(answer.get("answer") or "").casefold()
            searchable_text = " ".join(
                [
                    answer_text,
                    str(answer.get("direct_answer") or "").casefold(),
                    *[str(item.get("summary") or "").casefold() for item in items],
                    *[str(item.get("content") or "").casefold() for item in items],
                ]
            )
            missing_evidence = not answer.get("evidence")
            wrong_source = bool(normalized_expected_sources and evidence_sources.isdisjoint(normalized_expected_sources))
            missing_answer_points = [
                point
                for point in expected_answer_points
                if not self._answer_point_matches(point, searchable_text)
            ]
            wrong_text = bool(missing_answer_points)
            wrong_attribution = any(
                item.get("source_type") == "meeting_recorder"
                and (item.get("metadata") or {}).get("attribution_scope") == "meeting"
                and "my decision" in str(row["question"] or "").casefold()
                for item in items
            )
            privacy_risk = any(item.get("visibility") == VISIBILITY_PRIVATE for item in items) and row["visibility_scope"] == "team"
            passed = not any([missing_evidence, wrong_source, wrong_text, wrong_attribution, privacy_risk])
            results.append(
                {
                    "case_id": row["case_id"],
                    "question": row["question"],
                    "task_type": row["task_type"],
                    "passed": passed,
                    "missing_evidence": missing_evidence,
                    "wrong_source": wrong_source,
                    "wrong_text": wrong_text,
                    "missing_answer_points": missing_answer_points,
                    "wrong_attribution": wrong_attribution,
                    "privacy_risk": privacy_risk,
                    "evidence_count": len(answer.get("evidence") or []),
                    "confidence": answer.get("confidence") or "",
                    "expected_sources": expected_sources,
                    "expected_links": _normalize_string_list(metadata.get("expected_links") or []),
                    "domain": str(metadata.get("domain") or ""),
                    "suite_id": str(metadata.get("suite_id") or ""),
                }
            )
        passed_count = sum(1 for item in results if item["passed"])
        return {
            "status": "ok",
            "case_count": len(results),
            "passed_count": passed_count,
            "failed_count": len(results) - passed_count,
            "suite_id": normalized_suite_id,
            "results": results,
        }

    def health(self) -> dict[str, Any]:
        with self._connect() as connection:
            total = connection.execute("SELECT COUNT(*) FROM work_memory_items").fetchone()[0]
            feedback_total = connection.execute("SELECT COUNT(*) FROM work_memory_feedback").fetchone()[0]
            materialized_total = connection.execute("SELECT COUNT(*) FROM work_memory_materialized").fetchone()[0]
            by_source = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT source_type, COUNT(*) AS count, MAX(updated_at) AS latest_updated_at
                    FROM work_memory_items
                    GROUP BY source_type
                    ORDER BY count DESC
                    """
                ).fetchall()
            ]
            by_visibility = [
                dict(row)
                for row in connection.execute(
                    "SELECT visibility, COUNT(*) AS count FROM work_memory_items GROUP BY visibility ORDER BY visibility"
                ).fetchall()
            ]
            ingestion_runs = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT source_type, owner_email, cursor, status, scanned_count, matched_count, recorded_count,
                        duplicate_count, failed_count, error, completed_at
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY source_type, owner_email ORDER BY completed_at DESC) AS rn
                        FROM work_memory_ingestion_runs
                    )
                    WHERE rn = 1
                    ORDER BY completed_at DESC
                    """
                ).fetchall()
            ]
        return {
            "status": "ok",
            "item_count": int(total or 0),
            "feedback_count": int(feedback_total or 0),
            "materialized_count": int(materialized_total or 0),
            "by_source": by_source,
            "by_visibility": by_visibility,
            "ingestion_runs": ingestion_runs,
            "source_freshness": ingestion_runs,
        }

    def superagent_health(self, *, owner_email: str) -> dict[str, Any]:
        health = self.health()
        materialized = self._query_materialized(owner_email=owner_email, query="", visibility_scope="owner", limit=20)
        return {
            "status": "ok",
            "readonly": True,
            "owner_email": _normalize_email(owner_email),
            "item_count": health["item_count"],
            "feedback_count": health["feedback_count"],
            "materialized_count": health["materialized_count"],
            "ingestion_runs": health["ingestion_runs"],
            "materialized": materialized,
            "guardrails": {
                "external_writes_enabled": False,
                "requires_evidence": True,
                "meeting_facts_are_owner_profile_eligible": False,
            },
        }

    def _public_item(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        feedback_rows = connection.execute(
            "SELECT * FROM work_memory_feedback WHERE item_id = ? ORDER BY created_at DESC, rowid DESC",
            (row["item_id"],),
        ).fetchall()
        feedback = [dict(item) for item in feedback_rows]
        correction = next((item for item in feedback if item.get("action") == "correct" and item.get("correction_text")), None)
        entities = [
            dict(item)
            for item in connection.execute(
                """
                SELECT e.entity_type, e.entity_key, e.label, ie.relation
                FROM work_memory_item_entities ie
                JOIN work_memory_entities e ON e.entity_id = ie.entity_id
                WHERE ie.item_id = ?
                ORDER BY e.entity_type, e.label
                """,
                (row["item_id"],),
            ).fetchall()
        ]
        summary = str(row["summary"] or "")
        content = str(row["content"] or "")
        if correction:
            summary = str(correction.get("correction_text") or summary)
            content = summary
        return {
            "item_id": row["item_id"],
            "source_type": row["source_type"],
            "source_id": row["source_id"],
            "item_type": row["item_type"],
            "owner_email": row["owner_email"],
            "visibility": row["visibility"],
            "observed_at": row["observed_at"],
            "summary": summary,
            "content": content,
            "evidence": _load_json(row["evidence_json"], {}),
            "metadata": _load_json(row["metadata_json"], {}),
            "ingestion_status": row["ingestion_status"],
            "weight": row["weight"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "entities": entities,
            "latest_feedback_action": feedback[0]["action"] if feedback else "",
            "feedback": feedback,
        }

    def _public_materialized(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "materialized_id": row["materialized_id"],
            "owner_email": row["owner_email"],
            "graph_type": row["graph_type"],
            "graph_key": row["graph_key"],
            "visibility": row["visibility"],
            "summary": row["summary"],
            "content": row["content"],
            "evidence": _load_json(row["evidence_json"], {}),
            "metadata": _load_json(row["metadata_json"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _upsert_materialized(
        self,
        *,
        owner_email: str,
        graph_type: str,
        graph_key: str,
        visibility: str,
        summary: str,
        content: str,
        evidence: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        timestamp = _now_iso()
        normalized_owner = _normalize_email(owner_email)
        normalized_graph_type = str(graph_type or "").strip() or "unknown"
        normalized_graph_key = str(graph_key or "").strip() or "default"
        materialized_id = _memory_id(normalized_owner, normalized_graph_type, normalized_graph_key)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO work_memory_materialized (
                    materialized_id, owner_email, graph_type, graph_key, visibility, summary, content,
                    evidence_json, metadata_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(owner_email, graph_type, graph_key) DO UPDATE SET
                    visibility = excluded.visibility,
                    summary = excluded.summary,
                    content = excluded.content,
                    evidence_json = excluded.evidence_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    materialized_id,
                    normalized_owner,
                    normalized_graph_type,
                    normalized_graph_key,
                    normalize_visibility(visibility),
                    str(summary or "").strip(),
                    str(content or "").strip(),
                    _stable_json(evidence),
                    _stable_json(metadata),
                    timestamp,
                    timestamp,
                ),
            )
            connection.commit()
            row = connection.execute("SELECT * FROM work_memory_materialized WHERE materialized_id = ?", (materialized_id,)).fetchone()
            payload = self._public_materialized(row)
            payload["status"] = "ok"
            payload["materialized"] = True
            return payload

    def _delete_materialized(self, *, owner_email: str, graph_type: str, graph_key: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM work_memory_materialized WHERE owner_email = ? AND graph_type = ? AND graph_key = ?",
                (_normalize_email(owner_email), str(graph_type or ""), str(graph_key or "")),
            )
            connection.commit()

    def _query_materialized(self, *, owner_email: str, query: str, visibility_scope: str, limit: int) -> list[dict[str, Any]]:
        normalized_owner = _normalize_email(owner_email)
        lowered_query = str(query or "").strip().casefold()
        where = ["(visibility = ? OR owner_email = ?)"] if visibility_scope != "team" else ["visibility = ?"]
        params: list[Any] = [VISIBILITY_TEAM, normalized_owner] if visibility_scope != "team" else [VISIBILITY_TEAM]
        sql = "SELECT * FROM work_memory_materialized WHERE " + " AND ".join(where) + " ORDER BY updated_at DESC LIMIT ?"
        params.append(max(1, min(int(limit or 8), 50)))
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        items = [self._public_materialized(row) for row in rows]
        if lowered_query:
            items = [
                item
                for item in items
                if lowered_query in f"{item.get('summary')} {item.get('content')} {json.dumps(item.get('metadata'), ensure_ascii=False)}".casefold()
            ]
        return items

    def _query_by_resolved_terms(
        self,
        *,
        query: str,
        owner_email: str,
        visibility_scope: str,
        task_type: str,
        limit: int,
        resolved: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        resolved_entity = resolved or self.resolve_work_entity(query=query, owner_email=owner_email)
        terms = [
            str(term or "").strip()
            for term in [query, resolved_entity.get("canonical_key"), *(resolved_entity.get("aliases") or [])]
            if str(term or "").strip()
        ]
        for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}|\d{5,}", str(query or "")):
            if token not in terms:
                terms.append(token)
        if not terms:
            if task_type in {"what_changed", "monthly_focus"}:
                return self._sort_by_precedence(
                    [
                        item
                        for item in self.query_work_memory(owner_email=owner_email, visibility_scope=visibility_scope, limit=max(limit, 30))
                        if item.get("latest_feedback_action") not in {"ignore", "stale"}
                    ]
                )[:limit]
            return []
        by_id: dict[str, dict[str, Any]] = {}
        for term in terms[:12]:
            for item in self.query_work_memory(
                owner_email=owner_email,
                visibility_scope=visibility_scope,
                query=term,
                limit=max(limit, 30),
            ):
                if item.get("latest_feedback_action") in {"ignore", "stale"}:
                    continue
                by_id[str(item.get("item_id") or "")] = item
        if not by_id and task_type in {"what_changed", "monthly_focus"}:
            for item in self.query_work_memory(owner_email=owner_email, visibility_scope=visibility_scope, limit=max(limit, 30)):
                by_id[str(item.get("item_id") or "")] = item
        return self._rerank_items_for_query(
            [item for key, item in by_id.items() if key],
            query=query,
            task_type=task_type,
            domain=self._domain_from_query(query),
        )[:limit]

    def _detect_entity_type(self, query: str) -> str:
        if re.search(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", str(query or "")):
            return "person"
        if _extract_bpmis_ids(query) or _extract_jira_keys(query):
            return "project"
        lowered = str(query or "").casefold()
        if any(token in lowered for token in ("project", "bpmis", "jira", "uat", "prd")):
            return "project"
        return "unknown"

    def _canonical_key_from_candidates(self, *, normalized_query: str, entity_type: str, candidates: list[dict[str, Any]]) -> str:
        if entity_type == "project":
            for candidate in candidates:
                if candidate.get("entity_type") == "bpmis_id":
                    return str(candidate.get("entity_key") or "")
            for bpmis_id in _extract_bpmis_ids(normalized_query):
                return bpmis_id
            for candidate in candidates:
                if candidate.get("entity_type") == "jira_key":
                    return str(candidate.get("entity_key") or "")
            for jira_key in _extract_jira_keys(normalized_query):
                return jira_key
            for candidate in candidates:
                if candidate.get("entity_type") == "project":
                    return str(candidate.get("entity_key") or candidate.get("label") or "")
        if entity_type == "person":
            email_match = re.search(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", normalized_query)
            if email_match:
                return email_match.group(0).lower()
            for candidate in candidates:
                if candidate.get("entity_type") == "person":
                    return str(candidate.get("entity_key") or candidate.get("label") or "").lower()
        return normalized_query.strip()

    def _domain_from_query(self, query: str) -> str:
        lowered = str(query or "").casefold()
        if any(token in lowered for token in ("anti-fraud", "fraud", "afasa", "scam", "centum", "singpass", "money lock", "kill switch", "sfv")):
            return "Anti-Fraud"
        if any(token in lowered for token in ("credit risk", "credit", "underwriting", "b score", "a-score", "dbr", "limit", "income extraction", "experian")):
            return "Credit Risk"
        if any(token in lowered for token in ("grc", "rcsa", "outsourcing", "incident management", "issue management", "ops risk")):
            return "GRC"
        return "Other"

    def _query_tokens(self, query: str) -> list[str]:
        stopwords = {
            "the",
            "and",
            "for",
            "with",
            "what",
            "when",
            "where",
            "which",
            "how",
            "why",
            "project",
            "status",
            "市场",
            "什么",
            "如何",
            "哪些",
            "目前",
        }
        tokens = []
        for token in re.findall(r"[A-Za-z][A-Za-z0-9_+-]{2,}|\d{2,}|[\u4e00-\u9fff]{2,}", str(query or "")):
            normalized = token.casefold()
            if normalized not in stopwords and normalized not in tokens:
                tokens.append(normalized)
        return tokens

    def _rerank_items_for_query(self, items: list[dict[str, Any]], *, query: str, task_type: str, domain: str) -> list[dict[str, Any]]:
        tokens = self._query_tokens(query)
        task_type_key = str(task_type or "").strip()
        preferred_types_by_task = {
            "stakeholder_brief": {"stakeholder", "project", "key_project", "curated_report"},
            "follow_up": {"todo", "open_loop", "risk", "blocker", "curated_report"},
            "monthly_focus": {"curated_report", "key_project", "project"},
            "project_status": {"curated_report", "key_project", "project", "decision", "risk", "todo"},
        }
        preferred_types = preferred_types_by_task.get(task_type_key, set())

        def score(item: dict[str, Any]) -> tuple[float, str]:
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            text = f"{item.get('summary') or ''} {item.get('content') or ''} {json.dumps(metadata, ensure_ascii=False)}".casefold()
            token_score = sum(1 for token in tokens if token and token in text)
            source_score = SOURCE_PRECEDENCE.get(str(item.get("source_type") or ""), 0) / 10.0
            type_score = 4.0 if item.get("item_type") in preferred_types else 0.0
            domain_score = 0.0
            if domain != "Other":
                team_key = str(metadata.get("team_key") or "").casefold()
                team_label = str(metadata.get("team_label") or "").casefold()
                source_text = f"{team_key} {team_label} {text}"
                if domain.casefold().replace("-", "") in source_text.replace("-", ""):
                    domain_score = 6.0
                elif domain == "GRC" and ("grc" in source_text or "ops risk" in source_text):
                    domain_score = 6.0
                elif domain == "Credit Risk" and ("credit" in source_text or "crms" in source_text):
                    domain_score = 6.0
            return (
                float(item.get("weight") or 1.0) * 3.0 + source_score + type_score + domain_score + token_score * 2.0,
                str(item.get("observed_at") or item.get("updated_at") or ""),
            )

        return sorted(items, key=score, reverse=True)

    def _normalize_expected_sources(self, sources: list[str]) -> set[str]:
        normalized: set[str] = set()
        for source in sources:
            lowered = source.casefold()
            if "gmail" in lowered or "email" in lowered or "sent report" in lowered:
                normalized.update({"gmail_sent_monthly_report", "gmail", "gmail_attachment", "gmail_drive_link"})
            if "meeting" in lowered:
                normalized.add("meeting_recorder")
            if "team dashboard" in lowered or "bpmis" in lowered or "jira" in lowered:
                normalized.update({"team_dashboard", "bpmis", "jira"})
            if "prd" in lowered or "confluence" in lowered:
                normalized.update({"confluence", "team_dashboard"})
            if "seatalk" in lowered:
                normalized.add("seatalk")
            if "source code" in lowered:
                normalized.add("source_code_qa")
        return normalized

    def _answer_point_matches(self, point: str, searchable_text: str) -> bool:
        normalized_point = str(point or "").strip().casefold()
        if not normalized_point:
            return True
        if normalized_point in searchable_text:
            return True
        tokens = self._query_tokens(normalized_point)
        if not tokens:
            return False
        return all(token in searchable_text for token in tokens[:6])

    def _candidate_project_refs(self, *, owner_email: str, sources: list[str] | None = None) -> list[str]:
        source_set = {str(source or "").strip() for source in sources or [] if str(source or "").strip()}
        refs: list[str] = []
        for item in self.query_work_memory(owner_email=owner_email, visibility_scope="owner", limit=200):
            if source_set and item.get("source_type") not in source_set:
                continue
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            for key in ("bpmis_id", "project_name"):
                value = str(metadata.get(key) or "").strip()
                if value and value not in refs:
                    refs.append(value)
            for jira_key in metadata.get("jira_keys") or []:
                value = str(jira_key or "").strip()
                if value and value not in refs:
                    refs.append(value)
            for entity in item.get("entities") or []:
                if not isinstance(entity, dict):
                    continue
                if entity.get("entity_type") in {"project", "bpmis_id", "jira_key"}:
                    value = str(entity.get("entity_key") or entity.get("label") or "").strip()
                    if value and value not in refs:
                        refs.append(value)
        return refs

    def _review_score(self, item: dict[str, Any]) -> float:
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        score = float(REVIEW_ITEM_TYPE_PRIORITY.get(str(item.get("item_type") or ""), 10))
        score += float(SOURCE_PRECEDENCE.get(str(item.get("source_type") or ""), 0)) / 10.0
        score += float(item.get("weight") or 1.0) * 5.0
        if metadata.get("is_key_project"):
            score += 10.0
        if metadata.get("personal_profile_eligible") in {True, "candidate_after_review"}:
            score += 6.0
        return score

    def _sort_by_precedence(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            items,
            key=lambda item: (
                float(item.get("weight") or 1.0),
                SOURCE_PRECEDENCE.get(str(item.get("source_type") or ""), 0),
                str(item.get("observed_at") or item.get("updated_at") or ""),
            ),
            reverse=True,
        )

    def _stakeholders_from_items(self, items: list[dict[str, Any]]) -> list[dict[str, str]]:
        stakeholders: dict[str, dict[str, str]] = {}
        for item in items:
            for entity in item.get("entities") or []:
                if not isinstance(entity, dict) or entity.get("entity_type") != "person":
                    continue
                key = str(entity.get("entity_key") or "").strip().lower()
                if key:
                    stakeholders[key] = {
                        "email": key,
                        "label": str(entity.get("label") or key),
                        "relation": str(entity.get("relation") or "mentions"),
                    }
        return sorted(stakeholders.values(), key=lambda item: item["email"])

    def _evidence_summary(
        self,
        item: dict[str, Any],
        *,
        include_private_summary: bool,
        include_excerpt: bool = False,
        query: str = "",
    ) -> dict[str, Any]:
        summary = str(item.get("summary") or "")
        if item.get("visibility") == VISIBILITY_PRIVATE and not include_private_summary:
            summary = "Private evidence available to owner."
        payload = {
            "item_id": item.get("item_id"),
            "source_type": item.get("source_type"),
            "source_id": item.get("source_id"),
            "item_type": item.get("item_type"),
            "visibility": item.get("visibility"),
            "observed_at": item.get("observed_at"),
            "summary": summary,
            "latest_feedback_action": item.get("latest_feedback_action") or "",
            "weight": item.get("weight"),
        }
        if include_excerpt:
            excerpt = self._evidence_excerpt(item, query=query, allow_private=include_private_summary)
            if excerpt:
                payload["excerpt"] = excerpt
        return payload

    def _evidence_excerpt(self, item: dict[str, Any], *, query: str, allow_private: bool) -> str:
        if item.get("visibility") == VISIBILITY_PRIVATE and not allow_private:
            return ""
        content = self._sanitize_evidence_text(str(item.get("content") or item.get("summary") or ""))
        if not content:
            return ""
        tokens = self._query_tokens(query)
        if tokens:
            sentences = re.split(r"(?<=[.!?。！？])\s+|\n+", content)
            scored = []
            for sentence in sentences:
                cleaned = sentence.strip()
                if not cleaned:
                    continue
                lowered = cleaned.casefold()
                score = sum(1 for token in tokens if token in lowered)
                if score:
                    scored.append((score, cleaned))
            if scored:
                scored.sort(key=lambda item: item[0], reverse=True)
                return self._truncate_text(" ".join(sentence for _, sentence in scored[:2]), limit=420)
        return self._truncate_text(content, limit=420)

    def _sanitize_evidence_text(self, text: str) -> str:
        value = re.sub(r"\s+", " ", str(text or "")).strip()
        value = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[email]", value)
        value = re.sub(r"https?://\S+", "[link]", value)
        return value

    def _truncate_text(self, text: str, *, limit: int) -> str:
        value = str(text or "").strip()
        if len(value) <= limit:
            return value
        return value[: max(0, limit - 3)].rstrip() + "..."

    def _answer_prefix(self, item: dict[str, Any]) -> str:
        if item.get("source_type") == "gmail_sent_monthly_report":
            return "Final sent report says: "
        if item.get("source_type") == "team_dashboard":
            return "Team Dashboard says: "
        if item.get("source_type") == "meeting_recorder":
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            if metadata.get("attribution_scope") == "meeting":
                return "Meeting-level fact says: "
            if metadata.get("attribution_scope") == "owner_speech_candidate":
                return "Owner speech candidate says: "
        if item.get("source_type") == "source_code_qa":
            return "Technical evidence says: "
        return ""


def team_dashboard_memory_items(team_payload: dict[str, Any], *, owner_email: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    team_key = str(team_payload.get("team_key") or "").strip().upper()
    team_label = str(team_payload.get("label") or team_key).strip()
    member_emails = [email for email in (_normalize_email(item) for item in team_payload.get("member_emails") or []) if email]
    for section_key, section_label in (("under_prd", "Under PRD"), ("pending_live", "Pending Live")):
        for project in team_payload.get(section_key) or []:
            if not isinstance(project, dict):
                continue
            bpmis_id = str(project.get("bpmis_id") or project.get("issue_id") or "").strip()
            project_name = str(project.get("project_name") or "").strip()
            project_key = bpmis_id or project_name.casefold()
            if not project_key:
                continue
            tickets = [ticket for ticket in project.get("jira_tickets") or [] if isinstance(ticket, dict)]
            jira_keys = [str(ticket.get("jira_id") or ticket.get("issue_id") or "").strip() for ticket in tickets if str(ticket.get("jira_id") or ticket.get("issue_id") or "").strip()]
            prd_links = []
            for ticket in tickets:
                for link in ticket.get("prd_links") or []:
                    if isinstance(link, dict) and link.get("url"):
                        prd_links.append(str(link.get("url") or "").strip())
            pm_emails = [email for email in (_normalize_email(item) for item in project.get("matched_pm_emails") or []) if email]
            summary_parts = [
                f"{team_key} {section_label}",
                bpmis_id,
                project_name,
                f"status={project.get('status') or ''}".strip("="),
                f"release={project.get('release_date') or ''}".strip("="),
            ]
            summary = " | ".join(part for part in summary_parts if part)
            metadata = {
                "team_key": team_key,
                "team_label": team_label,
                "section": section_key,
                "bpmis_id": bpmis_id,
                "project_name": project_name,
                "status": project.get("status") or "",
                "market": project.get("market") or "",
                "priority": project.get("priority") or "",
                "release_date": project.get("release_date") or "",
                "is_key_project": bool(project.get("is_key_project")),
                "member_emails": member_emails,
                "pm_emails": pm_emails,
                "jira_keys": jira_keys,
                "prd_links": sorted(set(prd_links)),
                "task_count": len(tickets),
            }
            entities = [
                {"entity_type": "team", "entity_key": team_key, "label": team_label, "relation": "belongs_to"},
                {"entity_type": "project", "entity_key": project_key, "label": project_name or bpmis_id, "relation": "describes"},
            ]
            if bpmis_id:
                entities.append({"entity_type": "bpmis_id", "entity_key": bpmis_id, "label": bpmis_id, "relation": "identifies"})
            for email in pm_emails:
                entities.append({"entity_type": "person", "entity_key": email, "label": email, "relation": "owner"})
            for jira_key in jira_keys:
                entities.append({"entity_type": "jira_key", "entity_key": jira_key, "label": jira_key, "relation": "contains"})
            for prd_link in sorted(set(prd_links))[:10]:
                entities.append({"entity_type": "confluence_page", "entity_key": prd_link, "label": prd_link, "relation": "references"})
            items.append(
                {
                    "source_type": "team_dashboard",
                    "source_id": f"{team_key}:{section_key}:{project_key}",
                    "item_type": "key_project" if project.get("is_key_project") else "project",
                    "owner_email": owner_email,
                    "visibility": VISIBILITY_TEAM,
                    "summary": summary,
                    "content": json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                    "evidence": {"source": "team_dashboard", "team_key": team_key, "section": section_key},
                    "metadata": metadata,
                    "entities": entities,
                    "weight": 1.4 if project.get("is_key_project") else 1.0,
                }
            )
    return items


def meeting_record_memory_items(record: dict[str, Any]) -> list[dict[str, Any]]:
    owner_email = _normalize_email(record.get("owner_email"))
    record_id = str(record.get("record_id") or "").strip()
    title = str(record.get("title") or "Untitled meeting").strip()
    transcript = record.get("transcript") if isinstance(record.get("transcript"), dict) else {}
    minutes = record.get("minutes") if isinstance(record.get("minutes"), dict) else {}
    transcript_text = str(transcript.get("text") or "").strip()
    minutes_text = str(minutes.get("markdown") or "").strip()
    observed_at = str(record.get("recording_started_at") or record.get("started_at") or record.get("created_at") or "").strip()
    attendees = record.get("attendees") if isinstance(record.get("attendees"), list) else []
    entities = [{"entity_type": "meeting", "entity_key": record_id, "label": title, "relation": "source"}]
    for attendee in attendees:
        if not isinstance(attendee, dict):
            continue
        email = _normalize_email(attendee.get("email"))
        if email:
            entities.append({"entity_type": "person", "entity_key": email, "label": attendee.get("name") or email, "relation": "attendee"})
    for jira_key in _extract_jira_keys(f"{title}\n{transcript_text}\n{minutes_text}"):
        entities.append({"entity_type": "jira_key", "entity_key": jira_key, "label": jira_key, "relation": "mentions"})
    for bpmis_id in _extract_bpmis_ids(f"{title}\n{transcript_text}\n{minutes_text}"):
        entities.append({"entity_type": "bpmis_id", "entity_key": bpmis_id, "label": bpmis_id, "relation": "mentions"})
    base_metadata = {
        "record_id": record_id,
        "title": title,
        "platform": record.get("platform") or "",
        "meeting_link": record.get("meeting_link") or "",
        "transcript_status": transcript.get("status") or "",
        "minutes_status": minutes.get("status") or "",
        **MEETING_ATTRIBUTION_METADATA,
    }
    items = []
    if transcript_text or minutes_text:
        items.append(
            {
                "source_type": "meeting_recorder",
                "source_id": record_id,
                "item_type": "meeting",
                "owner_email": owner_email,
                "visibility": VISIBILITY_PRIVATE,
                "observed_at": observed_at,
                "summary": f"Meeting: {title}",
                "content": "\n\n".join(part for part in [minutes_text, transcript_text] if part),
                "evidence": {
                    "record_id": record_id,
                    "transcript_asset_url": transcript.get("asset_url") or "",
                    "minutes_asset_url": minutes.get("asset_url") or "",
                },
                "metadata": base_metadata,
                "entities": entities,
            }
        )
    for extracted in _meeting_structured_items(minutes_text):
        items.append(
            {
                "source_type": "meeting_recorder",
                "source_id": f"{record_id}:{extracted['item_type']}:{extracted['index']}",
                "item_type": extracted["item_type"],
                "owner_email": owner_email,
                "visibility": VISIBILITY_PRIVATE,
                "observed_at": observed_at,
                "summary": extracted["summary"],
                "content": extracted["summary"],
                "evidence": {"record_id": record_id, "minutes_asset_url": minutes.get("asset_url") or ""},
                "metadata": {**base_metadata, "extracted_from": "minutes", "extracted_fact_scope": "meeting_level"},
                "entities": entities,
            }
        )
    owner_speech_chunks = transcript.get("owner_speech_candidates") if isinstance(transcript.get("owner_speech_candidates"), list) else []
    owner_speech_lines = [
        str(chunk.get("text") or "").strip()
        for chunk in owner_speech_chunks
        if isinstance(chunk, dict) and str(chunk.get("text") or "").strip()
    ]
    if owner_speech_lines:
        items.append(
            {
                "source_type": "meeting_recorder",
                "source_id": f"{record_id}:owner_speech_candidate",
                "item_type": "owner_speech_candidate",
                "owner_email": owner_email,
                "visibility": VISIBILITY_PRIVATE,
                "observed_at": observed_at,
                "summary": f"Owner speech candidates: {title}",
                "content": "\n".join(owner_speech_lines),
                "evidence": {
                    "record_id": record_id,
                    "owner_speech_asset_url": transcript.get("owner_speech_asset_url") or "",
                    "source": "local_microphone_track",
                },
                "metadata": {
                    **base_metadata,
                    **OWNER_SPEECH_CANDIDATE_METADATA,
                    "candidate_count": len(owner_speech_lines),
                },
                "entities": entities,
                "weight": 0.8,
            }
        )
    return items


def _meeting_structured_items(minutes_text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    current_type = ""
    counters: dict[str, int] = {}
    for raw_line in str(minutes_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lowered = line.strip("# ").casefold()
        if "decision" in lowered:
            current_type = "decision"
            continue
        if "action" in lowered or "todo" in lowered or "follow up" in lowered or "follow-up" in lowered or "next step" in lowered:
            current_type = "todo"
            continue
        if "risk" in lowered or "blocker" in lowered:
            current_type = "risk"
            continue
        if not line.startswith(("-", "*")) or not current_type:
            continue
        summary = line.lstrip("-* ").strip()
        if not summary:
            continue
        counters[current_type] = counters.get(current_type, 0) + 1
        item_type = "blocker" if current_type == "risk" and "block" in summary.casefold() else current_type
        items.append({"item_type": item_type, "index": counters[current_type], "summary": summary})
    return items


def source_code_qa_memory_item(
    *,
    owner_email: str,
    pm_team: str,
    country: str,
    question: str,
    result: dict[str, Any],
    session_id: str = "",
    job_id: str = "",
) -> dict[str, Any]:
    trace_id = str(result.get("trace_id") or job_id or session_id or hashlib.sha256(str(question or "").encode("utf-8")).hexdigest()[:16])
    answer = str(result.get("llm_answer") or result.get("summary") or "").strip()
    matches = result.get("matches") if isinstance(result.get("matches"), list) else []
    citations = result.get("citations") if isinstance(result.get("citations"), list) else []
    metadata = {
        "pm_team": str(pm_team or "").strip(),
        "country": str(country or "").strip(),
        "question": str(question or "").strip(),
        "trace_id": trace_id,
        "citation_count": len(citations),
        "match_count": len(matches),
    }
    return {
        "source_type": "source_code_qa",
        "source_id": trace_id,
        "item_type": "technical_evidence",
        "owner_email": owner_email,
        "visibility": VISIBILITY_TEAM,
        "summary": f"Source Code Q&A: {str(question or '').strip()[:160]}",
        "content": answer,
        "evidence": {"trace_id": trace_id, "citations": citations, "matches": matches[:20]},
        "metadata": metadata,
        "entities": [
            {"entity_type": "team", "entity_key": str(pm_team or "").strip().upper(), "label": str(pm_team or "").strip().upper(), "relation": "scope"}
        ]
        if pm_team
        else [],
        "weight": 0.6,
    }


def sent_monthly_report_memory_item(
    *,
    owner_email: str,
    subject: str,
    body: str,
    recipient: str = "",
    message_id: str = "",
    observed_at: str = "",
) -> dict[str, Any]:
    source_id = message_id or hashlib.sha256(f"{owner_email}\n{recipient}\n{subject}\n{body}".encode("utf-8")).hexdigest()[:24]
    text = str(body or "").strip()
    entities = []
    for jira_key in _extract_jira_keys(text):
        entities.append({"entity_type": "jira_key", "entity_key": jira_key, "label": jira_key, "relation": "mentions"})
    for bpmis_id in _extract_bpmis_ids(text):
        entities.append({"entity_type": "bpmis_id", "entity_key": bpmis_id, "label": bpmis_id, "relation": "mentions"})
    return {
        "source_type": "gmail_sent_monthly_report",
        "source_id": source_id,
        "item_type": "curated_report",
        "owner_email": owner_email,
        "visibility": VISIBILITY_PRIVATE,
        "observed_at": observed_at,
        "summary": str(subject or "Monthly Report").strip(),
        "content": text,
        "evidence": {"message_id": message_id, "recipient": recipient, "source": "gmail_sent_mail"},
        "metadata": {"subject": subject, "recipient": recipient, "message_id": message_id},
        "entities": entities,
        "weight": 2.0,
    }


def sent_monthly_report_memory_item_from_gmail_record(*, owner_email: str, record: Any) -> dict[str, Any]:
    headers = getattr(record, "headers", {}) if record is not None else {}
    subject = str((headers or {}).get("subject") or "Monthly Report").strip()
    recipient = str((headers or {}).get("to") or "").strip()
    message_id = str(getattr(record, "message_id", "") or "").strip()
    observed = getattr(record, "internal_date", None)
    observed_at = observed.isoformat() if hasattr(observed, "isoformat") else ""
    return sent_monthly_report_memory_item(
        owner_email=owner_email,
        subject=subject,
        body=str(getattr(record, "body_text", "") or ""),
        recipient=recipient,
        message_id=message_id,
        observed_at=observed_at,
    )


def gmail_message_memory_item(
    *,
    owner_email: str,
    record: Any,
    matched_vips: list[dict[str, Any]] | None = None,
    vip_email_roles: dict[str, list[str]] | None = None,
    report_matches: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = getattr(record, "headers", {}) if record is not None else {}
    subject = str((headers or {}).get("subject") or "[no subject]").strip()
    message_id = str(getattr(record, "message_id", "") or "").strip()
    thread_id = str(getattr(record, "thread_id", "") or "").strip()
    observed = getattr(record, "internal_date", None)
    observed_at = observed.isoformat() if hasattr(observed, "isoformat") else ""
    body = str(getattr(record, "body_text", "") or "").strip()
    text = f"{subject}\n{body}".strip()
    entities = _entities_from_text(text)
    item_type = _gmail_fact_item_type(text)
    matched_vips = matched_vips or []
    metadata = {
        "subject": subject,
        "from": str((headers or {}).get("from") or "").strip(),
        "to": str((headers or {}).get("to") or "").strip(),
        "cc": str((headers or {}).get("cc") or "").strip(),
        "message_id": message_id,
        "thread_id": thread_id,
        "labels": sorted(str(item) for item in (getattr(record, "label_ids", None) or [])),
        "matched_vips": matched_vips,
        "vip_email_roles": vip_email_roles or {},
        "report_intelligence_matches": report_matches or {},
        "drive_links": list(getattr(record, "drive_links", []) or [])[:20],
        "attachment_count": len(getattr(record, "attachments", []) or []),
    }
    return {
        "source_type": "gmail",
        "source_id": message_id or _memory_id(owner_email, thread_id, subject, body[:200]),
        "item_type": item_type,
        "owner_email": owner_email,
        "visibility": VISIBILITY_PRIVATE,
        "observed_at": observed_at,
        "summary": f"Gmail: {subject}",
        "content": body,
        "evidence": {"message_id": message_id, "thread_id": thread_id, "source": "gmail"},
        "metadata": metadata,
        "entities": entities,
        "weight": 1.35 if matched_vips else 0.75,
    }


def gmail_attachment_memory_item(
    *,
    owner_email: str,
    record: Any,
    attachment: Any,
    text: str,
    sha256: str,
    matched_vips: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    headers = getattr(record, "headers", {}) if record is not None else {}
    subject = str((headers or {}).get("subject") or "[no subject]").strip()
    message_id = str(getattr(record, "message_id", "") or "").strip()
    thread_id = str(getattr(record, "thread_id", "") or "").strip()
    filename = str(getattr(attachment, "filename", "") or "").strip()
    attachment_id = str(getattr(attachment, "attachment_id", "") or "").strip()
    observed = getattr(record, "internal_date", None)
    observed_at = observed.isoformat() if hasattr(observed, "isoformat") else ""
    content = str(text or "").strip()
    return {
        "source_type": "gmail_attachment",
        "source_id": f"{message_id}:{attachment_id}:{sha256}",
        "item_type": "attachment_evidence",
        "owner_email": owner_email,
        "visibility": VISIBILITY_PRIVATE,
        "observed_at": observed_at,
        "summary": f"Gmail PDF attachment: {filename or subject}",
        "content": content,
        "evidence": {
            "message_id": message_id,
            "thread_id": thread_id,
            "attachment_id": attachment_id,
            "filename": filename,
            "sha256": sha256,
            "source": "gmail_attachment",
        },
        "metadata": {
            "subject": subject,
            "message_id": message_id,
            "thread_id": thread_id,
            "filename": filename,
            "mime_type": str(getattr(attachment, "mime_type", "") or ""),
            "size": int(getattr(attachment, "size", 0) or 0),
            "sha256": sha256,
            "matched_vips": matched_vips or [],
        },
        "entities": _entities_from_text(f"{subject}\n{filename}\n{content}"),
        "weight": 1.45,
    }


def gmail_drive_link_memory_item(
    *,
    owner_email: str,
    record: Any,
    url: str,
    title: str = "",
    text: str = "",
    access_status: str = "ok",
    matched_vips: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    headers = getattr(record, "headers", {}) if record is not None else {}
    subject = str((headers or {}).get("subject") or "[no subject]").strip()
    message_id = str(getattr(record, "message_id", "") or "").strip()
    thread_id = str(getattr(record, "thread_id", "") or "").strip()
    observed = getattr(record, "internal_date", None)
    observed_at = observed.isoformat() if hasattr(observed, "isoformat") else ""
    clean_url = str(url or "").strip()
    content = str(text or "").strip()
    return {
        "source_type": "gmail_drive_link",
        "source_id": f"{message_id}:{_memory_id(clean_url)}",
        "item_type": "drive_evidence",
        "owner_email": owner_email,
        "visibility": VISIBILITY_PRIVATE,
        "observed_at": observed_at,
        "summary": f"Gmail Drive link: {title or clean_url}",
        "content": content or f"Drive link could not be read: {access_status}",
        "evidence": {"message_id": message_id, "thread_id": thread_id, "url": clean_url, "source": "gmail_drive_link"},
        "metadata": {
            "subject": subject,
            "message_id": message_id,
            "thread_id": thread_id,
            "url": clean_url,
            "title": title,
            "access_status": access_status,
            "matched_vips": matched_vips or [],
        },
        "entities": _entities_from_text(f"{subject}\n{title}\n{content}\n{clean_url}"),
        "ingestion_status": "ok" if access_status == "ok" else "partial",
        "weight": 1.45 if access_status == "ok" else 0.55,
    }


def _entities_from_text(text: str) -> list[dict[str, Any]]:
    entities = []
    for jira_key in _extract_jira_keys(text):
        entities.append({"entity_type": "jira_key", "entity_key": jira_key, "label": jira_key, "relation": "mentions"})
    for bpmis_id in _extract_bpmis_ids(text):
        entities.append({"entity_type": "bpmis_id", "entity_key": bpmis_id, "label": bpmis_id, "relation": "mentions"})
    return entities


def _gmail_fact_item_type(text: str) -> str:
    lowered = str(text or "").casefold()
    if any(token in lowered for token in ("approval", "approved", "approve", "sign-off", "signoff", "批准", "审批")):
        return "decision"
    if any(token in lowered for token in ("risk", "blocked", "blocker", "issue", "delay", "延期", "风险", "阻塞")):
        return "risk"
    if any(token in lowered for token in ("todo", "follow up", "follow-up", "action item", "next step", "owner:", "due", "待办")):
        return "todo"
    if any(token in lowered for token in ("deadline", "target", "go-live", "launch", "上线")):
        return "project"
    return "email_evidence"
