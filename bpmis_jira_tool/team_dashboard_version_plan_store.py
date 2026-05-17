"""Version Plan storage backends.

The Team Dashboard config remains the source for non-Version-Plan settings.
Version Plan can be promoted to Firestore independently so Cloud Run and the
Mac portal share one document without moving the rest of the portal.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from typing import Any
from zoneinfo import ZoneInfo

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.team_dashboard_version_plan import normalize_version_plan_state


SGT = ZoneInfo("Asia/Singapore")
VERSION_PLAN_SCHEMA_VERSION = 1


class VersionPlanConflictError(ValueError):
    """Raised when a client saves against a stale Version Plan revision."""


def _sgt_now() -> str:
    return datetime.now(tz=SGT).strftime("%Y-%m-%d %H:%M:%S SGT")


def version_plan_source_hash(version_plan: dict[str, Any]) -> str:
    normalized = normalize_version_plan_state(version_plan)
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def version_plan_environment(settings: Settings) -> str:
    explicit = str(settings.version_plan_firestore_environment or "").strip().lower()
    if explicit:
        return explicit
    stage = str(settings.team_portal_stage or "").strip().lower()
    return "uat" if stage == "uat" else "live"


def firestore_document_id(settings: Settings) -> str:
    explicit = str(settings.version_plan_firestore_document or "").strip()
    if explicit:
        return explicit
    return f"version_plan_{version_plan_environment(settings)}"


def should_use_firestore_version_plan(settings: Settings) -> bool:
    backend = str(settings.version_plan_store_backend or "auto").strip().lower()
    if backend in {"firestore", "cloud_firestore"}:
        return True
    if backend in {"sqlite", "team_dashboard_config", "local", "disabled"}:
        return False
    stage = str(settings.team_portal_stage or "").strip().lower()
    return bool(settings.version_plan_firestore_project and stage in {"uat", "live"})


@dataclass(frozen=True)
class VersionPlanSnapshot:
    config: dict[str, Any]
    metadata: dict[str, Any]

    @property
    def revision(self) -> str:
        return str(self.metadata.get("revision") or "")


class TeamDashboardVersionPlanStore:
    def load_snapshot(self) -> VersionPlanSnapshot:
        raise NotImplementedError

    def save_config(self, config: dict[str, Any], *, expected_revision: str | None = None) -> VersionPlanSnapshot:
        raise NotImplementedError


class LocalTeamDashboardVersionPlanStore(TeamDashboardVersionPlanStore):
    def __init__(self, config_store: Any) -> None:
        self.config_store = config_store

    def _snapshot(self, config: dict[str, Any]) -> VersionPlanSnapshot:
        plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
        revision = version_plan_source_hash(plan)
        metadata = {
            "backend": "team_dashboard_config",
            "environment": "local",
            "schema_version": VERSION_PLAN_SCHEMA_VERSION,
            "revision": revision,
            "source_hash": revision,
            "updated_at_sgt": "",
        }
        normalized = dict(config)
        normalized["version_plan"] = plan
        return VersionPlanSnapshot(config=normalized, metadata=metadata)

    def load_snapshot(self) -> VersionPlanSnapshot:
        return self._snapshot(self.config_store.load())

    def save_config(self, config: dict[str, Any], *, expected_revision: str | None = None) -> VersionPlanSnapshot:
        current = self.load_snapshot()
        if expected_revision and expected_revision != current.revision:
            raise VersionPlanConflictError("Version Plan was updated by another session. Refresh and try again.")
        return self._snapshot(self.config_store.save(config))


class FirestoreVersionPlanStore(TeamDashboardVersionPlanStore):
    def __init__(self, *, settings: Settings, config_store: Any, firestore_client: Any | None = None) -> None:
        self.settings = settings
        self.config_store = config_store
        self.environment = version_plan_environment(settings)
        self.document_id = firestore_document_id(settings)
        self._client = firestore_client

    @property
    def client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from google.cloud import firestore  # type: ignore
        except ImportError as error:  # pragma: no cover - depends on runtime deps
            raise RuntimeError("google-cloud-firestore is required for Version Plan Firestore storage.") from error
        self._client = firestore.Client(project=self.settings.version_plan_firestore_project)
        return self._client

    @property
    def document_ref(self) -> Any:
        return self.client.collection("portal").document(self.document_id)

    def _base_config(self) -> dict[str, Any]:
        try:
            loaded = self.config_store.load()
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {"version_plan": normalize_version_plan_state({})}

    def _default_firestore_doc(self) -> dict[str, Any]:
        plan = normalize_version_plan_state(self._base_config().get("version_plan"))
        source_hash = version_plan_source_hash(plan)
        return {
            "environment": self.environment,
            "updated_at_sgt": "",
            "source_hash": source_hash,
            "schema_version": VERSION_PLAN_SCHEMA_VERSION,
            "revision": source_hash,
            "version_plan": plan,
            "migration_source_revision": "",
            "migrated_at_sgt": "",
        }

    def _config_from_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        config = self._base_config()
        config["version_plan"] = normalize_version_plan_state(doc.get("version_plan") if isinstance(doc, dict) else {})
        return config

    def _snapshot_from_doc(self, doc: dict[str, Any]) -> VersionPlanSnapshot:
        normalized_doc = dict(self._default_firestore_doc())
        if isinstance(doc, dict):
            normalized_doc.update(doc)
        normalized_doc["environment"] = self.environment
        normalized_doc["schema_version"] = VERSION_PLAN_SCHEMA_VERSION
        normalized_doc["version_plan"] = normalize_version_plan_state(normalized_doc.get("version_plan"))
        source_hash = version_plan_source_hash(normalized_doc["version_plan"])
        normalized_doc["source_hash"] = source_hash
        normalized_doc["revision"] = str(normalized_doc.get("revision") or source_hash)
        metadata = {key: value for key, value in normalized_doc.items() if key != "version_plan"}
        metadata["backend"] = "firestore"
        metadata["document_path"] = f"portal/{self.document_id}"
        return VersionPlanSnapshot(config=self._config_from_doc(normalized_doc), metadata=metadata)

    def load_snapshot(self) -> VersionPlanSnapshot:
        snapshot = self.document_ref.get()
        doc = snapshot.to_dict() if getattr(snapshot, "exists", False) else self._default_firestore_doc()
        return self._snapshot_from_doc(doc or {})

    def _doc_for_config(self, config: dict[str, Any], *, existing: dict[str, Any] | None = None) -> dict[str, Any]:
        existing = existing if isinstance(existing, dict) else {}
        plan = normalize_version_plan_state(config.get("version_plan") if isinstance(config, dict) else {})
        source_hash = version_plan_source_hash(plan)
        now = _sgt_now()
        revision_seed = f"{source_hash}:{now}"
        return {
            "environment": self.environment,
            "updated_at_sgt": now,
            "source_hash": source_hash,
            "schema_version": VERSION_PLAN_SCHEMA_VERSION,
            "revision": hashlib.sha256(revision_seed.encode("utf-8")).hexdigest(),
            "version_plan": plan,
            "migration_source_revision": str(existing.get("migration_source_revision") or ""),
            "migrated_at_sgt": str(existing.get("migrated_at_sgt") or ""),
        }

    def save_config(self, config: dict[str, Any], *, expected_revision: str | None = None) -> VersionPlanSnapshot:
        if self._client is not None and not hasattr(self._client, "transaction"):
            snapshot = self.document_ref.get()
            existing = snapshot.to_dict() if getattr(snapshot, "exists", False) else self._default_firestore_doc()
            current_revision = str((existing or {}).get("revision") or (existing or {}).get("source_hash") or "")
            if expected_revision and expected_revision != current_revision:
                raise VersionPlanConflictError("Version Plan was updated by another session. Refresh and try again.")
            doc = self._doc_for_config(config, existing=existing)
            self.document_ref.set(doc)
            return self._snapshot_from_doc(doc)
        try:
            from google.cloud import firestore  # type: ignore
        except ImportError as error:  # pragma: no cover - depends on runtime deps
            if self._client is None:
                raise RuntimeError("google-cloud-firestore is required for Version Plan Firestore storage.") from error
            snapshot = self.document_ref.get()
            existing = snapshot.to_dict() if getattr(snapshot, "exists", False) else self._default_firestore_doc()
            current_revision = str((existing or {}).get("revision") or (existing or {}).get("source_hash") or "")
            if expected_revision and expected_revision != current_revision:
                raise VersionPlanConflictError("Version Plan was updated by another session. Refresh and try again.")
            doc = self._doc_for_config(config, existing=existing)
            self.document_ref.set(doc)
            return self._snapshot_from_doc(doc)

        transaction = self.client.transaction()

        @firestore.transactional
        def _save(transaction: Any) -> dict[str, Any]:
            snapshot = self.document_ref.get(transaction=transaction)
            existing = snapshot.to_dict() if getattr(snapshot, "exists", False) else self._default_firestore_doc()
            current_revision = str((existing or {}).get("revision") or (existing or {}).get("source_hash") or "")
            if expected_revision and expected_revision != current_revision:
                raise VersionPlanConflictError("Version Plan was updated by another session. Refresh and try again.")
            doc = self._doc_for_config(config, existing=existing)
            transaction.set(self.document_ref, doc)
            return doc

        return self._snapshot_from_doc(_save(transaction))

    def migrate_from_config(
        self,
        *,
        source_revision: str = "",
        backup_payload: dict[str, Any] | None = None,
    ) -> tuple[VersionPlanSnapshot, bool]:
        snapshot = self.document_ref.get()
        if getattr(snapshot, "exists", False):
            existing = snapshot.to_dict() or {}
            existing_plan = normalize_version_plan_state(existing.get("version_plan"))
            if version_plan_source_hash(existing_plan) != version_plan_source_hash(normalize_version_plan_state({})):
                return self._snapshot_from_doc(existing), False
        config = self.config_store.load()
        doc = self._doc_for_config(config, existing={})
        doc["migration_source_revision"] = str(source_revision or "")
        doc["migrated_at_sgt"] = _sgt_now()
        if backup_payload is not None:
            doc["migration_backup_source_hash"] = version_plan_source_hash(
                normalize_version_plan_state(backup_payload.get("version_plan") if isinstance(backup_payload, dict) else {})
            )
        self.document_ref.set(doc)
        return self._snapshot_from_doc(doc), True


def build_version_plan_store(
    settings: Settings,
    config_store: Any,
    *,
    firestore_client: Any | None = None,
) -> TeamDashboardVersionPlanStore:
    if should_use_firestore_version_plan(settings):
        return FirestoreVersionPlanStore(settings=settings, config_store=config_store, firestore_client=firestore_client)
    return LocalTeamDashboardVersionPlanStore(config_store)
