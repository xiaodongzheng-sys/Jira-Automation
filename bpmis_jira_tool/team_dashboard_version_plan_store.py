"""Version Plan storage backends.

The Team Dashboard config remains the source for non-Version-Plan settings.
Version Plan can be promoted to Firestore independently so Cloud Run and the
Mac portal share one document without moving the rest of the portal.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
import shutil
import subprocess
from typing import Any
from zoneinfo import ZoneInfo

import requests

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.team_dashboard_version_plan import normalize_version_plan_state


SGT = ZoneInfo("Asia/Singapore")
VERSION_PLAN_SCHEMA_VERSION = 1
_METADATA_ACCESS_TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"


class VersionPlanConflictError(ValueError):
    """Raised when a client saves against a stale Version Plan revision."""


def _sgt_now() -> str:
    return datetime.now(tz=SGT).strftime("%Y-%m-%d %H:%M:%S SGT")


def version_plan_source_hash(version_plan: dict[str, Any]) -> str:
    normalized = normalize_version_plan_state(version_plan)
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def version_plan_environment(settings: Settings) -> str:
    return "live"


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
    stage = str(settings.team_portal_stage or "").strip().lower() or "live"
    return bool(settings.version_plan_firestore_project and stage == "live")


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
        normalized = copy.deepcopy(config) if isinstance(config, dict) else {}
        plan = normalize_version_plan_state(normalized.get("version_plan") if isinstance(normalized, dict) else {})
        revision = version_plan_source_hash(plan)
        metadata = {
            "backend": "team_dashboard_config",
            "environment": "local",
            "schema_version": VERSION_PLAN_SCHEMA_VERSION,
            "revision": revision,
            "source_hash": revision,
            "updated_at_sgt": "",
        }
        normalized["version_plan"] = plan
        return VersionPlanSnapshot(config=normalized, metadata=metadata)

    def load_snapshot(self) -> VersionPlanSnapshot:
        return self._snapshot(self.config_store.load())

    def save_config(self, config: dict[str, Any], *, expected_revision: str | None = None) -> VersionPlanSnapshot:
        current = self.load_snapshot()
        if expected_revision and expected_revision != current.revision:
            raise VersionPlanConflictError("Version Plan was updated by another session. Refresh and try again.")
        return self._snapshot(self.config_store.save(config))


class _FirestoreRestSnapshot:
    def __init__(self, payload: dict[str, Any] | None) -> None:
        self.payload = payload
        self.exists = payload is not None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload or {})


class _FirestoreRestDocument:
    def __init__(self, *, project: str, document_id: str, token_provider: Any | None = None) -> None:
        self.project = project
        self.document_id = document_id
        self._token_provider = token_provider or _gcloud_access_token

    @property
    def url(self) -> str:
        return (
            "https://firestore.googleapis.com/v1/projects/"
            f"{self.project}/databases/(default)/documents/portal/{self.document_id}"
        )

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token_provider()}", "Content-Type": "application/json"}

    def get(self, transaction: Any | None = None) -> _FirestoreRestSnapshot:
        response = requests.get(self.url, headers=self._headers(), timeout=20)
        if response.status_code == 404:
            return _FirestoreRestSnapshot(None)
        response.raise_for_status()
        return _FirestoreRestSnapshot(_decode_firestore_fields(response.json().get("fields") or {}))

    def set(self, payload: dict[str, Any]) -> None:
        response = requests.patch(
            self.url,
            headers=self._headers(),
            json={"fields": _encode_firestore_fields(payload)},
            timeout=20,
        )
        response.raise_for_status()


class _FirestoreRestCollection:
    def __init__(self, *, project: str, token_provider: Any | None = None) -> None:
        self.project = project
        self._token_provider = token_provider

    def document(self, document_id: str) -> _FirestoreRestDocument:
        return _FirestoreRestDocument(
            project=self.project,
            document_id=document_id,
            token_provider=self._token_provider,
        )


class _FirestoreRestClient:
    def __init__(self, *, project: str, token_provider: Any | None = None) -> None:
        self.project = project
        self._token_provider = token_provider

    def collection(self, name: str) -> _FirestoreRestCollection:
        if name != "portal":
            raise ValueError(f"Unsupported Firestore collection for Version Plan: {name}")
        return _FirestoreRestCollection(project=self.project, token_provider=self._token_provider)


def _gcloud_access_token() -> str:
    explicit = os.environ.get("FIRESTORE_ACCESS_TOKEN", "").strip()
    if explicit:
        return explicit
    metadata_token = _metadata_server_access_token()
    if metadata_token:
        return metadata_token
    gcloud = shutil.which("gcloud") or (os.path.expanduser("~/google-cloud-sdk/bin/gcloud"))
    if not gcloud or not os.path.exists(gcloud):
        raise RuntimeError("Firestore REST fallback requires Cloud Run metadata token, gcloud, or FIRESTORE_ACCESS_TOKEN.")
    completed = subprocess.run(
        [gcloud, "auth", "print-access-token"],
        capture_output=True,
        check=True,
        text=True,
        timeout=20,
    )
    token = completed.stdout.strip()
    if not token:
        raise RuntimeError("gcloud auth print-access-token returned an empty token.")
    return token


def _metadata_server_access_token() -> str:
    try:
        response = requests.get(
            _METADATA_ACCESS_TOKEN_URL,
            headers={"Metadata-Flavor": "Google"},
            timeout=5,
        )
    except requests.RequestException:
        return ""
    if response.status_code != 200:
        return ""
    try:
        payload = response.json()
    except ValueError:
        return ""
    return str(payload.get("access_token") or "").strip()


def _encode_firestore_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _encode_firestore_value(value) for key, value in payload.items()}


def _encode_firestore_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {"nullValue": None}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"integerValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [_encode_firestore_value(item) for item in value]}}
    if isinstance(value, dict):
        return {"mapValue": {"fields": _encode_firestore_fields(value)}}
    return {"stringValue": str(value)}


def _decode_firestore_fields(fields: dict[str, Any]) -> dict[str, Any]:
    return {key: _decode_firestore_value(value) for key, value in fields.items()}


def _decode_firestore_value(value: dict[str, Any]) -> Any:
    if "nullValue" in value:
        return None
    if "booleanValue" in value:
        return bool(value["booleanValue"])
    if "integerValue" in value:
        try:
            return int(value["integerValue"])
        except (TypeError, ValueError):
            return value["integerValue"]
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "stringValue" in value:
        return value["stringValue"]
    if "arrayValue" in value:
        return [_decode_firestore_value(item) for item in value.get("arrayValue", {}).get("values", [])]
    if "mapValue" in value:
        return _decode_firestore_fields(value.get("mapValue", {}).get("fields") or {})
    if "timestampValue" in value:
        return value["timestampValue"]
    return None


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
            self._client = firestore.Client(project=self.settings.version_plan_firestore_project)
            return self._client
        except Exception as error:  # pragma: no cover - depends on runtime deps/credentials
            project = str(self.settings.version_plan_firestore_project or "").strip()
            if not project:
                raise RuntimeError("VERSION_PLAN_FIRESTORE_PROJECT is required for Version Plan Firestore storage.") from error
            self._client = _FirestoreRestClient(project=project)
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
        client = self.client
        if not hasattr(client, "transaction"):
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
