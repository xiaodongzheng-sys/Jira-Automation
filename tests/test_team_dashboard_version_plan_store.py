from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.team_dashboard_config import TeamDashboardConfigStore
from bpmis_jira_tool.team_dashboard_version_plan_store import (
    FirestoreVersionPlanStore,
    VersionPlanConflictError,
    _decode_firestore_fields,
    _encode_firestore_fields,
    build_version_plan_store,
    firestore_document_id,
    should_use_firestore_version_plan,
)


class _FakeSnapshot:
    def __init__(self, payload=None):
        self.payload = payload
        self.exists = payload is not None

    def to_dict(self):
        return dict(self.payload or {})


class _FakeDocument:
    def __init__(self):
        self.payload = None

    def get(self, transaction=None):
        return _FakeSnapshot(self.payload)

    def set(self, payload):
        self.payload = dict(payload)


class _FakeCollection:
    def __init__(self, document):
        self.document_ref = document

    def document(self, document_id):
        self.document_id = document_id
        return self.document_ref


class _FakeFirestoreClient:
    def __init__(self):
        self.document_ref = _FakeDocument()
        self.collection_name = ""

    def collection(self, name):
        self.collection_name = name
        return _FakeCollection(self.document_ref)


class VersionPlanStoreTests(unittest.TestCase):
    def _settings(self, **env):
        base = {
            "FLASK_SECRET_KEY": "test-secret",
            "TEAM_PORTAL_DATA_DIR": tempfile.mkdtemp(),
        }
        base.update(env)
        patcher = patch.dict(os.environ, base, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        return Settings.from_env()

    def test_firestore_document_defaults_isolate_uat_and_live(self):
        uat_settings = self._settings(
            TEAM_PORTAL_STAGE="uat",
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        self.assertTrue(should_use_firestore_version_plan(uat_settings))
        self.assertEqual(firestore_document_id(uat_settings), "version_plan_uat")

        live_settings = self._settings(
            TEAM_PORTAL_STAGE="live",
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        self.assertEqual(firestore_document_id(live_settings), "version_plan_live")

    def test_build_store_keeps_local_default_when_firestore_not_configured(self):
        settings = self._settings()
        store = build_version_plan_store(settings, TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db"))
        self.assertEqual(store.load_snapshot().metadata["backend"], "team_dashboard_config")

    def test_firestore_save_rejects_stale_revision(self):
        settings = self._settings(
            TEAM_PORTAL_STAGE="live",
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        config_store = TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db")
        fake_client = _FakeFirestoreClient()
        store = FirestoreVersionPlanStore(settings=settings, config_store=config_store, firestore_client=fake_client)

        first = store.save_config(config_store.load())
        self.assertEqual(first.metadata["backend"], "firestore")
        self.assertEqual(first.metadata["environment"], "live")

        with self.assertRaises(VersionPlanConflictError):
            store.save_config(config_store.load(), expected_revision="stale")

    def test_migration_is_idempotent_when_document_exists(self):
        settings = self._settings(
            TEAM_PORTAL_STAGE="uat",
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        config_store = TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db")
        fake_client = _FakeFirestoreClient()
        store = FirestoreVersionPlanStore(settings=settings, config_store=config_store, firestore_client=fake_client)

        first, migrated = store.migrate_from_config(source_revision="abc123")
        self.assertTrue(migrated)
        second, migrated_again = store.migrate_from_config(source_revision="def456")
        self.assertFalse(migrated_again)
        self.assertEqual(second.metadata["source_hash"], first.metadata["source_hash"])

    def test_firestore_rest_field_round_trip(self):
        payload = {
            "environment": "live",
            "schema_version": 1,
            "enabled": True,
            "version_plan": {
                "rows": [
                    {"id": "row-1", "remarks": "Keep", "priority": 2},
                    {"id": "row-2", "remarks": "", "priority": None},
                ]
            },
        }
        self.assertEqual(_decode_firestore_fields(_encode_firestore_fields(payload)), payload)


if __name__ == "__main__":
    unittest.main()
