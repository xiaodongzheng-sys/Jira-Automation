from __future__ import annotations

import os
import builtins
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import requests

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.team_dashboard_config import TeamDashboardConfigStore
from bpmis_jira_tool.team_dashboard_version_plan_store import (
    FirestoreVersionPlanStore,
    LocalTeamDashboardVersionPlanStore,
    VersionPlanConflictError,
    _FirestoreRestClient,
    _FirestoreRestDocument,
    _FirestoreRestSnapshot,
    TeamDashboardVersionPlanStore,
    _decode_firestore_fields,
    _decode_firestore_value,
    _encode_firestore_fields,
    _encode_firestore_value,
    _gcloud_access_token,
    _metadata_server_access_token,
    build_version_plan_store,
    firestore_document_id,
    should_use_firestore_version_plan,
    version_plan_environment,
    version_plan_source_hash,
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
        self.set_calls = 0

    def get(self, transaction=None):
        return _FakeSnapshot(self.payload)

    def set(self, payload):
        self.set_calls += 1
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


class _FakeTransaction:
    def __init__(self):
        self.set_calls = []

    def set(self, document_ref, doc):
        self.set_calls.append((document_ref, dict(doc)))
        document_ref.set(doc)


class _FakeTransactionalFirestoreClient(_FakeFirestoreClient):
    def __init__(self):
        super().__init__()
        self.transaction_ref = _FakeTransaction()

    def transaction(self):
        return self.transaction_ref


class _FakeMetadataResponse:
    status_code = 200

    def json(self):
        return {"access_token": "metadata-token"}


class _FakeRestResponse:
    def __init__(self, *, status_code=200, payload=None, raise_error=None):
        self.status_code = status_code
        self.payload = payload or {}
        self.raise_error = raise_error

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.raise_error:
            raise self.raise_error


class VersionPlanStoreTests(unittest.TestCase):
    def _settings(self, **env):
        base = {
            "ENV_FILE": "",
            "FLASK_SECRET_KEY": "test-secret",
            "TEAM_PORTAL_DATA_DIR": tempfile.mkdtemp(),
        }
        base.update(env)
        patcher = patch.dict(os.environ, base, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        return Settings.from_env()

    def test_build_store_keeps_local_default_when_firestore_not_configured(self):
        settings = self._settings()
        store = build_version_plan_store(settings, TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db"))
        self.assertEqual(store.load_snapshot().metadata["backend"], "team_dashboard_config")

    def test_local_store_snapshot_is_immutable_and_rejects_stale_revision(self):
        settings = self._settings()
        config_store = TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db")
        store = LocalTeamDashboardVersionPlanStore(config_store)

        snapshot = store.load_snapshot()
        snapshot.config["version_plan"]["af"]["pipeline_rows"][0]["feature"] = "mutated in test"
        fresh_snapshot = store.load_snapshot()

        self.assertNotEqual(
            fresh_snapshot.config["version_plan"]["af"]["pipeline_rows"][0]["feature"],
            "mutated in test",
        )
        with self.assertRaises(VersionPlanConflictError):
            store.save_config(config_store.load(), expected_revision="stale-revision")

    def test_base_store_contract_and_local_save_success(self):
        base_store = TeamDashboardVersionPlanStore()
        with self.assertRaises(NotImplementedError):
            base_store.load_snapshot()
        with self.assertRaises(NotImplementedError):
            base_store.save_config({})

        settings = self._settings()
        config_store = TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db")
        store = LocalTeamDashboardVersionPlanStore(config_store)
        snapshot = store.load_snapshot()
        config_store.save(snapshot.config)
        snapshot = store.load_snapshot()
        config = snapshot.config
        config["version_plan"]["af"]["pipeline_rows"][0]["remarks"] = "Saved locally"

        saved = store.save_config(config, expected_revision=snapshot.revision)

        self.assertEqual(saved.config["version_plan"]["af"]["pipeline_rows"][0]["remarks"], "Saved locally")

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

    def test_firestore_rest_field_edge_cases(self):
        self.assertEqual(_encode_firestore_value(("tuple", 1)), {"stringValue": "('tuple', 1)"})
        self.assertEqual(_encode_firestore_value(2.5), {"doubleValue": 2.5})
        self.assertEqual(_decode_firestore_value({"integerValue": "not-int"}), "not-int")
        self.assertEqual(_decode_firestore_value({"doubleValue": "2.5"}), 2.5)
        self.assertEqual(_decode_firestore_value({"timestampValue": "2026-05-23T12:00:00Z"}), "2026-05-23T12:00:00Z")
        self.assertIsNone(_decode_firestore_value({"geoPointValue": {"latitude": 1}}))
        self.assertEqual(_decode_firestore_value({"arrayValue": {}}), [])
        self.assertEqual(_decode_firestore_value({"mapValue": {}}), {})

    def test_firestore_client_uses_rest_fallback_when_google_cloud_unavailable(self):
        original_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "google.cloud":
                raise ImportError("google cloud missing")
            return original_import(name, globals, locals, fromlist, level)

        missing_project_settings = self._settings(
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="",
        )
        missing_project_store = FirestoreVersionPlanStore(
            settings=missing_project_settings,
            config_store=TeamDashboardConfigStore(missing_project_settings.team_portal_data_dir / "team_dashboard.db"),
        )
        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaisesRegex(RuntimeError, "VERSION_PLAN_FIRESTORE_PROJECT"):
                _ = missing_project_store.client

        settings = self._settings(
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        store = FirestoreVersionPlanStore(
            settings=settings,
            config_store=TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db"),
        )
        with patch("builtins.__import__", side_effect=fake_import):
            self.assertIsInstance(store.client, _FirestoreRestClient)

    def test_firestore_client_uses_google_cloud_client_when_available(self):
        settings = self._settings(
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        store = FirestoreVersionPlanStore(
            settings=settings,
            config_store=TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db"),
        )
        fake_client = _FakeFirestoreClient()
        original_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "google.cloud":
                return SimpleNamespace(firestore=SimpleNamespace(Client=lambda project: fake_client))
            return original_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            self.assertIs(store.client, fake_client)

    def test_firestore_transactional_save_path(self):
        settings = self._settings(
            TEAM_PORTAL_STAGE="live",
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        config_store = TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db")
        fake_client = _FakeTransactionalFirestoreClient()
        store = FirestoreVersionPlanStore(settings=settings, config_store=config_store, firestore_client=fake_client)
        original_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "google.cloud":
                return SimpleNamespace(firestore=SimpleNamespace(transactional=lambda fn: fn))
            return original_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            snapshot = store.save_config(config_store.load())
            with self.assertRaises(VersionPlanConflictError):
                store.save_config(config_store.load(), expected_revision="stale")

        self.assertEqual(snapshot.metadata["backend"], "firestore")
        self.assertEqual(len(fake_client.transaction_ref.set_calls), 1)

    def test_firestore_rest_token_uses_metadata_server_before_gcloud(self):
        with patch.dict(os.environ, {}, clear=True), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            return_value=_FakeMetadataResponse(),
        ) as metadata_get, patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.shutil.which",
            side_effect=AssertionError("gcloud should not be needed on Cloud Run"),
        ):
            self.assertEqual(_gcloud_access_token(), "metadata-token")
        metadata_get.assert_called_once()

    def test_firestore_rest_token_uses_env_or_gcloud_and_reports_empty_token(self):
        with patch.dict(os.environ, {"FIRESTORE_ACCESS_TOKEN": " env-token "}, clear=True):
            self.assertEqual(_gcloud_access_token(), "env-token")

        with patch.dict(os.environ, {}, clear=True), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            side_effect=requests.RequestException("metadata unavailable"),
        ), patch("bpmis_jira_tool.team_dashboard_version_plan_store.shutil.which", return_value="/usr/bin/gcloud"), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.os.path.exists",
            return_value=True,
        ), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.subprocess.run",
            return_value=SimpleNamespace(stdout="gcloud-token\n"),
        ) as run:
            self.assertEqual(_gcloud_access_token(), "gcloud-token")
        run.assert_called_once()

        with patch.dict(os.environ, {}, clear=True), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            side_effect=requests.RequestException("metadata unavailable"),
        ), patch("bpmis_jira_tool.team_dashboard_version_plan_store.shutil.which", return_value="/usr/bin/gcloud"), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.os.path.exists",
            return_value=True,
        ), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.subprocess.run",
            return_value=SimpleNamespace(stdout=""),
        ):
            with self.assertRaisesRegex(RuntimeError, "empty token"):
                _gcloud_access_token()

    def test_metadata_token_failure_modes_and_missing_gcloud(self):
        with patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            side_effect=requests.RequestException("network down"),
        ):
            self.assertEqual(_metadata_server_access_token(), "")

        with patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            return_value=_FakeRestResponse(status_code=500),
        ):
            self.assertEqual(_metadata_server_access_token(), "")

        class BadJsonResponse(_FakeRestResponse):
            status_code = 200

            def json(self):
                raise ValueError("bad json")

        with patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            return_value=BadJsonResponse(),
        ):
            self.assertEqual(_metadata_server_access_token(), "")

        with patch.dict(os.environ, {}, clear=True), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            return_value=_FakeRestResponse(status_code=500),
        ), patch("bpmis_jira_tool.team_dashboard_version_plan_store.shutil.which", return_value=None), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.os.path.exists",
            return_value=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "Firestore REST fallback requires"):
                _gcloud_access_token()


if __name__ == "__main__":
    unittest.main()
