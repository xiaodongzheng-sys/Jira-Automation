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

    def test_firestore_document_explicit_environment_and_document_override(self):
        settings = self._settings(
            TEAM_PORTAL_STAGE="live",
            VERSION_PLAN_STORE_BACKEND="auto",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
            VERSION_PLAN_FIRESTORE_ENVIRONMENT="uat",
            VERSION_PLAN_FIRESTORE_DOCUMENT="custom-version-plan",
        )

        self.assertTrue(should_use_firestore_version_plan(settings))
        self.assertEqual(version_plan_environment(settings), "uat")
        self.assertEqual(firestore_document_id(settings), "custom-version-plan")

        local_settings = self._settings(
            TEAM_PORTAL_STAGE="live",
            VERSION_PLAN_STORE_BACKEND="disabled",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        self.assertFalse(should_use_firestore_version_plan(local_settings))

    def test_build_store_keeps_local_default_when_firestore_not_configured(self):
        settings = self._settings()
        store = build_version_plan_store(settings, TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db"))
        self.assertEqual(store.load_snapshot().metadata["backend"], "team_dashboard_config")

    def test_build_store_uses_firestore_backend_when_auto_cloud_configured(self):
        settings = self._settings(
            TEAM_PORTAL_STAGE="uat",
            VERSION_PLAN_STORE_BACKEND="auto",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )

        store = build_version_plan_store(
            settings,
            TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db"),
            firestore_client=_FakeFirestoreClient(),
        )

        self.assertIsInstance(store, FirestoreVersionPlanStore)
        self.assertEqual(store.environment, "uat")

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

    def test_firestore_missing_document_loads_default_without_writing(self):
        settings = self._settings(
            TEAM_PORTAL_STAGE="uat",
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        config_store = TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db")
        config = config_store.load()
        config["version_plan"]["af"]["pipeline_rows"][0]["feature"] = "Default fallback row"
        config_store.save(config)
        fake_client = _FakeFirestoreClient()
        store = FirestoreVersionPlanStore(settings=settings, config_store=config_store, firestore_client=fake_client)

        snapshot = store.load_snapshot()

        self.assertEqual(snapshot.metadata["backend"], "firestore")
        self.assertEqual(snapshot.metadata["environment"], "uat")
        self.assertEqual(snapshot.metadata["document_path"], "portal/version_plan_uat")
        self.assertEqual(fake_client.document_ref.set_calls, 0)
        self.assertTrue(
            any(row["feature"] == "Default fallback row" for row in snapshot.config["version_plan"]["af"]["pipeline_rows"])
        )

    def test_firestore_base_config_failure_falls_back_to_empty_plan(self):
        settings = self._settings(
            TEAM_PORTAL_STAGE="uat",
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )

        class BrokenConfigStore:
            def load(self):
                raise RuntimeError("sqlite unavailable")

        store = FirestoreVersionPlanStore(
            settings=settings,
            config_store=BrokenConfigStore(),
            firestore_client=_FakeFirestoreClient(),
        )

        snapshot = store.load_snapshot()

        self.assertEqual(snapshot.metadata["backend"], "firestore")
        self.assertEqual(snapshot.config["version_plan"]["af"]["sync_state"]["state"], "idle")

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

    def test_migration_records_backup_source_hash_for_empty_firestore_doc(self):
        settings = self._settings(
            TEAM_PORTAL_STAGE="uat",
            VERSION_PLAN_STORE_BACKEND="firestore",
            VERSION_PLAN_FIRESTORE_PROJECT="test-project",
        )
        config_store = TeamDashboardConfigStore(settings.team_portal_data_dir / "team_dashboard.db")
        fake_client = _FakeFirestoreClient()
        store = FirestoreVersionPlanStore(settings=settings, config_store=config_store, firestore_client=fake_client)
        backup_payload = config_store.load()

        snapshot, migrated = store.migrate_from_config(
            source_revision="source-rev",
            backup_payload=backup_payload,
        )

        self.assertTrue(migrated)
        self.assertEqual(snapshot.metadata["migration_source_revision"], "source-rev")
        self.assertEqual(
            snapshot.metadata["migration_backup_source_hash"],
            version_plan_source_hash(backup_payload["version_plan"]),
        )

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

    def test_firestore_rest_document_get_set_and_client_validation(self):
        get_calls = []
        patch_calls = []
        encoded = _encode_firestore_fields({"version_plan": {"af": {"pipeline_rows": []}}, "revision": "rev-1"})

        def fake_get(url, *, headers, timeout):
            get_calls.append((url, headers, timeout))
            return _FakeRestResponse(payload={"fields": encoded})

        def fake_patch(url, *, headers, json, timeout):
            patch_calls.append((url, headers, json, timeout))
            return _FakeRestResponse()

        document = _FirestoreRestDocument(
            project="test-project",
            document_id="version_plan_uat",
            token_provider=lambda: "token-123",
        )

        with patch("bpmis_jira_tool.team_dashboard_version_plan_store.requests.get", side_effect=fake_get), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.patch",
            side_effect=fake_patch,
        ):
            snapshot = document.get()
            document.set({"revision": "rev-2"})

        self.assertTrue(snapshot.exists)
        self.assertEqual(snapshot.to_dict()["revision"], "rev-1")
        self.assertIn("/portal/version_plan_uat", get_calls[0][0])
        self.assertEqual(get_calls[0][1]["Authorization"], "Bearer token-123")
        self.assertEqual(patch_calls[0][2]["fields"]["revision"], {"stringValue": "rev-2"})
        self.assertTrue(_FirestoreRestSnapshot(None).to_dict() == {})
        self.assertFalse(_FirestoreRestDocument(project="p", document_id="d", token_provider=lambda: "x").get is None)

        client = _FirestoreRestClient(project="test-project", token_provider=lambda: "token-123")
        self.assertEqual(client.collection("portal").document("doc").project, "test-project")
        with self.assertRaisesRegex(ValueError, "Unsupported Firestore collection"):
            client.collection("other")

    def test_firestore_rest_document_404_and_http_error_branches(self):
        document = _FirestoreRestDocument(project="test-project", document_id="version_plan_uat", token_provider=lambda: "token")
        with patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            return_value=_FakeRestResponse(status_code=404),
        ):
            self.assertFalse(document.get().exists)

        with patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.get",
            return_value=_FakeRestResponse(status_code=500, raise_error=RuntimeError("server down")),
        ):
            with self.assertRaisesRegex(RuntimeError, "server down"):
                document.get()

        with patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store.requests.patch",
            return_value=_FakeRestResponse(status_code=500, raise_error=RuntimeError("write down")),
        ):
            with self.assertRaisesRegex(RuntimeError, "write down"):
                document.set({"revision": "rev"})

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
