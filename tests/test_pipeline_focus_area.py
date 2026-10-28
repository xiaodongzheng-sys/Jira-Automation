from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
import os
import json
import sqlite3
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace
from urllib.parse import urlsplit

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.team_dashboard_config import TeamDashboardConfigStore
from bpmis_jira_tool.team_dashboard_version_plan import (
    merge_version_plan_editable_state,
    normalize_version_plan_state,
    pipeline_focus_area,
    update_version_plan_cell,
    update_version_plan_rows,
    version_plan_sync,
)
from bpmis_jira_tool.team_dashboard_version_plan_store import (
    FirestoreVersionPlanStore,
    LocalTeamDashboardVersionPlanStore,
    VersionPlanConflictError,
    _FirestoreRestDocument,
    _FirestoreRestSnapshot,
    _encode_firestore_fields,
)
from tests.test_team_dashboard_version_plan import FakeBPMISVersionPlanClient
from tests.test_team_dashboard_version_plan_store import _FakeFirestoreClient


def config_for(rows=()):
    return {"version_plan": normalize_version_plan_state({"af": {
        "pipeline_rows": list(rows), "bundles": {"v1": {"manual_rows": [], "synced_rows": []}},
    }})}


def add(config, row_id="row-1", project_id="123456", **fields):
    return update_version_plan_rows(config, {
        "scope": "pipeline", "action": "add", "row_id": row_id, "bpmis_project_id": project_id, **fields,
    })


def edit(config, field, value, row_id="row-1"):
    return update_version_plan_cell(config, {
        "scope": "pipeline", "row_id": row_id, "field": field, "value": value,
    })


class PipelineFocusAreaTests(unittest.TestCase):
    def test_prd_status_defaults_valid_values_and_history(self):
        config = add(config_for(), productization_efforts="Y")
        self.assertEqual(config["version_plan"]["af"]["pipeline_rows"][0]["prd_status"], "Not Started")
        for status in ("Not Started", "PRD In Progress", "PRD Reviewed"):
            config = edit(config, "prd_status", status)
            self.assertEqual(normalize_version_plan_state(config["version_plan"])["af"]["pipeline_rows"][0]["prd_status"], status)
        for value in ("Y", "N", "Done", "", None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "PRD Status must"):
                edit(config, "prd_status", value)
        config = update_version_plan_rows(config, {"scope": "pipeline", "action": "delete", "row_id": "row-1"})
        archived = config["version_plan"]["af"]["pipeline_history"][0]["row"]
        self.assertEqual(archived["prd_status"], "PRD Reviewed")
        self.assertEqual(archived["productization_efforts"], "Y")

    def test_defaults_and_strict_values(self):
        config = add(config_for())
        self.assertEqual(pipeline_focus_area(config, "123456")["focus_area"], "No")
        for value in ("Y", "yes", "", None, True):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "Yes or No"):
                edit(config, "focus_area", value)
        with self.assertRaisesRegex(ValueError, "only be edited in Pipeline"):
            update_version_plan_cell(config, {"scope": "bundle", "field": "focus_area", "value": "Yes"})
        for project_id in ("", "12345", "1234567", "１２３４５６", "abcdef"):
            with self.subTest(project_id=project_id), self.assertRaises(ValueError):
                pipeline_focus_area(config, project_id)

    def test_delete_retains_history_and_readd_restores_focus(self):
        config = edit(add(config_for(), feature="Full project", remarks="Keep this note", pm=["Jireh"]), "focus_area", "Yes")
        deleted_row = deepcopy(config["version_plan"]["af"]["pipeline_rows"][0])
        config = update_version_plan_rows(config, {"scope": "pipeline", "action": "delete", "row_id": "row-1"})
        record = pipeline_focus_area(config, "123456")
        self.assertEqual(record["focus_area"], "Yes")
        self.assertFalse(record["has_active_pipeline_row"])
        self.assertEqual(config["version_plan"]["af"]["pipeline_history"][0]["row"], deleted_row)
        self.assertIsNone(pipeline_focus_area(config, "654321"))
        config = add(config, row_id="row-2")
        self.assertEqual(pipeline_focus_area(config, "123456")["focus_area"], "Yes")
        self.assertTrue(pipeline_focus_area(config, "123456")["has_active_pipeline_row"])

    def test_change_or_clear_id_does_not_transfer_focus(self):
        config = edit(add(config_for()), "focus_area", "Yes")
        config = edit(config, "bpmis_project_id", "654321")
        self.assertEqual(pipeline_focus_area(config, "123456")["focus_area"], "Yes")
        self.assertFalse(pipeline_focus_area(config, "123456")["has_active_pipeline_row"])
        self.assertEqual(pipeline_focus_area(config, "654321")["focus_area"], "No")
        config = edit(config, "bpmis_project_id", "")
        self.assertFalse(pipeline_focus_area(config, "654321")["has_active_pipeline_row"])
        config = edit(config, "bpmis_project_id", "123456")
        self.assertEqual(pipeline_focus_area(config, "123456")["focus_area"], "Yes")

    def test_duplicate_add_edit_move_rejected_without_changing_original(self):
        config = add(config_for())
        config = add(config, row_id="row-2", project_id="654321")
        original = deepcopy(config)
        for operation in (
            lambda: add(config, row_id="row-3"),
            lambda: edit(config, "bpmis_project_id", "123456", row_id="row-2"),
        ):
            with self.assertRaisesRegex(ValueError, "already has an active Pipeline row"):
                operation()
            self.assertEqual(config, original)
        config["version_plan"]["af"]["bundles"]["v1"]["manual_rows"] = [{
            "row_id": "bundle-row", "bpmis_project_id": "123456", "focus_area": "No",
        }]
        with self.assertRaisesRegex(ValueError, "already has an active Pipeline row"):
            update_version_plan_rows(config, {
                "action": "move", "source_scope": "bundle", "source_version_id": "v1",
                "target_scope": "pipeline", "row_id": "bundle-row",
            })
        config = add(config, row_id="blank-1", project_id="")
        config = add(config, row_id="blank-2", project_id="")
        self.assertEqual(len(config["version_plan"]["af"]["pipeline_rows"]), 4)

    def test_move_out_and_back_keeps_relationship(self):
        config = edit(add(config_for()), "focus_area", "Yes")
        config = update_version_plan_rows(config, {
            "action": "move", "source_scope": "pipeline", "target_scope": "bundle", "target_version_id": "v1", "row_id": "row-1",
        })
        self.assertFalse(pipeline_focus_area(config, "123456")["has_active_pipeline_row"])
        config = update_version_plan_rows(config, {
            "action": "move", "source_scope": "bundle", "source_version_id": "v1", "target_scope": "pipeline", "row_id": "row-1",
        })
        self.assertTrue(pipeline_focus_area(config, "123456")["has_active_pipeline_row"])
        self.assertEqual(pipeline_focus_area(config, "123456")["focus_area"], "Yes")

    def test_legacy_duplicates_are_archived_in_full_and_normalization_is_idempotent(self):
        raw = {"af": {"pipeline_rows": [
            {"row_id": "old", "bpmis_project_id": "123456", "updated_at": "2026-10-27T09:00:00+08:00", "focus_area": "No", "custom_note": "Keep all fields"},
            {"row_id": "new-a", "bpmis_project_id": "123456", "updated_at": "2026-10-28T09:00:00+08:00", "focus_area": "No"},
            {"row_id": "new-b", "bpmis_project_id": "123456", "updated_at": "2026-10-28T09:00:00+08:00", "focus_area": "Yes"},
        ]}}
        normalized = normalize_version_plan_state(raw)
        af = normalized["af"]
        self.assertEqual([row["row_id"] for row in af["pipeline_rows"]], ["new-b"])
        self.assertEqual(len(af["pipeline_history"]), 2)
        self.assertEqual(af["pipeline_history"][0]["row"]["custom_note"], "Keep all fields")
        self.assertEqual(af["pipeline_focus_areas"]["123456"]["focus_area"], "Yes")
        self.assertEqual(normalize_version_plan_state(normalized), normalized)
        self.assertEqual(raw["af"]["pipeline_rows"][0]["row_id"], "old")

    def test_active_row_wins_and_legacy_row_can_restore_saved_relationship(self):
        config = config_for([{"row_id": "row-1", "bpmis_project_id": "123456", "focus_area": "No"}])
        config["version_plan"]["af"]["pipeline_focus_areas"]["123456"]["focus_area"] = "Yes"
        self.assertEqual(pipeline_focus_area(config, "123456")["focus_area"], "No")
        del config["version_plan"]["af"]["pipeline_rows"][0]["focus_area"]
        self.assertEqual(pipeline_focus_area(config, "123456")["focus_area"], "Yes")

    def test_sync_and_edit_merge_preserve_focus_registry_and_history(self):
        config = edit(add(config_for()), "focus_area", "Yes")
        config = update_version_plan_rows(config, {"scope": "pipeline", "action": "delete", "row_id": "row-1"})
        synced = version_plan_sync(config, FakeBPMISVersionPlanClient(), now=datetime(2026, 5, 1))
        for result in (synced, merge_version_plan_editable_state(config_for(), config)):
            self.assertEqual(pipeline_focus_area(result, "123456")["focus_area"], "Yes")
            self.assertEqual(result["version_plan"]["af"]["pipeline_history"], config["version_plan"]["af"]["pipeline_history"])

    def test_local_and_firestore_round_trip_and_stale_save(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {
            "ENV_FILE": os.devnull, "FLASK_SECRET_KEY": "test", "TEAM_PORTAL_DATA_DIR": temp_dir,
            "VERSION_PLAN_FIRESTORE_PROJECT": "test-project",
        }, clear=True):
            settings = Settings.from_env()
            config_store = TeamDashboardConfigStore(Path(temp_dir) / "config.db")
            for store in (LocalTeamDashboardVersionPlanStore(config_store), FirestoreVersionPlanStore(
                settings=settings, config_store=config_store, firestore_client=_FakeFirestoreClient(),
            )):
                with self.subTest(backend=type(store).__name__):
                    first = store.save_config(edit(edit(add(config_for()), "focus_area", "Yes"), "prd_status", "PRD In Progress"))
                    deleted = update_version_plan_rows(first.config, {"scope": "pipeline", "action": "delete", "row_id": "row-1"})
                    saved = store.save_config(deleted, expected_revision=first.revision)
                    self.assertEqual(store.load_snapshot().config["version_plan"], saved.config["version_plan"])
                    self.assertEqual(pipeline_focus_area(saved.config, "123456")["focus_area"], "Yes")
                    self.assertEqual(saved.config["version_plan"]["af"]["pipeline_history"][0]["row"]["prd_status"], "PRD In Progress")
                    with self.assertRaises(VersionPlanConflictError):
                        store.save_config(first.config, expected_revision=first.revision)

    def test_concurrent_sqlite_saves_only_commit_one_revision(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_store = TeamDashboardConfigStore(Path(temp_dir) / "config.db")
            store = LocalTeamDashboardVersionPlanStore(config_store)
            first = store.save_config(config_for())
            barrier = threading.Barrier(2)

            def save(row_id):
                config = add(first.config, row_id=row_id)
                barrier.wait(timeout=5)
                try:
                    store.save_config(config, expected_revision=first.revision)
                    return "saved"
                except VersionPlanConflictError:
                    return "conflict"

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(save, ["a", "b"]))
            self.assertCountEqual(results, ["saved", "conflict"])
            self.assertEqual(len(store.load_snapshot().config["version_plan"]["af"]["pipeline_rows"]), 1)

    def test_remote_local_agent_saves_plan_atomically_and_transports_conflicts(self):
        from bpmis_jira_tool.local_agent_client import LocalAgentClient, RemoteTeamDashboardConfigStore
        from bpmis_jira_tool.local_agent_server import create_local_agent_app

        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {
            "ENV_FILE": os.devnull, "TEAM_PORTAL_DATA_DIR": temp_dir,
            "LOCAL_AGENT_HMAC_SECRET": "test-secret", "LOCAL_AGENT_BPMIS_ENABLED": "true",
        }, clear=True):
            app = create_local_agent_app()
            app.testing = True

            def request(method, url, *, data, headers, **kwargs):
                response = app.test_client().open(urlsplit(url).path, method=method, data=data, headers=headers)
                return SimpleNamespace(status_code=response.status_code, json=response.get_json, text=response.get_data(as_text=True), url=url)

            session = Mock()
            session.request.side_effect = request
            client = LocalAgentClient(base_url="http://local.test", hmac_secret="test-secret", session=session)
            store = LocalTeamDashboardVersionPlanStore(RemoteTeamDashboardConfigStore(client))
            first = store.save_config(add(config_for()))
            store.save_config(edit(first.config, "focus_area", "Yes"), expected_revision=first.revision)
            with self.assertRaises(VersionPlanConflictError):
                store.save_config(first.config, expected_revision=first.revision)
            self.assertEqual(pipeline_focus_area(store.load_snapshot().config, "123456")["focus_area"], "Yes")

    def test_rest_compare_and_swap_checks_document_update_time(self):
        document = _FirestoreRestDocument(project="test", document_id="test", token_provider=lambda: "test")
        response = Mock(status_code=200)
        response.json.return_value = {"updateTime": "v1", "fields": _encode_firestore_fields({"revision": "r1"})}
        with patch("bpmis_jira_tool.team_dashboard_version_plan_store.requests.get", return_value=response):
            snapshot = document.get()
        with patch("bpmis_jira_tool.team_dashboard_version_plan_store.requests.patch", return_value=response) as request:
            document.set({"revision": "r2"}, snapshot=snapshot)
        self.assertEqual(request.call_args.kwargs["params"], {"currentDocument.updateTime": "v1"})
        for code in (409, 412, 400):
            response.status_code = code
            response.json.return_value = {"error": {"status": "FAILED_PRECONDITION"}}
            with self.subTest(code=code), patch("bpmis_jira_tool.team_dashboard_version_plan_store.requests.patch", return_value=response):
                with self.assertRaises(VersionPlanConflictError):
                    document.set({}, snapshot=snapshot)
        response.status_code = 200
        with patch("bpmis_jira_tool.team_dashboard_version_plan_store.requests.patch", return_value=response) as request:
            document.set({}, snapshot=_FirestoreRestSnapshot(None))
        self.assertEqual(request.call_args.kwargs["params"], {"currentDocument.exists": "false"})
        with self.assertRaisesRegex(RuntimeError, "unconditional write"):
            document.set({}, snapshot=_FirestoreRestSnapshot({}))

    def test_migration_is_read_only_by_default_and_backs_up_raw_duplicates_before_apply(self):
        from scripts.migrate_pipeline_focus_areas import migrate

        with tempfile.TemporaryDirectory() as temp_dir:
            config_store = TeamDashboardConfigStore(Path(temp_dir) / "config.db")
            store = LocalTeamDashboardVersionPlanStore(config_store)
            raw_config = {"version_plan": {"af": {"pipeline_rows": [
                {"row_id": "a", "bpmis_project_id": "123456", "updated_at": "2026-10-27T09:00:00", "custom_note": "Legacy note"},
                {"row_id": "b", "bpmis_project_id": "123456", "updated_at": "2026-10-28T09:00:00"},
            ]}}}
            with sqlite3.connect(config_store.db_path) as connection:
                connection.execute("INSERT INTO team_dashboard_configs (config_key, config_json) VALUES (?, ?)", (config_store.CONFIG_KEY, json.dumps(raw_config)))
            self.assertEqual(migrate(store)["newly_archived_duplicates"], 1)
            with sqlite3.connect(config_store.db_path) as connection:
                self.assertEqual(json.loads(connection.execute("SELECT config_json FROM team_dashboard_configs").fetchone()[0]), raw_config)
            with self.assertRaisesRegex(ValueError, "backup-file"):
                migrate(store, apply=True)
            backup = Path(temp_dir) / "backup.json"
            result = migrate(store, apply=True, backup_file=backup)
            self.assertEqual(result["status"], "applied")
            self.assertEqual(json.loads(backup.read_text())["source"], raw_config)
            self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
            self.assertEqual(len(store.load_snapshot().config["version_plan"]["af"]["pipeline_rows"]), 1)
            self.assertFalse(migrate(store)["changes_required"])
            with self.assertRaises(FileExistsError):
                migrate(store, apply=True, backup_file=backup)


class PipelineFocusAreaApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        env = patch.dict(os.environ, {
            "ENV_FILE": os.devnull, "FLASK_SECRET_KEY": "test", "TEAM_PORTAL_DATA_DIR": self.temp.name,
            "VERSION_PLAN_STORE_BACKEND": "local",
            "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
        }, clear=True)
        env.start()
        self.addCleanup(env.stop)
        from bpmis_jira_tool.web import create_app
        self.app = create_app()
        self.app.testing = True
        self.client = self.app.test_client()
        self.login("xiaodong.zheng@npt.sg")

    def login(self, email):
        with self.client.session_transaction() as session:
            session["google_profile"] = {"email": email, "name": email}
            session["google_credentials"] = {"token": "test"}

    def test_save_lookup_delete_lookup_and_permissions(self):
        base = "/api/team-dashboard/version-plan/af"
        response = self.client.post(f"{base}/rows", json={
            "scope": "pipeline", "action": "add", "row_id": "api-row", "bpmis_project_id": "123456",
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        response = self.client.post(f"{base}/cell", json={
            "scope": "pipeline", "row_id": "api-row", "field": "focus_area", "value": "Yes",
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        response = self.client.post(f"{base}/cell", json={
            "scope": "pipeline", "row_id": "api-row", "field": "prd_status", "value": "PRD Reviewed",
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(next(row for row in response.get_json()["pipeline_rows"] if row["row_id"] == "api-row")["prd_status"], "PRD Reviewed")
        lookup = f"{base}/focus-area?bpmis_project_id=123456"
        self.assertTrue(self.client.get(lookup).get_json()["has_active_pipeline_row"])
        self.login("sophia.wangzj@npt.sg")
        self.assertEqual(self.client.get(lookup).status_code, 200)
        self.assertEqual(self.client.post(f"{base}/cell", json={
            "scope": "pipeline", "row_id": "api-row", "field": "focus_area", "value": "No",
        }).status_code, 403)
        self.login("xiaodong.zheng@npt.sg")
        self.assertEqual(self.client.post(f"{base}/rows", json={
            "scope": "pipeline", "action": "delete", "row_id": "api-row",
        }).status_code, 200)
        saved = self.client.get(lookup).get_json()
        self.assertEqual(saved["focus_area"], "Yes")
        self.assertFalse(saved["has_active_pipeline_row"])
        self.assertTrue(saved["updated_at"])
        self.assertEqual(self.client.get(f"{base}/focus-area?bpmis_project_id=654321").status_code, 404)
        self.assertEqual(self.client.get(f"{base}/focus-area?bpmis_project_id=bad").status_code, 400)
        self.assertEqual(self.client.get(f"{base}/focus-area").status_code, 400)
        self.login("unknown@example.com")
        self.assertEqual(self.client.get(lookup).status_code, 403)

    def test_duplicate_and_stale_requests_do_not_overwrite_active_row(self):
        base = "/api/team-dashboard/version-plan/af"
        response = self.client.post(f"{base}/rows", json={
            "scope": "pipeline", "action": "add", "row_id": "api-row", "bpmis_project_id": "123456",
        })
        revision = response.get_json()["document_revision"]
        self.assertEqual(self.client.post(f"{base}/rows", json={
            "scope": "pipeline", "action": "add", "bpmis_project_id": "123456",
        }).status_code, 400)
        self.assertEqual(self.client.post(f"{base}/cell", json={
            "scope": "pipeline", "row_id": "api-row", "field": "focus_area", "value": "Yes",
        }).status_code, 200)
        response = self.client.post(f"{base}/cell", json={
            "scope": "pipeline", "row_id": "api-row", "field": "focus_area", "value": "No", "document_revision": revision,
        })
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.client.get(f"{base}/focus-area?bpmis_project_id=123456").get_json()["focus_area"], "Yes")
