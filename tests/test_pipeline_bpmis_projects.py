from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from bpmis_jira_tool.bpmis import BPMISDirectApiClient
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError
from bpmis_jira_tool.local_agent_client import RemoteBPMISClient
from bpmis_jira_tool.team_dashboard_version_plan import (
    merge_version_plan_editable_state,
    normalize_version_plan_state,
    suggest_pipeline_bpmis_projects,
    update_version_plan_cell,
    update_version_plan_rows,
)
from bpmis_jira_tool.web import create_app


class PipelineBPMISProjectTests(unittest.TestCase):
    def test_id_validation_and_persistence_across_move_and_sync(self):
        config = {"version_plan": {"af": {
            "pipeline_rows": [{"row_id": "pipeline-1", "feature": "[SG] Money Lock"}],
            "bundles": {"af-1": {"manual_rows": []}},
        }}}
        edit = {"scope": "pipeline", "row_id": "pipeline-1", "field": "bpmis_project_id"}
        saved = update_version_plan_cell(config, {**edit, "value": "001234"})
        for invalid in ("12345", "1234567", "12345x", "１２３４５６"):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "exactly 6 digits"):
                update_version_plan_cell(saved, {**edit, "value": invalid})
        moved = update_version_plan_rows(saved, {
            "action": "move", "row_id": "pipeline-1", "source_scope": "pipeline",
            "target_scope": "bundle", "target_version_id": "af-1",
        })
        merged = merge_version_plan_editable_state(config, moved)
        self.assertEqual(merged["version_plan"]["af"]["bundles"]["af-1"]["manual_rows"][0]["bpmis_project_id"], "001234")
        with self.assertRaisesRegex(ValueError, "only be edited in Pipeline"):
            update_version_plan_cell(merged, {**edit, "scope": "bundle", "version_id": "af-1", "value": "123456"})
        returned = update_version_plan_rows(merged, {
            "action": "move", "row_id": "pipeline-1", "source_scope": "bundle", "source_version_id": "af-1",
            "target_scope": "pipeline",
        })
        self.assertEqual(returned["version_plan"]["af"]["pipeline_rows"][0]["bpmis_project_id"], "001234")
        cleared = update_version_plan_cell(returned, {**edit, "value": ""})
        self.assertEqual(cleared["version_plan"]["af"]["pipeline_rows"][0]["bpmis_project_id"], "")

    def test_fuzzy_ranking_excludes_completed_unknown_and_wrong_market_projects(self):
        projects = [
            {"bpmis_id": "123456", "project_name": "Money Lock Fixed Deposit", "status": "Draft", "market": "SG"},
            {"bpmis_id": "123457", "project_name": "Fixed Deposit Money Lock Enhancements", "status": "KIV", "market": "SG"},
            {"bpmis_id": "123458", "project_name": "Money Lock Fixed Deposit", "status": "Done", "market": "SG"},
            {"bpmis_id": "123459", "project_name": "Money Lock Fixed Deposit", "status": "Closed", "market": "SG"},
            {"bpmis_id": "123460", "project_name": "[PH] Money Lock Fixed Deposit", "status": "Confirmed"},
            {"bpmis_id": "123461", "project_name": "Money Lock Fixed Deposit", "status": ""},
            {"bpmis_id": "123462", "project_name": "Completely unrelated", "status": "UAT"},
        ]
        suggestions = suggest_pipeline_bpmis_projects("[SG] Money Lok - Fixed Deposit", projects)
        self.assertEqual([item["bpmis_id"] for item in suggestions], ["123456", "123457"])
        self.assertGreater(suggestions[0]["match_score"], 0.9)
        self.assertEqual(suggest_pipeline_bpmis_projects("[SG]", projects), [])

    def test_bpmis_lookup_paginates_and_queries_every_unfinished_status(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        client = BPMISDirectApiClient(Settings(
            flask_secret_key="test-secret", google_oauth_client_secret_file=Path(temp_dir.name) / "client.json",
            google_oauth_redirect_uri=None, team_portal_host="127.0.0.1", team_portal_port=5000,
            team_portal_base_url=None, team_allowed_emails=(), team_allowed_email_domains=(),
            team_portal_data_dir=Path(temp_dir.name), spreadsheet_id="", common_tab_name="", input_tab_name="",
            bpmis_base_url="https://example.com", bpmis_api_access_token="test-token",
        ))
        client._resolve_biz_project_status_id = Mock(side_effect=[1, 6])
        first_page = [{"id": 100000 + i, "summary": f"Project {i}", "statusId": 4} for i in range(200)]
        client._api_request = Mock(side_effect=[
            {"data": {"rows": first_page}},
            {"data": {"rows": [
                {"id": 222222, "summary": "Draft Project", "statusId": 1},
                {"id": 333333, "summary": "KIV Project", "statusId": 6},
                {"id": 444444, "summary": "Done Project", "statusId": "Done"},
            ]}},
        ])
        projects = client.list_unfinished_biz_projects()
        self.assertEqual(len(projects), 202)
        self.assertEqual(projects[-2]["status"], "Draft")
        self.assertEqual(projects[-1]["status"], "KIV")
        for page, call in enumerate(client._api_request.call_args_list, 1):
            query = json.loads(call.kwargs["params"]["search"])
            self.assertEqual(query["page"], page)
            self.assertEqual(query["subQueries"][0], {"typeId": [1]})
            self.assertEqual(set(query["subQueries"][1]["statusId"]), {1, 4, 6, 10, 11, 12, 23})
        client._api_request.reset_mock(side_effect=True)
        client._api_request.return_value = {"data": {"rows": [{"id": 1234, "summary": "BPMIS canonical name", "statusId": "Done"}]}}
        self.assertEqual(client.get_biz_project("001234"), {"bpmis_id": "001234", "project_name": "BPMIS canonical name"})
        query = json.loads(client._api_request.call_args.kwargs["params"]["search"])
        self.assertEqual(query["subQueries"], [{"id": [1234]}, {"typeId": [1]}])
        client._api_request.return_value = {"data": {"rows": []}}
        with self.assertRaisesRegex(BPMISError, "was not found"):
            client.get_biz_project("123456")
        proxy = Mock()
        proxy.bpmis_call.return_value = [{"bpmis_id": "222222"}]
        remote = RemoteBPMISClient(proxy, access_token="test-token")
        self.assertEqual(remote.list_unfinished_biz_projects(), [{"bpmis_id": "222222"}])
        proxy.bpmis_call.assert_called_once_with(operation="list_unfinished_biz_projects", access_token="test-token", args=[], kwargs={})
        proxy.bpmis_call.return_value = {"bpmis_id": "222222", "project_name": "Draft Project"}
        self.assertEqual(remote.get_biz_project("222222")["project_name"], "Draft Project")
        proxy.bpmis_call.assert_called_with(operation="get_biz_project", access_token="test-token", args=["222222"], kwargs={})

    def test_suggestion_endpoint_uses_saved_pipeline_name_and_requires_admin(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {
            "ENV_FILE": os.devnull, "FLASK_SECRET_KEY": "test-secret",
            "TEAM_PORTAL_DATA_DIR": temp_dir, "TEAM_ALLOWED_EMAILS": "",
            "TEAM_ALLOWED_EMAIL_DOMAINS": "", "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
        }, clear=True):
            app = create_app()
            app.testing = True
            store = app.config["TEAM_DASHBOARD_CONFIG_STORE"]
            config = store.load()
            config["version_plan"] = normalize_version_plan_state({"af": {"pipeline_rows": [
                {"row_id": "pipeline-1", "feature": "[SG] Money Lock"},
            ]}})
            store.save(config)
            bpmis = Mock()
            bpmis.list_unfinished_biz_projects.return_value = [
                {"bpmis_id": "123456", "project_name": "Money Lock", "status": "Confirmed", "market": "SG"},
            ]
            bpmis.get_biz_project.return_value = {"bpmis_id": "123456", "project_name": "[SG] Canonical Money Lock"}
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=bpmis), app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                    session["google_credentials"] = {"token": "x"}
                url = "/api/team-dashboard/version-plan/af/project-suggestions"
                response = client.post(url, json={"row_id": "pipeline-1", "feature": "Ignored user input"})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json["feature"], "[SG] Money Lock")
                self.assertEqual(response.json["suggestions"][0]["bpmis_id"], "123456")
                self.assertEqual(store.load()["version_plan"]["af"]["pipeline_rows"][0]["bpmis_project_id"], "")
                self.assertEqual(client.post(url, json={"row_id": "missing"}).status_code, 400)
                cell_url = "/api/team-dashboard/version-plan/af/cell"
                edit = {"scope": "pipeline", "row_id": "pipeline-1", "field": "bpmis_project_id"}
                saved = client.post(cell_url, json={**edit, "value": "123456"})
                self.assertEqual(saved.status_code, 200)
                self.assertEqual(saved.json["pipeline_rows"][0]["feature"], "[SG] Canonical Money Lock")
                self.assertEqual(saved.json["pipeline_rows"][0]["bpmis_project_name"], "[SG] Canonical Money Lock")
                bpmis.get_biz_project.assert_called_once_with("123456")
                bpmis.get_biz_project.side_effect = BPMISError("BPMIS Biz Project was not found for this ID.")
                failed_lookup = client.post(cell_url, json={**edit, "value": "999999"})
                row = failed_lookup.json["pipeline_rows"][0]
                self.assertEqual(row["bpmis_project_id"], "999999")
                self.assertEqual(row["bpmis_project_name"], "")
                self.assertEqual(row["feature"], "[SG] Canonical Money Lock")
                self.assertIn("was not found", row["bpmis_project_lookup_error"])
                cleared = client.post(cell_url, json={**edit, "value": ""})
                self.assertEqual(cleared.json["pipeline_rows"][0]["bpmis_project_lookup_error"], "")
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "jireh.tanyx@npt.sg", "name": "Jireh"}
                self.assertEqual(client.post(url, json={"row_id": "pipeline-1"}).status_code, 403)
                bpmis.list_unfinished_biz_projects.assert_called_once()
