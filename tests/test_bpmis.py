import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.bpmis import BPMISDirectApiClient
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError
from bpmis_jira_tool.models import ProjectMatch


class BPMISClientTests(unittest.TestCase):
    class _FakeResponse:
        def __init__(self, status_code, payload=None, text=""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or (json.dumps(payload) if payload is not None else "")

        def json(self):
            if self._payload is None:
                raise ValueError("no json")
            return self._payload

    def _settings(self, temp_dir: str) -> Settings:
        return Settings(
            flask_secret_key="secret",
            google_oauth_client_secret_file=Path(temp_dir) / "client.json",
            google_oauth_redirect_uri=None,
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url=None,
            team_allowed_emails=(),
            team_allowed_email_domains=(),
            team_portal_data_dir=Path(temp_dir),
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Input",
            bpmis_base_url="https://example.com",
            bpmis_api_access_token="token",
        )

    def test_team_dashboard_task_lookup_batches_users_and_caches_parent_detail(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            calls: list[tuple[str, dict[str, object] | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                if path == "/api/v1/users/listByEmail":
                    self.assertEqual(json.loads(params["search"]), ["af@npt.sg", "pm@npt.sg"])
                    return {
                        "data": [
                            {"id": 101, "email": "af@npt.sg"},
                            {"id": 202, "email": "pm@npt.sg"},
                        ]
                    }
                if path == "/api/v1/issues/list":
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 1,
                                    "jiraKey": "AF-1",
                                    "summary": "First task",
                                    "reporter": {"id": 101},
                                    "parentIds": [{"id": 900}],
                                },
                                {
                                    "id": 2,
                                    "jiraKey": "AF-2",
                                    "summary": "Second task",
                                    "jiraRegionalPmPicId": [{"id": 202}],
                                    "parentIds": [{"id": 900}],
                                },
                            ]
                        }
                    }
                if path == "/api/v1/issues/detail":
                    self.assertEqual(params["id"], "900")
                    return {"data": {"id": 900, "typeId": "Biz Project", "summary": "Parent Project", "market": "SG"}}
                self.fail(f"unexpected API call: {path}")

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["af@npt.sg", "pm@npt.sg"])

        self.assertEqual([task["jira_id"] for task in tasks], ["AF-1", "AF-2"])
        self.assertEqual([task["pm_email"] for task in tasks], ["af@npt.sg", "pm@npt.sg"])
        self.assertEqual(tasks[0]["parent_project"]["project_name"], "Parent Project")
        self.assertEqual([path for path, _params in calls].count("/api/v1/users/listByEmail"), 1)
        self.assertEqual([path for path, _params in calls].count("/api/v1/issues/detail"), 1)

    def test_api_request_logs_timing_and_payload_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            class FakeSession:
                headers = {}

                def request(self, **kwargs):
                    return BPMISClientTests._FakeResponse(
                        200,
                        {
                            "code": 0,
                            "data": {
                                "rows": [{"id": 1}, {"id": 2}],
                                "total": 2,
                            },
                        },
                    )

            client.session = FakeSession()  # type: ignore[assignment]

            with self.assertLogs("bpmis_jira_tool.bpmis", level="INFO") as captured:
                payload = client._api_request(
                    "/api/v1/issues/list",
                    params={
                        "search": json.dumps(
                            {
                                "joinType": "and",
                                "subQueries": [{"creator": [101]}],
                                "page": 1,
                                "pageSize": 200,
                                "mapping": True,
                            }
                        )
                    },
                )

        self.assertEqual(payload["data"]["total"], 2)
        log_text = "\n".join(captured.output)
        self.assertIn('"event": "bpmis_api_request_done"', log_text)
        self.assertIn('"path": "/api/v1/issues/list"', log_text)
        self.assertIn('"row_count": 2', log_text)
        self.assertIn('"subquery_keys": [["creator"]]', log_text)

    def test_build_create_payload_supports_multiple_components_from_comma_separated_value(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {"name": "Market"},
                "taskType": {"name": "Task Type"},
                "componentId": {"name": "Component"},
            }
            resolved_calls: list[tuple[str, str, int | None]] = []

            def fake_resolve(field_def, raw_value, match_value=None):
                resolved_calls.append((field_def.get("name", ""), raw_value, match_value))
                if field_def.get("name") == "Market":
                    return 101
                if field_def.get("name") == "Task Type":
                    return 202
                if field_def.get("name") == "Component":
                    return {
                        "FE-Anti-fraud": 301,
                        "FE-User": 302,
                    }[raw_value]
                return 999

            client._resolve_option_value = fake_resolve  # type: ignore[method-assign]
            client._resolve_jira_user_id = lambda query: 999  # type: ignore[method-assign]
            client._resolve_fix_versions = lambda market_id, raw_value: [777]  # type: ignore[method-assign]

            payload = client._build_create_payload(
                ProjectMatch(project_id="12345"),
                {
                    "Market": "SG",
                    "Task Type": "Feature",
                    "Summary": "Frontend split component test",
                    "System": "FE",
                    "Component": "FE-Anti-fraud,FE-User",
                },
            )

            self.assertEqual(payload["componentId"], [301, 302])
            component_calls = [call for call in resolved_calls if call[0] == "Component"]
            self.assertEqual(
                component_calls,
                [
                    ("Component", "FE-Anti-fraud", 101),
                    ("Component", "FE-User", 101),
                ],
            )

    def test_build_create_payload_maps_description_to_desc(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {},
                "taskType": {},
            }
            client._resolve_option_value = lambda field_def, raw_value, match_value=None: 101  # type: ignore[method-assign]
            client._resolve_jira_user_id = lambda query: 999  # type: ignore[method-assign]
            client._resolve_fix_versions = lambda market_id, raw_value: [777]  # type: ignore[method-assign]

            payload = client._build_create_payload(
                ProjectMatch(project_id="12345"),
                {
                    "Market": "SG",
                    "Task Type": "Feature",
                    "Summary": "Fraud rule improvement",
                    "Description": "Detailed Jira description",
                    "System": "AF",
                },
            )

            self.assertEqual(payload["desc"], "Detailed Jira description")
            self.assertEqual(payload["summary"], "[Feature][AF] Fraud rule improvement")

    def test_build_create_payload_uses_productization_prefix_for_regional_market(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {},
                "taskType": {},
            }
            client._resolve_option_value = lambda field_def, raw_value, match_value=None: 101  # type: ignore[method-assign]
            client._resolve_jira_user_id = lambda query: 999  # type: ignore[method-assign]
            client._resolve_fix_versions = lambda market_id, raw_value: [777]  # type: ignore[method-assign]

            payload = client._build_create_payload(
                ProjectMatch(project_id="12345"),
                {
                    "Market": "Regional",
                    "Task Type": "Feature",
                    "Summary": "Fraud rule improvement",
                    "System": "AF",
                },
            )

            self.assertEqual(payload["summary"], "[Feature][Productization] Fraud rule improvement")

    def test_build_create_payload_does_not_duplicate_existing_prefix(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {},
                "taskType": {},
            }
            client._resolve_option_value = lambda field_def, raw_value, match_value=None: 101  # type: ignore[method-assign]
            client._resolve_jira_user_id = lambda query: 999  # type: ignore[method-assign]
            client._resolve_fix_versions = lambda market_id, raw_value: [777]  # type: ignore[method-assign]

            payload = client._build_create_payload(
                ProjectMatch(project_id="12345"),
                {
                    "Market": "SG",
                    "Task Type": "Feature",
                    "Summary": "[Feature][AF] Fraud rule improvement",
                    "System": "AF",
                },
            )

            self.assertEqual(payload["summary"], "[Feature][AF] Fraud rule improvement")

    def test_build_create_payload_does_not_duplicate_existing_system_only_prefix(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {},
                "taskType": {},
            }
            client._resolve_option_value = lambda field_def, raw_value, match_value=None: 101  # type: ignore[method-assign]
            client._resolve_jira_user_id = lambda query: 999  # type: ignore[method-assign]
            client._resolve_fix_versions = lambda market_id, raw_value: [777]  # type: ignore[method-assign]

            payload = client._build_create_payload(
                ProjectMatch(project_id="12345"),
                {
                    "Market": "SG",
                    "Task Type": "Feature",
                    "Summary": "[AF] Fraud rule improvement",
                    "System": "AF",
                },
            )

            self.assertEqual(payload["summary"], "[Feature][AF] Fraud rule improvement")

    def test_build_create_payload_does_not_duplicate_existing_productization_only_prefix(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {},
                "taskType": {},
            }
            client._resolve_option_value = lambda field_def, raw_value, match_value=None: 101  # type: ignore[method-assign]
            client._resolve_jira_user_id = lambda query: 999  # type: ignore[method-assign]
            client._resolve_fix_versions = lambda market_id, raw_value: [777]  # type: ignore[method-assign]

            payload = client._build_create_payload(
                ProjectMatch(project_id="12345"),
                {
                    "Market": "Regional",
                    "Task Type": "Feature",
                    "Summary": "[Productization] Fraud rule improvement",
                    "System": "AF",
                },
            )

            self.assertEqual(payload["summary"], "[Feature][Productization] Fraud rule improvement")

    def test_build_create_payload_normalizes_mixed_existing_prefix_variants(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {},
                "taskType": {},
            }
            client._resolve_option_value = lambda field_def, raw_value, match_value=None: 101  # type: ignore[method-assign]
            client._resolve_jira_user_id = lambda query: 999  # type: ignore[method-assign]
            client._resolve_fix_versions = lambda market_id, raw_value: [777]  # type: ignore[method-assign]

            payload = client._build_create_payload(
                ProjectMatch(project_id="12345"),
                {
                    "Market": "SG",
                    "Task Type": "Feature",
                    "Summary": "[AF]-[Feature]: Fraud rule improvement",
                    "System": "AF",
                },
            )

            self.assertEqual(payload["summary"], "[Feature][AF] Fraud rule improvement")

    def test_build_create_payload_normalizes_productization_prefix_variants(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {},
                "taskType": {},
            }
            client._resolve_option_value = lambda field_def, raw_value, match_value=None: 101  # type: ignore[method-assign]
            client._resolve_jira_user_id = lambda query: 999  # type: ignore[method-assign]
            client._resolve_fix_versions = lambda market_id, raw_value: [777]  # type: ignore[method-assign]

            payload = client._build_create_payload(
                ProjectMatch(project_id="12345"),
                {
                    "Market": "Regional",
                    "Task Type": "Feature",
                    "Summary": "[Productization]|[Feature] Fraud rule improvement",
                    "System": "AF",
                },
            )

            self.assertEqual(payload["summary"], "[Feature][Productization] Fraud rule improvement")

    def test_sync_query_scopes_to_biz_projects_and_allowed_statuses_before_or_pm_match(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, dict | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                if path == "/api/v1/users/listByEmail":
                    return {"data": [{"id": 123}]}
                if path == "/api/v1/issues/list":
                    search_payload = json.loads(params["search"])
                    subqueries = search_payload.get("subQueries") or []
                    if subqueries and subqueries[0].get("id") == [200]:
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": 200,
                                        "summary": "Enriched Biz Project",
                                        "marketId": "PH",
                                        "bizPriorityId": "P1",
                                        "regionalPmPicId": [{"name": "PM Lead", "email": "pm@npt.sg"}],
                                        "statusId": "Confirmed",
                                    }
                                ]
                            }
                        }
                    if subqueries and subqueries[0].get("typeId") == [BPMISDirectApiClient.BIZ_PROJECT_TYPE_ID]:
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": 100,
                                        "summary": "Draft Project",
                                        "marketId": "SG",
                                        "bizPriorityId": "P2",
                                        "regionalPmPicId": [{"email": "pm@npt.sg"}],
                                        "statusId": "Draft",
                                    },
                                    {
                                        "id": 200,
                                        "summary": "Enriched Biz Project",
                                        "marketId": "PH",
                                        "statusId": "Confirmed",
                                    },
                                ]
                            }
                        }
                    return {"data": {"rows": []}}
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]

            projects = client.list_biz_projects_for_pm_email("pm@npt.sg")

            self.assertEqual(calls[1][0], "/api/v1/issues/list")
            search_payload = json.loads(calls[1][1]["search"])
            self.assertEqual(search_payload["joinType"], "and")
            self.assertEqual(
                search_payload["subQueries"],
                [
                    {"typeId": [BPMISDirectApiClient.BIZ_PROJECT_TYPE_ID]},
                    {"statusId": [4, 23, 10, 11, 12]},
                    {
                        "joinType": "or",
                        "subQueries": [
                            {"regionalPmPicId": [123]},
                            {"involvedPM": [123]},
                        ],
                    },
                ],
            )
            self.assertEqual(
                projects,
                [
                    {
                        "issue_id": "200",
                        "bpmis_id": "200",
                        "project_name": "Enriched Biz Project",
                        "market": "PH",
                        "priority": "P1",
                        "regional_pm_pic": "PM Lead",
                        "status": "Confirmed",
                    }
                ],
            )

    def test_search_biz_projects_by_title_keywords_uses_keyword_and_allowed_statuses(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            calls: list[tuple[str, dict | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                if path == "/api/v1/issues/list":
                    search_payload = json.loads(params["search"])
                    if (search_payload.get("subQueries") or [{}])[0].get("id") == [225159]:
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": 225159,
                                        "summary": "Fraud Alert Revamp",
                                        "marketId": "SG",
                                        "bizPriorityId": "P1",
                                        "statusId": "Confirmed",
                                    }
                                ]
                            }
                        }
                    self.assertEqual(search_payload["keyword"], "Fraud Alert Revamp")
                    self.assertEqual(search_payload["subQueries"][0], {"typeId": [BPMISDirectApiClient.BIZ_PROJECT_TYPE_ID]})
                    self.assertEqual(search_payload["subQueries"][1], {"statusId": [4, 23, 10, 11, 12]})
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 225159,
                                    "summary": "Fraud Alert Revamp",
                                    "marketId": "SG",
                                    "bizPriorityId": "P1",
                                    "statusId": "Confirmed",
                                }
                            ]
                        }
                    }
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]

            projects = client.search_biz_projects_by_title_keywords(" Fraud   Alert Revamp ", max_pages=1)

        self.assertEqual(calls[0][0], "/api/v1/issues/list")
        self.assertEqual(projects[0]["bpmis_id"], "225159")
        self.assertEqual(projects[0]["project_name"], "Fraud Alert Revamp")

    def test_single_brd_doc_link_returns_link_only_when_exactly_one_brd_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, dict | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                return {"data": {"rows": [{"id": 9001, "link": "https://docs/brd-1", "parentIds": [12345]}]}}

            client._api_request = fake_api_request  # type: ignore[method-assign]

            link = client.get_single_brd_doc_link_for_project("12345")

            self.assertEqual(link, "https://docs/brd-1")
            search_payload = json.loads(calls[0][1]["search"])
            self.assertEqual(search_payload["joinType"], "and")
            self.assertEqual(
                search_payload["subQueries"],
                [
                    {"typeId": [BPMISDirectApiClient.BRD_TYPE_ID]},
                    {"parentIds": [12345]},
                ],
            )

    def test_single_brd_doc_link_returns_blank_when_multiple_brds_exist(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)

            def fake_api_request(path, method="GET", params=None, body=None):
                return {
                    "data": {
                        "rows": [
                            {"id": 9001, "link": "https://docs/brd-1"},
                            {"id": 9002, "link": "https://docs/brd-2"},
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            link = client.get_single_brd_doc_link_for_project("12345")

            self.assertEqual(link, "")

    def test_batch_brd_doc_links_groups_results_by_parent_issue(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, dict | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                return {
                    "data": {
                        "rows": [
                            {"id": 9001, "link": "https://docs/brd-1", "parentIds": [11111]},
                            {"id": 9002, "link": "https://docs/brd-2", "parentIds": [22222]},
                            {"id": 9003, "link": "https://docs/brd-3", "parentIds": [22222]},
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            links = client.get_single_brd_doc_links_for_projects(["11111", "22222"])

            self.assertEqual(links, {"11111": "https://docs/brd-1", "22222": ""})
            search_payload = json.loads(calls[0][1]["search"])
            self.assertEqual(
                search_payload["subQueries"],
                [
                    {"typeId": [BPMISDirectApiClient.BRD_TYPE_ID]},
                    {"parentIds": [11111, 22222]},
                ],
            )

    def test_batch_brd_doc_links_supports_parent_ids_as_objects(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)

            def fake_api_request(path, method="GET", params=None, body=None):
                return {
                    "data": {
                        "rows": [
                            {"id": 9001, "link": "https://docs/brd-1", "parentIds": [{"id": 11111}]},
                            {"id": 9002, "link": "https://docs/brd-2", "parentIds": [{"id": 22222}]},
                            {"id": 9003, "link": "https://docs/brd-3", "parentIds": [{"id": 22222}]},
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            links = client.get_single_brd_doc_links_for_projects(["11111", "22222"])

            self.assertEqual(links, {"11111": "https://docs/brd-1", "22222": ""})

    def test_search_versions_uses_versions_list_and_filters_by_contains_match(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, dict | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                return {
                    "data": {
                        "rows": [
                            {"id": 22, "fullName": "Planning_26Q2", "marketId": {"label": "SG"}},
                            {"id": 23, "fullName": "Hotfix_26Q2", "marketId": {"label": "PH"}},
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            rows = client.search_versions("26Q2")

            self.assertEqual(calls[0][0], "/api/v1/versions/list")
            search_payload = json.loads(calls[0][1]["search"])
            self.assertEqual(search_payload["name"], "26Q2")
            self.assertEqual({row["id"] for row in rows}, {22, 23})

    def test_list_issues_for_version_uses_selected_version_id_and_enriches_missing_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, dict | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                if path == "/api/v1/issues/list":
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 9001,
                                    "summary": "Upgrade wallet flow",
                                    "jiraLink": "https://jira.shopee.io/browse/ABC-101",
                                }
                            ]
                        }
                    }
                if path == "/api/v1/issues/detail":
                    return {
                        "data": {
                            "row": {
                                "id": 9001,
                                "desc": "Add wallet rollback handling.\nSupport new repayment path.",
                                "jiraRegionalPmPicId": [{"displayName": "Alice PM"}],
                                "jiraPrdLink": "https://confluence/prd-1",
                            }
                        }
                    }
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]

            rows = client.list_issues_for_version("321")

            self.assertEqual(calls[0][0], "/api/v1/issues/list")
            search_payload = json.loads(calls[0][1]["search"])
            self.assertEqual(
                search_payload["subQueries"],
                [
                    {"typeId": [BPMISDirectApiClient.TASK_TYPE_ID]},
                    {"fixVersionId": [321]},
                ],
            )
            self.assertEqual(rows[0]["desc"], "Add wallet rollback handling.\nSupport new repayment path.")
            self.assertEqual(rows[0]["jiraPrdLink"], "https://confluence/prd-1")
            self.assertEqual(rows[0]["jiraRegionalPmPicId"][0]["displayName"], "Alice PM")

    def test_get_jira_ticket_detail_uses_jira_key_detail_lookup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, dict | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                return {
                    "data": {
                        "row": {
                            "jiraKey": "AF-101",
                            "summary": "Live AF task",
                            "status": {"label": "In Progress"},
                            "fixVersionId": [{"fullName": "Planning_26Q2"}],
                        }
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            detail = client.get_jira_ticket_detail("https://jira.shopee.io/browse/AF-101")

            self.assertEqual(calls[0][0], "/api/v1/issues/detail")
            self.assertEqual(calls[0][1]["jiraKey"], "AF-101")
            self.assertEqual(detail["summary"], "Live AF task")
            self.assertEqual(detail["fixVersionId"][0]["fullName"], "Planning_26Q2")

    def test_row_matches_jira_key_ignores_rows_without_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)

            self.assertFalse(client._row_matches_jira_key({"summary": "No Jira key"}, "AF-101"))
            self.assertFalse(client._row_matches_jira_key({"jiraKey": ""}, "AF-101"))
            self.assertTrue(client._row_matches_jira_key({"jiraKey": "AF-101"}, "af-101"))

    def test_update_jira_ticket_status_posts_resolved_workflow_status(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, str, dict | None, dict | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, method, params, body))
                if path == "/api/v1/issues/detail":
                    return {
                        "data": {
                            "row": {
                                "id": 9001,
                                "jiraKey": "AF-101",
                                "summary": "Live AF task",
                                "status": {"label": "Testing"},
                            }
                        }
                    }
                if path == "/api/v1/issueField/list":
                    return {"data": {"statusId": {"key": "statusId", "optionGroup": "jiraStatus"}}}
                if path == "/api/v1/options/getGroupOptions":
                    return {"data": {"jiraStatus": [{"label": "Testing", "value": 44}]}}
                if path == "/api/v1/issues/updateStatus":
                    return {"data": {"ok": True}}
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]

            detail = client.update_jira_ticket_status("https://jira.shopee.io/browse/AF-101", "Testing")

            update_call = next(call for call in calls if call[0] == "/api/v1/issues/updateStatus")
            self.assertEqual(update_call[1], "POST")
            self.assertEqual(update_call[3], {"jiraKey": "AF-101", "statusId": 44})
            self.assertEqual(detail["status"]["label"], "Testing")

    def test_delink_jira_ticket_from_project_clears_parent_issue(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, str, dict | None, dict | None]] = []
            state = {"linked": True}

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, method, params, body))
                if path == "/api/v1/issues/detail":
                    return {
                        "data": {
                            "row": {
                                "id": 9001,
                                "jiraKey": "AF-101",
                                "summary": "Live AF task",
                                "parentIds": [225159] if state["linked"] else [],
                            }
                        }
                    }
                if path == "/api/v1/issues/list":
                    return {"data": {"rows": [{"id": 9001, "jiraKey": "AF-101"}] if state["linked"] else []}}
                if path == "/api/v1/issues/removeTask/9001":
                    self.assertEqual(method, "DELETE")
                    state["linked"] = False
                    return {"data": {"ok": True}}
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]

            detail = client.delink_jira_ticket_from_project("AF-101", "225159")

            update_call = next(call for call in calls if call[0] == "/api/v1/issues/removeTask/9001")
            self.assertEqual(update_call[1], "DELETE")
            self.assertEqual(detail["parentIds"], [])

    def test_link_jira_ticket_to_project_links_existing_ticket_and_verifies_parent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, str, dict | None, object | None]] = []
            state = {"linked": False}

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, method, params, body))
                if path == "/api/v1/issues/detail":
                    return {
                        "data": {
                            "row": {
                                "id": 9001,
                                "jiraKey": "AF-101",
                                "summary": "Live AF task",
                                "parentIds": [225159] if state["linked"] else [],
                            }
                        }
                    }
                if path == "/api/v1/issues/list":
                    if method == "PUT":
                        self.assertEqual(body["id"], [9001])
                        self.assertEqual(body["parentIds"], [225159])
                        self.assertEqual(body["parentIssueId"], 225159)
                        state["linked"] = True
                        return {"data": {"issuesToUpdate": [{"id": 9001, "parentIds": [225159]}]}}
                    search = json.loads(params["search"])
                    sub_queries = search.get("subQueries") or []
                    if len(sub_queries) > 1 and sub_queries[1] == {"parentIds": [225159]}:
                        return {"data": {"rows": [{"id": 9001, "jiraKey": "AF-101"}] if state["linked"] else []}}
                    if search.get("jiraLink") == "AF-101":
                        self.assertEqual(search.get("typeId"), BPMISDirectApiClient.TASK_TYPE_ID)
                        return {"data": {"rows": [{"id": 9001, "jiraLink": "AF-101", "parentIds": []}]}}
                    return {"data": {"rows": [{"id": 9001, "jiraKey": "AF-101", "parentIds": []}]}}
                if path == "/api/v1/issues/batchCreateJiraIssue":
                    self.fail("Existing Jira link must update the BPMIS task row, not create Jira.")
                    state["linked"] = True
                    return {"data": {"add": [{"jiraLink": "https://jira.shopee.io/browse/AF-101"}]}}
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]
            client._get_jira_ticket_detail_via_jira = lambda _ticket_key: {  # type: ignore[method-assign]
                "jiraKey": "AF-101",
                "summary": "Direct Jira task detail",
                "raw_jira": {"key": "AF-101"},
            }

            detail = client.link_jira_ticket_to_project("AF-101", "225159")

            link_call = next(call for call in calls if call[0] == "/api/v1/issues/list" and call[1] == "PUT")
            self.assertEqual(link_call[3]["id"], [9001])
            self.assertEqual(link_call[3]["parentIds"], [225159])
            self.assertEqual(link_call[3]["parentIssueId"], 225159)
            self.assertEqual(detail["parentIds"], ["225159"])
            self.assertEqual(detail["summary"], "Direct Jira task detail")

    def test_link_jira_ticket_to_project_adds_existing_jira_when_task_row_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, str, dict | None, object | None]] = []
            state = {"linked": False}

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, method, params, body))
                if path == "/api/v1/issues/detail":
                    return {
                        "data": {
                            "row": {
                                "id": 9002,
                                "jiraKey": "AF-102",
                                "summary": "Added AF task",
                                "parentIds": [225159] if state["linked"] else [],
                            }
                        }
                    }
                if path == "/api/v1/issues/list":
                    search = json.loads(params["search"])
                    sub_queries = search.get("subQueries") or []
                    if len(sub_queries) > 1 and sub_queries[1] == {"parentIds": [225159]}:
                        return {"data": {"rows": [{"id": 9002, "jiraKey": "AF-102"}] if state["linked"] else []}}
                    if state["linked"]:
                        return {"data": {"rows": [{"id": 9002, "jiraKey": "AF-102", "parentIds": [225159]}]}}
                    return {"data": {"rows": []}}
                if path == "/api/v1/issues/batchCreateJiraIssue":
                    self.assertEqual(method, "POST")
                    self.assertEqual(body["values"][0]["typeId"], BPMISDirectApiClient.TASK_TYPE_ID)
                    self.assertEqual(body["values"][0]["parentIssueId"], 225159)
                    self.assertEqual(body["values"][0]["jiraLink"], "https://jira.shopee.io/browse/AF-102")
                    state["linked"] = True
                    return {"data": {"add": [{"jiraLink": "https://jira.shopee.io/browse/AF-102"}]}}
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]
            client._get_jira_ticket_detail_via_jira = lambda _ticket_key: {  # type: ignore[method-assign]
                "jiraKey": "AF-102",
                "summary": "Direct Jira task detail",
                "raw_jira": {"key": "AF-102"},
            }

            detail = client.link_jira_ticket_to_project("AF-102", "225159")

            add_call = next(call for call in calls if call[0] == "/api/v1/issues/batchCreateJiraIssue")
            self.assertEqual(add_call[1], "POST")
            self.assertEqual(detail["parentIds"], [225159])
            self.assertEqual(detail["summary"], "Added AF task")

    def test_link_jira_ticket_to_project_finds_existing_task_by_jira_link(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            calls: list[tuple[str, str, dict | None, object | None]] = []
            state = {"linked": False}

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, method, params, body))
                if path == "/api/v1/issues/detail":
                    return {
                        "data": {
                            "row": {
                                "id": 9003,
                                "jiraLink": "AF-103",
                                "summary": "Existing task only has jiraLink",
                                "parentIds": [225159] if state["linked"] else [],
                            }
                        }
                    }
                if path == "/api/v1/issues/list":
                    if method == "PUT":
                        self.assertEqual(body["id"], [9003])
                        state["linked"] = True
                        return {"data": {"issuesToUpdate": [{"id": 9003, "parentIds": [225159]}]}}
                    search = json.loads(params["search"])
                    sub_queries = search.get("subQueries") or []
                    if len(sub_queries) > 1 and sub_queries[1] == {"parentIds": [225159]}:
                        return {"data": {"rows": [{"id": 9003, "jiraLink": "AF-103"}] if state["linked"] else []}}
                    if search.get("jiraLink") == "AF-103":
                        return {"data": {"rows": [{"id": 9003, "jiraLink": "AF-103", "parentIds": [225159] if state["linked"] else []}]}}
                    return {"data": {"rows": []}}
                if path == "/api/v1/issues/batchCreateJiraIssue":
                    self.fail("A globally existing Jira task row should be linked by issue update, not batch-created.")
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]
            client._get_jira_ticket_detail_via_jira = lambda _ticket_key: {  # type: ignore[method-assign]
                "jiraKey": "AF-103",
                "summary": "Direct Jira task detail",
                "raw_jira": {"key": "AF-103"},
            }

            detail = client.link_jira_ticket_to_project("AF-103", "225159")

            self.assertTrue(any(call[0] == "/api/v1/issues/list" and call[1] == "PUT" for call in calls))
            self.assertEqual(detail["parentIds"], [225159])
            self.assertEqual(detail["summary"], "Existing task only has jiraLink")

    def test_get_jira_ticket_detail_uses_direct_jira_api_when_token_configured(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            calls = []

            def fake_request(**kwargs):
                calls.append(kwargs)
                return self._FakeResponse(
                    200,
                    {
                        "id": "10001",
                        "key": "AF-101",
                        "fields": {
                            "summary": "Live AF task",
                            "status": {"name": "Waiting"},
                            "fixVersions": [{"name": "Planning_26Q2"}],
                            "components": [{"name": "Anti-fraud"}],
                        },
                    },
                )

            env = {
                "JIRA_API_TOKEN": "dXNlcjp0b2tlbg==",
                "JIRA_AUTH_SCHEME": "basic",
                "JIRA_BASE_URL": "https://jira.example.test",
                "JIRA_USERNAME": "",
                "JIRA_EMAIL": "",
            }
            with patch.dict(os.environ, env), patch("bpmis_jira_tool.bpmis.requests.request", side_effect=fake_request):
                detail = client.get_jira_ticket_detail("AF-101")

            self.assertEqual(detail["summary"], "Live AF task")
            self.assertEqual(detail["status"]["label"], "Waiting")
            self.assertEqual(detail["fixVersions"], ["Planning_26Q2"])
            self.assertEqual(detail["components"], ["Anti-fraud"])
            self.assertEqual(calls[0]["headers"]["Authorization"], "Basic dXNlcjp0b2tlbg==")

    def test_update_jira_ticket_status_uses_direct_jira_transition(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            calls = []

            def fake_request(**kwargs):
                calls.append(kwargs)
                if kwargs["method"] == "GET" and kwargs["url"].endswith("/transitions"):
                    return self._FakeResponse(
                        200,
                        {"transitions": [{"id": "31", "name": "Close", "to": {"name": "Closed"}}]},
                    )
                if kwargs["method"] == "POST" and kwargs["url"].endswith("/transitions"):
                    return self._FakeResponse(204, None)
                return self._FakeResponse(
                    200,
                    {"id": "10001", "key": "AF-101", "fields": {"summary": "Live AF task", "status": {"name": "Closed"}}},
                )

            env = {
                "JIRA_API_TOKEN": "dXNlcjp0b2tlbg==",
                "JIRA_AUTH_SCHEME": "basic",
                "JIRA_BASE_URL": "https://jira.example.test",
                "JIRA_USERNAME": "",
                "JIRA_EMAIL": "",
            }
            with patch.dict(os.environ, env), patch("bpmis_jira_tool.bpmis.requests.request", side_effect=fake_request):
                detail = client.update_jira_ticket_status("AF-101", "Closed")

            transition_call = next(call for call in calls if call["method"] == "POST")
            self.assertEqual(transition_call["json"], {"transition": {"id": "31"}})
            self.assertEqual(detail["status"]["label"], "Closed")

    def test_update_jira_ticket_status_rejects_unchanged_live_status(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)

            def fake_api_request(path, method="GET", params=None, body=None):
                if path == "/api/v1/issues/detail":
                    return {
                        "data": {
                            "row": {
                                "id": 9001,
                                "jiraKey": "AF-101",
                                "summary": "Live AF task",
                                "status": {"label": "Waiting"},
                            }
                        }
                    }
                if path == "/api/v1/issueField/list":
                    return {"data": {"statusId": {"key": "statusId", "optionGroup": "jiraStatus"}}}
                if path == "/api/v1/options/getGroupOptions":
                    return {"data": {"jiraStatus": [{"label": "Testing", "value": 44}]}}
                if path == "/api/v1/issues/updateStatus":
                    return {"data": {"ok": True}}
                raise BPMISError("not found")

            client._api_request = fake_api_request  # type: ignore[method-assign]

            with self.assertRaisesRegex(BPMISError, "Jira is still 'Waiting'"):
                client.update_jira_ticket_status("AF-101", "Testing")

    def test_update_jira_ticket_status_rejects_unknown_status(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)

            with self.assertRaises(BPMISError):
                client.update_jira_ticket_status("AF-101", "Not a workflow status")

    def test_update_jira_ticket_fix_version_prefers_name_over_bpmis_version_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            calls = []

            def fake_request(**kwargs):
                calls.append(kwargs)
                if kwargs["method"] == "PUT":
                    return self._FakeResponse(204, None)
                return self._FakeResponse(
                    200,
                    {
                        "key": "AF-101",
                        "fields": {
                            "summary": "Live AF task",
                            "fixVersions": [{"name": "Planning_26Q4"}],
                        },
                    },
                )

            env = {
                "JIRA_API_TOKEN": "token",
                "JIRA_AUTH_SCHEME": "bearer",
                "JIRA_BASE_URL": "https://jira.example.test",
            }
            with patch.dict(os.environ, env), patch("bpmis_jira_tool.bpmis.requests.request", side_effect=fake_request):
                detail = client.update_jira_ticket_fix_version("AF-101", "Planning_26Q4", version_id="991")

            update_call = next(call for call in calls if call["method"] == "PUT")
            self.assertEqual(update_call["json"], {"fields": {"fixVersions": [{"name": "Planning_26Q4"}]}})
            self.assertEqual(detail["fixVersions"], ["Planning_26Q4"])

    def test_list_jira_tasks_for_project_created_by_email_filters_and_normalizes_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._resolve_bpmis_user_ids_by_email = lambda email: [14420]  # type: ignore[method-assign]

            def fake_api_request(path, method="GET", params=None, body=None):
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                self.assertEqual(search["subQueries"][1], {"parentIds": [225159]})
                return {
                    "data": {
                        "rows": [
	                            {
	                                "id": 991,
	                                "jiraKey": "AF-991",
	                                "summary": "Creator-only task",
	                                "creator": {"emailAddress": "pm@npt.sg"},
	                                "reporter": {"emailAddress": "other@npt.sg"},
	                                "status": {"label": "Developing"},
	                                "fixVersions": [{"name": "Planning_26Q2"}],
	                                "componentId": {"label": "DBP-Anti-fraud"},
	                                "marketId": {"label": "SG"},
	                                "jiraPrdLink": "https://docs/prd",
	                            },
	                            {
	                                "id": 992,
	                                "jiraKey": "AF-992",
	                                "summary": "[Feature][AF]Existing task",
	                                "reporter": {"emailAddress": "pm@npt.sg"},
	                                "creator": {"emailAddress": "other@npt.sg"},
	                                "fixVersionId": [{"name": "26Q2", "fullName": "Planning_26Q2"}],
	                            },
	                            {
	                                "id": 994,
	                                "jiraKey": "AF-994",
	                                "summary": "BPMIS reporter task",
	                                "reporter": {"emailAddress": "pm@npt.sg"},
	                            },
	                            {
	                                "id": 993,
	                                "jiraKey": "AF-993",
	                                "summary": "Other task",
	                                "creator": {"emailAddress": "other@npt.sg"},
	                                "reporter": {"emailAddress": "other@npt.sg"},
	                            },
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_for_project_created_by_email("225159", "pm@npt.sg")

            self.assertEqual(len(tasks), 2)
            self.assertEqual(tasks[0]["ticket_key"], "AF-992")
            self.assertEqual(tasks[0]["ticket_link"], "https://jira.shopee.io/browse/AF-992")
            self.assertEqual(tasks[0]["jira_title"], "[Feature][AF]Existing task")
            self.assertEqual(tasks[0]["status"], "")
            self.assertEqual(tasks[0]["fix_version_name"], "Planning_26Q2")
            self.assertEqual(tasks[1]["ticket_key"], "AF-994")
            self.assertEqual(tasks[0]["component"], "")
            self.assertEqual(tasks[0]["market"], "")

    def test_list_biz_projects_for_pm_emails_batches_pm_lookup_and_tags_matches(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {  # type: ignore[method-assign]
                "pm1@npt.sg": [101],
                "pm2@npt.sg": [202],
            }
            calls = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                search = json.loads((params or {}).get("search") or "{}")
                self.assertEqual(search["subQueries"][2]["subQueries"][0], {"regionalPmPicId": [101, 202]})
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 100,
                                "summary": "PM1 Project",
                                "marketId": "SG",
                                "bizPriorityId": "P1",
                                "regionalPmPicId": [{"id": 101, "email": "pm1@npt.sg"}],
                                "statusId": "Confirmed",
                            },
                            {
                                "id": 200,
                                "summary": "PM2 Project",
                                "marketId": "ID",
                                "bizPriorityId": "P0",
                                "involvedPM": [{"id": 202, "email": "pm2@npt.sg"}],
                                "statusId": "Developing",
                            },
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            projects = client.list_biz_projects_for_pm_emails(["PM1@npt.sg", "pm2@npt.sg"])

        self.assertEqual(len(calls), 1)
        self.assertEqual([project["bpmis_id"] for project in projects], ["100", "200"])
        self.assertEqual(projects[0]["matched_pm_emails"], ["pm1@npt.sg"])
        self.assertEqual(projects[1]["matched_pm_emails"], ["pm2@npt.sg"])

    def test_list_jira_tasks_for_projects_created_by_emails_batches_parent_lookup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            calls = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                search = json.loads((params or {}).get("search") or "{}")
                self.assertEqual(search["subQueries"][1], {"parentIds": [225159, 225160]})
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 991,
                                "jiraKey": "AF-991",
                                "summary": "Project one task",
                                "reporter": {"emailAddress": "pm@npt.sg"},
                                "parentIds": [225159],
                                "status": {"label": "Waiting"},
                            },
                            {
                                "id": 992,
                                "jiraKey": "AF-992",
                                "summary": "Project two task",
                                "reporter": {"emailAddress": "pm@npt.sg"},
                                "parentIds": [225160],
                                "status": {"label": "Developing"},
                            },
                            {
                                "id": 993,
                                "jiraKey": "AF-993",
                                "summary": "Other PM task",
                                "reporter": {"emailAddress": "other@npt.sg"},
                                "parentIds": [225160],
                            },
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: (_ for _ in ()).throw(AssertionError("detail lookup not expected"))  # type: ignore[method-assign]

            tasks_by_project = client.list_jira_tasks_for_projects_created_by_emails(
                ["225159", "225160"],
                ["pm@npt.sg"],
            )

        self.assertEqual(len(calls), 1)
        self.assertEqual([task["jira_id"] for task in tasks_by_project["225159"]], ["AF-991"])
        self.assertEqual([task["jira_id"] for task in tasks_by_project["225160"]], ["AF-992"])

    def test_list_jira_tasks_created_by_emails_filters_and_normalizes_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {  # type: ignore[method-assign]
                email: {
                    "pm1@npt.sg": [101],
                    "pm2@npt.sg": [202],
                }.get(email, [])
                for email in emails
            }
            calls = []

            def fake_api_request(path, method="GET", params=None, body=None):
                if path == "/api/v1/issues/detail":
                    issue_id = str((params or {}).get("id") or (params or {}).get("issueId") or "")
                    return {
                        "data": {
                            "id": issue_id,
                            "summary": f"Parent Project {issue_id}",
                            "typeId": "Biz Project",
                            "marketId": {"label": "SG"},
                            "bizPriorityId": {"label": "P1"},
                            "regionalPmPicId": [{"emailAddress": "rpm@npt.sg"}],
                        }
                    }
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                if search.get("subQueries") == [{"id": [225159]}]:
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 225159,
                                    "summary": "Parent Project 225159",
                                    "typeId": "Biz Project",
                                    "marketId": {"label": "SG"},
                                    "bizPriorityId": {"label": "P1"},
                                    "regionalPmPicId": [{"emailAddress": "rpm@npt.sg"}],
                                }
                            ]
                        }
                    }
                if search.get("subQueries") == [{"id": [225160]}]:
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 225160,
                                    "summary": "Parent Project 225160",
                                    "typeId": "Biz Project",
                                }
                            ]
                        }
                    }
                calls.append(search)
                self.assertEqual(search["subQueries"][0], {"typeId": [BPMISDirectApiClient.TASK_TYPE_ID]})
                self.assertEqual(
                    search["subQueries"][1],
                    {
                        "joinType": "or",
                        "subQueries": [
                            {"reporter": [101, 202]},
                            {"jiraRegionalPmPicId": [101, 202]},
                        ],
                    },
                )
                if search["page"] == 1:
                    filler_rows = [
                        {
                            "id": 2000 + index,
                            "jiraKey": f"AF-X{index}",
                            "summary": "Filler wrong creator",
                            "creator": {"emailAddress": "other@npt.sg"},
                        }
                        for index in range(198)
                    ]
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 991,
                                    "jiraKey": "AF-991",
                                    "summary": "PRD task",
                                    "reporter": {"emailAddress": "pm1@npt.sg"},
                                    "status": {"label": "PRD Reviewed"},
                                    "fixVersions": [{"name": "Planning_26Q2"}],
                                    "jiraPrdLink": "https://docs/prd-1",
                                    "parentIds": [225159],
                                },
                                {
                                    "id": 992,
                                    "jiraKey": "AF-992",
                                    "summary": "Wrong creator",
                                    "creator": {"emailAddress": "other@npt.sg"},
                                },
                                *filler_rows,
                            ]
                        }
                    }
                return {
                    "data": {
                        "rows": [
                                {
                                    "id": 993,
                                    "jiraKey": "AF-993",
                                    "summary": "Pending task",
                                    "reporter": {"id": 999},
                                    "jiraRegionalPmPicId": [{"id": 202}],
                                    "status": {"label": "Testing"},
                                    "fixVersionId": [{"fullName": "Planning_26Q3"}],
                                    "jiraPrdLink": [{"url": "https://docs/prd-2"}],
                                "parentIds": [{"id": 225160}],
                            }
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["PM1@npt.sg", "pm2@npt.sg", "pm1@npt.sg"])

            self.assertEqual(len(calls), 2)
            self.assertEqual([task["jira_id"] for task in tasks], ["AF-991", "AF-993"])
            self.assertEqual(tasks[0]["pm_email"], "pm1@npt.sg")
            self.assertEqual(tasks[0]["jira_status"], "PRD Reviewed")
            self.assertEqual(tasks[0]["version"], "Planning_26Q2")
            self.assertEqual(tasks[0]["prd_links"], ["https://docs/prd-1"])
            self.assertEqual(tasks[0]["parent_project"]["bpmis_id"], "225159")
            self.assertEqual(tasks[0]["parent_project"]["project_name"], "Parent Project 225159")
            self.assertEqual(tasks[0]["parent_project"]["priority"], "P1")
            self.assertEqual(tasks[0]["parent_project"]["regional_pm_pic"], "rpm@npt.sg")
            self.assertEqual(tasks[1]["pm_email"], "pm2@npt.sg")
            self.assertEqual(tasks[1]["version"], "Planning_26Q3")
            self.assertEqual(tasks[1]["parent_project"]["bpmis_id"], "225160")

    def test_list_jira_tasks_created_by_emails_uses_bulk_live_jira_status_when_available(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm1@npt.sg": [101]}  # type: ignore[method-assign]

            def fake_api_request(path, method="GET", params=None, body=None):
                if path == "/api/v1/issues/detail":
                    issue_id = str((params or {}).get("id") or (params or {}).get("issueId") or "")
                    return {
                        "data": {
                            "id": issue_id,
                            "summary": f"Parent Project {issue_id}",
                            "typeId": "Biz Project",
                            "marketId": {"label": "SG"},
                        }
                    }
                self.assertEqual(path, "/api/v1/issues/list")
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 991,
                                "jiraKey": "SGDB-68363",
                                "summary": "BPMIS stale task title",
                                "reporter": {"id": 101},
                                "status": {"label": "Waiting"},
                                "parentIds": [225159],
                            }
                        ]
                    }
                }

            def fake_request(**kwargs):
                self.assertTrue(kwargs["url"].endswith("/rest/api/2/search"))
                self.assertEqual(kwargs["json"]["fields"], ["summary", "status", "fixVersions", "components"])
                self.assertEqual(kwargs["json"]["jql"], 'key in ("SGDB-68363")')
                return self._FakeResponse(
                    200,
                    {
                        "issues": [
                            {
                                "id": "10001",
                                "key": "SGDB-68363",
                                "fields": {
                                    "summary": "[Feature] AF - DFP upgrade trojan malware detection",
                                    "status": {"name": "Closed"},
                                    "fixVersions": [],
                                    "components": [],
                                },
                            }
                        ],
                    },
                )

            env = {
                "JIRA_API_TOKEN": "dXNlcjp0b2tlbg==",
                "JIRA_AUTH_SCHEME": "basic",
                "JIRA_BASE_URL": "https://jira.example.test",
                "JIRA_USERNAME": "",
                "JIRA_EMAIL": "",
            }
            client._api_request = fake_api_request  # type: ignore[method-assign]
            with patch.dict(os.environ, env), patch("bpmis_jira_tool.bpmis.requests.request", side_effect=fake_request):
                tasks = client.list_jira_tasks_created_by_emails(["pm1@npt.sg"])

            self.assertEqual(tasks[0]["jira_id"], "SGDB-68363")
            self.assertEqual(tasks[0]["issue_id"], "991")
            self.assertEqual(tasks[0]["jira_status"], "Closed")
            self.assertEqual(tasks[0]["jira_title"], "[Feature] AF - DFP upgrade trojan malware detection")
            self.assertEqual(client.request_stats["jira_live_bulk_lookup_count"], 1)
            self.assertEqual(client.request_stats["jira_live_bulk_issue_count"], 1)
            self.assertEqual(client.request_stats["jira_live_detail_lookup_count"], 0)
            self.assertEqual(client.request_stats["jira_live_status_override_count"], 1)

    def test_list_jira_tasks_created_by_emails_bulk_live_jira_chunks_large_loads(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]

            def fake_api_request(path, method="GET", params=None, body=None):
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                page = int(search.get("page") or 1)
                start = 0 if page == 1 else 200
                count = 200 if page == 1 else 20
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 1000 + index,
                                "jiraKey": f"AF-{index}",
                                "summary": f"Stale task {index}",
                                "reporter": {"id": 101},
                                "status": {"label": "Waiting"},
                            }
                            for index in range(start, start + count)
                        ]
                    }
                }

            bulk_jqls: list[str] = []

            def fake_request(**kwargs):
                self.assertTrue(kwargs["url"].endswith("/rest/api/2/search"))
                jql = kwargs["json"]["jql"]
                bulk_jqls.append(jql)
                keys = [item.strip().strip('"') for item in jql.removeprefix("key in (").removesuffix(")").split(",")]
                return self._FakeResponse(
                    200,
                    {
                        "issues": [
                            {
                                "id": str(9000 + index),
                                "key": key,
                                "fields": {
                                    "summary": f"Live {key}",
                                    "status": {"name": "Closed"},
                                    "fixVersions": [],
                                    "components": [],
                                },
                            }
                            for index, key in enumerate(keys)
                        ]
                    },
                )

            env = {
                "JIRA_API_TOKEN": "dXNlcjp0b2tlbg==",
                "JIRA_AUTH_SCHEME": "basic",
                "JIRA_BASE_URL": "https://jira.example.test",
                "JIRA_USERNAME": "",
                "JIRA_EMAIL": "",
            }
            client._api_request = fake_api_request  # type: ignore[method-assign]
            with patch.dict(os.environ, env), patch("bpmis_jira_tool.bpmis.requests.request", side_effect=fake_request):
                tasks = client.list_jira_tasks_created_by_emails(["pm@npt.sg"], enrich_missing_parent=False)

            self.assertEqual(len(tasks), 220)
            self.assertEqual(len(bulk_jqls), 3)
            self.assertEqual(client.request_stats["jira_live_bulk_lookup_count"], 3)
            self.assertEqual(client.request_stats["jira_live_bulk_issue_count"], 220)
            self.assertEqual(client.request_stats["jira_live_detail_lookup_count"], 0)
            self.assertEqual(tasks[0]["jira_status"], "Closed")
            self.assertEqual(tasks[0]["jira_title"], "Live AF-0")

    def test_list_jira_tasks_created_by_emails_falls_back_when_bulk_live_jira_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]

            def fake_api_request(path, method="GET", params=None, body=None):
                self.assertEqual(path, "/api/v1/issues/list")
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 991,
                                "jiraKey": "AF-991",
                                "summary": "Stale task",
                                "reporter": {"id": 101},
                                "status": {"label": "Waiting"},
                            }
                        ]
                    }
                }

            def fake_request(**kwargs):
                if kwargs["url"].endswith("/rest/api/2/search"):
                    return self._FakeResponse(400, {"errorMessages": ["bad jql"]})
                self.assertTrue(kwargs["url"].endswith("/rest/api/2/issue/AF-991"))
                return self._FakeResponse(
                    200,
                    {
                        "id": "10001",
                        "key": "AF-991",
                        "fields": {
                            "summary": "Live fallback task",
                            "status": {"name": "Closed"},
                            "fixVersions": [],
                            "components": [],
                        },
                    },
                )

            env = {
                "JIRA_API_TOKEN": "dXNlcjp0b2tlbg==",
                "JIRA_AUTH_SCHEME": "basic",
                "JIRA_BASE_URL": "https://jira.example.test",
                "JIRA_USERNAME": "",
                "JIRA_EMAIL": "",
            }
            client._api_request = fake_api_request  # type: ignore[method-assign]
            with patch.dict(os.environ, env), patch("bpmis_jira_tool.bpmis.requests.request", side_effect=fake_request):
                tasks = client.list_jira_tasks_created_by_emails(["pm@npt.sg"], enrich_missing_parent=False)

            self.assertEqual(tasks[0]["jira_status"], "Closed")
            self.assertEqual(tasks[0]["jira_title"], "Live fallback task")
            self.assertEqual(client.request_stats["jira_live_bulk_lookup_count"], 1)
            self.assertEqual(client.request_stats["jira_live_bulk_issue_count"], 0)
            self.assertEqual(client.request_stats["jira_live_detail_lookup_count"], 1)

    def test_team_dashboard_jira_lookup_can_cap_pages_and_skip_missing_parent_detail(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {email: [101] for email in emails}  # type: ignore[method-assign]
            calls = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                self.assertEqual(search["page"], 1)
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 1000 + index,
                                "jiraKey": f"AF-{index}",
                                "summary": f"Task {index}",
                                "jiraRegionalPmPicId": [{"id": 101}],
                                "status": {"label": "Testing"},
                            }
                            for index in range(200)
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(
                ["pm@npt.sg"],
                max_pages=1,
                enrich_missing_parent=False,
            )

            self.assertEqual(len(calls), 1)
            self.assertEqual(len(tasks), 200)
            self.assertEqual(client.request_stats["issue_list_page_count"], 1)
            self.assertEqual(client.request_stats["issue_list_page_cap_hit"], 1)
            self.assertEqual(client.request_stats["issue_rows_scanned"], 200)
            self.assertEqual(client.request_stats["issue_detail_lookup_count"], 0)
            self.assertEqual(client.request_stats["issue_detail_enrichment_skipped_count"], 200)

    def test_team_dashboard_jira_lookup_filters_release_after_cutoff(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )

            client = BPMISDirectApiClient(settings)
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {email: [101] for email in emails}  # type: ignore[method-assign]
            searches = []

            def fake_api_request(path, method="GET", params=None, body=None):
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                searches.append(search)
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 991,
                                "jiraKey": "AF-991",
                                "summary": "March task",
                                "reporter": {"id": 101},
                                "fixVersionId": [{"fullName": "Planning_26Q2", "timeline": {"release": "2026-03-01"}}],
                                "status": {"label": "Testing"},
                            },
                            {
                                "id": 992,
                                "jiraKey": "AF-992",
                                "summary": "No release date task",
                                "reporter": {"id": 101},
                                "fixVersionId": [{"fullName": "Planning_TBD"}],
                                "status": {"label": "Testing"},
                            },
                            {
                                "id": 993,
                                "jiraKey": "AF-993",
                                "summary": "February task",
                                "reporter": {"id": 101},
                                "fixVersionId": [{"fullName": "Planning_26Q1", "timeline": [{"label": "Golive", "value": "2026-02-28"}]}],
                                "status": {"label": "Testing"},
                            },
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(
                ["pm@npt.sg"],
                release_after="2026-03-01",
                enrich_missing_parent=False,
            )

            self.assertEqual([task["jira_id"] for task in tasks], ["AF-991", "AF-992"])
            self.assertEqual(len(searches[-1]["subQueries"]), 2)
            self.assertEqual(tasks[0]["release_date"], "2026-03-01")
            self.assertEqual(tasks[1]["release_date"], "")
            self.assertEqual(client.request_stats["bpmis_release_query_filter_probe_count"], 1)
            self.assertEqual(client.request_stats["bpmis_release_query_filter_disabled_count"], 1)
            self.assertEqual(client.request_stats["issue_release_before_cutoff_count"], 1)
            self.assertEqual(client.request_stats["issue_release_missing_included_count"], 1)

    def test_team_dashboard_jira_lookup_can_probe_and_apply_bpmis_release_filter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]
            searches = []

            def fake_api_request(path, method="GET", params=None, body=None):
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                searches.append(search)
                release_query = search.get("subQueries", [])[-1]
                release_text = json.dumps(release_query)
                if "2999-12-31" in release_text:
                    return {"data": {"rows": []}}
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 991,
                                "jiraKey": "AF-991",
                                "summary": "Future task",
                                "reporter": {"id": 101},
                                "fixVersionId": [{"timeline": {"release": "2026-05-01"}}],
                                "status": {"label": "Testing"},
                            }
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]
            tasks = client.list_jira_tasks_created_by_emails(
                ["pm@npt.sg"],
                release_after="2026-04-29",
                enrich_missing_parent=False,
            )

            self.assertEqual([task["jira_id"] for task in tasks], ["AF-991"])
            self.assertEqual(len(searches), 3)
            self.assertIn("2026-04-29", json.dumps(searches[-1]["subQueries"][-1]))
            self.assertEqual(client.request_stats["bpmis_release_query_filter_probe_count"], 1)
            self.assertEqual(client.request_stats["bpmis_release_query_filter_enabled_count"], 1)
            self.assertEqual(client.request_stats["bpmis_release_query_filter_used_count"], 1)

    def test_team_dashboard_jira_lookup_skips_bpmis_release_filter_when_probe_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]
            searches = []

            def fake_api_request(path, method="GET", params=None, body=None):
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                searches.append(search)
                if "2999-12-31" in json.dumps(search):
                    return {"data": {"rows": [{"id": 1}]}}
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 991,
                                "jiraKey": "AF-991",
                                "summary": "Future task",
                                "reporter": {"id": 101},
                                "fixVersionId": [{"timeline": {"release": "2026-05-01"}}],
                                "status": {"label": "Testing"},
                            }
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]
            tasks = client.list_jira_tasks_created_by_emails(
                ["pm@npt.sg"],
                release_after="2026-04-29",
                enrich_missing_parent=False,
            )

            self.assertEqual([task["jira_id"] for task in tasks], ["AF-991"])
            self.assertEqual(len(searches), 2)
            self.assertEqual(len(searches[-1]["subQueries"]), 2)
            self.assertEqual(client.request_stats["bpmis_release_query_filter_probe_count"], 1)
            self.assertEqual(client.request_stats["bpmis_release_query_filter_enabled_count"], 0)
            self.assertEqual(client.request_stats["bpmis_release_query_filter_disabled_count"], 1)

    def test_team_dashboard_parent_project_uses_inline_parent_when_detail_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            client._get_issue_detail_via_list = lambda issue_id: {}  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {}  # type: ignore[method-assign]

            parent = client._parent_project_for_task(
                {
                    "parentIds": [
                        {
                            "id": 123589,
                            "typeId": "Biz Project",
                            "summary": "Inline BPMIS Project",
                            "marketId": "PH",
                            "bizPriorityId": "P1",
                            "regionalPmPicId": [{"email": "rpm@npt.sg"}],
                        }
                    ]
                },
                {},
            )

            self.assertEqual(parent["bpmis_id"], "123589")
            self.assertEqual(parent["project_name"], "Inline BPMIS Project")
            self.assertEqual(parent["market"], "PH")
            self.assertEqual(parent["priority"], "P1")
            self.assertEqual(parent["regional_pm_pic"], "rpm@npt.sg")

    def test_team_dashboard_parent_project_skips_non_biz_parent_layers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            client._get_issue_detail_via_list = lambda issue_id: {}  # type: ignore[method-assign]
            details = {
                "155621": {
                    "id": 155621,
                    "typeId": "TRD",
                    "summary": "Intermediate TRD",
                    "parentIds": [{"id": 155119}],
                },
                "155119": {
                    "id": 155119,
                    "typeId": "Biz Project",
                    "summary": "Actual Biz Project",
                    "marketId": "Regional",
                    "bizPriorityId": "P2",
                    "regionalPmPicId": [{"email": "rpm@npt.sg"}],
                },
            }
            client.get_issue_detail = lambda issue_id: details.get(str(issue_id), {})  # type: ignore[method-assign]

            parent = client._parent_project_for_task(
                {
                    "parentIds": [
                        {
                            "id": 155621,
                            "typeId": "TRD",
                            "summary": "Intermediate TRD",
                            "parentIds": [155119],
                        }
                    ]
                },
                {},
            )

            self.assertEqual(parent["bpmis_id"], "155119")
            self.assertEqual(parent["project_name"], "Actual Biz Project")
            self.assertEqual(parent["market"], "Regional")
            self.assertEqual(parent["priority"], "P2")
            self.assertEqual(parent["regional_pm_pic"], "rpm@npt.sg")

    def test_team_dashboard_parent_project_returns_empty_when_parent_is_not_biz_project(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            client._get_issue_detail_via_list = lambda issue_id: {}  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {  # type: ignore[method-assign]
                "id": issue_id,
                "typeId": "Tech Project",
                "summary": "Not a Biz Project",
            }

            parent = client._parent_project_for_task({"parentIds": [{"id": 155119, "typeId": "Tech Project"}]}, {})

            self.assertEqual(parent["bpmis_id"], "")
            self.assertEqual(parent["project_name"], "")

    def test_team_dashboard_parent_project_prefers_mapped_detail_labels(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(temp_dir) / "client.json",
                google_oauth_redirect_uri=None,
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token="token",
            )
            client = BPMISDirectApiClient(settings)
            detail_payload = {
                "typeId": "Biz Project",
                "summary": "Mapped Biz Project",
                "marketId": "Regional",
                "bizPriorityId": "P1",
                "regionalPmPicId": [{"email": "rpm@npt.sg"}],
            }
            client._get_issue_detail_via_list = lambda issue_id: {"id": issue_id, **detail_payload}  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {  # type: ignore[method-assign]
                "id": issue_id,
                **detail_payload,
            }

            parent = client._parent_project_for_task(
                {
                    "parentIds": [
                        {
                            "id": 211471,
                            "typeId": 1,
                            "summary": "Raw Biz Project",
                            "marketId": 4,
                            "bizPriorityId": 1,
                            "regionalPmPicId": [87],
                        }
                    ]
                },
                {},
            )

            self.assertEqual(parent["project_name"], "Mapped Biz Project")
            self.assertEqual(parent["market"], "Regional")
            self.assertEqual(parent["priority"], "P1")
            self.assertEqual(parent["regional_pm_pic"], "rpm@npt.sg")


if __name__ == "__main__":
    unittest.main()
