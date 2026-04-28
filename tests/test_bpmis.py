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
                    return {"data": {"rows": []}}
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]

            client.list_biz_projects_for_pm_email("pm@npt.sg")

            self.assertEqual(calls[1][0], "/api/v1/issues/list")
            search_payload = json.loads(calls[1][1]["search"])
            self.assertEqual(search_payload["joinType"], "and")
            self.assertEqual(
                search_payload["subQueries"],
                [
                    {"typeId": [BPMISDirectApiClient.BIZ_PROJECT_TYPE_ID]},
                    {"statusId": [22, 4, 23, 10, 11, 12]},
                    {
                        "joinType": "or",
                        "subQueries": [
                            {"regionalPmPicId": [123]},
                            {"involvedPM": [123]},
                        ],
                    },
                ],
            )

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
            client._resolve_bpmis_user_ids_by_email = lambda email: {  # type: ignore[method-assign]
                "pm1@npt.sg": [101],
                "pm2@npt.sg": [202],
            }.get(email, [])
            calls = []

            def fake_api_request(path, method="GET", params=None, body=None):
                if path == "/api/v1/issues/detail":
                    issue_id = str((params or {}).get("id") or (params or {}).get("issueId") or "")
                    return {
                        "data": {
                            "id": issue_id,
                            "summary": f"Parent Project {issue_id}",
                            "marketId": {"label": "SG"},
                            "bizPriorityId": {"label": "P1"},
                            "regionalPmPicId": [{"emailAddress": "rpm@npt.sg"}],
                        }
                    }
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                calls.append(search)
                self.assertEqual(search["subQueries"][0], {"typeId": [BPMISDirectApiClient.TASK_TYPE_ID]})
                self.assertEqual(search["subQueries"][1], {"creator": [101, 202]})
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
                                    "creator": {"emailAddress": "pm1@npt.sg"},
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
                                "creator": {"id": 202},
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


if __name__ == "__main__":
    unittest.main()
