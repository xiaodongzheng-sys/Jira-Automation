import json
import tempfile
import unittest
from pathlib import Path

from bpmis_jira_tool.bpmis import BPMISDirectApiClient
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.models import ProjectMatch


class BPMISClientTests(unittest.TestCase):
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
                },
            )

            self.assertEqual(payload["desc"], "Detailed Jira description")
            self.assertEqual(payload["summary"], "[Feature] Fraud rule improvement")

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


if __name__ == "__main__":
    unittest.main()
