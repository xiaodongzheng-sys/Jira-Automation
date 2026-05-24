import json
import os
import tempfile
import threading
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import requests

from bpmis_jira_tool.bpmis import BPMISClient, BPMISDirectApiClient
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError, BPMISNotConfiguredError
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

    def setUp(self):
        self._jira_env_guard = patch.dict(
            os.environ,
            {
                "JIRA_API_TOKEN": "",
                "JIRA_PAT": "",
                "JIRA_PERSONAL_ACCESS_TOKEN": "",
                "JIRA_BASE_URL": "",
                "JIRA_USERNAME": "",
                "JIRA_EMAIL": "",
                "JIRA_AUTH_SCHEME": "",
            },
            clear=False,
        )
        self._jira_env_guard.start()
        self.addCleanup(self._jira_env_guard.stop)

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
                if path == "/api/v1/issues/tree":
                    search = json.loads(params["search"])
                    if "reporter" in search:
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": 1,
                                        "jiraKey": "AF-1",
                                        "summary": "First task",
                                        "reporter": {"id": 101},
                                        "parentIds": [{"id": 900}],
                                    }
                                ]
                            }
                        }
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 2,
                                    "jiraKey": "AF-2",
                                    "summary": "Second task",
                                    "jiraRegionalPmPicId": [{"id": 202}],
                                    "parentIds": [{"id": 900}],
                                }
                            ]
                        }
                    }
                if path == "/api/v1/issues/list":
                    search = json.loads(params["search"])
                    sub_queries = search.get("subQueries") or []
                    if sub_queries and "id" in sub_queries[0]:
                        self.assertEqual(sub_queries[0]["id"], [900])
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": 900,
                                        "typeId": "Biz Project",
                                        "summary": "Parent Project",
                                        "market": "SG",
                                    }
                                ]
                            }
                        }
                self.fail(f"unexpected API call: {path}")

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["af@npt.sg", "pm@npt.sg"])

        self.assertEqual([task["jira_id"] for task in tasks], ["AF-1", "AF-2"])
        self.assertEqual([task["pm_email"] for task in tasks], ["af@npt.sg", "pm@npt.sg"])
        self.assertEqual(tasks[0]["parent_project"]["project_name"], "Parent Project")
        self.assertEqual([path for path, _params in calls].count("/api/v1/users/listByEmail"), 1)
        self.assertEqual([path for path, _params in calls].count("/api/v1/issues/tree"), 2)
        self.assertEqual([path for path, _params in calls].count("/api/v1/issues/list"), 1)
        self.assertEqual(client.request_stats["issue_detail_bulk_lookup_count"], 1)
        self.assertEqual(client.request_stats["issue_detail_bulk_issue_count"], 1)
        self.assertEqual(client.request_stats["issue_detail_single_fallback_count"], 0)

    def test_actual_mandays_sums_open_subtask_story_points_for_each_project(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            calls: list[tuple[str, dict[str, object] | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                search = json.loads((params or {}).get("search") or "{}")
                if path == "/api/v1/issues/list":
                    if search["subQueries"] == [
                        {"typeId": [BPMISDirectApiClient.TASK_TYPE_ID]},
                        {"parentIds": [225159, 225160]},
                    ]:
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": 991,
                                        "typeId": "Task",
                                        "statusId": "Developing",
                                        "parentIds": [{"id": 225159}],
                                    },
                                    {
                                        "id": 993,
                                        "typeId": "Task",
                                        "statusId": "Testing",
                                        "parentIds": [{"id": 225159}],
                                    },
                                    {
                                        "id": 994,
                                        "typeId": "Task",
                                        "statusId": "Developing",
                                        "parentIds": [{"id": 225160}],
                                    },
                                    {
                                        "id": 992,
                                        "typeId": "Task",
                                        "statusId": "Closed",
                                        "parentIds": [{"id": 225159}],
                                    },
                                ]
                            }
                        }
                    self.assertEqual(search["subQueries"], [{"parentIds": [991, 993, 994]}])
                    return {
                        "data": {
                            "rows": [
                                {"id": 1001, "typeId": "Sub Task", "parentIds": [991], "statusId": "Open", "storyPoints": 2},
                                {"id": 1002, "typeId": "Sub Task", "parentIds": [993], "statusId": "Testing", "storyPoints": "3.5"},
                                {"id": 1004, "typeId": 5, "parentIds": [991], "statusId": "Open", "storyPoints": 1},
                                {"id": 1005, "typeId": "Sub Task", "parentIds": [993], "statusId": "Open", "storyPoints": 4},
                                {"id": 1007, "typeId": "Sub Task", "parentIds": [994], "statusId": "Open", "storyPoints": 6},
                                {"id": 1006, "typeId": "Sub Task", "parentIds": [999], "statusId": "Open", "storyPoints": 10},
                                {"id": 1003, "typeId": "Sub Task", "statusId": "Closed", "storyPoints": 8},
                            ]
                        }
                    }
                self.fail(f"unexpected API call: {path}")

            client._api_request = fake_api_request  # type: ignore[method-assign]

            actual_mandays = client.list_actual_mandays_for_projects(["225159", "225160"])

        self.assertEqual(actual_mandays, {"225159": 10.5, "225160": 6.0})
        self.assertEqual([path for path, _params in calls], ["/api/v1/issues/list", "/api/v1/issues/list"])
        self.assertEqual(client.request_stats["actual_mandays_project_task_list_page_count"], 1)
        self.assertEqual(client.request_stats["actual_mandays_subtask_list_page_count"], 1)

    def test_actual_mandays_falls_back_to_tree_when_bulk_task_lookup_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            calls: list[tuple[str, dict[str, object] | None]] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                calls.append((path, params))
                search = json.loads((params or {}).get("search") or "{}")
                if path == "/api/v1/issues/list" and search.get("subQueries") == [
                    {"typeId": [BPMISDirectApiClient.TASK_TYPE_ID]},
                    {"parentIds": [225159]},
                ]:
                    raise BPMISError("bulk task lookup failed")
                if path == "/api/v1/issues/tree":
                    self.assertEqual(search["parentIds"], [225159])
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 991,
                                    "typeId": "Task",
                                    "statusId": "Developing",
                                    "parentIds": [{"id": 225159}],
                                },
                            ]
                        }
                    }
                if path == "/api/v1/issues/list":
                    self.assertEqual(search["subQueries"], [{"parentIds": [991]}])
                    return {
                        "data": {
                            "rows": [
                                {"id": 1001, "typeId": "Sub Task", "parentIds": [991], "statusId": "Open", "storyPoints": 2},
                            ]
                        }
                    }
                self.fail(f"unexpected API call: {path}")

            client._api_request = fake_api_request  # type: ignore[method-assign]

            actual_mandays = client.list_actual_mandays_for_projects(["225159"])

        self.assertEqual(actual_mandays, {"225159": 2.0})
        self.assertEqual([path for path, _params in calls], ["/api/v1/issues/list", "/api/v1/issues/tree", "/api/v1/issues/list"])
        self.assertEqual(client.request_stats["actual_mandays_project_tree_fallback_count"], 1)

    def test_team_dashboard_parent_details_are_loaded_in_bulk_chunks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            searches = []

            def fake_api_request(path, method="GET", params=None, body=None):
                if path == "/api/v1/users/listByEmail":
                    return {"data": [{"id": 101, "email": "pm@npt.sg"}]}
                if path == "/api/v1/issues/tree":
                    search = json.loads((params or {}).get("search") or "{}")
                    searches.append(search)
                    if "jiraRegionalPmPicId" in search:
                        return {"data": {"rows": []}}
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": index,
                                    "jiraKey": f"AF-{index}",
                                    "summary": f"Task {index}",
                                    "reporter": {"id": 101},
                                    "parentIds": [{"id": 1000 + index}],
                                }
                                for index in range(72)
                            ]
                        }
                    }
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                searches.append(search)
                sub_queries = search.get("subQueries") or []
                if sub_queries and "id" in sub_queries[0]:
                    issue_ids = sub_queries[0]["id"]
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": issue_id,
                                    "typeId": "Biz Project",
                                    "summary": f"Parent Project {issue_id}",
                                    "market": "SG",
                                }
                                for issue_id in issue_ids
                            ]
                        }
                    }
                self.fail(f"unexpected issue list search: {search}")

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["pm@npt.sg"], enrich_missing_parent=False)

        self.assertEqual(len(tasks), 72)
        self.assertEqual(tasks[0]["parent_project"]["project_name"], "Parent Project 1000")
        self.assertEqual(tasks[-1]["parent_project"]["project_name"], "Parent Project 1071")
        bulk_searches = [search for search in searches if (search.get("subQueries") or [{}])[0].get("id")]
        self.assertEqual(sorted(len(search["subQueries"][0]["id"]) for search in bulk_searches), [22, 50])
        self.assertEqual(client.request_stats["issue_detail_bulk_lookup_count"], 2)
        self.assertEqual(client.request_stats["issue_detail_bulk_issue_count"], 72)
        self.assertEqual(client.request_stats["issue_detail_single_fallback_count"], 0)

    def test_team_dashboard_parallel_tree_queries_preserve_serial_dedupe_order(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {"TEAM_DASHBOARD_TREE_WORKERS": "2"},
            clear=False,
        ):
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]

            def fake_api_request(path, method="GET", params=None, body=None):
                self.assertEqual(path, "/api/v1/issues/tree")
                search = json.loads((params or {}).get("search") or "{}")
                if "reporter" in search:
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 1,
                                    "jiraKey": "DUP-1",
                                    "summary": "Reporter wins",
                                    "reporter": {"id": 101},
                                    "status": {"label": "Waiting"},
                                },
                                {
                                    "id": 2,
                                    "jiraKey": "REP-2",
                                    "summary": "Reporter only",
                                    "reporter": {"id": 101},
                                    "status": {"label": "Waiting"},
                                },
                            ]
                        }
                    }
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 3,
                                "jiraKey": "DUP-1",
                                "summary": "PM duplicate",
                                "jiraRegionalPmPicId": [{"id": 101}],
                                "status": {"label": "Testing"},
                            },
                            {
                                "id": 4,
                                "jiraKey": "PM-4",
                                "summary": "PM only",
                                "jiraRegionalPmPicId": [{"id": 101}],
                                "status": {"label": "Testing"},
                            },
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["pm@npt.sg"], enrich_missing_parent=False)

        self.assertEqual([task["jira_id"] for task in tasks], ["DUP-1", "REP-2", "PM-4"])
        self.assertEqual(tasks[0]["jira_title"], "Reporter wins")
        self.assertEqual(client.request_stats["issue_tree_page_count"], 2)
        self.assertIn("issue_tree_reporter", client.request_timings)
        self.assertIn("issue_tree_jiraRegionalPmPicId", client.request_timings)

    def test_team_dashboard_tree_query_uses_larger_page_size(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {"TEAM_DASHBOARD_TREE_WORKERS": "1", "TEAM_DASHBOARD_TREE_PAGE_SIZE": "500"},
            clear=False,
        ):
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]
            page_sizes: list[int] = []

            def fake_api_request(path, method="GET", params=None, body=None):
                self.assertEqual(path, "/api/v1/issues/tree")
                search = json.loads((params or {}).get("search") or "{}")
                page_sizes.append(search.get("pageSize"))
                if "jiraRegionalPmPicId" in search:
                    return {"data": {"rows": []}}
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 1,
                                "jiraKey": "AF-1",
                                "summary": "Task",
                                "reporter": {"id": 101},
                                "status": {"label": "Testing"},
                            }
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["pm@npt.sg"], enrich_missing_parent=False)

        self.assertEqual([task["jira_id"] for task in tasks], ["AF-1"])
        self.assertEqual(page_sizes, [500, 500])

    def test_team_dashboard_parallel_tree_single_field_failure_uses_list_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {"TEAM_DASHBOARD_TREE_WORKERS": "2"},
            clear=False,
        ):
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]

            def fake_api_request(path, method="GET", params=None, body=None):
                search = json.loads((params or {}).get("search") or "{}")
                if path == "/api/v1/issues/tree":
                    if "reporter" in search:
                        raise BPMISError("reporter tree unavailable")
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 2,
                                    "jiraKey": "PM-2",
                                    "summary": "PM tree",
                                    "jiraRegionalPmPicId": [{"id": 101}],
                                    "status": {"label": "Testing"},
                                }
                            ]
                        }
                    }
                self.assertEqual(path, "/api/v1/issues/list")
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 1,
                                "jiraKey": "REP-1",
                                "summary": "Reporter fallback",
                                "reporter": {"id": 101},
                                "status": {"label": "Waiting"},
                            }
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["pm@npt.sg"], enrich_missing_parent=False)

        self.assertEqual([task["jira_id"] for task in tasks], ["REP-1", "PM-2"])
        self.assertEqual(client.request_stats["issue_tree_fallback_count"], 1)
        self.assertEqual(client.request_stats["issue_list_page_count"], 1)
        self.assertEqual(client.request_stats["issue_tree_page_count"], 1)

    def test_team_dashboard_parent_bulk_failure_falls_back_to_single_lookup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            def fake_api_request(path, method="GET", params=None, body=None):
                if path == "/api/v1/users/listByEmail":
                    return {"data": [{"id": 101, "email": "pm@npt.sg"}]}
                if path == "/api/v1/issues/tree":
                    search = json.loads((params or {}).get("search") or "{}")
                    if "jiraRegionalPmPicId" in search:
                        return {"data": {"rows": []}}
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": 1,
                                    "jiraKey": "AF-1",
                                    "summary": "Task 1",
                                    "reporter": {"id": 101},
                                    "parentIds": [{"id": 900}],
                                },
                                {
                                    "id": 2,
                                    "jiraKey": "AF-2",
                                    "summary": "Task 2",
                                    "reporter": {"id": 101},
                                    "parentIds": [{"id": 901}],
                                },
                            ]
                        }
                    }
                if path == "/api/v1/issues/list":
                    search = json.loads((params or {}).get("search") or "{}")
                    sub_queries = search.get("subQueries") or []
                    if sub_queries and "id" in sub_queries[0]:
                        issue_ids = sub_queries[0]["id"]
                        if len(issue_ids) > 1:
                            raise BPMISError("bulk id list unsupported")
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": issue_ids[0],
                                        "typeId": "Biz Project",
                                        "summary": "Fallback Parent",
                                        "market": "SG",
                                    }
                                ]
                            }
                        }
                    self.fail(f"unexpected issue list search: {search}")
                self.fail(f"unexpected API call: {path}")

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["pm@npt.sg"], enrich_missing_parent=False)

        self.assertEqual([task["parent_project"]["project_name"] for task in tasks], ["Fallback Parent", "Fallback Parent"])
        self.assertEqual(client.request_stats["issue_detail_bulk_lookup_count"], 1)
        self.assertEqual(client.request_stats["issue_detail_single_fallback_count"], 2)

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
                                        "statusId": 23,
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
                                        "statusId": 23,
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

    def test_team_dashboard_biz_project_status_label_maps_numeric_status_ids(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            self.assertEqual(client._team_dashboard_biz_project_status_label({"statusId": 4}), "Pending Review")
            self.assertEqual(client._team_dashboard_biz_project_status_label({"statusId": 23}), "Confirmed")
            self.assertEqual(client._team_dashboard_biz_project_status_label({"statusId": {"id": 23}}), "Confirmed")
            self.assertEqual(client._team_dashboard_biz_project_status_label({"statusId": 10}), "Developing")
            self.assertEqual(
                client._team_dashboard_biz_project_status_label({"bizProjectStatus": {"value": 11}}),
                "Testing",
            )
            self.assertEqual(
                client._team_dashboard_biz_project_status_label({"currentStatus": {"label": "UAT"}}),
                "UAT",
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
                    search_payload = json.loads((params or {})["search"])
                    sub_queries = search_payload.get("subQueries") or []
                    if any("fixVersionId" in item for item in sub_queries):
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
                    if any("id" in item for item in sub_queries):
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": 9001,
                                        "desc": "Add wallet rollback handling.\nSupport new repayment path.",
                                        "jiraRegionalPmPicId": [{"displayName": "Alice PM"}],
                                        "jiraPrdLink": "https://confluence/prd-1",
                                    }
                                ]
                            }
                        }
                if path == "/api/v1/issues/detail":
                    raise AssertionError("Productization issue enrichment should use bulk issues/list lookup.")
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
            bulk_payload = json.loads(calls[1][1]["search"])
            self.assertEqual(bulk_payload["subQueries"], [{"id": [9001]}])
            self.assertEqual(client.request_stats["issue_detail_bulk_lookup_count"], 1)
            self.assertEqual(client.request_stats["issue_detail_lookup_count"], 0)
            self.assertEqual(rows[0]["desc"], "Add wallet rollback handling.\nSupport new repayment path.")
            self.assertEqual(rows[0]["jiraPrdLink"], "https://confluence/prd-1")
            self.assertEqual(rows[0]["jiraRegionalPmPicId"][0]["displayName"], "Alice PM")

    def test_list_issues_for_version_batches_many_missing_detail_rows(self):
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
                    search_payload = json.loads((params or {})["search"])
                    sub_queries = search_payload.get("subQueries") or []
                    if any("fixVersionId" in item for item in sub_queries):
                        return {
                            "data": {
                                "rows": [
                                    {
                                        "id": issue_id,
                                        "summary": f"Upgrade item {issue_id}",
                                        "jiraLink": f"https://jira.shopee.io/browse/ABC-{issue_id}",
                                    }
                                    for issue_id in range(1, 73)
                                ]
                            }
                        }
                    id_query = next((item["id"] for item in sub_queries if "id" in item), [])
                    return {
                        "data": {
                            "rows": [
                                {
                                    "id": issue_id,
                                    "desc": f"Detail {issue_id}",
                                    "jiraRegionalPmPicId": [{"displayName": "Alice PM"}],
                                    "jiraPrdLink": f"https://confluence/prd-{issue_id}",
                                }
                                for issue_id in id_query
                            ]
                        }
                    }
                if path == "/api/v1/issues/detail":
                    raise AssertionError("Productization issue enrichment should not perform per-issue detail calls.")
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]

            rows = client.list_issues_for_version("321")

            issue_list_payloads = [
                json.loads((params or {})["search"])
                for path, params in calls
                if path == "/api/v1/issues/list"
            ]
            id_lookup_payloads = [
                payload
                for payload in issue_list_payloads
                if any("id" in item for item in payload.get("subQueries") or [])
            ]
            self.assertEqual(len(rows), 72)
            self.assertEqual(len(id_lookup_payloads), 2)
            self.assertEqual(client.request_stats["issue_detail_bulk_lookup_count"], 2)
            self.assertEqual(client.request_stats["issue_detail_bulk_issue_count"], 72)
            self.assertEqual(client.request_stats["issue_detail_lookup_count"], 0)
            self.assertEqual(rows[0]["desc"], "Detail 1")
            self.assertEqual(rows[-1]["jiraPrdLink"], "https://confluence/prd-72")

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

    def test_update_biz_project_status_posts_resolved_status(self):
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
                if path == "/api/v1/issueField/list":
                    return {
                        "data": {
                            "statusId": {
                                "key": "statusId",
                                "optionGroup": ["jiraStatus", "bizProjectStatus"],
                                "optionGroupFilter": {"match": {"value": [[4], [1]]}},
                            }
                        }
                    }
                if path == "/api/v1/options/getGroupOptions":
                    return {"data": {"bizProjectStatus": [{"label": "Developing", "value": 10}]}}
                if path == "/api/v1/issues/list":
                    self.assertEqual(method, "PUT")
                    return {"data": {"ok": True}}
                if path == "/api/v1/issues/updateStatus":
                    return {"data": {"ok": True}}
                if path == "/api/v1/issues/detail":
                    return {
                        "data": {
                            "row": {
                                "id": 221733,
                                "summary": "BPMIS Project",
                                "status": {"label": "Developing"},
                            }
                        }
                    }
                raise AssertionError(path)

            client._api_request = fake_api_request  # type: ignore[method-assign]

            detail = client.update_biz_project_status("221733", "Developing")

            update_call = next(call for call in calls if call[0] == "/api/v1/issues/list" and call[1] == "PUT")
            self.assertEqual(update_call[3], {"id": [221733], "statusId": 10})
            self.assertEqual(detail["status"]["label"], "Developing")

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
                "TEAM_DASHBOARD_TREE_PAGE_SIZE": "200",
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
                "TEAM_DASHBOARD_TREE_PAGE_SIZE": "200",
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
                if path == "/api/v1/issues/tree":
                    search = json.loads((params or {}).get("search") or "{}")
                    calls.append(search)
                    self.assertEqual(search["typeId"], BPMISDirectApiClient.TASK_TYPE_ID)
                    self.assertEqual(search["taskType"], 1)
                    if "reporter" in search:
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
                self.assertEqual(path, "/api/v1/issues/list")
                search = json.loads((params or {}).get("search") or "{}")
                if search.get("subQueries") == [{"id": [225159, 225160]}]:
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
                                },
                                {
                                    "id": 225160,
                                    "summary": "Parent Project 225160",
                                    "typeId": "Biz Project",
                                },
                            ]
                        }
                    }
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
                self.fail(f"unexpected issue list search: {search}")

            client._api_request = fake_api_request  # type: ignore[method-assign]

            tasks = client.list_jira_tasks_created_by_emails(["PM1@npt.sg", "pm2@npt.sg", "pm1@npt.sg"])

            self.assertEqual(len(calls), 2)
            self.assertEqual([task["jira_id"] for task in tasks], ["AF-991", "AF-993"])
            self.assertEqual(tasks[0]["pm_email"], "pm1@npt.sg")
            self.assertEqual(tasks[0]["jira_status"], "PRD Reviewed")
            self.assertEqual(tasks[0]["version"], "Planning_26Q2")
            self.assertEqual(tasks[0]["jira_board"], "AF")
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
                if path == "/api/v1/issues/tree":
                    search = json.loads((params or {}).get("search") or "{}")
                    if "jiraRegionalPmPicId" in search:
                        return {"data": {"rows": []}}
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
                                }
                            ]
                        }
                    }
                self.fail(f"unexpected issue list search: {search}")

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
                self.assertEqual(path, "/api/v1/issues/tree")
                search = json.loads((params or {}).get("search") or "{}")
                if "jiraRegionalPmPicId" in search:
                    return {"data": {"rows": []}}
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
                "TEAM_DASHBOARD_TREE_PAGE_SIZE": "200",
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
                self.assertEqual(path, "/api/v1/issues/tree")
                search = json.loads((params or {}).get("search") or "{}")
                if "jiraRegionalPmPicId" in search:
                    return {"data": {"rows": []}}
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
                self.assertEqual(path, "/api/v1/issues/tree")
                search = json.loads((params or {}).get("search") or "{}")
                self.assertEqual(search["page"], 1)
                if "reporter" in search:
                    return {"data": {"rows": []}}
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

            with patch.dict(os.environ, {"TEAM_DASHBOARD_TREE_PAGE_SIZE": "200"}, clear=False):
                tasks = client.list_jira_tasks_created_by_emails(
                    ["pm@npt.sg"],
                    max_pages=1,
                    enrich_missing_parent=False,
                )

            self.assertEqual(len(calls), 2)
            self.assertEqual(len(tasks), 200)
            self.assertEqual(client.request_stats["issue_tree_page_count"], 2)
            self.assertEqual(client.request_stats["issue_list_page_cap_hit"], 1)
            self.assertEqual(client.request_stats["issue_tree_rows_scanned"], 200)
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
                search = json.loads((params or {}).get("search") or "{}")
                searches.append(search)
                if path == "/api/v1/versions/list":
                    self.assertEqual(search["timelineEndAfter"], "2026-03-01")
                    self.assertEqual(search["timelineEndBefore"], "2028-02-29")
                    self.assertEqual(search["pageSize"], 1000)
                    return {
                        "data": {
                            "rows": [
                                {"id": 321, "fullName": "Planning_26Q2", "timelineEnd": "2026-03-01"},
                                {"id": 322, "fullName": "Planning_TBD"},
                                {"id": 321, "fullName": "Planning_26Q2", "timelineEnd": "2026-03-01"},
                            ]
                        }
                    }
                self.assertEqual(path, "/api/v1/issues/tree")
                self.assertEqual(search.get("fixVersionId"), [321, 322])
                if "jiraRegionalPmPicId" in search:
                    return {"data": {"rows": []}}
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 991,
                                "jiraKey": "AF-991",
                                "summary": "March task",
                                "reporter": {"id": 101},
                                "fixVersionId": 321,
                                "status": {"label": "Testing"},
                            },
                            {
                                "id": 992,
                                "jiraKey": "AF-992",
                                "summary": "No release date task",
                                "reporter": {"id": 101},
                                "fixVersionId": 322,
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
            self.assertEqual(searches[0]["timelineEndAfter"], "2026-03-01")
            self.assertEqual(searches[-1]["fixVersionId"], [321, 322])
            self.assertEqual(tasks[0]["release_date"], "2026-03-01")
            self.assertEqual(tasks[0]["version"], "Planning_26Q2")
            self.assertEqual(tasks[1]["version"], "Planning_TBD")
            self.assertEqual(tasks[1]["release_date"], "")
            self.assertEqual(client.request_stats["release_version_lookup_count"], 1)
            self.assertEqual(client.request_stats["release_version_count"], 2)
            self.assertEqual(client.request_stats["issue_release_before_cutoff_count"], 1)
            self.assertEqual(client.request_stats["issue_release_missing_included_count"], 1)

    def test_team_dashboard_jira_lookup_filters_release_window(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]
            searches = []

            def fake_api_request(path, method="GET", params=None, body=None):
                search = json.loads((params or {}).get("search") or "{}")
                searches.append(search)
                if path == "/api/v1/versions/list":
                    self.assertEqual(search["timelineEndAfter"], "2026-05-20")
                    self.assertEqual(search["timelineEndBefore"], "2026-05-28")
                    return {
                        "data": {
                            "rows": [
                                {"id": 321, "fullName": "AF_v1.0.80", "timelineEnd": "2026-05-20"},
                                {"id": 322, "fullName": "DBPSG_v3.01", "timelineEnd": "2026-05-28"},
                            ]
                        }
                    }
                self.assertEqual(path, "/api/v1/issues/tree")
                if "jiraRegionalPmPicId" in search:
                    return {"data": {"rows": []}}
                return {
                    "data": {
                        "rows": [
                            {
                                "id": 991,
                                "jiraKey": "AF-991",
                                "summary": "In window",
                                "reporter": {"id": 101},
                                "fixVersionId": [{"timeline": {"release": "2026-05-20"}}],
                                "status": {"label": "Testing"},
                            },
                            {
                                "id": 992,
                                "jiraKey": "AF-992",
                                "summary": "After window",
                                "reporter": {"id": 101},
                                "fixVersionId": [{"timeline": {"release": "2026-06-01"}}],
                                "status": {"label": "Testing"},
                            },
                            {
                                "id": 993,
                                "jiraKey": "AF-993",
                                "summary": "Missing release",
                                "reporter": {"id": 101},
                                "status": {"label": "Testing"},
                            },
                        ]
                    }
                }

            client._api_request = fake_api_request  # type: ignore[method-assign]
            tasks = client.list_jira_tasks_created_by_emails(
                ["pm@npt.sg"],
                release_after="2026-05-20",
                release_before="2026-05-28",
                enrich_missing_parent=False,
            )

            self.assertEqual([task["jira_id"] for task in tasks], ["AF-991"])
            self.assertEqual(searches[-1]["fixVersionId"], [321, 322])
            self.assertEqual(client.request_stats["issue_release_after_window_count"], 1)
            self.assertEqual(client.request_stats["issue_release_missing_excluded_count"], 1)

    def test_team_dashboard_jira_lookup_falls_back_when_release_version_lookup_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]
            searches = []

            def fake_api_request(path, method="GET", params=None, body=None):
                search = json.loads((params or {}).get("search") or "{}")
                searches.append(search)
                if path == "/api/v1/versions/list":
                    raise BPMISError("version lookup failed")
                self.assertEqual(path, "/api/v1/issues/list")
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
            self.assertEqual(searches[0]["timelineEndAfter"], "2026-04-29")
            self.assertEqual(client.request_stats["release_version_lookup_failed_count"], 1)
            self.assertEqual(client.request_stats["issue_list_page_count"], 1)

    def test_team_dashboard_jira_lookup_falls_back_when_release_version_lookup_is_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]
            paths = []

            def fake_api_request(path, method="GET", params=None, body=None):
                paths.append(path)
                if path == "/api/v1/versions/list":
                    return {"data": {"rows": []}}
                self.assertEqual(path, "/api/v1/issues/list")
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
            self.assertEqual(paths, ["/api/v1/versions/list", "/api/v1/issues/list"])
            self.assertEqual(client.request_stats["release_version_count"], 0)
            self.assertEqual(client.request_stats["issue_list_page_count"], 1)

    def test_team_dashboard_jira_lookup_falls_back_when_issues_tree_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]
            searches = []

            def fake_api_request(path, method="GET", params=None, body=None):
                search = json.loads((params or {}).get("search") or "{}")
                searches.append(search)
                if path == "/api/v1/versions/list":
                    return {"data": {"rows": [{"id": 321}]}}
                if path == "/api/v1/issues/tree":
                    raise BPMISError("tree failed")
                self.assertEqual(path, "/api/v1/issues/list")
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
            self.assertEqual(client.request_stats["release_version_count"], 1)
            self.assertEqual(client.request_stats["issue_tree_fallback_count"], 2)
            self.assertEqual(client.request_stats["issue_list_page_count"], 2)

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

    def test_bpmis_abstract_contract_methods_raise_not_implemented(self):
        abstract_calls = [
            (BPMISClient.ping, (object(),)),
            (BPMISClient.find_project, (object(), "123")),
            (BPMISClient.create_jira_ticket, (object(), ProjectMatch(project_id="1", raw={}), {})),
            (BPMISClient.list_biz_projects_for_pm_email, (object(), "pm@npt.sg")),
            (BPMISClient.list_biz_projects_for_pm_emails, (object(), ["pm@npt.sg"])),
            (BPMISClient.search_biz_projects_by_title_keywords, (object(), "risk")),
            (BPMISClient.list_jira_tasks_for_project_created_by_email, (object(), "1", "pm@npt.sg")),
            (BPMISClient.list_jira_tasks_for_projects_created_by_emails, (object(), ["1"], ["pm@npt.sg"])),
            (BPMISClient.list_jira_tasks_created_by_emails, (object(), ["pm@npt.sg"])),
            (BPMISClient.get_single_brd_doc_link_for_project, (object(), "1")),
            (BPMISClient.get_single_brd_doc_links_for_projects, (object(), ["1"])),
            (BPMISClient.get_brd_doc_links_for_projects, (object(), ["1"])),
            (BPMISClient.search_versions, (object(), "v1")),
            (BPMISClient.list_issues_for_version, (object(), "1")),
            (BPMISClient.list_actual_mandays_for_projects, (object(), ["1"])),
            (BPMISClient.get_issue_detail, (object(), "1")),
            (BPMISClient.get_jira_ticket_detail, (object(), "ABC-1")),
            (BPMISClient.get_jira_ticket_details, (object(), ["ABC-1"])),
            (BPMISClient.update_jira_ticket_status, (object(), "ABC-1", "Done")),
            (BPMISClient.update_biz_project_status, (object(), "1", "Done")),
            (BPMISClient.update_jira_ticket_fix_version, (object(), "ABC-1", "v1")),
            (BPMISClient.link_jira_ticket_to_project, (object(), "ABC-1", "1")),
            (BPMISClient.delink_jira_ticket_from_project, (object(), "ABC-1", "1")),
        ]

        for method, args in abstract_calls:
            with self.subTest(method=method.__name__):
                with self.assertRaises(NotImplementedError):
                    method(*args)

    def test_low_level_helpers_cover_dates_threads_and_nested_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            with patch.dict(os.environ, {"TEAM_DASHBOARD_TREE_PAGE_SIZE": "bad", "WORKERS": "bad"}, clear=False):
                self.assertEqual(client._tree_page_size(), 500)
                self.assertEqual(client._worker_count("WORKERS", 2, 4), 2)
            with patch.dict(os.environ, {"TEAM_DASHBOARD_TREE_PAGE_SIZE": "1", "WORKERS": "99"}, clear=False):
                self.assertEqual(client._tree_page_size(), 50)
                self.assertEqual(client._worker_count("WORKERS", 2, 4), 4)

            sessions = []

            def capture_worker_session():
                sessions.append(client._bpmis_session_for_current_thread())

            worker = threading.Thread(target=capture_worker_session)
            worker.start()
            worker.join()
            self.assertEqual(sessions[0].headers["Authorization"], client.session.headers["Authorization"])
            self.assertIs(client._bpmis_session_for_current_thread(), client.session)

            client._store_issue_detail("", {"id": "ignored"})
            nested_rows = client._flatten_issue_tree_rows(
                {
                    "children": [
                        {"id": "1", "summary": "One"},
                        {"list": [{"jiraKey": "ABC-2"}, "ignored"]},
                    ]
                }
            )
            self.assertEqual([row.get("id") or row.get("jiraKey") for row in nested_rows], ["1", "ABC-2"])
            self.assertEqual(client._extract_issue_rows_from_response({"data": nested_rows})[0]["id"], "1")
            self.assertEqual(client._extract_issue_rows_from_response("bad"), [])
            self.assertFalse(client._bpmis_release_query_filter_enabled([1], None))
            self.assertFalse(client._bpmis_release_query_filter_enabled([1], datetime(2026, 1, 1)))

            self.assertIsNone(client._extract_team_dashboard_version_id({"id": "abc"}))
            self.assertEqual(client._extract_team_dashboard_version_id({"versionId": "42"}), 42)
            client._team_dashboard_release_versions_by_id[42] = {"fullName": "May", "timeline": {"release": "2026-05-01"}}
            self.assertEqual(client._enrich_team_dashboard_fix_version_value({"id": 42, "name": "Old"})["fullName"], "May")
            self.assertEqual(client._enrich_team_dashboard_fix_version_value("missing"), "missing")

            self.assertEqual(client._parse_issue_datetime(date(2026, 5, 1)), datetime(2026, 5, 1))
            self.assertEqual(client._parse_issue_datetime(datetime(2026, 5, 1, tzinfo=timezone.utc)), datetime(2026, 5, 1))
            self.assertEqual(client._parse_issue_datetime("2026/05/01 12:13:14"), datetime(2026, 5, 1, 12, 13, 14))
            self.assertIsNone(client._parse_issue_datetime("not-a-date"))
            self.assertIsNone(client._parse_issue_datetime(10**30))

            row = {
                "fields": {
                    "jiraKey": "ABC-9",
                    "fixVersions": [{"timeline": [{"label": "Go Live", "value": "2026-06-01"}]}],
                    "componentId": [{"label": "Core"}],
                }
            }
            task = client._normalize_team_dashboard_jira_task(row, pm_email="pm@npt.sg")
            self.assertEqual(task["jira_id"], "ABC-9")
            self.assertEqual(task["jira_board"], "ABC")
            self.assertEqual(task["release_date"], "2026-06-01")
            self.assertEqual(task["component"], "Core")
            self.assertTrue(client._issue_created_on_or_after({"createdAt": "2026-05-02"}, datetime(2026, 5, 1)))
            self.assertTrue(client._issue_release_on_or_after({"releaseDate": "2026-05-02"}, datetime(2026, 5, 1)))
            self.assertTrue(client._all_rows_before_created_cutoff([{"createdAt": "2026-04-01"}], datetime(2026, 5, 1)))

    def test_create_payload_and_create_ticket_error_and_success_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            client._get_issue_fields = lambda: {  # type: ignore[method-assign]
                "marketId": {"key": "marketId", "optionGroup": "market"},
                "taskType": {"key": "taskType", "optionGroup": "taskType"},
                "componentId": {
                    "key": "componentId",
                    "optionGroup": ["sgComponents", "phComponents"],
                    "optionGroupFilter": {"match": {"value": [[1], [2]]}},
                },
                "bizPriorityId": {"key": "bizPriorityId", "optionGroup": "priority"},
                "uatRequired": {"key": "uatRequired", "optionGroup": "uat"},
                "involvedProductTrackId": {"key": "involvedProductTrackId", "optionGroup": "track"},
            }
            client._group_options_cache = {
                "market": [{"label": "SG", "value": 1}],
                "taskType": [{"label": "Feature", "value": 4}],
                "sgComponents": [{"label": "Core", "value": 9}],
                "priority": [{"label": "P1", "value": 11}],
                "uat": [{"label": "Yes", "value": 1}],
                "track": [{"label": "Risk", "value": 77}],
            }
            client._resolve_jira_user_id = lambda query: {"dev": 101, "qa": 102, "pm": 103, "reporter": 104, "biz": 105}[query]  # type: ignore[method-assign]
            project = ProjectMatch(project_id="225159", raw={})

            payload = client._build_create_payload(
                project,
                {
                    "Market": "SG",
                    "Task Type": "Feature",
                    "Summary": "[Feature][Core] Existing title",
                    "System": "Core",
                    "Component": "Core",
                    "Priority": "P1",
                    "Need UAT": "Yes",
                    "Involved Tracks": "Risk",
                    "PRD Link/s": "https://prd",
                    "TD Link/s": "https://td",
                    "Description": "desc",
                    "Dev PIC": "dev",
                    "QA PIC": "qa",
                    "Product Manager": "pm",
                    "Reporter": "reporter",
                    "Biz PIC": "biz",
                },
            )
            self.assertEqual(payload["summary"], "[Feature][Core] Existing title")
            self.assertEqual(payload["componentId"], [9])
            self.assertEqual(payload["jiraRegionalPmPicId"], [103])
            self.assertEqual(payload["reporter"], 104)

            with self.assertRaises(BPMISError):
                client._required_field({}, "Summary")
            with self.assertRaises(BPMISError):
                client._select_option_groups({"key": "bad"}, 1)
            with self.assertRaises(BPMISError):
                client._resolve_option_value({"key": "x", "optionGroup": "market"}, "Missing")
            original_api_request = client._api_request
            client._api_request = lambda *args, **kwargs: {"data": {"rows": []}}  # type: ignore[method-assign]
            with self.assertRaises(BPMISError):
                client._resolve_fix_versions(1, "Missing")
            client._api_request = original_api_request  # type: ignore[method-assign]

            responses = [
                {"data": {"created": [{"errors": {"summary": "required"}}]}},
                {"data": {"created": [{}], "add": [{}], "update": [{}]}},
                {"data": {"created": [{"key": "ABC-1", "self": "https://jira/browse/ABC-1"}]}},
            ]
            client._api_request = lambda *args, **kwargs: responses.pop(0)  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "validation failed"):
                client.create_jira_ticket(project, {"Market": "SG", "Summary": "x"})
            with self.assertRaisesRegex(BPMISError, "did not return"):
                client.create_jira_ticket(project, {"Market": "SG", "Summary": "x"})
            created = client.create_jira_ticket(project, {"Market": "SG", "Summary": "x"})
            self.assertEqual(created.ticket_key, "ABC-1")

    def test_bpmis_list_and_detail_edges_cover_fallbacks_and_dedupe(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            self.assertEqual(client.list_jira_tasks_for_project_created_by_email("", "pm@npt.sg"), [])
            self.assertEqual(client.list_jira_tasks_for_project_created_by_email("abc", "pm@npt.sg"), [])

            rows = [
                {"id": 1, "jiraKey": "ABC-1", "summary": "Needs detail", "parentIds": [{"id": 225159}]},
                {"id": 1, "jiraKey": "ABC-1", "summary": "Duplicate", "parentIds": [{"id": 225159}]},
                {"id": 2, "jiraKey": "ABC-2", "summary": "Wrong reporter", "reporter": {"email": "other@npt.sg"}, "parentIds": [{"id": 225159}]},
            ]
            client._api_request = lambda *args, **kwargs: {"data": {"rows": rows}}  # type: ignore[method-assign]
            client._issue_requires_user_enrichment = lambda row, email: row.get("id") == 1  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {"id": issue_id, "reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": 225159}]}  # type: ignore[method-assign]
            client._get_jira_ticket_details_via_jira_bulk = lambda keys: None  # type: ignore[method-assign]
            tasks = client.list_jira_tasks_for_project_created_by_email("225159", "pm@npt.sg")
            self.assertEqual([task["ticket_key"] for task in tasks], ["ABC-1"])

            client._worker_count = lambda *args: 2  # type: ignore[method-assign]

            def fake_parent_chunk(parent_chunk, _page_size):
                return [
                    {"id": parent_chunk[0] + 1000, "jiraKey": f"ABC-{parent_chunk[0]}", "reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": parent_chunk[0]}]},
                    {"id": parent_chunk[0] + 2000, "jiraKey": "", "reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": parent_chunk[0]}]},
                    {"id": parent_chunk[0] + 3000, "jiraKey": f"DUP-{parent_chunk[0]}", "reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": parent_chunk[0]}]},
                    {"id": parent_chunk[0] + 3000, "jiraKey": f"DUP-{parent_chunk[0]}", "reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": parent_chunk[0]}]},
                ]

            client._list_jira_task_rows_for_parent_chunk = fake_parent_chunk  # type: ignore[method-assign]
            grouped = client.list_jira_tasks_for_projects_created_by_emails([str(i) for i in range(1, 55)] + ["bad", "1"], ["pm@npt.sg"])
            self.assertIn("ABC-1", [task["jira_id"] for task in grouped["1"]])
            self.assertEqual(sum(1 for task in grouped["1"] if task["jira_id"] == "DUP-1"), 1)
            self.assertEqual(client.list_jira_tasks_for_projects_created_by_emails(["bad"], ["pm@npt.sg"]), {})

            client._get_jira_ticket_detail_via_jira = lambda key: None  # type: ignore[method-assign]
            detail_call_count = {"count": 0}

            def fake_safe_api_request(*args, **kwargs):
                detail_call_count["count"] += 1
                if detail_call_count["count"] <= 6:
                    return None
                return {"data": {"rows": [{"jiraKey": "ABC-3", "id": 333, "summary": "Match"}]}}

            client._safe_api_request = fake_safe_api_request  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {"id": issue_id, "desc": "detail"}  # type: ignore[method-assign]
            detail = client.get_jira_ticket_detail("ABC-3")
            self.assertEqual(detail["desc"], "detail")

            client._get_jira_ticket_details_via_jira_bulk = lambda keys: None  # type: ignore[method-assign]
            client.get_jira_ticket_detail = lambda key: {"jiraKey": key, "summary": key}  # type: ignore[method-assign]
            self.assertEqual(set(client.get_jira_ticket_details(["ABC-3", "ABC-3", "bad"]).keys()), {"ABC-3", "BAD"})

    def test_bpmis_status_link_jira_and_api_error_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            self.assertEqual(client._normalize_jira_status("done"), "Done")
            self.assertEqual(client._normalize_jira_status("unknown"), "")
            self.assertEqual(client._normalize_biz_project_status("uat"), "UAT")
            with self.assertRaises(BPMISError):
                client._normalize_biz_project_status("unknown")
            self.assertTrue(client._looks_like_basic_auth_blob("dXNlcjp0b2tlbg"))
            self.assertFalse(client._looks_like_basic_auth_blob("not base64"))
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token", "JIRA_USERNAME": "user"}, clear=False):
                self.assertEqual(client._jira_auth_candidates("token")[0][1], ("user", "token"))
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token", "JIRA_AUTH_SCHEME": "bearer"}, clear=False):
                self.assertIn("Bearer", client._jira_auth_candidates("token")[0][0]["Authorization"])
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token", "JIRA_AUTH_SCHEME": "basic"}, clear=False):
                self.assertIn("Basic", client._jira_auth_candidates("token")[0][0]["Authorization"])

            client._get_issue_fields = lambda: {"statusId": {"key": "statusId", "optionGroup": "status"}}  # type: ignore[method-assign]
            client._group_options_cache = {"status": [{"label": "Done", "value": 12}, {"label": "UAT", "value": 13}]}
            self.assertEqual(client._resolve_jira_status_id("Done"), 12)
            self.assertEqual(client._resolve_biz_project_status_id("UAT"), 13)

            client._update_jira_ticket_status_via_jira = lambda key, status: None  # type: ignore[method-assign]
            client.get_jira_ticket_detail = lambda key: {"id": 222, "status": {"label": "Waiting"}}  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("boom"))  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "Last error"):
                client.update_jira_ticket_status("ABC-1", "Done")

            detail_states = iter([
                {"id": 225159, "status": {"label": "Testing"}},
                {"id": 225159, "status": {"label": "UAT"}},
            ])
            client.get_issue_detail = lambda issue_id: next(detail_states)  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: {"data": {}}  # type: ignore[method-assign]
            updated = client.update_biz_project_status("225159", "UAT")
            self.assertEqual(client._team_dashboard_biz_project_status_label(updated), "UAT")

            client._issue_is_linked_to_parent = lambda key, project_id: False  # type: ignore[method-assign]
            client._find_bpmis_task_row_for_jira_key = lambda key: {"id": 99, "parentIds": [{"id": 1}]}  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "still not linked"):
                client.link_jira_ticket_to_project("ABC-1", 225159)
            client._issue_is_linked_to_parent = lambda key, project_id: False  # type: ignore[method-assign]
            client._find_linked_bpmis_task_id = lambda key, project_id: ""  # type: ignore[method-assign]
            client.get_jira_ticket_detail = lambda key: {"jiraKey": key}  # type: ignore[method-assign]
            self.assertEqual(client.delink_jira_ticket_from_project("ABC-1", 225159)["jiraKey"], "ABC-1")

            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token"}, clear=False):
                responses = [
                    self._FakeResponse(401, {"error": "bad"}),
                    self._FakeResponse(200, None, "not-json"),
                    self._FakeResponse(204, None, ""),
                ]
                with patch("bpmis_jira_tool.bpmis.requests.request", side_effect=responses):
                    with self.assertRaises(BPMISError):
                        client._jira_api_request("GET", "/x")
                    self.assertEqual(client._jira_api_request("GET", "/x", expected_statuses={204}, allow_empty=True), {})

            client._api_request = BPMISDirectApiClient._api_request.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client.session.request = lambda *args, **kwargs: self._FakeResponse(500, {"code": 0})  # type: ignore[method-assign]
            with self.assertRaises(BPMISError):
                client._api_request("/bad")
            client.session.request = lambda *args, **kwargs: self._FakeResponse(200, None, "not-json")  # type: ignore[method-assign]
            with self.assertRaises(BPMISError):
                client._api_request("/bad-json")
            client.session.request = lambda *args, **kwargs: self._FakeResponse(200, {"code": 1, "message": "bad code"})  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "bad code"):
                client._api_request("/bad-code")

    def test_bpmis_normalization_search_and_actual_mandays_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            self.assertEqual(client.search_versions(""), [])
            version_pages = [
                {"data": {"rows": [{"id": 1, "fullName": "May Release"}, {"id": "", "fullName": "bad"}]}},
            ]
            client._api_request = lambda *args, **kwargs: version_pages.pop(0)  # type: ignore[method-assign]
            self.assertEqual(client.search_versions("May")[0]["id"], 1)

            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": 1, "parentIds": [{"id": "225159"}]}, {"id": 1, "parentIds": [{"id": "225159"}]}, {"id": "closed", "status": "Closed", "parentIds": [{"id": "225159"}]}, {"id": 2, "parentIds": [{"id": "other"}]}]}}  # type: ignore[method-assign]
            grouped = client._list_open_project_task_rows_via_list_bulk(["225159", "225159", "abc"])
            self.assertEqual([row["id"] for row in grouped["225159"]], [1])
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("boom"))  # type: ignore[method-assign]
            self.assertIsNone(client._list_open_project_task_rows_via_list_bulk(["225159"]))
            self.assertEqual(client._list_open_project_task_rows_via_tree(""), [])

            pages = [
                {"data": {"rows": [{"id": "10", "parentIds": [{"id": "225159"}]}, {"id": "225159", "parentIds": [{"id": "225159"}]}, {"id": "11", "status": "Closed", "parentIds": [{"id": "225159"}]}]}},
            ]
            client._api_request = lambda *args, **kwargs: pages.pop(0)  # type: ignore[method-assign]
            self.assertEqual([row["id"] for row in client._list_open_project_task_rows_via_tree("225159")], ["10"])

            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": "s1", "parentIds": [{"id": "10"}], "storyPoints": "3"}, "bad", {"id": "s2", "status": "Closed", "parentIds": [{"id": "10"}]}, {"id": "s3", "parentIds": [{"id": "other"}], "storyPoints": "5"}]}}  # type: ignore[method-assign]
            self.assertEqual(client._sum_open_subtask_story_points_for_tasks(["10", "10", "abc"]), 3.0)

            rows = client._normalize_team_dashboard_biz_project_rows(
                [
                    {"id": 1, "summary": "Done Project", "status": "Done"},
                    {"id": 2, "summary": "", "status": "4", "marketId": "SG", "bizPriorityId": "P1"},
                    {"id": 2, "summary": "Duplicate", "status": "4"},
                ]
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["issue_id"], "2")
            self.assertTrue(client._is_team_dashboard_biz_project_status_allowed({}))
            self.assertFalse(client._is_team_dashboard_biz_project_status_allowed({"status": "Closed"}))
            self.assertEqual(client._issue_first_text({"fields": {"x": {"label": "Value"}}}, "x"), "Value")
            self.assertEqual(client._issue_first_person({"people": [{"email": "a@npt.sg"}, {"name": "B"}]}, "people"), "a@npt.sg, B")
            self.assertEqual(client._extract_links({"url": "https://a.example/x", "href": "n/a"}), ["https://a.example/x"])
            self.assertEqual(client._extract_market_label({"label": "SG"}), "SG")
            self.assertEqual(client._emails_for_bpmis_user({"displayName": "PM <pm@npt.sg>"}, ["pm@npt.sg"]), ["pm@npt.sg"])

    def test_bpmis_project_brd_and_search_pagination_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            with self.assertRaises(BPMISError):
                client.list_biz_projects_for_pm_email("")
            client._resolve_bpmis_user_ids_by_email = lambda email: []  # type: ignore[method-assign]
            self.assertEqual(client.list_biz_projects_for_pm_email("pm@npt.sg"), [])
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {email: [] for email in emails}  # type: ignore[method-assign]
            self.assertEqual(client.list_biz_projects_for_pm_emails(["pm@npt.sg"]), [])
            self.assertEqual(client.list_biz_projects_for_pm_emails(["", " "]), [])
            self.assertEqual(client.search_biz_projects_by_title_keywords("  "), [])
            self.assertEqual(client.get_brd_doc_links_for_projects(["", " "]), {})

            def normalize_rows(rows):
                return [{"issue_id": str(row.get("id")), "project_name": row.get("summary", "")} for row in rows]

            client._normalize_team_dashboard_biz_project_rows = normalize_rows  # type: ignore[method-assign]
            client._resolve_bpmis_user_ids_by_email = lambda email: [101]  # type: ignore[method-assign]
            client._resolve_bpmis_user_ids_by_emails = lambda emails: {email: [101] for email in emails}  # type: ignore[method-assign]
            first_page = [{"id": index, "summary": f"Project {index}", "regionalPmPicId": [{"id": 101}]} for index in range(200)]
            calls = {"count": 0}

            def fake_project_api(*args, **kwargs):
                calls["count"] += 1
                return {"data": {"rows": first_page if calls["count"] == 1 else [{"id": 999, "summary": "Last", "regionalPmPicId": [{"id": 101}]}]}}

            client._api_request = fake_project_api  # type: ignore[method-assign]
            projects = client.list_biz_projects_for_pm_email("pm@npt.sg")
            self.assertEqual(projects[-1]["issue_id"], "999")

            calls["count"] = 0
            projects_for_many = client.list_biz_projects_for_pm_emails(["pm@npt.sg"])
            self.assertEqual(projects_for_many[-1]["matched_pm_emails"], ["pm@npt.sg"])

            calls["count"] = 0
            self.assertEqual(client.search_biz_projects_by_title_keywords("risk", max_pages=2)[-1]["issue_id"], "999")

            brd_pages = [
                {"data": {"rows": [{"id": str(index), "parentIds": [{"id": 1}], "link": f"https://prd/{index}"} for index in range(500)]}},
                {"data": {"rows": [{"id": "1", "parentIds": [{"id": 1}], "link": "https://prd/duplicate"}, {"id": "last", "parentIds": [2], "link": "https://prd/last"}]}},
            ]
            client._api_request = lambda *args, **kwargs: brd_pages.pop(0)  # type: ignore[method-assign]
            links = client.get_brd_doc_links_for_projects(["1", "2", "1"])
            self.assertEqual(links["2"], ["https://prd/last"])
            self.assertNotIn("https://prd/duplicate", links["1"])

    def test_bpmis_task_listing_and_actual_mandays_control_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))
            self.assertEqual(client.list_jira_tasks_created_by_emails([]), [])
            client._resolve_team_dashboard_user_ids_timed = lambda emails: {email: [] for email in emails}  # type: ignore[method-assign]
            client._team_dashboard_release_version_ids = lambda *args: []  # type: ignore[method-assign]
            self.assertEqual(client.list_jira_tasks_created_by_emails(["pm@npt.sg"]), [])

            client._resolve_team_dashboard_user_ids_timed = lambda emails: {"pm@npt.sg": [101]}  # type: ignore[method-assign]
            client._team_dashboard_release_version_ids = lambda *args: []  # type: ignore[method-assign]
            client._list_team_dashboard_jira_task_rows_via_tree = lambda *args, **kwargs: None  # type: ignore[method-assign]
            task_rows = [
                {"id": "1", "jiraKey": "ABC-1", "createdAt": "2026-01-01", "reporter": {"id": 101}, "parentIds": [{"id": 225159}]},
                {"id": "2", "jiraKey": "ABC-2", "createdAt": "2026-06-01", "reporter": {"id": 101}},
                {"id": "2", "jiraKey": "ABC-2", "createdAt": "2026-06-01", "reporter": {"id": 101}},
                {"id": "3", "jiraKey": "ABC-3", "createdAt": "2026-06-01"},
            ]
            client._list_team_dashboard_jira_task_rows_via_list = lambda *args, **kwargs: list(task_rows)  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {"id": issue_id, "reporter": {"id": 101}, "parentIds": [{"id": 225159}]}  # type: ignore[method-assign]
            client._prime_biz_project_parent_details = lambda rows: None  # type: ignore[method-assign]
            client._parent_project_for_task = lambda row, cache: {}  # type: ignore[method-assign]
            client._get_jira_ticket_details_via_jira_bulk = lambda keys: {"ABC-2": {"jiraKey": "ABC-2", "summary": "Live", "status": {"label": "Done"}}}  # type: ignore[method-assign]
            tasks = client.list_jira_tasks_created_by_emails(["pm@npt.sg"], created_after="2026-05-01")
            self.assertEqual([task["jira_id"] for task in tasks], ["ABC-2", "ABC-3"])
            self.assertEqual(client.request_stats["issue_tree_fallback_count"], 1)
            self.assertEqual(client.request_stats["issue_created_before_cutoff_count"], 1)

            client._list_open_project_task_rows_via_list_bulk = lambda project_ids: None  # type: ignore[method-assign]
            client._calculate_actual_mandays_for_project = lambda project_id: 2.5  # type: ignore[method-assign]
            self.assertEqual(client.list_actual_mandays_for_projects(["", "225159", "225159"]), {"225159": 2.5})
            self.assertEqual(client.list_actual_mandays_for_projects(["", " "]), {})
            self.assertEqual(client._sum_open_subtask_story_points(""), 0.0)
            self.assertEqual(client._sum_open_subtask_story_points_for_tasks([]), 0.0)
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("subtask fail"))  # type: ignore[method-assign]
            self.assertEqual(client._sum_open_subtask_story_points_for_tasks(["10"]), 0.0)

    def test_bpmis_issue_detail_link_and_update_failure_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            self.assertEqual(client.get_issue_detail(""), {})
            client._safe_api_request = lambda *args, **kwargs: None  # type: ignore[method-assign]
            client._get_issue_detail_via_list = lambda issue_id: {"id": issue_id, "summary": "From list"}  # type: ignore[method-assign]
            self.assertEqual(client.get_issue_detail("101")["summary"], "From list")
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("list fail"))  # type: ignore[method-assign]
            self.assertEqual(BPMISDirectApiClient._get_issue_detail_via_list(client, "bad"), {})
            self.assertEqual(client._get_parent_issue_detail(""), {})
            client._issue_detail_cache.clear()
            client._get_issue_detail_via_list = lambda issue_id: {}  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {"id": issue_id, "summary": "fallback"}  # type: ignore[method-assign]
            self.assertEqual(client._get_parent_issue_detail("102")["summary"], "fallback")
            self.assertEqual(client._get_issue_details_via_list_bulk(["", "abc"]), {})

            client._get_jira_ticket_detail_via_jira = lambda key: None  # type: ignore[method-assign]
            client._safe_api_request = lambda *args, **kwargs: None  # type: ignore[method-assign]
            self.assertEqual(client.get_jira_ticket_detail(""), {})
            self.assertEqual(client.get_jira_ticket_detail("ABC-404"), {})
            client._get_jira_ticket_details_via_jira_bulk = lambda keys: {}  # type: ignore[method-assign]
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token"}, clear=False):
                self.assertEqual(client.get_jira_ticket_details(["ABC-1"]), {})
            self.assertEqual(client.get_jira_ticket_details([]), {})

            with self.assertRaises(BPMISError):
                client.update_jira_ticket_status("", "Done")
            with self.assertRaises(BPMISError):
                client.update_jira_ticket_status("ABC-1", "bad")
            client._update_jira_ticket_status_via_jira = lambda key, status: None  # type: ignore[method-assign]
            client.get_jira_ticket_detail = lambda key: {}  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: {"data": {}}  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "does not expose"):
                client.update_jira_ticket_status("ABC-1", "Done")

            with self.assertRaises(BPMISError):
                client.update_biz_project_status("", "UAT")
            with self.assertRaises(BPMISError):
                client.update_biz_project_status("225159", "")
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("status fail"))  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {}  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "Last error"):
                client.update_biz_project_status("225159", "UAT")

            with self.assertRaises(BPMISError):
                client.update_jira_ticket_fix_version("", "v1")
            with self.assertRaises(BPMISError):
                client.update_jira_ticket_fix_version("ABC-1", "")
            client._update_jira_ticket_fix_version_via_jira = lambda *args: None  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "direct Jira API token"):
                client.update_jira_ticket_fix_version("ABC-1", "v1")

            with self.assertRaises(BPMISError):
                client.link_jira_ticket_to_project("", 1)
            with self.assertRaises(BPMISError):
                client.link_jira_ticket_to_project("ABC-1", "")
            with self.assertRaises(BPMISError):
                client.delink_jira_ticket_from_project("", 1)
            with self.assertRaises(BPMISError):
                client.delink_jira_ticket_from_project("ABC-1", "")
            client._issue_is_linked_to_parent = lambda key, project_id: True  # type: ignore[method-assign]
            client._find_linked_bpmis_task_id = lambda key, project_id: "99"  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("delete fail"))  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "Last error"):
                client.delink_jira_ticket_from_project("ABC-1", 225159)

            with self.assertRaises(BPMISError):
                client._add_existing_jira_ticket_to_project("", 1)
            with self.assertRaises(BPMISError):
                client._add_existing_jira_ticket_to_project("ABC-1", "bad")
            client._api_request = lambda *args, **kwargs: {"data": {"created": {"errors": {"x": "bad"}}, "add": [{"error": "nope"}]}}  # type: ignore[method-assign]
            client._wait_until_jira_ticket_is_linked = lambda *args: False  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "Could not add"):
                client._add_existing_jira_ticket_to_project("ABC-1", 225159)
            self.assertIn("x: bad", client._extract_batch_jira_issue_error({"data": {"created": {"errors": {"x": "bad"}}}}))
            self.assertIn("nope", client._extract_batch_jira_issue_error({"data": {"failed": [{"error": "nope"}, "bad"]}}))

    def test_bpmis_jira_live_and_private_extraction_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            self.assertIsNone(client._get_jira_ticket_detail_via_jira("ABC-1"))
            client._jira_api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("status 404"))  # type: ignore[method-assign]
            self.assertIsNone(client._get_jira_ticket_detail_via_jira("ABC-1"))
            client._jira_api_request = lambda *args, **kwargs: {"issues": ["bad"]}  # type: ignore[method-assign]
            self.assertEqual(client._get_jira_ticket_details_via_jira_bulk_chunk(["ABC-1"]), {})
            client._jira_api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("status 400"))  # type: ignore[method-assign]
            self.assertIsNone(client._get_jira_ticket_details_via_jira_bulk_chunk(["ABC-1"]))

            client._jira_api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("status 404"))  # type: ignore[method-assign]
            self.assertIsNone(client._update_jira_ticket_status_via_jira("ABC-1", "Done"))
            client._jira_api_request = lambda *args, **kwargs: {"transitions": [{"id": "1", "to": {"name": "Testing"}}]}  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "Available transitions"):
                client._update_jira_ticket_status_via_jira("ABC-1", "Done")
            calls = {"count": 0}

            def fake_jira_request(*args, **kwargs):
                calls["count"] += 1
                if calls["count"] == 1:
                    return {"transitions": [{"id": "2", "to": {"name": "Done"}}]}
                return {}

            client._jira_api_request = fake_jira_request  # type: ignore[method-assign]
            client._get_jira_ticket_detail_via_jira = lambda key: None  # type: ignore[method-assign]
            self.assertEqual(client._update_jira_ticket_status_via_jira("ABC-1", "Done")["status"]["label"], "Done")
            client._get_jira_ticket_detail_via_jira = lambda key: {"jiraKey": key, "status": {"label": "Testing"}}  # type: ignore[method-assign]
            calls["count"] = 0
            with self.assertRaisesRegex(BPMISError, "still"):
                client._update_jira_ticket_status_via_jira("ABC-1", "Done")

            client._jira_api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("status 401"))  # type: ignore[method-assign]
            self.assertIsNone(client._update_jira_ticket_fix_version_via_jira("ABC-1", {"name": "v1"}))
            with patch.dict(os.environ, {"JIRA_API_TOKEN": ""}, clear=False):
                self.assertIsNone(client._update_jira_ticket_fix_version_via_jira("ABC-1", {"name": "v1"}))

            client._get_jira_ticket_detail_via_jira = lambda key: (_ for _ in ()).throw(BPMISError("boom"))  # type: ignore[method-assign]
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token"}, clear=False):
                self.assertEqual(client._with_live_jira_fields({"jiraKey": "ABC-1"}, "ABC-1")["jiraKey"], "ABC-1")
            self.assertEqual(client._merge_live_jira_fields({"status": "Waiting"}, {"status": {"label": "Done"}, "fixVersions": ["v1"], "components": ["Core"]})["fixVersions"], ["v1"])

            self.assertEqual(client._extract_issue_description({"desc": {"label": "Desc"}}), "Desc")
            self.assertEqual(client._extract_issue_pm({"regionalPmPic": [{"email": "pm@npt.sg"}]}), "pm@npt.sg")
            self.assertFalse(client._is_biz_project_issue({}))
            self.assertTrue(client._is_biz_project_issue({"typeId": {"label": "Biz Project"}}))
            self.assertFalse(client._is_subtask_row({"typeId": "Task", "parentIds": [10]}, "10"))
            self.assertTrue(client._is_subtask_row({"typeId": "Sub-task"}, "10"))
            self.assertEqual(client._extract_story_points({"storyPoints": "about 3.5 days"}), 3.5)
            self.assertEqual(client._extract_story_points({"storyPoints": "bad"}), 0.0)
            self.assertEqual(client._extract_release_date_from_version_value("not dict"), "")
            self.assertEqual(client._extract_release_date_from_version_value({"timeline": [{"label": "Build", "value": "x"}]}), "")
            self.assertEqual(client._prefix_summary("Bug", "SG", "Core", "Title"), "Title")
            self.assertEqual(client._prefix_summary("Feature", "Regional", "", "[Feature][Productization]"), "[Feature][Productization]")
            self.assertIsNone(client._normalize_ticket_link(""))
            self.assertEqual(client._summarize_api_params({"search": "{bad"})["search_type"], "raw")
            self.assertEqual(client._summarize_api_params({"search": json.dumps([1, 2])})["search_item_count"], 2)
            self.assertEqual(client._summarize_api_payload({"data": [1, 2]})["data_count"], 2)
            self.assertEqual(client._resolve_project_url(), "https://example.com/me")
            no_token_settings = Settings(
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
                bpmis_api_access_token=None,
            )
            with self.assertRaises(BPMISNotConfiguredError):
                BPMISDirectApiClient(no_token_settings)

    def test_bpmis_remaining_branch_edges_for_full_module_coverage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            client._get_issue_fields = lambda: {}  # type: ignore[method-assign]
            client.ping()
            self.assertEqual(client.find_project("123").project_id, "123")
            self.assertEqual(client._bpmis_release_cutoff_subquery(datetime(2026, 5, 1))["subQueries"][0]["releaseDate"]["gte"], "2026-05-01")
            self.assertEqual(client._extract_team_dashboard_version_id(object()), None)
            self.assertIs(client._with_team_dashboard_release_version_detail({"fixVersionId": 1})["fixVersionId"], 1)
            self.assertEqual(client._enrich_team_dashboard_fix_version_value(999), 999)

            client._api_request = lambda *args, **kwargs: {"data": [{"id": 7, "emailAddress": "user@npt.sg"}, {"id": 8, "displayName": "Other"}]}  # type: ignore[method-assign]
            self.assertEqual(client._resolve_jira_user_id("user@npt.sg"), 7)
            self.assertEqual(client._resolve_jira_user_id("missing"), 7)
            client._api_request = lambda *args, **kwargs: {"data": []}  # type: ignore[method-assign]
            with self.assertRaises(BPMISError):
                client._resolve_jira_user_id("missing")

            version_page_queue = [
                {"data": {"rows": [{"id": index, "fullName": "May Release"} for index in range(100)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: version_page_queue.pop(0)  # type: ignore[method-assign]
            self.assertEqual(len(client.search_versions("May")), 99)
            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": 1, "fullName": "Other"}, {"id": 1, "fullName": "May Release"}]}}  # type: ignore[method-assign]
            self.assertEqual(len(client.search_versions("May")), 1)

            client._api_request = lambda *args, **kwargs: {"data": {"rows": []}}  # type: ignore[method-assign]
            self.assertEqual(client.list_issues_for_version(""), [])
            self.assertEqual(client.list_issues_for_version("5"), [])
            issue_page_queue = [
                {"data": {"rows": [{"id": index, "jiraKey": f"ABC-{index}"} for index in range(200)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: issue_page_queue.pop(0)  # type: ignore[method-assign]
            client._get_issue_details_via_list_bulk = lambda ids: {}  # type: ignore[method-assign]
            with patch.object(client, "get_issue_detail", return_value={}):
                self.assertEqual(len(client.list_issues_for_version("5")), 200)

            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": index, "createdAt": "2026-01-01"} for index in range(200)]}}  # type: ignore[method-assign]
            self.assertEqual(len(client._list_team_dashboard_jira_task_rows_via_list([1], max_pages=1, created_cutoff=datetime(2026, 5, 1))), 200)
            self.assertGreater(client.request_stats["issue_list_created_cutoff_hit"], 0)
            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": index, "createdAt": "2026-01-01"} for index in range(50)]}}  # type: ignore[method-assign]
            self.assertEqual(len(client._list_team_dashboard_jira_task_rows_via_tree_field([1], field_name="reporter", fix_version_ids=[], max_pages=None, created_cutoff=datetime(2026, 5, 1))), 50)
            client._list_team_dashboard_jira_task_rows_via_tree_field = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("tree"))  # type: ignore[method-assign]
            client._list_team_dashboard_jira_task_rows_via_list = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("list"))  # type: ignore[method-assign]
            self.assertIsNone(client._list_team_dashboard_jira_task_rows_via_tree_or_fallback_field([1], field_name="reporter", fix_version_ids=[], max_pages=None, created_cutoff=None))
            client._worker_count = lambda *args: 1  # type: ignore[method-assign]
            client._list_team_dashboard_jira_task_rows_via_tree_or_fallback_field = lambda *args, **kwargs: [{"summary": "No key"}]  # type: ignore[method-assign]
            self.assertEqual(len(client._list_team_dashboard_jira_task_rows_via_tree([1], fix_version_ids=[], max_pages=None, created_cutoff=None)), 2)

            release_page_queue = [
                {"data": {"rows": [{"id": index} for index in range(1000)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: release_page_queue.pop(0)  # type: ignore[method-assign]
            self.assertEqual(client._team_dashboard_release_version_ids(datetime(2026, 1, 1))[0], 1)
            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": "bad"}]}}  # type: ignore[method-assign]
            self.assertEqual(client._team_dashboard_release_version_ids(datetime(2026, 1, 1)), [])

            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": "s1", "typeId": "Sub-task", "storyPoints": "2"}, {"id": "s2", "parentIds": [{"id": "task-a"}], "storyPoints": "1"}, "bad"]}}  # type: ignore[method-assign]
            self.assertEqual(client._sum_open_subtask_story_points_by_project({"": ["x"], "p1": ["", "task-a"]})["p1"], 3.0)
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("boom"))  # type: ignore[method-assign]
            self.assertEqual(client._sum_open_subtask_story_points_by_project({"p1": ["task-a"]})["p1"], 0.0)

            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": "1", "parentIds": [{"id": "p1"}]}, "bad", {"id": "2", "parentIds": [{"id": "p1"}], "typeId": "Other"}]}}  # type: ignore[method-assign]
            self.assertEqual([row["id"] for row in client._list_open_project_task_rows_via_list_bulk(["p1"])["p1"]], ["1"])
            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": index, "parentIds": [{"id": "p1"}]} for index in range(50)]}}  # type: ignore[method-assign]
            self.assertEqual(len(client._list_open_project_task_rows_via_tree("p1")), 50)
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("tree"))  # type: ignore[method-assign]
            self.assertEqual(client._list_open_project_task_rows_via_tree("p1"), [])
            chunk_page_queue = [
                {"data": {"rows": [{"id": "1", "parentIds": [{"id": "p1"}]} for _ in range(200)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: chunk_page_queue.pop(0)  # type: ignore[method-assign]
            self.assertGreaterEqual(len(client._list_jira_task_rows_for_parent_chunk([1], 200)), 200)

            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": "1"}]}}  # type: ignore[method-assign]
            self.assertFalse(client._issue_is_linked_to_parent("ABC-1", 225159))
            client.get_jira_ticket_detail = lambda key: {"id": "77", "parentIds": [{"id": 225159}]}  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad"))  # type: ignore[method-assign]
            self.assertTrue(client._issue_is_linked_to_parent("ABC-1", 225159))
            self.assertEqual(client._find_linked_bpmis_task_id("ABC-1", 225159), "77")

            client._issue_is_linked_to_parent = lambda key, project_id: True  # type: ignore[method-assign]
            client._verified_linked_jira_detail = lambda key, project_id: {"jiraKey": key, "parentIds": [str(project_id)]}  # type: ignore[method-assign]
            self.assertEqual(client.link_jira_ticket_to_project("ABC-1", 225159)["jiraKey"], "ABC-1")
            client._issue_is_linked_to_parent = lambda key, project_id: False  # type: ignore[method-assign]
            client._find_bpmis_task_row_for_jira_key = lambda key: {}  # type: ignore[method-assign]
            client._add_existing_jira_ticket_to_project = lambda key, project_id: None  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "still not linked"):
                client.link_jira_ticket_to_project("ABC-1", 225159)
            client._find_linked_bpmis_task_id = lambda key, project_id: "99"  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: {"data": {}}  # type: ignore[method-assign]
            client._issue_is_linked_to_parent = lambda key, project_id: True  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "still linked"):
                client.delink_jira_ticket_from_project("ABC-1", 225159)

            sleep_calls = []
            with patch("bpmis_jira_tool.bpmis.time.sleep", side_effect=lambda seconds: sleep_calls.append(seconds)):
                client._issue_is_linked_to_parent = lambda key, project_id: False  # type: ignore[method-assign]
                self.assertFalse(client._wait_until_jira_ticket_is_linked("ABC-1", 225159))
            self.assertEqual(sleep_calls, [0.5, 0.5])

            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("safe fail"))  # type: ignore[method-assign]
            self.assertIsNone(client._safe_api_request("/x"))
            client._api_request = lambda *args, **kwargs: {"ok": True}  # type: ignore[method-assign]
            self.assertEqual(client._safe_api_request("/x"), {"ok": True})
            self.assertEqual(client._extract_parent_issue_ids({"parentIssueId": 1}), ["1"])
            self.assertEqual(client._extract_parent_issue_payload({"parentIssueId": {"id": 1}}, "1")["id"], 1)
            self.assertEqual(client._extract_parent_issue_payload({}, "1"), {})
            self.assertFalse(client._value_matches_email([{"email": "other@npt.sg"}], "pm@npt.sg"))
            self.assertFalse(client._value_matches_user("", "pm@npt.sg", {"1"}))
            self.assertFalse(client._is_subtask_row({"typeId": "Bug"}, "1"))
            self.assertFalse(client._is_subtask_row_for_any_task({"typeId": "Task", "parentIds": ["1"]}, {"1"}))
            self.assertEqual(client._extract_story_points({"storyPoints": ""}), 0.0)
            self.assertEqual(client._extract_issue_pm({}), "")
            self.assertEqual(client._extract_issue_version_text({"fixVersionId": "123", "version": "Release"}), "Release")
            self.assertEqual(client._extract_issue_jira_board_text({"jiraBoard": "Board"}, ""), "Board")
            self.assertEqual(client._extract_links(" "), [])
            self.assertEqual(client._extract_market_label(None), "")
            self.assertEqual(client._stringify_person({"unknown": "x"}), "")
            self.assertEqual(client._extract_version_name({}), "")
            self.assertEqual(client._version_sort_key("May", "May"), (0, 3, "may"))
            self.assertIsNone(client._normalize_ticket_link(" "))
            self.assertFalse(client._looks_like_basic_auth_blob(""))
            self.assertEqual(client._resolve_jira_status_id("Done"), None)
            self.assertEqual(client._resolve_biz_project_status_id("Done"), None)

            client._group_options_cache = {"g1": [{"label": "Alpha Beta", "value": 9}]}
            self.assertEqual(client._resolve_option_value({"key": "x", "optionGroup": "g1"}, "Beta"), 9)
            self.assertEqual(client._select_option_groups({"key": "x", "optionGroup": ["a", "b"], "optionGroupFilter": {"match": {"value": [1, 2]}}}, 2), ["b"])
            client._api_request = lambda *args, **kwargs: {"data": [{"id": None}, {"id": 2, "email": "pm@npt.sg"}]}  # type: ignore[method-assign]
            self.assertEqual(client._resolve_bpmis_user_ids_by_emails(["pm@npt.sg"])["pm@npt.sg"], [2])
            client._issue_detail_cache.clear()
            client._store_issue_detail("child", {"id": "child", "parentIds": [{"id": "parent"}]})
            client._prime_biz_project_parent_details([{"parentIds": [{"id": "child"}]}])

    def test_bpmis_final_uncovered_edges_for_coverage_policy(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            rows = [
                "bad",
                {"id": "1", "jiraKey": "ABC-1", "parentIds": [{"id": 1}]},
                {"id": "2", "jiraKey": "ABC-2", "reporter": {"email": "other@npt.sg"}, "parentIds": [{"id": 1}]},
                {"id": "3", "jiraKey": "", "reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": 2}]},
                {"id": "4", "jiraKey": "ABC-4", "reporter": {"email": "pm@npt.sg"}, "parentIds": []},
            ]
            client._worker_count = lambda *args: 1  # type: ignore[method-assign]
            client._list_jira_task_rows_for_parent_chunk = lambda *args, **kwargs: list(rows)  # type: ignore[method-assign]
            client._issue_requires_user_enrichment = lambda row, email: row.get("id") == "1"  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {"id": issue_id, "reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": 1}]}  # type: ignore[method-assign]
            client._get_jira_ticket_details_via_jira_bulk = lambda keys: {"ABC-1": {"jiraKey": "ABC-1", "summary": "Live"}}  # type: ignore[method-assign]
            grouped = client.list_jira_tasks_for_projects_created_by_emails(["1", "2"], ["pm@npt.sg"])
            self.assertEqual([task["jira_id"] for task in grouped["1"]], ["ABC-1"])

            page_queue = [
                {"data": {"rows": [{"id": index} for index in range(200)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: page_queue.pop(0)  # type: ignore[method-assign]
            self.assertEqual(len(client._list_team_dashboard_jira_task_rows_via_list([1], max_pages=None, created_cutoff=None)), 200)

            client._worker_count = lambda *args: 2  # type: ignore[method-assign]
            client._list_team_dashboard_jira_task_rows_via_tree_or_fallback_field = lambda *args, **kwargs: None  # type: ignore[method-assign]
            self.assertIsNone(client._list_team_dashboard_jira_task_rows_via_tree([1], fix_version_ids=[], max_pages=None, created_cutoff=None))
            client._worker_count = lambda *args: 1  # type: ignore[method-assign]
            client._list_team_dashboard_jira_task_rows_via_tree_or_fallback_field = lambda *args, **kwargs: None  # type: ignore[method-assign]
            self.assertIsNone(client._list_team_dashboard_jira_task_rows_via_tree([1], fix_version_ids=[], max_pages=None, created_cutoff=None))

            tree_pages = [
                {"data": {"rows": [{"id": index, "createdAt": "2026-01-01"} for index in range(50)]}},
                {"data": {"rows": []}},
            ]
            client._tree_page_size = lambda: 50  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: tree_pages.pop(0)  # type: ignore[method-assign]
            self.assertEqual(len(client._list_team_dashboard_jira_task_rows_via_tree_field([1], field_name="reporter", fix_version_ids=[], max_pages=None, created_cutoff=datetime(2026, 5, 1))), 50)

            self.assertEqual(client._list_open_project_task_rows_via_list_bulk([]), {})
            client._api_request = lambda *args, **kwargs: {"data": {"rows": ["bad", {"id": "1", "parentIds": [{"id": "p"}]}, {"id": "2", "parentIds": [{"id": "p"}]}]}}  # type: ignore[method-assign]
            self.assertEqual([row["id"] for row in client._list_open_project_task_rows_via_list_bulk(["p"])["p"]], ["1", "2"])
            tree_task_pages = [
                {"data": {"rows": [{"id": str(index), "parentIds": [{"id": "p"}]} for index in range(50)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: tree_task_pages.pop(0)  # type: ignore[method-assign]
            self.assertEqual(len(client._list_open_project_task_rows_via_tree("p")), 50)

            self.assertEqual(client._sum_open_subtask_story_points({}), 0.0)
            subtask_pages = [
                {"data": {"rows": [{"id": "s", "parentIds": [{"id": "task"}], "storyPoints": "1"} for _ in range(200)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: subtask_pages.pop(0)  # type: ignore[method-assign]
            self.assertEqual(client._sum_open_subtask_story_points_for_tasks(["task"]), 200.0)
            by_project_pages = [
                {"data": {"rows": [{"id": "s", "parentIds": [{"id": "task"}], "storyPoints": "1"} for _ in range(200)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: by_project_pages.pop(0)  # type: ignore[method-assign]
            self.assertEqual(client._sum_open_subtask_story_points_by_project({"p": ["task"]})["p"], 200.0)
            self.assertEqual(client._sum_open_subtask_story_points_by_project({"p": [""]})["p"], 0.0)

            client._get_issue_fields = lambda: {"status": "bad"}  # type: ignore[method-assign]
            self.assertIsNone(client._resolve_jira_status_id("Done"))
            self.assertIsNone(client._resolve_biz_project_status_id("Done"))
            self.assertEqual(client._extract_team_dashboard_version_id({"id": object()}), None)
            self.assertIsNone(client._parse_issue_datetime(None))
            self.assertEqual(client._parse_issue_datetime("1700000000000").year, 2023)
            self.assertEqual(client._extract_release_date_from_version_value({"timeline": ["bad"]}), "")
            self.assertEqual(client._extract_issue_version_text({"fixVersionId": "123"}), "123")

            row = {"parentIssueId": {"id": "p", "summary": "Parent"}}
            self.assertEqual(client._extract_parent_issue_payload(row, "p")["summary"], "Parent")
            client._get_parent_issue_detail = lambda issue_id: {}  # type: ignore[method-assign]
            self.assertEqual(client._resolve_biz_project_parent({}, "", {}), client._normalize_team_dashboard_parent_project({}))
            self.assertEqual(client._resolve_biz_project_parent({"parentIds": [{"id": "a"}]}, "a", {"b": {"bpmis_id": "b"}})["bpmis_id"], "")

            client._get_issue_detail_via_list = lambda issue_id: {}  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {"jiraKey": "ABC-1"}  # type: ignore[method-assign]
            self.assertEqual(client._find_bpmis_task_row_for_jira_key(""), {})
            client._safe_api_request = lambda *args, **kwargs: {"data": {"rows": [{"jiraKey": "ABC-1"}]}}  # type: ignore[method-assign]
            self.assertEqual(client._find_bpmis_task_row_for_jira_key("ABC-1")["jiraKey"], "ABC-1")
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad"))  # type: ignore[method-assign]
            client.get_jira_ticket_detail = lambda key: {"raw_jira": {}, "parentIds": [{"id": 1}], "id": "99"}  # type: ignore[method-assign]
            self.assertEqual(client._find_linked_bpmis_task_id("ABC-1", 1), "")

            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad"))  # type: ignore[method-assign]
            client.get_jira_ticket_detail = lambda key: {}  # type: ignore[method-assign]
            with self.assertRaises(ValueError):
                client._issue_is_linked_to_parent("ABC-1", 1)

            client._find_bpmis_task_row_for_jira_key = lambda key: {"id": "99", "parentIds": [{"id": 1}]}  # type: ignore[method-assign]
            client._issue_is_linked_to_parent = lambda key, project_id: False  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("link fail"))  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "Could not link"):
                client.link_jira_ticket_to_project("ABC-1", 1)
            client._find_linked_bpmis_task_id = lambda key, project_id: ""  # type: ignore[method-assign]
            client._issue_is_linked_to_parent = lambda key, project_id: True  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "did not return"):
                client.delink_jira_ticket_from_project("ABC-1", 1)

            client._api_request = lambda *args, **kwargs: {"data": {"created": [{}]}}  # type: ignore[method-assign]
            client._wait_until_jira_ticket_is_linked = lambda *args: False  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "verification"):
                client._add_existing_jira_ticket_to_project("ABC-1", 1)
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad"))  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "Last error"):
                client._add_existing_jira_ticket_to_project("ABC-1", 1)

            client._update_jira_ticket_status_via_jira = lambda key, status: None  # type: ignore[method-assign]
            client.get_jira_ticket_detail = lambda key: {"id": "99", "status": {"label": "Waiting"}}  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: {"data": {}}  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "still 'Waiting'"):
                client.update_jira_ticket_status("ABC-1", "Done")
            client.get_issue_detail = lambda issue_id: {"id": issue_id, "status": {"label": "Testing"}}  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "still 'Testing'"):
                client.update_biz_project_status("1", "UAT")

            client._get_jira_ticket_detail_via_jira = BPMISDirectApiClient._get_jira_ticket_detail_via_jira.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client._get_jira_ticket_details_via_jira_bulk_chunk = BPMISDirectApiClient._get_jira_ticket_details_via_jira_bulk_chunk.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client._update_jira_ticket_status_via_jira = BPMISDirectApiClient._update_jira_ticket_status_via_jira.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client._update_jira_ticket_fix_version_via_jira = BPMISDirectApiClient._update_jira_ticket_fix_version_via_jira.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client._jira_api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("status 500"))  # type: ignore[method-assign]
            with self.assertRaises(BPMISError):
                client._get_jira_ticket_detail_via_jira("ABC-1")
            with self.assertRaises(BPMISError):
                client._get_jira_ticket_details_via_jira_bulk_chunk(["ABC-1"])
            with self.assertRaises(BPMISError):
                client._update_jira_ticket_status_via_jira("ABC-1", "Done")
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token"}, clear=False):
                with self.assertRaises(BPMISError):
                    client._update_jira_ticket_fix_version_via_jira("ABC-1", {"name": "v1"})
            client._get_jira_ticket_detail_via_jira = lambda key: None  # type: ignore[method-assign]
            client._get_jira_ticket_details_via_jira_bulk = BPMISDirectApiClient._get_jira_ticket_details_via_jira_bulk.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client._jira_api_request = lambda *args, **kwargs: {}  # type: ignore[method-assign]
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token"}, clear=False):
                self.assertEqual(client._update_jira_ticket_fix_version_via_jira("ABC-1", {"id": "123"})["fixVersions"], ["123"])
                self.assertEqual(client._get_jira_ticket_details_via_jira_bulk(["ABC-1"] + [f"ABC-{index}" for index in range(2, 103)]), {})

            client._jira_api_request = BPMISDirectApiClient._jira_api_request.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token"}, clear=False):
                with patch("bpmis_jira_tool.bpmis.requests.request", side_effect=requests.RequestException("down")):
                    with self.assertRaisesRegex(BPMISError, "Jira API request failed"):
                        client._jira_api_request("GET", "/down")
                response_401 = self._FakeResponse(401, {"error": "bad"})
                response_403 = self._FakeResponse(403, {"error": "bad"})
                with patch("bpmis_jira_tool.bpmis.requests.request", side_effect=[response_401, response_403]):
                    with self.assertRaisesRegex(BPMISError, "status 403"):
                        client._jira_api_request("GET", "/denied")
                with patch("bpmis_jira_tool.bpmis.requests.request", return_value=self._FakeResponse(200, None, "not-json")):
                    self.assertEqual(client._jira_api_request("GET", "/empty", allow_empty=True), {})

            client._api_request = BPMISDirectApiClient._api_request.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client.session.request = lambda *args, **kwargs: (_ for _ in ()).throw(requests.RequestException("down"))  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "BPMIS API request failed"):
                client._api_request("/down")

    def test_bpmis_last_coverage_gap_edges(self):
        class BadStr:
            def __str__(self):
                raise ValueError("bad str")

        with tempfile.TemporaryDirectory() as temp_dir:
            client = BPMISDirectApiClient(self._settings(temp_dir))

            list_pages = [
                {"data": {"rows": [{"id": index, "jiraKey": f"ABC-{index}", "reporter": {"email": "pm@npt.sg"}} for index in range(200)]}},
                {"data": {"rows": []}},
            ]
            client._api_request = lambda *args, **kwargs: list_pages.pop(0)  # type: ignore[method-assign]
            self.assertEqual(len(client.list_jira_tasks_for_project_created_by_email("1", "pm@npt.sg")), 200)

            rows = [{"reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": 1}]}]
            client._list_jira_task_rows_for_parent_chunk = lambda *args, **kwargs: rows  # type: ignore[method-assign]
            client._worker_count = lambda *args: 1  # type: ignore[method-assign]
            grouped = client.list_jira_tasks_for_projects_created_by_emails(["1"], ["pm@npt.sg"])
            self.assertEqual(grouped["1"], [])
            client._worker_count = lambda *args: 2  # type: ignore[method-assign]
            client._list_jira_task_rows_for_parent_chunk = lambda *args, **kwargs: [{"summary": "No key", "reporter": {"email": "pm@npt.sg"}, "parentIds": [{"id": 1}]}]  # type: ignore[method-assign]
            self.assertEqual(len(client.list_jira_tasks_for_projects_created_by_emails([str(i) for i in range(1, 60)], ["pm@npt.sg"])["1"]), 0)

            client._api_request = lambda *args, **kwargs: {"data": {"rows": []}}  # type: ignore[method-assign]
            self.assertEqual(client._list_team_dashboard_jira_task_rows_via_list([1], max_pages=0, created_cutoff=None), [])
            self.assertEqual(client._extract_team_dashboard_version_id(BadStr()), None)
            client._worker_count = lambda *args: 2  # type: ignore[method-assign]
            client._list_team_dashboard_jira_task_rows_via_tree_or_fallback_field = lambda *args, **kwargs: [{"summary": "No key"}]  # type: ignore[method-assign]
            self.assertEqual(len(client._list_team_dashboard_jira_task_rows_via_tree([1], fix_version_ids=[], max_pages=None, created_cutoff=None)), 2)

            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": 1, "fullName": "May Release"}, {"id": 1, "fullName": "May Release"}]}}  # type: ignore[method-assign]
            self.assertEqual(len(client.search_versions("May")), 1)
            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": 1, "jiraKey": "ABC-1"}, {"id": 1, "jiraKey": "ABC-1"}]}}  # type: ignore[method-assign]
            self.assertEqual(len(client.list_issues_for_version("1")), 1)

            client._extract_issue_rows_from_response = lambda response: ["bad", {"id": "1", "parentIds": [{"id": "p"}], "typeId": "Other"}]  # type: ignore[method-assign]
            self.assertEqual(client._list_open_project_task_rows_via_list_bulk(["p"])["p"], [])
            bulk_pages = [
                {"data": {"rows": [{"id": str(index), "parentIds": [{"id": "p"}]} for index in range(200)]}},
                {"data": {"rows": []}},
            ]
            client._extract_issue_rows_from_response = BPMISDirectApiClient._extract_issue_rows_from_response.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: bulk_pages.pop(0)  # type: ignore[method-assign]
            self.assertEqual(len(client._list_open_project_task_rows_via_list_bulk(["p"])["p"]), 200)
            tree_response_count = {"count": 0}

            def fake_tree_rows(response):
                tree_response_count["count"] += 1
                if tree_response_count["count"] == 1:
                    return [{"id": str(index), "parentIds": [{"id": "p"}]} for index in range(50)]
                return ["bad", {"id": "x", "parentIds": [{"id": "other"}]}]

            client._tree_page_size = lambda: 50  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: {"data": {"rows": []}}  # type: ignore[method-assign]
            client._extract_issue_rows_from_response = fake_tree_rows  # type: ignore[method-assign]
            self.assertEqual(len(client._list_open_project_task_rows_via_tree("p")), 50)
            self.assertEqual(client._sum_open_subtask_story_points("task"), 0.0)

            client._get_jira_ticket_detail_via_jira = lambda key: None  # type: ignore[method-assign]
            detail_calls = {"count": 0}

            def fake_safe_detail(*args, **kwargs):
                detail_calls["count"] += 1
                if detail_calls["count"] <= 6:
                    return None
                return {"data": {"rows": [{"jiraKey": "ABC-1", "summary": "Match without id"}]}}

            client._safe_api_request = fake_safe_detail  # type: ignore[method-assign]
            self.assertEqual(client.get_jira_ticket_detail("ABC-1")["summary"], "Match without id")

            client._api_request = lambda *args, **kwargs: {"data": {}}  # type: ignore[method-assign]
            client.get_issue_detail = lambda issue_id: {"status": ""}  # type: ignore[method-assign]
            with self.assertRaisesRegex(BPMISError, "Could not update BPMIS"):
                client.update_biz_project_status("1", "UAT")

            client._get_issue_fields = lambda: {"marketId": {"optionGroup": "market"}, "taskType": {"optionGroup": "taskType"}}  # type: ignore[method-assign]
            client._group_options_cache = {"market": [{"label": "SG", "value": 1}], "taskType": [{"label": "Feature", "value": 4}]}
            client._api_request = lambda *args, **kwargs: {"data": {"rows": [{"id": 123, "fullName": "Fallback"}]}}  # type: ignore[method-assign]
            payload = client._build_create_payload(ProjectMatch(project_id="1", raw={}), {"Market": "SG", "Task Type": "Feature", "Summary": "S", "Fix Version": "Missing"})
            self.assertEqual(payload["fixVersionId"], [123])

            client.get_jira_ticket_detail = lambda key: {"parentIds": [{"id": 2}]}  # type: ignore[method-assign]
            client._api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("bad"))  # type: ignore[method-assign]
            self.assertFalse(client._issue_is_linked_to_parent("ABC-1", 1))
            client._get_jira_ticket_details_via_jira_bulk_chunk = lambda chunk: None  # type: ignore[method-assign]
            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token"}, clear=False):
                self.assertIsNone(client._get_jira_ticket_details_via_jira_bulk([f"ABC-{index}" for index in range(101)]))
            client._get_jira_ticket_details_via_jira_bulk_chunk = BPMISDirectApiClient._get_jira_ticket_details_via_jira_bulk_chunk.__get__(client, BPMISDirectApiClient)  # type: ignore[method-assign]
            client._jira_api_request = lambda *args, **kwargs: None  # type: ignore[method-assign]
            self.assertIsNone(client._get_jira_ticket_details_via_jira_bulk_chunk(["ABC-1"]))

            with patch.dict(os.environ, {"JIRA_API_TOKEN": "token"}, clear=False):
                client._jira_api_request = lambda *args, **kwargs: (_ for _ in ()).throw(BPMISError("status 401"))  # type: ignore[method-assign]
                self.assertIsNone(client._update_jira_ticket_fix_version_via_jira("ABC-1", {"name": "v1"}))
                client._get_jira_ticket_detail_via_jira = lambda key: None  # type: ignore[method-assign]
                self.assertEqual(client._with_live_jira_fields({"jiraKey": "ABC-1"}, "ABC-1")["jiraKey"], "ABC-1")

            client._get_parent_issue_detail = lambda issue_id: {"id": issue_id, "typeId": "TRD", "parentIds": [{"id": "b"}]}  # type: ignore[method-assign]
            self.assertEqual(client._resolve_biz_project_parent({"parentIds": [{"id": "a"}]}, "a", {"b": {"bpmis_id": "b"}})["bpmis_id"], "b")
            client._get_parent_issue_detail = lambda issue_id: {"id": issue_id, "typeId": "Closed", "status": "Closed"}  # type: ignore[method-assign]
            rows = client._normalize_team_dashboard_biz_project_rows([{"id": "1", "summary": "", "status": ""}])
            self.assertEqual(rows, [])
            self.assertEqual(client._select_option_groups({"key": "x", "optionGroup": ["a", ""]}, None), ["a"])


if __name__ == "__main__":
    unittest.main()
