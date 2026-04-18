import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError
from bpmis_jira_tool.models import CreatedTicket, ProjectMatch
from bpmis_jira_tool.upload_create_jira import (
    DEBUG_PAYLOAD_PATH,
    build_fields_from_request,
    create_jira_from_request,
    main,
)


class _FakeBPMISClient:
    def __init__(self, ticket_key="ABC-123", ticket_link="https://jira.shopee.io/browse/ABC-123"):
        self.ticket_key = ticket_key
        self.ticket_link = ticket_link
        self.find_project_calls = []
        self.create_calls = []

    def find_project(self, issue_id):
        self.find_project_calls.append(issue_id)
        return ProjectMatch(project_id=issue_id)

    def create_jira_ticket(self, project, fields):
        self.create_calls.append((project, fields))
        return CreatedTicket(ticket_key=self.ticket_key, ticket_link=self.ticket_link)


class UploadCreateJiraTests(unittest.TestCase):
    def setUp(self):
        self.settings = Settings(
            flask_secret_key="test",
            google_oauth_client_secret_file=Path("google-client-secret.json"),
            google_oauth_redirect_uri=None,
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url=None,
            team_allowed_emails=(),
            team_allowed_email_domains=(),
            team_portal_data_dir=Path("."),
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Input",
            bpmis_base_url="https://bpmis-uat1.uat.npt.seabank.io",
            bpmis_api_access_token=None,
        )
        self.minimal_request = {
            "access_token": "token-123",
            "issue_id": "10001",
            "market": "SG",
            "summary": "Fix login issue",
        }

    def test_build_fields_defaults_task_type_and_maps_optional_fields(self):
        fields, resolved_task_type = build_fields_from_request(
            {
                **self.minimal_request,
                "description": "Detailed description",
                "dev_pic": "dev@example.com",
                "need_uat": "Yes",
            }
        )

        self.assertEqual("Feature", resolved_task_type)
        self.assertEqual("SG", fields["Market"])
        self.assertEqual("Fix login issue", fields["Summary"])
        self.assertEqual("Feature", fields["Task Type"])
        self.assertEqual("Detailed description", fields["Description"])
        self.assertEqual("dev@example.com", fields["Dev PIC"])
        self.assertEqual("Yes", fields["Need UAT"])

    def test_build_fields_requires_access_token_issue_id_market_and_summary(self):
        with self.assertRaisesRegex(ValueError, "access_token, issue_id, market, summary"):
            build_fields_from_request({})

    def test_create_jira_from_request_returns_success_payload(self):
        fake_client = _FakeBPMISClient()

        def client_factory(settings, access_token):
            self.assertEqual(self.settings, settings)
            self.assertEqual("token-123", access_token)
            return fake_client

        response = create_jira_from_request(
            self.minimal_request,
            settings=self.settings,
            client_factory=client_factory,
        )

        self.assertTrue(response["success"])
        self.assertEqual("ABC-123", response["ticket_key"])
        self.assertEqual("https://jira.shopee.io/browse/ABC-123", response["ticket_link"])
        self.assertEqual("10001", response["issue_id"])
        self.assertEqual("Feature", response["resolved_task_type"])
        self.assertEqual(["10001"], fake_client.find_project_calls)
        self.assertEqual("Feature", fake_client.create_calls[0][1]["Task Type"])
        self.assertEqual("Fix login issue", fake_client.create_calls[0][1]["Summary"])

    def test_create_jira_from_request_returns_error_payload_when_client_fails(self):
        def client_factory(settings, access_token):
            raise BPMISError("Could not resolve BPMIS Jira user 'nobody'.")

        response = create_jira_from_request(
            {**self.minimal_request, "task_type": "Support"},
            settings=self.settings,
            client_factory=client_factory,
        )

        self.assertFalse(response["success"])
        self.assertEqual("Support", response["resolved_task_type"])
        self.assertIn("Could not resolve BPMIS Jira user", response["message"])
        self.assertIsNone(response["ticket_key"])
        self.assertIsNone(response["ticket_link"])

    def test_create_jira_from_request_returns_error_payload_when_required_fields_missing(self):
        response = create_jira_from_request(
            {"access_token": "token-123", "issue_id": "10001"},
            settings=self.settings,
            client_factory=lambda settings, access_token: _FakeBPMISClient(),
        )

        self.assertFalse(response["success"])
        self.assertEqual("10001", response["issue_id"])
        self.assertEqual("Feature", response["resolved_task_type"])
        self.assertIn("market, summary", response["message"])

    def test_main_prints_json_error_for_missing_stdin_payload(self):
        stdout = io.StringIO()
        with patch("sys.stdout", stdout):
            exit_code = main("")

        self.assertEqual(1, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["success"])
        self.assertIn("Expected a JSON object on stdin", payload["message"])

    def test_main_includes_debug_payload_path_when_capture_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            debug_path = Path(temp_dir) / "last_bpmis_api_result.json"
            debug_path.write_text("{}", encoding="utf-8")
            stdout = io.StringIO()

            with patch("bpmis_jira_tool.upload_create_jira.DEBUG_PAYLOAD_PATH", debug_path), patch(
                "bpmis_jira_tool.upload_create_jira.create_jira_from_request",
                return_value={
                    "success": True,
                    "message": "Created Jira ticket successfully.",
                    "ticket_key": "ABC-123",
                    "ticket_link": "https://jira.shopee.io/browse/ABC-123",
                    "issue_id": "10001",
                    "resolved_task_type": "Feature",
                    "debug_payload_path": str(debug_path),
                },
            ), patch("sys.stdout", stdout):
                exit_code = main(json.dumps(self.minimal_request))

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["success"])
        self.assertEqual(str(debug_path), payload["debug_payload_path"])


if __name__ == "__main__":
    unittest.main()
