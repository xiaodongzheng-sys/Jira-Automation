import unittest

from bpmis_jira_tool.errors import BPMISError
from bpmis_jira_tool.models import CreatedTicket, FieldMapping, InputRow, ProjectMatch
from bpmis_jira_tool.service import JiraCreationService


class FakeSheetsService:
    def __init__(self):
        self.snapshot = type(
            "Snapshot",
            (),
            {
                "field_mappings": [FieldMapping("Summary", "column:Summary")],
                "rows": [
                    InputRow(
                        row_number=2,
                        values={
                            "Issue ID": "ISS-1",
                            "Jira Ticket Link": "",
                            "Summary": "Need Jira ticket",
                        },
                    ),
                    InputRow(
                        row_number=3,
                        values={
                            "Issue ID": "ISS-2",
                            "Jira Ticket Link": "JIRA-1",
                            "Summary": "Already created",
                        },
                    ),
                ],
                "headers": ["Issue ID", "Jira Ticket Link", "Summary"],
            },
        )()
        self.updates = []

    def read_snapshot(self):
        return self.snapshot

    def update_success(self, row_number, headers, ticket_value):
        self.updates.append((row_number, headers, ticket_value))


class FakeBPMISClient:
    def __init__(self, fail=False):
        self.fail = fail

    def find_project(self, issue_id):
        if self.fail:
            raise BPMISError("lookup failed")
        return ProjectMatch(project_id=f"project-{issue_id}")

    def create_jira_ticket(self, project, fields):
        if self.fail:
            raise BPMISError("create failed")
        return CreatedTicket(ticket_key="JIRA-22", ticket_link="https://jira/browse/JIRA-22")


class ServiceTests(unittest.TestCase):
    def test_run_creates_only_eligible_rows(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient()
        service.field_mappings_override = None

        results = service.run(dry_run=False)

        self.assertEqual(results[0].status, "created")
        self.assertEqual(results[1].status, "skipped")
        self.assertEqual(service.sheets_service.updates[0][0], 2)

    def test_run_captures_bpmis_errors(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient(fail=True)
        service.field_mappings_override = None

        results = service.run(dry_run=False)

        self.assertEqual(results[0].status, "error")
        self.assertIn("lookup failed", results[0].message)

    def test_run_uses_web_config_override_mappings(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient()
        service.field_mappings_override = [FieldMapping("Summary", "literal:Override Summary")]

        results = service.run(dry_run=True)

        self.assertEqual(results[0].status, "preview")


if __name__ == "__main__":
    unittest.main()
