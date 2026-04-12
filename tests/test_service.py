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
                            "SDLC Approval Status": "",
                            "Business Lead": "1052",
                        },
                    ),
                    InputRow(
                        row_number=4,
                        values={
                            "Issue ID": "ISS-3",
                            "Jira Ticket Link": "https://jira/browse/JIRA-9",
                            "Summary": "Need SDLC approval",
                            "SDLC Approval Status": "",
                            "Business Lead": "1052",
                        },
                    ),
                    InputRow(
                        row_number=5,
                        values={
                            "Issue ID": "ISS-4",
                            "Jira Ticket Link": "https://jira/browse/JIRA-10",
                            "Summary": "Already submitted to SDLC",
                            "SDLC Approval Status": "Y",
                            "Business Lead": "1052",
                        },
                    ),
                    InputRow(
                        row_number=6,
                        values={
                            "Issue ID": "ISS-5",
                            "Jira Ticket Link": "https://jira/browse/JIRA-11",
                            "Summary": "Missing business lead",
                            "SDLC Approval Status": "",
                        },
                    ),
                    InputRow(
                        row_number=7,
                        values={
                            "Issue ID": "ISS-6",
                            "Jira Ticket Link": "https://jira/browse/JIRA-12",
                            "Summary": "Non numeric business lead",
                            "SDLC Approval Status": "",
                            "Business Lead": "lead@example.com",
                        },
                    ),
                ],
                "headers": ["Issue ID", "Jira Ticket Link", "Summary", "SDLC Approval Status", "Business Lead"],
            },
        )()
        self.updates = []
        self.sdlc_updates = []

    def read_snapshot(self):
        return self.snapshot

    def update_success(self, row_number, headers, ticket_value):
        self.updates.append((row_number, headers, ticket_value))

    def update_sdlc_status(self, row_number, headers, status_value):
        self.sdlc_updates.append((row_number, headers, status_value))


class FakeBPMISClient:
    def __init__(self, fail=False):
        self.fail = fail
        self.sdlc_payloads = []

    def find_project(self, issue_id):
        if self.fail:
            raise BPMISError("lookup failed")
        return ProjectMatch(project_id=f"project-{issue_id}")

    def create_jira_ticket(self, project, fields):
        if self.fail:
            raise BPMISError("create failed")
        return CreatedTicket(ticket_key="JIRA-22", ticket_link="https://jira/browse/JIRA-22")

    def submit_sdlc_approval(self, approval):
        if self.fail:
            raise BPMISError("submit failed")
        self.sdlc_payloads.append(approval)
        return {"status": "submitted"}


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

    def test_preview_sdlc_only_marks_rows_with_jira_and_blank_status(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient()
        service.field_mappings_override = [FieldMapping("Summary", "column:Summary"), FieldMapping("Market", "literal:ID")]

        results, _headers = service.preview_sdlc_approval()

        self.assertEqual(results[0].status, "skipped")
        self.assertEqual(results[1].status, "preview")
        self.assertEqual(results[2].status, "preview")
        self.assertEqual(results[3].status, "skipped")
        self.assertEqual(results[4].status, "error")
        self.assertEqual(results[5].status, "preview")

    def test_run_sdlc_submits_and_writes_back_y(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient()
        service.field_mappings_override = [FieldMapping("Summary", "column:Summary"), FieldMapping("Market", "literal:ID")]

        results = service.run_sdlc_approval()

        self.assertEqual(results[1].status, "created")
        self.assertEqual(results[2].status, "created")
        self.assertEqual(service.sheets_service.sdlc_updates[0][0], 3)
        self.assertEqual(service.sheets_service.sdlc_updates[0][2], "Y")
        self.assertEqual(service.sheets_service.sdlc_updates[1][0], 4)
        self.assertEqual(service.bpmis_client.sdlc_payloads[1]["title"], "Need SDLC approval")
        self.assertEqual(service.bpmis_client.sdlc_payloads[1]["content"], "Need SDLC approval")
        self.assertEqual(service.bpmis_client.sdlc_payloads[1]["business_lead"], "1052")


if __name__ == "__main__":
    unittest.main()
