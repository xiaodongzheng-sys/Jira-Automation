import unittest

from bpmis_jira_tool.errors import BPMISError
from bpmis_jira_tool.models import CreatedTicket, FieldMapping, InputRow, ProjectMatch
from bpmis_jira_tool.project_sync import BPMISProjectSyncService
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
                            "Description": "Description text",
                            "Market": "SG",
                            "BRD Link": "",
                        },
                    ),
                    InputRow(
                        row_number=3,
                        values={
                            "Issue ID": "ISS-2",
                            "Jira Ticket Link": "JIRA-1",
                            "Summary": "Already created",
                            "Description": "",
                            "Market": "ID",
                            "BRD Link": "",
                        },
                    ),
                ],
                "headers": ["Issue ID", "Jira Ticket Link", "Summary", "Description", "Market", "BRD Link"],
            },
        )()
        self.updates = []
        self.appends = []

    def read_snapshot(self):
        return self.snapshot

    def update_success(self, row_number, headers, ticket_value):
        self.updates.append((row_number, headers, ticket_value))

    def append_records(self, headers, records):
        self.appends.append((headers, records))


class FakeBPMISClient:
    def __init__(self, fail=False):
        self.fail = fail
        self.find_project_calls = []
        self.create_calls = []

    def find_project(self, issue_id):
        self.find_project_calls.append(issue_id)
        if self.fail:
            raise BPMISError("lookup failed")
        return ProjectMatch(project_id=f"project-{issue_id}")

    def create_jira_ticket(self, project, fields):
        self.create_calls.append((project, fields))
        if self.fail:
            raise BPMISError("create failed")
        return CreatedTicket(ticket_key="JIRA-22", ticket_link="https://jira/browse/JIRA-22")

    def list_biz_projects_for_pm_email(self, _email):
        return [
            {"issue_id": "ISS-1", "project_name": "Existing", "market": "SG"},
            {"issue_id": "ISS-3", "project_name": "New Project", "market": "PH"},
        ]

    def get_single_brd_doc_link_for_project(self, issue_id):
        return {"ISS-1": "https://docs/existing-brd", "ISS-3": "https://docs/new-project-brd"}.get(issue_id, "")

    def get_single_brd_doc_links_for_projects(self, issue_ids):
        return {
            issue_id: {"ISS-1": "https://docs/existing-brd", "ISS-3": "https://docs/new-project-brd"}.get(issue_id, "")
            for issue_id in issue_ids
        }


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

    def test_run_passes_description_when_mapped_from_sheet(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient()
        service.field_mappings_override = [
            FieldMapping("Summary", "column:Summary"),
            FieldMapping("Description", "column:Description"),
        ]

        results = service.run(dry_run=False)

        self.assertEqual(results[0].status, "created")
        self.assertEqual(service.bpmis_client.create_calls[0][1]["Description"], "Description text")

    def test_run_does_not_require_description(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient()
        service.field_mappings_override = [
            FieldMapping("Summary", "column:Summary"),
            FieldMapping("Market", "column:Market"),
            FieldMapping("Description", "column:Missing Description"),
        ]

        results = service.run(dry_run=False)

        self.assertEqual(results[0].status, "created")
        self.assertNotIn("Description", service.bpmis_client.create_calls[0][1])

    def test_preview_does_not_require_system_when_create_would(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient()
        service.field_mappings_override = [
            FieldMapping("Summary", "column:Summary"),
            FieldMapping("Market", "column:Market"),
            FieldMapping("System", "column:Missing System"),
            FieldMapping(
                "Component",
                'component_routes:[{"system":"AF","market":"SG","component":"DBP-Anti-fraud"}]',
            ),
            FieldMapping(
                "Assignee",
                'component_defaults:{"field":"assignee","rules":[{"component":"DBP-Anti-fraud","assignee":"owner@npt.sg"}]}',
            ),
        ]

        preview_results, _headers = service.preview()
        run_results = service.run(dry_run=False)

        self.assertEqual(preview_results[0].status, "preview")
        self.assertEqual(run_results[0].status, "error")
        self.assertIn("System", run_results[0].message)

    def test_preview_does_not_require_summary_when_create_would(self):
        service = JiraCreationService.__new__(JiraCreationService)
        service.sheets_service = FakeSheetsService()
        service.bpmis_client = FakeBPMISClient()
        service.field_mappings_override = [
            FieldMapping("Summary", "column:Missing Summary"),
            FieldMapping("Market", "column:Market"),
        ]

        preview_results, _headers = service.preview()
        run_results = service.run(dry_run=False)

        self.assertEqual(preview_results[0].status, "preview")
        self.assertEqual(preview_results[0].project_label, "ISS-1")
        self.assertEqual(run_results[0].status, "error")
        self.assertIn("Summary", run_results[0].message)

    def test_sync_projects_appends_only_missing_issue_ids(self):
        service = BPMISProjectSyncService(
            sheets_service=FakeSheetsService(),
            bpmis_client=FakeBPMISClient(),
        )

        results = service.sync_projects(
            pm_email="pm@npt.sg",
            issue_id_header="Issue ID",
            project_name_header="Summary",
            market_header="Market",
        )

        self.assertEqual(results[0].status, "skipped")
        self.assertEqual(results[1].status, "created")
        self.assertEqual(results[1].row_number, 4)
        self.assertEqual(
            service.sheets_service.appends[0][1],
            [{"Issue ID": "ISS-3", "Summary": "New Project", "Market": "PH"}],
        )

    def test_sync_projects_writes_brd_link_when_header_is_configured(self):
        service = BPMISProjectSyncService(
            sheets_service=FakeSheetsService(),
            bpmis_client=FakeBPMISClient(),
        )

        results = service.sync_projects(
            pm_email="pm@npt.sg",
            issue_id_header="Issue ID",
            project_name_header="Summary",
            market_header="Market",
            brd_link_header="BRD Link",
        )

        self.assertEqual(results[1].status, "created")
        self.assertEqual(
            service.sheets_service.appends[0][1],
            [
                {
                    "Issue ID": "ISS-3",
                    "Summary": "New Project",
                    "Market": "PH",
                    "BRD Link": "https://docs/new-project-brd",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
