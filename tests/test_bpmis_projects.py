import tempfile
import unittest
from pathlib import Path

from bpmis_jira_tool.bpmis_projects import BPMISProjectStore, PortalJiraCreationService, PortalProjectSyncService
from bpmis_jira_tool.errors import BPMISError
from bpmis_jira_tool.models import CreatedTicket
from bpmis_jira_tool.user_config import WebConfigStore


class FakeBPMISClient:
    def __init__(self):
        self.create_calls = []
        self.detail_calls = []
        self.status_calls = []
        self.version_calls = []
        self.delink_calls = []

    def list_biz_projects_for_pm_email(self, _email):
        return [
            {"issue_id": "225159", "project_name": "Fraud Rule Upgrade", "market": "SG"},
            {"issue_id": "225160", "project_name": "Deleted Project", "market": "ID"},
        ]

    def list_jira_tasks_for_project_created_by_email(self, project_issue_id, email):
        if str(email).lower() != "pm@npt.sg":
            return []
        return {
            "225159": [
                {
                    "component": "DBP-Anti-fraud",
                    "market": "SG",
                    "system": "AF",
                    "jira_title": "[Feature][AF]Existing synced task",
                    "prd_link": "https://docs/prd-1",
                    "description": "Existing task description",
                    "fix_version_name": "Planning_26Q2",
                    "ticket_key": "AF-EXIST-1",
                    "ticket_link": "https://jira/browse/AF-EXIST-1",
                    "status": "Developing",
                    "raw_response": {"id": 991},
                }
            ],
            "225160": [],
        }.get(str(project_issue_id), [])

    def get_brd_doc_links_for_projects(self, issue_ids):
        return {
            issue_id: {
                "225159": ["https://docs/brd-1"],
                "225160": ["https://docs/brd-2"],
            }.get(issue_id, [])
            for issue_id in issue_ids
        }

    def create_jira_ticket(self, project, fields, *, preformatted_summary=False):
        self.create_calls.append((project, fields, preformatted_summary))
        if fields.get("Component") == "Broken":
            raise BPMISError("component failed")
        return CreatedTicket(ticket_key="AF-1", ticket_link="https://jira/browse/AF-1", raw={"ok": True})

    def get_jira_ticket_detail(self, ticket_key):
        self.detail_calls.append(ticket_key)
        return {
            "jiraKey": ticket_key,
            "summary": "Live Jira title",
            "status": {"label": "In Progress"},
            "fixVersionId": [{"fullName": "Live_26Q2"}],
        }

    def update_jira_ticket_status(self, ticket_key, status):
        self.status_calls.append((ticket_key, status))
        return {"jiraKey": ticket_key, "status": {"label": status}}

    def update_jira_ticket_fix_version(self, ticket_key, version_name, version_id=None):
        self.version_calls.append((ticket_key, version_name, version_id))
        return {"jiraKey": ticket_key, "fixVersions": [version_name]}

    def delink_jira_ticket_from_project(self, ticket_key, project_issue_id):
        self.delink_calls.append((ticket_key, project_issue_id))
        return {"jiraKey": ticket_key, "parentIds": []}


class BPMISProjectStoreTests(unittest.TestCase):
    def test_store_upsert_soft_delete_and_duplicate_tickets(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = BPMISProjectStore(Path(temp_dir) / "team_portal.db")
            self.assertEqual(
                store.upsert_project(
                    user_key="google:pm@npt.sg",
                    bpmis_id="225159",
                    project_name="Fraud Rule",
                    brd_link="https://docs/brd",
                    market="SG",
                ),
                "created",
            )

            first = store.add_jira_ticket(
                user_key="google:pm@npt.sg",
                bpmis_id="225159",
                component="DBP-Anti-fraud",
                market="SG",
                system="AF",
                jira_title="[Feature][AF]Fraud Rule",
                prd_link="",
                description="",
                fix_version_name="Planning_26Q2",
                ticket_key="AF-1",
                ticket_link="https://jira/browse/AF-1",
            )
            second = store.add_jira_ticket(
                user_key="google:pm@npt.sg",
                bpmis_id="225159",
                component="DBP-Anti-fraud",
                market="SG",
                system="AF",
                jira_title="[Feature][AF]Fraud Rule",
                prd_link="",
                description="",
                fix_version_name="Planning_26Q2",
                ticket_key="AF-2",
                ticket_link="https://jira/browse/AF-2",
            )

            projects = store.list_projects(user_key="google:pm@npt.sg")
            self.assertEqual(len(projects), 1)
            self.assertEqual([first["id"], second["id"]], [ticket["id"] for ticket in projects[0]["jira_tickets"]])
            self.assertTrue(store.soft_delete_project(user_key="google:pm@npt.sg", bpmis_id="225159"))
            self.assertEqual(
                store.upsert_project(
                    user_key="google:pm@npt.sg",
                    bpmis_id="225159",
                    project_name="Fraud Rule",
                    brd_link="https://docs/brd",
                    market="SG",
                ),
                "restored",
            )
            restored_projects = store.list_projects(user_key="google:pm@npt.sg")
            self.assertEqual(len(restored_projects), 1)
            self.assertEqual([first["id"], second["id"]], [ticket["id"] for ticket in restored_projects[0]["jira_tickets"]])

    def test_portal_sync_restores_portal_removed_projects_returned_by_bpmis(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = BPMISProjectStore(Path(temp_dir) / "team_portal.db")
            store.upsert_project(
                user_key="google:pm@npt.sg",
                bpmis_id="225160",
                project_name="Deleted Project",
                brd_link="",
                market="ID",
            )
            store.soft_delete_project(user_key="google:pm@npt.sg", bpmis_id="225160")
            service = PortalProjectSyncService(store, FakeBPMISClient())

            results = service.sync_projects(user_key="google:pm@npt.sg", pm_email="pm@npt.sg")

            self.assertEqual(["created", "updated"], [result.status for result in results])
            self.assertEqual("Restored because this BPMIS project is still returned by BPMIS sync.", results[1].message)
            projects = store.list_projects(user_key="google:pm@npt.sg")
            projects_by_id = {project["bpmis_id"]: project for project in projects}
            self.assertEqual(set(projects_by_id), {"225159", "225160"})
            self.assertEqual(projects_by_id["225159"]["brd_link"], "https://docs/brd-1")
            self.assertEqual(projects_by_id["225160"]["brd_link"], "https://docs/brd-2")
            self.assertEqual(projects_by_id["225159"]["jira_tickets"][0]["ticket_key"], "AF-EXIST-1")
            self.assertEqual(projects_by_id["225159"]["jira_tickets"][0]["jira_title"], "[Feature][AF]Existing synced task")

    def test_portal_sync_skips_unchanged_existing_projects(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = BPMISProjectStore(Path(temp_dir) / "team_portal.db")
            store.upsert_project(
                user_key="google:pm@npt.sg",
                bpmis_id="225159",
                project_name="Fraud Rule Upgrade",
                brd_link="https://docs/brd-1",
                market="SG",
            )
            service = PortalProjectSyncService(store, FakeBPMISClient())

            results = service.sync_projects(user_key="google:pm@npt.sg", pm_email="pm@npt.sg")

            self.assertEqual(["skipped", "created"], [result.status for result in results])
            self.assertEqual(
                "Skipped because this BPMIS project is already up to date. Synced 1 existing Jira task created by this user.",
                results[0].message,
            )

    def test_portal_sync_dedupes_existing_jira_tasks_on_repeated_sync(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = BPMISProjectStore(Path(temp_dir) / "team_portal.db")
            service = PortalProjectSyncService(store, FakeBPMISClient())

            service.sync_projects(user_key="google:pm@npt.sg", pm_email="pm@npt.sg")
            service.sync_projects(user_key="google:pm@npt.sg", pm_email="pm@npt.sg")

            tickets = store.get_project(user_key="google:pm@npt.sg", bpmis_id="225159")["jira_tickets"]
            self.assertEqual(["AF-EXIST-1"], [ticket["ticket_key"] for ticket in tickets])
            self.assertEqual(tickets[0]["status"], "Developing")

    def test_portal_sync_does_not_import_other_users_jira_tasks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = BPMISProjectStore(Path(temp_dir) / "team_portal.db")
            service = PortalProjectSyncService(store, FakeBPMISClient())

            service.sync_projects(user_key="google:other@npt.sg", pm_email="other@npt.sg")

            project = store.get_project(user_key="google:other@npt.sg", bpmis_id="225159")
            self.assertEqual(project["jira_tickets"], [])

    def test_portal_sync_updates_when_existing_project_fields_change(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = BPMISProjectStore(Path(temp_dir) / "team_portal.db")
            store.upsert_project(
                user_key="google:pm@npt.sg",
                bpmis_id="225159",
                project_name="Old Fraud Rule",
                brd_link="https://docs/brd-1",
                market="SG",
            )
            service = PortalProjectSyncService(store, FakeBPMISClient())

            results = service.sync_projects(user_key="google:pm@npt.sg", pm_email="pm@npt.sg")

            self.assertEqual(["updated", "created"], [result.status for result in results])
            self.assertEqual("Fraud Rule Upgrade", store.get_project(user_key="google:pm@npt.sg", bpmis_id="225159")["project_name"])

    def test_portal_jira_creation_uses_preformatted_title_and_allows_append(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "team_portal.db"
            store = BPMISProjectStore(db_path)
            config_store = WebConfigStore(Path(temp_dir))
            store.upsert_project(
                user_key="google:pm@npt.sg",
                bpmis_id="225159",
                project_name="Fraud Rule Upgrade",
                brd_link="",
                market="SG",
            )
            config = config_store._normalize(
                {
                    "pm_team": "AF",
                    "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                    "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    "priority_value": "P1",
                    "product_manager_value": "pm@npt.sg",
                    "reporter_value": "pm@npt.sg",
                    "biz_pic_value": "pm@npt.sg",
                    "need_uat_by_market": {"SG": "Need UAT_by UAT Team"},
                }
            )
            bpmis_client = FakeBPMISClient()
            service = PortalJiraCreationService(
                store=store,
                bpmis_client=bpmis_client,
                config_store=config_store,
                config_data=config,
            )

            results = service.create_tickets(
                user_key="google:pm@npt.sg",
                bpmis_id="225159",
                items=[
                    {
                        "component": "DBP-Anti-fraud",
                        "market": "SG",
                        "jira_title": "[Feature][AF]Fraud Rule Upgrade",
                        "prd_link": "",
                        "description": "",
                        "fix_version": "Planning_26Q3",
                    },
                    {
                        "component": "DBP-Anti-fraud",
                        "market": "SG",
                        "jira_title": "[Feature][AF]Fraud Rule Upgrade",
                        "prd_link": "",
                        "description": "",
                        "fix_version": "Planning_26Q3",
                    },
                ],
            )

            self.assertEqual(["created", "created"], [result["status"] for result in results])
            self.assertTrue(all(call[2] for call in bpmis_client.create_calls))
            self.assertEqual("[Feature][AF]Fraud Rule Upgrade", bpmis_client.create_calls[0][1]["Summary"])
            self.assertEqual(len(store.list_projects(user_key="google:pm@npt.sg")[0]["jira_tickets"]), 2)

            tickets = service.list_tickets(user_key="google:pm@npt.sg", bpmis_id="225159")
            self.assertNotIn("live_jira_title", tickets[0])
            self.assertEqual(bpmis_client.detail_calls, [])

            tickets = service.list_tickets(user_key="google:pm@npt.sg", bpmis_id="225159", include_live=True)
            self.assertEqual(tickets[0]["live_jira_title"], "Live Jira title")
            self.assertEqual(tickets[0]["live_jira_status"], "In Progress")
            self.assertEqual(tickets[0]["live_fix_version"], "Live_26Q2")
            self.assertEqual(bpmis_client.detail_calls, ["AF-1", "AF-1"])
            updated = service.update_ticket_status(
                user_key="google:pm@npt.sg",
                bpmis_id="225159",
                ticket_id=tickets[0]["id"],
                status="Testing",
            )
            self.assertEqual(bpmis_client.status_calls, [("AF-1", "Testing")])
            self.assertEqual(updated["live_jira_status"], "In Progress")

            updated = service.update_ticket_version(
                user_key="google:pm@npt.sg",
                bpmis_id="225159",
                ticket_id=tickets[0]["id"],
                version_name="Planning_26Q4",
                version_id="991",
            )
            self.assertEqual(bpmis_client.version_calls, [("AF-1", "Planning_26Q4", "991")])
            self.assertEqual(updated["fix_version_name"], "Planning_26Q4")
            self.assertEqual(store.list_projects(user_key="google:pm@npt.sg")[0]["jira_tickets"][0]["fix_version_name"], "Planning_26Q4")
            self.assertEqual(
                service.list_tickets(user_key="google:pm@npt.sg", bpmis_id="225159")[0]["status"],
                "Testing",
            )
            self.assertTrue(service.delete_ticket(user_key="google:pm@npt.sg", bpmis_id="225159", ticket_id=tickets[0]["id"]))
            self.assertEqual(bpmis_client.delink_calls, [("AF-1", "225159")])
            self.assertEqual(len(service.list_tickets(user_key="google:pm@npt.sg", bpmis_id="225159")), 1)


if __name__ == "__main__":
    unittest.main()
