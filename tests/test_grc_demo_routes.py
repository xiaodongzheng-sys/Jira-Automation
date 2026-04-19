import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.web import create_app


class GRCDemoRouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
            },
            clear=False,
        ):
            self.app = create_app()
            self.app.testing = True

    def tearDown(self):
        self.temp_dir.cleanup()

    def _login_owner(self, client):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
            session["google_credentials"] = {"token": "x"}

    def test_owner_sees_grc_demo_tab_on_index(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"GRC Demo", response.data)
        self.assertIn(b"/grc-demo/", response.data)

    def test_owner_can_open_grc_demo(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"GRC Demo", response.data)
        self.assertIn(b"Incident Overview", response.data)

    def test_owner_can_open_outsourcing_overview(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/outsourcing-management")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Outsourcing Management", response.data)
        self.assertIn(b"Assessment ID", response.data)
        self.assertIn(b"Outsourcing Arrangement Identifier", response.data)
        self.assertIn(b"Description of Outsourced Service(s)", response.data)
        self.assertIn(b"Status of Assessment", response.data)
        self.assertIn(b"Add New Outsourcing Arrangement", response.data)
        self.assertIn(b"Periodic Review / Contractual Renewal / Change Management", response.data)
        self.assertIn(b"More Search Criteria", response.data)
        self.assertIn(b"All search criteria are applied with AND logic", response.data)
        self.assertNotIn(b'<a class="submenu-item is-active" href="/grc-demo/overview">Incident Management</a>', response.data)
        self.assertNotIn(b'<a class="submenu-item " href="/grc-demo/overview">Incident Management</a>', response.data)
        self.assertNotIn(b'href="/grc-demo/incident/new">Add Incident</a>', response.data)

    def test_owner_can_open_outsourcing_detail_and_assessment(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            detail_response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018")
            assessment_response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/SSA-SG-2026-0042")
            dd_response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/DD-SG-2026-0015?tab=risk-committee")
            dd_submit_response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/DD-SG-2026-0015?tab=submit")

        self.assertEqual(detail_response.status_code, 200)
        self.assertIn(b"OUT-SG-2026-0018", detail_response.data)
        self.assertIn(b"Outsourcing Arrangement Status", detail_response.data)
        self.assertIn(b"Withdrawal Date", detail_response.data)
        self.assertIn(b"Termination Date", detail_response.data)
        self.assertIn(b"Outsourcing Management List", detail_response.data)
        self.assertIn(b"Outsourcing Arrangement Identifier", detail_response.data)
        self.assertIn(b"Category of Provider", detail_response.data)
        self.assertIn(b"Description of Outsourced Service(s)", detail_response.data)
        self.assertIn(b"First Commencement", detail_response.data)
        self.assertIn(b"Action", detail_response.data)
        self.assertIn(b"Edit", detail_response.data)
        self.assertIn(b"Add New Assessment", detail_response.data)
        self.assertIn(b"Select Assessment Event", detail_response.data)
        self.assertNotIn(b"Core Payment Switch Managed Service", detail_response.data)
        self.assertNotIn(b"Other Commencement", detail_response.data)
        self.assertNotIn(b"Date of Request", detail_response.data)
        self.assertNotIn(b"Status of Assessment", detail_response.data)
        self.assertNotIn(b">OUT-SG-2026-0018</a>", detail_response.data)
        self.assertNotIn(b"Confirmation is required to withdraw the outsourcing arrangement", detail_response.data)
        self.assertEqual(assessment_response.status_code, 200)
        self.assertIn(b"SSA-SG-2026-0042", assessment_response.data)
        self.assertIn(b"Details", assessment_response.data)
        self.assertIn(b"Submit Comment", assessment_response.data)
        self.assertIn(b"Review Comment", assessment_response.data)
        self.assertIn(b"Withdraw Comment", assessment_response.data)
        self.assertNotIn(b"Approve Comment", assessment_response.data)
        self.assertIn(b"Supporting Document", assessment_response.data)
        self.assertIn(b"Key Assessment Details", assessment_response.data)
        self.assertIn(b"Name of Contracting Entity", assessment_response.data)
        self.assertIn(b"History", assessment_response.data)
        self.assertIn(b"Quick Search", assessment_response.data)
        self.assertIn(b"Download", assessment_response.data)
        self.assertIn(b"Withdraw", assessment_response.data)
        self.assertNotIn(b"Edit SSA", assessment_response.data)
        self.assertIn(b"Functional Unit Owning the Outsourcing Arrangement", assessment_response.data)
        self.assertIn(b"Date of SignOff", assessment_response.data)
        self.assertIn(b"textarea", assessment_response.data)
        self.assertEqual(dd_response.status_code, 200)
        self.assertIn(b"Risk Committee", dd_response.data)
        self.assertIn(b"Key Assessment Details", dd_response.data)
        self.assertIn(b"Business Self Assessment Identifier", dd_response.data)
        self.assertIn(b"Business Registration Number of Service Provider / Subcontractor", dd_response.data)
        self.assertIn(b"Frequency of audit on the provider", dd_response.data)
        self.assertIn(b"Date of Approval", dd_response.data)
        self.assertIn(b"Assessment ID (tagged SSA)", dd_response.data)
        self.assertIn(b"Reopen Date", dd_response.data)
        self.assertIn(b"Withdrawal Justification", dd_response.data)
        self.assertIn(b"Conditions", dd_response.data)
        self.assertIn(b"Description of the Condition", dd_response.data)
        self.assertIn(b"Approve Comment", dd_response.data)
        self.assertIn(b"When was the outsourcing arrangement notified and/or approved by the Risk Management Committee (RMC)?", dd_response.data)
        self.assertNotIn(b"When was the outsourcing arrangement notified and/or approved by the Risk Oversight Committee (ROC)?", dd_response.data)
        self.assertIn(b"RMC_Minutes_Jan2026.pdf", dd_response.data)
        self.assertIn(b"Download", dd_response.data)
        self.assertIn(b"Reopen", dd_response.data)
        self.assertIn(b"Edit DD", dd_response.data)
        self.assertIn(b"History", dd_response.data)

    def test_outsourcing_assessment_new_and_edit_forms_show_event_specific_content(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            new_response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/new?event=pir")
            edit_response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/SSA-SG-2026-0042/edit")
            dd_new_response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/new?event=dd")
            termination_new_response = client.get("/grc-demo/outsourcing-management/OUT-PH-2026-0007/assessment/new?event=termination")
            new_arrangement_response = client.get("/grc-demo/outsourcing-management/new")

        self.assertEqual(new_response.status_code, 200)
        self.assertIn(b"Add New Post-Implementation Review", new_response.data)
        self.assertIn(b"Key Assessment Details", new_response.data)
        self.assertIn(b"Implementation Review", new_response.data)
        self.assertIn(b"Approve Comment", new_response.data)
        self.assertNotIn(b"Review Comment", new_response.data)
        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b"Edit Service Supplier Assessment", edit_response.data)
        self.assertIn(b"Key Assessment Details", edit_response.data)
        self.assertIn(b"General Information", edit_response.data)
        self.assertIn(b"Nature and Scope of the Third Party Service", edit_response.data)
        self.assertIn(b"Categorization and Materiality", edit_response.data)
        self.assertIn(b"Business Self Assessment Identifier", edit_response.data)
        self.assertIn(b"Business Registration Number of Service Provider / Subcontractor", edit_response.data)
        self.assertIn(b"Supporting Documents", edit_response.data)
        self.assertIn(b"Reopen SSA", edit_response.data)
        self.assertIn(b"Add New DD", edit_response.data)
        self.assertEqual(dd_new_response.status_code, 200)
        self.assertIn(b"Key Assessment Details", dd_new_response.data)
        self.assertIn(b"General Information", dd_new_response.data)
        self.assertIn(b"Nature and Scope of the Third Party Service", dd_new_response.data)
        self.assertIn(b"Categorization and Materiality", dd_new_response.data)
        self.assertIn(b"Submit Comment", dd_new_response.data)
        self.assertIn(b"Review Comment", dd_new_response.data)
        self.assertIn(b"Withdraw Comment", dd_new_response.data)
        self.assertIn(b"Risk Committee", dd_new_response.data)
        self.assertIn(b"Business Self Assessment Identifier", dd_new_response.data)
        self.assertIn(b"Business Registration Number of Service Provider / Subcontractor", dd_new_response.data)
        self.assertIn(b"Input all the cities where the service will be carried out", dd_new_response.data)
        self.assertIn(b"When was the outsourcing arrangement notified and/or approved by the Risk Management Committee (RMC)?", dd_new_response.data)
        self.assertNotIn(b"When was the outsourcing arrangement notified and/or approved by the Risk Oversight Committee (ROC)?", dd_new_response.data)
        self.assertIn(b"Conditions", dd_new_response.data)
        self.assertIn(b"Approve Comment", dd_new_response.data)
        self.assertIn(b"Review Comment", dd_new_response.data)
        self.assertEqual(termination_new_response.status_code, 200)
        self.assertIn(b"Key Assessment Details", termination_new_response.data)
        self.assertIn(b"General Information", termination_new_response.data)
        self.assertIn(b"Termination Details", termination_new_response.data)
        self.assertNotIn(b"Review Comment", termination_new_response.data)
        self.assertIn(b"Withdraw Comment", termination_new_response.data)
        self.assertEqual(new_arrangement_response.status_code, 200)
        self.assertIn(b"Key Assessment Details", new_arrangement_response.data)
        self.assertIn(b"General Information", new_arrangement_response.data)
        self.assertIn(b"Outsourcing Arrangement Identifier", new_arrangement_response.data)
        self.assertIn(b"Name of Contracting Entity", new_arrangement_response.data)
        self.assertIn(b"Functional Unit Owning the Outsourcing Arrangement", new_arrangement_response.data)
        self.assertIn(b"Service Scope", new_arrangement_response.data)
        self.assertIn(b"What is the outsourcing categorization?", new_arrangement_response.data)
        self.assertIn(b"Supporting Document", new_arrangement_response.data)
        self.assertIn(b"Save", new_arrangement_response.data)
        self.assertIn(b"Submit", new_arrangement_response.data)

    def test_assessment_forms_reuse_same_core_fields_as_existing_views(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            ssa_detail = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/SSA-SG-2026-0042")
            ssa_edit = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/SSA-SG-2026-0042/edit")
            dd_detail = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/DD-SG-2026-0015")
            dd_edit = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/DD-SG-2026-0015/edit")
            pir_detail = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/PIR-SG-2026-0006")
            pir_new = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/new?event=pir")
            osv_detail = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/OSV-SG-2026-0003")
            osv_new = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/new?event=on-site-visit")
            term_detail = client.get("/grc-demo/outsourcing-management/OUT-PH-2026-0007/assessment/TERM-ID-2026-0002")
            term_new = client.get("/grc-demo/outsourcing-management/OUT-PH-2026-0007/assessment/new?event=termination")

        for response in [ssa_detail, ssa_edit, dd_detail, dd_edit, pir_detail, pir_new, osv_detail, osv_new, term_detail, term_new]:
            self.assertEqual(response.status_code, 200)

        for field in [
            b"Business Self Assessment Identifier",
            b"Business Registration Number of Service Provider / Subcontractor",
            b"What is the outsourcing categorization?",
        ]:
            self.assertIn(field, ssa_detail.data)
            self.assertIn(field, ssa_edit.data)
            self.assertIn(field, dd_detail.data)
            self.assertIn(field, dd_edit.data)

        for field in [b"Implementation Review", b"Functional Unit Owning the Outsourcing Arrangement", b"Co-PIC"]:
            self.assertIn(field, pir_detail.data)
            self.assertIn(field, pir_new.data)

        for field in [b"On-site Visit Details", b"Functional Unit Owning the Outsourcing Arrangement", b"Co-PIC"]:
            self.assertIn(field, osv_detail.data)
            self.assertIn(field, osv_new.data)

        for field in [b"Termination Details", b"Functional Unit Owning the Outsourcing Arrangement", b"Co-PIC"]:
            self.assertIn(field, term_detail.data)
            self.assertIn(field, term_new.data)

    def test_pir_detail_uses_prd_tabs_and_fu_approval_rows(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/PIR-SG-2026-0006")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Key Assessment Details", response.data)
        self.assertIn(b"Post-Implementation Review", response.data)
        self.assertIn(b"Functional Unit Owning the Outsourcing Arrangement", response.data)
        self.assertIn(b"Approve Comment", response.data)
        self.assertIn(b"Withdraw Comment", response.data)
        self.assertNotIn(b"Review Comment</a>", response.data)
        self.assertIn(b"Download", response.data)
        self.assertIn(b"Edit PIR", response.data)
        self.assertIn(b"Withdraw", response.data)
        self.assertIn(b"History", response.data)

    def test_on_site_detail_uses_prd_tabs_and_fu_approval_rows(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/OSV-SG-2026-0003")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Key Assessment Details", response.data)
        self.assertIn(b"On-site Visit Details", response.data)
        self.assertIn(b"Functional Unit Owning the Outsourcing Arrangement", response.data)
        self.assertIn(b"Approve Comment", response.data)
        self.assertIn(b"Withdraw Comment", response.data)
        self.assertNotIn(b"Review Comment</a>", response.data)
        self.assertIn(b"Download", response.data)
        self.assertNotIn(b"Edit On-site Visit", response.data)
        self.assertIn(b"Withdraw", response.data)
        self.assertIn(b"History", response.data)

    def test_termination_detail_uses_market_specific_tabs(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/outsourcing-management/OUT-PH-2026-0007/assessment/TERM-ID-2026-0002")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Key Assessment Details", response.data)
        self.assertIn(b"Termination Details", response.data)
        self.assertIn(b"Approve Comment", response.data)
        self.assertIn(b"Withdraw Comment", response.data)
        self.assertNotIn(b"Review Comment</a>", response.data)
        self.assertIn(b"Functional Unit Owning the Outsourcing Arrangement", response.data)
        self.assertIn(b"Download", response.data)
        self.assertNotIn(b"Edit Termination", response.data)
        self.assertIn(b"History", response.data)

    def test_authorization_page_links_to_outsourcing_assessment(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/authorization")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"SSA-SG-2026-0042", response.data)
        self.assertIn(b"/grc-demo/outsourcing-management/OUT-SG-2026-0018/assessment/SSA-SG-2026-0042", response.data)
        self.assertIn(b"Outsourcing Authorization Management Requirement", response.data)
        self.assertIn(b"Outsourcing - Report Confirmation", response.data)
        self.assertIn(b"Audit History", response.data)
        self.assertIn(b"Maker cannot approve or review their own request", response.data)
        self.assertIn(b"Audit S/N", response.data)
        self.assertIn(b"Service Ticket Number", response.data)

    def test_outsourcing_approval_detail_uses_assessment_context(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/approval-center/20260419000004")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Outsourcing - Authorize New Assessment", response.data)
        self.assertIn(b"Assessment Snapshot", response.data)
        self.assertIn(b"SSA-SG-2026-0042", response.data)
        self.assertIn(b"Submit Comment", response.data)

    def test_reports_include_outsourcing_specific_section(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/reports")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"MAS Outsourcing Register - Service Provider and Material Subcontractors", response.data)
        self.assertIn(b"MAS Outsourcing Register - Non-material Subcontractors", response.data)
        self.assertIn(b"Termination Withdrawal Report", response.data)
        self.assertIn(b"Available Reports", response.data)
        self.assertIn(b"Report Confirmation Requirement", response.data)
        self.assertIn(b"Email Routing", response.data)
        self.assertIn(b"Export Confirmation", response.data)
        self.assertIn(b"Download XLSX", response.data)
        self.assertIn(b"Today means real-time data up to the current timestamp", response.data)

    def test_access_control_includes_outsourcing_permissions_and_audit_history(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/grc-demo/access-control")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"GRC Access Control", response.data)
        self.assertIn(b"Incident Management Role Permissions", response.data)
        self.assertIn(b"Outsourcing Management Role Permissions", response.data)
        self.assertIn(b"Outsourcing Data Access Control", response.data)
        self.assertIn(b"Overview - View", response.data)
        self.assertIn(b"Overview - Add", response.data)
        self.assertIn(b"Details - View", response.data)

    def test_parameter_and_email_management_include_outsourcing_supporting_content(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            parameter_response = client.get("/grc-demo/parameter-management")
            email_response = client.get("/grc-demo/email-management")
            parameter_view_response = client.get("/grc-demo/parameter-management/Para10002")
            parameter_edit_response = client.get("/grc-demo/parameter-management/Para10005/edit")
            email_edit_response = client.get("/grc-demo/email-management/EML-1001/edit")

        self.assertEqual(parameter_response.status_code, 200)
        self.assertIn(b"Outsourcing Management Parameter List", parameter_response.data)
        self.assertIn(b"Service Scope", parameter_response.data)
        self.assertIn(b"What is the outsourcing categorization?", parameter_response.data)
        self.assertEqual(email_response.status_code, 200)
        self.assertIn(b"Outsourcing Management Email Templates", email_response.data)
        self.assertIn(b"SG / PH Outsourcing ORM Report", email_response.data)
        self.assertEqual(parameter_view_response.status_code, 200)
        self.assertIn(b"Outsourcing Management", parameter_view_response.data)
        self.assertIn(b"IT Scope", parameter_view_response.data)
        self.assertEqual(parameter_edit_response.status_code, 200)
        self.assertIn(b"Frequency of audit on the provider", parameter_edit_response.data)
        self.assertEqual(email_edit_response.status_code, 200)
        self.assertIn(b"SG / PH Outsourcing ORM Report", email_edit_response.data)

    def test_non_owner_cannot_open_grc_demo_or_see_tab(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}

            index_response = client.get("/")
            route_response = client.get("/grc-demo/", follow_redirects=False)

        self.assertEqual(index_response.status_code, 200)
        self.assertNotIn(b"GRC Demo", index_response.data)
        self.assertEqual(route_response.status_code, 302)
        self.assertEqual(route_response.headers["Location"], "/")


if __name__ == "__main__":
    unittest.main()
