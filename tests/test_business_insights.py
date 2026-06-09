import io
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
from openpyxl import load_workbook

from bpmis_jira_tool.business_insights import (
    AF_SCENARIOS_ACTIONS_REPORT_ID,
    AF_SCENE_TABLE,
    APPLICATION_DISBURSEMENT_FUNNEL_REPORT_ID,
    BusinessInsightsStore,
    LIMIT_UTILIZATION_REPORT_ID,
    PORTFOLIO_REPAYMENT_REPORT_ID,
    UNDERWRITING_FUNNEL_REPORT_ID,
    UNDERWRITING_FUNNEL_TABLE,
    build_af_scenarios_actions_sql,
    build_application_disbursement_funnel_sql,
    build_limit_utilization_sql,
    build_portfolio_repayment_sql,
    build_underwriting_funnel_mis_sql,
    build_underwriting_funnel_sql,
    build_underwriting_funnel_workbook,
    product_label,
)
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.web import create_app
from scripts.generate_business_insights_live_reports import (
    REPORT_BUILDERS,
    extract_sql_sections,
    write_visualization,
)


FIXED_NOW = datetime(2026, 5, 26, 10, 30, tzinfo=ZoneInfo("Asia/Singapore"))


def _synthetic_underwriting_rows():
    return [
        {
            "underwriting_id": "UW-1",
            "product_code": "CASH_LOAN",
            "sub_product_code": "NEW",
            "application_submission_time": "2026-04-05 09:00:00",
            "borrower_id": "B1",
            "apply_loan_amount": "10000",
            "apply_loan_tenor": "6",
            "credit_score_partner": "720",
            "underwriting_status": "APPROVED",
            "current_stage": "DONE",
            "step": "FINAL",
            "reject_reason": "",
            "create_date": "2026-04-05 09:00:00",
            "modify_date": "2026-04-05 10:00:00",
        },
        {
            "underwriting_id": "UW-2",
            "product_code": "CASH_LOAN",
            "sub_product_code": "REPEAT",
            "application_submission_time": "2026-05-10 11:00:00",
            "borrower_id": "B2",
            "apply_loan_amount": "5000",
            "apply_loan_tenor": "3",
            "credit_score_partner": "510",
            "underwriting_status": "REJECTED",
            "current_stage": "POLICY",
            "step": "RULE",
            "reject_reason": "LOW_SCORE",
            "create_date": "2026-05-10 11:00:00",
            "modify_date": "2026-05-10 11:30:00",
        },
        {
            "underwriting_id": "UW-3",
            "product_code": "PAY_LATER",
            "sub_product_code": "",
            "application_submission_time": "2026-05-12 13:00:00",
            "borrower_id": "B3",
            "apply_loan_amount": "1500",
            "apply_loan_tenor": "1",
            "credit_score_partner": "650",
            "underwriting_status": "PENDING",
            "current_stage": "MANUAL_REVIEW",
            "step": "REVIEW",
            "reject_reason": "",
            "create_date": "2026-05-12 13:00:00",
            "modify_date": "2026-05-12 13:05:00",
        },
    ]


def _csv_export_bytes(rows=None):
    rows = rows or _synthetic_underwriting_rows()
    headers = list(rows[0].keys())
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(str(row.get(header, "")) for header in headers))
    return ("\n".join(lines) + "\n").encode("utf-8")


class BusinessInsightsTests(unittest.TestCase):
    def test_underwriting_sql_uses_previous_month_and_current_mtd(self):
        sql = build_underwriting_funnel_sql(FIXED_NOW)

        self.assertIn("Credit Risk PH - Underwriting Funnel", sql)
        self.assertIn("Duration: Apr 2026 + May 2026 MTD", sql)
        self.assertIn(UNDERWRITING_FUNNEL_TABLE, sql)
        self.assertIn("application_submission_time >= 1774972800000", sql)
        self.assertIn("application_submission_time < 1779811200000", sql)
        self.assertIn("product_code", sql)
        self.assertIn("sub_product_code", sql)

    def test_underwriting_mis_sql_uses_snapshot_and_aggregation_queries(self):
        sql = build_underwriting_funnel_mis_sql(snapshot_pt_date="2026-05-25", now=FIXED_NOW)

        self.assertIn("Credit Risk PH - Underwriting Funnel MIS", sql)
        self.assertIn("Snapshot: 2026-05-25", sql)
        self.assertIn("pt_date = '2026-05-25'", sql)
        self.assertIn("Summary by Product", sql)
        self.assertIn("Product Funnel", sql)
        self.assertIn("Product Reject Reasons", sql)
        self.assertIn("Product Stage Backlog", sql)
        self.assertIn("Sub-product Funnel", sql)

    def test_new_credit_risk_report_sql_builders_use_accessible_tables(self):
        portfolio_sql = build_portfolio_repayment_sql(snapshot_pt_date="2026-05-25", now=FIXED_NOW)
        limit_sql = build_limit_utilization_sql(snapshot_pt_date="2026-05-25", now=FIXED_NOW)
        funnel_sql = build_application_disbursement_funnel_sql(snapshot_pt_date="2026-05-25", now=FIXED_NOW)

        self.assertIn("Credit Risk PH - Portfolio Repayment", portfolio_sql)
        self.assertIn("cbs_ph_bke_loan_core_db_repay_plan_tab_ss", portfolio_sql)
        self.assertIn("Repay Flow Status", portfolio_sql)
        self.assertIn("Credit Risk PH - Limit Utilization", limit_sql)
        self.assertIn("cbs_ph_bke_loan_core_db_credit_limit_tab_ss_d", limit_sql)
        self.assertIn("Utilization Buckets", limit_sql)
        self.assertIn("Credit Risk PH - Application to Disbursement Funnel", funnel_sql)
        self.assertIn("cbs_ph_bke_loan_txn_db_loan_application_tab_ss", funnel_sql)
        self.assertIn("cbs_ph_bke_loan_core_db_disburse_flow_tab_ss", funnel_sql)

    def test_underwriting_workbook_contains_expected_sheets_and_summary_values(self):
        workbook_bytes = build_underwriting_funnel_workbook(_synthetic_underwriting_rows(), now=FIXED_NOW)
        workbook = load_workbook(io.BytesIO(workbook_bytes), data_only=True)

        self.assertEqual(
            workbook.sheetnames,
            [
                "Summary by Product",
                "Product Funnel",
                "Product Reject Reasons",
                "Product Stage Backlog",
                "Sub-product Funnel",
                "Raw Export",
            ],
        )
        summary_rows = list(workbook["Summary by Product"].iter_rows(values_only=True))
        self.assertIn(("Apr 2026", "CASH_LOAN", 1, 1, 0, 0, 1, 10000), summary_rows)
        self.assertIn(("May 2026 MTD", "CASH_LOAN", 1, 0, 1, 0, 0, 5000), summary_rows)
        self.assertIn(("May 2026 MTD", "PAY_LATER", 1, 0, 0, 1, 0, 1500), summary_rows)

    def test_product_codes_are_displayed_as_apollo_product_names_when_known(self):
        rows = [
            {
                **_synthetic_underwriting_rows()[0],
                "underwriting_id": "UW-SPL",
                "product_code": "801",
                "sub_product_code": "101",
            }
        ]
        workbook_bytes = build_underwriting_funnel_workbook(rows, now=FIXED_NOW)
        workbook = load_workbook(io.BytesIO(workbook_bytes), data_only=True)

        summary_rows = list(workbook["Summary by Product"].iter_rows(values_only=True))
        subproduct_rows = list(workbook["Sub-product Funnel"].iter_rows(values_only=True))
        raw_rows = list(workbook["Raw Export"].iter_rows(values_only=True))
        self.assertEqual(product_label("812"), "Credit Card")
        self.assertEqual(product_label("812F"), "Credit Card")
        self.assertEqual(product_label("807"), "Employee Loan")
        self.assertEqual(product_label("108"), "Employee Loan")
        self.assertIn(("Apr 2026", "801", 1, 1, 0, 0, 1, 10000), summary_rows)
        self.assertIn(("Apr 2026", "801", "101", "APPROVED", 1, 1), subproduct_rows)
        self.assertIn("801", raw_rows[1])

    def test_visualization_includes_product_filter_for_product_level_data(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "visualization.html"
            write_visualization(
                output_path,
                report_title="Credit Risk PH - Application to Disbursement Funnel",
                snapshot_pt_date="2026-05-25",
                sheets=[
                    (
                        "Funnel Summary by Product",
                        ["period", "product", "applications", "disbursed_loans", "disbursed_principal", "application_to_disbursement_rate"],
                        [
                            ["Apr 2026", "807", 10, 7, 7000, 0.7],
                            ["Apr 2026", "812F", 20, 10, 10000, 0.5],
                        ],
                    )
                ],
            )
            html = output_path.read_text(encoding="utf-8")

        self.assertIn("data-product-filter", html)
        self.assertIn("807 - Employee Loan", html)
        self.assertIn("812F - Credit Card", html)
        self.assertIn('data-product="807 - Employee Loan"', html)
        self.assertIn("Filters product-level charts and tables", html)
        self.assertIn('data-product-visual="1"', html)
        self.assertIn('document.querySelectorAll("[data-global-visual]")', html)

    def test_visualization_tables_render_all_rows_with_pagination(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "visualization.html"
            rows = [["Apr 2026", "807", index, index - 1, index * 100, 0.5] for index in range(1, 56)]
            write_visualization(
                output_path,
                report_title="Credit Risk PH - Application to Disbursement Funnel",
                snapshot_pt_date="2026-05-25",
                sheets=[
                    (
                        "Funnel Summary by Product",
                        ["period", "product", "applications", "disbursed_loans", "disbursed_principal", "application_to_disbursement_rate"],
                        rows,
                    )
                ],
            )
            html = output_path.read_text(encoding="utf-8")

        self.assertIn("data-table-pagination", html)
        self.assertIn('data-page-size="50"', html)
        self.assertIn("<td class=\"num\">55</td>", html)
        self.assertNotIn("Full data is in Excel", html)
        self.assertNotIn("Showing top", html)

    def test_store_persists_metadata_handles_corrupt_metadata_and_missing_artifact(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = BusinessInsightsStore(Path(temp_dir))
            artifact = store.save_underwriting_export(content=_csv_export_bytes(), filename="export.csv", now=FIXED_NOW)
            reloaded = BusinessInsightsStore(Path(temp_dir))
            report = reloaded.report(UNDERWRITING_FUNNEL_REPORT_ID)

            self.assertEqual(report["artifact"]["id"], artifact["id"])
            metadata, artifact_path = reloaded.artifact_path(artifact["id"])
            self.assertEqual(metadata["row_count"], 3)
            self.assertTrue(artifact_path.exists())
            visualization_path = artifact_path.with_suffix(".html")
            visualization_path.write_text("<html><body>Visualization</body></html>", encoding="utf-8")
            payload = json.loads((Path(temp_dir) / "reports.json").read_text(encoding="utf-8"))
            payload["artifacts"][UNDERWRITING_FUNNEL_REPORT_ID]["visualization_filename"] = visualization_path.name
            (Path(temp_dir) / "reports.json").write_text(json.dumps(payload), encoding="utf-8")
            visualization_metadata, resolved_visualization_path = reloaded.visualization_path(artifact["id"])
            self.assertEqual(visualization_metadata["id"], artifact["id"])
            self.assertEqual(resolved_visualization_path, visualization_path.resolve())

            artifact_path.unlink()
            with self.assertRaises(ToolError):
                reloaded.artifact_path(artifact["id"])

            (Path(temp_dir) / "reports.json").write_text("{broken", encoding="utf-8")
            fallback_report = reloaded.report(UNDERWRITING_FUNNEL_REPORT_ID)
            self.assertIsNone(fallback_report["artifact"])

    def test_business_insights_page_nav_tabs_and_headers_render_for_admin(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "ENV_FILE": os.devnull,
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x"}
                response = client.get("/business-insights")

        self.assertEqual(response.status_code, 200)
        soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        labels = [node.get_text(strip=True) for node in soup.select(".site-switcher-tab")]
        self.assertLess(labels.index("Projects"), labels.index("Business Insights"))
        self.assertLess(labels.index("Business Insights"), labels.index("Others"))
        subtabs = [node.get_text(strip=True) for node in soup.select(".business-insights-tabs .workspace-tab")]
        self.assertEqual(subtabs, ["Anti-fraud", "Credit Risk", "Ops Risk"])
        for table in soup.select(".business-insights-table"):
            headers = [node.get_text(strip=True) for node in table.select("thead th")]
            self.assertEqual(headers, ["Report Name", "Link"])
        self.assertIn("Credit Risk PH - Underwriting Funnel", response.get_data(as_text=True))
        self.assertIn("Credit Risk PH - Portfolio Repayment", response.get_data(as_text=True))
        self.assertIn("Credit Risk PH - Limit Utilization", response.get_data(as_text=True))
        self.assertIn("Credit Risk PH - Application to Disbursement Funnel", response.get_data(as_text=True))
        self.assertIn("Anti-fraud PH - L1+L2 Scenarios, Actions &amp; Auth Steps", response.get_data(as_text=True))
        self.assertEqual(response.get_data(as_text=True).count("Generate SQL"), 5)
        self.assertNotIn("Upload Export", response.get_data(as_text=True))
        self.assertIsNone(soup.select_one("[data-business-insights-upload]"))

    def test_business_insights_access_route_sql_ingest_and_download(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "ENV_FILE": os.devnull,
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                non_admin_page = client.get("/business-insights", follow_redirects=False)
                non_admin_reports = client.get("/api/business-insights/reports?domain=credit-risk")

                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x"}
                reports_response = client.get("/api/business-insights/reports?domain=credit-risk")
                sql_response = client.get(f"/api/business-insights/reports/{UNDERWRITING_FUNNEL_REPORT_ID}/sql")
                sql_download_response = client.get(f"/api/business-insights/reports/{UNDERWRITING_FUNNEL_REPORT_ID}/sql?format=raw&download=1")
                portfolio_sql_response = client.get(f"/api/business-insights/reports/{PORTFOLIO_REPAYMENT_REPORT_ID}/sql?format=raw&download=1")
                limit_sql_response = client.get(f"/api/business-insights/reports/{LIMIT_UTILIZATION_REPORT_ID}/sql?format=raw&download=1")
                funnel_sql_response = client.get(f"/api/business-insights/reports/{APPLICATION_DISBURSEMENT_FUNNEL_REPORT_ID}/sql?format=raw&download=1")
                ingest_response = client.post(
                    f"/api/business-insights/reports/{UNDERWRITING_FUNNEL_REPORT_ID}/ingest",
                    data={"file": (io.BytesIO(_csv_export_bytes()), "underwriting_export.csv")},
                    content_type="multipart/form-data",
                )
                artifact = ingest_response.get_json()["artifact"]
                artifact_url = artifact["url"]
                visualization_filename = artifact["filename"].replace(".xlsx", ".html")
                (Path(temp_dir) / "business_insights" / "artifacts" / visualization_filename).write_text(
                    "<html><body>Credit Risk Visualization</body></html>",
                    encoding="utf-8",
                )
                metadata_path = Path(temp_dir) / "business_insights" / "reports.json"
                payload = json.loads(metadata_path.read_text(encoding="utf-8"))
                payload["artifacts"][UNDERWRITING_FUNNEL_REPORT_ID]["visualization_filename"] = visualization_filename
                metadata_path.write_text(json.dumps(payload), encoding="utf-8")
                page_with_visualization = client.get("/business-insights")
                visualized_reports_response = client.get("/api/business-insights/reports?domain=credit-risk")
                visualization_url = visualized_reports_response.get_json()["reports"][0]["artifact"]["visualization_url"]
                visualization_response = client.get(visualization_url)
                visualization_body = visualization_response.get_data(as_text=True)
                visualization_response.close()
                download_response = client.get(artifact_url)
                download_status = download_response.status_code
                download_mimetype = download_response.mimetype
                download_response.get_data()
                download_response.close()

        self.assertEqual(non_admin_page.status_code, 200)
        self.assertIn("Business Insights", non_admin_page.get_data(as_text=True))
        self.assertEqual(non_admin_reports.status_code, 200)
        self.assertEqual(len(non_admin_reports.get_json()["reports"]), 4)
        self.assertEqual(reports_response.status_code, 200)
        self.assertEqual(len(reports_response.get_json()["reports"]), 4)
        self.assertEqual(sql_response.status_code, 200)
        self.assertIn(UNDERWRITING_FUNNEL_TABLE, sql_response.get_json()["sql"])
        self.assertEqual(sql_download_response.status_code, 200)
        self.assertEqual(sql_download_response.mimetype, "text/plain")
        self.assertIn("attachment; filename=credit-risk-ph-underwriting-funnel.sql", sql_download_response.headers["Content-Disposition"])
        self.assertIn("Summary by Product", sql_download_response.get_data(as_text=True))
        self.assertEqual(portfolio_sql_response.status_code, 200)
        self.assertIn("Portfolio Repayment", portfolio_sql_response.get_data(as_text=True))
        self.assertEqual(limit_sql_response.status_code, 200)
        self.assertIn("Limit Utilization", limit_sql_response.get_data(as_text=True))
        self.assertEqual(funnel_sql_response.status_code, 200)
        self.assertIn("Application to Disbursement Funnel", funnel_sql_response.get_data(as_text=True))
        self.assertEqual(ingest_response.status_code, 200)
        self.assertEqual(ingest_response.get_json()["artifact"]["row_count"], 3)
        self.assertEqual(page_with_visualization.status_code, 200)
        self.assertIn("Open Visualization", page_with_visualization.get_data(as_text=True))
        self.assertEqual(visualization_response.status_code, 200)
        self.assertEqual(visualization_response.mimetype, "text/html")
        self.assertIn("Credit Risk Visualization", visualization_body)
        self.assertEqual(download_status, 200)
        self.assertEqual(download_mimetype, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


class AntiFraudBusinessInsightsTests(unittest.TestCase):
    def test_seeded_reports_include_scenarios_actions_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = BusinessInsightsStore(Path(tmp))
            reports = store.reports("anti-fraud")
        ids = {report["id"] for report in reports}
        self.assertEqual(ids, {AF_SCENARIOS_ACTIONS_REPORT_ID})
        report = reports[0]
        self.assertEqual(report["domain"], "anti-fraud")
        self.assertEqual(report["status"], "generator_ready")
        self.assertIsNone(report["artifact"])

    def test_store_returns_sql_for_scenarios_actions_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = BusinessInsightsStore(Path(tmp))
            sql = store.sql_for_report(AF_SCENARIOS_ACTIONS_REPORT_ID, now=FIXED_NOW)
            self.assertTrue(sql.strip())
            self.assertEqual(
                store.sql_filename_for_report(AF_SCENARIOS_ACTIONS_REPORT_ID),
                f"{AF_SCENARIOS_ACTIONS_REPORT_ID}.sql",
            )

    def test_scenarios_actions_sql_uses_granted_tables_and_snapshot(self):
        sql = build_af_scenarios_actions_sql(snapshot_pt_date="2026-05-25")
        self.assertIn(AF_SCENE_TABLE, sql)
        self.assertEqual(AF_SCENE_TABLE, "ods.mbs_ph_seabank_anti_fraud_db_scene_tab_ss")
        self.assertIn("pt_date = '2026-05-25'", sql)
        # Auth steps are surfaced via the action type label and the flow-config challenge steps.
        self.assertIn("Authentication (Auth Step)", sql)
        self.assertIn("default_step", sql)
        self.assertIn("challenge1_step", sql)

    def test_scenarios_actions_sql_has_numbered_sections_for_generator(self):
        sql = build_af_scenarios_actions_sql(snapshot_pt_date="2026-05-25", now=FIXED_NOW)
        sections = extract_sql_sections(sql)
        self.assertTrue(sections, "scenarios/actions SQL produced no generator sections")
        for section in sections:
            self.assertTrue(section.query.strip())

    def test_scenario_flow_section_exposes_enum_name_columns(self):
        sql = build_af_scenarios_actions_sql(snapshot_pt_date="2026-05-25", now=FIXED_NOW)
        flow_section = next(
            section for section in extract_sql_sections(sql) if section.sheet_name == "Scenario Action Auth Flow"
        )
        for column in (
            "l1_scene_name",
            "l1_enum_name",
            "l2_sub_scene_name",
            "l2_enum_name",
            "action_name",
            "action_enum_name",
            "default_step",
            "challenge5_step",
        ):
            self.assertIn(column, flow_section.query)
        # The flow-config table stores names (not codes), so dims join on name.
        self.assertIn("s.name = f.scene", flow_section.query)
        self.assertIn("ss.name = f.sub_scene", flow_section.query)
        self.assertIn("a.name = f.action", flow_section.query)
        self.assertNotIn("s.code = f.scene", flow_section.query)

    def test_visualization_renders_only_searchable_flow_table(self):
        flow_headers = [
            "l1_scene_name",
            "l1_enum_name",
            "l2_sub_scene_name",
            "l2_enum_name",
            "action_name",
            "action_enum_name",
            "default_step",
            "challenge1_step",
            "challenge2_step",
            "challenge3_step",
            "challenge4_step",
            "challenge5_step",
        ]
        flow_rows = [
            ["Login", "LOGIN", "Password Login", "PWD_LOGIN", "OTP", "SEND_OTP", "step_a", "step_b", "", "", "", ""],
            ["Transfer", "TRANSFER", "P2P", "P2P_TRANSFER", "Face", "FACE_CHECK", "step_c", "", "", "", "", ""],
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "visualization.html"
            write_visualization(
                output_path,
                report_title="Anti-fraud PH - L1+L2 Scenarios, Actions & Auth Steps",
                snapshot_pt_date="2026-05-25",
                sheets=[
                    ("L1 Scenarios", ["l1_scene_code", "l1_scene_name"], [["100", "Login"]]),
                    ("Scenario Action Auth Flow", flow_headers, flow_rows),
                ],
                report_id=AF_SCENARIOS_ACTIONS_REPORT_ID,
            )
            html = output_path.read_text(encoding="utf-8")

        # Only the flow table is rendered, with a search box.
        self.assertIn("data-flow-table", html)
        self.assertIn("data-search", html)
        self.assertIn("Search scene name or step", html)
        for header in flow_headers:
            expected = f'<th class="step">{header}</th>' if header.endswith("_step") else f"<th>{header}</th>"
            self.assertIn(expected, html)
        self.assertIn("Password Login", html)
        self.assertIn("P2P_TRANSFER", html)
        # Step columns get a fixed 50-character width and wrap.
        self.assertIn('<th class="step">default_step</th>', html)
        self.assertIn('<th class="step">challenge5_step</th>', html)
        self.assertIn('<td class="step">step_a</td>', html)
        self.assertIn("th.step,td.step{width:50ch;min-width:50ch;max-width:50ch;white-space:normal;", html)
        # Non-step columns are not tagged.
        self.assertIn("<th>l1_scene_name</th>", html)
        # The other sheets and the generic dashboard chrome are not rendered.
        self.assertNotIn("L1 Scenarios", html)
        self.assertNotIn("data-product-filter", html)
        self.assertNotIn("Data Quality Notes", html)

    def test_latest_snapshot_resolves_to_anchor_table_max_pt_date(self):
        from scripts import generate_business_insights_live_reports as gen

        self.assertEqual(
            gen.REPORT_SNAPSHOT_ANCHOR_TABLE[AF_SCENARIOS_ACTIONS_REPORT_ID],
            "ods.mbs_ph_seabank_anti_fraud_db_biz_scenario_flow_config_tab_df",
        )
        captured = {}

        def fake_run(session, section, *, poll_seconds, max_polls):
            captured["query"] = section.query
            return ["pt_date"], [["2026-06-08"]], "exec-1"

        with patch.object(gen, "run_workbench_query", fake_run):
            resolved = gen.resolve_snapshot_pt_date(
                object(), AF_SCENARIOS_ACTIONS_REPORT_ID, poll_seconds=1, max_polls=1
            )
        self.assertEqual(resolved, "2026-06-08")
        self.assertIn("max(pt_date)", captured["query"])
        self.assertIn("biz_scenario_flow_config_tab_df", captured["query"])

    def test_unknown_report_has_no_snapshot_anchor(self):
        from scripts import generate_business_insights_live_reports as gen

        self.assertIsNone(
            gen.resolve_snapshot_pt_date(object(), "does-not-exist", poll_seconds=1, max_polls=1)
        )

    def test_generator_report_builders_include_scenarios_actions(self):
        self.assertIn(AF_SCENARIOS_ACTIONS_REPORT_ID, REPORT_BUILDERS)
        title, builder = REPORT_BUILDERS[AF_SCENARIOS_ACTIONS_REPORT_ID]
        self.assertTrue(title.startswith("Anti-fraud PH -"))
        self.assertTrue(callable(builder))

    def test_anti_fraud_report_renders_on_page(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "ENV_FILE": os.devnull,
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x"}
                reports_response = client.get("/api/business-insights/reports?domain=anti-fraud")
                page_response = client.get("/business-insights?domain=anti-fraud")

        self.assertEqual(reports_response.status_code, 200)
        returned_ids = {report["id"] for report in reports_response.get_json()["reports"]}
        self.assertEqual(returned_ids, {AF_SCENARIOS_ACTIONS_REPORT_ID})
        self.assertEqual(page_response.status_code, 200)
        self.assertIn(
            "Anti-fraud PH - L1+L2 Scenarios, Actions &amp; Auth Steps",
            page_response.get_data(as_text=True),
        )


if __name__ == "__main__":
    unittest.main()
