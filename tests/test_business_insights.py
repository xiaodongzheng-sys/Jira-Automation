import io
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
    BusinessInsightsStore,
    UNDERWRITING_FUNNEL_REPORT_ID,
    UNDERWRITING_FUNNEL_TABLE,
    build_underwriting_funnel_sql,
    build_underwriting_funnel_workbook,
)
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.web import create_app


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
        self.assertIn("Portfolio Repayment", response.get_data(as_text=True))
        self.assertIn("Limit Utilization", response.get_data(as_text=True))

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
                denied = client.get("/business-insights", follow_redirects=False)

                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x"}
                reports_response = client.get("/api/business-insights/reports?domain=credit-risk")
                sql_response = client.get(f"/api/business-insights/reports/{UNDERWRITING_FUNNEL_REPORT_ID}/sql")
                ingest_response = client.post(
                    f"/api/business-insights/reports/{UNDERWRITING_FUNNEL_REPORT_ID}/ingest",
                    data={"file": (io.BytesIO(_csv_export_bytes()), "underwriting_export.csv")},
                    content_type="multipart/form-data",
                )
                artifact_url = ingest_response.get_json()["artifact"]["url"]
                download_response = client.get(artifact_url)
                download_status = download_response.status_code
                download_mimetype = download_response.mimetype
                download_response.get_data()
                download_response.close()

        self.assertEqual(denied.status_code, 302)
        self.assertEqual(denied.headers["Location"], "/access-denied")
        self.assertEqual(reports_response.status_code, 200)
        self.assertEqual(len(reports_response.get_json()["reports"]), 3)
        self.assertEqual(sql_response.status_code, 200)
        self.assertIn(UNDERWRITING_FUNNEL_TABLE, sql_response.get_json()["sql"])
        self.assertEqual(ingest_response.status_code, 200)
        self.assertEqual(ingest_response.get_json()["artifact"]["row_count"], 3)
        self.assertEqual(download_status, 200)
        self.assertEqual(download_mimetype, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    unittest.main()
