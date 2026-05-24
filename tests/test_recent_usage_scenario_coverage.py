import importlib
import json
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECENT_USAGE_COVERAGE_PATH = PROJECT_ROOT / "config" / "recent_usage_scenario_test_coverage.json"

REQUIRED_RECENT_USAGE_AREAS = {
    "auth_access_control",
    "portal_home",
    "team_dashboard",
    "bpmis_projects",
    "local_agent_proxy",
    "source_code_qa",
    "meeting_recorder",
    "prd_self_assessment",
    "monthly_report",
    "report_intelligence_seatalk",
    "productization_summary",
    "vpn_connection",
}

REQUIRED_EVIDENCE_SOURCES = {
    "local_llm_ledger",
    "host_llm_ledger",
    "host_access_log",
    "cloud_run_logging",
    "host_team_portal_db",
    "host_meeting_records",
    "seatalk_name_mapping_cache",
    "source_code_qa_release_gate",
    "deploy_timings",
}

REQUIRED_CLOUD_RUN_SIGNALS = {
    "GET /",
    "GET /version-plan",
    "GET /team-dashboard",
    "GET /cloud-auth/google/login",
    "GET /cloud-auth/google/callback",
    "POST /cloud-auth/google/logout",
    "GET /api/team-dashboard/config",
    "GET /api/team-dashboard/version-plan/af",
    "GET /api/team-dashboard/version-plan/af/sync-status",
    "POST /api/team-dashboard/version-plan/af/rows including 17 stale revision 409s",
    "POST /api/team-dashboard/version-plan/af/cell",
    "POST /api/team-dashboard/version-plan/af/sync",
    "GET /source-code-qa",
    "GET /api/source-code-qa/sessions",
    "GET /api/source-code-qa/config",
    "GET /prd-self-assessment",
    "GET /api/prd-self-assessment/latest",
    "GET /api/bpmis-projects",
    "GET /reports",
    "GET /vpn-connection",
    "GET /api/vpn-connection/profiles",
}


class RecentUsageScenarioCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix = json.loads(RECENT_USAGE_COVERAGE_PATH.read_text(encoding="utf-8"))

    def test_recent_usage_audit_window_and_sources_are_reviewable(self):
        audit_window = self.matrix.get("audit_window") or {}
        self.assertEqual(audit_window.get("timezone"), "Asia/Singapore")
        self.assertEqual(audit_window.get("start"), "2026-05-10T00:00:00+08:00")
        self.assertEqual(audit_window.get("end"), "2026-05-24T23:59:59+08:00")

        sources = self.matrix.get("evidence_sources") or []
        source_ids = [source.get("id") for source in sources]
        self.assertEqual(set(source_ids), REQUIRED_EVIDENCE_SOURCES)
        self.assertEqual(len(source_ids), len(set(source_ids)))
        for source in sources:
            source_id = source.get("id")
            self.assertTrue(str(source.get("path") or "").strip(), source_id)
            self.assertTrue(str(source.get("summary") or "").strip(), source_id)

        excluded = self.matrix.get("excluded_evidence") or []
        excluded_ids = {item.get("id") for item in excluded}
        self.assertIn("healthz_only", excluded_ids)
        self.assertIn("unknown_llm_flow", excluded_ids)
        self.assertNotIn("cloud_logging", excluded_ids)

    def test_every_recently_used_area_has_observed_usage_and_tests(self):
        evidence_source_ids = {source["id"] for source in self.matrix["evidence_sources"]}
        area_ids = [area.get("id") for area in self.matrix.get("areas", [])]
        self.assertEqual(set(area_ids), REQUIRED_RECENT_USAGE_AREAS)
        self.assertEqual(len(area_ids), len(set(area_ids)))

        scenario_ids: set[str] = set()
        for area in self.matrix.get("areas", []):
            area_id = area.get("id")
            observed_usage = area.get("observed_usage") or []
            self.assertGreaterEqual(len(observed_usage), 1, area_id)
            for evidence in observed_usage:
                with self.subTest(area=area_id, evidence=evidence):
                    self.assertIn(evidence.get("source"), evidence_source_ids)
                    self.assertTrue(str(evidence.get("signal") or "").strip())
                    self.assertIsInstance(evidence.get("count"), int)
                    self.assertGreater(evidence.get("count"), 0)
                    self.assertRegex(str(evidence.get("last_seen") or ""), r"^2026-05-(1[0-9]|2[0-4])$")

            scenarios = area.get("scenarios") or []
            self.assertGreaterEqual(len(scenarios), 1, area_id)
            for scenario in scenarios:
                scenario_key = f"{area_id}::{scenario.get('id')}"
                self.assertNotIn(scenario_key, scenario_ids)
                scenario_ids.add(scenario_key)
                self.assertTrue(str(scenario.get("description") or "").strip(), scenario_key)
                selectors = scenario.get("tests") or []
                self.assertGreaterEqual(len(selectors), 1, scenario_key)
                for selector in selectors:
                    with self.subTest(selector=selector):
                        self._assert_test_selector_resolves(selector)

    def test_recent_browser_risk_scenarios_keep_browser_smoke_coverage(self):
        browser_required = {
            "auth_access_control",
            "portal_home",
            "team_dashboard",
            "source_code_qa",
            "meeting_recorder",
            "prd_self_assessment",
            "monthly_report",
            "vpn_connection",
        }
        for area in self.matrix.get("areas", []):
            if area.get("id") not in browser_required:
                continue
            selectors = [
                selector
                for scenario in area.get("scenarios") or []
                for selector in scenario.get("tests") or []
            ]
            self.assertTrue(
                any(selector.startswith("tests.e2e.portal_smoke_e2e.") for selector in selectors),
                area.get("id"),
            )

    def test_cloud_run_product_routes_are_classified_in_matrix(self):
        cloud_signals = {
            evidence.get("signal")
            for area in self.matrix.get("areas", [])
            for evidence in area.get("observed_usage") or []
            if evidence.get("source") == "cloud_run_logging"
        }
        self.assertEqual(cloud_signals, REQUIRED_CLOUD_RUN_SIGNALS)

    def _assert_test_selector_resolves(self, selector: str) -> None:
        parts = selector.split(".")
        self.assertGreaterEqual(len(parts), 4, selector)
        module_name = ".".join(parts[:-2])
        class_name = parts[-2]
        method_name = parts[-1]
        module = importlib.import_module(module_name)
        test_class = getattr(module, class_name)
        test_method = getattr(test_class, method_name)
        self.assertTrue(callable(test_method), selector)


if __name__ == "__main__":
    unittest.main()
