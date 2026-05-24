import importlib
import json
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCENARIO_COVERAGE_PATH = PROJECT_ROOT / "config" / "common_scenario_test_coverage.json"
REQUIRED_AREAS = {
    "source_code_qa",
    "meeting_recorder",
    "team_dashboard",
    "productization_summary",
    "daily_brief",
    "report_intelligence",
    "seatalk_name_mapping",
}


class CommonScenarioCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix = json.loads(SCENARIO_COVERAGE_PATH.read_text(encoding="utf-8"))

    def test_required_common_scenario_areas_are_declared_once(self):
        area_ids = [area.get("id") for area in self.matrix.get("areas", [])]
        self.assertEqual(set(area_ids), REQUIRED_AREAS)
        self.assertEqual(len(area_ids), len(set(area_ids)))

    def test_each_common_scenario_has_automation_and_resolvable_test_selectors(self):
        scenario_ids: set[str] = set()
        for area in self.matrix.get("areas", []):
            scenarios = area.get("scenarios") or []
            self.assertGreaterEqual(len(scenarios), 5, area.get("id"))
            for scenario in scenarios:
                scenario_key = f"{area['id']}::{scenario.get('id')}"
                self.assertNotIn(scenario_key, scenario_ids)
                scenario_ids.add(scenario_key)
                self.assertTrue(str(scenario.get("description") or "").strip(), scenario_key)
                tests = scenario.get("tests") or []
                self.assertGreaterEqual(len(tests), 1, scenario_key)
                for selector in tests:
                    with self.subTest(selector=selector):
                        self._assert_test_selector_resolves(selector)

    def test_browser_smoke_exists_for_highest_risk_interactive_areas(self):
        browser_required = {
            "source_code_qa",
            "meeting_recorder",
            "team_dashboard",
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
