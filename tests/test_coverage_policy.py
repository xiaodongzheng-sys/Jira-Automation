import unittest

from scripts.check_coverage_policy import evaluate_coverage_policy


def _coverage_report(*, governed_covered=10, governed_total=10, risk_covered=8, risk_total=10):
    return {
        "files": {
            "bpmis_jira_tool/config.py": {"summary": {"covered_lines": governed_covered, "num_statements": governed_total}},
            "bpmis_jira_tool/local_agent_client.py": {"summary": {"covered_lines": risk_covered, "num_statements": risk_total}},
            "bpmis_jira_tool/other.py": {"summary": {"covered_lines": 9, "num_statements": 10}},
            "prd_briefing/reviewer.py": {"summary": {"covered_lines": 8, "num_statements": 10}},
        }
    }


class CoveragePolicyTests(unittest.TestCase):
    def test_policy_passes_at_threshold(self):
        policy = {
            "governed": {"min_percent": 100.0, "files": ["bpmis_jira_tool/config.py"]},
            "critical_modules": [{"path": "bpmis_jira_tool/local_agent_client.py", "min_percent": 80.0}],
            "overall": {"label": "runtime", "min_percent": 87.5, "paths": ["bpmis_jira_tool/", "prd_briefing/"]},
        }

        result = evaluate_coverage_policy(_coverage_report(), policy)

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["failures"], [])

    def test_policy_fails_below_critical_threshold(self):
        policy = {
            "governed": {"min_percent": 100.0, "files": ["bpmis_jira_tool/config.py"]},
            "critical_modules": [{"path": "bpmis_jira_tool/local_agent_client.py", "min_percent": 90.0}],
            "overall": {"label": "runtime", "min_percent": 80.0, "paths": ["bpmis_jira_tool/", "prd_briefing/"]},
        }

        result = evaluate_coverage_policy(_coverage_report(), policy)

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["failures"][0]["kind"], "critical_module")
        self.assertEqual(result["failures"][0]["path"], "bpmis_jira_tool/local_agent_client.py")

    def test_policy_fails_if_governed_module_drops_below_100(self):
        policy = {
            "governed": {"min_percent": 100.0, "files": ["bpmis_jira_tool/config.py"]},
            "critical_modules": [],
            "overall": {"label": "runtime", "min_percent": 50.0, "paths": ["bpmis_jira_tool/", "prd_briefing/"]},
        }

        result = evaluate_coverage_policy(_coverage_report(governed_covered=9), policy)

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["failures"][0]["kind"], "governed")
        self.assertEqual(result["failures"][0]["percent"], 90.0)

    def test_policy_fails_below_overall_threshold(self):
        policy = {
            "governed": {"min_percent": 100.0, "files": ["bpmis_jira_tool/config.py"]},
            "critical_modules": [],
            "overall": {"label": "runtime", "min_percent": 90.0, "paths": ["bpmis_jira_tool/", "prd_briefing/"]},
        }

        result = evaluate_coverage_policy(_coverage_report(), policy)

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["failures"][0]["kind"], "overall")
        self.assertEqual(result["failures"][0]["label"], "runtime")


if __name__ == "__main__":
    unittest.main()
