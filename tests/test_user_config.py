import tempfile
import unittest
from pathlib import Path

from cryptography.fernet import Fernet

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.user_config import DEFAULT_SHEET_HEADERS, WebConfigStore


class UserConfigStoreTests(unittest.TestCase):
    def test_save_and_load_are_scoped_per_user(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))
            store.save({"spreadsheet_link": "sheet-a"}, user_key="google:user-a@example.com")
            store.save({"spreadsheet_link": "sheet-b"}, user_key="google:user-b@example.com")

            user_a = store.load("google:user-a@example.com")
            user_b = store.load("google:user-b@example.com")

            self.assertEqual(user_a["spreadsheet_link"], "sheet-a")
            self.assertEqual(user_b["spreadsheet_link"], "sheet-b")

    def test_missing_user_config_does_not_fallback_to_shared_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            (temp_path / "jira_web_config.json").write_text(
                '{"spreadsheet_link": "legacy-sheet", "input_tab_name": "AF Projects", "summary_header": "Summary"}',
                encoding="utf-8",
            )
            store = WebConfigStore(temp_path)

            loaded = store.load("google:new-user@example.com")

            self.assertIsNone(loaded)

    def test_migrate_moves_anonymous_config_to_google_user_when_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))
            store.save(
                {"spreadsheet_link": "sheet-anon", "input_tab_name": "AF Projects"},
                user_key="anon:abc123",
            )

            store.migrate("anon:abc123", "google:user@example.com")

            migrated = store.load("google:user@example.com")
            self.assertEqual(migrated["spreadsheet_link"], "")
            self.assertEqual(migrated["input_tab_name"], "Sheet1")

    def test_uses_data_root_and_can_fallback_to_legacy_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            data_root = temp_path / "data"
            legacy_root = temp_path / "legacy"
            legacy_root.mkdir(parents=True, exist_ok=True)
            (legacy_root / "jira_web_config.json").write_text('{"spreadsheet_link": "legacy-sheet"}', encoding="utf-8")

            store = WebConfigStore(data_root, legacy_root=legacy_root)

            self.assertEqual(store.load()["spreadsheet_link"], "legacy-sheet")
            self.assertTrue((data_root / "team_portal.db").exists())

    def test_build_field_mappings_supports_system_market_component_routing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            mappings = store.build_field_mappings(
                {
                    "market_header": "Market",
                    "system_header": "System",
                    "summary_header": "Summary",
                    "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                    "component_default_rules_text": (
                        "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2"
                    ),
                    "task_type_value": "Feature",
                    "priority_value": "P1",
                    "product_manager_value": "pm@npt.sg",
                    "reporter_value": "reporter@npt.sg",
                    "need_uat_by_market": {"SG": "Need UAT"},
                }
            )

            self.assertEqual(mappings[0].jira_field, "Market")
            self.assertEqual(mappings[1].jira_field, "System")
            self.assertEqual(mappings[3].jira_field, "Component")
            self.assertTrue(mappings[3].source.startswith("component_routes:"))
            assignee_mapping = next(mapping for mapping in mappings if mapping.jira_field == "Assignee")
            self.assertTrue(assignee_mapping.source.startswith("component_defaults:"))
            need_uat_mapping = next(mapping for mapping in mappings if mapping.jira_field == "Need UAT")
            self.assertTrue(need_uat_mapping.source.startswith("market_choices:"))

    def test_normalize_sets_default_priority_for_new_user_config(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            normalized = store._normalize({})

            self.assertEqual(normalized["task_type_value"], "Feature")
            self.assertEqual(normalized["priority_value"], "P1")

    def test_component_route_rules_reject_duplicate_system_market_pairs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            with self.assertRaises(ToolError):
                store.build_field_mappings(
                    {
                        "market_header": "Market",
                        "system_header": "System",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nAF | SG | Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    }
                )

    def test_component_default_rules_reject_duplicate_components(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            with self.assertRaises(ToolError):
                store.build_field_mappings(
                    {
                        "market_header": "Market",
                        "system_header": "System",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": (
                            "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2\n"
                            "DBP-Anti-fraud | owner2@npt.sg | dev2@npt.sg | qa2@npt.sg | Planning_26Q3"
                        ),
                    }
                )

    def test_component_route_rules_allow_same_component_for_multiple_system_market_pairs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            rules = store._parse_component_route_rules(
                "CRMS DWH | ID | DWH_CreditRisk\nCRMS DWH | PH | DWH_CreditRisk"
            )

            self.assertEqual(2, len(rules))
            self.assertEqual("DWH_CreditRisk", rules[0]["component"])
            self.assertEqual("DWH_CreditRisk", rules[1]["component"])

    def test_align_component_defaults_to_routes_keeps_saved_components_and_adds_new_blank_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            aligned = store.align_component_defaults_to_routes(
                "AF | SG | DBP-Anti-fraud\nDC | SG | Deposit",
                "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2\nObsolete | x@npt.sg | x@npt.sg | x@npt.sg | Old",
            )

            self.assertIn("DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2", aligned)
            self.assertIn("Deposit |  |  |  | ", aligned)
            self.assertNotIn("Obsolete", aligned)

    def test_align_component_defaults_to_routes_tolerates_duplicate_existing_component_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            aligned = store.align_component_defaults_to_routes(
                "CRMS DWH | ID | DWH_CreditRisk\nCRMS DWH | PH | DWH_CreditRisk",
                (
                    "DWH_CreditRisk | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2\n"
                    "DWH_CreditRisk | owner2@npt.sg | dev2@npt.sg | qa2@npt.sg | Planning_26Q3"
                ),
            )

            self.assertEqual(
                aligned,
                "DWH_CreditRisk | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
            )

    def test_component_route_rules_require_system_header(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            with self.assertRaises(ToolError):
                store.build_field_mappings(
                    {
                        "market_header": "Market",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                    }
                )

    def test_component_route_rules_require_component_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            with self.assertRaises(ToolError):
                store.build_field_mappings(
                    {
                        "market_header": "Market",
                        "system_header": "System",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                    }
                )

    def test_normalize_backfills_market_header_from_sync_market_header(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            normalized = store._normalize(
                {
                    "sync_market_header": "Market",
                    "system_header": "System",
                }
            )

            self.assertEqual(normalized["market_header"], "Market")

    def test_normalize_uses_default_sheet_headers_for_first_time_users(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            normalized = store._normalize({})

            self.assertEqual(normalized["input_tab_name"], "Sheet1")
            self.assertEqual(normalized["issue_id_header"], DEFAULT_SHEET_HEADERS[0])
            self.assertEqual(normalized["summary_header"], DEFAULT_SHEET_HEADERS[5])
            self.assertEqual(normalized["description_header"], DEFAULT_SHEET_HEADERS[7])

    def test_normalize_recovers_legacy_component_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            normalized = store._normalize(
                {
                    "component_by_market": {
                        "ID": "DBP-Anti-fraud",
                        "SG": "DBP-Anti-fraud",
                        "PH": "DBP-Anti-fraud",
                        "Regional": "Anti-fraud",
                    },
                    "assignee_value": "xiaodong.zheng@npt.sg",
                    "dev_pic_value": "xiaodong.zheng@npt.sg",
                    "qa_pic_value": "xiaodong.zheng@npt.sg",
                    "fix_version_value": "Planning_26Q2",
                }
            )

            self.assertTrue(normalized["legacy_component_defaults_recovered"])
            self.assertIn(
                "DBP-Anti-fraud | xiaodong.zheng@npt.sg | xiaodong.zheng@npt.sg | xiaodong.zheng@npt.sg | Planning_26Q2",
                normalized["component_default_rules_text"],
            )
            self.assertIn(
                "Anti-fraud | xiaodong.zheng@npt.sg | xiaodong.zheng@npt.sg | xiaodong.zheng@npt.sg | Planning_26Q2",
                normalized["component_default_rules_text"],
            )

    def test_save_encrypts_bpmis_token_and_load_decrypts_it(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir), encryption_key=Fernet.generate_key().decode("utf-8"))

            store.save({"bpmis_api_access_token": "portal-token"}, user_key="google:user@example.com")

            raw_row = store._fetch_row("google:user@example.com")
            self.assertIn('"bpmis_api_access_token": "enc:', raw_row)
            loaded = store.load("google:user@example.com")
            self.assertEqual(loaded["bpmis_api_access_token"], "portal-token")


if __name__ == "__main__":
    unittest.main()
