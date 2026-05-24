import tempfile
import unittest
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import patch

from cryptography.fernet import Fernet

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.user_config import DEFAULT_SHEET_HEADERS, FieldMapping, WebConfigStore
from bpmis_jira_tool.vpn_manager import CiscoVPNClient, VPNProfileStore, json_response_payload, vpn_payload


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

    def test_shared_json_load_save_and_clear_are_file_backed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = WebConfigStore(root)

            self.assertIsNone(store.load())
            saved = store.save({"spreadsheet_link": "shared-sheet"})
            loaded = store.load()
            store.clear()

            self.assertEqual(saved["spreadsheet_link"], "shared-sheet")
            self.assertEqual(loaded["spreadsheet_link"], "shared-sheet")
            self.assertIsNone(store.load())

    def test_user_config_persistence_depends_on_same_data_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            first_root = temp_path / "first"
            second_root = temp_path / "second"

            WebConfigStore(first_root).save(
                {"pm_team": "AF", "sync_pm_email": "pm@npt.sg"},
                user_key="google:user@example.com",
            )

            self.assertEqual(
                WebConfigStore(first_root).load("google:user@example.com")["pm_team"],
                "AF",
            )
            self.assertIsNone(WebConfigStore(second_root).load("google:user@example.com"))

    def test_migrate_noops_for_same_missing_or_existing_target_user(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))
            store.save({"spreadsheet_link": "source-sheet"}, user_key="google:source@npt.sg")
            store.save({"spreadsheet_link": "target-sheet"}, user_key="google:target@npt.sg")

            store.migrate("google:source@npt.sg", "google:source@npt.sg")
            store.migrate("google:missing@npt.sg", "google:new@npt.sg")
            store.migrate("google:source@npt.sg", "google:target@npt.sg")
            store.clear("google:source@npt.sg")

            self.assertIsNone(store.load("google:source@npt.sg"))
            self.assertIsNone(store.load("google:new@npt.sg"))
            self.assertEqual(store.load("google:target@npt.sg")["spreadsheet_link"], "target-sheet")

    def test_load_team_profiles_skips_invalid_json_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))
            store.save_team_profile("AF", {"label": "Anti-fraud", "ready": True})
            with sqlite3.connect(store.db_path) as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO team_profile_configs (team_key, profile_json) VALUES (?, ?)",
                    ("BROKEN", "{not-json"),
                )
                connection.commit()

            profiles = store.load_team_profiles()

            self.assertEqual(sorted(profiles), ["AF"])

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

    def test_build_field_mappings_supports_legacy_component_market_choices(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            mappings = store.build_field_mappings(
                {
                    "market_header": "Market",
                    "component_by_market": {"SG": "DBP-Anti-fraud", "ID": "DBP-Indonesia"},
                    "need_uat_by_market": {"SG": "Need UAT"},
                }
            )

            component_mapping = next(mapping for mapping in mappings if mapping.jira_field == "Component")
            self.assertIn("market_choices:", component_mapping.source)
            self.assertIn("DBP-Anti-fraud", component_mapping.source)

    def test_normalize_sets_default_priority_for_new_user_config(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            normalized = store._normalize({})

            self.assertEqual(normalized["task_type_value"], "Feature")
            self.assertEqual(normalized["priority_value"], "P1")

    def test_build_component_default_rules_from_routes_uses_shared_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            defaults = store.build_component_default_rules_from_routes(
                "AF | SG | DBP-Anti-fraud\nFE | SG | FE-Anti-fraud,FE-User",
                assignee="__CURRENT_USER_EMAIL__",
                dev_pic="__CURRENT_USER_EMAIL__",
                qa_pic="__CURRENT_USER_EMAIL__",
                fix_version="Planning_26Q2",
            )

            self.assertIn(
                "DBP-Anti-fraud | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | Planning_26Q2",
                defaults,
            )
            self.assertIn(
                "FE-Anti-fraud,FE-User | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | Planning_26Q2",
                defaults,
            )

    def test_save_and_load_team_profile_generates_component_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            saved = store.save_team_profile(
                "AF",
                {
                    "label": "Anti-fraud",
                    "ready": True,
                    "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nUC | SG | User",
                },
            )
            loaded = store.load_team_profiles()

            self.assertEqual(saved["component_route_rules_text"], "AF | SG | DBP-Anti-fraud\nUC | SG | User")
            self.assertIn(
                "User | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | Planning_26Q2",
                saved["component_default_rules_text"],
            )
            self.assertEqual(saved["component_default_rules_text"], loaded["AF"]["component_default_rules_text"])

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

    def test_component_rules_reject_malformed_rows_and_missing_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            with self.assertRaisesRegex(ToolError, "Invalid System"):
                store._parse_component_route_rules("# comment\nAF | SG")
            with self.assertRaisesRegex(ToolError, "Invalid Component default"):
                store._parse_component_default_rules("# comment\nDBP-Anti-fraud | owner")
            with self.assertRaisesRegex(ToolError, "missing these routed components"):
                store.build_field_mappings(
                    {
                        "market_header": "Market",
                        "system_header": "System",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nDC | SG | Deposit",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
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

    def test_normalize_backfills_blank_sync_headers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            normalized = store._normalize(
                {
                    "sync_project_name_header": "",
                    "sync_market_header": "",
                    "sync_brd_link_header": "",
                    "market_header": "",
                }
            )

            self.assertEqual(normalized["sync_project_name_header"], "Project Name")
            self.assertEqual(normalized["sync_market_header"], "Market")
            self.assertEqual(normalized["sync_brd_link_header"], "BRD Link")
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

    def test_recover_legacy_component_defaults_requires_complete_legacy_payload(self):
        self.assertEqual(WebConfigStore._recover_legacy_component_default_rules({"component_by_market": []}), "")
        self.assertEqual(WebConfigStore._recover_legacy_component_default_rules({"component_by_market": {"SG": "DBP"}}), "")
        self.assertEqual(
            WebConfigStore._recover_legacy_component_default_rules(
                {
                    "component_by_market": {"SG": ""},
                    "assignee_value": "owner@npt.sg",
                    "dev_pic_value": "dev@npt.sg",
                    "qa_pic_value": "qa@npt.sg",
                    "fix_version_value": "Planning_26Q2",
                }
            ),
            "",
        )

    def test_derive_from_sheet_handles_column_market_choices_and_direct_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            derived = store.derive_from_sheet(
                [
                    FieldMapping(jira_field="Summary", source="column B"),
                    FieldMapping(jira_field="Description", source="column:Custom Description"),
                    FieldMapping(jira_field="Need UAT", source='market_choices:{"SG": "Need UAT"}'),
                    FieldMapping(jira_field="Product Manager", source="literal:pm@npt.sg"),
                    FieldMapping(jira_field="Priority", source="P0"),
                    FieldMapping(jira_field="Unknown", source="ignored"),
                    FieldMapping(jira_field="Component", source="market_choices:{bad-json"),
                ],
                ["Issue ID", "Summary From Sheet"],
            )

            self.assertEqual(derived["summary_header"], "Summary From Sheet")
            self.assertEqual(derived["description_header"], "Custom Description")
            self.assertEqual(derived["need_uat_by_market"]["SG"], "Need UAT")
            self.assertEqual(derived["product_manager_value"], "pm@npt.sg")
            self.assertEqual(derived["priority_value"], "P0")
            self.assertEqual(derived["component_by_market"]["SG"], "")

    def test_derive_from_sheet_ignores_malformed_market_choice_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            derived = store.derive_from_sheet(
                [FieldMapping(jira_field="Need UAT", source="market_choices:{bad-json")],
                [],
            )

            self.assertEqual(derived["need_uat_by_market"], {"ID": "", "SG": "", "PH": "", "Regional": ""})

    def test_lenient_component_default_parser_ignores_historical_bad_rows(self):
        parsed = WebConfigStore._parse_component_default_rules_lenient(
            "\n"
            "# comment\n"
            "Malformed | row\n"
            " | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2\n"
            "DBP | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2\n"
            "DBP | owner2@npt.sg | dev2@npt.sg | qa2@npt.sg | Planning_26Q3\n"
        )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["component"], "DBP")

    def test_empty_component_defaults_compose_to_blank(self):
        self.assertEqual(WebConfigStore._compose_component_default_rules([]), "")

    def test_build_component_defaults_deduplicates_reused_components(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))

            defaults = store.build_component_default_rules_from_routes(
                "CRMS DWH | ID | DWH_CreditRisk\nCRMS DWH | PH | DWH_CreditRisk",
                assignee="owner@npt.sg",
                dev_pic="dev@npt.sg",
                qa_pic="qa@npt.sg",
                fix_version="Planning_26Q2",
            )

            self.assertEqual(defaults.count("DWH_CreditRisk"), 1)

    def test_column_letter_supports_multi_letter_columns(self):
        self.assertEqual(WebConfigStore._column_letter(1), "A")
        self.assertEqual(WebConfigStore._column_letter(27), "AA")

    def test_save_encrypts_bpmis_token_and_load_decrypts_it(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir), encryption_key=Fernet.generate_key().decode("utf-8"))

            store.save({"bpmis_api_access_token": "portal-token"}, user_key="google:user@example.com")

            raw_row = store._fetch_row("google:user@example.com")
            self.assertIn('"bpmis_api_access_token": "enc:', raw_row)
            loaded = store.load("google:user@example.com")
            self.assertEqual(loaded["bpmis_api_access_token"], "portal-token")

    def test_encrypted_config_requires_key_and_rejects_invalid_token(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            key = Fernet.generate_key().decode("utf-8")
            encrypted_store = WebConfigStore(root, encryption_key=key)
            encrypted_store.save({"bpmis_api_access_token": "portal-token"}, user_key="google:user@example.com")

            with self.assertRaisesRegex(ToolError, "ENCRYPTION_KEY"):
                WebConfigStore(root).load("google:user@example.com")

            with sqlite3.connect(encrypted_store.db_path) as connection:
                connection.execute(
                    "UPDATE user_configs SET config_json = ? WHERE user_key = ?",
                    ('{"bpmis_api_access_token": "enc:not-a-valid-token"}', "google:user@example.com"),
                )
                connection.commit()
            with self.assertRaisesRegex(ToolError, "Could not decrypt"):
                encrypted_store.load("google:user@example.com")

    def test_serialize_leaves_blank_and_pre_encrypted_tokens_unchanged(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir), encryption_key=Fernet.generate_key().decode("utf-8"))

            serialized = store._serialize_config(
                {
                    "bpmis_api_access_token": "enc:already",
                    "bpmis_secondary_api_access_token": "",
                }
            )

            self.assertEqual(serialized["bpmis_api_access_token"], "enc:already")
            self.assertEqual(serialized["bpmis_secondary_api_access_token"], "")

    def test_vpn_profile_password_is_encrypted_and_update_can_keep_existing_password(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            key = Fernet.generate_key().decode("utf-8")
            store = VPNProfileStore(Path(temp_dir) / "team_portal.db", encryption_key=key)

            saved = store.save_profile(
                {
                    "display_name": "Shopee VPN",
                    "vpn_host": "ShopeeVPN",
                    "username": "vpn-user",
                    "password": "vpn-secret",
                }
            )
            updated = store.save_profile(
                {
                    "id": saved["id"],
                    "display_name": "Shopee VPN SG",
                    "vpn_host": "ShopeeVPN",
                    "username": "vpn-user",
                    "password": "",
                }
            )

            raw_password = store._encrypted_password_for_profile(saved["id"])
            self.assertTrue(raw_password.startswith("enc:"))
            self.assertNotIn("vpn-secret", raw_password)
            loaded = store.get_profile(updated["id"], include_password=True)
            self.assertEqual(loaded["password"], "vpn-secret")
            self.assertEqual(loaded["display_name"], "Shopee VPN SG")

    def test_vpn_profile_rejects_plaintext_password_without_encryption_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = VPNProfileStore(Path(temp_dir) / "team_portal.db")

            with self.assertRaisesRegex(ToolError, "ENCRYPTION_KEY"):
                store.save_profile(
                    {
                        "display_name": "Shopee VPN",
                        "vpn_host": "ShopeeVPN",
                        "username": "vpn-user",
                        "password": "vpn-secret",
                    }
                )

    def test_vpn_profile_store_validation_decrypt_and_payload_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            key = Fernet.generate_key().decode("utf-8")
            store = VPNProfileStore(Path(temp_dir) / "team_portal.db", encryption_key=key)

            for payload, message in [
                ({"vpn_host": "ShopeeVPN", "username": "user", "password": "secret"}, "display name"),
                ({"display_name": "VPN", "username": "user", "password": "secret"}, "host"),
                ({"display_name": "VPN", "vpn_host": "ShopeeVPN", "password": "secret"}, "username"),
                ({"display_name": "VPN", "vpn_host": "ShopeeVPN", "username": "user"}, "password"),
            ]:
                with self.subTest(message=message):
                    with self.assertRaisesRegex(ToolError, message):
                        store.save_profile(payload)

            saved = store.save_profile(
                {
                    "display_name": "Shopee VPN",
                    "vpn_host": "ShopeeVPN",
                    "username": "vpn-user",
                    "password": "vpn-secret",
                }
            )
            self.assertTrue(store.list_profiles()[0]["has_password"])
            store.record_connected(saved["id"])
            self.assertIsNotNone(store.get_profile(saved["id"])["last_connected_at"])
            self.assertEqual(vpn_payload(store.get_profile(saved["id"], include_password=True))["profile"].get("password"), None)
            self.assertEqual(json_response_payload(profiles=[saved], status={"connected": False}, hosts=["ShopeeVPN"])["hosts"], ["ShopeeVPN"])
            store.delete_profile(saved["id"])
            with self.assertRaisesRegex(ToolError, "not found"):
                store.get_profile(saved["id"])

            with self.assertRaisesRegex(ToolError, "profile id"):
                store.get_profile("")
            with self.assertRaisesRegex(ToolError, "invalid"):
                store.get_profile("bad id")

            no_key_store = VPNProfileStore(Path(temp_dir) / "no_key.db")
            with self.assertRaisesRegex(ToolError, "not encrypted"):
                no_key_store._decrypt_secret("plain-secret")
            with self.assertRaisesRegex(ToolError, "required to read"):
                no_key_store._decrypt_secret("enc:token")

            encrypted = store._encrypt_secret("vpn-secret")
            wrong_key_store = VPNProfileStore(Path(temp_dir) / "wrong_key.db", encryption_key=Fernet.generate_key().decode("utf-8"))
            with self.assertRaisesRegex(ToolError, "Could not decrypt"):
                wrong_key_store._decrypt_secret(encrypted)

    @patch("bpmis_jira_tool.vpn_manager.subprocess.run")
    def test_cisco_vpn_connect_feeds_credentials_on_stdin_not_command_args(self, run_mock):
        vpn_bin = "/tmp/fake-cisco-vpn"
        Path(vpn_bin).write_text("", encoding="utf-8")
        run_mock.return_value = subprocess.CompletedProcess(
            args=[vpn_bin],
            returncode=0,
            stdout="state: Connected",
            stderr="",
        )
        client = CiscoVPNClient(vpn_bin=vpn_bin)

        result = client.connect(host="ShopeeVPN", username="vpn-user", password="vpn-secret")

        command = run_mock.call_args_list[0].args[0]
        self.assertEqual(command, [vpn_bin, "-s", "connect", "ShopeeVPN"])
        self.assertNotIn("vpn-secret", command)
        self.assertIn("vpn-secret", run_mock.call_args_list[0].kwargs["input"])
        self.assertTrue(result["connected"])

    @patch("bpmis_jira_tool.vpn_manager.subprocess.run")
    def test_cisco_vpn_connect_rejects_zero_returncode_disconnected_state(self, run_mock):
        vpn_bin = "/tmp/fake-cisco-vpn"
        Path(vpn_bin).write_text("", encoding="utf-8")

        def fake_run(args, **kwargs):
            if args[1:4] == ["-s", "connect", "ShopeeVPN"]:
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="state: Disconnected", stderr="")
            if args[-1] == "stats":
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="Connection State: Disconnected", stderr="")
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="state: Disconnected", stderr="")

        run_mock.side_effect = fake_run
        client = CiscoVPNClient(
            vpn_bin=vpn_bin,
            connect_verify_timeout_seconds=0.1,
            poll_interval_seconds=0.05,
        )

        with self.assertRaisesRegex(ToolError, "did not reach Connected"):
            client.connect(host="ShopeeVPN", username="vpn-user", password="vpn-secret")

    def test_cisco_vpn_connect_restarts_gui_once_when_capability_is_owned_by_gui(self):
        vpn_bin = "/tmp/fake-cisco-vpn"
        Path(vpn_bin).write_text("", encoding="utf-8")
        calls = []
        restarts = []

        def fake_run(args, **kwargs):
            calls.append(args)
            connect_calls = [call for call in calls if call[1:4] == ["-s", "connect", "ShopeeVPN"]]
            if args[1:4] == ["-s", "connect", "ShopeeVPN"] and len(connect_calls) == 1:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout=(
                        "state: Disconnected\n"
                        "error: Connect capability is unavailable. Another Cisco Secure Client application acquired it."
                    ),
                    stderr="",
                )
            if args[1:4] == ["-s", "connect", "ShopeeVPN"]:
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="state: Connecting", stderr="")
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="state: Connected", stderr="")

        client = CiscoVPNClient(
            vpn_bin=vpn_bin,
            process_runner=fake_run,
            app_restarter=lambda: restarts.append("restart"),
            sleeper=lambda _seconds: None,
            connect_verify_timeout_seconds=0.1,
        )

        result = client.connect(host="ShopeeVPN", username="vpn-user", password="vpn-secret")

        self.assertTrue(result["connected"])
        self.assertEqual(restarts, ["restart"])
        self.assertEqual(
            [call for call in calls if call[1:4] == ["-s", "connect", "ShopeeVPN"]],
            [[vpn_bin, "-s", "connect", "ShopeeVPN"], [vpn_bin, "-s", "connect", "ShopeeVPN"]],
        )

    @patch("bpmis_jira_tool.vpn_manager.subprocess.run")
    def test_cisco_vpn_connect_feeds_second_password_on_stdin_and_redacts_it(self, run_mock):
        vpn_bin = "/tmp/fake-cisco-vpn"
        Path(vpn_bin).write_text("", encoding="utf-8")
        run_mock.return_value = subprocess.CompletedProcess(
            args=[vpn_bin],
            returncode=0,
            stdout="notice: second-secret\nstate: Connected",
            stderr="",
        )
        client = CiscoVPNClient(vpn_bin=vpn_bin)

        result = client.connect(
            host="Seabank PH",
            username="vpn-user",
            password="vpn-secret",
            second_password="second-secret",
        )

        command = run_mock.call_args_list[0].args[0]
        stdin_payload = run_mock.call_args_list[0].kwargs["input"]
        self.assertEqual(command, [vpn_bin, "-s", "connect", "Seabank PH"])
        self.assertIn("second-secret", stdin_payload)
        self.assertNotIn("second-secret", result["message"])
        self.assertTrue(result["connected"])

    def test_cisco_vpn_status_hosts_disconnect_and_command_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            vpn_bin = str(Path(temp_dir) / "fake-cisco-vpn")
            Path(vpn_bin).write_text("", encoding="utf-8")
            calls = []

            def fake_run(args, **kwargs):
                calls.append(args)
                command = args[-1]
                if command == "state":
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="state: Connected", stderr="")
                if command == "hosts":
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="  ignored\n> ShopeeVPN\n>   \n> GalaxisVPN", stderr="")
                if command == "disconnect":
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="state: Disconnected", stderr="")
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

            client = CiscoVPNClient(vpn_bin=vpn_bin, process_runner=fake_run)
            self.assertTrue(client.status()["connected"])
            self.assertEqual(client.hosts(), ["ShopeeVPN", "GalaxisVPN"])
            self.assertEqual(client.disconnect()["state"], "Disconnected")

            error_client = CiscoVPNClient(
                vpn_bin=vpn_bin,
                process_runner=lambda args, **kwargs: subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr="boom"),
            )
            with self.assertRaisesRegex(ToolError, "boom"):
                error_client.hosts()
            with self.assertRaisesRegex(ToolError, "boom"):
                error_client.disconnect()

            for kwargs, message in [
                ({"host": "", "username": "u", "password": "p"}, "host"),
                ({"host": "h", "username": "", "password": "p"}, "username"),
                ({"host": "h", "username": "u", "password": ""}, "password"),
            ]:
                with self.subTest(message=message):
                    with self.assertRaisesRegex(ToolError, message):
                        client.connect(**kwargs)

            missing_client = CiscoVPNClient(vpn_bin=str(Path(temp_dir) / "missing"))
            with self.assertRaisesRegex(ToolError, "not found"):
                missing_client.status()

            timeout_client = CiscoVPNClient(
                vpn_bin=vpn_bin,
                process_runner=lambda args, **kwargs: (_ for _ in ()).throw(subprocess.TimeoutExpired(args, 1)),
            )
            with self.assertRaisesRegex(ToolError, "timed out"):
                timeout_client.status()

            oserror_client = CiscoVPNClient(
                vpn_bin=vpn_bin,
                process_runner=lambda args, **kwargs: (_ for _ in ()).throw(OSError("permission denied")),
            )
            with self.assertRaisesRegex(ToolError, "permission denied"):
                oserror_client.status()

    def test_cisco_vpn_connect_failure_restart_wait_and_message_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            vpn_bin = str(Path(temp_dir) / "fake-cisco-vpn")
            Path(vpn_bin).write_text("", encoding="utf-8")
            calls = []
            restarts = []

            def fake_run(args, **kwargs):
                calls.append(args)
                if args[1:4] == ["-s", "connect", "ShopeeVPN"]:
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="state: Connecting", stderr="vpn-user vpn-secret")
                return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

            client = CiscoVPNClient(
                vpn_bin=vpn_bin,
                process_runner=fake_run,
                app_restarter=lambda: restarts.append("restart"),
                sleeper=lambda _seconds: None,
                connect_verify_timeout_seconds=0.1,
            )
            wait_results = iter(
                [
                    {"connected": False, "state": "Disconnected", "message": "Connect capability is unavailable"},
                    {"connected": False, "state": "Disconnected", "message": "state: Disconnected"},
                ]
            )
            client._wait_for_connected = lambda **kwargs: next(wait_results)

            with self.assertRaisesRegex(ToolError, "restarted once"):
                client.connect(host="ShopeeVPN", username="vpn-user", password="vpn-secret")
            self.assertEqual(restarts, ["restart"])

            fail_client = CiscoVPNClient(
                vpn_bin=vpn_bin,
                process_runner=lambda args, **kwargs: subprocess.CompletedProcess(args=args, returncode=1, stdout="state: Disconnected", stderr="vpn-secret"),
            )
            with self.assertRaisesRegex(ToolError, "did not reach Connected"):
                fail_client.connect(host="ShopeeVPN", username="vpn-user", password="vpn-secret")
            self.assertNotIn("vpn-secret", fail_client._sanitize_output("vpn-secret", secrets=["vpn-secret"]))
            self.assertEqual(fail_client._connection_failure_message(""), "Cisco Secure Client did not reach Connected state. Last state: Unknown.")

            empty_status_client = CiscoVPNClient(
                vpn_bin=vpn_bin,
                process_runner=lambda args, **kwargs: subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr=""),
                sleeper=lambda _seconds: None,
                connect_verify_timeout_seconds=0.001,
                poll_interval_seconds=0.001,
            )
            self.assertIn(
                "did not report",
                empty_status_client._wait_for_connected(secrets=[], previous_output="")["message"],
            )

    @patch("bpmis_jira_tool.vpn_manager.subprocess.run")
    def test_cisco_vpn_default_gui_restart_and_wait_helpers_are_bounded(self, run_mock):
        client = CiscoVPNClient(vpn_bin="/tmp/fake-cisco-vpn", sleeper=lambda _seconds: None)
        with patch.object(client, "_wait_for_cisco_gui_exit", side_effect=[False, True]) as wait_mock:
            client._restart_cisco_gui()

        self.assertEqual(run_mock.call_args_list[0].args[0][0], "osascript")
        self.assertEqual(run_mock.call_args_list[1].args[0][0], "pkill")
        self.assertEqual(wait_mock.call_count, 2)

        run_mock.reset_mock()
        run_mock.return_value = subprocess.CompletedProcess(args=["pgrep"], returncode=1, stdout="", stderr="")
        self.assertTrue(client._wait_for_cisco_gui_exit(timeout_seconds=0.1))

        run_mock.return_value = subprocess.CompletedProcess(args=["pgrep"], returncode=0, stdout="123", stderr="")
        self.assertFalse(client._wait_for_cisco_gui_exit(timeout_seconds=0.1))


if __name__ == "__main__":
    unittest.main()
