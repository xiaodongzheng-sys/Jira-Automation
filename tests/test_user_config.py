import tempfile
import unittest
from pathlib import Path

from bpmis_jira_tool.user_config import WebConfigStore


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

    def test_migrate_moves_anonymous_config_to_google_user_when_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebConfigStore(Path(temp_dir))
            store.save({"spreadsheet_link": "sheet-anon"}, user_key="anon:abc123")

            store.migrate("anon:abc123", "google:user@example.com")

            migrated = store.load("google:user@example.com")
            self.assertEqual(migrated["spreadsheet_link"], "sheet-anon")

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


if __name__ == "__main__":
    unittest.main()
