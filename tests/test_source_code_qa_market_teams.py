import io
import json
import os
from pathlib import Path
import random
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

from bpmis_jira_tool import public_artifacts_gcs as gcs
from bpmis_jira_tool import source_code_qa_repo_downloads as bundles
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa import MARKET_TEAM_PROFILES, SourceCodeQAService
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS
from bpmis_jira_tool.web import create_app


class MarketTeamTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = patch.dict(os.environ, {
            "ENV_FILE": os.devnull,
            "FLASK_SECRET_KEY": "market-test-secret",
            "TEAM_PORTAL_DATA_DIR": self.temp.name,
            "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
            "SOURCE_CODE_QA_GITLAB_TOKEN": "test-token",
            "LOCAL_AGENT_MODE": "disabled",
            "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED": "false",
            "BPMIS_CALL_MODE": "direct",
            "TEAM_PORTAL_PUBLIC_GCS_BUCKET": "",
            "TEAM_PORTAL_PUBLIC_GCS_PUBLISH_BUCKET": "",
            "CLOUD_RUN_PUBLIC_GCS_BUCKET": "",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        self.app = create_app()
        self.app.testing = True
        self.service = self.app.config["SOURCE_CODE_QA_SERVICE"]
        self.service.semantic_index_enabled = False

    @staticmethod
    def login(client, email="xiaodong.zheng@npt.sg"):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": email, "name": "Test User"}
            session["google_credentials"] = {"token": "test", "scopes": []}

    def seed(self, team="SG_MARKET", extra_files=None):
        self.service.save_mapping(pm_team=team, country="All", repositories=[{
            "display_name": "Market Repo",
            "url": f"https://gitlab.company.test/{team.lower()}/market.git",
        }])
        key = self.service.mapping_key(team, "All")
        entry = self.service._load_entries_for_key(key)[0]
        root = self.service._repo_path(key, entry)
        root.mkdir(parents=True)
        files = {
            "src/decision.py": "def decide(amount):\n    return amount > 100\n",
            "README.md": "# Market logic\nSee src/decision.py.\n",
            "docs/architecture.md": "Decisions use the amount in the request.\n",
            "application.yml": "decision:\n  threshold: 100\n",
            "node_modules/vendor.js": "ignored dependency",
            "build/compiled.py": "ignored build output",
            "image.png": b"\x89PNG\x00",
        }
        files.update(extra_files or {})
        for relative, content in files.items():
            file = root / relative
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(content if isinstance(content, bytes) else content.encode())
        subprocess.run(["git", "init", "-q", str(root)], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(root), "add", "."], check=True, capture_output=True)
        subprocess.run([
            "git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.com",
            "commit", "-qm", "fixture",
        ], check=True, capture_output=True)
        self.service._build_repo_index(key, entry, root)
        return root

    def test_market_options_do_not_change_bpmis_teams(self):
        options = {item["code"]: item for item in self.service.options_payload()["pm_teams"]}
        for team, profile in MARKET_TEAM_PROFILES.items():
            self.assertEqual(options[team]["label"], profile["label"])
            self.assertTrue(options[team]["all_country_only"])
            self.assertNotIn(team, TEAM_PROFILE_DEFAULTS)
        self.assertEqual(self.service.mapping_key("AF", "PH"), "AF:All")
        self.assertEqual(self.service.mapping_key("CRMS", "PH"), "CRMS:PH")

    def test_admin_can_save_separate_market_mappings_and_reject_non_all(self):
        with self.app.test_client() as client:
            self.login(client)
            for team in MARKET_TEAM_PROFILES:
                response = client.post("/api/source-code-qa/config", json={
                    "pm_team": team, "country": "All",
                    "repositories": [{"url": f"https://gitlab.npt.seabank.io/{team}/repo.git"}],
                })
                self.assertEqual(response.status_code, 200, response.json)
                self.assertEqual(response.json["key"], f"{team}:All")
                invalid = client.post("/api/source-code-qa/config", json={
                    "pm_team": team, "country": "SG", "repositories": [],
                })
                self.assertEqual(invalid.status_code, 400)
            self.assertEqual(len(self.service.load_config()["mappings"]), 3)

    def test_teammate_cannot_change_market_mapping_or_sync(self):
        with self.app.test_client() as client:
            self.login(client, "teammate@npt.sg")
            for endpoint in ("config", "sync"):
                response = client.post(f"/api/source-code-qa/{endpoint}", json={
                    "pm_team": "SG_MARKET", "country": "All", "repositories": [],
                })
                self.assertEqual(response.status_code, 403)

    def test_market_scopes_appear_on_page_and_config(self):
        with self.app.test_client() as client:
            self.login(client)
            html = client.get("/source-code-qa").get_data(as_text=True)
            payload = client.get("/api/source-code-qa/config").json
        for team, profile in MARKET_TEAM_PROFILES.items():
            self.assertIn(f'value="{team}"', html)
            self.assertIn(profile["label"], html)
            self.assertIn(f"/repo-downloads/{team}:All", html)
        self.assertEqual(len(payload["options"]["pm_teams"]), 6)

    def test_market_sync_clones_and_pulls_source_with_indexes(self):
        origin = self.seed("AF")
        for team in MARKET_TEAM_PROFILES:
            self.service.save_mapping(pm_team=team, country="All", repositories=[{
                "display_name": "Market Repo", "url": "https://gitlab.npt.seabank.io/market/repo.git",
            }])
            with patch.object(self.service, "_authenticated_git_url", return_value=str(origin)):
                first = self.service.sync(pm_team=team, country="All")
                second = self.service.sync(pm_team=team, country="All")
            self.assertEqual(first["status"], "ok", first)
            self.assertEqual(second["status"], "ok", second)
            self.assertEqual(first["key"], f"{team}:All")
            metadata, content = bundles.build_repo_download_zip(self.service, f"{team}:All")
            self.assertEqual(metadata["repo_count"], 1)
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                self.assertIn("Market-Repo/src/decision.py", archive.namelist())

    def test_each_market_download_includes_source_indexes_and_key_documents(self):
        for team in MARKET_TEAM_PROFILES:
            with self.subTest(team=team):
                self.seed(team)
                with self.app.test_client() as client:
                    self.login(client, "teammate@npt.sg")
                    response = client.get(f"/api/source-code-qa/repo-downloads/{team}:All")
                self.assertEqual(response.status_code, 200, response.get_data(as_text=False)[:100])
                self.assertLess(len(response.data), 20_000_000)
                with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
                    names = archive.namelist()
                    manifest = json.loads(archive.read("manifest.json"))
                    self.assertEqual(manifest["scope_key"], f"{team}:All")
                    self.assertIn("Market-Repo/src/decision.py", names)
                    self.assertIn("Market-Repo/README.md", names)
                    self.assertIn("Market-Repo/docs/architecture.md", names)
                    self.assertIn("Market-Repo/application.yml", names)
                    self.assertNotIn("Market-Repo/node_modules/vendor.js", names)
                    self.assertNotIn("Market-Repo/build/compiled.py", names)
                    self.assertNotIn("Market-Repo/image.png", names)
                    repo = manifest["repos"][0]
                    files = [json.loads(row) for row in archive.read(repo["file_index"]).splitlines()]
                    symbols = [json.loads(row) for row in archive.read(repo["symbol_index"]).splitlines()]
                    self.assertTrue(any(row["name"] == "decide" for row in symbols))
                    self.assertEqual({row["path"] for row in files}, {name for name in names if name.startswith("Market-Repo/")})
                    self.assertTrue(all(row["path"] in names for row in symbols))

    def test_large_bundle_keeps_source_before_documents_and_preserves_indexes(self):
        rng = random.Random(42)
        self.seed(extra_files={
            "src/decision.py": "def decide(amount):\n    return amount > 100\n# " + rng.randbytes(2500).hex(),
            "docs/design.md": rng.randbytes(24000).hex(),
        })
        with patch.object(bundles, "MAX_BUNDLE_BYTES", 12_000):
            metadata, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
            cached_metadata, cached = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        self.assertLess(len(content), 12_000)
        self.assertEqual(content, cached)
        self.assertEqual(metadata["generated_at"], cached_metadata["generated_at"])
        self.assertGreater(metadata["omitted_files"], 0)
        self.assertEqual(metadata["omitted_files"], cached_metadata["omitted_files"])
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            self.assertIn("Market-Repo/src/decision.py", archive.namelist())
            self.assertNotIn("Market-Repo/docs/design.md", archive.namelist())
            manifest = json.loads(archive.read("manifest.json"))
            self.assertGreater(manifest["selection"]["omitted_files"], 0)
            for repo in manifest["repos"]:
                for line in archive.read(repo["symbol_index"]).splitlines():
                    self.assertIn(json.loads(line)["path"], archive.namelist())

    def test_strict_size_boundary_and_oversized_prebuilt_is_ignored(self):
        bundles.require_repo_download_size(19_999_999)
        with self.assertRaises(ToolError):
            bundles.require_repo_download_size(20_000_000)
        root = self.service.data_root / "repo_downloads"
        root.mkdir(parents=True)
        with (root / "source-code-repos-SG-Market-All.zip").open("wb") as handle:
            handle.truncate(20_000_000)
        self.assertIsNone(bundles.existing_repo_download_archive(self.service, "SG_MARKET:All"))

    def test_signed_gcs_download_checks_actual_blob_size_and_pins_generation(self):
        blob = SimpleNamespace(
            exists=lambda: True, reload=lambda: None, size=20_000_000, generation=7,
            generate_signed_url=lambda **kwargs: "https://signed.company.test/archive",
        )
        with patch.dict(os.environ, {"TEAM_PORTAL_PUBLIC_GCS_BUCKET": "test"}), patch.object(
            gcs, "_bucket", return_value=SimpleNamespace(blob=lambda filename: blob)
        ), patch.object(gcs, "gcs_read_bytes", return_value=b'{"size_bytes":1}'):
            with self.assertRaises(ToolError):
                gcs.fetch_repo_download_signed_url("bundle.zip")
            blob.size = 1024
            with patch.object(blob, "generate_signed_url", return_value="https://signed.company.test/archive") as signer:
                metadata, _ = gcs.fetch_repo_download_signed_url("bundle.zip")
            self.assertEqual(metadata["size_bytes"], 1024)
            self.assertEqual(signer.call_args.kwargs["generation"], 7)

    def test_gcs_hydrated_oversized_bundle_is_rejected_and_temp_file_removed(self):
        hydrated = []

        def hydrate(filename, destination):
            hydrated.append(destination)
            with destination.open("wb") as handle:
                handle.truncate(20_000_000)
            return {"filename": filename}

        with patch.dict(os.environ, {"TEAM_PORTAL_PUBLIC_GCS_BUCKET": "test"}), patch.object(
            gcs, "fetch_repo_download_signed_url", return_value=None
        ), patch.object(gcs, "fetch_repo_download_archive_to_file", side_effect=hydrate):
            with self.app.test_client() as client:
                self.login(client, "teammate@npt.sg")
                response = client.get("/api/source-code-qa/repo-downloads/SG_MARKET:All")
        self.assertEqual(response.status_code, 400)
        self.assertTrue(hydrated)
        self.assertFalse(hydrated[0].parent.exists())

    def test_local_agent_stream_limit_cleans_up_and_closes_response(self):
        from bpmis_jira_tool import web

        response = SimpleNamespace(
            headers={"Content-Type": "application/zip"},
            iter_content=lambda chunk_size: iter([b"12345", b"67890"]),
            close=lambda: None,
        )
        settings = self.app.config["SETTINGS"]
        for name, value in {
            "local_agent_mode": "enabled", "local_agent_base_url": "https://agent.company.test",
            "local_agent_hmac_secret": "test", "local_agent_source_code_qa_enabled": True,
        }.items():
            object.__setattr__(settings, name, value)
        with patch.object(web, "_build_local_agent_client", return_value=SimpleNamespace(
            source_code_qa_repo_download=lambda scope: response
        )), patch.object(bundles, "MAX_BUNDLE_BYTES", 10), patch.object(response, "close") as close:
            with self.app.test_client() as client:
                self.login(client, "teammate@npt.sg")
                result = client.get("/api/source-code-qa/repo-downloads/SG_MARKET:All")
        self.assertEqual(result.status_code, 400)
        close.assert_called_once()

    def test_oversized_bundles_cannot_be_published(self):
        with patch.object(bundles, "MAX_BUNDLE_BYTES", 10), patch.object(gcs, "gcs_upload_bytes") as upload:
            with self.assertRaises(ToolError):
                gcs.publish_repo_download_archive({"filename": "bundle.zip"}, b"1234567890")
            upload.assert_not_called()


if __name__ == "__main__":
    unittest.main()
