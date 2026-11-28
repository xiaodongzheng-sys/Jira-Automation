import io
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
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
        self.assertEqual(self.service.mapping_key("CRMS", "PH"), "CRMS:All")

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
        self.assertEqual(len(payload["options"]["pm_teams"]), 7)

    def test_digital_ops_download_is_listed_and_contains_full_source(self):
        self.seed("DIGITAL_OPS")
        with self.app.test_client() as client:
            url = "/cloud-auth/api/source-code-qa/repo-downloads/DIGITAL_OPS:All"
            self.assertEqual(client.get(url).status_code, 401)
            self.login(client, "teammate@npt.sg")
            self.assertEqual(client.get(url).status_code, 403)
            self.login(client)
            self.assertEqual(client.get("/cloud-auth/api/source-code-qa/repo-downloads/AF:All").status_code, 404)
            html = client.get("/source-code-qa").get_data(as_text=True)
            self.assertIn("Digital Ops", html)
            self.assertIn("/cloud-auth/api/source-code-qa/repo-downloads/DIGITAL_OPS:All", html)
            response = client.get("/cloud-auth/api/source-code-qa/repo-downloads/DIGITAL_OPS:All")
            self.assertEqual(response.status_code, 200)
            with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
                manifest = json.loads(archive.read("manifest.json"))
                self.assertEqual(manifest["scope_key"], "DIGITAL_OPS:All")
                self.assertIn("Market-Repo/src/decision.py", archive.namelist())

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
                    self.login(client)
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
                    self.assertIn("Market-Repo/node_modules/vendor.js", names)
                    self.assertIn("Market-Repo/build/compiled.py", names)
                    self.assertNotIn("Market-Repo/image.png", names)
                    repo = manifest["repos"][0]
                    files = [json.loads(row) for row in archive.read(repo["file_index"]).splitlines()]
                    symbols = [json.loads(row) for row in archive.read(repo["symbol_index"]).splitlines()]
                    self.assertTrue(any(row["name"] == "decide" for row in symbols))
                    self.assertEqual({row["path"] for row in files}, {name for name in names if name.startswith("Market-Repo/")})
                    self.assertTrue(all(row["path"] in names for row in symbols))

    def test_large_market_bundle_splits_without_dropping_any_source(self):
        rng = random.Random(42)
        extra = {f"src/indirect_{i}.py": f"def helper_{i}():\n    return True\n# " + rng.randbytes(6000).hex() for i in range(8)}
        extra["src/af_client.py"] = "# RiskDecision client\nfrom indirect_7 import helper_7\n"
        extra["spec/payment_flow.md"] = "Call helper_7 after the risk decision."
        extra["src/main/resources/data/af_payload.json"] = '{"riskDecision":"AUTH","fields":["deviceId"]}'
        root = self.seed(extra_files=extra)
        with patch.object(bundles, "MAX_BUNDLE_BYTES", 28_000):
            metadata, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
            cached_metadata, cached = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
            self.assertEqual(content, cached)
            self.assertGreater(metadata["part_count"], 1)
            self.assertEqual(metadata["omitted_files"], 0)
            self.assertEqual(metadata["generated_at"], cached_metadata["generated_at"])
            seen = {}
            for part in metadata["parts"]:
                key = "SG_MARKET:All" if part["part_number"] == 1 else "SG_MARKET:All~part-" + str(part["part_number"])
                part_metadata, payload = bundles.build_repo_download_zip(self.service, key)
                self.assertLess(len(payload), 28_000)
                manifest = bundles.validate_repo_download_archive(payload)
                self.assertEqual(manifest["bundle_set"]["omitted_source_files"], 0)
                with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                    for repo in manifest["repos"]:
                        for line in archive.read(repo["file_index"]).splitlines():
                            row = json.loads(line)
                            self.assertNotIn(row["path"], seen)
                            seen[row["path"]] = archive.read(row["path"])
            for relative, expected in extra.items():
                self.assertEqual(seen["Market-Repo/" + relative], expected.encode())
            self.assertEqual(len(seen), manifest["bundle_set"]["scope_source_files"])

    def test_full_source_bundle_accepts_readme_only_repository(self):
        root = self.seed()
        for child in ("src", "docs", "node_modules", "build"):
            shutil.rmtree(root / child)
        (root / "application.yml").unlink()
        (root / "image.png").unlink()
        subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "readme only"], check=True)
        metadata, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            self.assertIn("Market-Repo/README.md", archive.namelist())
            manifest = json.loads(archive.read("manifest.json"))
            self.assertEqual(manifest["repos"][0]["source_kind"], "documents")

    def test_single_oversized_source_file_fails_instead_of_being_omitted(self):
        self.seed(extra_files={"docs/design.md": random.Random(42).randbytes(24000).hex()})
        with patch.object(bundles, "MAX_BUNDLE_BYTES", 12_000):
            with self.assertRaisesRegex(ToolError, "complete source file"):
                bundles.build_repo_download_zip(self.service, "SG_MARKET:All")

    def test_combined_download_preserves_all_parts_and_rejects_stale_set(self):
        from bpmis_jira_tool.source_code_qa_combined import build_combined_archive, combined_filename, install_combined_archive
        from bpmis_jira_tool.source_code_qa_download_parts import cached_part_artifacts, install_local_parts

        rng = random.Random(42)
        files = {f"src/part_{i}.py": "# " + rng.randbytes(6000).hex() for i in range(8)}
        self.seed(extra_files=files)
        with patch.object(bundles, "MAX_BUNDLE_BYTES", 28_000):
            metadata, _ = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
            self.assertGreater(metadata["part_count"], 1)
            artifacts = list(cached_part_artifacts(self.service, metadata))
            combined = artifacts[0][1].parent / combined_filename("source-code-repos-SG-Market-All.zip")
            combined_meta = build_combined_archive(artifacts, combined)
            self.assertGreater(combined.stat().st_size, 28_000)
            install_local_parts(self.service, metadata)
            install_combined_archive(self.service, combined)
            with self.app.test_client() as client:
                url = "/api/source-code-qa/repo-downloads/SG_MARKET:All/combined"
                self.assertEqual(client.get(url).status_code, 401)
                self.login(client, "teammate@npt.sg")
                self.assertEqual(client.get(url).status_code, 403)
                self.login(client)
                self.assertNotIn("SG Market · Combined", client.get("/source-code-qa").get_data(as_text=True))
                response = client.get(url)
                self.assertEqual(response.status_code, 200)
                with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
                    self.assertIsNone(archive.testzip())
                    self.assertEqual(json.loads(archive.read("manifest.json"))["set_id"], combined_meta["set_id"])
                    for name, content in files.items():
                        self.assertEqual(archive.read("Market-Repo/" + name), content.encode())
                    self.assertTrue(any(name.startswith("indexes/part-") for name in archive.namelist()))
                response.close()
                sidecar = self.service.data_root / "repo_downloads" / (combined.name + ".json")
                installed = self.service.data_root / "repo_downloads" / combined.name
                with installed.open("ab") as handle:
                    handle.write(b"unexpected bytes")
                self.assertEqual(client.get(url).status_code, 404)
                install_combined_archive(self.service, combined)
                sidecar.write_text(json.dumps({**combined_meta, "set_id": "an-old-set"}))
                self.assertEqual(client.get(url).status_code, 404)
                self.assertNotIn("SG Market · Combined", client.get("/source-code-qa").get_data(as_text=True))

    def test_combined_download_rejects_mixed_parts(self):
        from bpmis_jira_tool.source_code_qa_combined import build_combined_archive, combined_filename
        from bpmis_jira_tool.source_code_qa_download_parts import cached_part_artifacts

        self.seed()
        metadata, _ = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        artifacts = list(cached_part_artifacts(self.service, metadata))
        part, path = artifacts[0]
        with self.assertRaisesRegex(ToolError, "manifest does not match"):
            build_combined_archive([({**part, "set_id": "not-the-manifest-set"}, path)], path.parent / combined_filename("source-code-repos-SG-Market-All.zip"))

    def test_combined_gcs_verifies_set_and_digest_without_the_part_size_limit(self):
        name = "source-code-repos-PH-Market-All-combined.zip"
        metadata = {
            "format": "combined-source-and-index-v1", "scope_key": "PH_MARKET:All", "set_id": "current-set",
            "filename": name, "source_verified": True, "size_bytes": 100_000_000,
            "md5_base64": "expected-md5", "sha256": "a" * 64,
        }
        blob = SimpleNamespace(exists=lambda: True, reload=lambda: None, size=100_000_000,
                               md5_hash="expected-md5", generation=9,
                               generate_signed_url=lambda **kwargs: "https://signed.company.test/combined")
        with patch.dict(os.environ, {"TEAM_PORTAL_PUBLIC_GCS_BUCKET": "test"}), patch.object(
            gcs, "_bucket", return_value=SimpleNamespace(blob=lambda filename: blob)
        ), patch.object(gcs, "gcs_read_bytes", return_value=json.dumps(metadata).encode()):
            self.assertEqual(gcs.fetch_combined_repo_download_status(name, "PH_MARKET:All", "current-set"), metadata)
            self.assertIsNone(gcs.fetch_combined_repo_download_status(name, "PH_MARKET:All", "older-set"))
            with patch.object(blob, "generate_signed_url", return_value="https://signed.company.test/combined") as signer:
                self.assertIsNotNone(gcs.fetch_combined_repo_download_signed_url(name, "PH_MARKET:All", "current-set"))
                self.assertEqual(signer.call_args.kwargs["generation"], 9)
            blob.md5_hash = "wrong"
            self.assertIsNone(gcs.fetch_combined_repo_download_status(name, "PH_MARKET:All", "current-set"))

    def test_combined_cloud_route_is_owner_gated_and_pins_current_set(self):
        url = "/api/source-code-qa/repo-downloads/PH_MARKET:All/combined"
        with patch("bpmis_jira_tool.web.public_gcs_read_bucket", return_value="test"), patch.object(
            gcs, "fetch_repo_download_status", return_value={"available": True, "set_id": "current-set"}
        ), patch.object(gcs, "fetch_combined_repo_download_signed_url", return_value=({}, "https://signed.company.test/combined")) as signed:
            with self.app.test_client() as client:
                self.assertEqual(client.get(url).status_code, 401)
                self.login(client, "teammate@npt.sg")
                self.assertEqual(client.get(url).status_code, 403)
                self.login(client)
                response = client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.location, "https://signed.company.test/combined")
                self.assertEqual(signed.call_args.args[-1], "current-set")

    def test_combined_cloud_fallback_streams_without_content_length(self):
        url = "/api/source-code-qa/repo-downloads/PH_MARKET:All/combined"
        with patch("bpmis_jira_tool.web.public_gcs_read_bucket", return_value="test"), patch.object(
            gcs, "fetch_repo_download_status", return_value={"available": True, "set_id": "current-set"}
        ), patch.object(gcs, "fetch_combined_repo_download_signed_url", return_value=None), patch.object(
            gcs, "stream_combined_repo_download", return_value=({"sha256": "a" * 64}, iter((b"part-1", b"part-2")))
        ):
            with self.app.test_client() as client:
                self.login(client)
                response = client.get(url, buffered=False)
                self.assertEqual(response.status_code, 200)
                self.assertNotIn("Content-Length", response.headers)
                self.assertEqual(response.headers["X-Content-SHA256"], "a" * 64)
                self.assertEqual(b"".join(response.response), b"part-1part-2")
                response.close()

    def test_combined_gcs_stream_pins_generation_and_checks_content(self):
        payload = b"source bundle data"
        blob = SimpleNamespace(generation=9, open=lambda *args, **kwargs: io.BytesIO(payload))
        metadata = {"size_bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
        with patch.object(gcs, "_combined_blob_and_metadata", return_value=(blob, metadata)), patch.object(
            blob, "open", wraps=blob.open
        ) as reader:
            _, chunks = gcs.stream_combined_repo_download("bundle.zip", "PH_MARKET:All", "set")
            self.assertEqual(b"".join(chunks), payload)
            self.assertEqual(reader.call_args.kwargs["if_generation_match"], 9)
        bad_metadata = {**metadata, "sha256": "0" * 64}
        with patch.object(gcs, "_combined_blob_and_metadata", return_value=(blob, bad_metadata)):
            _, chunks = gcs.stream_combined_repo_download("bundle.zip", "PH_MARKET:All", "set")
            with self.assertRaisesRegex(OSError, "checksum mismatch"):
                b"".join(chunks)

    def test_risk_scopes_serve_every_numbered_source_part(self):
        from bpmis_jira_tool.source_code_qa_download_parts import install_local_parts

        for team in ("AF", "CRMS", "GRC"):
            with self.subTest(team=team):
                rng = random.Random(42)
                files = {f"src/part_{i}.py": "# " + rng.randbytes(6000).hex() for i in range(8)}
                self.seed(team, extra_files=files)
                scope = f"{team}:All"
                with patch.object(bundles, "MAX_BUNDLE_BYTES", 28_000):
                    metadata, _ = bundles.build_repo_download_zip(self.service, scope)
                    self.assertGreater(metadata["part_count"], 1)
                    install_local_parts(self.service, metadata)
                    found = set()
                    with self.app.test_client() as client:
                        self.login(client)
                        for part in metadata["parts"]:
                            key = scope if part["part_number"] == 1 else scope + f"~part-{part['part_number']}"
                            response = client.get(f"/api/source-code-qa/repo-downloads/{key}")
                            self.assertEqual(response.status_code, 200)
                            with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
                                self.assertIsNone(archive.testzip())
                                found.update(archive.namelist())
                    for name in files:
                        self.assertIn("Market-Repo/" + name, found)

    def test_download_access_is_limited_to_the_owner_email(self):
        self.seed("AF")
        url = "/api/source-code-qa/repo-downloads/AF:All"
        with self.app.test_client() as client:
            self.assertIn(client.get(url).status_code, {401, 403})
            for email in ("teammate@npt.sg", "xiaodong.zheng1991@gmail.com", "xiaodong.zheng+alias@npt.sg"):
                self.login(client, email)
                self.assertEqual(client.get("/source-code-qa").status_code, 302)
                self.assertEqual(client.get(url).status_code, 403)
                self.assertEqual(client.get(url + "~part-2").status_code, 403)
            self.login(client, "XIAODONG.ZHENG@NPT.SG")
            self.assertEqual(client.get("/source-code-qa").status_code, 200)
            self.assertEqual(client.get(url).status_code, 200)

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
            exists=lambda: True, reload=lambda: None, size=20_000_000, generation=7, md5_hash="verified-md5",
            generate_signed_url=lambda **kwargs: "https://signed.company.test/archive",
        )
        with patch.dict(os.environ, {"TEAM_PORTAL_PUBLIC_GCS_BUCKET": "test"}), patch.object(
            gcs, "_bucket", return_value=SimpleNamespace(blob=lambda filename: blob)
        ), patch.object(gcs, "gcs_read_bytes", return_value=json.dumps({
            "format": bundles.BUNDLE_FORMAT, "source_verified": True, "repo_count": 1,
            "source_files": 1, "size_bytes": 1024, "md5_base64": "verified-md5",
        }).encode()):
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
                self.login(client)
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
                self.login(client)
                result = client.get("/api/source-code-qa/repo-downloads/SG_MARKET:All")
        self.assertEqual(result.status_code, 400)
        close.assert_called_once()

    def test_oversized_bundles_cannot_be_published(self):
        with patch.object(bundles, "MAX_BUNDLE_BYTES", 10), patch.object(gcs, "gcs_upload_bytes") as upload:
            with self.assertRaises(ToolError):
                gcs.publish_repo_download_archive({"filename": "bundle.zip"}, b"1234567890")
            upload.assert_not_called()

    def test_all_six_scopes_build_verified_source_bundles(self):
        for scope in bundles.REPO_DOWNLOAD_SCOPES:
            with self.subTest(scope=scope["scope_key"]):
                self.seed(scope["scope_key"].split(":")[0])
                metadata, content = bundles.build_repo_download_zip(self.service, scope["scope_key"])
                self.assertLess(len(content), 20_000_000)
                self.assertTrue(metadata["source_verified"])
                self.assertGreater(metadata["code_files"], 0)
                bundles.validate_repo_download_archive(content, expected_scope=scope["scope_key"])

    def test_download_indexes_do_not_require_a_full_qa_index(self):
        self.seed()
        with patch.object(self.service, "_repo_index_info", side_effect=AssertionError("must not read QA index")):
            metadata, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        self.assertTrue(metadata["source_verified"])
        self.assertGreater(metadata["code_files"], 0)
        bundles.validate_repo_download_archive(content, expected_scope="SG_MARKET:All")

    def test_readme_only_and_index_only_archives_are_rejected(self):
        self.seed()
        _, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        for drop_source in [True, False]:
            output = io.BytesIO()
            with zipfile.ZipFile(io.BytesIO(content)) as source, zipfile.ZipFile(output, "w") as target:
                for name in source.namelist():
                    if drop_source and name.endswith(".py"):
                        continue
                    if not drop_source and name == "manifest.json":
                        manifest = json.loads(source.read(name))
                        manifest["format"] = "source-and-index-v3"
                        target.writestr(name, json.dumps(manifest))
                    else:
                        target.writestr(name, source.read(name))
            with self.assertRaises(ToolError):
                bundles.validate_repo_download_archive(output.getvalue())
            with patch.dict(os.environ, {"TEAM_PORTAL_PUBLIC_GCS_PUBLISH_BUCKET": "test"}), patch.object(gcs, "gcs_upload_bytes") as upload:
                with self.assertRaises(ToolError):
                    gcs.publish_repo_download_archive({"filename": "bundle.zip"}, output.getvalue())
                upload.assert_not_called()

    def test_corrupt_cache_is_rebuilt_with_source(self):
        self.seed()
        _, original = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        next((self.service.data_root / "repo_download_cache").rglob("*.zip")).write_bytes(b"invalid")
        _, rebuilt = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        self.assertEqual(bundles.validate_repo_download_archive(original)["repos"], bundles.validate_repo_download_archive(rebuilt)["repos"])

    def test_source_checksum_and_unlisted_content_are_rejected(self):
        self.seed()
        _, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        for corrupt_source in [True, False]:
            output = io.BytesIO()
            with zipfile.ZipFile(io.BytesIO(content)) as source, zipfile.ZipFile(output, "w") as target:
                for name in source.namelist():
                    target.writestr(name, b"changed" if corrupt_source and name.endswith("decision.py") else source.read(name))
                if not corrupt_source:
                    target.writestr("uploads/material.sql", "must not be served")
            with self.assertRaises(ToolError):
                bundles.validate_repo_download_archive(output.getvalue())

    def test_configuration_only_repository_is_identified(self):
        root = self.seed()
        for relative in ["src/decision.py", "node_modules/vendor.js", "build/compiled.py"]:
            (root / relative).unlink()
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qam", "configuration only"], check=True, capture_output=True)
        _, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        repo = bundles.validate_repo_download_archive(content)["repos"][0]
        self.assertEqual(repo["source_kind"], "configuration")
        self.assertEqual(repo["code_files"], 0)
        (root / "application.yml").unlink()
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qam", "docs only"], check=True, capture_output=True)
        _, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        self.assertEqual(bundles.validate_repo_download_archive(content)["repos"][0]["source_kind"], "documents")

    def test_signed_url_rejects_unverified_or_mismatched_metadata(self):
        blob = SimpleNamespace(exists=lambda: True, reload=lambda: None, size=1024, generation=7, md5_hash="actual", generate_signed_url=lambda **kwargs: "https://signed.test")
        for sidecar in [{}, {"format": bundles.BUNDLE_FORMAT, "source_verified": True, "repo_count": 1, "source_files": 2, "size_bytes": 1024, "md5_base64": "old-archive"}]:
            with patch.dict(os.environ, {"TEAM_PORTAL_PUBLIC_GCS_BUCKET": "test"}), patch.object(gcs, "_bucket", return_value=SimpleNamespace(blob=lambda filename: blob)), patch.object(gcs, "gcs_read_bytes", return_value=json.dumps(sidecar).encode()):
                self.assertIsNone(gcs.fetch_repo_download_signed_url("bundle.zip"))


if __name__ == "__main__":
    unittest.main()
