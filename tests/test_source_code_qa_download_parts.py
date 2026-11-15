import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import unittest
from unittest.mock import patch
import zipfile

from bpmis_jira_tool import source_code_qa_repo_downloads as bundles
from bpmis_jira_tool.source_code_qa_download_parts import install_local_parts
from bpmis_jira_tool.source_code_qa_download_sources import https_git_url, is_text_blob
from bpmis_jira_tool.errors import ToolError
from tests import test_source_code_qa_market_teams as market_fixtures


class CompleteMarketDownloadTests(unittest.TestCase):
    setUp = market_fixtures.MarketTeamTests.setUp
    seed = market_fixtures.MarketTeamTests.seed
    login = staticmethod(market_fixtures.MarketTeamTests.login)

    def git(self, root, *args):
        return subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.com", *args], capture_output=True, check=True).stdout

    def test_reads_git_blobs_and_excludes_untracked_local_data(self):
        root = self.seed(extra_files={"spec/af/payment.md": "RiskDecision call chain", "device/config.custom": "AF_DEVICE_ID=deviceId"})
        original = (root / "src/decision.py").read_bytes()
        (root / "src/decision.py").write_text("uncommitted local change")
        (root / "private.py").write_text("LOCAL_SENTINEL")
        _, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            self.assertEqual(archive.read("Market-Repo/src/decision.py"), original)
            self.assertEqual(archive.read("Market-Repo/device/config.custom"), b"AF_DEVICE_ID=deviceId")
            self.assertIn("Market-Repo/spec/af/payment.md", archive.namelist())
            self.assertNotIn("Market-Repo/private.py", archive.namelist())

    def test_pinned_runtime_submodule_source_is_included_and_missing_source_blocks(self):
        root = self.seed()
        child = Path(self.temp.name) / "child-origin"
        child.mkdir()
        self.git(child, "init", "-q")
        (child / "af_client.ts").write_text("export function riskDecision() { return 'AUTH'; }\n")
        self.git(child, "add", ".")
        self.git(child, "commit", "-qm", "client")
        self.git(root, "-c", "protocol.file.allow=always", "submodule", "add", str(child), "shared-rn")
        self.git(root, "commit", "-qam", "pin shared code")
        shared = root / "shared-rn"
        hidden = root.parent / "saved-child"
        shared.rename(hidden)
        with self.assertRaisesRegex(ToolError, "runtime submodule"):
            bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        hidden.rename(shared)
        metadata, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        manifest = bundles.validate_repo_download_archive(content)
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            self.assertIn("Market-Repo/shared-rn/af_client.ts", archive.namelist())
        audit = manifest["repos"][0]["full_source_audit"]
        self.assertEqual(audit["submodules"][0]["git_revision"], self.git(child, "rev-parse", "HEAD").decode().strip())
        self.assertFalse(audit["unavailable_submodules"])

    def test_unavailable_fastlane_is_explicit_in_manifest(self):
        root = self.seed()
        (root / ".gitmodules").write_text('[submodule "fastlane"]\npath = fastlane\nurl = https://gitlab.npt.seabank.io/bfe-horizontal-products/app/ios/libs/fastlane.git\n')
        oid = self.git(root, "rev-parse", "HEAD").decode().strip()
        self.git(root, "add", ".gitmodules")
        self.git(root, "update-index", "--add", "--cacheinfo", "160000," + oid + ",fastlane")
        self.git(root, "commit", "-qm", "tooling pointer")
        _, content = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
        manifest = bundles.validate_repo_download_archive(content)
        missing = manifest["bundle_set"]["unavailable_submodules"]
        self.assertEqual(missing[0]["path"], "Market-Repo/fastlane")
        self.assertEqual(missing[0]["git_revision"], oid)

    def test_local_part_routes_and_list_use_each_part(self):
        import random

        self.seed(extra_files={f"src/flow_{i}.py": "# " + random.Random(i).randbytes(5000).hex() for i in range(8)})
        with patch.object(bundles, "MAX_BUNDLE_BYTES", 28_000):
            metadata, _ = bundles.build_repo_download_zip(self.service, "SG_MARKET:All")
            self.assertGreater(metadata["part_count"], 1)
            install_local_parts(self.service, metadata)
            with self.app.test_client() as client:
                self.assertEqual(client.get("/api/source-code-qa/repo-downloads/SG_MARKET:All~part-2").status_code, 401)
                self.login(client, "teammate@npt.sg")
                html = client.get("/source-code-qa").get_data(as_text=True)
                self.assertIn(f"Part 2 of {metadata['part_count']}", html)
                for part in metadata["parts"]:
                    suffix = "" if part["part_number"] == 1 else "~part-" + str(part["part_number"])
                    response = client.get("/api/source-code-qa/repo-downloads/SG_MARKET:All" + suffix)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(hashlib.sha256(response.data).hexdigest(), part["sha256"])
                    response.close()
                self.assertEqual(client.get("/api/source-code-qa/repo-downloads/SG_MARKET:All~part-0").status_code, 400)
                with self.assertRaisesRegex(ToolError, "Unknown"):
                    bundles.build_repo_download_zip(self.service, "SG_MARKET:All~part-999")

    def test_url_normalization_and_text_detection(self):
        self.assertEqual(https_git_url("ssh://git@git.example.test:10022/team/shared.git"), "https://git.example.test/team/shared.git")
        self.assertEqual(https_git_url("git@git.example.test:team/shared.git"), "https://git.example.test/team/shared.git")
        self.assertEqual(https_git_url("../shared.git", "https://git.example.test/team/app.git"), "https://git.example.test/team/shared.git")
        self.assertTrue(is_text_blob("deviceId".encode("utf-16")))
        self.assertFalse(is_text_blob(b"\x89PNG\0binary"))
