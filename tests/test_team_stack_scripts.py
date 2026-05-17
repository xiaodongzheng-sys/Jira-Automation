import json
import os
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TeamStackScriptTests(unittest.TestCase):
    SCRIPT_TEST_ENV_KEYS = (
        "ALLOW_LEGACY_NGROK_LOCAL_AGENT",
        "CLOUDFLARE_TUNNEL_TOKEN",
        "CLOUD_RUN_DEPLOY_ACCOUNT",
        "GOOGLE_CLOUD_PROJECT",
        "TEAM_PORTAL_BASE_URL",
        "TEAM_PORTAL_CLOUDFLARE_PROTOCOL",
        "TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME",
        "TEAM_PORTAL_DATA_DIR",
        "RELEASE_WINDOW_POLICY_BYPASS",
        "RELEASE_WINDOW_POLICY_NOW",
    )

    def setUp(self):
        self._timing_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._timing_dir.cleanup)
        timing_file = str(Path(self._timing_dir.name) / "deploy_timings.jsonl")
        self._timing_env_patch = patch.dict(
            os.environ,
            {
                "TEAM_DEPLOY_TIMING_FILE": timing_file,
                "RELEASE_WINDOW_POLICY_BYPASS": "1",
            },
        )
        self._timing_env_patch.start()
        self.addCleanup(self._timing_env_patch.stop)

    def _script_env(self, **overrides: str) -> dict:
        env = os.environ.copy()
        for key in self.SCRIPT_TEST_ENV_KEYS:
            env.pop(key, None)
        env["RELEASE_WINDOW_POLICY_BYPASS"] = "1"
        env.update(overrides)
        return env

    def _current_release_revision(self) -> str:
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        command = f'''
source "{helper_path}"
current_release_revision
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "ROOT_DIR": str(PROJECT_ROOT),
                "PYTHON_BIN": sys.executable,
            },
        )
        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        return completed.stdout.strip()

    def _run_team_env_helper(self, command: str, **env_overrides: str) -> subprocess.CompletedProcess:
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        return subprocess.run(
            ["bash", "-lc", f'source "{helper_path}"\n{command}'],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "ROOT_DIR": str(PROJECT_ROOT),
                "PYTHON_BIN": sys.executable,
                "ENV_FILE": "/dev/null",
                **env_overrides,
            },
        )

    def _write_fake_curl(self, bin_dir: Path) -> Path:
        curl_path = bin_dir / "curl"
        curl_path.write_text(
            """#!/usr/bin/env bash
set -euo pipefail

url="${@: -1}"
case "$url" in
  http://127.0.0.1:5000/healthz)
    printf '{"status":"ok","revision":"%s"}\\n' "${FAKE_HEALTHZ_REVISION:-unknown}"
    ;;
  http://127.0.0.1:4040/api/tunnels)
    printf '{"tunnels":[]}'\\n
    ;;
  https://example.ngrok.dev)
    ;;
  https://example.ngrok.dev/healthz)
    ;;
  https://app.bankpmtool.uk/healthz)
    if [[ "${FAKE_PUBLIC_HEALTH_FAIL:-0}" == "1" ]]; then
      exit 22
    fi
    ;;
  *)
    exit 1
    ;;
esac
""",
            encoding="utf-8",
        )
        curl_path.chmod(0o755)
        return curl_path

    def _write_fake_pgrep(self, bin_dir: Path, *, cloudflared_pid: str = "", ngrok_pid: str = "") -> Path:
        pgrep_path = bin_dir / "pgrep"
        pgrep_path.write_text(
            """#!/usr/bin/env bash
set -euo pipefail

pattern="${*: -1}"
if [[ "$pattern" == *"cloudflared"* && -n "${FAKE_CLOUDFLARED_PID:-}" ]]; then
  printf '%s\\n' "$FAKE_CLOUDFLARED_PID"
  exit 0
fi
if [[ "$pattern" == *"ngrok"* && -n "${FAKE_NGROK_PID:-}" ]]; then
  printf '%s\\n' "$FAKE_NGROK_PID"
  exit 0
fi
exit 1
""",
            encoding="utf-8",
        )
        pgrep_path.chmod(0o755)
        return pgrep_path

    def _write_fake_cloudflared(self, bin_dir: Path) -> Path:
        cloudflared_path = bin_dir / "cloudflared"
        cloudflared_path.write_text(
            """#!/usr/bin/env bash
set -euo pipefail

if [[ "$*" == "tunnel info bankpmtool-live" || "$*" == "tunnel info custom-live" ]]; then
  printf 'NAME:     %s\\n' "${*: -1}"
  printf 'CONNECTOR ID                         CREATED\\n'
  printf 'fake-connector                       2026-05-06T13:40:52Z\\n'
  exit 0
fi

printf '%s\\n' "$*" > "${FAKE_CLOUDFLARED_ARGS_FILE:?}"
exit 0
""",
            encoding="utf-8",
        )
        cloudflared_path.chmod(0o755)
        return cloudflared_path

    def test_stack_scripts_have_valid_bash_syntax(self):
        script_paths = [
            "scripts/lib/team_env.sh",
            "scripts/lib/cloud_run_image_policy.sh",
            "scripts/lib/release_window_policy.sh",
            "scripts/run_team_portal_prod.sh",
            "scripts/run_team_portal_foreground.sh",
            "scripts/run_ngrok_tunnel.sh",
            "scripts/run_ngrok_tunnel_foreground.sh",
            "scripts/run_cloudflare_tunnel.sh",
            "scripts/run_cloudflare_tunnel_foreground.sh",
            "scripts/run_team_stack_guard.sh",
            "scripts/run_team_stack_guard_daemon.sh",
            "scripts/run_team_stack.sh",
            "scripts/install_team_portal_launchd.sh",
            "scripts/install_ngrok_launchd.sh",
            "scripts/install_team_stack_launchd.sh",
            "scripts/deploy_cloud_run.sh",
            "scripts/deploy_cloud_run_full.sh",
            "scripts/deploy_cloud_run_uat.sh",
            "scripts/release_uat_fast.sh",
            "scripts/setup_uat_local_agent.sh",
            "scripts/promote_uat_to_live.sh",
            "scripts/build_cloud_run_image.sh",
            "scripts/setup_cloud_build_image_trigger.sh",
        ]

        for relative_path in script_paths:
            script_path = PROJECT_ROOT / relative_path
            completed = subprocess.run(
                ["bash", "-n", str(script_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                completed.returncode,
                0,
                msg=f"{relative_path} failed bash -n:\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}",
            )

    def test_cloud_run_deploy_script_supports_dry_run_without_deploying(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

if [[ "$*" == "run services describe"* ]]; then
  printf 'https://team-portal-example.run.app\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  echo "unexpected deploy" >&2
  exit 44
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "CLOUD_RUN_DEPLOY_DRY_RUN": "1",
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://app.bankpmtool.uk",
                    "CLOUD_RUN_RESTART_LOCAL_AGENT_AFTER_DEPLOY": "0",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                    "TRELLO_API_KEY": "trello-key",
                    "TRELLO_API_TOKEN": "trello-token",
                    "TRELLO_BOARD_ID": "trello-board",
                    "TRELLO_DAILY_LIST_NAME": "Daily Summary Email",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertIn("Cloud Run service: team-portal", completed.stdout)
            self.assertIn("Cloud Run source hash:", completed.stdout)
            self.assertIn("Dry run only", completed.stdout)
            self.assertNotIn("unexpected deploy", completed.stderr)

    def test_cloud_run_uat_deploy_uses_no_traffic_tagged_revision(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "run services describe"* ]]; then
  printf '{"metadata":{"annotations":{"run.googleapis.com/invoker-iam-disabled":"true"}},"status":{"url":"https://team-portal-ekaykywtvq-as.a.run.app","traffic":[{"revisionName":"team-portal-00090-live","percent":100},{"tag":"uat","url":"https://uat---team-portal-ekaykywtvq-as.a.run.app","revisionName":"team-portal-00091-uat","percent":0}]}}\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  exit 0
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_GCLOUD_CALLS": str(calls_path),
                    "CLOUD_RUN_UAT_SKIP_GIT_CHECK": "1",
                    "CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY": "0",
                    "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                    "TRELLO_API_KEY": "trello-key",
                    "TRELLO_API_TOKEN": "trello-token",
                    "TRELLO_BOARD_ID": "trello-board",
                    "TRELLO_DAILY_LIST_NAME": "Daily Summary Email",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            calls = calls_path.read_text(encoding="utf-8")
            deploy_calls = [line for line in calls.splitlines() if line.startswith("run deploy")]
            self.assertEqual(len(deploy_calls), 1, msg=calls)
            self.assertIn("--no-traffic", deploy_calls[0])
            self.assertIn("--tag uat", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_STAGE=uat", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_BASE_URL=https://uat---team-portal-ekaykywtvq-as.a.run.app", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_CLOUD_HOME_ENABLED=true", deploy_calls[0])
            self.assertIn("VERSION_PLAN_STORE_BACKEND=firestore", deploy_calls[0])
            self.assertIn("VERSION_PLAN_FIRESTORE_ENVIRONMENT=uat", deploy_calls[0])
            self.assertIn("LOCAL_AGENT_BASE_URL=https://app.bankpmtool.uk/uat-local-agent", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_RELEASE_REVISION=", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_DEPLOY_HASH=", deploy_calls[0])
            self.assertIn("--update-secrets LOCAL_AGENT_HMAC_SECRET=local-agent-uat-hmac-secret:latest", deploy_calls[0])
            self.assertIn("TRELLO_API_KEY=trello-key", deploy_calls[0])
            self.assertIn("TRELLO_API_TOKEN=trello-token", deploy_calls[0])
            self.assertIn("TRELLO_BOARD_ID=trello-board", deploy_calls[0])
            self.assertIn("TRELLO_DAILY_LIST_NAME=Daily Summary Email", deploy_calls[0])
            self.assertIn("Cloud Run UAT revision: team-portal-00091-uat", completed.stdout)
            self.assertIn("keeps live traffic unchanged", completed.stdout)

    def test_cloud_run_uat_deploy_supports_hash_based_skip(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"
        contents = deploy_script.read_text(encoding="utf-8")

        self.assertIn("CLOUD_RUN_UAT_SKIP_UNCHANGED", contents)
        self.assertIn("TEAM_PORTAL_DEPLOY_HASH", contents)
        self.assertIn("Cloud Run UAT deploy skipped", contents)
        self.assertIn("describe_revision \"$EXISTING_UAT_REVISION\"", contents)
        self.assertIn("sync_mac_local_agent_for_uat", contents)

    def test_cloud_run_uat_deploy_auto_selects_prebuilt_sha_image(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"
        contents = deploy_script.read_text(encoding="utf-8")

        self.assertIn("CLOUD_RUN_UAT_AUTO_PREBUILT_IMAGE", contents)
        self.assertIn("select_prebuilt_sha_image_if_available \"$GIT_SHA\"", contents)
        self.assertIn("artifacts docker tags list", contents)
        self.assertIn("grep -Fx \"$image_tag\"", contents)
        self.assertIn("Using prebuilt UAT image for current SHA", contents)
        self.assertIn("falling back to Cloud Run source deploy", contents)

    def test_cloud_run_uat_deploy_auto_falls_back_when_uat_secret_is_missing(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"
        contents = deploy_script.read_text(encoding="utf-8")

        self.assertIn("CLOUD_RUN_UAT_AUTO_ENV_FALLBACK_ON_MISSING_SECRET", contents)
        self.assertIn("select_uat_local_agent_secret_source", contents)
        self.assertIn("secrets versions access latest", contents)
        self.assertIn("using UAT env fallback", contents)
        self.assertIn("UAT_LOCAL_AGENT_SECRET_SOURCE=\"env\"", contents)

    def test_cloud_run_uat_deploy_reads_project_and_account_from_env_file(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            env_file = temp_path / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "GOOGLE_CLOUD_PROJECT=demo-project",
                        "CLOUD_RUN_DEPLOY_ACCOUNT=deploy@example.iam.gserviceaccount.com",
                        "TEAM_PORTAL_BASE_URL=https://app.bankpmtool.uk",
                        "TEAM_ALLOWED_EMAIL_DOMAINS=npt.sg",
                        "BPMIS_BASE_URL=https://bpmis.example.test",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "run services describe"* ]]; then
  printf '{"metadata":{"annotations":{"run.googleapis.com/invoker-iam-disabled":"true"}},"status":{"url":"https://team-portal-ekaykywtvq-as.a.run.app","traffic":[]}}\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  echo "unexpected deploy" >&2
  exit 44
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env=self._script_env(
                    PATH=f"{fake_bin}:{os.environ['PATH']}",
                    PYTHON_BIN=sys.executable,
                    ENV_FILE=str(env_file),
                    FAKE_GCLOUD_CALLS=str(calls_path),
                    CLOUD_RUN_UAT_SKIP_GIT_CHECK="1",
                    CLOUD_RUN_UAT_DRY_RUN="1",
                    CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY="0",
                ),
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            calls = calls_path.read_text(encoding="utf-8")
            self.assertIn(
                "run services describe team-portal --project demo-project --account deploy@example.iam.gserviceaccount.com",
                calls,
            )
            self.assertIn("Dry run only", completed.stdout)
            self.assertNotIn("unexpected deploy", completed.stderr)

    def test_cloud_run_uat_deploy_can_use_explicit_env_hmac_fallback(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "run services describe"* ]]; then
  printf '{"metadata":{"annotations":{"run.googleapis.com/invoker-iam-disabled":"true"}},"status":{"url":"https://team-portal-ekaykywtvq-as.a.run.app","traffic":[{"tag":"uat","url":"https://uat---team-portal-ekaykywtvq-as.a.run.app","revisionName":"team-portal-00091-uat","percent":0}]}}\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  exit 0
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_GCLOUD_CALLS": str(calls_path),
                    "CLOUD_RUN_UAT_SKIP_GIT_CHECK": "1",
                    "CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY": "0",
                    "CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_SOURCE": "env",
                    "CLOUD_RUN_UAT_LOCAL_AGENT_HMAC_SECRET": "uat-only-secret",
                    "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            calls = calls_path.read_text(encoding="utf-8")
            deploy_calls = [line for line in calls.splitlines() if line.startswith("run deploy")]
            self.assertEqual(len(deploy_calls), 1, msg=calls)
            self.assertNotIn("--update-secrets LOCAL_AGENT_HMAC_SECRET=", deploy_calls[0])
            self.assertIn("--set-secrets", deploy_calls[0])
            self.assertNotIn("local-agent-uat-hmac-secret", deploy_calls[0])
            self.assertIn("--set-env-vars", deploy_calls[0])
            self.assertIn("LOCAL_AGENT_HMAC_SECRET=uat-only-secret", deploy_calls[0])
            self.assertIn("CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_SOURCE=env", completed.stdout)

    def test_cloud_run_uat_env_hmac_fallback_replaces_secret_bindings_without_preclear_by_default(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "run services describe"* ]]; then
  printf '{"metadata":{"annotations":{"run.googleapis.com/invoker-iam-disabled":"true"}},"spec":{"template":{"spec":{"containers":[{"image":"existing-image","env":[{"name":"LOCAL_AGENT_HMAC_SECRET","valueFrom":{"secretKeyRef":{"name":"local-agent-uat-hmac-secret","key":"latest"}}}]}]}}},"status":{"url":"https://team-portal-ekaykywtvq-as.a.run.app","traffic":[{"tag":"uat","url":"https://uat---team-portal-ekaykywtvq-as.a.run.app","revisionName":"team-portal-00091-uat","percent":0}]}}\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  exit 0
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env=self._script_env(
                    PATH=f"{fake_bin}:{os.environ['PATH']}",
                    PYTHON_BIN=sys.executable,
                    FAKE_GCLOUD_CALLS=str(calls_path),
                    CLOUD_RUN_UAT_SKIP_GIT_CHECK="1",
                    CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY="0",
                    CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_SOURCE="env",
                    CLOUD_RUN_UAT_LOCAL_AGENT_HMAC_SECRET="uat-only-secret",
                    TEAM_PORTAL_BASE_URL="https://app.bankpmtool.uk",
                    TEAM_ALLOWED_EMAIL_DOMAINS="npt.sg",
                    BPMIS_BASE_URL="https://bpmis.example.test",
                ),
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            calls = calls_path.read_text(encoding="utf-8")
            deploy_calls = [line for line in calls.splitlines() if line.startswith("run deploy")]
            self.assertEqual(len(deploy_calls), 1, msg=calls)
            self.assertNotIn("--remove-secrets LOCAL_AGENT_HMAC_SECRET", deploy_calls[0])
            self.assertNotIn("--remove-env-vars LOCAL_AGENT_HMAC_SECRET", deploy_calls[0])
            self.assertIn("--set-secrets", deploy_calls[0])
            self.assertIn("--set-env-vars", deploy_calls[0])
            self.assertIn("LOCAL_AGENT_HMAC_SECRET=uat-only-secret", deploy_calls[0])

    def test_cloud_run_uat_deploy_syncs_isolated_mac_local_agent_by_default(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"
        contents = deploy_script.read_text(encoding="utf-8")

        self.assertIn("CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY:-1", contents)
        self.assertIn("CLOUD_RUN_UAT_LOCAL_AGENT_SYNC_MODE", contents)
        self.assertIn("CLOUD_RUN_UAT_PARALLEL_HOST_SYNC", contents)
        self.assertIn("classify_uat_local_agent_sync_mode", contents)
        self.assertIn("finish_uat_host_sync", contents)
        self.assertIn("CLOUD_RUN_UAT_HOST_WORKSPACE", contents)
        self.assertIn("recommended_uat_team_stack_root", contents)
        self.assertIn("git -C \"$host_workspace\" merge --ff-only \"$GIT_SHA\"", contents)
        self.assertIn("requirements.sha256", contents)
        self.assertIn("CLOUD_RUN_UAT_FORCE_INSTALL_HOST_DEPS", contents)
        self.assertIn("\"$host_workspace/.venv/bin/pip\" install -r \"$requirements_path\"", contents)
        self.assertIn("BriefingStore(data_path / \"prd_briefing\")", contents)
        self.assertIn("UAT host .env is missing LOCAL_AGENT_HMAC_SECRET", contents)
        self.assertIn("LOCAL_AGENT_HMAC_SECRET=\"$uat_local_agent_hmac_secret\"", contents)
        self.assertIn("LOCAL_AGENT_PORT=\"$UAT_LOCAL_AGENT_PORT\"", contents)
        self.assertIn("LOCAL_AGENT_TEAM_PORTAL_DATA_DIR=\"$UAT_LOCAL_AGENT_DATA_DIR\"", contents)
        self.assertIn("LOCAL_AGENT_SCREEN_SESSION=\"$UAT_LOCAL_AGENT_SCREEN_SESSION\"", contents)
        self.assertIn("assert_no_active_meeting_recording_before_local_agent_restart", contents)
        self.assertIn("./scripts/run_local_agent.sh restart", contents)
        self.assertIn("CLOUD_RUN_UAT_VERIFY_SOURCE_CODE_QA_OPS:-1", contents)
        self.assertIn("source_code_qa_ops_summary.py\" --strict", contents)
        self.assertIn("/uat-local-agent", contents)

    def test_cloud_run_uat_deploy_has_explicit_ui_only_sync_skip(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"
        contents = deploy_script.read_text(encoding="utf-8")

        self.assertIn("uat_local_agent_sync_requires_file", contents)
        self.assertIn("static/*|templates/*|tests/*|docs/*|README.md|.dockerignore|.github/*", contents)
        self.assertIn("app.py|bpmis_jira_tool/web.py|bpmis_jira_tool/web_*.py", contents)
        self.assertIn('if uat_local_agent_sync_requires_file "$changed_file"; then', contents)
        self.assertIn("Skipping UAT Mac local-agent sync/restart", contents)

    def test_live_promotion_skips_local_agent_restart_for_cloud_run_web_ui_changes(self):
        promote_script = PROJECT_ROOT / "scripts/promote_uat_to_live.sh"
        contents = promote_script.read_text(encoding="utf-8")

        self.assertIn("classify_live_restart_mode", contents)
        self.assertIn("classify_live_local_agent_restart_mode", contents)
        self.assertIn("app.py|bpmis_jira_tool/web.py|bpmis_jira_tool/web_*.py|static/*|templates/*", contents)
        self.assertIn("live_local_agent_restart_requires_file", contents)
        self.assertIn("Skipping live local-agent restart", contents)
        self.assertIn("PROMOTE_UAT_LOCAL_AGENT_RESTART_MODE", contents)

    def test_release_checklist_documents_full_gate_and_read_only_smoke(self):
        checklist = (PROJECT_ROOT / "docs/release-checklist.md").read_text(encoding="utf-8")

        self.assertIn("System Full Test Gate", checklist)
        self.assertIn("./.venv/bin/python scripts/run_system_full_test_gate.py --skip-smoke", checklist)
        self.assertIn("./.venv/bin/python scripts/run_system_full_test_gate.py --smoke-only", checklist)
        self.assertIn("ENV_FILE=/dev/null", checklist)
        self.assertIn("--coverage-fail-under 100", checklist)
        self.assertIn("risk_coverage_gate", checklist)
        self.assertIn("config/coverage_risk_policy.json", checklist)
        self.assertIn("./.venv/bin/python -m coverage run --rcfile=/dev/null --source=bpmis_jira_tool,prd_briefing -m unittest discover -s tests", checklist)
        self.assertIn("scripts/check_coverage_policy.py --coverage-json .team-portal/run/system_full_coverage.json --policy config/coverage_risk_policy.json --governed-fail-under 100", checklist)
        self.assertIn("node --check static/gmail_seatalk_demo.js", checklist)
        self.assertIn("./.venv/bin/python scripts/run_source_code_qa_release_gate.py", checklist)
        self.assertIn("read-only", checklist)
        self.assertIn("--uat-url \"$UAT_URL\"", checklist)
        self.assertIn("--live-url \"$LIVE_URL\"", checklist)
        self.assertIn("--expected-revision \"$EXPECTED_REVISION\"", checklist)
        self.assertIn("--expect-live-promoted", checklist)
        self.assertIn("curl -fsS \"$UAT_URL/healthz/\"", checklist)
        self.assertIn("curl -fsS \"$UAT_URL/api/local-agent/healthz\"", checklist)
        self.assertIn("curl -fsS \"$LIVE_URL/healthz\"", checklist)
        self.assertIn("curl -fsS \"$LIVE_URL/api/local-agent/healthz\"", checklist)
        for unsafe_command in (
            "curl -X POST",
            "curl --request POST",
            "/api/jobs/run",
            "/api/team-dashboard/monthly-report/send",
            "/api/meeting-recorder",
        ):
            self.assertNotIn(unsafe_command, checklist)

    def test_system_full_test_gate_isolates_env_file_by_default(self):
        import importlib.util

        gate_path = PROJECT_ROOT / "scripts/run_system_full_test_gate.py"
        spec = importlib.util.spec_from_file_location("run_system_full_test_gate", gate_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        captured_envs: list[dict[str, str]] = []

        def fake_run(*args, **kwargs):
            captured_envs.append(kwargs["env"])
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.dict(os.environ, {}, clear=True), patch("subprocess.run", side_effect=fake_run):
            module._run_command("unit", [sys.executable, "-c", "pass"])

        self.assertEqual(captured_envs[0]["ENV_FILE"], os.devnull)

    def test_system_full_test_gate_preserves_explicit_env_file(self):
        import importlib.util

        gate_path = PROJECT_ROOT / "scripts/run_system_full_test_gate.py"
        spec = importlib.util.spec_from_file_location("run_system_full_test_gate", gate_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        captured_envs: list[dict[str, str]] = []

        def fake_run(*args, **kwargs):
            captured_envs.append(kwargs["env"])
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.dict(os.environ, {"ENV_FILE": "/tmp/explicit.env"}, clear=True), patch(
            "subprocess.run",
            side_effect=fake_run,
        ):
            module._run_command("unit", [sys.executable, "-c", "pass"])

        self.assertEqual(captured_envs[0]["ENV_FILE"], "/tmp/explicit.env")

    def test_system_full_test_gate_can_validate_promoted_live_revision(self):
        import importlib.util

        gate_path = PROJECT_ROOT / "scripts/run_system_full_test_gate.py"
        spec = importlib.util.spec_from_file_location("run_system_full_test_gate", gate_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        def fake_fetch(url):
            if url.endswith("/healthz/") or url.endswith("/healthz"):
                return {"status": "ok", "revision": "abc123"}
            return {"status": "ok"}

        with patch.object(module, "_fetch_json", side_effect=fake_fetch):
            pre_promotion = module._smoke_check(
                uat_url="https://uat.example",
                live_url="https://live.example",
                expected_revision="abc123",
            )
            post_promotion = module._smoke_check(
                uat_url="https://uat.example",
                live_url="https://live.example",
                expected_revision="abc123",
                expect_live_promoted=True,
            )

        self.assertEqual(pre_promotion.status, "fail")
        self.assertIn("Live already serves", pre_promotion.stderr)
        self.assertEqual(post_promotion.status, "pass")

    def test_uat_local_agent_setup_uses_separate_workspace_port_and_data_root(self):
        setup_script = (PROJECT_ROOT / "scripts/setup_uat_local_agent.sh").read_text(encoding="utf-8")

        self.assertIn("recommended_uat_team_stack_root", setup_script)
        self.assertIn(".team-portal-uat", setup_script)
        self.assertIn("7008", setup_script)
        self.assertIn("bpmis-local-agent-uat", setup_script)
        self.assertIn("/opt/homebrew/bin/python3.12", setup_script)
        self.assertIn('rm -rf "$UAT_WORKSPACE/.venv"', setup_script)
        self.assertIn("LOCAL_AGENT_HMAC_SECRET", setup_script)
        self.assertIn("--exclude '/logs/'", setup_script)
        self.assertIn("--exclude '/run/'", setup_script)

    def test_local_agent_launcher_uses_agent_data_root_for_pid_and_logs(self):
        script = (PROJECT_ROOT / "scripts/run_local_agent.sh").read_text(encoding="utf-8")

        self.assertIn("LOCAL_AGENT_TEAM_PORTAL_DATA_DIR", script)
        self.assertIn('DATA_DIR="$(resolve_team_data_dir "$AGENT_DATA_DIR")"', script)

    def test_promote_uat_to_live_fails_when_origin_main_differs_from_uat_commit(self):
        promote_script = PROJECT_ROOT / "scripts/promote_uat_to_live.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            origin_path = temp_path / "origin"
            host_path = temp_path / "host"
            seed_path = temp_path / "seed"
            subprocess.run(["git", "init", "-b", "main", str(seed_path)], check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=seed_path, check=True)
            subprocess.run(["git", "config", "user.name", "Test User"], cwd=seed_path, check=True)
            (seed_path / "README.md").write_text("first\n", encoding="utf-8")
            subprocess.run(["git", "add", "README.md"], cwd=seed_path, check=True)
            subprocess.run(["git", "commit", "-m", "first"], cwd=seed_path, check=True, capture_output=True)
            uat_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=seed_path, text=True).strip()
            subprocess.run(["git", "clone", "--bare", str(seed_path), str(origin_path)], check=True, capture_output=True)
            subprocess.run(["git", "clone", str(origin_path), str(host_path)], check=True, capture_output=True)
            (seed_path / "README.md").write_text("second\n", encoding="utf-8")
            subprocess.run(["git", "commit", "-am", "second"], cwd=seed_path, check=True, capture_output=True)
            subprocess.run(["git", "remote", "add", "origin", str(origin_path)], cwd=seed_path, check=True)
            subprocess.run(["git", "push", "origin", "main"], cwd=seed_path, check=True, capture_output=True)

            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

if [[ "$*" == "run services describe"* ]]; then
  printf '{"status":{"traffic":[{"tag":"uat","url":"https://uat---team-portal-ekaykywtvq-as.a.run.app","revisionName":"team-portal-00091-uat","percent":0}]}}\\n'
  exit 0
fi
if [[ "$*" == "run revisions describe"* ]]; then
  printf '{"spec":{"containers":[{"env":[{"name":"TEAM_PORTAL_RELEASE_REVISION","value":"%s"}]}]}}\\n' "$FAKE_UAT_COMMIT"
  exit 0
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(promote_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_UAT_COMMIT": uat_commit,
                    "TEAM_STACK_HOST_ROOT": str(host_path),
                    "PROMOTE_UAT_DRY_RUN": "1",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("UAT commit is not the current origin/main", completed.stdout)

    def test_cloud_run_default_deploy_still_uses_source(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "run services describe"* ]]; then
  printf 'https://team-portal-example.run.app\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  exit 0
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_GCLOUD_CALLS": str(calls_path),
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://app.bankpmtool.uk",
                    "CLOUD_RUN_RESTART_LOCAL_AGENT_AFTER_DEPLOY": "0",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            calls = calls_path.read_text(encoding="utf-8")
            deploy_calls = [line for line in calls.splitlines() if line.startswith("run deploy")]
            traffic_calls = [line for line in calls.splitlines() if line.startswith("run services update-traffic")]
            self.assertEqual(len(deploy_calls), 1, msg=calls)
            self.assertEqual(len(traffic_calls), 1, msg=calls)
            self.assertIn("--source .", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_DATA_DIR=/workspace/team-portal-runtime", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_STAGE=live", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_RELEASE_REVISION=", deploy_calls[0])
            self.assertIn("LOCAL_AGENT_BASE_URL=https://app.bankpmtool.uk", deploy_calls[0])
            self.assertNotIn("/tmp/team-portal", deploy_calls[0])
            self.assertNotIn("--image", deploy_calls[0])
            self.assertIn("--to-latest", traffic_calls[0])
            self.assertIn("Cloud Run traffic moved to latest revision", completed.stdout)

    def test_cloud_run_deploy_skips_iam_binding_when_invoker_iam_is_disabled(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "run services describe"* ]]; then
  printf '{"metadata":{"annotations":{"run.googleapis.com/invoker-iam-disabled":"true"}},"status":{"url":"https://team-portal-example.run.app"}}\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  exit 0
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_GCLOUD_CALLS": str(calls_path),
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://app.bankpmtool.uk",
                    "CLOUD_RUN_RESTART_LOCAL_AGENT_AFTER_DEPLOY": "0",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            calls = calls_path.read_text(encoding="utf-8")
            deploy_calls = [line for line in calls.splitlines() if line.startswith("run deploy")]
            self.assertEqual(len(deploy_calls), 1, msg=calls)
            self.assertNotIn("--allow-unauthenticated", deploy_calls[0])
            self.assertIn("invoker IAM check is disabled", completed.stdout)

    def test_cloud_run_image_deploy_path_is_opt_in(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "run services describe"* ]]; then
  printf 'https://team-portal-example.run.app\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  exit 0
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)
            image = "asia-southeast1-docker.pkg.dev/demo/team-portal/team-portal:test"

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_GCLOUD_CALLS": str(calls_path),
                    "CLOUD_RUN_IMAGE": image,
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://app.bankpmtool.uk",
                    "CLOUD_RUN_RESTART_LOCAL_AGENT_AFTER_DEPLOY": "0",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            calls = calls_path.read_text(encoding="utf-8")
            deploy_calls = [line for line in calls.splitlines() if line.startswith("run deploy")]
            self.assertEqual(len(deploy_calls), 1, msg=calls)
            self.assertIn(f"--image {image}", deploy_calls[0])
            self.assertNotIn("--source .", deploy_calls[0])

    def test_cloud_run_deploy_passes_opt_in_runtime_tuning_args(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "run services describe"* ]]; then
  printf 'https://team-portal-example.run.app\\n'
  exit 0
fi
if [[ "$*" == "run deploy"* ]]; then
  exit 0
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_GCLOUD_CALLS": str(calls_path),
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://app.bankpmtool.uk",
                    "CLOUD_RUN_RESTART_LOCAL_AGENT_AFTER_DEPLOY": "0",
                    "CLOUD_RUN_MIN_INSTANCES": "1",
                    "CLOUD_RUN_CPU_BOOST": "true",
                    "CLOUD_RUN_CPU": "2",
                    "CLOUD_RUN_MEMORY": "1Gi",
                    "CLOUD_RUN_CONCURRENCY": "40",
                    "CLOUD_RUN_TIMEOUT": "600s",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            calls = calls_path.read_text(encoding="utf-8")
            deploy_calls = [line for line in calls.splitlines() if line.startswith("run deploy")]
            self.assertEqual(len(deploy_calls), 1, msg=calls)
            self.assertIn("--min-instances=1", deploy_calls[0])
            self.assertIn("--cpu-boost=true", deploy_calls[0])
            self.assertIn("--cpu=2", deploy_calls[0])
            self.assertIn("--memory=1Gi", deploy_calls[0])
            self.assertIn("--concurrency=40", deploy_calls[0])
            self.assertIn("--timeout=600s", deploy_calls[0])

    def test_cloud_run_deploy_script_restarts_local_agent_by_default(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run.sh"

        contents = deploy_script.read_text(encoding="utf-8")

        self.assertIn("CLOUD_RUN_RESTART_LOCAL_AGENT_AFTER_DEPLOY:-1", contents)
        self.assertIn("assert_no_active_meeting_recording_before_local_agent_restart", contents)
        self.assertIn('"$ROOT_DIR/scripts/run_local_agent.sh" restart', contents)

    def test_local_agent_restart_does_not_ignore_guarded_stop_failure(self):
        local_agent_script = (PROJECT_ROOT / "scripts/run_local_agent.sh").read_text(encoding="utf-8")

        self.assertIn("restart() {", local_agent_script)
        self.assertIn('assert_no_active_meeting_recording_before_local_agent_restart "restart Mac local-agent" "$DATA_DIR"', local_agent_script)
        self.assertIn("\n  stop\n  start\n", local_agent_script)
        self.assertNotIn("stop || true\n  start", local_agent_script)

    def test_mac_stack_restart_refreshes_local_agent_when_bpmis_proxy_is_enabled(self):
        stack_script = PROJECT_ROOT / "scripts/run_team_stack.sh"

        contents = stack_script.read_text(encoding="utf-8")

        self.assertIn("restart_local_agent_if_needed", contents)
        self.assertIn('read_env_value BPMIS_CALL_MODE', contents)
        self.assertIn("assert_no_active_meeting_recording_before_local_agent_restart", contents)
        self.assertIn('"$ROOT_DIR/scripts/run_local_agent.sh" restart', contents)

    def test_local_agent_restart_guard_detects_active_meeting_recording(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            record_dir = data_root / "meeting_records" / "records" / "meeting-active"
            record_dir.mkdir(parents=True)
            status_path = record_dir / "screencapture-status.json"
            status_path.write_text(json.dumps({"status": "recording"}), encoding="utf-8")
            (record_dir / "metadata.json").write_text(
                json.dumps(
                    {
                        "record_id": "meeting-active",
                        "title": "Live Review",
                        "status": "recording",
                        "recording_started_at": "2026-05-11T06:03:51+00:00",
                        "media": {
                            "screencapture_status_path": str(status_path),
                        },
                    }
                ),
                encoding="utf-8",
            )

            completed = self._run_team_env_helper(
                f'meeting_recorder_active_recordings "{data_root}"',
            )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertIn("meeting-active", completed.stdout)
        self.assertIn("ScreenCaptureKit status is recording", completed.stdout)

    def test_local_agent_restart_guard_ignores_stopped_meeting_recording(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            record_dir = data_root / "meeting_records" / "records" / "meeting-stopped"
            record_dir.mkdir(parents=True)
            (record_dir / "metadata.json").write_text(
                json.dumps(
                    {
                        "record_id": "meeting-stopped",
                        "title": "Completed Review",
                        "status": "recording",
                        "recording_started_at": "2026-05-11T06:03:51+00:00",
                        "recording_stopped_at": "2026-05-11T07:23:58+00:00",
                    }
                ),
                encoding="utf-8",
            )

            completed = self._run_team_env_helper(
                f'meeting_recorder_active_recordings "{data_root}"',
            )

        self.assertEqual(completed.returncode, 1, msg=completed.stdout + completed.stderr)
        self.assertEqual(completed.stdout, "")

    def test_local_agent_restart_guard_blocks_when_active_meeting_recording_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            record_dir = data_root / "meeting_records" / "records" / "meeting-active"
            record_dir.mkdir(parents=True)
            status_path = record_dir / "screencapture-status.json"
            status_path.write_text(json.dumps({"status": "recording"}), encoding="utf-8")
            (record_dir / "metadata.json").write_text(
                json.dumps(
                    {
                        "record_id": "meeting-active",
                        "title": "Live Review",
                        "status": "recording",
                        "recording_started_at": "2026-05-11T06:03:51+00:00",
                        "media": {
                            "screencapture_status_path": str(status_path),
                        },
                    }
                ),
                encoding="utf-8",
            )

            completed = self._run_team_env_helper(
                f'assert_no_active_meeting_recording_before_local_agent_restart "restart Mac local-agent" "{data_root}"',
            )

        self.assertEqual(completed.returncode, 1, msg=completed.stdout + completed.stderr)
        self.assertIn("Refusing to restart Mac local-agent", completed.stdout)
        self.assertIn("meeting-active", completed.stdout)

    def test_mac_stack_supports_portal_only_restart(self):
        stack_script = PROJECT_ROOT / "scripts/run_team_stack.sh"

        contents = stack_script.read_text(encoding="utf-8")

        self.assertIn("restart-portal", contents)
        self.assertIn("restart_portal()", contents)
        self.assertIn('"$ROOT_DIR/scripts/run_team_portal_prod.sh" restart', contents)

    def test_mac_stack_supports_guard_restart_without_local_agent(self):
        stack_script = PROJECT_ROOT / "scripts/run_team_stack.sh"

        contents = stack_script.read_text(encoding="utf-8")

        self.assertIn("restart-guard", contents)
        self.assertIn("restart_guard()", contents)
        self.assertIn('restart) restart "$GUARD_ENV"', contents)
        self.assertIn('restart-guard) restart_guard "$GUARD_ENV"', contents)

    def test_team_portal_slot_script_starts_candidate_revision(self):
        slot_script = (PROJECT_ROOT / "scripts/run_team_portal_slot.sh").read_text(encoding="utf-8")

        self.assertIn("TEAM_PORTAL_SLOT_PORT", slot_script)
        self.assertIn("TEAM_PORTAL_SLOT_REVISION", slot_script)
        self.assertIn("TEAM_PORTAL_RELEASE_REVISION=$REVISION", slot_script)
        self.assertIn("slot_revision_matches", slot_script)
        self.assertIn("TEAM_PORTAL_SLOT_REPLACE_STALE", slot_script)
        self.assertIn("pid_listens_on_slot_port", slot_script)

    def test_promote_uat_to_live_supports_change_aware_restart(self):
        promote_script = PROJECT_ROOT / "scripts/promote_uat_to_live.sh"

        contents = promote_script.read_text(encoding="utf-8")

        self.assertIn("PROMOTE_UAT_RESTART_MODE", contents)
        self.assertIn("classify_live_restart_mode", contents)
        self.assertIn("classify_live_local_agent_restart_mode", contents)
        self.assertIn("live_local_agent_restart_requires_file", contents)
        self.assertIn("PROMOTE_UAT_BLUE_GREEN_VALIDATE", contents)
        self.assertIn("TEAM_PORTAL_SLOT_REPLACE_STALE=1", contents)
        self.assertIn("run_team_portal_slot.sh", contents)
        self.assertIn("assert_no_active_meeting_recording_before_local_agent_restart", contents)
        self.assertIn("Skipping live local-agent restart", contents)
        self.assertIn("restart-guard", contents)
        self.assertIn("Live restart mode:", contents)

    def test_deploy_scripts_persist_timing_metrics(self):
        helper = (PROJECT_ROOT / "scripts/lib/team_env.sh").read_text(encoding="utf-8")
        uat_script = (PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh").read_text(encoding="utf-8")
        live_script = (PROJECT_ROOT / "scripts/promote_uat_to_live.sh").read_text(encoding="utf-8")
        build_script = (PROJECT_ROOT / "scripts/build_cloud_run_image.sh").read_text(encoding="utf-8")
        report_script = (PROJECT_ROOT / "scripts/report_deploy_timings.py").read_text(encoding="utf-8")

        self.assertIn("team_deploy_timing_file", helper)
        self.assertIn("deploy_timings.jsonl", helper)
        self.assertIn("record_deploy_timing", helper)
        self.assertIn("record_uat_deploy_timing_on_exit", uat_script)
        self.assertIn("record_uat_stage_timing", uat_script)
        self.assertIn("prebuilt_image_lookup", uat_script)
        self.assertIn("cloud_run_deploy", uat_script)
        self.assertIn("uat_host_sync", uat_script)
        self.assertIn("record_promote_timing_on_exit", live_script)
        self.assertIn("Deploy UAT with: CLOUD_RUN_IMAGE=$IMAGE_URI ./scripts/deploy_cloud_run_uat.sh", build_script)
        self.assertIn("Recent deploy timings", report_script)
        self.assertIn("Averages by script/phase", report_script)

    def test_fast_uat_release_orchestrator_uses_fast_paths(self):
        script = (PROJECT_ROOT / "scripts/release_uat_fast.sh").read_text(encoding="utf-8")

        self.assertIn("run_system_full_test_gate.py", script)
        self.assertIn("enforce_release_window_target uat", script)
        self.assertIn("--parallel-workers", script)
        self.assertIn("run_gate_and_image_in_parallel", script)
        self.assertIn("cloud_run_image_policy.sh", script)
        self.assertIn("find_reusable_image_without_runtime_changes", script)
        self.assertIn("RELEASE_UAT_FAST_REUSE_IMAGE_WITHOUT_RUNTIME_CHANGES", script)
        self.assertIn("RELEASE_UAT_FAST_REUSE_VERIFIED_GATE", script)
        self.assertIn("--check-proof", script)
        self.assertIn("RELEASE_UAT_FAST_GATE_PROOF_MAX_AGE_SECONDS", script)
        self.assertIn("wait_for_github_image_workflow", script)
        self.assertIn("run watch \"$run_id\"", script)
        self.assertIn("RELEASE_UAT_FAST_BUILD_IMAGE_FALLBACK", script)
        self.assertIn("build_cloud_run_image.sh", script)
        self.assertIn("CLOUD_RUN_UAT_SKIP_UNCHANGED", script)
        self.assertIn("CLOUD_RUN_UAT_PARALLEL_HOST_SYNC", script)
        self.assertIn("deploy_cloud_run_uat.sh", script)

    def test_fast_uat_live_release_orchestrator_is_release_window_aware(self):
        script = (PROJECT_ROOT / "scripts/release_uat_live_fast.sh").read_text(encoding="utf-8")

        self.assertIn("run_gate_and_image_in_parallel", script)
        self.assertIn("release_window_target", script)
        self.assertIn("selected UAT default path", script)
        self.assertIn("selected Live default path", script)
        self.assertIn("Live already serves $SHA; skipping Cloud Run/UAT gcloud promotion checks.", script)
        self.assertIn("require_gcloud_noninteractive_auth", script)
        self.assertIn("gcloud credentials are not usable non-interactively", script)
        self.assertIn("cloud_run_image_policy.sh", script)
        self.assertIn("find_reusable_image_without_runtime_changes", script)
        self.assertIn("RELEASE_UAT_LIVE_REUSE_IMAGE_WITHOUT_RUNTIME_CHANGES", script)
        self.assertIn("RELEASE_UAT_LIVE_REUSE_VERIFIED_GATE", script)
        self.assertIn("--check-proof", script)
        self.assertIn("RELEASE_UAT_LIVE_GATE_PROOF_MAX_AGE_SECONDS", script)
        self.assertIn("wait_for_github_image_workflow", script)
        self.assertIn("run watch \"$run_id\"", script)
        self.assertIn("release_uat_fast.sh", script)
        self.assertIn("run_system_full_test_gate.py", script)
        self.assertIn("promote_uat_to_live.sh", script)
        self.assertIn("run_team_stack.sh\" doctor", script)
        self.assertIn("report_deploy_timings.py", script)

    def test_release_window_policy_routes_targets_by_singapore_business_hours(self):
        helper_path = PROJECT_ROOT / "scripts/lib/release_window_policy.sh"
        team_env_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        command = f'''
source "{team_env_path}"
source "{helper_path}"
RELEASE_WINDOW_POLICY_NOW="2026-05-08T10:00:00+08:00" release_window_target
RELEASE_WINDOW_POLICY_NOW="2026-05-08T18:59:00+08:00" release_window_target
RELEASE_WINDOW_POLICY_NOW="2026-05-08T19:00:00+08:00" release_window_target
RELEASE_WINDOW_POLICY_NOW="2026-05-09T12:00:00+08:00" release_window_target
RELEASE_WINDOW_POLICY_NOW="2026-05-11T09:59:00+08:00" release_window_target
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env=self._script_env(PYTHON_BIN=sys.executable),
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertEqual(completed.stdout.splitlines(), ["uat", "uat", "live", "live", "live"])

    def test_release_window_policy_allows_uat_anytime(self):
        helper_path = PROJECT_ROOT / "scripts/lib/release_window_policy.sh"
        team_env_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        command = f'''
source "{team_env_path}"
source "{helper_path}"
RELEASE_WINDOW_POLICY_NOW="2026-05-08T12:00:00+08:00" enforce_release_window_target uat
RELEASE_WINDOW_POLICY_NOW="2026-05-08T19:00:00+08:00" enforce_release_window_target uat
RELEASE_WINDOW_POLICY_NOW="2026-05-09T12:00:00+08:00" enforce_release_window_target uat
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env=self._script_env(PYTHON_BIN=sys.executable, RELEASE_WINDOW_POLICY_BYPASS="0"),
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)

    def test_release_window_policy_allows_live_outside_weekday_business_hours(self):
        helper_path = PROJECT_ROOT / "scripts/lib/release_window_policy.sh"
        team_env_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        command = f'''
source "{team_env_path}"
source "{helper_path}"
RELEASE_WINDOW_POLICY_NOW="2026-05-08T19:00:00+08:00" enforce_release_window_target live
RELEASE_WINDOW_POLICY_NOW="2026-05-09T12:00:00+08:00" enforce_release_window_target live
RELEASE_WINDOW_POLICY_NOW="2026-05-11T09:59:00+08:00" enforce_release_window_target live
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env=self._script_env(PYTHON_BIN=sys.executable, RELEASE_WINDOW_POLICY_BYPASS="0"),
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)

    def test_release_window_policy_blocks_wrong_target_by_default(self):
        helper_path = PROJECT_ROOT / "scripts/lib/release_window_policy.sh"
        team_env_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        scenarios = [
            ("2026-05-08T12:00:00+08:00", "live", "uat"),
        ]

        for timestamp, requested, allowed in scenarios:
            command = f'''
source "{team_env_path}"
source "{helper_path}"
RELEASE_WINDOW_POLICY_NOW="{timestamp}" enforce_release_window_target {requested}
'''
            completed = subprocess.run(
                ["bash", "-lc", command],
                capture_output=True,
                text=True,
                check=False,
                env=self._script_env(PYTHON_BIN=sys.executable, RELEASE_WINDOW_POLICY_BYPASS="0"),
            )

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn(f"blocked '{requested}' release", completed.stderr)
            self.assertIn(f"Default target: {allowed}", completed.stderr)
            self.assertIn(f"Allowed targets: {allowed}", completed.stderr)

    def test_deploy_scripts_enforce_release_window_policy(self):
        uat_script = (PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh").read_text(encoding="utf-8")
        live_script = (PROJECT_ROOT / "scripts/promote_uat_to_live.sh").read_text(encoding="utf-8")

        self.assertIn("release_window_policy.sh", uat_script)
        self.assertIn("enforce_release_window_target uat", uat_script)
        self.assertIn("release_window_policy.sh", live_script)
        self.assertIn("enforce_release_window_target live", live_script)

    def test_report_deploy_timings_summarizes_recent_records(self):
        report_script = PROJECT_ROOT / "scripts/report_deploy_timings.py"

        with tempfile.TemporaryDirectory() as temp_dir:
            timing_file = Path(temp_dir) / "deploy_timings.jsonl"
            timing_file.write_text(
                "\n".join(
                    [
                        '{"script":"deploy_cloud_run_uat.sh","phase":"cloud_run_deploy","status":0,"duration_seconds":16,"details":"image=sha"}',
                        '{"script":"promote_uat_to_live.sh","phase":"script","status":0,"duration_seconds":9,"details":"host=mac"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [sys.executable, str(report_script), "--file", str(timing_file), "--limit", "2"],
                capture_output=True,
                text=True,
                check=False,
                cwd=PROJECT_ROOT,
            )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertIn("Recent deploy timings", completed.stdout)
        self.assertIn("cloud_run_deploy", completed.stdout)
        self.assertIn("Averages by script/phase", completed.stdout)

    def test_cloud_run_image_build_path_uses_speed_tuning_and_lean_context(self):
        build_script = (PROJECT_ROOT / "scripts/build_cloud_run_image.sh").read_text(encoding="utf-8")
        dockerignore = (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8")
        workflow = (PROJECT_ROOT / ".github/workflows/cloud-run-image.yml").read_text(encoding="utf-8")

        self.assertIn("E2_HIGHCPU_8", build_script)
        self.assertIn("--disk-size", build_script)
        self.assertIn("CLOUD_RUN_DEPLOY_ACCOUNT", build_script)
        self.assertIn("artifacts repositories create", build_script)
        self.assertIn("git -C \"$ROOT_DIR\" rev-parse HEAD", build_script)
        self.assertIn("--async", build_script)
        self.assertIn("builds describe \"$BUILD_ID\"", build_script)
        self.assertIn("git -C \"$ROOT_DIR\" rev-parse HEAD", (PROJECT_ROOT / "scripts/release_uat_fast.sh").read_text(encoding="utf-8"))
        self.assertIn("docs/", dockerignore)
        self.assertIn("source_code_qa/", dockerignore)
        self.assertIn("tests/", dockerignore)
        self.assertIn("./scripts/build_cloud_run_image.sh", workflow)
        self.assertIn("actions/checkout@v6", workflow)
        self.assertIn("google-github-actions/auth@v3", workflow)
        self.assertIn("google-github-actions/setup-gcloud@v3", workflow)
        self.assertIn("CLOUD_RUN_IMAGE_TAG=\"$GITHUB_SHA\"", workflow)
        self.assertIn("FORCE_JAVASCRIPT_ACTIONS_TO_NODE24", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("push:", workflow)
        self.assertIn("concurrency:", workflow)

    def test_cloud_run_image_policy_distinguishes_runtime_inputs(self):
        helper_path = PROJECT_ROOT / "scripts/lib/cloud_run_image_policy.sh"
        command = f'''
source "{helper_path}"
cloud_run_image_runtime_path_requires_image "bpmis_jira_tool/web.py"
printf 'runtime=%s\\n' "$?"
cloud_run_image_runtime_path_requires_image "docs/release-checklist.md"
printf 'docs=%s\\n' "$?"
cloud_run_image_trigger_included_files_csv
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "ROOT_DIR": str(PROJECT_ROOT)},
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertIn("runtime=0", completed.stdout)
        self.assertIn("docs=1", completed.stdout)
        self.assertIn("Dockerfile", completed.stdout)
        self.assertIn("bpmis_jira_tool/**", completed.stdout)

    def test_cloud_build_trigger_setup_script_uses_runtime_included_files(self):
        script = (PROJECT_ROOT / "scripts/setup_cloud_build_image_trigger.sh").read_text(encoding="utf-8")

        self.assertIn("cloud_run_image_trigger_included_files_csv", script)
        self.assertIn("CLOUD_BUILD_GITHUB_CONNECTION_NAME", script)
        self.assertIn("builds repositories create", script)
        self.assertIn("builds triggers create github", script)
        self.assertIn("CLOUD_BUILD_IMAGE_TRIGGER_RECREATE", script)
        self.assertIn("builds triggers delete", script)
        self.assertIn("--repository \"$repository_resource\"", script)
        self.assertIn("--included-files", script)
        self.assertIn("_TAG=\\$COMMIT_SHA", script)

    def test_cloud_run_full_deploy_skips_base_url_update_when_service_exists(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_full.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            calls_path = temp_path / "gcloud-calls.log"
            google_secret = temp_path / "google-client-secret.json"
            google_secret.write_text('{"web":{}}', encoding="utf-8")
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "$FAKE_GCLOUD_CALLS"
if [[ "$*" == "auth list"* ]]; then
  printf 'teammate@example.com\\n'
  exit 0
fi
if [[ "$*" == *"run services describe"* ]]; then
  printf 'https://team-portal-example.run.app\\n'
  exit 0
fi
if [[ "$*" == *"projects describe"* ]]; then
  printf '123456789\\n'
  exit 0
fi
if [[ "$*" == *"secrets describe"* ]]; then
  exit 0
fi
if [[ "$*" == *"secrets versions access latest"* && "$*" == *"team-portal-config-encryption-key"* ]]; then
  printf 'config-key'
  exit 0
fi
if [[ "$*" == *"secrets versions access latest"* && "$*" == *"local-agent-hmac-secret"* ]]; then
  printf 'shared-secret'
  exit 0
fi
if [[ "$*" == *"secrets versions access latest"* && "$*" == *"google-oauth-client-secret-json"* ]]; then
  printf '{"web":{}}'
  exit 0
fi
if [[ "$*" == *"run deploy"* ]]; then
  exit 0
fi
if [[ "$*" == *"run services update"* ]]; then
  echo "unexpected update" >&2
  exit 46
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(deploy_script)],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_GCLOUD_CALLS": str(calls_path),
                    "GOOGLE_CLOUD_PROJECT": "demo-project",
                    "CLOUD_RUN_SKIP_SERVICE_ENABLE": "1",
                    "CLOUD_RUN_SKIP_IAM_BINDINGS": "1",
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://app.bankpmtool.uk",
                    "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
                    "GOOGLE_OAUTH_CLIENT_SECRET_FILE": str(google_secret),
                    "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "config-key",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}")
            calls = calls_path.read_text(encoding="utf-8")
            deploy_calls = [line for line in calls.splitlines() if "run deploy" in line]
            update_calls = [line for line in calls.splitlines() if "run services update" in line]
            self.assertEqual(len(deploy_calls), 1, msg=calls)
            self.assertEqual(update_calls, [], msg=calls)
            self.assertIn("TEAM_PORTAL_DATA_DIR=/workspace/team-portal-runtime", deploy_calls[0])
            self.assertNotIn("/tmp/team-portal", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_BASE_URL=https://team-portal-example.run.app", deploy_calls[0])
            self.assertIn("base URL update skipped", completed.stdout)
            self.assertNotIn("unexpected update", completed.stderr)

    def test_cloud_run_scripts_do_not_default_to_tmp_team_portal(self):
        for relative_path in ("scripts/deploy_cloud_run.sh", "scripts/deploy_cloud_run_full.sh"):
            script = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
            self.assertNotIn("/tmp/team-portal", script)
            self.assertIn("/workspace/team-portal-runtime", script)

    def test_cloud_run_image_build_script_is_dry_run_safe(self):
        build_script = PROJECT_ROOT / "scripts/build_cloud_run_image.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            gcloud_path = fake_bin / "gcloud"
            gcloud_path.write_text(
                """#!/usr/bin/env bash
set -euo pipefail

if [[ "$*" == "config get-value project"* ]]; then
  printf 'demo-project\\n'
  exit 0
fi
if [[ "$*" == "builds submit"* ]]; then
  echo "unexpected build" >&2
  exit 45
fi
exit 0
""",
                encoding="utf-8",
            )
            gcloud_path.chmod(0o755)

            completed = subprocess.run(
                ["bash", str(build_script)],
                capture_output=True,
                text=True,
                check=False,
                env=self._script_env(
                    PATH=f"{fake_bin}:{os.environ['PATH']}",
                    PYTHON_BIN=sys.executable,
                    CLOUD_RUN_BUILD_IMAGE_DRY_RUN="1",
                    CLOUD_RUN_IMAGE_TAG="test-tag",
                ),
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertIn(
                "asia-southeast1-docker.pkg.dev/demo-project/team-portal/team-portal:test-tag",
                completed.stdout,
            )
            self.assertIn("Dry run only", completed.stdout)
            self.assertNotIn("unexpected build", completed.stderr)

    def test_gcloudignore_excludes_non_runtime_uploads_but_keeps_runtime_inputs(self):
        ignored = (PROJECT_ROOT / ".gcloudignore").read_text(encoding="utf-8").splitlines()

        for expected in ("docs/", "tests/", "evals/", ".team-portal/", ".team-portal-uat/", ".secrets/", "*.db"):
            self.assertIn(expected, ignored)
        for runtime_path in ("bpmis_jira_tool/", "config/", "static/", "templates/", "prd_briefing/"):
            self.assertNotIn(runtime_path, ignored)

    def test_gitignore_excludes_uat_data_root(self):
        ignored = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()

        self.assertIn(".team-portal-uat/", ignored)

    def test_cloud_run_dockerfile_copies_runtime_inputs_explicitly(self):
        dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")

        self.assertNotIn("COPY . .", dockerfile)
        for expected in (
            "COPY app.py local_agent.py ./",
            "COPY bpmis_jira_tool ./bpmis_jira_tool",
            "COPY config ./config",
            "COPY prd_briefing ./prd_briefing",
            "COPY static ./static",
            "COPY templates ./templates",
        ):
            self.assertIn(expected, dockerfile)

    def test_local_agent_foreground_prefers_gunicorn_single_worker_threads(self):
        script = (PROJECT_ROOT / "scripts/run_local_agent_foreground.sh").read_text(encoding="utf-8")

        self.assertIn("-m gunicorn", script)
        self.assertIn('--workers "${LOCAL_AGENT_WORKERS:-1}"', script)
        self.assertIn('--threads "${LOCAL_AGENT_THREADS:-8}"', script)
        self.assertIn("-m flask --app local_agent run", script)

    def test_local_agent_ngrok_tunnel_is_disabled_by_default(self):
        tunnel_script = PROJECT_ROOT / "scripts/run_local_agent_tunnel_foreground.sh"

        completed = subprocess.run(
            ["bash", str(tunnel_script)],
            capture_output=True,
            text=True,
            check=False,
            env=self._script_env(
                PYTHON_BIN=sys.executable,
                ENV_FILE=str(PROJECT_ROOT / ".env.example"),
            ),
            cwd=PROJECT_ROOT,
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("ngrok tunnel is sunset", completed.stdout)
        self.assertIn("ALLOW_LEGACY_NGROK_LOCAL_AGENT=1", completed.stdout)

    def test_local_agent_ngrok_tunnel_requires_explicit_rollback_flag(self):
        script = (PROJECT_ROOT / "scripts/run_local_agent_tunnel_foreground.sh").read_text(encoding="utf-8")

        self.assertIn("ALLOW_LEGACY_NGROK_LOCAL_AGENT", script)
        self.assertIn("Use the Cloudflare-backed team portal proxy instead", script)

    def test_runtime_docs_do_not_tell_operators_to_start_local_agent_ngrok(self):
        for relative_path in ("docs/gcp-cloud-run-local-agent.md", "docs/release-checklist.md", "docs/team-deployment.md"):
            contents = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")

            self.assertNotIn("./scripts/run_local_agent_tunnel.sh start", contents)
            self.assertNotIn("your-fixed-agent-domain.ngrok.app", contents)
            self.assertNotIn("breeze-lung-clunky.ngrok-free.dev", contents)

    def test_cloudflare_tunnel_foreground_defaults_to_named_http2_tunnel(self):
        foreground_script = PROJECT_ROOT / "scripts/run_cloudflare_tunnel_foreground.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            args_file = temp_path / "cloudflared.args"
            self._write_fake_cloudflared(fake_bin)

            completed = subprocess.run(
                ["bash", str(foreground_script)],
                capture_output=True,
                text=True,
                check=False,
                env=self._script_env(
                    PATH=f"{fake_bin}:{os.environ['PATH']}",
                    PYTHON_BIN=sys.executable,
                    ENV_FILE=str(temp_path / ".env"),
                    FAKE_CLOUDFLARED_ARGS_FILE=str(args_file),
                ),
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertEqual(args_file.read_text(encoding="utf-8").strip(), "tunnel --protocol http2 run bankpmtool-live")

    def test_cloudflare_tunnel_foreground_honors_protocol_name_and_token(self):
        foreground_script = PROJECT_ROOT / "scripts/run_cloudflare_tunnel_foreground.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            args_file = temp_path / "cloudflared.args"
            self._write_fake_cloudflared(fake_bin)

            env_file = temp_path / ".env"
            env_file.write_text(
                "TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME=custom-live\nTEAM_PORTAL_CLOUDFLARE_PROTOCOL=quic\n",
                encoding="utf-8",
            )

            completed = subprocess.run(
                ["bash", str(foreground_script)],
                capture_output=True,
                text=True,
                check=False,
                env=self._script_env(
                    PATH=f"{fake_bin}:{os.environ['PATH']}",
                    PYTHON_BIN=sys.executable,
                    ENV_FILE=str(env_file),
                    CLOUDFLARE_TUNNEL_TOKEN="secret-token",
                    FAKE_CLOUDFLARED_ARGS_FILE=str(args_file),
                ),
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertEqual(
                args_file.read_text(encoding="utf-8").strip(),
                "tunnel --protocol quic run --token secret-token",
            )

    def test_cloudflare_tunnel_status_uses_tunnel_info_and_pid_file(self):
        tunnel_script = PROJECT_ROOT / "scripts/run_cloudflare_tunnel.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            self._write_fake_cloudflared(fake_bin)
            self._write_fake_pgrep(fake_bin)

            data_dir = temp_path / "team-data"
            args_file = temp_path / "cloudflared.args"
            env_file = temp_path / ".env"
            env_file.write_text(
                f"TEAM_PORTAL_DATA_DIR={data_dir}\nTEAM_PORTAL_BASE_URL=https://app.bankpmtool.uk\n",
                encoding="utf-8",
            )

            completed = subprocess.run(
                ["bash", str(tunnel_script), "status"],
                capture_output=True,
                text=True,
                check=False,
                env=self._script_env(
                    PATH=f"{fake_bin}:{os.environ['PATH']}",
                    PYTHON_BIN=sys.executable,
                    ENV_FILE=str(env_file),
                    FAKE_CLOUDFLARED_PID="4242",
                    FAKE_CLOUDFLARED_ARGS_FILE=str(args_file),
                ),
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertIn("Cloudflare Tunnel running (pid 4242).", completed.stdout)
            self.assertEqual((data_dir / "run/cloudflare_tunnel.pid").read_text(encoding="utf-8").strip(), "4242")

    def test_cloud_build_uses_latest_image_as_layer_cache(self):
        config = (PROJECT_ROOT / "cloudbuild.yaml").read_text(encoding="utf-8")

        self.assertIn("docker pull", config)
        self.assertIn("--cache-from", config)
        self.assertIn("${_IMAGE_NAME}:latest", config)
        self.assertIn("DOCKER_BUILDKIT=1", config)
        self.assertIn("BUILDKIT_INLINE_CACHE=1", config)
        self.assertIn("${_IMAGE_NAME}:buildcache", config)
        self.assertIn("CLOUD_LOGGING_ONLY", config)

    def test_team_env_helper_reads_multiple_values(self):
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "TEAM_PORTAL_DATA_DIR=custom-data",
                        "TEAM_PORTAL_PORT=5123",
                        "TEAM_PORTAL_BASE_URL=https://example.ngrok.dev",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            command = f'''
source "{helper_path}"
read_env_values TEAM_PORTAL_DATA_DIR TEAM_PORTAL_PORT TEAM_PORTAL_BASE_URL
'''
            completed = subprocess.run(
                ["bash", "-lc", command],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "ROOT_DIR": str(PROJECT_ROOT),
                    "ENV_FILE": str(env_file),
                    "PYTHON_BIN": sys.executable,
                },
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertEqual(
                completed.stdout.splitlines(),
                ["custom-data", "5123", "https://example.ngrok.dev"],
            )

    def test_team_env_export_preserves_explicit_environment_overrides(self):
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / ".env"
            env_file.write_text("LOCAL_AGENT_PORT=7007\nTEAM_PORTAL_DATA_DIR=.team-portal\n", encoding="utf-8")
            command = f'''
source "{helper_path}"
export_env_file
printf '%s\\n' "$LOCAL_AGENT_PORT"
'''
            completed = subprocess.run(
                ["bash", "-lc", command],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "ROOT_DIR": str(PROJECT_ROOT),
                    "ENV_FILE": str(env_file),
                    "PYTHON_BIN": sys.executable,
                    "LOCAL_AGENT_PORT": "7008",
                },
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertEqual(completed.stdout.strip(), "7008")

    def test_team_env_helper_normalizes_relative_data_dir(self):
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        command = f'''
source "{helper_path}"
resolve_team_data_dir "relative-dir"
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "ROOT_DIR": str(PROJECT_ROOT),
                "PYTHON_BIN": sys.executable,
            },
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertEqual(completed.stdout.strip(), str(PROJECT_ROOT / "relative-dir"))

    def test_team_env_helper_reports_current_release_revision(self):
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        command = f'''
source "{helper_path}"
current_release_revision
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "ROOT_DIR": str(PROJECT_ROOT),
                "PYTHON_BIN": sys.executable,
            },
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertRegex(completed.stdout.strip(), r"^(?:[0-9a-f]{40}(?:-dirty-[0-9a-f]{12})?|unknown)$")

    def test_team_env_helper_reports_recommended_host_root(self):
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        command = f'''
source "{helper_path}"
recommended_team_stack_root
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "ROOT_DIR": str(PROJECT_ROOT),
                "PYTHON_BIN": sys.executable,
            },
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertTrue(completed.stdout.strip().endswith("/Workspace/jira-creation-stack-host"))

    def test_team_env_helper_detects_protected_documents_path(self):
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        command = f'''
source "{helper_path}"
if is_protected_mac_path "$HOME/Documents/demo"; then
  echo "yes"
else
  echo "no"
fi
'''
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                "ROOT_DIR": str(PROJECT_ROOT),
                "PYTHON_BIN": sys.executable,
            },
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertEqual(completed.stdout.strip(), "yes")

    def test_team_stack_guard_supports_cloudflare_tunnel_provider(self):
        guard_script = (PROJECT_ROOT / "scripts/run_team_stack_guard.sh").read_text(encoding="utf-8")

        self.assertIn('TUNNEL_PROVIDER="${TEAM_PORTAL_TUNNEL_PROVIDER:-$(read_env_value TEAM_PORTAL_TUNNEL_PROVIDER)}"', guard_script)
        self.assertIn('TUNNEL_LABEL="Cloudflare Tunnel"', guard_script)
        self.assertIn('TUNNEL_FOREGROUND_SCRIPT="$CLOUDFLARE_FOREGROUND_SCRIPT"', guard_script)
        self.assertIn('"tunnel_provider":"$(json_escape "$TUNNEL_PROVIDER")"', guard_script)
        self.assertIn('"tunnel_health":"$(json_escape "$tunnel_health")"', guard_script)
        self.assertNotIn("SOURCE_CODE_QA" + "_NIGHTLY_EVAL", guard_script)

    def test_stack_doctor_omits_source_code_qa_scheduled_eval(self):
        stack_script = (PROJECT_ROOT / "scripts/run_team_stack.sh").read_text(encoding="utf-8")

        self.assertNotIn("== Source Code QA Eval", stack_script)
        self.assertNotIn("source_code_qa" + "_eval_status.json", stack_script)

    def test_release_status_script_separates_cloud_run_and_mac_live(self):
        from scripts.release_status import build_status_lines

        with tempfile.TemporaryDirectory() as temp_dir:
            gcloud_path = Path(temp_dir) / "gcloud"
            gcloud_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

            def fake_run(command, *, env):
                joined = " ".join(command)
                if command[0] == "git":
                    return subprocess.CompletedProcess(command, 0, stdout="d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc\n", stderr="")
                if command[0] == str(gcloud_path) and "run services describe" in joined:
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps(
                            {
                                "status": {
                                    "traffic": [
                                        {
                                            "tag": "uat",
                                            "revisionName": "team-portal-00301-viv",
                                            "url": "https://uat---team-portal-ekaykywtvq-as.a.run.app",
                                        },
                                        {"revisionName": "team-portal-00200-n7q", "percent": 100},
                                    ]
                                }
                            }
                        ),
                        stderr="",
                    )
                if command[0] == str(gcloud_path) and "run revisions describe team-portal-00301-viv" in joined:
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps(
                            {
                                "spec": {
                                    "containers": [
                                        {
                                            "env": [
                                                {
                                                    "name": "TEAM_PORTAL_RELEASE_REVISION",
                                                    "value": "d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc",
                                                }
                                            ]
                                        }
                                    ]
                                }
                            }
                        ),
                        stderr="",
                    )
                if command[0] == str(gcloud_path) and "run revisions describe team-portal-00200-n7q" in joined:
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps(
                            {
                                "spec": {
                                    "containers": [
                                        {
                                            "env": [
                                                {
                                                    "name": "TEAM_PORTAL_RELEASE_REVISION",
                                                    "value": "1e19bfd647b0a60a1284aaaad8d2411cf17bca77",
                                                }
                                            ]
                                        }
                                    ]
                                }
                            }
                        ),
                        stderr="",
                    )
                if command[0] == "curl":
                    url = command[-1]
                    if url.endswith("/api/local-agent/healthz") or url.endswith(":7007/healthz"):
                        payload = {"status": "ok", "capabilities": {"source_code_qa": True, "codex_ready": True}}
                    else:
                        payload = {"status": "ok", "revision": "d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc"}
                    return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="unexpected command")

            lines = build_status_lines(
                env={
                    "GCLOUD_BIN": str(gcloud_path),
                    "GOOGLE_CLOUD_PROJECT": "civil-partition-492805-v7",
                    "CLOUD_RUN_SERVICE": "team-portal",
                    "CLOUD_RUN_REGION": "asia-southeast1",
                    "CLOUD_RUN_UAT_TAG": "uat",
                    "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                    "TEAM_PORTAL_PORT": "5000",
                    "LOCAL_AGENT_BASE_URL": "http://127.0.0.1:7007",
                },
                runner=fake_run,
            )

        output = "\n".join(lines)
        self.assertIn("Cloud Run UAT tag: tag=uat revision=team-portal-00301-viv", output)
        self.assertIn("git_revision=d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc", output)
        self.assertIn("Cloud Run service live traffic: revision=team-portal-00200-n7q percent=100", output)
        self.assertIn("(Cloud Run traffic, not Mac public Live)", output)
        self.assertIn("Public Live URL (Mac/Cloudflare): url=https://app.bankpmtool.uk/healthz status=ok revision=d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc", output)
        self.assertIn("Local portal: url=http://127.0.0.1:5000/healthz status=ok revision=d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc", output)
        self.assertIn("Direct local-agent: url=http://127.0.0.1:7007/healthz status=ok source_code_qa=True codex_ready=True", output)
        self.assertIn("Public local-agent proxy: url=https://app.bankpmtool.uk/api/local-agent/healthz status=ok source_code_qa=True codex_ready=True", output)
        self.assertIn("Version Plan Firestore:", output)

    def test_stack_doctor_exposes_release_status_command(self):
        stack_script = (PROJECT_ROOT / "scripts/run_team_stack.sh").read_text(encoding="utf-8")

        self.assertIn("release-status", stack_script)
        self.assertIn("release_status()", stack_script)
        self.assertIn('"$PYTHON_BIN" "$ROOT_DIR/scripts/release_status.py"', stack_script)

    def test_stack_doctor_runs_portal_runtime_doctor(self):
        stack_script = (PROJECT_ROOT / "scripts/run_team_stack.sh").read_text(encoding="utf-8")

        self.assertIn("== Portal Runtime Doctor ==", stack_script)
        self.assertIn("portal_runtime_doctor.py", stack_script)
        self.assertIn('"$PYTHON_BIN" "$ROOT_DIR/scripts/portal_runtime_doctor.py" --strict', stack_script)

    def test_stack_doctor_reports_cloudflare_tunnel_provider(self):
        stack_script = PROJECT_ROOT / "scripts/run_team_stack.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            self._write_fake_curl(fake_bin)
            self._write_fake_pgrep(fake_bin)

            data_dir = temp_path / "team-data"
            run_dir = data_dir / "run"
            run_dir.mkdir(parents=True)
            status_file = run_dir / "team_stack_status.json"
            status_file.write_text(
                """
{"state":"running","updated_at":"2026-05-06 21:00:00","updated_unix":4102444800,"guard_pid":123,"portal_child_pid":456,"tunnel_child_pid":789,"ngrok_child_pid":null,"caffeinate_pid":321,"portal_health":"healthy","tunnel_health":"unhealthy","ngrok_health":"unhealthy","tunnel_provider":"cloudflare","alert_state":"none","public_url":"https://app.bankpmtool.uk","probe_url":"http://127.0.0.1:5000/healthz"}
""".strip()
                + "\n",
                encoding="utf-8",
            )

            completed = subprocess.run(
                ["bash", str(stack_script), "doctor"],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_HEALTHZ_REVISION": self._current_release_revision(),
                    "FAKE_CLOUDFLARED_PID": "789",
                    "TEAM_PORTAL_DATA_DIR": str(data_dir),
                    "TEAM_PORTAL_PORT": "5000",
                    "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                    "TEAM_PORTAL_TUNNEL_PROVIDER": "cloudflare",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertNotEqual(completed.returncode, 0, msg=completed.stdout)
            self.assertIn("Tunnel provider: cloudflare", completed.stdout)
            self.assertIn("Cloudflare Tunnel process running", completed.stdout)
            self.assertIn("public URL reachable", completed.stdout)
            self.assertIn(
                "status summary is stale: tunnel probe is healthy but file says unhealthy",
                completed.stdout,
            )

    def test_stack_doctor_fails_cloudflare_public_error_status(self):
        stack_script = PROJECT_ROOT / "scripts/run_team_stack.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            self._write_fake_curl(fake_bin)
            self._write_fake_pgrep(fake_bin)

            data_dir = temp_path / "team-data"
            (data_dir / "run").mkdir(parents=True)

            completed = subprocess.run(
                ["bash", str(stack_script), "doctor"],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_HEALTHZ_REVISION": self._current_release_revision(),
                    "FAKE_CLOUDFLARED_PID": "789",
                    "FAKE_PUBLIC_HEALTH_FAIL": "1",
                    "TEAM_PORTAL_DATA_DIR": str(data_dir),
                    "TEAM_PORTAL_PORT": "5000",
                    "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                    "TEAM_PORTAL_TUNNEL_PROVIDER": "cloudflare",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertNotEqual(completed.returncode, 0, msg=completed.stdout)
            self.assertIn("Cloudflare Tunnel process running", completed.stdout)
            self.assertIn("public URL check failed", completed.stdout)

    def test_doctor_reports_stale_status_summary_when_live_probes_disagree(self):
        stack_script = PROJECT_ROOT / "scripts/run_team_stack.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            self._write_fake_curl(fake_bin)

            data_dir = temp_path / "team-data"
            run_dir = data_dir / "run"
            run_dir.mkdir(parents=True)
            status_file = run_dir / "team_stack_status.json"
            status_file.write_text(
                """
{"state":"running","updated_at":"2026-04-19 13:30:00","updated_unix":4102444800,"guard_pid":123,"portal_child_pid":456,"ngrok_child_pid":789,"caffeinate_pid":321,"portal_health":"unhealthy","ngrok_health":"unhealthy","alert_state":"none","probe_url":"http://127.0.0.1:5000/healthz"}
""".strip()
                + "\n",
                encoding="utf-8",
            )

            completed = subprocess.run(
                ["bash", str(stack_script), "doctor"],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_HEALTHZ_REVISION": self._current_release_revision(),
                    "TEAM_PORTAL_DATA_DIR": str(data_dir),
                    "TEAM_PORTAL_PORT": "5000",
                    "TEAM_PORTAL_BASE_URL": "https://example.ngrok.dev",
                    "TEAM_PORTAL_TUNNEL_PROVIDER": "ngrok",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertNotEqual(completed.returncode, 0, msg=completed.stdout)
            self.assertIn(
                "status summary is stale: file says running but guard is not running",
                completed.stdout,
            )
            self.assertIn(
                "status summary is stale: portal probe is healthy but file says unhealthy",
                completed.stdout,
            )
            self.assertIn(
                "status summary is stale: tunnel probe is healthy but file says unhealthy",
                completed.stdout,
            )
            self.assertIn(
                "status summary is incomplete: public_url missing while public probe passed",
                completed.stdout,
            )

    def test_doctor_reports_stale_stopped_summary_when_live_probes_respond(self):
        stack_script = PROJECT_ROOT / "scripts/run_team_stack.sh"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir()
            self._write_fake_curl(fake_bin)

            data_dir = temp_path / "team-data"
            run_dir = data_dir / "run"
            run_dir.mkdir(parents=True)
            status_file = run_dir / "team_stack_status.json"
            status_file.write_text(
                """
{"state":"stopped","updated_at":"2026-04-19 13:35:00","updated_unix":4102444800,"guard_pid":null,"portal_child_pid":null,"ngrok_child_pid":null,"caffeinate_pid":null,"portal_health":"unknown","ngrok_health":"unknown","alert_state":"none","public_url":"https://example.ngrok.dev","probe_url":"http://127.0.0.1:5000/healthz"}
""".strip()
                + "\n",
                encoding="utf-8",
            )

            completed = subprocess.run(
                ["bash", str(stack_script), "doctor"],
                capture_output=True,
                text=True,
                check=False,
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "FAKE_HEALTHZ_REVISION": self._current_release_revision(),
                    "TEAM_PORTAL_DATA_DIR": str(data_dir),
                    "TEAM_PORTAL_PORT": "5000",
                    "TEAM_PORTAL_BASE_URL": "https://example.ngrok.dev",
                    "TEAM_PORTAL_TUNNEL_PROVIDER": "ngrok",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertNotEqual(completed.returncode, 0, msg=completed.stdout)
            self.assertIn(
                "status summary is stale: file says stopped but live probes still respond",
                completed.stdout,
            )


if __name__ == "__main__":
    unittest.main()
