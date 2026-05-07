import os
import subprocess
import sys
import tempfile
import unittest
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
    )

    def _script_env(self, **overrides: str) -> dict:
        env = os.environ.copy()
        for key in self.SCRIPT_TEST_ENV_KEYS:
            env.pop(key, None)
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
            "scripts/setup_uat_local_agent.sh",
            "scripts/promote_uat_to_live.sh",
            "scripts/build_cloud_run_image.sh",
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
            self.assertIn("LOCAL_AGENT_BASE_URL=https://app.bankpmtool.uk/uat-local-agent", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_RELEASE_REVISION=", deploy_calls[0])
            self.assertIn("--update-secrets LOCAL_AGENT_HMAC_SECRET=local-agent-uat-hmac-secret:latest", deploy_calls[0])
            self.assertIn("TRELLO_API_KEY=trello-key", deploy_calls[0])
            self.assertIn("TRELLO_API_TOKEN=trello-token", deploy_calls[0])
            self.assertIn("TRELLO_BOARD_ID=trello-board", deploy_calls[0])
            self.assertIn("TRELLO_DAILY_LIST_NAME=Daily Summary Email", deploy_calls[0])
            self.assertIn("Cloud Run UAT revision: team-portal-00091-uat", completed.stdout)
            self.assertIn("keeps live traffic unchanged", completed.stdout)

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

    def test_cloud_run_uat_env_hmac_fallback_preclears_existing_secret_binding(self):
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
            self.assertEqual(len(deploy_calls), 2, msg=calls)
            self.assertIn("--remove-secrets LOCAL_AGENT_HMAC_SECRET", deploy_calls[0])
            self.assertIn("--update-env-vars LOCAL_AGENT_HMAC_SECRET=uat-only-secret", deploy_calls[0])
            self.assertIn("--set-secrets", deploy_calls[1])
            self.assertIn("--set-env-vars", deploy_calls[1])
            self.assertIn("LOCAL_AGENT_HMAC_SECRET=uat-only-secret", deploy_calls[1])

    def test_cloud_run_uat_deploy_syncs_isolated_mac_local_agent_by_default(self):
        deploy_script = PROJECT_ROOT / "scripts/deploy_cloud_run_uat.sh"
        contents = deploy_script.read_text(encoding="utf-8")

        self.assertIn("CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY:-1", contents)
        self.assertIn("CLOUD_RUN_UAT_HOST_WORKSPACE", contents)
        self.assertIn("recommended_uat_team_stack_root", contents)
        self.assertIn("git -C \"$host_workspace\" merge --ff-only \"$GIT_SHA\"", contents)
        self.assertIn("pip\" install -r \"$host_workspace/requirements.txt\"", contents)
        self.assertIn("BriefingStore(data_path / \"prd_briefing\")", contents)
        self.assertIn("LOCAL_AGENT_PORT=\"$UAT_LOCAL_AGENT_PORT\"", contents)
        self.assertIn("LOCAL_AGENT_TEAM_PORTAL_DATA_DIR=\"$UAT_LOCAL_AGENT_DATA_DIR\"", contents)
        self.assertIn("LOCAL_AGENT_SCREEN_SESSION=\"$UAT_LOCAL_AGENT_SCREEN_SESSION\"", contents)
        self.assertIn("./scripts/run_local_agent.sh restart", contents)
        self.assertIn("/uat-local-agent", contents)

    def test_release_checklist_documents_full_gate_and_read_only_smoke(self):
        checklist = (PROJECT_ROOT / "docs/release-checklist.md").read_text(encoding="utf-8")

        self.assertIn("System Full Test Gate", checklist)
        self.assertIn("./.venv/bin/python scripts/run_system_full_test_gate.py --skip-smoke", checklist)
        self.assertIn("--coverage-fail-under 100", checklist)
        self.assertIn("./.venv/bin/python -m coverage run -m unittest discover -s tests", checklist)
        self.assertIn("node --check static/gmail_seatalk_demo.js", checklist)
        self.assertIn("./.venv/bin/python scripts/run_source_code_qa_release_gate.py", checklist)
        self.assertIn("read-only", checklist)
        self.assertIn("--uat-url \"$UAT_URL\"", checklist)
        self.assertIn("--live-url \"$LIVE_URL\"", checklist)
        self.assertIn("--expected-revision \"$EXPECTED_REVISION\"", checklist)
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
        self.assertIn('"$ROOT_DIR/scripts/run_local_agent.sh" restart', contents)

    def test_mac_stack_restart_refreshes_local_agent_when_bpmis_proxy_is_enabled(self):
        stack_script = PROJECT_ROOT / "scripts/run_team_stack.sh"

        contents = stack_script.read_text(encoding="utf-8")

        self.assertIn("restart_local_agent_if_needed", contents)
        self.assertIn('read_env_value BPMIS_CALL_MODE', contents)
        self.assertIn('"$ROOT_DIR/scripts/run_local_agent.sh" restart', contents)

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

        for expected in ("docs/", "tests/", "evals/", ".team-portal/", ".secrets/", "*.db"):
            self.assertIn(expected, ignored)
        for runtime_path in ("bpmis_jira_tool/", "config/", "static/", "templates/", "prd_briefing/"):
            self.assertNotIn(runtime_path, ignored)

    def test_cloud_run_dockerfile_copies_runtime_inputs_explicitly(self):
        dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")

        self.assertNotIn("COPY . .", dockerfile)
        for expected in (
            "COPY app.py local_agent.py jira_web_config.json ./",
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
