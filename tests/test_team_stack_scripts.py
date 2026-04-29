import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TeamStackScriptTests(unittest.TestCase):
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
  *)
    exit 1
    ;;
esac
""",
            encoding="utf-8",
        )
        curl_path.chmod(0o755)
        return curl_path

    def test_stack_scripts_have_valid_bash_syntax(self):
        script_paths = [
            "scripts/lib/team_env.sh",
            "scripts/run_team_portal_prod.sh",
            "scripts/run_team_portal_foreground.sh",
            "scripts/run_ngrok_tunnel.sh",
            "scripts/run_ngrok_tunnel_foreground.sh",
            "scripts/run_team_stack_guard.sh",
            "scripts/run_team_stack_guard_daemon.sh",
            "scripts/run_team_stack.sh",
            "scripts/install_team_portal_launchd.sh",
            "scripts/install_ngrok_launchd.sh",
            "scripts/install_team_stack_launchd.sh",
            "scripts/deploy_cloud_run.sh",
            "scripts/deploy_cloud_run_full.sh",
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
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://agent.example.ngrok.app",
                    "CLOUD_RUN_RESTART_LOCAL_AGENT_AFTER_DEPLOY": "0",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_BASE_URL": "https://bpmis.example.test",
                },
                cwd=PROJECT_ROOT,
            )

            self.assertEqual(completed.returncode, 0, msg=completed.stderr)
            self.assertIn("Cloud Run service: team-portal", completed.stdout)
            self.assertIn("Cloud Run source hash:", completed.stdout)
            self.assertIn("Dry run only", completed.stdout)
            self.assertNotIn("unexpected deploy", completed.stderr)

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
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://agent.example.ngrok.app",
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
            self.assertIn("--source .", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_DATA_DIR=/workspace/team-portal-runtime", deploy_calls[0])
            self.assertNotIn("/tmp/team-portal", deploy_calls[0])
            self.assertNotIn("--image", deploy_calls[0])

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
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://agent.example.ngrok.app",
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
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://agent.example.ngrok.app",
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
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://agent.example.ngrok.app",
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
if [[ "$*" == "run services describe"* ]]; then
  printf 'https://team-portal-example.run.app\\n'
  exit 0
fi
if [[ "$*" == "projects describe"* ]]; then
  printf '123456789\\n'
  exit 0
fi
if [[ "$*" == "secrets describe"* ]]; then
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
if [[ "$*" == "run deploy"* ]]; then
  exit 0
fi
if [[ "$*" == "run services update"* ]]; then
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
                    "CLOUD_RUN_LOCAL_AGENT_BASE_URL": "https://agent.example.ngrok.app",
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
            deploy_calls = [line for line in calls.splitlines() if line.startswith("run deploy")]
            update_calls = [line for line in calls.splitlines() if line.startswith("run services update")]
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
                env={
                    **os.environ,
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "PYTHON_BIN": sys.executable,
                    "CLOUD_RUN_BUILD_IMAGE_DRY_RUN": "1",
                    "CLOUD_RUN_IMAGE_TAG": "test-tag",
                },
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
                "status summary is stale: ngrok probe is healthy but file says unhealthy",
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
