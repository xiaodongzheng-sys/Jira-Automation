import json
import hashlib
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
                "CLOUD_RUN_DEPLOY_ACCOUNT": "deploy@example.iam.gserviceaccount.com",
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
    printf '{"status":"ok","revision":"%s"}\\n' "${FAKE_HEALTHZ_REVISION:-unknown}"
    ;;
  https://app.bankpmtool.uk/healthz)
    if [[ "${FAKE_PUBLIC_HEALTH_FAIL:-0}" == "1" ]]; then
      exit 22
    fi
    printf '{"status":"ok","revision":"%s"}\\n' "${FAKE_HEALTHZ_REVISION:-unknown}"
    ;;
  http://127.0.0.1:7007/healthz|https://example.ngrok.dev/api/local-agent/healthz|https://app.bankpmtool.uk/api/local-agent/healthz)
    printf '{"status":"ok","capabilities":{"source_code_qa":true,"codex_ready":true}}\\n'
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

    def test_project_python_launcher_uses_repo_root_pythonpath(self):
        launcher = PROJECT_ROOT / "scripts/project_python.sh"
        completed = subprocess.run(
            [
                "bash",
                str(launcher),
                "-c",
                "import os, sys; assert sys.path[0] == ''; assert os.environ['PYTHONPATH'].split(os.pathsep)[0] == os.getcwd(); import bpmis_jira_tool",
            ],
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "PYTHON_BIN": sys.executable},
            cwd="/tmp",
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)

    def test_team_env_exports_repo_root_pythonpath(self):
        completed = self._run_team_env_helper(
            'printf "%s\\n" "$PYTHONPATH"',
            PYTHONPATH="/tmp/existing",
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertEqual(completed.stdout.strip().split(os.pathsep)[:2], [str(PROJECT_ROOT), "/tmp/existing"])

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

    def test_local_agent_launcher_uses_agent_data_root_for_pid_and_logs(self):
        script = (PROJECT_ROOT / "scripts/run_local_agent.sh").read_text(encoding="utf-8")

        self.assertIn("LOCAL_AGENT_TEAM_PORTAL_DATA_DIR", script)
        self.assertIn('DATA_DIR="$(resolve_team_data_dir "$AGENT_DATA_DIR")"', script)

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
                    "GOOGLE_CLOUD_PROJECT": "demo-project",
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
            self.assertIn("VERSION_PLAN_FIRESTORE_PROJECT=demo-project", deploy_calls[0])
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

    def test_generic_restart_guard_blocks_portal_restart_when_recording(self):
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
                        "title": "Portal Guard Review",
                        "status": "recording",
                        "recording_started_at": "2026-05-21T08:03:51+00:00",
                        "media": {
                            "screencapture_status_path": str(status_path),
                        },
                    }
                ),
                encoding="utf-8",
            )

            completed = self._run_team_env_helper(
                f'assert_no_active_meeting_recording_before_restart "restart team portal" "{data_root}"',
            )

        self.assertEqual(completed.returncode, 1, msg=completed.stdout + completed.stderr)
        self.assertIn("Refusing to restart team portal", completed.stdout)
        self.assertIn("portal, team stack, launchd, or Mac local-agent", completed.stdout)
        self.assertIn("meeting-active", completed.stdout)

    def test_portal_restart_scripts_use_active_recording_guard(self):
        prod_script = (PROJECT_ROOT / "scripts/run_team_portal_prod.sh").read_text(encoding="utf-8")
        dev_script = (PROJECT_ROOT / "scripts/run_team_portal.sh").read_text(encoding="utf-8")
        server_script = (PROJECT_ROOT / "scripts/run_server.sh").read_text(encoding="utf-8")

        self.assertIn('assert_no_active_meeting_recording_before_restart "stop team portal" "$DATA_DIR"', prod_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "restart team portal" "$DATA_DIR"', prod_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "replace stale team portal process on port $PORT" "$DATA_DIR"', prod_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "stop team portal" "$DATA_DIR"', dev_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "restart team portal" "$DATA_DIR"', dev_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "kickstart Flask portal launchd job" "$DATA_DIR"', server_script)

    def test_team_stack_and_launchd_restart_paths_use_active_recording_guard(self):
        stack_script = (PROJECT_ROOT / "scripts/run_team_stack.sh").read_text(encoding="utf-8")
        stack_launchd_script = (PROJECT_ROOT / "scripts/install_team_stack_launchd.sh").read_text(encoding="utf-8")
        portal_launchd_script = (PROJECT_ROOT / "scripts/install_team_portal_launchd.sh").read_text(encoding="utf-8")
        setup_script = (PROJECT_ROOT / "scripts/setup_team_stack_host_workspace.sh").read_text(encoding="utf-8")

        self.assertIn('assert_no_active_meeting_recording_before_restart "stop team stack"', stack_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "restart team stack guard"', stack_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "restart team portal"', stack_script)
        self.assertIn('launchctl kickstart -k "$domain_label"', stack_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "reload team stack launchd job" "$DATA_DIR"', stack_launchd_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "reload team portal launchd job" "$DATA_DIR"', portal_launchd_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "kickstart team stack launchd job"', setup_script)
        reminder_launchd_script = (PROJECT_ROOT / "scripts/install_meeting_recorder_reminder_launchd.sh").read_text(encoding="utf-8")
        self.assertIn(
            'assert_no_active_meeting_recording_before_restart "reload Meeting Recorder reminder launchd job" "$DATA_DIR"',
            reminder_launchd_script,
        )
        self.assertIn("deploy/launchd/meeting-recorder-reminder.plist.template", reminder_launchd_script)
        reminder_runner_script = (PROJECT_ROOT / "scripts/run_meeting_recorder_reminder.sh").read_text(encoding="utf-8")
        self.assertIn("bpmis_jira_tool.meeting_recorder_reminder", reminder_runner_script)

    def test_team_stack_guard_does_not_restart_or_cleanup_portal_while_recording(self):
        guard_script = (PROJECT_ROOT / "scripts/run_team_stack_guard.sh").read_text(encoding="utf-8")
        guard_daemon_script = (PROJECT_ROOT / "scripts/run_team_stack_guard_daemon.sh").read_text(encoding="utf-8")

        self.assertIn("restart_blocked_by_active_recording()", guard_script)
        self.assertIn('restart_blocked_by_active_recording "team stack guard cleanup stop"', guard_script)
        self.assertIn('restart_blocked_by_active_recording "portal launch"', guard_script)
        self.assertIn('restart_blocked_by_active_recording "portal restart after failed health checks"', guard_script)
        self.assertIn("Leaving portal and tunnel processes running during guard shutdown", guard_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "stop team stack guard" "$DATA_DIR"', guard_daemon_script)
        self.assertIn('assert_no_active_meeting_recording_before_restart "restart team stack guard" "$DATA_DIR"', guard_daemon_script)

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
                    "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
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
            self.assertIn("TEAM_PORTAL_BASE_URL=https://app.bankpmtool.uk", deploy_calls[0])
            self.assertIn("GOOGLE_CLOUD_OAUTH_REDIRECT_URI=https://app.bankpmtool.uk/cloud-auth/google/callback", deploy_calls[0])
            self.assertIn("TEAM_PORTAL_MAC_FULL_PORTAL_URL=https://app.bankpmtool.uk/portal-home", deploy_calls[0])
            self.assertIn("VERSION_PLAN_FIRESTORE_PROJECT=demo-project", deploy_calls[0])
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

    def test_team_env_write_release_manifest_records_clean_restart_inputs(self):
        helper_path = PROJECT_ROOT / "scripts/lib/team_env.sh"
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            command = f'''
source "{helper_path}"
manifest_id="$(write_release_manifest "{data_root}" "mac_public_live")"
manifest_path="$(release_manifest_path "{data_root}")"
printf '%s\\n%s\\n' "$manifest_id" "$manifest_path"
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
            manifest_id, manifest_path_text = completed.stdout.strip().splitlines()
            manifest_path = Path(manifest_path_text)
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(manifest_id, payload["manifest_id"])
        self.assertEqual(payload["surface"], "mac_public_live")
        self.assertEqual(payload["project_root"], str(PROJECT_ROOT))
        self.assertIn("release_revision", payload)

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

    def test_release_status_treats_cloud_run_standby_mismatch_as_info(self):
        from scripts.release_status import build_status_report

        with tempfile.TemporaryDirectory() as temp_dir:
            gcloud_path = Path(temp_dir) / "gcloud"
            gcloud_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

            def fake_run(command, *, env):
                joined = " ".join(command)
                if command[0] == "git":
                    return subprocess.CompletedProcess(command, 0, stdout="expected-sha\n", stderr="")
                if command[0] == str(gcloud_path) and "run services describe" in joined:
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps({"status": {"traffic": [{"revisionName": "team-portal-live", "percent": 100}]}}),
                        stderr="",
                    )
                if command[0] == str(gcloud_path) and "run revisions describe" in joined:
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps({"spec": {"containers": [{"env": [{"name": "TEAM_PORTAL_RELEASE_REVISION", "value": "old-sha"}]}]}}),
                        stderr="",
                    )
                if command[0] == "curl":
                    return subprocess.CompletedProcess(command, 0, stdout=json.dumps({"status": "ok", "revision": "expected-sha"}), stderr="")
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="unexpected command")

            report = build_status_report(
                env={
                    "GCLOUD_BIN": str(gcloud_path),
                    "TEAM_PORTAL_CLOUD_RUN_ROLE": "standby",
                    "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                    "TEAM_PORTAL_RELEASE_MANIFEST_PATH": "/tmp/missing-manifest.json",
                    "FLASK_SECRET_KEY": "shared-secret",
                },
                runner=fake_run,
            )

        output = "\n".join(report["lines"])
        self.assertIn("Cloud Run standby info: cloud_run_live_revision_mismatch", output)
        issue_codes = {issue["code"] for issue in report["issues"]}
        self.assertNotIn("cloud_run_live_revision_mismatch", issue_codes)

    def test_release_manifest_status_validates_expected_revision(self):
        from scripts.release_status import _release_manifest_status

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "manifest_id": "manifest-1",
                        "surface": "mac_public_live",
                        "release_revision": "expected-sha",
                        "python_version": "3.12.13",
                    }
                ),
                encoding="utf-8",
            )

            ok = _release_manifest_status(
                "expected-sha",
                env={"TEAM_PORTAL_RELEASE_MANIFEST_PATH": str(manifest_path)},
            )
            mismatch = _release_manifest_status(
                "other-sha",
                env={"TEAM_PORTAL_RELEASE_MANIFEST_PATH": str(manifest_path)},
            )

        self.assertIn("status=ok", ok)
        self.assertIn("manifest_id=manifest-1", ok)
        self.assertIn("surface=mac_public_live", ok)
        self.assertIn("status=fail", mismatch)
        self.assertIn("revision_mismatch", mismatch)

    def test_release_probes_format_health_and_cloud_run_role(self):
        from scripts.release_probes import cloud_run_mismatch_message, cloud_run_role, health_probe, manifest_path

        def fake_run(command, *, env):
            self.assertEqual(command[0], "curl")
            self.assertEqual(env["TOKEN"], "present")
            payload = {
                "status": "ok",
                "revision": "rev-1",
                "release_manifest_id": "manifest-1",
                "live_surface": "mac_public_live",
                "capabilities": {"source_code_qa": True, "codex_ready": True},
            }
            return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")

        details = health_probe("https://app.bankpmtool.uk/healthz", env={"TOKEN": "present"}, runner=fake_run)

        self.assertIn("status=ok", details)
        self.assertIn("revision=rev-1", details)
        self.assertIn("release_manifest_id=manifest-1", details)
        self.assertIn("live_surface=mac_public_live", details)
        self.assertIn("source_code_qa=True", details)
        self.assertEqual(cloud_run_role(""), "standby")
        self.assertIn(
            "Mac public Live is authoritative",
            cloud_run_mismatch_message("standby", "revision mismatch."),
        )
        self.assertEqual(
            manifest_path("/tmp/team-portal").as_posix(),
            "/tmp/team-portal/run/team_portal_release_manifest.json",
        )

    def test_release_status_report_flags_mac_live_revision_mismatch_as_failure(self):
        from scripts.release_status import build_status_report

        with tempfile.TemporaryDirectory() as temp_dir:
            gcloud_path = Path(temp_dir) / "gcloud"
            gcloud_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

            def fake_run(command, *, env):
                joined = " ".join(command)
                if command[0] == "git":
                    return subprocess.CompletedProcess(command, 0, stdout="expected-sha\n", stderr="")
                if command[0] == str(gcloud_path) and "run services describe" in joined:
                    return subprocess.CompletedProcess(command, 0, stdout=json.dumps({"status": {"traffic": []}}), stderr="")
                if command[0] == "curl":
                    url = command[-1]
                    if url.endswith("/api/local-agent/healthz") or url.endswith(":7007/healthz"):
                        payload = {"status": "ok", "capabilities": {"source_code_qa": True, "codex_ready": True}}
                    else:
                        payload = {"status": "ok", "revision": "served-sha"}
                    return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="unexpected command")

            report = build_status_report(
                env={
                    "GCLOUD_BIN": str(gcloud_path),
                    "GOOGLE_CLOUD_PROJECT": "risk-pm-tool",
                    "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                    "TEAM_PORTAL_PORT": "5000",
                    "LOCAL_AGENT_BASE_URL": "http://127.0.0.1:7007",
                    "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                    "FLASK_SECRET_KEY": "shared-secret",
                },
                runner=fake_run,
            )

        self.assertEqual(report["status"], "fail")
        issue_codes = {issue["code"] for issue in report["issues"]}
        self.assertIn("local_portal_revision_mismatch", issue_codes)
        self.assertIn("public_live_revision_mismatch", issue_codes)
        self.assertIn("Readiness: status=fail", "\n".join(report["lines"]))

    def test_release_status_flags_default_local_secret_for_cloud_home(self):
        from scripts.release_status import _shared_session_status

        status = _shared_session_status(
            env={
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "FLASK_SECRET_KEY": "local-dev-secret-change-me",
            }
        )

        self.assertIn("status=fail", status)
        self.assertIn("reason=local_flask_secret_default", status)

    def test_release_status_expected_revision_includes_dirty_fingerprint(self):
        from scripts.release_status import _expected_source_revision

        def fake_run(command, *, env):
            joined = " ".join(command)
            if "rev-parse HEAD" in joined:
                return subprocess.CompletedProcess(command, 0, stdout="abc123\n", stderr="")
            if "diff --no-ext-diff --full-index --binary HEAD -- ." in joined:
                return subprocess.CompletedProcess(command, 0, stdout="diff --git a/app.py b/app.py\n", stderr="")
            if "ls-files --others --exclude-standard" in joined:
                return subprocess.CompletedProcess(command, 0, stdout="new_file.py\n", stderr="")
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="unexpected command")

        revision, error = _expected_source_revision(env={}, runner=fake_run)
        expected_material = "diff --git a/app.py b/app.py\n\n--UNTRACKED--\nnew_file.py\n"
        expected_hash = hashlib.sha1(expected_material.encode("utf-8")).hexdigest()[:12]

        self.assertEqual(error, "")
        self.assertEqual(revision, f"abc123-dirty-{expected_hash}")

    def test_release_status_expected_revision_ignores_local_runtime_untracked_paths(self):
        from scripts.release_status import _expected_source_revision

        def fake_run(command, *, env):
            joined = " ".join(command)
            if "rev-parse HEAD" in joined:
                return subprocess.CompletedProcess(command, 0, stdout="abc123\n", stderr="")
            if "diff --no-ext-diff --full-index --binary HEAD -- ." in joined:
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            if "ls-files --others --exclude-standard" in joined:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout=".venv.backup-20260524114830/bin/python\n.team-portal/run/status.json\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="unexpected command")

        revision, error = _expected_source_revision(env={}, runner=fake_run)

        self.assertEqual(error, "")
        self.assertEqual(revision, "abc123")

    def test_release_status_firestore_token_uses_deploy_service_account(self):
        from scripts.release_status import _gcloud_firestore_access_token

        with tempfile.TemporaryDirectory() as temp_dir:
            gcloud_path = Path(temp_dir) / "gcloud"
            gcloud_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
            captured: list[list[str]] = []

            def fake_run(command, *, env):
                captured.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="service-account-token\n", stderr="")

            token = _gcloud_firestore_access_token(
                env={
                    "GCLOUD_BIN": str(gcloud_path),
                    "CLOUD_RUN_DEPLOY_ACCOUNT": "deploy@example.iam.gserviceaccount.com",
                    "VERSION_PLAN_FIRESTORE_PROJECT": "risk-pm-tool",
                },
                runner=fake_run,
            )

        self.assertEqual(token, "service-account-token")
        self.assertEqual(captured[0][1:3], ["auth", "print-access-token"])
        self.assertIn("--account", captured[0])
        self.assertIn("deploy@example.iam.gserviceaccount.com", captured[0])
        self.assertIn("--project", captured[0])
        self.assertIn("risk-pm-tool", captured[0])

    def test_release_status_firestore_prefers_service_account_rest(self):
        from scripts.release_status import _version_plan_firestore_status

        class FakeSnapshot:
            exists = True

            def to_dict(self):
                return {
                    "environment": "live",
                    "updated_at_sgt": "2026-05-24 09:00:00 SGT",
                    "source_hash": "abc123",
                }

        with patch(
            "google.cloud.firestore.Client",
            side_effect=AssertionError("SDK ADC should not be used when deploy service account is configured"),
        ), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store._FirestoreRestDocument.get",
            return_value=FakeSnapshot(),
        ), patch(
            "scripts.release_status._gcloud_firestore_access_token",
            return_value="service-account-token",
        ):
            status = _version_plan_firestore_status(
                env={
                    "VERSION_PLAN_STORE_BACKEND": "firestore",
                    "VERSION_PLAN_FIRESTORE_PROJECT": "risk-pm-tool",
                    "CLOUD_RUN_DEPLOY_ACCOUNT": "deploy@example.iam.gserviceaccount.com",
                }
            )

        self.assertIn("status=ok", status)
        self.assertIn("document=portal/version_plan_live", status)
        self.assertIn("source_hash=abc123", status)

    def test_release_status_firestore_service_account_failure_skips_adc(self):
        from scripts.release_status import _version_plan_firestore_status

        with patch(
            "google.cloud.firestore.Client",
            side_effect=AssertionError("SDK ADC should not be used when deploy service account is configured"),
        ), patch(
            "bpmis_jira_tool.team_dashboard_version_plan_store._FirestoreRestDocument.get",
            side_effect=RuntimeError("service account denied"),
        ), patch(
            "scripts.release_status._gcloud_firestore_access_token",
            return_value="service-account-token",
        ):
            status = _version_plan_firestore_status(
                env={
                    "VERSION_PLAN_STORE_BACKEND": "firestore",
                    "VERSION_PLAN_FIRESTORE_PROJECT": "risk-pm-tool",
                    "CLOUD_RUN_DEPLOY_ACCOUNT": "deploy@example.iam.gserviceaccount.com",
                }
            )

        self.assertIn("status=unavailable", status)
        self.assertIn("REST service-account: RuntimeError", status)
        self.assertNotIn("ADC", status)

    def test_portal_doctor_firestore_uses_release_status_loader(self):
        from scripts import portal_runtime_doctor

        with patch.dict(
            os.environ,
            {
                "VERSION_PLAN_STORE_BACKEND": "firestore",
                "VERSION_PLAN_FIRESTORE_PROJECT": "risk-pm-tool",
                "CLOUD_RUN_DEPLOY_ACCOUNT": "deploy@example.iam.gserviceaccount.com",
            },
        ), patch(
            "scripts.release_status._load_version_plan_firestore_payload",
            return_value=(
                {
                    "environment": "live",
                    "updated_at_sgt": "2026-05-24 09:00:00 SGT",
                    "source_hash": "abc123",
                },
                "",
            ),
        ) as loader:
            summary, issues = portal_runtime_doctor._version_plan_firestore_summary()

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["source_hash"], "abc123")
        self.assertEqual(issues, [])
        loader.assert_called_once()

    def test_stack_doctor_exposes_release_status_command(self):
        stack_script = (PROJECT_ROOT / "scripts/run_team_stack.sh").read_text(encoding="utf-8")

        self.assertIn("release-status", stack_script)
        self.assertIn("release_status()", stack_script)
        self.assertIn('"$PYTHON_BIN" "$ROOT_DIR/scripts/release_status.py"', stack_script)
        self.assertIn("release_status --strict", stack_script)

    def test_stack_doctor_runs_portal_runtime_doctor(self):
        stack_script = (PROJECT_ROOT / "scripts/run_team_stack.sh").read_text(encoding="utf-8")

        self.assertIn("== Portal Runtime Doctor ==", stack_script)
        self.assertIn("portal_runtime_doctor.py", stack_script)
        self.assertIn('"$PYTHON_BIN" "$ROOT_DIR/scripts/portal_runtime_doctor.py" --strict', stack_script)
        self.assertIn("Source Code QA data dir resolved via local-agent", stack_script)
        self.assertIn('LOCAL_AGENT_TEAM_PORTAL_DATA_DIR="$local_agent_data_dir"', stack_script)

    def test_sync_team_stack_host_script_guards_runtime_data_and_dirty_host(self):
        sync_script = (PROJECT_ROOT / "scripts/sync_team_stack_host.sh").read_text(encoding="utf-8")

        self.assertIn("recommended_team_stack_root", sync_script)
        self.assertIn("git -C \"$HOST_ROOT\" status --porcelain", sync_script)
        self.assertIn("--allow-dirty-host", sync_script)
        self.assertIn("--exclude '.team-portal/'", sync_script)
        self.assertIn("--exclude '.venv/'", sync_script)
        self.assertIn("--exclude '.venv.backup-*'", sync_script)
        self.assertIn("--exclude '.git/'", sync_script)
        self.assertIn("team_portal_release_manifest.json", sync_script)
        self.assertIn("bpmis_jira_tool.release_manifest", sync_script)
        self.assertIn("source_code_qa_ops_summary.py\" --strict", sync_script)
        self.assertIn("run_team_stack.sh\" restart", sync_script)

    def test_team_stack_guard_stops_caffeinate_on_battery_power(self):
        guard_script = (PROJECT_ROOT / "scripts/run_team_stack_guard.sh").read_text(encoding="utf-8")

        self.assertIn("TEAM_STACK_CAFFEINATE_ON_BATTERY", guard_script)
        self.assertIn('current_power_source)', guard_script)
        self.assertIn('"Battery Power"', guard_script)
        self.assertIn("stop_caffeinate", guard_script)
        self.assertIn("reconcile_caffeinate", guard_script)
        self.assertIn("Sleep prevention disabled on battery power.", guard_script)

    def test_host_python_upgrade_script_builds_python312_candidate(self):
        upgrade_script = (PROJECT_ROOT / "scripts/upgrade_host_python_runtime.sh").read_text(encoding="utf-8")
        setup_script = (PROJECT_ROOT / "scripts/setup_team_stack_host_workspace.sh").read_text(encoding="utf-8")
        foreground_script = (PROJECT_ROOT / "scripts/run_team_portal_foreground.sh").read_text(encoding="utf-8")

        self.assertIn("/opt/homebrew/opt/python@3.12/bin/python3.12", upgrade_script)
        self.assertIn("Python 3.12+ is required", upgrade_script)
        self.assertIn("OpenSSL-backed Python is required", upgrade_script)
        self.assertIn("--apply", upgrade_script)
        self.assertIn(".venv.backup-", upgrade_script)
        self.assertIn("/opt/homebrew/opt/python@3.12/bin/python3.12", setup_script)
        self.assertIn("TEAM_PORTAL_RELEASE_MANIFEST_ID", foreground_script)

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
                    "FLASK_SECRET_KEY": "shared-secret",
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
                    "FLASK_SECRET_KEY": "shared-secret",
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
                    "FLASK_SECRET_KEY": "shared-secret",
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
                    "FLASK_SECRET_KEY": "shared-secret",
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
