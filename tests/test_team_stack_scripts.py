import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TeamStackScriptTests(unittest.TestCase):
    def _write_fake_curl(self, bin_dir: Path) -> Path:
        curl_path = bin_dir / "curl"
        curl_path.write_text(
            """#!/usr/bin/env bash
set -euo pipefail

url="${@: -1}"
case "$url" in
  http://127.0.0.1:5000/healthz)
    printf '{"status":"ok"}\\n'
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
