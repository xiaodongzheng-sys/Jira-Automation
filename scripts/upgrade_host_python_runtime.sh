#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

HOST_ROOT="${TEAM_STACK_HOST_ROOT:-$(recommended_team_stack_root)}"
TARGET_PYTHON="${TEAM_STACK_HOST_TARGET_PYTHON:-}"
APPLY=0
RESTART=0

usage() {
  cat <<'USAGE'
Usage: scripts/upgrade_host_python_runtime.sh [--host-root PATH] [--python PATH] [--apply] [--restart]

Build and optionally activate a Python 3.12+ virtual environment for the Mac
Team Portal host checkout. Without --apply, this only builds and verifies a
candidate venv.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host-root)
      HOST_ROOT="${2:?--host-root requires a path}"
      shift 2
      ;;
    --python)
      TARGET_PYTHON="${2:?--python requires a path}"
      shift 2
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    --restart)
      APPLY=1
      RESTART=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

HOST_ROOT="$(cd "$HOST_ROOT" && pwd)"
if [[ ! -d "$HOST_ROOT/.git" ]]; then
  echo "Host root is not a Git checkout: $HOST_ROOT" >&2
  exit 1
fi

if [[ -z "$TARGET_PYTHON" ]]; then
  for candidate in \
    /opt/homebrew/opt/python@3.12/bin/python3.12 \
    /opt/homebrew/bin/python3.12 \
    /opt/homebrew/opt/python@3.13/bin/python3.13 \
    /opt/homebrew/bin/python3.13; do
    if [[ -x "$candidate" ]]; then
      TARGET_PYTHON="$candidate"
      break
    fi
  done
fi
if [[ -z "$TARGET_PYTHON" || ! -x "$TARGET_PYTHON" ]]; then
  echo "No Python 3.12+ runtime found. Set --python /path/to/python3.12." >&2
  exit 1
fi

"$TARGET_PYTHON" - <<'PY'
import ssl
import sys

if sys.version_info < (3, 12):
    raise SystemExit(f"Python 3.12+ is required, got {sys.version.split()[0]}")
if "LibreSSL" in ssl.OPENSSL_VERSION:
    raise SystemExit(f"OpenSSL-backed Python is required, got {ssl.OPENSSL_VERSION}")
print(f"target_python={sys.executable} version={sys.version.split()[0]} ssl={ssl.OPENSSL_VERSION}")
PY

candidate="$HOST_ROOT/.venv-python-upgrade"
rm -rf "$candidate"
"$TARGET_PYTHON" -m venv "$candidate"
"$candidate/bin/python" -m pip install --upgrade pip >/dev/null
"$candidate/bin/pip" install -r "$HOST_ROOT/requirements.txt"
PYTHONPATH="$HOST_ROOT" "$candidate/bin/python" -m py_compile \
  "$HOST_ROOT/app.py" \
  "$HOST_ROOT/bpmis_jira_tool/web.py" \
  "$HOST_ROOT/bpmis_jira_tool/job_store.py" \
  "$HOST_ROOT/bpmis_jira_tool/release_manifest.py" \
  "$HOST_ROOT/scripts/release_status.py" \
  "$HOST_ROOT/scripts/portal_runtime_doctor.py"

if [[ "$APPLY" != "1" ]]; then
  echo "Candidate venv verified: $candidate"
  echo "Re-run with --apply to replace $HOST_ROOT/.venv."
  exit 0
fi

timestamp="$(date '+%Y%m%d%H%M%S')"
backup="$HOST_ROOT/.venv.backup-$timestamp"
if [[ -e "$HOST_ROOT/.venv" ]]; then
  mv "$HOST_ROOT/.venv" "$backup"
  echo "Backed up existing host venv: $backup"
fi
mv "$candidate" "$HOST_ROOT/.venv"
echo "Activated host venv: $HOST_ROOT/.venv"
"$HOST_ROOT/.venv/bin/python" - <<'PY'
import ssl
import sys

print(f"active_python={sys.executable} version={sys.version.split()[0]} ssl={ssl.OPENSSL_VERSION}")
PY

if [[ "$RESTART" == "1" ]]; then
  cd "$HOST_ROOT"
  ./scripts/run_team_stack.sh restart
fi
