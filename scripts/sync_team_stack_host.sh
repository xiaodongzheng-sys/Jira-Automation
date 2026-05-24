#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

HOST_ROOT="${TEAM_STACK_HOST_ROOT:-$(recommended_team_stack_root)}"
ALLOW_DIRTY_HOST=0
RUN_VERIFY=0
RUN_RESTART=0

usage() {
  cat <<'USAGE'
Usage: scripts/sync_team_stack_host.sh [--host-root PATH] [--allow-dirty-host] [--verify] [--restart]

Synchronize the current source checkout to the Mac host checkout that serves the
public Team Portal. Runtime data, virtualenvs, Git metadata, and local caches are
excluded.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host-root)
      HOST_ROOT="${2:?--host-root requires a path}"
      shift 2
      ;;
    --allow-dirty-host)
      ALLOW_DIRTY_HOST=1
      shift
      ;;
    --verify)
      RUN_VERIFY=1
      shift
      ;;
    --restart)
      RUN_RESTART=1
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

if [[ "$ROOT_DIR" == "$HOST_ROOT" ]]; then
  echo "Source and host root are the same: $ROOT_DIR" >&2
  exit 1
fi
if [[ ! -d "$HOST_ROOT/.git" ]]; then
  echo "Host root is not a Git checkout: $HOST_ROOT" >&2
  exit 1
fi
if [[ ! -d "$ROOT_DIR/.git" ]]; then
  echo "Source root is not a Git checkout: $ROOT_DIR" >&2
  exit 1
fi

host_status="$(git -C "$HOST_ROOT" status --porcelain)"
if [[ -n "$host_status" && "$ALLOW_DIRTY_HOST" != "1" ]]; then
  {
    echo "Host checkout has uncommitted changes; refusing to sync."
    echo "Host root: $HOST_ROOT"
    echo "$host_status"
    echo "Use --allow-dirty-host only after reviewing those changes."
  } >&2
  exit 1
fi

echo "== Sync Team Stack Host =="
echo "Source: $ROOT_DIR"
echo "Host:   $HOST_ROOT"
echo "Source revision: $(current_release_revision)"
echo "Host revision before sync: $(git -C "$HOST_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"

rsync -a --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '.venv-*' \
  --exclude '.venv.backup-*' \
  --exclude '.team-portal/' \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  --exclude '.pytest_cache/' \
  --exclude '.mypy_cache/' \
  --exclude '.coverage' \
  --exclude 'node_modules/' \
  --exclude '.DS_Store' \
  "$ROOT_DIR/" "$HOST_ROOT/"

echo "Host revision after sync: $(cd "$HOST_ROOT" && current_release_revision)"
if [[ -x "$HOST_ROOT/.venv/bin/python" ]]; then
  HOST_DATA_DIR="${TEAM_STACK_HOST_DATA_DIR:-$HOST_ROOT/.team-portal}"
  HOST_MANIFEST_PATH="$HOST_DATA_DIR/run/team_portal_release_manifest.json"
  echo "Host release manifest: $HOST_MANIFEST_PATH"
  PYTHONPATH="$HOST_ROOT" "$HOST_ROOT/.venv/bin/python" -m bpmis_jira_tool.release_manifest \
    --root "$HOST_ROOT" \
    --host-root "$HOST_ROOT" \
    --surface "${TEAM_PORTAL_LIVE_SURFACE:-mac_public_live}" \
    --output "$HOST_MANIFEST_PATH" \
    --print-id >/dev/null
fi

if [[ "$RUN_VERIFY" == "1" ]]; then
  echo
  echo "== Host Verify =="
  if [[ ! -x "$HOST_ROOT/.venv/bin/python" ]]; then
    echo "Host venv missing: $HOST_ROOT/.venv/bin/python" >&2
    exit 1
  fi
  PYTHONPATH="$HOST_ROOT" "$HOST_ROOT/.venv/bin/python" -m py_compile \
    "$HOST_ROOT/app.py" \
    "$HOST_ROOT/bpmis_jira_tool/web.py" \
    "$HOST_ROOT/bpmis_jira_tool/web_runtime_status.py" \
    "$HOST_ROOT/bpmis_jira_tool/background_jobs.py" \
    "$HOST_ROOT/scripts/release_status.py" \
    "$HOST_ROOT/scripts/source_code_qa_ops_summary.py" \
    "$HOST_ROOT/scripts/portal_runtime_doctor.py"
  TEAM_PORTAL_DATA_DIR="${LOCAL_AGENT_TEAM_PORTAL_DATA_DIR:-$HOST_ROOT/.team-portal}" \
    PYTHONPATH="$HOST_ROOT" "$HOST_ROOT/.venv/bin/python" "$HOST_ROOT/scripts/source_code_qa_ops_summary.py" --strict
fi

if [[ "$RUN_RESTART" == "1" ]]; then
  echo
  echo "== Host Restart =="
  cd "$HOST_ROOT"
  if [[ -x "$HOST_ROOT/scripts/run_local_agent.sh" ]]; then
    "$HOST_ROOT/scripts/run_local_agent.sh" restart
  fi
  "$HOST_ROOT/scripts/run_team_stack.sh" restart
fi
