#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

LIVE_WORKSPACE="${LIVE_TEAM_STACK_HOST_WORKSPACE:-$(recommended_team_stack_root)}"
UAT_WORKSPACE="${CLOUD_RUN_UAT_HOST_WORKSPACE:-$(recommended_uat_team_stack_root)}"
UAT_DATA_DIR="${CLOUD_RUN_UAT_LOCAL_AGENT_DATA_DIR:-.team-portal-uat}"
UAT_PORT="${CLOUD_RUN_UAT_LOCAL_AGENT_PORT:-7008}"
UAT_SCREEN_SESSION="${CLOUD_RUN_UAT_LOCAL_AGENT_SCREEN_SESSION:-bpmis-local-agent-uat}"
UAT_PYTHON_BIN="${CLOUD_RUN_UAT_PYTHON_BIN:-}"
FORCE=0

usage() {
  echo "Usage: $0 [--force]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done

resolve_data_path_for_workspace() {
  local workspace="$1"
  local data_dir="$2"
  WORKSPACE_VALUE="$workspace" DATA_DIR_VALUE="$data_dir" "$PYTHON_BIN" - <<'PY'
import os
from pathlib import Path

workspace = Path(os.environ["WORKSPACE_VALUE"]).expanduser().resolve()
data_dir = os.environ.get("DATA_DIR_VALUE", "").strip() or ".team-portal"
path = Path(data_dir).expanduser()
if not path.is_absolute():
    path = workspace / path
print(path)
PY
}

live_env_value() {
  local key="$1"
  ENV_FILE="$LIVE_WORKSPACE/.env" read_env_value "$key"
}

resolve_uat_python_bin() {
  if [[ -n "$UAT_PYTHON_BIN" ]]; then
    printf '%s\n' "$UAT_PYTHON_BIN"
    return 0
  fi
  for candidate in /opt/homebrew/bin/python3.12 /opt/homebrew/bin/python3.13 "$PYTHON_BIN" "$(command -v python3 || true)"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  printf '%s\n' "python3"
}

if [[ ! -d "$LIVE_WORKSPACE/.git" ]]; then
  echo "Live host workspace is missing: $LIVE_WORKSPACE"
  exit 1
fi

if [[ ! -d "$UAT_WORKSPACE/.git" ]]; then
  mkdir -p "$(dirname "$UAT_WORKSPACE")"
  repo_url="$(git -C "$ROOT_DIR" remote get-url origin 2>/dev/null || printf '%s\n' "$ROOT_DIR")"
  echo "Creating UAT host workspace: $UAT_WORKSPACE"
  git clone "$repo_url" "$UAT_WORKSPACE"
fi

git -C "$UAT_WORKSPACE" checkout main >/dev/null
git -C "$UAT_WORKSPACE" fetch origin main >/dev/null
git -C "$UAT_WORKSPACE" merge --ff-only origin/main >/dev/null

UAT_PYTHON_BIN="$(resolve_uat_python_bin)"
if [[ ! -x "$UAT_WORKSPACE/.venv/bin/pip" ]]; then
  rm -rf "$UAT_WORKSPACE/.venv"
  echo "Creating UAT host virtual environment"
  "$UAT_PYTHON_BIN" -m venv "$UAT_WORKSPACE/.venv"
fi
"$UAT_WORKSPACE/.venv/bin/pip" install -r "$UAT_WORKSPACE/requirements.txt"

LIVE_DATA_DIR="${CLOUD_RUN_UAT_SEED_SOURCE_DATA_DIR:-$(live_env_value LOCAL_AGENT_TEAM_PORTAL_DATA_DIR)}"
LIVE_DATA_DIR="${LIVE_DATA_DIR:-$(live_env_value TEAM_PORTAL_DATA_DIR)}"
LIVE_DATA_DIR="${LIVE_DATA_DIR:-.team-portal}"
LIVE_DATA_PATH="$(resolve_data_path_for_workspace "$LIVE_WORKSPACE" "$LIVE_DATA_DIR")"
UAT_DATA_PATH="$(resolve_data_path_for_workspace "$UAT_WORKSPACE" "$UAT_DATA_DIR")"

if [[ ! -f "$UAT_WORKSPACE/.env" || "$FORCE" == "1" ]]; then
  echo "Writing UAT .env with isolated local-agent defaults"
  UAT_ENV_PATH="$UAT_WORKSPACE/.env" \
  LIVE_ENV_PATH="$LIVE_WORKSPACE/.env" \
  UAT_DATA_DIR_VALUE="$UAT_DATA_DIR" \
  UAT_PORT_VALUE="$UAT_PORT" \
  UAT_SCREEN_SESSION_VALUE="$UAT_SCREEN_SESSION" \
  "$PYTHON_BIN" - <<'PY'
import os
import secrets
from pathlib import Path

live_env = Path(os.environ["LIVE_ENV_PATH"])
uat_env = Path(os.environ["UAT_ENV_PATH"])
values: dict[str, str] = {}
if live_env.exists():
    for raw in live_env.read_text(encoding="utf-8").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip()

values.update(
    {
        "TEAM_PORTAL_DATA_DIR": os.environ["UAT_DATA_DIR_VALUE"],
        "LOCAL_AGENT_TEAM_PORTAL_DATA_DIR": os.environ["UAT_DATA_DIR_VALUE"],
        "LOCAL_AGENT_HOST": "127.0.0.1",
        "LOCAL_AGENT_PORT": os.environ["UAT_PORT_VALUE"],
        "LOCAL_AGENT_SCREEN_SESSION": os.environ["UAT_SCREEN_SESSION_VALUE"],
        "LOCAL_AGENT_MODE": "sync",
        "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED": "true",
        "LOCAL_AGENT_SEATALK_ENABLED": "true",
        "LOCAL_AGENT_BPMIS_ENABLED": "true",
        "LOCAL_AGENT_HMAC_SECRET": values.get("LOCAL_AGENT_UAT_HMAC_SECRET") or secrets.token_urlsafe(48),
    }
)
values.pop("LOCAL_AGENT_PUBLIC_URL", None)
lines = [f"{key}={value}" for key, value in values.items()]
uat_env.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
fi

if [[ -e "$UAT_DATA_PATH" && "$FORCE" != "1" ]]; then
  if [[ -n "$(find "$UAT_DATA_PATH" -mindepth 1 -maxdepth 1 2>/dev/null | head -n 1)" ]]; then
    echo "UAT data root already exists and is not empty: $UAT_DATA_PATH"
    echo "Use --force to reseed it from live."
    exit 1
  fi
fi

if [[ "$FORCE" == "1" ]]; then
  rm -rf "$UAT_DATA_PATH"
fi
mkdir -p "$UAT_DATA_PATH"

if [[ -d "$LIVE_DATA_PATH" ]]; then
  echo "Seeding UAT data root from $LIVE_DATA_PATH"
  rsync -a \
    --exclude '/logs/' \
    --exclude '/run/' \
    --exclude '/source_code_qa/answer_cache/' \
    --exclude '/source_code_qa/eval_runs/' \
    --exclude '/source_code_qa/indexes/' \
    --exclude '/source_code_qa/locks/' \
    --exclude '/source_code_qa/sync_jobs.json' \
    --exclude '*.pid' \
    --exclude '*.sock' \
    "$LIVE_DATA_PATH/" "$UAT_DATA_PATH/"
else
  echo "Live data root not found; created empty UAT data root: $UAT_DATA_PATH"
fi

echo "UAT local-agent workspace: $UAT_WORKSPACE"
echo "UAT local-agent data root: $UAT_DATA_PATH"
echo "UAT local-agent port: $UAT_PORT"
echo "UAT Cloud Run secret name: ${CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_NAME:-local-agent-uat-hmac-secret}"
echo "Sync Secret Manager local-agent-uat-hmac-secret with LOCAL_AGENT_HMAC_SECRET from $UAT_WORKSPACE/.env before the first deploy."
echo "Start with:"
echo "  CLOUD_RUN_UAT_HOST_WORKSPACE='$UAT_WORKSPACE' ./scripts/deploy_cloud_run_uat.sh"
