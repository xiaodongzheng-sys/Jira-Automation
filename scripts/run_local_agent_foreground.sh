#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
export_env_file

HOST="${LOCAL_AGENT_HOST:-$(read_env_value LOCAL_AGENT_HOST)}"
HOST="${HOST:-127.0.0.1}"
PORT="${LOCAL_AGENT_PORT:-$(read_env_value LOCAL_AGENT_PORT)}"
PORT="${PORT:-7007}"
AGENT_DATA_DIR="${LOCAL_AGENT_TEAM_PORTAL_DATA_DIR:-$(read_env_value LOCAL_AGENT_TEAM_PORTAL_DATA_DIR)}"
if [[ -n "${AGENT_DATA_DIR:-}" ]]; then
  export TEAM_PORTAL_DATA_DIR="$AGENT_DATA_DIR"
fi

import_host_runtime_env() {
  local data_dir="${1:-}"
  [[ -n "$data_dir" && "$data_dir" == */.team-portal ]] || return 0
  local host_env="${data_dir%/.team-portal}/.env"
  [[ -f "$host_env" ]] || return 0
  while IFS= read -r -d '' pair; do
    if [[ -n "$pair" ]]; then
      export "$pair"
    fi
  done < <(HOST_ENV_FILE="$host_env" "$PYTHON_BIN" - <<'PY'
import os
import re
from dotenv import dotenv_values

host_env = os.environ.get("HOST_ENV_FILE", "")
values = dotenv_values(host_env)
name_pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
allowed_prefixes = (
    "BPMIS_",
    "CONFLUENCE_",
    "ELEVENLABS_",
    "GEMINI_",
    "GMAIL_",
    "GOOGLE_APPLICATION_",
    "GOOGLE_CLOUD_",
    "JIRA_",
    "OPENAI_",
    "PRD_BRIEFING_",
    "SEATALK_",
    "SOURCE_CODE_QA_",
)
allowed_names = {
    "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY",
}
secret_markers = ("TOKEN", "SECRET", "KEY", "CREDENTIAL", "PASSWORD", "COOKIE", "AUTH")
blocked_prefixes = ("LOCAL_AGENT_", "TEAM_ALLOWED_", "TEAM_PORTAL_")
blocked_names = {
    "FLASK_SECRET_KEY",
    "GOOGLE_OAUTH_CLIENT_SECRET_FILE",
    "GOOGLE_OAUTH_REDIRECT_URI",
    "TEAM_PORTAL_BASE_URL",
    "TEAM_PORTAL_DATA_DIR",
    "TEAM_PORTAL_HOST",
    "TEAM_PORTAL_PORT",
}

for key, value in values.items():
    if not key or value is None or not name_pattern.match(str(key)):
        continue
    key = str(key)
    if key in os.environ:
        continue
    if key not in allowed_names and (key in blocked_names or key.startswith(blocked_prefixes)):
        continue
    allowed = key in allowed_names or key.startswith(allowed_prefixes) or any(marker in key for marker in secret_markers)
    if not allowed:
        continue
    os.write(1, f"{key}={value}".encode("utf-8") + b"\0")
PY
)
}

import_host_runtime_env "$TEAM_PORTAL_DATA_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python virtual environment at $PYTHON_BIN"
  exit 1
fi

cd "$ROOT_DIR"
if "$PYTHON_BIN" -c "import gunicorn.app.wsgiapp" >/dev/null 2>&1; then
  exec "$PYTHON_BIN" -m gunicorn \
    --bind "$HOST:$PORT" \
    --workers "${LOCAL_AGENT_WORKERS:-1}" \
    --threads "${LOCAL_AGENT_THREADS:-8}" \
    --timeout "${LOCAL_AGENT_GUNICORN_TIMEOUT:-360}" \
    local_agent:app
fi

exec "$PYTHON_BIN" -m flask --app local_agent run --host "$HOST" --port "$PORT"
