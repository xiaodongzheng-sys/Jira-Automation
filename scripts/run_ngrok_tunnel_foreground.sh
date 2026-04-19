#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

read_env_value() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" || ! -x "$PYTHON_BIN" ]]; then
    return 0
  fi
  "$PYTHON_BIN" - <<PY
from dotenv import dotenv_values
values = dotenv_values("$ENV_FILE")
value = values.get("$key", "")
print(value if value is not None else "")
PY
}

PUBLIC_URL="${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
UPSTREAM_ADDR="${NGROK_UPSTREAM_ADDR:-127.0.0.1:$PORT}"
NGROK_BIN="${NGROK_BIN:-$(command -v ngrok || true)}"
if [[ -z "$NGROK_BIN" && -x "/opt/homebrew/bin/ngrok" ]]; then
  NGROK_BIN="/opt/homebrew/bin/ngrok"
fi

if [[ -z "$NGROK_BIN" ]]; then
  echo "ngrok is not installed or not on PATH."
  exit 1
fi

if [[ -z "$PUBLIC_URL" ]]; then
  echo "TEAM_PORTAL_BASE_URL is required to start the public ngrok tunnel."
  exit 1
fi

exec "$NGROK_BIN" http --url "$PUBLIC_URL" "$UPSTREAM_ADDR"
