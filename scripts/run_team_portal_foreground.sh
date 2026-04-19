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

HOST="${TEAM_PORTAL_HOST:-$(read_env_value TEAM_PORTAL_HOST)}"
HOST="${HOST:-127.0.0.1}"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python virtual environment at $PYTHON_BIN"
  exit 1
fi

cd "$ROOT_DIR"
exec "$PYTHON_BIN" -m flask --app app run --host "$HOST" --port "$PORT"
