#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
export_env_file

HOST="${LOCAL_AGENT_HOST:-$(read_env_value LOCAL_AGENT_HOST)}"
HOST="${HOST:-127.0.0.1}"
PORT="${LOCAL_AGENT_PORT:-$(read_env_value LOCAL_AGENT_PORT)}"
PORT="${PORT:-7007}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python virtual environment at $PYTHON_BIN"
  exit 1
fi

cd "$ROOT_DIR"
exec "$PYTHON_BIN" -m flask --app local_agent run --host "$HOST" --port "$PORT"
