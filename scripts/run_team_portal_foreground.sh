#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
export_env_file

HOST="${TEAM_PORTAL_HOST:-$(read_env_value TEAM_PORTAL_HOST)}"
HOST="${HOST:-127.0.0.1}"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
RELEASE_REVISION="${TEAM_PORTAL_RELEASE_REVISION:-$(current_release_revision)}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python virtual environment at $PYTHON_BIN"
  exit 1
fi

cd "$ROOT_DIR"
exec env "TEAM_PORTAL_RELEASE_REVISION=$RELEASE_REVISION" "$PYTHON_BIN" -m flask --app app run --host "$HOST" --port "$PORT"
