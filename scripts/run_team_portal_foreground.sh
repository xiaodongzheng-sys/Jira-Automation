#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
export_env_file

HOST="${TEAM_PORTAL_HOST:-$(read_env_value TEAM_PORTAL_HOST)}"
HOST="${HOST:-127.0.0.1}"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
RELEASE_REVISION="${TEAM_PORTAL_RELEASE_REVISION:-$(current_release_revision)}"
LIVE_SURFACE="${TEAM_PORTAL_LIVE_SURFACE:-mac_public_live}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python virtual environment at $PYTHON_BIN"
  exit 1
fi

cd "$ROOT_DIR"
MANIFEST_PATH="$(release_manifest_path "$DATA_DIR")"
MANIFEST_ID="$(write_release_manifest "$DATA_DIR" "$LIVE_SURFACE")"
exec env \
  "TEAM_PORTAL_RELEASE_REVISION=$RELEASE_REVISION" \
  "TEAM_PORTAL_RELEASE_MANIFEST_PATH=$MANIFEST_PATH" \
  "TEAM_PORTAL_RELEASE_MANIFEST_ID=$MANIFEST_ID" \
  "TEAM_PORTAL_LIVE_SURFACE=$LIVE_SURFACE" \
  "$PYTHON_BIN" -m flask --app app run --host "$HOST" --port "$PORT"
