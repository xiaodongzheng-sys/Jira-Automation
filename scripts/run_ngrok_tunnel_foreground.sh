#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

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
