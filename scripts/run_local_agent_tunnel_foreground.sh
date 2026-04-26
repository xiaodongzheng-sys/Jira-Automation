#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

PUBLIC_URL="${LOCAL_AGENT_PUBLIC_URL:-$(read_env_value LOCAL_AGENT_PUBLIC_URL)}"
PORT="${LOCAL_AGENT_PORT:-$(read_env_value LOCAL_AGENT_PORT)}"
PORT="${PORT:-7007}"
UPSTREAM_ADDR="${LOCAL_AGENT_NGROK_UPSTREAM_ADDR:-127.0.0.1:$PORT}"
NGROK_BIN="${NGROK_BIN:-$(command -v ngrok || true)}"
if [[ -z "$NGROK_BIN" && -x "/opt/homebrew/bin/ngrok" ]]; then
  NGROK_BIN="/opt/homebrew/bin/ngrok"
fi

if [[ -z "$NGROK_BIN" ]]; then
  echo "ngrok is not installed or not on PATH."
  exit 1
fi

if [[ -z "$PUBLIC_URL" ]]; then
  echo "LOCAL_AGENT_PUBLIC_URL is required to start the Mac local-agent ngrok tunnel."
  exit 1
fi

exec "$NGROK_BIN" http --url "$PUBLIC_URL" "$UPSTREAM_ADDR"
