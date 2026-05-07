#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

TUNNEL_NAME="${TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME:-$(read_env_value TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME)}"
TUNNEL_NAME="${TUNNEL_NAME:-bankpmtool-live}"
CLOUDFLARE_PROTOCOL="${TEAM_PORTAL_CLOUDFLARE_PROTOCOL:-$(read_env_value TEAM_PORTAL_CLOUDFLARE_PROTOCOL)}"
CLOUDFLARE_PROTOCOL="${CLOUDFLARE_PROTOCOL:-http2}"
CLOUDFLARED_BIN="${CLOUDFLARED_BIN:-$(command -v cloudflared || true)}"
if [[ -z "$CLOUDFLARED_BIN" && -x "/opt/homebrew/bin/cloudflared" ]]; then
  CLOUDFLARED_BIN="/opt/homebrew/bin/cloudflared"
fi

if [[ -z "$CLOUDFLARED_BIN" ]]; then
  echo "cloudflared is not installed or not on PATH."
  exit 1
fi

if [[ -n "${CLOUDFLARE_TUNNEL_TOKEN:-}" ]]; then
  exec "$CLOUDFLARED_BIN" tunnel --protocol "$CLOUDFLARE_PROTOCOL" run --token "$CLOUDFLARE_TUNNEL_TOKEN"
fi

exec "$CLOUDFLARED_BIN" tunnel --protocol "$CLOUDFLARE_PROTOCOL" run "$TUNNEL_NAME"
