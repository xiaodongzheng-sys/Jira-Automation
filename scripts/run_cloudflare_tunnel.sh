#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
FOREGROUND_SCRIPT="$ROOT_DIR/scripts/run_cloudflare_tunnel_foreground.sh"
DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
PUBLIC_URL="${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}"
TUNNEL_NAME="${TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME:-$(read_env_value TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME)}"
TUNNEL_NAME="${TUNNEL_NAME:-bankpmtool-live}"
CLOUDFLARED_BIN="${CLOUDFLARED_BIN:-$(command -v cloudflared || true)}"
if [[ -z "$CLOUDFLARED_BIN" && -x "/opt/homebrew/bin/cloudflared" ]]; then
  CLOUDFLARED_BIN="/opt/homebrew/bin/cloudflared"
fi

mkdir -p "$DATA_DIR/logs" "$DATA_DIR/run"

PID_FILE="$DATA_DIR/run/cloudflare_tunnel.pid"
LOG_FILE="$DATA_DIR/logs/cloudflare_tunnel.log"

find_live_pid() {
  pgrep -f "cloudflared .*${TUNNEL_NAME}" | head -n 1 || pgrep -f "cloudflared .*run --token" | head -n 1 || true
}

tunnel_ready() {
  if [[ -n "$CLOUDFLARED_BIN" ]]; then
    local tunnel_info
    tunnel_info="$("$CLOUDFLARED_BIN" tunnel info "$TUNNEL_NAME" 2>/dev/null || true)"
    if [[ "$tunnel_info" == *"CONNECTOR ID"* ]]; then
      return 0
    fi
  fi
  [[ -n "$PUBLIC_URL" ]] && curl -fsS --max-time 10 "$PUBLIC_URL/healthz" >/dev/null 2>&1
}

is_running() {
  local pid
  pid="$(find_live_pid)"
  if [[ -n "${pid:-}" ]]; then
    echo "$pid" >"$PID_FILE"
    return 0
  fi
  return 1
}

start() {
  if is_running; then
    echo "Cloudflare Tunnel already running (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
    return 0
  fi

  nohup "$FOREGROUND_SCRIPT" >"$LOG_FILE" 2>&1 < /dev/null &
  echo $! >"$PID_FILE"

  for _ in {1..30}; do
    if tunnel_ready; then
      echo "Cloudflare Tunnel started."
      echo "PID: $(cat "$PID_FILE")"
      echo "Log: $LOG_FILE"
      return 0
    fi
    sleep 1
  done

  echo "Cloudflare Tunnel did not become ready in time."
  tail -n 80 "$LOG_FILE" || true
  return 1
}

stop() {
  if [[ -f "$PID_FILE" ]]; then
    kill "$(cat "$PID_FILE")" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
  fi
  local pid
  pid="$(find_live_pid)"
  if [[ -n "${pid:-}" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  echo "Cloudflare Tunnel stopped."
}

status() {
  if is_running && tunnel_ready; then
    echo "Cloudflare Tunnel running (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
  else
    echo "Cloudflare Tunnel is not running."
    return 1
  fi
}

logs() {
  touch "$LOG_FILE"
  tail -n 80 "$LOG_FILE"
}

restart() {
  stop || true
  start
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  restart) restart ;;
  status) status ;;
  logs) logs ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs}"
    exit 1
    ;;
esac
