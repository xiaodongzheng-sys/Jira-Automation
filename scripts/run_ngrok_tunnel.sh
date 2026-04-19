#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
FOREGROUND_SCRIPT="$ROOT_DIR/scripts/run_ngrok_tunnel_foreground.sh"
DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
PUBLIC_URL="${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}"

mkdir -p "$DATA_DIR/logs" "$DATA_DIR/run"

PID_FILE="$DATA_DIR/run/ngrok_tunnel.pid"
LOG_FILE="$DATA_DIR/logs/ngrok_tunnel.log"

find_live_pid() {
  pgrep -f "ngrok http .*127.0.0.1:$PORT|ngrok http .*localhost:$PORT" | head -n 1 || true
}

tunnel_ready() {
  local payload
  payload="$(curl -fsS --max-time 5 "http://127.0.0.1:4040/api/tunnels" 2>/dev/null)" || return 1
  if [[ ! -x "$PYTHON_BIN" ]]; then
    [[ -n "$payload" ]]
    return
  fi
  TUNNELS_PAYLOAD="$payload" PUBLIC_URL="$PUBLIC_URL" PORT="$PORT" "$PYTHON_BIN" - <<'PY' >/dev/null
import json
import os
import sys

payload = os.environ.get("TUNNELS_PAYLOAD", "")
public_url = os.environ.get("PUBLIC_URL", "").strip()
port = os.environ.get("PORT", "").strip()

try:
    tunnels = json.loads(payload).get("tunnels", [])
except json.JSONDecodeError:
    sys.exit(1)

expected_addrs = {f"http://127.0.0.1:{port}", f"http://localhost:{port}", f"127.0.0.1:{port}", f"localhost:{port}"}

for tunnel in tunnels:
    if public_url and tunnel.get("public_url") != public_url:
      continue
    addr = str((tunnel.get("config") or {}).get("addr") or "").strip()
    if not addr or addr in expected_addrs:
      sys.exit(0)

sys.exit(1)
PY
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
    echo "ngrok tunnel already running (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
    return 0
  fi

  nohup "$FOREGROUND_SCRIPT" >"$LOG_FILE" 2>&1 < /dev/null &
  echo $! >"$PID_FILE"

  for _ in {1..20}; do
    if tunnel_ready; then
      echo "ngrok tunnel started."
      echo "PID: $(cat "$PID_FILE")"
      echo "Log: $LOG_FILE"
      return 0
    fi
    sleep 1
  done

  echo "ngrok tunnel did not become ready in time."
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
  echo "ngrok tunnel stopped."
}

status() {
  if is_running && tunnel_ready; then
    echo "ngrok tunnel running (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
  else
    echo "ngrok tunnel is not running."
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
