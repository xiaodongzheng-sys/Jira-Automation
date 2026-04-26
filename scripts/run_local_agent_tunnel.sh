#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
FOREGROUND_SCRIPT="$ROOT_DIR/scripts/run_local_agent_tunnel_foreground.sh"
DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
PORT="${LOCAL_AGENT_PORT:-$(read_env_value LOCAL_AGENT_PORT)}"
PORT="${PORT:-7007}"
PUBLIC_URL="${LOCAL_AGENT_PUBLIC_URL:-$(read_env_value LOCAL_AGENT_PUBLIC_URL)}"

mkdir -p "$DATA_DIR/logs" "$DATA_DIR/run"

PID_FILE="$DATA_DIR/run/local_agent_ngrok.pid"
LOG_FILE="$DATA_DIR/logs/local_agent_ngrok.log"

find_live_pid() {
  pgrep -f "ngrok http .*127.0.0.1:$PORT|ngrok http .*localhost:$PORT" | head -n 1 || true
}

tunnel_ready() {
  local payload
  payload="$(curl -fsS --max-time 5 "http://127.0.0.1:4040/api/tunnels" 2>/dev/null)" || return 1
  if [[ -z "$PUBLIC_URL" ]]; then
    [[ -n "$payload" ]]
    return
  fi
  [[ "$payload" == *"$PUBLIC_URL"* ]]
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
    echo "Mac local-agent ngrok tunnel already running (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
    return 0
  fi

  nohup "$FOREGROUND_SCRIPT" >"$LOG_FILE" 2>&1 < /dev/null &
  echo $! >"$PID_FILE"

  for _ in {1..20}; do
    if tunnel_ready; then
      echo "Mac local-agent ngrok tunnel started."
      echo "PID: $(cat "$PID_FILE")"
      echo "Log: $LOG_FILE"
      return 0
    fi
    sleep 1
  done

  echo "Mac local-agent ngrok tunnel did not become ready in time."
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
  echo "Mac local-agent ngrok tunnel stopped."
}

status() {
  if is_running && tunnel_ready; then
    echo "Mac local-agent ngrok tunnel running (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
  else
    echo "Mac local-agent ngrok tunnel is not running."
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
