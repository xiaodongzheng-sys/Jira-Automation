#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

HOST="${LOCAL_AGENT_HOST:-$(read_env_value LOCAL_AGENT_HOST)}"
HOST="${HOST:-127.0.0.1}"
PORT="${LOCAL_AGENT_PORT:-$(read_env_value LOCAL_AGENT_PORT)}"
PORT="${PORT:-7007}"
DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
FOREGROUND_SCRIPT="$ROOT_DIR/scripts/run_local_agent_foreground.sh"

mkdir -p "$DATA_DIR/logs" "$DATA_DIR/run"

PID_FILE="$DATA_DIR/run/local_agent.pid"
LOG_FILE="$DATA_DIR/logs/local_agent.log"

find_live_pid() {
  lsof -tiTCP:"$PORT" -sTCP:LISTEN -n -P 2>/dev/null | head -n 1
}

is_running() {
  local pid
  pid="$(find_live_pid || true)"
  if [[ -n "${pid:-}" ]] && curl -fsS --max-time 5 "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
    echo "$pid" >"$PID_FILE"
    return 0
  fi
  return 1
}

start() {
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Missing Python virtual environment at $PYTHON_BIN"
    echo "Create it first, for example:"
    echo "  python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt"
    exit 1
  fi

  if is_running; then
    echo "Mac local-agent already running on http://$HOST:$PORT (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
    return 0
  fi

  cd "$ROOT_DIR"
  nohup "$FOREGROUND_SCRIPT" >"$LOG_FILE" 2>&1 < /dev/null &
  echo $! >"$PID_FILE"

  for _ in {1..20}; do
    if is_running; then
      echo "Mac local-agent started at http://$HOST:$PORT"
      echo "PID: $(cat "$PID_FILE")"
      echo "Log: $LOG_FILE"
      return 0
    fi
    sleep 1
  done

  echo "Mac local-agent did not become ready in time."
  tail -n 80 "$LOG_FILE" || true
  return 1
}

stop() {
  if [[ -f "$PID_FILE" ]]; then
    kill "$(cat "$PID_FILE")" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
  fi
  local pid
  pid="$(find_live_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  echo "Mac local-agent stopped."
}

status() {
  if is_running; then
    echo "Mac local-agent running on http://$HOST:$PORT (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
  else
    echo "Mac local-agent is not running."
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
