#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$ROOT_DIR/tmp"
PID_FILE="$TMP_DIR/team_portal.pid"
LOG_FILE="$TMP_DIR/team_portal.log"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5000}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

mkdir -p "$TMP_DIR"

find_live_pid() {
  lsof -tiTCP:"$PORT" -sTCP:LISTEN -n -P 2>/dev/null | head -n 1
}

is_running() {
  local pid
  pid="$(find_live_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    echo "$pid" >"$PID_FILE"
    return 0
  fi
  return 1
}

start() {
  if is_running; then
    echo "Team portal already running on http://$HOST:$PORT (pid $(cat "$PID_FILE"))."
    return 0
  fi

  cd "$ROOT_DIR"
  nohup "$PYTHON_BIN" -m flask --app app run --host "$HOST" --port "$PORT" >"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"

  for _ in {1..20}; do
    if curl -fsS "http://$HOST:$PORT/" >/dev/null 2>&1; then
      echo "Team portal started at http://$HOST:$PORT"
      echo "PID: $(cat "$PID_FILE")"
      echo "Log: $LOG_FILE"
      return 0
    fi
    sleep 1
  done

  echo "Team portal did not become ready in time."
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
  echo "Team portal stopped."
}

status() {
  if is_running; then
    echo "Team portal running on http://$HOST:$PORT (pid $(cat "$PID_FILE"))."
  else
    echo "Team portal is not running."
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
