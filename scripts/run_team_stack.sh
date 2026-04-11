#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$ROOT_DIR/tmp"
HELPER_PID_FILE="$TMP_DIR/team_helper.pid"
HELPER_LOG_FILE="$TMP_DIR/team_helper.log"
HELPER_PORT="${HELPER_PORT:-8787}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

mkdir -p "$TMP_DIR"

find_helper_pid() {
  lsof -tiTCP:"$HELPER_PORT" -sTCP:LISTEN -n -P 2>/dev/null | head -n 1
}

helper_running() {
  local pid
  pid="$(find_helper_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    echo "$pid" >"$HELPER_PID_FILE"
    return 0
  fi
  return 1
}

start_helper() {
  if helper_running; then
    echo "Team helper already running on http://127.0.0.1:$HELPER_PORT (pid $(cat "$HELPER_PID_FILE"))."
    return 0
  fi

  cd "$ROOT_DIR"
  nohup "$PYTHON_BIN" -m team_helper.app >"$HELPER_LOG_FILE" 2>&1 &
  echo $! >"$HELPER_PID_FILE"

  for _ in {1..20}; do
    if curl -fsS "http://127.0.0.1:$HELPER_PORT/health" >/dev/null 2>&1; then
      echo "Team helper started at http://127.0.0.1:$HELPER_PORT"
      echo "PID: $(cat "$HELPER_PID_FILE")"
      echo "Log: $HELPER_LOG_FILE"
      return 0
    fi
    sleep 1
  done

  echo "Team helper did not become ready in time."
  tail -n 80 "$HELPER_LOG_FILE" || true
  return 1
}

stop_helper() {
  if [[ -f "$HELPER_PID_FILE" ]]; then
    kill "$(cat "$HELPER_PID_FILE")" >/dev/null 2>&1 || true
    rm -f "$HELPER_PID_FILE"
  fi
  local pid
  pid="$(find_helper_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  echo "Team helper stopped."
}

helper_status() {
  if helper_running; then
    echo "Team helper running on http://127.0.0.1:$HELPER_PORT (pid $(cat "$HELPER_PID_FILE"))."
  else
    echo "Team helper is not running."
    return 1
  fi
}

logs() {
  echo "== Team Portal =="
  "$ROOT_DIR/scripts/run_team_portal.sh" logs || true
  echo
  echo "== Team Helper =="
  touch "$HELPER_LOG_FILE"
  tail -n 80 "$HELPER_LOG_FILE"
}

start() {
  "$ROOT_DIR/scripts/run_team_portal.sh" start
  start_helper
}

stop() {
  stop_helper || true
  "$ROOT_DIR/scripts/run_team_portal.sh" stop || true
}

status() {
  "$ROOT_DIR/scripts/run_team_portal.sh" status || true
  helper_status || true
}

restart() {
  stop
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
