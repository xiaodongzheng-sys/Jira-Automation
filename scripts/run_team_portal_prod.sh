#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

HOST="${TEAM_PORTAL_HOST:-$(read_env_value TEAM_PORTAL_HOST)}"
HOST="${HOST:-0.0.0.0}"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
PROBE_HOST="${TEAM_PORTAL_PROBE_HOST:-127.0.0.1}"
DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
EXPECTED_REVISION="$(current_release_revision)"

mkdir -p "$DATA_DIR" "$DATA_DIR/logs" "$DATA_DIR/run"

PID_FILE="$DATA_DIR/run/team_portal.pid"
LOG_FILE="$DATA_DIR/logs/team_portal.log"

find_live_pid() {
  lsof -tiTCP:"$PORT" -sTCP:LISTEN -n -P 2>/dev/null | head -n 1
}

is_running() {
  local pid
  pid="$(find_live_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    if portal_revision_matches "$PROBE_HOST" "$PORT" "$EXPECTED_REVISION"; then
      echo "$pid" >"$PID_FILE"
      return 0
    fi
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
    echo "Team portal already running on http://$HOST:$PORT (pid $(cat "$PID_FILE"))."
    echo "Revision: $EXPECTED_REVISION"
    echo "Log: $LOG_FILE"
    return 0
  fi

  local stale_pid
  stale_pid="$(find_live_pid || true)"
  if [[ -n "${stale_pid:-}" ]]; then
    echo "Stopping stale portal process on port $PORT (pid $stale_pid) because its revision does not match $EXPECTED_REVISION."
    kill "$stale_pid" >/dev/null 2>&1 || true
    sleep 1
    kill -9 "$stale_pid" >/dev/null 2>&1 || true
  fi

  cd "$ROOT_DIR"
  nohup env "TEAM_PORTAL_RELEASE_REVISION=$EXPECTED_REVISION" "$PYTHON_BIN" -m flask --app app run --host "$HOST" --port "$PORT" >"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"

  for _ in {1..20}; do
    if portal_revision_matches "$PROBE_HOST" "$PORT" "$EXPECTED_REVISION"; then
      echo "Team portal started at http://$HOST:$PORT"
      echo "PID: $(cat "$PID_FILE")"
      echo "Revision: $EXPECTED_REVISION"
      echo "Data dir: $DATA_DIR"
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
    echo "Revision: $EXPECTED_REVISION"
    echo "Data dir: $DATA_DIR"
    echo "Log: $LOG_FILE"
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
