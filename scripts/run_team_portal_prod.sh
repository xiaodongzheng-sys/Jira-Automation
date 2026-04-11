#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

read_env_value() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" || ! -x "$PYTHON_BIN" ]]; then
    return 0
  fi
  "$PYTHON_BIN" - <<PY
from dotenv import dotenv_values
values = dotenv_values("$ENV_FILE")
value = values.get("$key", "")
print(value if value is not None else "")
PY
}

HOST="${TEAM_PORTAL_HOST:-$(read_env_value TEAM_PORTAL_HOST)}"
HOST="${HOST:-0.0.0.0}"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
PROBE_HOST="${TEAM_PORTAL_PROBE_HOST:-127.0.0.1}"
DATA_DIR="${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/.team-portal}"

if [[ "$DATA_DIR" != /* ]]; then
  DATA_DIR="$ROOT_DIR/$DATA_DIR"
fi

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
    echo "Team portal already running on http://$HOST:$PORT (pid $(cat "$PID_FILE"))."
    echo "Log: $LOG_FILE"
    return 0
  fi

  cd "$ROOT_DIR"
  nohup "$PYTHON_BIN" -m flask --app app run --host "$HOST" --port "$PORT" >"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"

  for _ in {1..20}; do
    if curl -fsS "http://$PROBE_HOST:$PORT/" >/dev/null 2>&1; then
      echo "Team portal started at http://$HOST:$PORT"
      echo "PID: $(cat "$PID_FILE")"
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
