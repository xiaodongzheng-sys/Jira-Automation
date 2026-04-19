#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
GUARD_SCRIPT="$ROOT_DIR/scripts/run_team_stack_guard.sh"

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

DATA_DIR="${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/.team-portal}"
if [[ "$DATA_DIR" != /* ]]; then
  DATA_DIR="$ROOT_DIR/$DATA_DIR"
fi

mkdir -p "$DATA_DIR/logs" "$DATA_DIR/run"

PID_FILE="$DATA_DIR/run/team_stack_guard.pid"
CAFFEINATE_PID_FILE="$DATA_DIR/run/team_stack_caffeinate.pid"
PORTAL_CHILD_PID_FILE="$DATA_DIR/run/team_portal.child.pid"
NGROK_CHILD_PID_FILE="$DATA_DIR/run/ngrok_tunnel.child.pid"
STATUS_FILE="$DATA_DIR/run/team_stack_status.json"
ALERT_FILE="$DATA_DIR/run/team_stack_alert.json"
LAUNCH_LOG="$DATA_DIR/logs/team_stack_guard.launch.log"
USE_CAFFEINATE="${TEAM_STACK_USE_CAFFEINATE:-auto}"

find_live_pid() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      local command
      command="$(ps -o command= -p "$pid" 2>/dev/null || true)"
      if [[ "$command" == *"$GUARD_SCRIPT"* || "$command" == *"run_team_stack_guard.sh"* ]]; then
        echo "$pid"
        return 0
      fi
    fi
  fi
  return 1
}

start() {
  local pid
  pid="$(find_live_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    echo "Team stack guard already running (pid $pid)."
    return 0
  fi

  rm -f "$PID_FILE" "$CAFFEINATE_PID_FILE"
  nohup env "TEAM_STACK_USE_CAFFEINATE=$USE_CAFFEINATE" "$GUARD_SCRIPT" >>"$LAUNCH_LOG" 2>&1 < /dev/null &
  sleep 2
  pid="$(find_live_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    echo "Team stack guard started (pid $pid)."
    echo "Log: $DATA_DIR/logs/team_stack_guard.log"
    if [[ -f "$CAFFEINATE_PID_FILE" ]]; then
      echo "caffeinate pid: $(cat "$CAFFEINATE_PID_FILE")"
    fi
    return 0
  fi

  echo "Team stack guard failed to start."
  tail -n 80 "$LAUNCH_LOG" || true
  return 1
}

stop() {
  local pid
  pid="$(find_live_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$PID_FILE" "$CAFFEINATE_PID_FILE" "$PORTAL_CHILD_PID_FILE" "$NGROK_CHILD_PID_FILE"
  echo "Team stack guard stopped."
}

status() {
  local pid
  pid="$(find_live_pid || true)"
  if [[ -n "${pid:-}" ]]; then
    echo "Team stack guard running (pid $pid)."
    echo "Log: $DATA_DIR/logs/team_stack_guard.log"
    if [[ -f "$PORTAL_CHILD_PID_FILE" ]]; then
      echo "Portal child pid: $(cat "$PORTAL_CHILD_PID_FILE")"
    fi
    if [[ -f "$NGROK_CHILD_PID_FILE" ]]; then
      echo "ngrok child pid: $(cat "$NGROK_CHILD_PID_FILE")"
    fi
    if [[ -f "$CAFFEINATE_PID_FILE" ]]; then
      local caffeinate_pid
      caffeinate_pid="$(cat "$CAFFEINATE_PID_FILE" 2>/dev/null || true)"
      if [[ -n "${caffeinate_pid:-}" ]] && kill -0 "$caffeinate_pid" >/dev/null 2>&1; then
        echo "Sleep prevention: enabled with caffeinate (pid $caffeinate_pid)."
      else
        echo "Sleep prevention: requested but not currently active."
      fi
    else
      echo "Sleep prevention: disabled."
    fi
    if [[ -f "$STATUS_FILE" ]]; then
      echo "Status summary: $STATUS_FILE"
    fi
    if [[ -f "$ALERT_FILE" ]]; then
      echo "Alert marker: $ALERT_FILE"
    fi
  else
    echo "Team stack guard is not running."
    return 1
  fi
}

logs() {
  touch "$DATA_DIR/logs/team_stack_guard.log"
  tail -n 80 "$DATA_DIR/logs/team_stack_guard.log"
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
