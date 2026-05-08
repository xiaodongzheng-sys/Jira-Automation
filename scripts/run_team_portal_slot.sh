#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
export_env_file

SLOT="${TEAM_PORTAL_SLOT:-candidate}"
HOST="${TEAM_PORTAL_HOST:-$(read_env_value TEAM_PORTAL_HOST)}"
HOST="${HOST:-127.0.0.1}"
PORT="${TEAM_PORTAL_SLOT_PORT:-5001}"
REVISION="${TEAM_PORTAL_SLOT_REVISION:-$(current_release_revision)}"
DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
PID_FILE="$DATA_DIR/run/team_portal.$SLOT.pid"
LOG_FILE="$DATA_DIR/logs/team_portal.$SLOT.log"

mkdir -p "$DATA_DIR/run" "$DATA_DIR/logs"

find_slot_pid() {
  lsof -tiTCP:"$PORT" -sTCP:LISTEN -n -P 2>/dev/null | head -n 1
}

pid_listens_on_slot_port() {
  local pid="$1"
  lsof -tiTCP:"$PORT" -sTCP:LISTEN -n -P 2>/dev/null | grep -Fxq "$pid"
}

slot_has_portal_healthz() {
  fetch_healthz_field "$HOST" "$PORT" revision >/dev/null 2>&1
}

slot_revision_matches() {
  local served
  served="$(fetch_healthz_field "$HOST" "$PORT" revision 2>/dev/null || true)"
  [[ "$served" == "$REVISION" ]]
}

replace_stale_slot_allowed() {
  [[ "${TEAM_PORTAL_SLOT_REPLACE_STALE:-0}" == "1" ]]
}

kill_slot_pid() {
  local pid="$1"
  echo "Stopping team portal slot process on port $PORT (pid $pid)."
  kill "$pid" >/dev/null 2>&1 || true
  sleep 1
  kill -9 "$pid" >/dev/null 2>&1 || true
}

start() {
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Missing Python virtual environment at $PYTHON_BIN"
    exit 1
  fi

  local stale_pid
  stale_pid="$(find_slot_pid || true)"
  if [[ -n "$stale_pid" ]]; then
    if slot_revision_matches; then
      echo "Team portal slot $SLOT already running on http://$HOST:$PORT (pid $stale_pid)."
      echo "$stale_pid" >"$PID_FILE"
      return 0
    fi
    if replace_stale_slot_allowed && slot_has_portal_healthz; then
      kill_slot_pid "$stale_pid"
    else
      echo "Port $PORT is already in use by pid $stale_pid and does not serve expected revision $REVISION."
      echo "Set TEAM_PORTAL_SLOT_REPLACE_STALE=1 only if this is an old team portal slot."
      return 1
    fi
  fi

  cd "$ROOT_DIR"
  nohup env "TEAM_PORTAL_RELEASE_REVISION=$REVISION" "$PYTHON_BIN" -m flask --app app run --host "$HOST" --port "$PORT" >"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
  for _ in {1..20}; do
    if slot_revision_matches; then
      echo "Team portal slot $SLOT started at http://$HOST:$PORT"
      echo "PID: $(cat "$PID_FILE")"
      echo "Revision: $REVISION"
      echo "Log: $LOG_FILE"
      return 0
    fi
    sleep 1
  done

  echo "Team portal slot $SLOT did not become ready in time."
  tail -n 80 "$LOG_FILE" || true
  return 1
}

stop() {
  if [[ -f "$PID_FILE" ]]; then
    local recorded_pid
    recorded_pid="$(cat "$PID_FILE")"
    if [[ -n "$recorded_pid" ]] && pid_listens_on_slot_port "$recorded_pid"; then
      kill_slot_pid "$recorded_pid"
    fi
    rm -f "$PID_FILE"
  fi
  local pid
  pid="$(find_slot_pid || true)"
  if [[ -n "$pid" ]]; then
    if slot_revision_matches || { replace_stale_slot_allowed && slot_has_portal_healthz; }; then
      kill_slot_pid "$pid"
    else
      echo "Port $PORT is in use by pid $pid, but it is not the expected team portal slot; leaving it running."
      return 1
    fi
  fi
  echo "Team portal slot $SLOT stopped."
}

status() {
  if slot_revision_matches; then
    echo "Team portal slot $SLOT healthy on http://$HOST:$PORT"
    echo "Revision: $REVISION"
    return 0
  fi
  echo "Team portal slot $SLOT is not healthy on http://$HOST:$PORT"
  return 1
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  restart) stop || true; start ;;
  status) status ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac
