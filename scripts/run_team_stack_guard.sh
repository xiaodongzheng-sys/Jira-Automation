#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
PORTAL_FOREGROUND_SCRIPT="$ROOT_DIR/scripts/run_team_portal_foreground.sh"
NGROK_FOREGROUND_SCRIPT="$ROOT_DIR/scripts/run_ngrok_tunnel_foreground.sh"

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
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
PROBE_HOST="${TEAM_PORTAL_PROBE_HOST:-127.0.0.1}"
PUBLIC_URL="${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}"
if [[ "$DATA_DIR" != /* ]]; then
  DATA_DIR="$ROOT_DIR/$DATA_DIR"
fi

mkdir -p "$DATA_DIR/logs" "$DATA_DIR/run"

LOG_FILE="$DATA_DIR/logs/team_stack_guard.log"
PID_FILE="$DATA_DIR/run/team_stack_guard.pid"
CAFFEINATE_PID_FILE="$DATA_DIR/run/team_stack_caffeinate.pid"
PORTAL_CHILD_PID_FILE="$DATA_DIR/run/team_portal.child.pid"
NGROK_CHILD_PID_FILE="$DATA_DIR/run/ngrok_tunnel.child.pid"
STATUS_FILE="$DATA_DIR/run/team_stack_status.json"
ALERT_FILE="$DATA_DIR/run/team_stack_alert.json"
PORTAL_LOG_FILE="$DATA_DIR/logs/team_portal.log"
NGROK_LOG_FILE="$DATA_DIR/logs/ngrok_tunnel.log"
CHECK_INTERVAL="${TEAM_STACK_GUARD_INTERVAL_SECONDS:-15}"
USE_CAFFEINATE="${TEAM_STACK_USE_CAFFEINATE:-auto}"
RESTART_WINDOW_SECONDS="${TEAM_STACK_RESTART_WINDOW_SECONDS:-60}"
MAX_RESTART_BACKOFF_SECONDS="${TEAM_STACK_MAX_RESTART_BACKOFF_SECONDS:-30}"
RESTART_ALERT_THRESHOLD="${TEAM_STACK_RESTART_ALERT_THRESHOLD:-3}"

cd "$ROOT_DIR"
echo $$ >"$PID_FILE"

portal_restart_count=0
ngrok_restart_count=0
portal_last_start_at=0
ngrok_last_start_at=0
portal_unhealthy_count=0
ngrok_unhealthy_count=0

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '%s' "$value"
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" | tee -a "$LOG_FILE"
}

read_pid_file() {
  local pid_file="$1"
  if [[ ! -f "$pid_file" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -z "${pid:-}" ]]; then
    return 1
  fi
  printf '%s\n' "$pid"
}

pid_is_running() {
  local pid="$1"
  [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1
}

stop_child() {
  local pid_file="$1"
  local label="$2"
  local pid
  pid="$(read_pid_file "$pid_file" || true)"
  if [[ -n "${pid:-}" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
    sleep 1
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$pid_file"
  if [[ -n "${pid:-}" ]]; then
    log "$label stopped (pid $pid)."
  fi
}

cleanup() {
  stop_child "$NGROK_CHILD_PID_FILE" "ngrok tunnel"
  stop_child "$PORTAL_CHILD_PID_FILE" "portal"
  if [[ -f "$CAFFEINATE_PID_FILE" ]]; then
    local caffeinate_pid
    caffeinate_pid="$(cat "$CAFFEINATE_PID_FILE" 2>/dev/null || true)"
    if [[ -n "${caffeinate_pid:-}" ]]; then
      kill "$caffeinate_pid" >/dev/null 2>&1 || true
    fi
    rm -f "$CAFFEINATE_PID_FILE"
  fi
  cat >"$STATUS_FILE" <<EOF
{"state":"stopped","updated_at":"$(date '+%Y-%m-%d %H:%M:%S')","guard_pid":null,"portal_child_pid":null,"ngrok_child_pid":null}
EOF
  rm -f "$PID_FILE"
}
trap cleanup EXIT

write_alert_marker() {
  local service_name="$1"
  local restart_count="$2"
  cat >"$ALERT_FILE" <<EOF
{"state":"alert","updated_at":"$(date '+%Y-%m-%d %H:%M:%S')","service":"$(json_escape "$service_name")","restart_count":$restart_count,"window_seconds":$RESTART_WINDOW_SECONDS,"message":"$(json_escape "$service_name restarted frequently within the restart window")"}
EOF
}

clear_alert_marker_if_stable() {
  if [[ -f "$ALERT_FILE" ]] && (( portal_restart_count < RESTART_ALERT_THRESHOLD )) && (( ngrok_restart_count < RESTART_ALERT_THRESHOLD )); then
    rm -f "$ALERT_FILE"
  fi
}

compute_backoff() {
  local service_name="$1"
  local now
  now="$(date +%s)"

  if [[ "$service_name" == "portal" ]]; then
    if (( now - portal_last_start_at <= RESTART_WINDOW_SECONDS )); then
      portal_restart_count=$((portal_restart_count + 1))
    else
      portal_restart_count=0
    fi
    portal_last_start_at="$now"
    local backoff=$((portal_restart_count * 5))
    if (( backoff > MAX_RESTART_BACKOFF_SECONDS )); then
      backoff="$MAX_RESTART_BACKOFF_SECONDS"
    fi
    if (( portal_restart_count >= RESTART_ALERT_THRESHOLD )); then
      write_alert_marker "portal" "$portal_restart_count"
    fi
    printf '%s\n' "$backoff"
    return
  fi

  if (( now - ngrok_last_start_at <= RESTART_WINDOW_SECONDS )); then
    ngrok_restart_count=$((ngrok_restart_count + 1))
  else
    ngrok_restart_count=0
  fi
  ngrok_last_start_at="$now"
  local backoff=$((ngrok_restart_count * 5))
  if (( backoff > MAX_RESTART_BACKOFF_SECONDS )); then
    backoff="$MAX_RESTART_BACKOFF_SECONDS"
  fi
  if (( ngrok_restart_count >= RESTART_ALERT_THRESHOLD )); then
    write_alert_marker "ngrok" "$ngrok_restart_count"
  fi
  printf '%s\n' "$backoff"
}

start_caffeinate() {
  local should_use="0"
  case "$USE_CAFFEINATE" in
    1|true|yes|on)
      should_use="1"
      ;;
    auto)
      if command -v caffeinate >/dev/null 2>&1; then
        should_use="1"
      fi
      ;;
  esac

  if [[ "$should_use" != "1" ]]; then
    log "Sleep prevention disabled for this run."
    return 0
  fi

  if ! command -v caffeinate >/dev/null 2>&1; then
    log "caffeinate was requested but is not available. Continuing without sleep prevention."
    return 0
  fi

  caffeinate -dimsu -w $$ >/dev/null 2>&1 &
  echo $! >"$CAFFEINATE_PID_FILE"
  log "Sleep prevention enabled with caffeinate (pid $(cat "$CAFFEINATE_PID_FILE"))."
}

portal_is_healthy() {
  curl -fsS --max-time 5 "http://$PROBE_HOST:$PORT/healthz" >/dev/null 2>&1
}

ngrok_is_healthy() {
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

start_portal() {
  local backoff
  backoff="$(compute_backoff portal)"
  if (( backoff > 0 )); then
    log "Portal restart backoff ${backoff}s before next launch."
    sleep "$backoff"
  fi
  touch "$PORTAL_LOG_FILE"
  "$PORTAL_FOREGROUND_SCRIPT" >>"$PORTAL_LOG_FILE" 2>&1 &
  echo $! >"$PORTAL_CHILD_PID_FILE"
  log "Portal launched (pid $(cat "$PORTAL_CHILD_PID_FILE"))."
}

start_ngrok() {
  local backoff
  backoff="$(compute_backoff ngrok)"
  if (( backoff > 0 )); then
    log "ngrok restart backoff ${backoff}s before next launch."
    sleep "$backoff"
  fi
  touch "$NGROK_LOG_FILE"
  "$NGROK_FOREGROUND_SCRIPT" >>"$NGROK_LOG_FILE" 2>&1 &
  echo $! >"$NGROK_CHILD_PID_FILE"
  log "ngrok launched (pid $(cat "$NGROK_CHILD_PID_FILE"))."
}

ensure_portal_running() {
  local pid
  pid="$(read_pid_file "$PORTAL_CHILD_PID_FILE" || true)"
  if [[ -n "${pid:-}" ]] && ! pid_is_running "$pid"; then
    log "Portal process exited unexpectedly (pid $pid)."
    rm -f "$PORTAL_CHILD_PID_FILE"
  fi

  if [[ ! -f "$PORTAL_CHILD_PID_FILE" ]]; then
    start_portal
    portal_unhealthy_count=0
    return
  fi

  if portal_is_healthy; then
    portal_unhealthy_count=0
    clear_alert_marker_if_stable
    return
  fi

  portal_unhealthy_count=$((portal_unhealthy_count + 1))
  if (( portal_unhealthy_count >= 2 )); then
    log "Portal health check failed twice; restarting it."
    stop_child "$PORTAL_CHILD_PID_FILE" "portal"
    start_portal
    portal_unhealthy_count=0
  fi
}

ensure_ngrok_running() {
  local pid
  pid="$(read_pid_file "$NGROK_CHILD_PID_FILE" || true)"
  if [[ -n "${pid:-}" ]] && ! pid_is_running "$pid"; then
    log "ngrok process exited unexpectedly (pid $pid)."
    rm -f "$NGROK_CHILD_PID_FILE"
  fi

  if [[ ! -f "$NGROK_CHILD_PID_FILE" ]]; then
    start_ngrok
    ngrok_unhealthy_count=0
    return
  fi

  if ngrok_is_healthy; then
    ngrok_unhealthy_count=0
    clear_alert_marker_if_stable
    return
  fi

  ngrok_unhealthy_count=$((ngrok_unhealthy_count + 1))
  if (( ngrok_unhealthy_count >= 2 )); then
    log "ngrok health check failed twice; restarting it."
    stop_child "$NGROK_CHILD_PID_FILE" "ngrok tunnel"
    start_ngrok
    ngrok_unhealthy_count=0
  fi
}

write_status_summary() {
  local portal_child_pid ngrok_child_pid caffeinate_pid portal_health ngrok_health alert_state
  portal_child_pid="$(read_pid_file "$PORTAL_CHILD_PID_FILE" || true)"
  ngrok_child_pid="$(read_pid_file "$NGROK_CHILD_PID_FILE" || true)"
  caffeinate_pid="$(read_pid_file "$CAFFEINATE_PID_FILE" || true)"
  if portal_is_healthy; then
    portal_health="healthy"
  else
    portal_health="unhealthy"
  fi
  if ngrok_is_healthy; then
    ngrok_health="healthy"
  else
    ngrok_health="unhealthy"
  fi
  if [[ -f "$ALERT_FILE" ]]; then
    alert_state="present"
  else
    alert_state="none"
  fi

  cat >"$STATUS_FILE" <<EOF
{"state":"running","updated_at":"$(date '+%Y-%m-%d %H:%M:%S')","guard_pid":$$,"portal_child_pid":${portal_child_pid:-null},"ngrok_child_pid":${ngrok_child_pid:-null},"caffeinate_pid":${caffeinate_pid:-null},"portal_health":"$(json_escape "$portal_health")","ngrok_health":"$(json_escape "$ngrok_health")","alert_state":"$(json_escape "$alert_state")","public_url":"$(json_escape "$PUBLIC_URL")","probe_url":"$(json_escape "http://$PROBE_HOST:$PORT/healthz")"}
EOF
}

log "Team stack guard started."
start_caffeinate

while true; do
  ensure_portal_running
  if portal_is_healthy; then
    ensure_ngrok_running
  fi
  write_status_summary

  sleep "$CHECK_INTERVAL"
done
