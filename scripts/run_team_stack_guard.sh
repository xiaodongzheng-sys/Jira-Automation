#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
export_env_file
PORTAL_FOREGROUND_SCRIPT="$ROOT_DIR/scripts/run_team_portal_foreground.sh"
NGROK_FOREGROUND_SCRIPT="$ROOT_DIR/scripts/run_ngrok_tunnel_foreground.sh"
DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
PORT="${TEAM_PORTAL_PORT:-$(read_env_value TEAM_PORTAL_PORT)}"
PORT="${PORT:-5000}"
PROBE_HOST="${TEAM_PORTAL_PROBE_HOST:-127.0.0.1}"
PUBLIC_URL="${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}"
EXPECTED_REVISION="$(current_release_revision)"

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
SOURCE_QA_EVAL_STATUS_FILE="$DATA_DIR/run/source_code_qa_eval_status.json"
SOURCE_QA_EVAL_LAST_RUN_FILE="$DATA_DIR/run/source_code_qa_eval_last_run"
SOURCE_QA_EVAL_LOG_FILE="$DATA_DIR/logs/source_code_qa_nightly_eval.log"
CHECK_INTERVAL="${TEAM_STACK_GUARD_INTERVAL_SECONDS:-15}"
START_READY_TIMEOUT_SECONDS="${TEAM_STACK_START_READY_TIMEOUT_SECONDS:-12}"
USE_CAFFEINATE="${TEAM_STACK_USE_CAFFEINATE:-auto}"
RESTART_WINDOW_SECONDS="${TEAM_STACK_RESTART_WINDOW_SECONDS:-60}"
MAX_RESTART_BACKOFF_SECONDS="${TEAM_STACK_MAX_RESTART_BACKOFF_SECONDS:-30}"
RESTART_ALERT_THRESHOLD="${TEAM_STACK_RESTART_ALERT_THRESHOLD:-3}"
SOURCE_QA_EVAL_ENABLED="${SOURCE_CODE_QA_NIGHTLY_EVAL_ENABLED:-1}"
SOURCE_QA_EVAL_INTERVAL_SECONDS="${SOURCE_CODE_QA_NIGHTLY_EVAL_INTERVAL_SECONDS:-86400}"
SOURCE_QA_EVAL_MIN_START_DELAY_SECONDS="${SOURCE_CODE_QA_NIGHTLY_EVAL_MIN_START_DELAY_SECONDS:-60}"

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
{"state":"stopped","updated_at":"$(date '+%Y-%m-%d %H:%M:%S')","updated_unix":$(date +%s),"guard_pid":null,"portal_child_pid":null,"ngrok_child_pid":null,"caffeinate_pid":null,"portal_health":"unknown","ngrok_health":"unknown","alert_state":"none","public_url":"$(json_escape "$PUBLIC_URL")","probe_url":"$(json_escape "http://$PROBE_HOST:$PORT/healthz")"}
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

source_qa_eval_enabled() {
  case "$SOURCE_QA_EVAL_ENABLED" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
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
  portal_revision_matches "$PROBE_HOST" "$PORT" "$EXPECTED_REVISION"
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
  env "TEAM_PORTAL_RELEASE_REVISION=$EXPECTED_REVISION" "$PORTAL_FOREGROUND_SCRIPT" >>"$PORTAL_LOG_FILE" 2>&1 &
  echo $! >"$PORTAL_CHILD_PID_FILE"
  log "Portal launched (pid $(cat "$PORTAL_CHILD_PID_FILE"))."
  local _attempt
  for ((_attempt=0; _attempt<START_READY_TIMEOUT_SECONDS; _attempt++)); do
    if portal_is_healthy; then
      log "Portal became healthy."
      return
    fi
    sleep 1
  done
  log "Portal did not become healthy within ${START_READY_TIMEOUT_SECONDS}s."
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
  local _attempt
  for ((_attempt=0; _attempt<START_READY_TIMEOUT_SECONDS; _attempt++)); do
    if ngrok_is_healthy; then
      log "ngrok became healthy."
      return
    fi
    sleep 1
  done
  log "ngrok did not become healthy within ${START_READY_TIMEOUT_SECONDS}s."
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
{"state":"running","updated_at":"$(date '+%Y-%m-%d %H:%M:%S')","updated_unix":$(date +%s),"guard_pid":$$,"portal_child_pid":${portal_child_pid:-null},"ngrok_child_pid":${ngrok_child_pid:-null},"caffeinate_pid":${caffeinate_pid:-null},"portal_health":"$(json_escape "$portal_health")","ngrok_health":"$(json_escape "$ngrok_health")","alert_state":"$(json_escape "$alert_state")","public_url":"$(json_escape "$PUBLIC_URL")","probe_url":"$(json_escape "http://$PROBE_HOST:$PORT/healthz")"}
EOF
}

read_source_qa_eval_last_run() {
  if [[ ! -f "$SOURCE_QA_EVAL_LAST_RUN_FILE" ]]; then
    printf '0\n'
    return
  fi
  local raw
  raw="$(cat "$SOURCE_QA_EVAL_LAST_RUN_FILE" 2>/dev/null || true)"
  if [[ "$raw" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "$raw"
  else
    printf '0\n'
  fi
}

write_source_qa_eval_status() {
  local state="$1"
  local message="$2"
  local exit_code="${3:-0}"
  cat >"$SOURCE_QA_EVAL_STATUS_FILE" <<EOF
{"state":"$(json_escape "$state")","updated_at":"$(date '+%Y-%m-%d %H:%M:%S')","updated_unix":$(date +%s),"exit_code":$exit_code,"interval_seconds":$SOURCE_QA_EVAL_INTERVAL_SECONDS,"log_file":"$(json_escape "$SOURCE_QA_EVAL_LOG_FILE")","message":"$(json_escape "$message")"}
EOF
}

run_source_qa_eval_if_due() {
  if ! source_qa_eval_enabled; then
    return 0
  fi
  if ! portal_is_healthy; then
    return 0
  fi

  local now
  now="$(date +%s)"
  if (( portal_last_start_at > 0 && now - portal_last_start_at < SOURCE_QA_EVAL_MIN_START_DELAY_SECONDS )); then
    return 0
  fi

  local last_run
  last_run="$(read_source_qa_eval_last_run)"
  if (( now - last_run < SOURCE_QA_EVAL_INTERVAL_SECONDS )); then
    return 0
  fi

  echo "$now" >"$SOURCE_QA_EVAL_LAST_RUN_FILE"
  write_source_qa_eval_status "running" "Source Code QA nightly eval started."
  log "Source Code QA nightly eval started."
  touch "$SOURCE_QA_EVAL_LOG_FILE"
  if PYTHONPATH="$ROOT_DIR" TEAM_PORTAL_DATA_DIR="$DATA_DIR" "$PYTHON_BIN" "$ROOT_DIR/scripts/run_source_code_qa_nightly_eval.py" --include-useful-feedback --json >>"$SOURCE_QA_EVAL_LOG_FILE" 2>&1; then
    write_source_qa_eval_status "passed" "Source Code QA nightly eval passed."
    log "Source Code QA nightly eval passed."
  else
    local exit_code=$?
    write_source_qa_eval_status "failed" "Source Code QA nightly eval failed. Check the eval log." "$exit_code"
    log "Source Code QA nightly eval failed (exit $exit_code)."
  fi
}

log "Team stack guard started."
start_caffeinate

while true; do
  ensure_portal_running
  if portal_is_healthy; then
    ensure_ngrok_running
    run_source_qa_eval_if_due
  fi
  write_status_summary

  sleep "$CHECK_INTERVAL"
done
