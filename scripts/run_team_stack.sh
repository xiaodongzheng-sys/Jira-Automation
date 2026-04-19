#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
GUARD_DAEMON_SCRIPT="$ROOT_DIR/scripts/run_team_stack_guard_daemon.sh"

usage() {
  echo "Usage: $0 {start|stop|restart|status|logs|doctor} [--caffeinate|--no-caffeinate]"
}

resolve_guard_env() {
  local mode="${1:-auto}"
  case "$mode" in
    auto)
      if command -v caffeinate >/dev/null 2>&1; then
        echo "TEAM_STACK_USE_CAFFEINATE=1"
      else
        echo "TEAM_STACK_USE_CAFFEINATE=0"
      fi
      ;;
    caffeinate)
      echo "TEAM_STACK_USE_CAFFEINATE=1"
      ;;
    no-caffeinate)
      echo "TEAM_STACK_USE_CAFFEINATE=0"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

logs() {
  echo "== Stack Guard =="
  "$GUARD_DAEMON_SCRIPT" logs || true
  echo
  echo "== Team Portal =="
  "$ROOT_DIR/scripts/run_team_portal_prod.sh" logs || true
  echo
  echo "== ngrok Tunnel =="
  "$ROOT_DIR/scripts/run_ngrok_tunnel.sh" logs || true
}

doctor() {
  local env_file="$ENV_FILE"
  local data_dir="${TEAM_PORTAL_DATA_DIR:-}"
  local port="${TEAM_PORTAL_PORT:-}"
  local public_url="${TEAM_PORTAL_BASE_URL:-}"
  local status_file
  local alert_file
  local ok=0
  local guard_ok=1
  local portal_ok=1
  local ngrok_ok=1
  local public_ok=1

  local resolved
  resolved="$(read_env_values TEAM_PORTAL_DATA_DIR TEAM_PORTAL_PORT TEAM_PORTAL_BASE_URL)"
  if [[ -z "$data_dir" ]]; then
    data_dir="$(printf '%s' "$resolved" | sed -n '1p')"
  fi
  if [[ -z "$port" ]]; then
    port="$(printf '%s' "$resolved" | sed -n '2p')"
  fi
  if [[ -z "$public_url" ]]; then
    public_url="$(printf '%s' "$resolved" | sed -n '3p')"
  fi

  data_dir="$(resolve_team_data_dir "$data_dir")"
  port="${port:-5000}"
  status_file="$data_dir/run/team_stack_status.json"
  alert_file="$data_dir/run/team_stack_alert.json"

  echo "== Doctor =="
  echo "Env file: $env_file"
  echo "Data dir: $data_dir"
  echo "Port: $port"
  echo "Public URL: ${public_url:-<missing>}"
  echo

  echo "== Guard Status =="
  "$GUARD_DAEMON_SCRIPT" status || guard_ok=0
  if (( guard_ok == 0 )); then
    ok=1
  fi
  echo

  echo "== Healthz =="
  if curl -fsS --max-time 5 "http://127.0.0.1:$port/healthz"; then
    echo
  else
    echo "healthz check failed"
    portal_ok=0
    ok=1
  fi
  echo

  echo "== ngrok API =="
  if curl -fsS --max-time 5 "http://127.0.0.1:4040/api/tunnels" >/dev/null; then
    echo "ngrok inspector reachable"
  else
    echo "ngrok inspector unreachable"
    ngrok_ok=0
    ok=1
  fi
  echo

  echo "== Public URL =="
  if [[ -n "$public_url" ]]; then
    if curl -I --max-time 10 "$public_url" >/dev/null 2>&1; then
      echo "public URL reachable"
    else
      echo "public URL check failed"
      public_ok=0
      ok=1
    fi
  else
    echo "TEAM_PORTAL_BASE_URL missing"
    public_ok=0
    ok=1
  fi
  echo

  echo "== Status Files =="
  if [[ -f "$status_file" ]]; then
    echo "status summary present: $status_file"
    cat "$status_file"
    echo
    if [[ -x "$PYTHON_BIN" ]]; then
      local status_analysis
      status_analysis="$(
        STATUS_FILE="$status_file" \
        GUARD_OK="$guard_ok" \
        PORTAL_OK="$portal_ok" \
        NGROK_OK="$ngrok_ok" \
        PUBLIC_OK="$public_ok" \
        "$PYTHON_BIN" - <<'PY'
import json
import os
import time

status_file = os.environ["STATUS_FILE"]
guard_ok = os.environ.get("GUARD_OK") == "1"
portal_ok = os.environ.get("PORTAL_OK") == "1"
ngrok_ok = os.environ.get("NGROK_OK") == "1"
public_ok = os.environ.get("PUBLIC_OK") == "1"

with open(status_file, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

messages = []
updated_unix = payload.get("updated_unix")
if isinstance(updated_unix, int):
    age = int(time.time()) - updated_unix
    messages.append(f"status summary age: {age}s")
    if age > 120:
        messages.append("status summary is older than 120s")

state = str(payload.get("state") or "")
portal_health = str(payload.get("portal_health") or "")
ngrok_health = str(payload.get("ngrok_health") or "")

if not guard_ok and state == "running":
    messages.append("status summary is stale: file says running but guard is not running")
if guard_ok and state == "stopped":
    messages.append("status summary is stale: file says stopped but guard is running")
if state == "stopped" and (portal_ok or ngrok_ok or public_ok):
    messages.append("status summary is stale: file says stopped but live probes still respond")
if portal_ok and portal_health == "unhealthy":
    messages.append("status summary is stale: portal probe is healthy but file says unhealthy")
if ngrok_ok and ngrok_health == "unhealthy":
    messages.append("status summary is stale: ngrok probe is healthy but file says unhealthy")
if public_ok and not payload.get("public_url"):
    messages.append("status summary is incomplete: public_url missing while public probe passed")

for message in messages:
    print(message)
PY
      )"
      if [[ -n "$status_analysis" ]]; then
        while IFS= read -r line; do
          [[ -n "$line" ]] || continue
          echo "$line"
          if [[ "$line" == status\ summary\ is\ stale:* || "$line" == status\ summary\ is\ older\ than* ]]; then
            ok=1
          fi
        done <<<"$status_analysis"
      fi
    fi
  else
    echo "status summary missing: $status_file"
    ok=1
  fi
  if [[ -f "$alert_file" ]]; then
    echo "alert marker present: $alert_file"
    cat "$alert_file"
    echo
  else
    echo "alert marker not present"
  fi

  return "$ok"
}

start() {
  local guard_env="$1"
  env "$guard_env" "$GUARD_DAEMON_SCRIPT" start
}

stop() {
  "$GUARD_DAEMON_SCRIPT" stop || true
  "$ROOT_DIR/scripts/run_ngrok_tunnel.sh" stop || true
  "$ROOT_DIR/scripts/run_team_portal_prod.sh" stop || true
}

status() {
  "$GUARD_DAEMON_SCRIPT" status || true
  "$ROOT_DIR/scripts/run_team_portal_prod.sh" status || true
  "$ROOT_DIR/scripts/run_ngrok_tunnel.sh" status || true
}

restart() {
  local guard_env="$1"
  stop
  start "$guard_env"
}

ACTION="${1:-start}"
MODE="auto"

case "${2:-}" in
  --caffeinate) MODE="caffeinate" ;;
  --no-caffeinate) MODE="no-caffeinate" ;;
  "") ;;
  *)
    usage
    exit 1
    ;;
esac

GUARD_ENV="$(resolve_guard_env "$MODE")"

case "$ACTION" in
  start) start "$GUARD_ENV" ;;
  stop) stop ;;
  restart) restart "$GUARD_ENV" ;;
  status) status ;;
  logs) logs ;;
  doctor) doctor ;;
  *)
    usage
    exit 1
    ;;
esac
