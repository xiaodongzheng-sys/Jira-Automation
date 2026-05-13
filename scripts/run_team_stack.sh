#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
GUARD_DAEMON_SCRIPT="$ROOT_DIR/scripts/run_team_stack_guard_daemon.sh"

usage() {
  echo "Usage: $0 {start|stop|restart|restart-guard|restart-portal|status|logs|doctor|release-status} [--caffeinate|--no-caffeinate]"
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
  echo
  echo "== Cloudflare Tunnel =="
  "$ROOT_DIR/scripts/run_cloudflare_tunnel.sh" logs || true
}

launchd_domain_label() {
  local launchd_label="${TEAM_STACK_LAUNCHD_LABEL:-io.npt.jira-creation-stack}"
  printf 'gui/%s/%s\n' "$(id -u)" "$launchd_label"
}

launchd_stack_installed() {
  command -v launchctl >/dev/null 2>&1 && launchctl print "$(launchd_domain_label)" >/dev/null 2>&1
}

public_url_reachable() {
  local url="$1"
  local probe_url="${url%/}/healthz"
  if curl -fsS --max-time 10 "$probe_url" >/dev/null 2>&1; then
    return 0
  fi

  if [[ "$probe_url" =~ ^https://([^/:]+)(/.*)?$ ]] && command -v dig >/dev/null 2>&1; then
    local host="${BASH_REMATCH[1]}"
    local ip
    ip="$(dig +short A "$host" @1.1.1.1 | head -n 1)"
    if [[ -n "$ip" ]]; then
      curl -fsS --resolve "$host:443:$ip" --max-time 10 "$probe_url" >/dev/null 2>&1
      return
    fi
  fi

  return 1
}

doctor() {
  local env_file="$ENV_FILE"
  local data_dir="${TEAM_PORTAL_DATA_DIR:-}"
  local port="${TEAM_PORTAL_PORT:-}"
  local public_url="${TEAM_PORTAL_BASE_URL:-}"
  local tunnel_provider="${TEAM_PORTAL_TUNNEL_PROVIDER:-}"
  local launchd_label="${TEAM_STACK_LAUNCHD_LABEL:-io.npt.jira-creation-stack}"
  local status_file
  local alert_file
  local ok=0
  local guard_ok=1
  local portal_ok=1
  local ngrok_ok=1
  local public_ok=1
  local expected_revision
  local served_revision=""
  local recommended_root

  local resolved
  resolved="$(read_env_values TEAM_PORTAL_DATA_DIR TEAM_PORTAL_PORT TEAM_PORTAL_BASE_URL TEAM_PORTAL_TUNNEL_PROVIDER)"
  if [[ -z "$data_dir" ]]; then
    data_dir="$(printf '%s' "$resolved" | sed -n '1p')"
  fi
  if [[ -z "$port" ]]; then
    port="$(printf '%s' "$resolved" | sed -n '2p')"
  fi
  if [[ -z "$public_url" ]]; then
    public_url="$(printf '%s' "$resolved" | sed -n '3p')"
  fi
  if [[ -z "$tunnel_provider" ]]; then
    tunnel_provider="$(printf '%s' "$resolved" | sed -n '4p')"
  fi

  data_dir="$(resolve_team_data_dir "$data_dir")"
  port="${port:-5000}"
  tunnel_provider="${tunnel_provider:-ngrok}"
  expected_revision="$(current_release_revision)"
  recommended_root="$(recommended_team_stack_root)"
  status_file="$data_dir/run/team_stack_status.json"
  alert_file="$data_dir/run/team_stack_alert.json"

  echo "== Doctor =="
  echo "Repo root: $ROOT_DIR"
  if is_protected_mac_path "$ROOT_DIR"; then
    echo "Repo path: protected macOS folder"
    echo "Recommended host workspace: $recommended_root"
    ok=1
  else
    echo "Repo path: launchd-friendly"
  fi
  echo "Env file: $env_file"
  echo "Data dir: $data_dir"
  echo "Port: $port"
  echo "Public URL: ${public_url:-<missing>}"
  echo "Tunnel provider: $tunnel_provider"
  echo "Expected revision: $expected_revision"
  echo

  echo "== launchd =="
  if launchctl print "gui/$(id -u)/$launchd_label" >/dev/null 2>&1; then
    echo "launchd job installed: $launchd_label"
  else
    echo "launchd job not installed: $launchd_label"
    if is_protected_mac_path "$ROOT_DIR"; then
      echo "Recommended fix: ./scripts/setup_team_stack_host_workspace.sh"
    else
      echo "Recommended fix: ./scripts/install_team_stack_launchd.sh"
    fi
    ok=1
  fi
  echo

  echo "== Guard Status =="
  "$GUARD_DAEMON_SCRIPT" status || guard_ok=0
  if (( guard_ok == 0 )); then
    ok=1
  fi
  echo

  echo "== Healthz =="
  if curl -fsS --max-time 5 "http://127.0.0.1:$port/healthz"; then
    served_revision="$(fetch_healthz_field "127.0.0.1" "$port" revision 2>/dev/null || true)"
    echo
    if [[ -n "$served_revision" ]]; then
      echo "served revision: $served_revision"
      if [[ "$served_revision" != "$expected_revision" ]]; then
        echo "healthz revision mismatch: served revision does not match working tree"
        portal_ok=0
        ok=1
      fi
    else
      echo "healthz revision missing"
      portal_ok=0
      ok=1
    fi
  else
    echo "healthz check failed"
    portal_ok=0
    ok=1
  fi
  echo

  echo "== Tunnel =="
  if [[ "$tunnel_provider" == "cloudflare" ]]; then
    if pgrep -f "cloudflared .*tunnel .*run" >/dev/null 2>&1; then
      echo "Cloudflare Tunnel process running"
    else
      echo "Cloudflare Tunnel process not found"
      ngrok_ok=0
      ok=1
    fi
  elif curl -fsS --max-time 5 "http://127.0.0.1:4040/api/tunnels" >/dev/null; then
    echo "ngrok inspector reachable"
  else
    echo "ngrok inspector unreachable"
    ngrok_ok=0
    ok=1
  fi
  echo

  echo "== Public URL =="
  if [[ -n "$public_url" ]]; then
    if public_url_reachable "$public_url"; then
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

  release_status
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
tunnel_health = str(payload.get("tunnel_health") or payload.get("ngrok_health") or "")

if not guard_ok and state == "running":
    messages.append("status summary is stale: file says running but guard is not running")
if guard_ok and state == "stopped":
    messages.append("status summary is stale: file says stopped but guard is running")
if state == "stopped" and (portal_ok or ngrok_ok or public_ok):
    messages.append("status summary is stale: file says stopped but live probes still respond")
if portal_ok and portal_health == "unhealthy":
    messages.append("status summary is stale: portal probe is healthy but file says unhealthy")
if ngrok_ok and tunnel_health == "unhealthy":
    messages.append("status summary is stale: tunnel probe is healthy but file says unhealthy")
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

  echo
  echo "== Source Code QA Ops Summary =="
  if [[ -x "$PYTHON_BIN" && -f "$ROOT_DIR/scripts/source_code_qa_ops_summary.py" ]]; then
    if ! PYTHONPATH="$ROOT_DIR" TEAM_PORTAL_DATA_DIR="$data_dir" "$PYTHON_BIN" "$ROOT_DIR/scripts/source_code_qa_ops_summary.py" --strict; then
      ok=1
    fi
  else
    echo "ops summary unavailable"
  fi

  return "$ok"
}

release_status() {
  if [[ -x "$PYTHON_BIN" && -f "$ROOT_DIR/scripts/release_status.py" ]]; then
    "$PYTHON_BIN" "$ROOT_DIR/scripts/release_status.py" || true
  else
    echo "== Release Status =="
    echo "release status unavailable"
  fi
}

start() {
  local guard_env="$1"
  env "$guard_env" "$GUARD_DAEMON_SCRIPT" start
}

stop() {
  "$GUARD_DAEMON_SCRIPT" stop || true
  "$ROOT_DIR/scripts/run_ngrok_tunnel.sh" stop || true
  "$ROOT_DIR/scripts/run_cloudflare_tunnel.sh" stop || true
  "$ROOT_DIR/scripts/run_team_portal_prod.sh" stop || true
}

status() {
  "$GUARD_DAEMON_SCRIPT" status || true
  "$ROOT_DIR/scripts/run_team_portal_prod.sh" status || true
  "$ROOT_DIR/scripts/run_ngrok_tunnel.sh" status || true
  "$ROOT_DIR/scripts/run_cloudflare_tunnel.sh" status || true
}

restart_local_agent_if_needed() {
  local bpmis_call_mode="${BPMIS_CALL_MODE:-$(read_env_value BPMIS_CALL_MODE)}"
  if [[ "$bpmis_call_mode" != "local_agent" ]]; then
    return 0
  fi
  if [[ ! -x "$ROOT_DIR/scripts/run_local_agent.sh" ]]; then
    echo "BPMIS_CALL_MODE=local_agent but scripts/run_local_agent.sh is not executable."
    return 1
  fi
  assert_no_active_meeting_recording_before_local_agent_restart "restart Mac local-agent for team stack" \
    "${LOCAL_AGENT_TEAM_PORTAL_DATA_DIR:-${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}}"
  echo "BPMIS_CALL_MODE=local_agent; restarting Mac local-agent to avoid stale BPMIS proxy code."
  "$ROOT_DIR/scripts/run_local_agent.sh" restart
}

restart() {
  local guard_env="$1"
  restart_local_agent_if_needed
  restart_guard "$guard_env"
}

restart_guard() {
  local guard_env="$1"
  if launchd_stack_installed; then
    local domain_label
    domain_label="$(launchd_domain_label)"
    echo "launchd job installed; restarting team stack with launchctl kickstart -k $domain_label"
    launchctl kickstart -k "$domain_label"
    sleep 3
    return 0
  fi
  stop
  start "$guard_env"
}

restart_portal() {
  echo "Restarting team portal only; tunnel, guard, and local-agent are left running."
  "$ROOT_DIR/scripts/run_team_portal_prod.sh" restart
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
  restart-guard) restart_guard "$GUARD_ENV" ;;
  restart-portal) restart_portal ;;
  status) status ;;
  release-status) release_status ;;
  logs) logs ;;
  doctor) doctor ;;
  *)
    usage
    exit 1
    ;;
esac
