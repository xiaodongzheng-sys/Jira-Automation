#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

logs() {
  echo "== Team Portal =="
  "$ROOT_DIR/scripts/run_team_portal.sh" logs || true
}

start() {
  "$ROOT_DIR/scripts/run_team_portal.sh" start
}

stop() {
  "$ROOT_DIR/scripts/run_team_portal.sh" stop || true
}

status() {
  "$ROOT_DIR/scripts/run_team_portal.sh" status || true
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
