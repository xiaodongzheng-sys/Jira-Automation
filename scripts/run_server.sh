#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="$ROOT_DIR/.venv/bin/python"
SERVER_WRAPPER="$ROOT_DIR/scripts/serve_forever.sh"
TMP_DIR="$ROOT_DIR/tmp"
PID_FILE="$TMP_DIR/flask.pid"
LOG_FILE="$TMP_DIR/flask.log"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5000}"
LABEL="io.codex.bpmis-jira-tool"
PLIST_FILE="$HOME/Library/LaunchAgents/$LABEL.plist"

mkdir -p "$TMP_DIR" "$HOME/Library/LaunchAgents"

find_server_pid() {
  lsof -tiTCP:"$PORT" -sTCP:LISTEN -n -P 2>/dev/null | head -n 1
}

is_running() {
  local live_pid
  live_pid="$(find_server_pid || true)"
  if [[ -n "${live_pid:-}" ]]; then
    echo "$live_pid" >"$PID_FILE"
    return 0
  fi
  return 1
}

write_plist() {
  cat >"$PLIST_FILE" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>$LABEL</string>
    <key>WorkingDirectory</key>
    <string>$ROOT_DIR</string>
    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>$SERVER_WRAPPER</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
      <key>PATH</key>
      <string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
      <key>HOST</key>
      <string>$HOST</string>
      <key>PORT</key>
      <string>$PORT</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_FILE</string>
    <key>StandardErrorPath</key>
    <string>$LOG_FILE</string>
  </dict>
</plist>
EOF
}

bootout_if_loaded() {
  launchctl bootout "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
}

start() {
  if is_running; then
    echo "Server already running on http://$HOST:$PORT (pid $(cat "$PID_FILE"))."
    return 0
  fi

  if [[ ! -x "$VENV_PYTHON" ]]; then
    echo "Missing virtualenv python at $VENV_PYTHON"
    return 1
  fi

  : >"$LOG_FILE"
  write_plist
  bootout_if_loaded
  launchctl bootstrap "gui/$(id -u)" "$PLIST_FILE"
  launchctl kickstart -k "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true

  for _ in {1..20}; do
    if is_running && curl -fsS "http://$HOST:$PORT/" >/dev/null 2>&1; then
      echo "Server started at http://$HOST:$PORT"
      echo "PID: $(cat "$PID_FILE")"
      echo "Log: $LOG_FILE"
      return 0
    fi
    sleep 1
  done

  echo "Server did not become ready in time."
  echo "Recent log output:"
  tail -n 40 "$LOG_FILE" || true
  return 1
}

stop() {
  bootout_if_loaded
  rm -f "$PID_FILE"
  echo "Server stopped."
}

status() {
  if is_running; then
    echo "Server running on http://$HOST:$PORT (pid $(cat "$PID_FILE"))."
  else
    echo "Server is not running."
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
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    restart
    ;;
  status)
    status
    ;;
  logs)
    logs
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs}"
    exit 1
    ;;
esac
