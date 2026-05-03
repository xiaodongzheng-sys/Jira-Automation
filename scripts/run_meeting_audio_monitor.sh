#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
RUN_DIR="${ROOT_DIR}/.team-portal/run"
LOG_DIR="${ROOT_DIR}/.team-portal/logs"
PID_FILE="${RUN_DIR}/meeting_audio_monitor.pid"
LOG_FILE="${LOG_DIR}/meeting_audio_monitor.log"
FFMPEG_BIN="${MEETING_RECORDER_FFMPEG_BIN:-/opt/homebrew/bin/ffmpeg}"
INPUT_DEVICE="${MEETING_AUDIO_MONITOR_INPUT:-BlackHole 2ch}"
OUTPUT_DEVICE_INDEX="${MEETING_AUDIO_MONITOR_OUTPUT_INDEX:-2}"

mkdir -p "${RUN_DIR}" "${LOG_DIR}"

is_running() {
  [[ -f "${PID_FILE}" ]] || return 1
  local pid
  pid="$(cat "${PID_FILE}")"
  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" 2>/dev/null
}

start_monitor() {
  if is_running; then
    echo "meeting audio monitor already running: $(cat "${PID_FILE}")"
    return 0
  fi
  : > "${LOG_FILE}"
  nohup "${FFMPEG_BIN}" \
    -hide_banner \
    -nostdin \
    -loglevel warning \
    -f lavfi \
    -i "anullsrc=r=48000:cl=stereo" \
    -f avfoundation \
    -i ":${INPUT_DEVICE}" \
    -filter_complex "[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0" \
    -ac 2 \
    -ar 48000 \
    -audio_device_index "${OUTPUT_DEVICE_INDEX}" \
    -f audiotoolbox - \
    > "${LOG_FILE}" 2>&1 &
  echo "$!" > "${PID_FILE}"
  echo "meeting audio monitor started: $(cat "${PID_FILE}")"
}

stop_monitor() {
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    pkill -P "${pid}" 2>/dev/null || true
    kill "${pid}" 2>/dev/null || true
    rm -f "${PID_FILE}"
    echo "meeting audio monitor stopped"
  else
    rm -f "${PID_FILE}"
    echo "meeting audio monitor is not running"
  fi
}

status_monitor() {
  if is_running; then
    echo "meeting audio monitor running: $(cat "${PID_FILE}")"
  else
    echo "meeting audio monitor is not running"
    return 1
  fi
}

case "${1:-start}" in
  start) start_monitor ;;
  stop) stop_monitor ;;
  restart)
    stop_monitor
    start_monitor
    ;;
  status) status_monitor ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}" >&2
    exit 2
    ;;
esac
