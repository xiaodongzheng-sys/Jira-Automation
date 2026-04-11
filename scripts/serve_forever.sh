#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5000}"

cd "$ROOT_DIR"
exec "$ROOT_DIR/.venv/bin/python" -m flask --app app run --host "$HOST" --port "$PORT"
