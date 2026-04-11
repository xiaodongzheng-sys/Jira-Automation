#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$ROOT_DIR"
./scripts/run_team_stack.sh start
sleep 1
open "http://127.0.0.1:5000"
