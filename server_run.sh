#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
PY="$VENV_DIR/bin/python"

# Proxy (optional)
# export PROXY_HOST=45.112.192.138
# export PROXY_PORT=3128
# export PROXY_USER=dxkmARZPoEJH
# export PROXY_PASS=ImZOQGTbRPjr

# Intervals for scheduler (optional)
# export DOTA_INTERVAL_SECONDS=600
# export CS2_INTERVAL_SECONDS=600

if [ ! -x "$PY" ]; then
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --prefer-binary -r "$ROOT_DIR/requirements.txt"
fi

"$PY" "$ROOT_DIR/scheduler.py"
