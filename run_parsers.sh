#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"

LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

# Optional proxy settings (export before running):
#   PROXY_URL=http://user:pass@host:port
# or
#   PROXY_HOST=host PROXY_PORT=port PROXY_USER=user PROXY_PASS=pass

if [ -n "${PROXY_URL:-}" ]; then
    export HTTP_PROXY="$PROXY_URL"
    export HTTPS_PROXY="$PROXY_URL"
elif [ -n "${PROXY_HOST:-}" ] && [ -n "${PROXY_PORT:-}" ]; then
    if [ -n "${PROXY_USER:-}" ] && [ -n "${PROXY_PASS:-}" ]; then
        export HTTP_PROXY="http://${PROXY_USER}:${PROXY_PASS}@${PROXY_HOST}:${PROXY_PORT}"
        export HTTPS_PROXY="$HTTP_PROXY"
    else
        export HTTP_PROXY="http://${PROXY_HOST}:${PROXY_PORT}"
        export HTTPS_PROXY="$HTTP_PROXY"
    fi
fi

TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
SCORE_TIMEOUT_SECONDS="${SCORE_TIMEOUT_SECONDS:-120}"

TIMEOUT_BIN="timeout"
if ! command -v "$TIMEOUT_BIN" >/dev/null 2>&1; then
    if command -v gtimeout >/dev/null 2>&1; then
        TIMEOUT_BIN="gtimeout"
    else
        TIMEOUT_BIN=""
    fi
fi

start_ts="$(date +"%Y-%m-%d %H:%M:%S")"
echo "[$start_ts] Run parsers: start" | tee -a "$LOG_DIR/run_parsers.log"

run_cmd() {
    local name="$1"
    local script="$2"
    local log="$3"
    local timeout_sec="${4:-$TIMEOUT_SECONDS}"
    local rc

    if [ -n "$TIMEOUT_BIN" ]; then
        "$TIMEOUT_BIN" "$timeout_sec" "$VENV_PY" "$script" | tee -a "$log"
    else
        echo "[WARN] timeout not found; running without timeout" | tee -a "$LOG_DIR/run_parsers.log"
        "$VENV_PY" "$script" | tee -a "$log"
    fi
    rc="${PIPESTATUS[0]}"
    if [ "$rc" -ne 0 ]; then
        echo "[WARN] $name exited with code $rc" | tee -a "$LOG_DIR/run_parsers.log"
    fi
    return "$rc"
}

set +e
SKIP_SCORE_UPDATE=1 run_cmd "dota" "$ROOT_DIR/main.py" "$LOG_DIR/dota.run.log"
SKIP_SCORE_UPDATE=1 run_cmd "cs2" "$ROOT_DIR/cs2_main.py" "$LOG_DIR/cs2.run.log"

run_cmd "dota_score" "$ROOT_DIR/dota_scores.py" "$LOG_DIR/dota.score.log" "$SCORE_TIMEOUT_SECONDS"
run_cmd "cs2_score" "$ROOT_DIR/cs2_scores.py" "$LOG_DIR/cs2.score.log" "$SCORE_TIMEOUT_SECONDS"
set -e

end_ts="$(date +"%Y-%m-%d %H:%M:%S")"
echo "[$end_ts] Run parsers: done" | tee -a "$LOG_DIR/run_parsers.log"
