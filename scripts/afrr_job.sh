#!/bin/bash

set -euo pipefail

export PATH="$HOME/.local/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${AFRR_DATA_PATH:-${HOME}/tennet-afrr-data}"
LOG_DIR="${DATA_DIR}/logs"
UV_CACHE_DIR="${DATA_DIR}/.uv-cache"

# Robustly find uv
UV_BIN="$(command -v uv || true)"
LOCK_FILE="${REPO_DIR}/.afrr_fetch.lock"
LOG_FILE="${LOG_DIR}/afrr_fetch.log"

mkdir -p "$LOG_DIR" "$UV_CACHE_DIR"

# Check for uv
if [[ -z "$UV_BIN" || ! -x "$UV_BIN" ]]; then
    echo "ERROR: 'uv' binary not found. PATH is: $PATH" | tee -a "$LOG_FILE"
    exit 1
fi

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2" | tee -a "$LOG_FILE"
}

trap 'log ERROR "Script failed on line $LINENO. Exit code: $?"' ERR

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    log INFO "Another run is active, skipping this trigger"
    exit 0
fi

log INFO "Starting AFRR fetch job"
cd "$REPO_DIR"

# Git operations
git fetch origin main
git switch main >/dev/null 2>&1 || git switch -c main --track origin/main
git pull --ff-only origin main

# Run the python script
AFRR_DATA_PATH="$DATA_DIR" UV_CACHE_DIR="$UV_CACHE_DIR" \
    "$UV_BIN" run --frozen affr_data_fetcher.py

# Cleanup old logs
find "$LOG_DIR" -type f -name "*.log*" -mtime +5 -delete
log INFO "AFRR fetch job completed"
