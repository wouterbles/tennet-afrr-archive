#!/bin/bash

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${AFRR_DATA_PATH:-${HOME}/tennet-afrr-data}"
LOG_DIR="${DATA_DIR}/logs"
UV_CACHE_DIR="${DATA_DIR}/.uv-cache"
UV_BIN="$(command -v uv || true)"
LOCK_FILE="${REPO_DIR}/.afrr_fetch.lock"
LOG_FILE="${LOG_DIR}/afrr_fetch.log"

mkdir -p "$LOG_DIR" "$UV_CACHE_DIR"
[[ -n "$UV_BIN" && -x "$UV_BIN" ]] || { echo "uv binary not found"; exit 1; }

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

git fetch origin main
git switch main >/dev/null 2>&1 || git switch -c main --track origin/main
git pull --ff-only origin main

AFRR_DATA_PATH="$DATA_DIR" UV_CACHE_DIR="$UV_CACHE_DIR" \
    "$UV_BIN" run --frozen affr_data_fetcher.py

find "$LOG_DIR" -type f -name "*.log*" -mtime +5 -delete
log INFO "AFRR fetch job completed"
