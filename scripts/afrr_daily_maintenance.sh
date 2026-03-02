#!/bin/bash

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${AFRR_DATA_PATH:-${HOME}/tennet-afrr-data}"
LOG_DIR="${DATA_DIR}/logs"
BACKUP_DIR="${DATA_DIR}/backups"
UV_CACHE_DIR="${DATA_DIR}/.uv-cache"
UV_BIN="$(command -v uv || true)"
LOG_FILE="${LOG_DIR}/afrr_maintenance.log"
GITHUB_REPO="${GITHUB_REPO:-}"

mkdir -p "$LOG_DIR" "$BACKUP_DIR" "$UV_CACHE_DIR"
[[ -n "$UV_BIN" && -x "$UV_BIN" ]] || { echo "uv binary not found"; exit 1; }
cd "$REPO_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2" | tee -a "$LOG_FILE"
}

log INFO "Starting daily maintenance"

AFRR_DATA_PATH="$DATA_DIR" UV_CACHE_DIR="$UV_CACHE_DIR" \
    "$UV_BIN" run --frozen python scripts/delta_maintenance.py | tee -a "$LOG_FILE"

if [[ ! -d "${DATA_DIR}/delta" ]]; then
    log INFO "No Delta directory, skipping backup"
    exit 0
fi

timestamp="$(date -u '+%Y%m%dT%H%M%SZ')"
archive_path="${BACKUP_DIR}/afrr_delta_${timestamp}.tar.zst"
tar --zstd -C "$DATA_DIR" -cf "$archive_path" delta

find "$BACKUP_DIR" -type f -name "afrr_delta_*.tar.zst" -mtime +14 -delete

if [[ -n "$GITHUB_REPO" ]]; then
    if ! command -v gh >/dev/null 2>&1; then
        log ERROR "GITHUB_REPO is set but gh CLI is not installed"
        exit 1
    fi

    GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}" \
        GITHUB_REPO="$GITHUB_REPO" \
        GITHUB_RELEASE_TAG="${GITHUB_RELEASE_TAG:-afrr-delta-backups}" \
        GITHUB_KEEP_ASSETS="${GITHUB_KEEP_ASSETS:-14}" \
        /bin/bash scripts/upload_backup_to_github.sh "$archive_path" | tee -a "$LOG_FILE"
fi

log INFO "Daily maintenance completed"
