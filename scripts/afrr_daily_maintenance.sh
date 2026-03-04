#!/bin/bash

set -euo pipefail

export PATH="$HOME/.local/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${AFRR_DATA_PATH:-${HOME}/tennet-afrr-data}"
LOG_DIR="${DATA_DIR}/logs"
BACKUP_DIR="${DATA_DIR}/backups"
UV_CACHE_DIR="${DATA_DIR}/.uv-cache"
UV_BIN="$(command -v uv || true)"
LOG_FILE="${LOG_DIR}/afrr_maintenance.log"
GITHUB_REPO="${GITHUB_REPO:-}"

mkdir -p "$LOG_DIR" "$BACKUP_DIR" "$UV_CACHE_DIR"

if [[ -z "$UV_BIN" || ! -x "$UV_BIN" ]]; then
    echo "ERROR: 'uv' binary not found. PATH is: $PATH" | tee -a "$LOG_FILE"
    exit 1
fi

cd "$REPO_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2" | tee -a "$LOG_FILE"
}

log INFO "Starting daily maintenance"

# 1. Run the Python Delta Maintenance
AFRR_DATA_PATH="$DATA_DIR" UV_CACHE_DIR="$UV_CACHE_DIR" \
    "$UV_BIN" run --frozen python scripts/delta_maintenance.py | tee -a "$LOG_FILE"

if [[ ! -d "${DATA_DIR}/delta" ]]; then
    log INFO "No Delta directory, skipping backup"
    exit 0
fi

# 2. Create the Archive
timestamp="$(date -u '+%Y%m%dT%H%M%SZ')"
archive_path="${BACKUP_DIR}/afrr_delta_${timestamp}.tar.zst"
tar --zstd -C "$DATA_DIR" -cf "$archive_path" delta

# 3. Upload to GitHub (if REPO is set)
if [[ -n "$GITHUB_REPO" ]]; then
    if ! command -v gh >/dev/null 2>&1; then
        log ERROR "GITHUB_REPO is set but 'gh' CLI is not found in PATH"
        exit 1
    fi
    
    # Note: Ensure 'scripts/upload_backup_to_github.sh' is executable!
    chmod +x scripts/upload_backup_to_github.sh

    GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}" \
        GITHUB_REPO="$GITHUB_REPO" \
        GITHUB_RELEASE_TAG="${GITHUB_RELEASE_TAG:-afrr-delta-backups}" \
        GITHUB_KEEP_ASSETS="${GITHUB_KEEP_ASSETS:-14}" \
        /bin/bash scripts/upload_backup_to_github.sh "$archive_path" | tee -a "$LOG_FILE"
fi

rm -f "$archive_path"

log INFO "Daily maintenance completed"
