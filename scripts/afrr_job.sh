#!/bin/bash

# Configuration
REPO_DIR="${HOME}/tennet-afrr-archive"
LOG_DIR="${REPO_DIR}/logs"
BASH_LOG="${LOG_DIR}/afrr_fetch.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Set error handling
set -e

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2" | tee -a "$BASH_LOG"
}

trap 'log ERROR "Script failed on line $LINENO. Exit code: $?"' ERR

# Verify repository exists
[[ -d "$REPO_DIR" ]] || { log ERROR "Repository not found: $REPO_DIR"; exit 1; }

log "INFO" "Starting AFRR data fetch script"
cd "$REPO_DIR"

# Update repository
log "INFO" "Updating repository"
git fetch origin main
git switch -C main origin/main

# Run data fetcher
log "INFO" "Fetching AFRR data..."
$HOME/.local/bin/uv run --frozen affr_data_fetcher.py

# Commit changes if any
if ! git diff --quiet HEAD -- data/ || ! git diff --staged --quiet; then
    log "INFO" "Committing changes..."
    git config user.name "github-actions[bot]"
    git config user.email "github-actions[bot]@users.noreply.github.com"
    git add data/
    git commit -m "Update AFRR data $(date '+%Y-%m-%d %H:%M:%S') (UTC)"
    git push origin main
    log "INFO" "Changes pushed successfully"
else
    log "INFO" "No changes to commit"
fi

# Cleanup logs older than 5 days
find "$LOG_DIR" -type f -name "*.log*" -mtime +5 -delete

log "INFO" "Script completed successfully"