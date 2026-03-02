#!/bin/bash

set -euo pipefail

ARCHIVE_PATH="${1:-}"
[[ -n "$ARCHIVE_PATH" ]] || { echo "archive path is required"; exit 1; }
[[ -f "$ARCHIVE_PATH" ]] || { echo "archive not found: $ARCHIVE_PATH"; exit 1; }

GITHUB_REPO="${GITHUB_REPO:-}"
GITHUB_RELEASE_TAG="${GITHUB_RELEASE_TAG:-afrr-delta-backups}"
GITHUB_KEEP_ASSETS="${GITHUB_KEEP_ASSETS:-14}"
GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"

[[ -n "$GITHUB_REPO" ]] || { echo "GITHUB_REPO is required"; exit 1; }
command -v gh >/dev/null 2>&1 || { echo "gh CLI not found"; exit 1; }

if [[ -z "$GH_TOKEN" ]] && ! gh auth status >/dev/null 2>&1; then
    echo "GitHub auth missing: set GH_TOKEN/GITHUB_TOKEN or run 'gh auth login'"
    exit 1
fi

if ! gh release view "$GITHUB_RELEASE_TAG" --repo "$GITHUB_REPO" >/dev/null 2>&1; then
    gh release create "$GITHUB_RELEASE_TAG" \
        --repo "$GITHUB_REPO" \
        --title "AFRR Delta Backups" \
        --notes "Automated Delta backup assets." >/dev/null
fi

gh release upload "$GITHUB_RELEASE_TAG" "$ARCHIVE_PATH" --repo "$GITHUB_REPO" >/dev/null

backup_assets=()
while IFS= read -r line; do
    backup_assets+=("$line")
done < <(
    gh release view "$GITHUB_RELEASE_TAG" \
        --repo "$GITHUB_REPO" \
        --json assets \
        --jq '.assets[].name' \
        | grep '^afrr_delta_.*\.tar\.zst$' \
        | sort -r
)

if (( ${#backup_assets[@]} > GITHUB_KEEP_ASSETS )); then
    for ((i=GITHUB_KEEP_ASSETS; i<${#backup_assets[@]}; i++)); do
        gh release delete-asset "$GITHUB_RELEASE_TAG" "${backup_assets[$i]}" \
            --repo "$GITHUB_REPO" \
            -y >/dev/null
    done
fi

echo "Uploaded $(basename "$ARCHIVE_PATH") to $GITHUB_REPO release $GITHUB_RELEASE_TAG"
