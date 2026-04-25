#!/bin/bash
# Removes the RDD daily ingest cron entry. Does not touch other crontab entries.

set -euo pipefail

MARKER="# rdd-daily-ingest"

CURRENT=$(crontab -l 2>/dev/null || true)

if ! echo "$CURRENT" | grep -q "$MARKER"; then
  echo "No rdd-managed cron entry found — nothing to remove."
  exit 0
fi

CLEANED=$(echo "$CURRENT" | grep -v "$MARKER" | grep -v "run_daily_ingest" | grep -v "^TZ=UTC$" || true)

if [[ -z "$CLEANED" ]]; then
  crontab -r
else
  echo "$CLEANED" | crontab -
fi

echo "Removed. Current crontab:"
crontab -l 2>/dev/null || echo "(empty)"
