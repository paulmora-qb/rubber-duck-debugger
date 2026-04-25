#!/bin/bash
# Installs the RDD daily ingest cron entry at 10:00 UTC Mon–Fri.
# Idempotent — safe to run multiple times; replaces any existing rdd-managed entry.
# TZ=UTC is written into the crontab so the 10AM trigger is timezone-independent.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RUNNER="$SCRIPT_DIR/run_daily_ingest.sh"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/cron.log"
MARKER="# rdd-daily-ingest"

mkdir -p "$LOG_DIR"

CURRENT=$(crontab -l 2>/dev/null || true)

CLEANED=$(echo "$CURRENT" | grep -v "$MARKER" | grep -v "run_daily_ingest" || true)

NEW_ENTRY="TZ=UTC
0 10 * * 1-5 /bin/bash $RUNNER >> $LOG_FILE 2>&1 $MARKER"

printf '%s\n%s\n' "$CLEANED" "$NEW_ENTRY" | grep -v '^$' | crontab -

echo "Installed. Current crontab:"
crontab -l
