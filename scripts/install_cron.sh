#!/bin/bash
# Installs the RDD daily ingest cron entry at 10:00 UTC Mon–Fri.
# Idempotent — safe to run multiple times; replaces any existing rdd-managed entry.
# TZ=UTC is written into the crontab so the 10AM trigger is timezone-independent.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RUNNER="$SCRIPT_DIR/run_daily_ingest.sh"
MONTHLY_RUNNER="$SCRIPT_DIR/run_monthly_news_analysis.sh"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/cron.log"
MONTHLY_LOG="$LOG_DIR/monthly_news_analysis.log"
MARKER_DAILY="# rdd-daily-ingest"
MARKER_MONTHLY="# rdd-monthly-news-analysis"

mkdir -p "$LOG_DIR"

CURRENT=$(crontab -l 2>/dev/null || true)

CLEANED=$(echo "$CURRENT" \
  | grep -v "^TZ=" \
  | grep -v "$MARKER_DAILY" \
  | grep -v "run_daily_ingest" \
  | grep -v "$MARKER_MONTHLY" \
  | grep -v "run_monthly_news_analysis" \
  || true)

NEW_ENTRIES="TZ=UTC
0 10 * * 1-5 /bin/bash $RUNNER >> $LOG_FILE 2>&1 $MARKER_DAILY
0 10 26 * * /bin/bash $MONTHLY_RUNNER >> $MONTHLY_LOG 2>&1 $MARKER_MONTHLY"

printf '%s\n%s\n' "$CLEANED" "$NEW_ENTRIES" | grep -v '^$' | crontab -

echo "Installed. Current crontab:"
crontab -l
