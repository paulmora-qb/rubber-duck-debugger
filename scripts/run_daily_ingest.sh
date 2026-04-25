#!/bin/bash
# Runs the RDD data ingestion pipelines sequentially.
# Safe to call from cron — sets PATH explicitly and sources .env from the project root.
#
# Usage:
#   ./run_daily_ingest.sh            # normal run
#   ./run_daily_ingest.sh --dry-run  # print commands without executing
#
# Log rotation (optional):
#   Add to crontab: 0 9 * * 0 /usr/local/bin/logrotate /path/to/scripts/logrotate.conf
#   Or use macOS launchd with a weekly StartCalendarInterval trigger.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/daily_ingest.log"
DRY_RUN=false

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

mkdir -p "$LOG_DIR"
# OmegaConf globals resolver requires conf/local to exist even when empty.
mkdir -p "$PROJECT_ROOT/conf/local"

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }

log() {
  echo "[$(ts)] $*" | tee -a "$LOG_FILE"
}

run_pipeline() {
  local pipeline="$1"

  if $DRY_RUN; then
    echo "[DRY-RUN] uv run kedro run --pipeline $pipeline"
    return 0
  fi

  local registered
  registered=$(cd "$PROJECT_ROOT" && uv run kedro pipeline list 2>/dev/null || true)

  if ! echo "$registered" | grep -qx "$pipeline"; then
    log "[SKIP] Pipeline '$pipeline' not registered — skipping"
    return 0
  fi

  log "[START] $pipeline"
  (cd "$PROJECT_ROOT" && uv run kedro run --pipeline "$pipeline") >> "$LOG_FILE" 2>&1
  log "[OK]    $pipeline"
}

main() {
  if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.env"
    set +a
  fi

  log "=== daily ingest start ==="

  run_pipeline data_ingestion
  run_pipeline finnhub_news
  run_pipeline newsapi_news

  log "=== [DONE] ==="
}

main "$@"
