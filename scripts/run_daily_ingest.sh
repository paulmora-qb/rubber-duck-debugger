#!/bin/bash
# Runs the RDD data ingestion pipelines independently.
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
CRON_BRANCH="main"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

mkdir -p "$LOG_DIR"

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }

log() {
  echo "[$(ts)] $*" | tee -a "$LOG_FILE"
}

# Parallel arrays collecting results for the email report.
PIPELINE_NAMES=()
PIPELINE_STATUSES=()
PIPELINE_LOG_FILES=()

run_pipeline() {
  local pipeline="$1"
  local pipeline_log="$LOG_DIR/${pipeline}_last.log"

  PIPELINE_NAMES+=("$pipeline")
  PIPELINE_LOG_FILES+=("$pipeline_log")

  if $DRY_RUN; then
    echo "[DRY-RUN] uv run kedro run --pipeline $pipeline"
    PIPELINE_STATUSES+=("skip")
    return 0
  fi

  local registered
  registered=$(cd "$PROJECT_ROOT" && uv run kedro pipeline list 2>/dev/null || true)

  if ! echo "$registered" | grep -qx "$pipeline"; then
    log "[SKIP] Pipeline '$pipeline' not registered — skipping"
    PIPELINE_STATUSES+=("skip")
    return 0
  fi

  log "[START] $pipeline"
  local exit_code=0
  (cd "$PROJECT_ROOT" && uv run kedro run --pipeline "$pipeline") > "$pipeline_log" 2>&1 || exit_code=$?
  cat "$pipeline_log" >> "$LOG_FILE"

  if [[ $exit_code -eq 0 ]]; then
    log "[OK]    $pipeline"
    PIPELINE_STATUSES+=("ok")
  else
    log "[FAIL]  $pipeline (exit $exit_code)"
    PIPELINE_STATUSES+=("fail")
  fi
}

send_report() {
  local args=()
  for i in "${!PIPELINE_NAMES[@]}"; do
    args+=("${PIPELINE_NAMES[$i]}" "${PIPELINE_STATUSES[$i]}" "${PIPELINE_LOG_FILES[$i]}")
  done
  log "[EMAIL] sending report..."
  (cd "$PROJECT_ROOT" && uv run python scripts/send_report.py "${args[@]}") >> "$LOG_FILE" 2>&1 || \
    log "[EMAIL] failed to send report (check SMTP credentials in .env)"
}

sync_branch() {
  log "[GIT] syncing to $CRON_BRANCH..."
  git -C "$PROJECT_ROOT" fetch origin "$CRON_BRANCH" >> "$LOG_FILE" 2>&1
  git -C "$PROJECT_ROOT" checkout "$CRON_BRANCH" >> "$LOG_FILE" 2>&1
  git -C "$PROJECT_ROOT" pull --ff-only origin "$CRON_BRANCH" >> "$LOG_FILE" 2>&1
  log "[GIT] HEAD=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD)"
}

main() {
  if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.env"
    set +a
  fi

  log "=== daily ingest start ==="
  sync_branch

  run_pipeline data_ingestion
  run_pipeline finnhub_news
  run_pipeline newsapi_news

  if ! $DRY_RUN; then
    send_report
  fi

  log "=== [DONE] ==="
}

main "$@"
