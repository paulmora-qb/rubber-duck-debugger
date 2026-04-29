#!/bin/bash
# Runs the weekly GenAI news analysis pipeline.
# Intended to be called from cron once a week (e.g. every Sunday morning).
#
# Usage:
#   ./run_weekly_analysis.sh            # normal run
#   ./run_weekly_analysis.sh --dry-run  # print commands without executing

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/weekly_analysis.log"
DRY_RUN=false
CRON_BRANCH="main"

export PATH="/Users/Paul_Mora/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

mkdir -p "$LOG_DIR"

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "[$(ts)] $*" | tee -a "$LOG_FILE"; }

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

  log "=== weekly analysis start ==="

  if $DRY_RUN; then
    echo "[DRY-RUN] uv run kedro run --pipeline news_analysis"
    log "=== [DONE] ==="
    return 0
  fi

  sync_branch

  log "[START] news_analysis"
  local exit_code=0
  (cd "$PROJECT_ROOT" && uv run kedro run --pipeline news_analysis) >> "$LOG_FILE" 2>&1 || exit_code=$?

  if [[ $exit_code -eq 0 ]]; then
    log "[OK]    news_analysis"
  else
    log "[FAIL]  news_analysis (exit $exit_code)"
  fi

  log "=== [DONE] ==="
}

main "$@"
