#!/bin/bash
# Runs the portfolio_performance pipeline every Friday — computes metrics and
# sends the weekly performance email.
# Sources .env and syncs to main before running.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/weekly_performance.log"

export PATH="/Users/Paul_Mora/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

mkdir -p "$LOG_DIR"

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }
log() { echo "[$(ts)] $*" | tee -a "$LOG_FILE"; }

_on_exit() {
  local rc=$?
  [[ $rc -eq 0 ]] && return
  log "[ALERT] script failed (exit $rc) — sending failure alert..."
  if [[ -f "$PROJECT_ROOT/.env" ]] && [[ -z "${RDD_SMTP_USER:-}" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.env"
    set +a
  fi
  (cd "$PROJECT_ROOT" && uv run python scripts/send_alert.py \
    --subject "[RDD] run_weekly_performance.sh FAILED (exit $rc)" \
    --log "$LOG_FILE") >> "$LOG_FILE" 2>&1 || true
}
trap '_on_exit' EXIT

main() {
  if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.env"
    set +a
  fi

  log "=== weekly performance run start ==="
  git -C "$PROJECT_ROOT" fetch origin main >> "$LOG_FILE" 2>&1
  git -C "$PROJECT_ROOT" checkout -f main >> "$LOG_FILE" 2>&1
  git -C "$PROJECT_ROOT" reset --hard origin/main >> "$LOG_FILE" 2>&1
  log "[GIT] HEAD=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD)"

  (cd "$PROJECT_ROOT" && uv run kedro run --pipeline portfolio_performance) >> "$LOG_FILE" 2>&1
  log "=== [DONE] ==="
}

main "$@"
