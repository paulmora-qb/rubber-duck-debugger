#!/bin/bash
# Installs all RDD launchd agents. Idempotent — safe to run multiple times.
#
# Agents installed:
#   com.rdd.daily-ingest           — Mon–Fri 10:00 local  (data ingestion)
#   com.rdd.weekly-performance     — every Friday 12:00 local  (price_strategies rebalance + performance email)
#   com.rdd.monthly-strategy       — 1st of each month 12:00 local  (ai_fundamental_screen)
#   com.rdd.weekly-news-analysis   — every Friday 12:00 local  (news analysis reports)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"

export PATH="/Users/Paul_Mora/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

mkdir -p "$LOG_DIR"

install_agent() {
  local label="$1"
  local runner="$2"
  local plist_body="$3"
  local plist_dest="$HOME/Library/LaunchAgents/${label}.plist"

  echo "$plist_body" > "$plist_dest"
  launchctl unload "$plist_dest" 2>/dev/null || true
  launchctl load "$plist_dest"
  echo "Installed: $label  →  $plist_dest"
}

# ── 1. Daily ingest — Mon–Fri 10:00 ──────────────────────────────────────────

install_agent "com.rdd.daily-ingest" "$SCRIPT_DIR/run_daily_ingest.sh" \
"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
    <key>Label</key><string>com.rdd.daily-ingest</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/run_daily_ingest.sh</string>
    </array>
    <!-- Mon–Fri 10:00 local -->
    <key>StartCalendarInterval</key>
    <array>
        <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>10</integer><key>Minute</key><integer>0</integer></dict>
        <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>10</integer><key>Minute</key><integer>0</integer></dict>
        <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>10</integer><key>Minute</key><integer>0</integer></dict>
        <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>10</integer><key>Minute</key><integer>0</integer></dict>
        <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>10</integer><key>Minute</key><integer>0</integer></dict>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key><string>/Users/Paul_Mora/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>HOME</key><string>$HOME</string>
    </dict>
    <key>StandardOutPath</key><string>$LOG_DIR/launchd.log</string>
    <key>StandardErrorPath</key><string>$LOG_DIR/launchd.log</string>
</dict>
</plist>"

# ── 2. Monthly strategy — 1st 12:00 ──────────────────────────────────────────

install_agent "com.rdd.monthly-strategy" "$SCRIPT_DIR/run_monthly_strategy.sh" \
"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
    <key>Label</key><string>com.rdd.monthly-strategy</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/run_monthly_strategy.sh</string>
    </array>
    <!-- 1st of each month at 12:00 local -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Day</key><integer>1</integer>
        <key>Hour</key><integer>12</integer>
        <key>Minute</key><integer>0</integer>
    </dict>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key><string>/Users/Paul_Mora/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>HOME</key><string>$HOME</string>
    </dict>
    <key>StandardOutPath</key><string>$LOG_DIR/monthly_strategy.log</string>
    <key>StandardErrorPath</key><string>$LOG_DIR/monthly_strategy.log</string>
</dict>
</plist>"

# ── 3. Weekly performance email — Friday 12:00 ───────────────────────────────

install_agent "com.rdd.weekly-performance" "$SCRIPT_DIR/run_weekly_performance.sh" \
"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
    <key>Label</key><string>com.rdd.weekly-performance</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/run_weekly_performance.sh</string>
    </array>
    <!-- Every Friday at 12:00 local -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Weekday</key><integer>5</integer>
        <key>Hour</key><integer>12</integer>
        <key>Minute</key><integer>0</integer>
    </dict>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key><string>/Users/Paul_Mora/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>HOME</key><string>$HOME</string>
    </dict>
    <key>StandardOutPath</key><string>$LOG_DIR/weekly_performance.log</string>
    <key>StandardErrorPath</key><string>$LOG_DIR/weekly_performance.log</string>
</dict>
</plist>"

# ── 4. Weekly news analysis — every Friday 12:00 ─────────────────────────────

install_agent "com.rdd.weekly-news-analysis" "$SCRIPT_DIR/run_weekly_news_analysis.sh" \
"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
    <key>Label</key><string>com.rdd.weekly-news-analysis</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/run_weekly_news_analysis.sh</string>
    </array>
    <!-- Every Friday at 12:00 local -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Weekday</key><integer>5</integer>
        <key>Hour</key><integer>12</integer>
        <key>Minute</key><integer>0</integer>
    </dict>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key><string>/Users/Paul_Mora/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>HOME</key><string>$HOME</string>
    </dict>
    <key>StandardOutPath</key><string>$LOG_DIR/weekly_news_analysis.log</string>
    <key>StandardErrorPath</key><string>$LOG_DIR/weekly_news_analysis.log</string>
</dict>
</plist>"

echo ""
echo "All agents installed. Schedule summary:"
echo "  Daily ingest        → Mon–Fri 10:00 local"
echo "  Monthly strategy    → 1st of each month 12:00 local"
echo "  Weekly performance  → every Friday 12:00 local (price_strategies rebalance + portfolio_performance)"
echo "  Weekly news analysis → every Friday 12:00 local"
