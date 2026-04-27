#!/bin/bash
# Installs the RDD daily ingest as a launchd agent at 10:00 local, Mon–Fri.
# Unlike cron, launchd catches up missed runs when the machine wakes up.
# Idempotent — safe to run multiple times.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RUNNER="$SCRIPT_DIR/run_daily_ingest.sh"
LOG_DIR="$PROJECT_ROOT/logs"
LABEL="com.rdd.daily-ingest"
PLIST_DEST="$HOME/Library/LaunchAgents/$LABEL.plist"

mkdir -p "$LOG_DIR"

# Remove any existing cron entry for this job.
CURRENT=$(crontab -l 2>/dev/null || true)
CLEANED=$(echo "$CURRENT" | grep -v "rdd-daily-ingest" | grep -v "run_daily_ingest" || true)
if [[ "$CLEANED" != "$CURRENT" ]]; then
  echo "$CLEANED" | grep -v '^$' | crontab - 2>/dev/null || crontab -r 2>/dev/null || true
  echo "Removed existing cron entry."
fi

# Generate the plist with resolved absolute paths.
cat > "$PLIST_DEST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$LABEL</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$RUNNER</string>
    </array>

    <!-- 10:00 local, weekdays only -->
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
        <key>PATH</key>
        <string>/Users/Paul_Mora/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>HOME</key>
        <string>$HOME</string>
    </dict>

    <key>StandardOutPath</key>
    <string>$LOG_DIR/launchd.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/launchd.log</string>
</dict>
</plist>
PLIST

# Reload the agent.
launchctl unload "$PLIST_DEST" 2>/dev/null || true
launchctl load "$PLIST_DEST"

echo "Installed launchd agent: $LABEL"
echo "Runs Mon–Fri at 10:00 local. Catches up missed runs on wake."
echo "Plist: $PLIST_DEST"
