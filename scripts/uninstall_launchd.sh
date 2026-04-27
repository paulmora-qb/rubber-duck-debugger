#!/bin/bash
# Uninstalls the RDD launchd agent.

set -euo pipefail

LABEL="com.rdd.daily-ingest"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

launchctl unload "$PLIST" 2>/dev/null || true
rm -f "$PLIST"
echo "Removed launchd agent: $LABEL"
