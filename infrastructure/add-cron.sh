#!/usr/bin/env bash
# Idempotent cron registration for weekly data collection.
# Run on the always-on EC2 (micro instance).
#
# Usage: bash infrastructure/add-cron.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CRON_CMD="cd $SCRIPT_DIR && source .venv/bin/activate && python weekly_collector.py >> /var/log/weekly-collector.log 2>&1"
CRON_SCHEDULE="0 1 * * 6"  # Saturday 01:00 UTC (Fri 9pm ET, after extended hours close)

# Remove any existing entry for this script
(crontab -l 2>/dev/null || true) | grep -v "weekly_collector.py" | crontab -

# Add the new entry
(crontab -l 2>/dev/null || true; echo "$CRON_SCHEDULE $CRON_CMD") | crontab -

echo "Cron registered: $CRON_SCHEDULE"
echo "Logs: /var/log/weekly-collector.log"
crontab -l | grep weekly_collector
