#!/usr/bin/env bash
# hourly_sync.sh — Run the full pymyx pipeline incrementally every hour.
#
# Uses --last to detect the delta between input (00_raw) and output (10_parsed).
# If no new data is found, exits cleanly with code 0.
#
# Install in crontab:
#   crontab -e
#   0 * * * * /home/jbaudry/Documents/2026/CLAUDE/PYMYX/scripts/hourly_sync.sh >> /var/log/pymyx_hourly.log 2>&1
#
# Or with systemd timer (see hourly_sync.service / hourly_sync.timer)

set -euo pipefail

PYMYX_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOGFILE="${PYMYX_ROOT}/pymyx_hourly.log"
FLOW="valvometry_daily"

cd "$PYMYX_ROOT"

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "--- [$(timestamp)] hourly_sync START ---" >> "$LOGFILE"

# Run the full pipeline in incremental mode
# --last: computes time range from max(output_ts) to max(input_ts)
# If already up-to-date, flow exits gracefully (exit 0)
python -m pymyx.core.flow --flow "$FLOW" --last >> "$LOGFILE" 2>&1
EXIT_CODE=$?

echo "--- [$(timestamp)] hourly_sync END (exit=$EXIT_CODE) ---" >> "$LOGFILE"
exit $EXIT_CODE
