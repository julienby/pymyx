#!/usr/bin/env bash
# hourly_sync.sh — Run the full pyperun pipeline incrementally every hour.
#
# Uses --last to detect the delta between input (00_raw) and output (10_parsed).
# If no new data is found, exits cleanly with code 0.
#
# Install in crontab:
#   crontab -e
#   0 * * * * /path/to/pyperun/scripts/hourly_sync.sh >> /var/log/pyperun_hourly.log 2>&1
#
# Or with systemd timer (see hourly_sync.service / hourly_sync.timer)

set -euo pipefail

PYPERUN_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOGFILE="${PYPERUN_ROOT}/logs/pyperun_hourly.log"
FLOW="${1:-my-flow}"  # Pass flow name as argument or set here

cd "$PYPERUN_ROOT"
mkdir -p "$(dirname "$LOGFILE")"

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "--- [$(timestamp)] hourly_sync START ---" >> "$LOGFILE"

# Run the full pipeline in incremental mode
# --last: computes time range from max(output_ts) to max(input_ts)
# If already up-to-date, flow exits gracefully (exit 0)
python -m pyperun.core.flow --flow "$FLOW" --last >> "$LOGFILE" 2>&1
EXIT_CODE=$?

echo "--- [$(timestamp)] hourly_sync END (exit=$EXIT_CODE) ---" >> "$LOGFILE"
exit $EXIT_CODE
