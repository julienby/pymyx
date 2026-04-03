#!/bin/bash
# run_flow_hourly.sh — Run a pyperun flow every hour
#
# Usage:
#   ./run_flow_hourly.sh <flow-name> [interval-seconds]
#
# Examples:
#   ./run_flow_hourly.sh expo_pre_grace_2_streaming
#   ./run_flow_hourly.sh expo_pre_grace_2_streaming 1800   # every 30 min

FLOW="${1:?Usage: $0 <flow-name> [interval-seconds]}"
INTERVAL="${2:-3600}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYPERUN_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PYPERUN_DIR/logs"
LOG_FILE="$LOG_DIR/${FLOW}.log"

mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "Starting hourly runner: flow=$FLOW interval=${INTERVAL}s"
log "Logs: $LOG_FILE"

while true; do
    log "--- Run start ---"
    if pyperun flow "$FLOW" --last >> "$LOG_FILE" 2>&1; then
        log "--- Run OK ---"
    else
        log "--- Run FAILED (exit $?) ---"
    fi
    log "Next run in ${INTERVAL}s"
    sleep "$INTERVAL"
done
