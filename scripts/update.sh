#!/usr/bin/env bash
# update.sh — Pull latest code and reinstall pymyx on the server.
#
# Run from the project root on the server:
#   bash scripts/update.sh
#
# Or from anywhere:
#   /path/to/pymyx/scripts/update.sh

set -euo pipefail

PYMYX_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(timestamp)] update START"
echo "Project: $PYMYX_ROOT"

cd "$PYMYX_ROOT"

# 1. Pull latest code
echo ""
echo ">>> git pull"
git pull

# 2. Reinstall package (picks up any new dependencies or entry points)
echo ""
echo ">>> pip install -e ."
pip install -e . --quiet

# 3. Verify
echo ""
echo ">>> pymyx --help"
pymyx --help | head -5

echo ""
echo "[$(timestamp)] update DONE"
