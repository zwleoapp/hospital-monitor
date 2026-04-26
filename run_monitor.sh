#!/usr/bin/env bash
# run_monitor.sh — Full pipeline: Scraper → Silver → Publisher
# Cadence: every 15 min via systemd timer (hospital_monitor.timer)
# Operational hours: 07:00–23:00 Melbourne local (publish_latest.py enforces this)
#
# Each stage writes its own status and logs independently so a failure in one
# stage does not corrupt the outputs of a previous stage.

set -euo pipefail

REPO=/home/pi-zwapp/hospital-monitor
SCRIPTS=$REPO/scripts
LOG_DIR=/var/log/hospital-monitor
PYTHON=python3

# ── Logging ────────────────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
LOG=$LOG_DIR/run_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) run_monitor START ==="

# ── Step 1: Scraper (Bronze append) ───────────────────────────────────────────
echo "--- Step 1: hospital_monitor (scrape)"
$PYTHON "$SCRIPTS/hospital_monitor.py"

# ── Step 2: Silver transform ───────────────────────────────────────────────────
echo "--- Step 2: transform_silver"
$PYTHON "$SCRIPTS/transform_silver.py"

# ── Step 3: Publish + push ─────────────────────────────────────────────────────
# publish_latest.py enforces the 07:00–23:00 operational hours gate internally.
# Outside those hours it logs "Trial Mode: Sleeping" and exits 0 cleanly.
echo "--- Step 3: publish_latest (--push)"
$PYTHON "$SCRIPTS/publish_latest.py" --push

# ── Rotate logs: keep last 7 days ──────────────────────────────────────────────
find "$LOG_DIR" -name "run_*.log" -mtime +7 -delete

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) run_monitor END ==="
