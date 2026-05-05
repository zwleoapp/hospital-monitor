#!/usr/bin/env bash
# run_backtest.sh — Manual backtest: re-evolve model + recompute backtest columns.
#
# Run this after any model change (formula tweak, alpha grid update, new features):
#   bash run_backtest.sh
#
# What it does:
#   1. evolve_model.py  — re-fits Ridge (GCV alpha) + damping per hospital,
#                         writes updated model_config.json
#   2. backtest_model.py — recomputes backtest_* columns on all forecast_audit.csv rows
#                          using the just-written model, writes the file in-place
#   3. backtest_model.py --audit — prints live vs backtest accuracy per hospital/segment

set -euo pipefail

REPO="$(cd "$(dirname "$0")" && pwd)"
SCRIPTS="$REPO/scripts"
PYTHON=python3

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) run_backtest START ==="

echo ""
echo "--- Step 1: evolve_model (GCV + damping, with segment audit)"
$PYTHON "$SCRIPTS/evolve_model.py" --audit

echo ""
echo "--- Step 2: backtest_model (recompute backtest columns)"
$PYTHON "$SCRIPTS/backtest_model.py"

echo ""
echo "--- Step 3: backtest audit (live vs current model)"
$PYTHON "$SCRIPTS/backtest_model.py" --audit --dry-run

echo ""
echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) run_backtest END ==="
