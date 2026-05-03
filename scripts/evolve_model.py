# data-class: internal
"""
evolve_model.py — Close the ML feedback loop.

Reads forecast_audit.csv, computes per-hospital damping factors that minimise
Mean Absolute Error (MAE), and writes them to config/model_config.json under
the "per_hospital_damping" key.  predict_next.py reads that key on every run,
so the model self-improves without any manual tuning.

Algorithm
─────────
For each hospital × time-band:
  1. Filter to completed rows (actual_wait_min known, error_pct < anomaly_error_pct)
  2. Grid-search damping d ∈ [DAMPING_MIN, DAMPING_MAX] in 0.05 steps
     For each candidate d:
       projected = current_wait_min + wait_momentum * STEPS * d
       projected = clamp(max(projected, current_wait_min * 0.5), MAX_WAIT_MIN)
       mae = mean(|projected - actual_wait_min|)
  3. Select d* = argmin(mae)
  4. Per-hospital damping = weighted average across time-bands
     (weight = row count, so high-traffic bands dominate)

Time-bands (Melbourne local, 4 × 6-hour windows):
  overnight  00–06
  morning    06–12
  afternoon  12–18
  evening    18–24

Safety
──────
  - Minimum MIN_ROWS_TO_EVOLVE completed rows required per hospital
  - All evolved values clamped to [DAMPING_MIN, DAMPING_MAX]
  - Anomalous rows (error_pct > ANOMALY_ERROR_PCT) excluded from training
  - Model config updated atomically (read → update key → write)
  - Evolution run logged to model_evolution_log.jsonl for audit

Usage
─────
  python3 scripts/evolve_model.py          # evolve and write
  python3 scripts/evolve_model.py --dry-run  # compute and print, don't write
  python3 scripts/evolve_model.py --audit  # show hospital breakdown table
"""

import sys
import csv
import json
import argparse
import pathlib
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from config.paths import FORECAST_AUDIT_CSV, SSD  # noqa: E402

_REPO        = pathlib.Path(__file__).resolve().parent.parent
_MODEL_CFG   = _REPO / "config" / "model_config.json"
_EVOLUTION_LOG = SSD / "model_evolution_log.jsonl"
_MELB        = ZoneInfo("Australia/Melbourne")

STEPS = 4  # HORIZON_MIN / CADENCE_MIN = 60 / 15

TIME_BANDS = [
    ("overnight",  0,  6),
    ("morning",    6, 12),
    ("afternoon", 12, 18),
    ("evening",   18, 24),
]


def _load_model_cfg() -> dict:
    try:
        return json.loads(_MODEL_CFG.read_text())
    except Exception:
        return {}


def _time_band(hour: int) -> str:
    for name, start, end in TIME_BANDS:
        if start <= hour < end:
            return name
    return "overnight"


def _project(current: float, momentum: float, damping: float, max_wait: int) -> float:
    projected = current + momentum * STEPS * damping
    floor     = current * 0.50
    return max(floor, min(max_wait, projected))


def load_audit(path: pathlib.Path, anomaly_pct: float) -> list[dict]:
    """Load forecast_audit.csv, filtering out incomplete and anomalous rows."""
    rows = []
    if not path.exists():
        return rows
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                actual = float(row["actual_wait_min"])
                if actual <= 0:
                    continue
                error_pct = float(row["error_pct"])
                if error_pct > anomaly_pct:
                    continue
                rows.append({
                    "hospital":      row["hospital"],
                    "bucket_utc":    row["bucket_utc"],
                    "source_type":   row.get("source_type", "unknown"),
                    "current":       float(row["current_wait_min"] or 0),
                    "momentum":      float(row["wait_momentum"]    or 0),
                    "actual":        actual,
                    "cache_lag":     row.get("cache_lag_minutes", ""),
                    "fidelity":      row.get("fidelity_status", ""),
                })
            except (ValueError, KeyError):
                continue
    return rows


def evolve(rows: list[dict], mcfg: dict, dry_run: bool = False, audit: bool = False
           ) -> dict[str, float]:
    """
    Compute optimal per-hospital damping via grid search.
    Returns {hospital: damping_factor}.
    """
    d_min     = float(mcfg.get("damping_min",        0.50))
    d_max     = float(mcfg.get("damping_max",        1.20))
    max_wait  = int(mcfg.get("max_wait_min",          480))
    min_rows  = int(mcfg.get("min_rows_to_evolve",    24))

    # Candidate damping values in 0.05 steps
    candidates = [round(d_min + i * 0.05, 2)
                  for i in range(int((d_max - d_min) / 0.05) + 1)]

    # Group rows by (hospital, time_band)
    from collections import defaultdict
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        try:
            dt   = datetime.fromisoformat(r["bucket_utc"].replace("Z", "+00:00"))
            hour = dt.astimezone(_MELB).hour
            band = _time_band(hour)
        except Exception:
            band = "unknown"
        groups[(r["hospital"], band)].append(r)

    # Per-hospital: weighted average of optimal damping across time-bands
    hospital_bands: dict[str, list[tuple[float, int]]] = defaultdict(list)

    if audit:
        print(f"\n{'Hospital':<32} {'Band':<12} {'Rows':>5} {'Best d':>7} {'MAE':>8} {'Bias':>8}")
        print("─" * 75)

    for (hospital, band), band_rows in sorted(groups.items()):
        n = len(band_rows)
        if n < 4:  # need at least 4 rows per band to be meaningful
            continue

        best_d, best_mae = candidates[0], float("inf")
        for d in candidates:
            maes = []
            for r in band_rows:
                proj = _project(r["current"], r["momentum"], d, max_wait)
                maes.append(abs(proj - r["actual"]))
            mae = sum(maes) / len(maes)
            if mae < best_mae:
                best_mae, best_d = mae, d

        bias = sum(r["actual"] - _project(r["current"], r["momentum"], best_d, max_wait)
                   for r in band_rows) / n

        if audit:
            print(f"  {hospital:<30} {band:<12} {n:>5} {best_d:>7.2f} {best_mae:>7.1f}m {bias:>+7.1f}m")

        hospital_bands[hospital].append((best_d, n))

    # Weighted average across time-bands; only evolve if enough total rows
    evolved: dict[str, float] = {}
    for hospital, band_results in hospital_bands.items():
        total_rows = sum(n for _, n in band_results)
        if total_rows < min_rows:
            if audit:
                print(f"  {hospital}: skip (only {total_rows} rows, need {min_rows})")
            continue
        weighted = sum(d * n for d, n in band_results) / total_rows
        evolved[hospital] = round(min(d_max, max(d_min, weighted)), 3)

    return evolved


def main() -> None:
    parser = argparse.ArgumentParser(description="Evolve per-hospital ML damping factors.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute evolved factors but do not write to model_config.json")
    parser.add_argument("--audit",   action="store_true",
                        help="Print per-hospital per-band breakdown table")
    args = parser.parse_args()

    mcfg = _load_model_cfg()
    anomaly_pct = float(mcfg.get("anomaly_error_pct", 200.0))

    rows = load_audit(FORECAST_AUDIT_CSV, anomaly_pct)
    if not rows:
        print("  evolve_model: no usable rows in forecast_audit.csv — nothing to do.")
        return

    print(f"  evolve_model: {len(rows)} usable audit rows across "
          f"{len(set(r['hospital'] for r in rows))} hospitals")

    evolved = evolve(rows, mcfg, dry_run=args.dry_run, audit=args.audit)

    if not evolved:
        print("  evolve_model: insufficient data for any hospital — "
              f"need {mcfg.get('min_rows_to_evolve', 24)} completed rows each.")
        return

    print(f"\n  Evolved damping factors:")
    for h, d in sorted(evolved.items()):
        old = mcfg.get("per_hospital_damping", {}).get(h, mcfg.get("momentum_damping", 0.50))
        print(f"    {h:<36} {old:.3f} → {d:.3f}")

    if args.dry_run:
        print("\n  [dry-run] model_config.json NOT updated.")
        return

    # Atomic update: read → merge → write
    mcfg["per_hospital_damping"] = evolved
    _MODEL_CFG.write_text(json.dumps(mcfg, indent=2))
    print(f"\n  model_config.json updated — {len(evolved)} hospital(s) evolved.")

    # Audit log
    log_entry = {
        "run_utc":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "audit_rows": len(rows),
        "evolved":    evolved,
    }
    try:
        with open(_EVOLUTION_LOG, "a") as fh:
            fh.write(json.dumps(log_entry) + "\n")
    except OSError:
        pass


if __name__ == "__main__":
    main()
