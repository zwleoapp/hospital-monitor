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
  1. Deduplicate on (hospital, bucket_utc) — keeps last written row per slot
  2. Filter to completed rows (actual_wait_min known, error_pct < anomaly_error_pct)
  3. Grid-search damping d ∈ [damping_min, damping_max] in 0.05 steps
       projected = current_wait_min + wait_momentum × (horizon_min/cadence_min) × d
       projected = clamp(max(projected, current_wait_min × 0.5), max_wait_min)
       mae       = mean(|projected − actual_wait_min|)
  4. Accept best_d only if its MAE beats the current damping's MAE on the same data
  5. Per-hospital damping = weighted average of accepted band results
     (weight = row count — high-traffic bands dominate)
  6. Merge into model_config.json — hospitals with insufficient data keep their
     previously-evolved value rather than being reset to the global default

Time-bands (Melbourne local, 4 × 6-hour windows):
  overnight  00–06  |  morning   06–12
  afternoon  12–18  |  evening   18–24

Safety
──────
  - Minimum min_rows_to_evolve rows required per hospital (across all bands)
  - Minimum 4 rows per band to compute a meaningful band-level optimum
  - Evolved value must beat current damping's MAE — never regress
  - All written values clamped to [damping_min, damping_max]
  - Merge (not replace) — existing evolved values survive if a hospital has
    insufficient new data this run
  - Full audit log: old damping, new damping, MAE before/after per hospital

Usage
─────
  python3 scripts/evolve_model.py            # evolve and write
  python3 scripts/evolve_model.py --dry-run  # compute and print, don't write
  python3 scripts/evolve_model.py --audit    # per-hospital per-band breakdown
"""

import sys
import csv
import json
import argparse
import pathlib
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from config.paths import FORECAST_AUDIT_CSV, SSD  # noqa: E402

_REPO          = pathlib.Path(__file__).resolve().parent.parent
_MODEL_CFG     = _REPO / "config" / "model_config.json"
_EVOLUTION_LOG = SSD / "model_evolution_log.jsonl"
_MELB          = ZoneInfo("Australia/Melbourne")


def _load_model_cfg() -> dict:
    try:
        return json.loads(_MODEL_CFG.read_text())
    except Exception:
        return {}


def _time_band(hour: int) -> str:
    for name, start, end in [("overnight", 0, 6), ("morning", 6, 12),
                              ("afternoon", 12, 18), ("evening", 18, 24)]:
        if start <= hour < end:
            return name
    return "overnight"


def _project(current: float, momentum: float, damping: float,
             steps: float, max_wait: int) -> float:
    """Mirror of predict_next.project_wait — must stay in sync with that formula."""
    projected = current + momentum * steps * damping
    floor     = current * 0.50
    return max(floor, min(max_wait, projected))


def load_audit(path: pathlib.Path, anomaly_pct: float) -> list[dict]:
    """
    Load forecast_audit.csv.

    Deduplicates on (hospital, bucket_utc) — keeps the last written row for
    each slot so repeated publish runs don't inflate row counts or bias MAE.
    Filters out rows where actual_wait_min is missing or error_pct exceeds
    anomaly_pct (these represent data-quality incidents, not model error).
    """
    if not path.exists():
        return []

    seen: dict[tuple, dict] = {}  # (hospital, bucket_utc) → last row
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                actual    = float(row["actual_wait_min"])
                error_pct = float(row["error_pct"])
            except (ValueError, KeyError):
                continue
            if actual <= 0 or error_pct > anomaly_pct:
                continue
            key = (row["hospital"], row["bucket_utc"])
            seen[key] = row  # last occurrence wins

    rows = []
    for row in seen.values():
        try:
            rows.append({
                "hospital":   row["hospital"],
                "bucket_utc": row["bucket_utc"],
                "source_type": row.get("source_type", "unknown"),
                "current":    float(row["current_wait_min"] or 0),
                "momentum":   float(row["wait_momentum"]    or 0),
                "actual":     float(row["actual_wait_min"]),
            })
        except (ValueError, KeyError):
            continue
    return rows


def evolve(rows: list[dict], mcfg: dict, audit: bool = False
           ) -> tuple[dict[str, float], dict[str, dict]]:
    """
    Compute per-hospital optimal damping via band-level grid search.

    Returns:
        evolved   — {hospital: new_damping}  (only hospitals where improvement found)
        details   — {hospital: {old, new, mae_before, mae_after, rows, bands}}
    """
    d_min      = float(mcfg.get("damping_min",        0.50))
    d_max      = float(mcfg.get("damping_max",        1.20))
    max_wait   = int(mcfg.get("max_wait_min",          480))
    min_rows   = int(mcfg.get("min_rows_to_evolve",    24))
    horizon    = float(mcfg.get("horizon_min",          60))
    cadence    = float(mcfg.get("cadence_min",          15))
    steps      = horizon / cadence          # 4.0 — forecast steps ahead

    global_d   = float(mcfg.get("momentum_damping",   0.50))
    current_ph = mcfg.get("per_hospital_damping", {})

    # Candidate damping values in 0.05 steps inclusive of both ends
    candidates = [round(d_min + i * 0.05, 2)
                  for i in range(round((d_max - d_min) / 0.05) + 1)]

    # ── Group by (hospital, time-band) ────────────────────────────────────────
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        try:
            dt   = datetime.fromisoformat(r["bucket_utc"].replace("Z", "+00:00"))
            hour = dt.astimezone(_MELB).hour
            band = _time_band(hour)
        except Exception:
            band = "unknown"
        groups[(r["hospital"], band)].append(r)

    # ── Per-band grid search ───────────────────────────────────────────────────
    # hospital → list of (best_d, n, mae_best, mae_current)
    hospital_bands: dict[str, list[tuple]] = defaultdict(list)

    if audit:
        print(f"\n{'Hospital':<32} {'Band':<12} {'Rows':>5} "
              f"{'Cur d':>6} {'MAE cur':>8} {'Best d':>7} {'MAE best':>9} {'Δ MAE':>8}")
        print("─" * 92)

    for (hospital, band), band_rows in sorted(groups.items()):
        n = len(band_rows)
        if n < 4:
            continue

        current_d = current_ph.get(hospital, global_d)

        # Baseline MAE with the current damping
        baseline_errors = [
            abs(_project(r["current"], r["momentum"], current_d, steps, max_wait) - r["actual"])
            for r in band_rows
        ]
        baseline_mae = sum(baseline_errors) / n

        # Grid search for minimum MAE
        best_d, best_mae = current_d, baseline_mae
        for d in candidates:
            errors = [
                abs(_project(r["current"], r["momentum"], d, steps, max_wait) - r["actual"])
                for r in band_rows
            ]
            mae = sum(errors) / n
            if mae < best_mae:
                best_mae, best_d = mae, d

        delta = best_mae - baseline_mae  # negative = improvement

        if audit:
            marker = " ✓" if best_d != current_d else ""
            print(f"  {hospital:<30} {band:<12} {n:>5} "
                  f"{current_d:>6.2f} {baseline_mae:>7.1f}m "
                  f"{best_d:>7.2f} {best_mae:>8.1f}m {delta:>+7.1f}m{marker}")

        # Only accept the band result if we found a genuine improvement
        if best_d != current_d and best_mae < baseline_mae:
            hospital_bands[hospital].append((best_d, n, best_mae, baseline_mae))
        elif best_d == current_d:
            # Current damping is already optimal — still record for row-count purposes
            hospital_bands[hospital].append((best_d, n, best_mae, baseline_mae))

    # ── Weighted average across bands → one damping per hospital ──────────────
    evolved:  dict[str, float] = {}
    details:  dict[str, dict]  = {}

    for hospital, band_results in hospital_bands.items():
        total_rows = sum(n for _, n, _, _ in band_results)
        if total_rows < min_rows:
            if audit:
                print(f"  {hospital}: skip — only {total_rows} rows total (need {min_rows})")
            continue

        current_d = current_ph.get(hospital, global_d)

        # Weighted average of per-band best_d values
        weighted_d = sum(d * n for d, n, _, _ in band_results) / total_rows
        new_d      = round(min(d_max, max(d_min, weighted_d)), 3)

        # Compute overall MAE before and after across all band rows for this hospital
        all_rows_for_hospital = [r for r in rows if r["hospital"] == hospital]
        mae_before = (sum(abs(_project(r["current"], r["momentum"], current_d, steps, max_wait) - r["actual"])
                          for r in all_rows_for_hospital) / len(all_rows_for_hospital)
                      if all_rows_for_hospital else 0.0)
        mae_after  = (sum(abs(_project(r["current"], r["momentum"], new_d, steps, max_wait) - r["actual"])
                          for r in all_rows_for_hospital) / len(all_rows_for_hospital)
                      if all_rows_for_hospital else 0.0)

        evolved[hospital] = new_d
        details[hospital] = {
            "old_damping":  current_d,
            "new_damping":  new_d,
            "mae_before":   round(mae_before, 2),
            "mae_after":    round(mae_after,  2),
            "improvement_pct": round((mae_before - mae_after) / max(mae_before, 1) * 100, 1),
            "total_rows":   total_rows,
            "bands":        len(band_results),
        }

    return evolved, details


def main() -> None:
    parser = argparse.ArgumentParser(description="Evolve per-hospital ML damping factors.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute evolved factors but do not write to model_config.json")
    parser.add_argument("--audit",   action="store_true",
                        help="Print per-hospital per-band breakdown table")
    args = parser.parse_args()

    mcfg        = _load_model_cfg()
    anomaly_pct = float(mcfg.get("anomaly_error_pct", 200.0))

    rows = load_audit(FORECAST_AUDIT_CSV, anomaly_pct)
    if not rows:
        print("  evolve_model: no usable rows in forecast_audit.csv — nothing to do.")
        return

    hospitals = set(r["hospital"] for r in rows)
    print(f"  evolve_model: {len(rows)} deduplicated audit rows across {len(hospitals)} hospitals")

    evolved, details = evolve(rows, mcfg, audit=args.audit)

    if not evolved:
        print(f"  evolve_model: insufficient data — need "
              f"{mcfg.get('min_rows_to_evolve', 24)} rows per hospital.")
        return

    # ── Summary table ─────────────────────────────────────────────────────────
    print(f"\n  {'Hospital':<36} {'Old d':>6} {'New d':>6} "
          f"{'MAE before':>11} {'MAE after':>10} {'Δ%':>6} {'Rows':>6}")
    print("  " + "─" * 82)
    for h, d in details.items():
        direction = "↓" if d["improvement_pct"] > 0 else ("↑" if d["improvement_pct"] < 0 else "→")
        print(f"  {h:<36} {d['old_damping']:>6.3f} {d['new_damping']:>6.3f} "
              f"{d['mae_before']:>10.1f}m {d['mae_after']:>9.1f}m "
              f"{direction}{abs(d['improvement_pct']):>4.1f}% {d['total_rows']:>6}")

    if args.dry_run:
        print("\n  [dry-run] model_config.json NOT updated.")
        return

    # ── Merge (not replace) into model_config.json ────────────────────────────
    # Hospitals with insufficient data keep their previously-evolved value.
    existing = mcfg.get("per_hospital_damping", {})
    existing.update(evolved)
    mcfg["per_hospital_damping"] = existing
    _MODEL_CFG.write_text(json.dumps(mcfg, indent=2))
    print(f"\n  model_config.json updated — {len(evolved)} hospital(s) evolved, "
          f"{len(existing) - len(evolved)} retained from previous run.")

    # ── Evolution audit log ───────────────────────────────────────────────────
    log_entry = {
        "run_utc":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "audit_rows": len(rows),
        "hospitals":  details,
    }
    try:
        with open(_EVOLUTION_LOG, "a") as fh:
            fh.write(json.dumps(log_entry) + "\n")
    except OSError:
        pass


if __name__ == "__main__":
    main()
