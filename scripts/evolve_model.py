# data-class: internal
"""
evolve_model.py — Close the ML feedback loop.

Reads forecast_audit.csv, computes per-hospital damping factors segmented by
day_type × time_band, and writes them to config/model_config.json under
"per_hospital_damping".  predict_next.py selects the right segment at runtime.

Segmentation dimensions
───────────────────────
  day_type   weekday | weekend | public_holiday
               Public holidays are Victorian calendar days (holidays package).
               They behave like Sundays for demand but can spike on the following
               Monday (substitute holiday effect) — tracked separately.

  time_band  overnight (00–06) | morning (06–12)
             afternoon (12–18) | evening (18–24)
               Melbourne local time.

  Compound key written to model_config.json: "{day_type}_{time_band}"
  e.g. "weekday_morning", "weekend_evening", "public_holiday_afternoon"

Algorithm
─────────
For each hospital × day_type × time_band:
  1. Deduplicate on (hospital, bucket_utc) — last written row per slot
  2. Filter: actual_wait_min must be present; error_pct < anomaly_error_pct
  3. Grid-search d ∈ [damping_min, damping_max] in 0.05 steps
       projected = current_wait_min + momentum × steps × d
       projected = clamp(max(projected, current × 0.5), max_wait_min)
       mae       = mean(|projected − actual_wait_min|)
  4. Accept best_d only if its MAE beats current damping — never regress
  5. Per-hospital damping = weighted average across accepted segments
     (weight = row count per segment — high-traffic segments dominate)
  6. MERGE into model_config.json — hospitals below minimum data threshold
     keep their previously-evolved values rather than being reset

Safety
──────
  - min_rows_to_evolve rows required per hospital (across all segments)
  - min 4 rows per segment for a meaningful band-level optimum
  - Evolved value must beat current damping's MAE on the same data
  - All values clamped to [damping_min, damping_max]
  - Merge, never replace
  - Full audit log: old/new damping, MAE before/after, segment breakdown

Future path
───────────
  Once you have ~500 rows per hospital (≈6 months), replace this grid search
  with multivariate Ridge regression using current_wait, momentum,
  treating_count, hour_sin/cos, is_weekend, is_holiday as features.
  The forecast_audit.csv schema already captures all required inputs.

Usage
─────
  python3 scripts/evolve_model.py            # evolve and write
  python3 scripts/evolve_model.py --dry-run  # compute only, don't write
  python3 scripts/evolve_model.py --audit    # per-segment breakdown table
"""

import sys
import csv
import json
import argparse
import pathlib
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from config.paths import FORECAST_AUDIT_CSV, SSD       # noqa: E402
from predict_next import time_band, day_type            # noqa: E402

_REPO          = pathlib.Path(__file__).resolve().parent.parent
_MODEL_CFG     = _REPO / "config" / "model_config.json"
_EVOLUTION_LOG = SSD / "model_evolution_log.jsonl"


def _load_model_cfg() -> dict:
    try:
        return json.loads(_MODEL_CFG.read_text())
    except Exception:
        return {}


def _project(current: float, momentum: float, damping: float,
             steps: float, max_wait: int) -> float:
    """Mirror of predict_next.project_wait — must stay in sync with that formula."""
    projected = current + momentum * steps * damping
    return max(current * 0.50, min(max_wait, projected))


def _current_damping_for(hospital: str, segment: str, ph: dict,
                          global_d: float) -> float:
    """
    Read the current evolved damping for a (hospital, segment) pair.
    Handles nested dict schema and legacy flat-float schema.
    """
    entry = ph.get(hospital)
    if entry is None:
        return global_d
    if isinstance(entry, dict):
        return float(entry.get(segment, global_d))
    return float(entry)  # legacy flat value


def load_audit(path: pathlib.Path, anomaly_pct: float) -> list[dict]:
    """
    Load forecast_audit.csv.

    Deduplicates on (hospital, bucket_utc) — last written row wins.
    Rows must have actual_wait_min and error_pct below anomaly threshold.
    """
    if not path.exists():
        return []

    seen: dict[tuple, dict] = {}
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                actual    = float(row["actual_wait_min"])
                error_pct = float(row["error_pct"])
            except (ValueError, KeyError):
                continue
            if actual <= 0 or error_pct > anomaly_pct:
                continue
            seen[(row["hospital"], row["bucket_utc"])] = row

    rows = []
    for row in seen.values():
        try:
            # Use pre-computed day_type / time_band if present (new schema),
            # else derive from bucket_utc for backward compat with old CSV rows.
            d_type = row.get("day_type", "").strip()
            t_band = row.get("time_band", "").strip()
            if not d_type or not t_band:
                from zoneinfo import ZoneInfo
                dt = datetime.fromisoformat(
                    row["bucket_utc"].replace("Z", "+00:00")
                ).astimezone(ZoneInfo("Australia/Melbourne"))
                d_type = day_type(dt)
                t_band = time_band(dt.hour)

            rows.append({
                "hospital":   row["hospital"],
                "bucket_utc": row["bucket_utc"],
                "day_type":   d_type,
                "time_band":  t_band,
                "segment":    f"{d_type}_{t_band}",
                "current":    float(row.get("current_wait_min") or 0),
                "momentum":   float(row.get("wait_momentum")    or 0),
                "treating":   float(row.get("treating_count")   or 0),
                "actual":     float(row["actual_wait_min"]),
            })
        except (ValueError, KeyError):
            continue
    return rows


def evolve(rows: list[dict], mcfg: dict, audit: bool = False
           ) -> tuple[dict[str, dict], dict[str, dict]]:
    """
    Compute per-hospital per-segment optimal damping via grid search.

    Returns:
        evolved  — {hospital: {segment: damping}}
        details  — {hospital: {old, new (dict), mae_before, mae_after, rows}}
    """
    d_min    = float(mcfg.get("damping_min",        0.50))
    d_max    = float(mcfg.get("damping_max",        1.20))
    max_wait = int(mcfg.get("max_wait_min",          480))
    min_rows = int(mcfg.get("min_rows_to_evolve",    24))
    steps    = float(mcfg.get("horizon_min",          60)) / float(mcfg.get("cadence_min", 15))
    global_d = float(mcfg.get("momentum_damping",   0.50))
    ph       = mcfg.get("per_hospital_damping", {})

    candidates = [round(d_min + i * 0.05, 2)
                  for i in range(round((d_max - d_min) / 0.05) + 1)]

    # Group by (hospital, segment)
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        groups[(r["hospital"], r["segment"])].append(r)

    # per-hospital: accepted segment results
    hospital_segments: dict[str, list[tuple]] = defaultdict(list)
    # (segment, best_d, n, mae_best, mae_current)

    if audit:
        print(f"\n  {'Hospital':<32} {'Segment':<26} {'n':>4} "
              f"{'Cur d':>6} {'MAE cur':>8} {'Best d':>7} {'MAE new':>8} {'Δ':>7}")
        print("  " + "─" * 100)

    for (hospital, segment), seg_rows in sorted(groups.items()):
        n = len(seg_rows)
        if n < 4:
            continue

        cur_d      = _current_damping_for(hospital, segment, ph, global_d)
        cur_errors = [abs(_project(r["current"], r["momentum"], cur_d, steps, max_wait)
                          - r["actual"]) for r in seg_rows]
        cur_mae    = sum(cur_errors) / n

        best_d, best_mae = cur_d, cur_mae
        for d in candidates:
            errors = [abs(_project(r["current"], r["momentum"], d, steps, max_wait)
                          - r["actual"]) for r in seg_rows]
            mae = sum(errors) / n
            if mae < best_mae:
                best_mae, best_d = mae, d

        delta   = best_mae - cur_mae
        improved = best_d != cur_d and best_mae < cur_mae

        if audit:
            mark = " ✓" if improved else ""
            print(f"  {hospital:<32} {segment:<26} {n:>4} "
                  f"{cur_d:>6.2f} {cur_mae:>7.1f}m "
                  f"{best_d:>7.2f} {best_mae:>7.1f}m {delta:>+6.1f}m{mark}")

        hospital_segments[hospital].append((segment, best_d, n, best_mae, cur_mae))

    # Aggregate per hospital — weighted average of per-segment best_d values
    evolved: dict[str, dict] = {}
    details: dict[str, dict] = {}

    for hospital, seg_results in hospital_segments.items():
        total_rows = sum(n for _, _, n, _, _ in seg_results)
        if total_rows < min_rows:
            if audit:
                print(f"  {hospital}: skip — {total_rows} rows (need {min_rows})")
            continue

        # Build per-segment damping dict
        new_segments: dict[str, float] = {}
        for segment, best_d, _, _, _ in seg_results:
            new_segments[segment] = round(min(d_max, max(d_min, best_d)), 3)

        # Overall MAE comparison (all rows for this hospital)
        hospital_rows = [r for r in rows if r["hospital"] == hospital]
        # "before" uses the current damping per segment; "after" uses new
        mae_before = (sum(abs(_project(r["current"], r["momentum"],
                               _current_damping_for(hospital, r["segment"], ph, global_d),
                               steps, max_wait) - r["actual"])
                         for r in hospital_rows) / len(hospital_rows)
                      if hospital_rows else 0.0)
        mae_after  = (sum(abs(_project(r["current"], r["momentum"],
                               new_segments.get(r["segment"],
                                   _current_damping_for(hospital, r["segment"], ph, global_d)),
                               steps, max_wait) - r["actual"])
                         for r in hospital_rows) / len(hospital_rows)
                      if hospital_rows else 0.0)

        old_entry = ph.get(hospital, global_d)
        evolved[hospital] = new_segments
        details[hospital] = {
            "old_damping":      old_entry,
            "new_segments":     new_segments,
            "mae_before":       round(mae_before, 2),
            "mae_after":        round(mae_after,  2),
            "improvement_pct":  round((mae_before - mae_after) / max(mae_before, 1) * 100, 1),
            "total_rows":       total_rows,
            "segments_evolved": len(new_segments),
        }

    return evolved, details


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evolve per-hospital per-segment ML damping factors.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute but do not write to model_config.json")
    parser.add_argument("--audit",   action="store_true",
                        help="Print per-hospital per-segment breakdown table")
    args = parser.parse_args()

    mcfg        = _load_model_cfg()
    anomaly_pct = float(mcfg.get("anomaly_error_pct", 200.0))

    rows = load_audit(FORECAST_AUDIT_CSV, anomaly_pct)
    if not rows:
        print("  evolve_model: no usable rows in forecast_audit.csv — nothing to do.")
        return

    hospitals = set(r["hospital"] for r in rows)
    segments  = set(r["segment"]  for r in rows)
    print(f"  evolve_model: {len(rows)} deduplicated rows | "
          f"{len(hospitals)} hospitals | segments: {sorted(segments)}")

    evolved, details = evolve(rows, mcfg, audit=args.audit)

    if not evolved:
        print(f"  evolve_model: insufficient data — "
              f"need {mcfg.get('min_rows_to_evolve', 24)} rows per hospital.")
        return

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n  {'Hospital':<36} {'Segs':>5} {'MAE before':>11} "
          f"{'MAE after':>10} {'Δ%':>6} {'Rows':>6}")
    print("  " + "─" * 76)
    for h, d in sorted(details.items()):
        direction = "↓" if d["improvement_pct"] > 0 else ("↑" if d["improvement_pct"] < 0 else "→")
        print(f"  {h:<36} {d['segments_evolved']:>5} "
              f"{d['mae_before']:>10.1f}m {d['mae_after']:>9.1f}m "
              f" {direction}{abs(d['improvement_pct']):>4.1f}% {d['total_rows']:>6}")

    if args.dry_run:
        print("\n  [dry-run] model_config.json NOT updated.")
        return

    # ── Merge (not replace) ───────────────────────────────────────────────────
    existing = mcfg.get("per_hospital_damping", {})
    for hospital, new_segs in evolved.items():
        entry = existing.get(hospital, {})
        if not isinstance(entry, dict):
            entry = {}           # upgrade legacy flat float to nested dict
        entry.update(new_segs)
        existing[hospital] = entry
    mcfg["per_hospital_damping"] = existing
    _MODEL_CFG.write_text(json.dumps(mcfg, indent=2))
    print(f"\n  model_config.json updated — {len(evolved)} hospital(s) evolved, "
          f"{len(existing) - len(evolved)} retained from previous run.")

    # ── Audit log ─────────────────────────────────────────────────────────────
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
