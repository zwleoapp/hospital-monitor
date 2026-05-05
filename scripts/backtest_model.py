# data-class: internal
"""
backtest_model.py — Recompute forecast_audit.csv backtest columns.

For every row in forecast_audit.csv, apply the *current* model formula
(predict_next.project_wait + get_effective_damping) and write three new columns:

  backtest_predicted_wait_min  — what the current model would have predicted
  backtest_error_pct           — |backtest - actual| / actual × 100
  backtest_accuracy            — 100 − min(backtest_error_pct, 100)

Also fills bucket_local_melb for any older rows that predate the field.

The original predicted_wait_min / error_pct / forecast_accuracy columns are
preserved unchanged — they record what was *live* at the time.

This script is idempotent: safe to run multiple times. Run it after any model
change (evolved damping, formula tweak, momentum_floor_ratio adjustment) to see
how the new model would have performed on all historical data.

Usage
─────
  python3 scripts/backtest_model.py           # rewrite forecast_audit.csv in-place
  python3 scripts/backtest_model.py --dry-run # compute and print summary only
  python3 scripts/backtest_model.py --audit   # per-hospital per-segment table
"""

import sys
import csv
import json
import argparse
import pathlib
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from predict_next import predict_wait, day_type, time_band                         # noqa: E402
from config.paths import FORECAST_AUDIT_CSV                                         # noqa: E402

_MELB_TZ_NAME = "Australia/Melbourne"

# Columns written by get_history.py (original schema).  Backtest columns and
# bucket_local_melb are appended / updated by this script.
_LIVE_COLUMNS = [
    "bucket_utc", "hospital", "cohort", "source_type",
    "day_type", "time_band",
    "current_wait_min", "wait_momentum", "treating_count",
    "actual_wait_min", "predicted_wait_min", "error_pct", "forecast_accuracy",
    "cache_lag_minutes", "fidelity_status",
]
_FULL_HEADER = _LIVE_COLUMNS + [
    "bucket_local_melb",
    "backtest_predicted_wait_min", "backtest_error_pct", "backtest_accuracy",
]


def _melb_local(bucket_utc_str: str):
    """Parse bucket_utc ISO string → Melbourne-local aware datetime."""
    from zoneinfo import ZoneInfo
    dt = datetime.fromisoformat(bucket_utc_str.replace("Z", "+00:00"))
    return dt.astimezone(ZoneInfo(_MELB_TZ_NAME))


def load_rows(path: pathlib.Path) -> list[dict]:
    """
    Read all rows from forecast_audit.csv, handling legacy schema gracefully.

    forecast_audit.csv may have been written in one of several historical schemas:

      12-col (legacy): bucket_utc, hospital, cohort, source_type,
                       current_wait_min, wait_momentum, actual_wait_min,
                       predicted_wait_min, error_pct, forecast_accuracy,
                       cache_lag_minutes, fidelity_status

      15-col (current): inserts day_type, time_band, treating_count at positions 4–6

      19-col (new): appends bucket_local_melb, backtest_* at the end

    The file header line is taken as authoritative for fields that ARE present.
    For rows that have MORE values than the header declares (e.g. 15-value rows
    under a 12-column header), the extra positional values are mapped using the
    known 15-col schema so the column shift does not corrupt current_wait_min.

    Does NOT deduplicate — preserves every row so the file structure is kept.
    """
    _15COL_FIELDS = [
        "bucket_utc", "hospital", "cohort", "source_type",
        "day_type", "time_band",
        "current_wait_min", "wait_momentum", "treating_count",
        "actual_wait_min", "predicted_wait_min", "error_pct", "forecast_accuracy",
        "cache_lag_minutes", "fidelity_status",
    ]

    if not path.exists():
        return []

    rows = []
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        for values in reader:
            n_header = len(header)
            n_values = len(values)

            if n_values <= n_header:
                # Normal case — map by header position (DictReader behaviour)
                row = dict(zip(header, values))
            else:
                # More values than header columns — use positional remapping.
                # The known 15-col schema covers the standard case.
                mapping = _15COL_FIELDS if n_values >= 15 else header
                row = dict(zip(mapping, values[:len(mapping)]))
                # Preserve any extra trailing columns by merging header keys
                for k, v in zip(header[len(mapping):], values[len(mapping):]):
                    row[k] = v

            rows.append(row)
    return rows


def compute_backtest(row: dict) -> tuple[str, str, str, str]:
    """
    Compute (bucket_local_melb, backtest_predicted, backtest_error_pct, backtest_accuracy)
    for a single audit row.  Returns ("", "", "", "") if actual_wait_min is missing.
    """
    bucket_utc = row.get("bucket_utc", "")
    actual_str = row.get("actual_wait_min", "")
    current_str = row.get("current_wait_min", "")
    momentum_str = row.get("wait_momentum", "")
    hospital = row.get("hospital", "")

    if not bucket_utc:
        return "", "", "", ""

    try:
        bucket_melb = _melb_local(bucket_utc)
        local_str = bucket_melb.isoformat(timespec="seconds")
    except Exception:
        local_str = ""
        bucket_melb = None

    if not actual_str or not current_str:
        return local_str, "", "", ""

    try:
        actual   = float(actual_str)
        current  = float(current_str)
        momentum = float(momentum_str) if momentum_str else 0.0
    except ValueError:
        return local_str, "", "", ""

    if actual <= 0:
        return local_str, "", "", ""

    bt_pred = predict_wait(hospital, current, momentum,
                           float(row.get("treating_count") or 0), bucket_melb)
    bt_err  = abs(bt_pred - actual) / actual * 100
    bt_acc  = round(100 - min(bt_err, 100), 1)

    return local_str, str(round(bt_pred, 1)), str(round(bt_err, 1)), str(bt_acc)


def run(rows: list[dict], audit: bool = False) -> list[dict]:
    """
    Apply compute_backtest to every row and return updated rows.
    Also summarises per-hospital improvement for the audit table.
    """
    from zoneinfo import ZoneInfo
    updated = []

    # For audit summary: {hospital: {segment: [(live_acc, bt_acc)]}}
    summary: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))

    for row in rows:
        local_str, bt_pred, bt_err, bt_acc = compute_backtest(row)

        new_row = dict(row)
        if local_str:
            new_row["bucket_local_melb"] = local_str
        new_row["backtest_predicted_wait_min"] = bt_pred
        new_row["backtest_error_pct"]          = bt_err
        new_row["backtest_accuracy"]           = bt_acc

        if audit and bt_acc:
            hospital = row.get("hospital", "")
            d_type   = row.get("day_type", "").strip()
            t_band   = row.get("time_band", "").strip()
            if not d_type or not t_band:
                try:
                    bm = _melb_local(row.get("bucket_utc", ""))
                    d_type = day_type(bm)
                    t_band = time_band(bm.hour)
                except Exception:
                    d_type, t_band = "unknown", "unknown"
            segment = f"{d_type}_{t_band}"
            live_acc_str = row.get("forecast_accuracy", "")
            try:
                live_acc = float(live_acc_str)
                summary[hospital][segment].append((live_acc, float(bt_acc)))
            except ValueError:
                pass

        updated.append(new_row)

    if audit:
        _print_audit(summary)

    return updated


def _print_audit(summary: dict) -> None:
    print(f"\n  {'Hospital':<34} {'Segment':<26} {'n':>4} "
          f"{'Live acc':>9} {'BT acc':>8} {'Δ':>7}")
    print("  " + "─" * 92)
    for hospital in sorted(summary):
        for segment in sorted(summary[hospital]):
            pairs = summary[hospital][segment]
            n = len(pairs)
            if n == 0:
                continue
            live_mean = sum(p[0] for p in pairs) / n
            bt_mean   = sum(p[1] for p in pairs) / n
            delta     = bt_mean - live_mean
            mark      = " ↑" if delta > 0.5 else (" ↓" if delta < -0.5 else "  →")
            print(f"  {hospital:<34} {segment:<26} {n:>4} "
                  f"{live_mean:>8.1f}% {bt_mean:>7.1f}%  {delta:>+5.1f}%{mark}")

    # Per-hospital summary line
    print()
    for hospital in sorted(summary):
        all_pairs = [p for segs in summary[hospital].values() for p in segs]
        if not all_pairs:
            continue
        n = len(all_pairs)
        live_mean = sum(p[0] for p in all_pairs) / n
        bt_mean   = sum(p[1] for p in all_pairs) / n
        print(f"  {hospital:<34}  overall n={n:4d}  "
              f"live {live_mean:.1f}%  backtest {bt_mean:.1f}%  "
              f"Δ {bt_mean - live_mean:+.1f}%")


def write_csv(path: pathlib.Path, rows: list[dict]) -> None:
    """Rewrite forecast_audit.csv in-place with the full header + updated rows."""
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FULL_HEADER, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recompute backtest columns in forecast_audit.csv.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and summarise but do not write to the CSV")
    parser.add_argument("--audit", action="store_true",
                        help="Print per-hospital per-segment live vs backtest accuracy table")
    args = parser.parse_args()

    rows = load_rows(FORECAST_AUDIT_CSV)
    if not rows:
        print("  backtest_model: forecast_audit.csv not found or empty — nothing to do.")
        return

    total = len(rows)
    has_actual = sum(1 for r in rows if r.get("actual_wait_min", ""))
    print(f"  backtest_model: {total} rows ({has_actual} with actual_wait_min) "
          f"from {FORECAST_AUDIT_CSV.name}")

    updated = run(rows, audit=args.audit)

    bt_computed = sum(1 for r in updated if r.get("backtest_accuracy", ""))
    print(f"  backtest columns computed for {bt_computed} rows.")

    if args.dry_run:
        print("  [dry-run] forecast_audit.csv NOT updated.")
        return

    write_csv(FORECAST_AUDIT_CSV, updated)
    print(f"  forecast_audit.csv rewritten — {len(updated)} rows, "
          f"header: {', '.join(_FULL_HEADER[-4:])}")


if __name__ == "__main__":
    main()
