# data-class: public-aggregate
"""
predict_next.py — Rule-based 60-minute ED wait-time outlook.

This is the Phase 1 heuristic baseline.  It makes no assumptions beyond three
observable inputs so the trained ML model has a clear, reproducible bar to beat:

  Input (from Silver CSV, most-recent row per hospital):
    current_wait_min   min_wait_mins from live Bronze
    wait_momentum      change per 15-min cadence (computed in transform_silver.py)
    ctx_*              VAHI quarterly benchmarks

  Projection formula (damped linear extrapolation):
    horizon = 60 min = 4 cadence steps
    projected = current_wait + momentum * 4 * MOMENTUM_DAMPING
    clamped to [0, MAX_WAIT_MIN]

  Confidence score (0.0 – 1.0):
    Composite of three signals, each grounded in the baseline chart:
      los_score      = min(1, ctx_los_pct_under_4hr / 70)   weight 0.50
                       How close is the hospital to the 70% national target?
                       High → system in "normal" regime → more predictable.
      momentum_score = max(0, 1 - |momentum| / MOMENTUM_CEILING)  weight 0.30
                       Stable trend → more predictable.
      p90_score      = max(0, 1 - max(0, wait - p90) / p90)       weight 0.20
                       Is current wait within historical norms?

    confidence_label: High (>=0.70) | Moderate (>=0.40) | Low (<0.40)

Output (stdout + optional --out <path>.json):
  JSON matching the DESIGN.md §6 publisher schema:
  { generated_utc, horizon_min, sites: [{site, latest_obs_utc,
    current_wait_min, predicted_wait_min, confidence, confidence_label,
    wait_momentum, ctx_source}] }
"""

import sys
import json
import argparse
import pathlib
from datetime import datetime, timezone, timedelta

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from config.hospitals import ALL_HOSPITALS

# ── Paths ─────────────────────────────────────────────────────────────────────
_SSD = pathlib.Path("/mnt/router_ssd/Data_Hub/Waiting_Live_time")
DEFAULT_SILVER = _SSD / "eastern_hospital_silver.csv"

# ── Tuning constants ──────────────────────────────────────────────────────────
HORIZON_MIN       = 60     # forecast horizon in minutes
CADENCE_MIN       = 15     # scraper cadence — momentum is normalised to this unit
MOMENTUM_DAMPING  = 0.50   # default damping — overridden by get_effective_damping()
MOMENTUM_CEILING  = 30.0   # momentum beyond this (min/cadence) = max uncertainty
MAX_WAIT_MIN      = 480    # hard upper clamp on projected wait (8 hours)
LOS_TARGET_PCT    = 70.0   # Australian national 4-hour ED target

# ── Safety & evolution constraints ────────────────────────────────────────────
DAMPING_MIN       = 0.50   # ML-evolved damping cannot go below this (D ∈ [0.5, 1.2])
DAMPING_MAX       = 1.20   # ML-evolved damping cannot exceed this
ANOMALY_ERROR_PCT = 200.0  # skip ML training on snapshots with error > 200%

# ── Derived paths ─────────────────────────────────────────────────────────────
_OVERRIDES_PATH = pathlib.Path(__file__).resolve().parent.parent / "config" / "overrides.json"
_ACCURACY_LOG   = _SSD / "accuracy_postmortem.jsonl"
_ANOMALY_LOG    = _SSD / "damping_anomalies.jsonl"


# ── Override & safety layer ───────────────────────────────────────────────────

def _load_overrides() -> dict:
    """Return config/overrides.json if it exists and is valid, else empty dict."""
    try:
        return json.loads(_OVERRIDES_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def get_effective_damping(hospital: str | None = None) -> float:
    """
    Resolve the damping factor for a hospital, clamped to [DAMPING_MIN, DAMPING_MAX].

    Priority order:
      1. config/overrides.json → manual_damping_per_site[hospital]  (human override)
      2. config/overrides.json → manual_damping                      (global human override)
      3. ML-evolved value from evolve_damping_factors()              (Phase 2 — placeholder)
      4. MOMENTUM_DAMPING compile-time default
    """
    ov = _load_overrides()

    if hospital and "manual_damping_per_site" in ov:
        per_site = ov["manual_damping_per_site"]
        if hospital in per_site:
            return float(min(DAMPING_MAX, max(DAMPING_MIN, per_site[hospital])))

    if "manual_damping" in ov:
        return float(min(DAMPING_MAX, max(DAMPING_MIN, ov["manual_damping"])))

    return MOMENTUM_DAMPING


def evolve_damping_factors(accuracy_log: pathlib.Path = _ACCURACY_LOG) -> dict:
    """
    Phase 2 ML loop — reads last 72h of accuracy postmortem entries and will
    compute the per-hospital damping factor that minimises mean-absolute-error.

    Safety constraints (enforced when compute is activated):
      - All results clamped to [DAMPING_MIN, DAMPING_MAX]
      - Snapshots where |predicted − actual| / actual > ANOMALY_ERROR_PCT are skipped

    Returns dict mapping hospital name → evolved damping factor.
    Returns {} (no-op) until Phase 2 grid-search compute is activated.
    """
    if not accuracy_log.exists():
        return {}
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
        records = []
        with open(accuracy_log) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    logged = datetime.fromisoformat(rec["logged_utc"].replace("Z", "+00:00"))
                    if logged >= cutoff:
                        records.append(rec)
                except Exception:
                    continue
        # TODO Phase 2: grid-search optimal damping per hospital over records
        _ = records  # consumed when Phase 2 compute is activated
        return {}
    except OSError:
        return {}


# ── Core functions ────────────────────────────────────────────────────────────

def project_wait(current_wait: float, momentum: float, damping: float | None = None) -> float:
    """
    Damped linear extrapolation across HORIZON_MIN.

    steps = HORIZON_MIN / CADENCE_MIN = 4 cadence units.
    Damping prevents runaway compounding (mean-reverting assumption).
    Floor at 50% of current_wait: a one-cycle momentum spike shouldn't predict
    near-zero wait when the system is clearly still busy.
    """
    d = damping if damping is not None else MOMENTUM_DAMPING
    steps = HORIZON_MIN / CADENCE_MIN
    projected = current_wait + momentum * steps * d
    floor = current_wait * 0.50
    return round(max(floor, min(MAX_WAIT_MIN, projected)), 1)


def confidence_score(
    current_wait: float,
    momentum: float,
    ctx_los_pct_under_4hr: float,
    ctx_wait_p90_mins: float,
) -> tuple[float, str]:
    """
    Composite confidence (0.0–1.0) grounded in the 14-year baseline chart.

    Returns (score, label) where label is "High" | "Moderate" | "Low".
    """
    # [1] LOS target proximity — primary signal from the trend chart
    # A hospital near 70% is in a predictable, "normal" operating regime.
    los_score = min(1.0, ctx_los_pct_under_4hr / LOS_TARGET_PCT)

    # [2] Momentum stability — a rapidly changing wait is harder to extrapolate
    momentum_score = max(0.0, 1.0 - abs(momentum) / MOMENTUM_CEILING)

    # [3] Wait within historical norms — above the quarterly p90 = unusual territory
    overshoot = max(0.0, current_wait - ctx_wait_p90_mins)
    p90_score = max(0.0, 1.0 - overshoot / max(1.0, ctx_wait_p90_mins))

    score = round(0.50 * los_score + 0.30 * momentum_score + 0.20 * p90_score, 3)

    if score >= 0.70:
        label = "High"
    elif score >= 0.40:
        label = "Moderate"
    else:
        label = "Low"

    return score, label


# ── I/O ───────────────────────────────────────────────────────────────────────

def load_latest_silver(path: pathlib.Path) -> pd.DataFrame:
    """Return the most-recent Silver row per hospital, sorted by hospital name."""
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    latest = (
        df.sort_values("timestamp")
          .groupby("hospital", sort=False)
          .last()
          .reset_index()
    )
    return latest[latest["hospital"].isin(ALL_HOSPITALS)]


def build_outlook(silver_row: pd.Series) -> dict:
    """Produce a single-site outlook dict from the most-recent Silver row."""
    hospital      = silver_row["hospital"]
    network       = str(silver_row.get("ctx_network", ""))
    current_wait  = float(silver_row["min_wait_mins"])
    raw_max       = silver_row.get("max_wait_mins", float("nan"))
    max_wait      = None if pd.isna(raw_max) else int(raw_max)
    waiting_count   = int(silver_row.get("waiting",  0) or 0)
    treating_count  = int(silver_row.get("treating", 0) or 0)
    raw_momentum = silver_row.get("wait_momentum", float("nan"))
    momentum     = 0.0 if pd.isna(raw_momentum) else float(raw_momentum)
    los_pct      = float(silver_row["ctx_los_pct_under_4hr"])
    p90          = float(silver_row["ctx_wait_p90_mins"])
    ctx_source   = str(silver_row["ctx_source"])
    obs_utc      = silver_row["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ")

    raw_med123 = silver_row.get("ctx_wait_median_cat123_mins", float("nan"))
    raw_med45  = silver_row.get("ctx_wait_median_cat45_mins",  float("nan"))
    vahi_median_cat123 = None if pd.isna(raw_med123) else int(raw_med123)
    vahi_median_cat45  = None if pd.isna(raw_med45)  else int(raw_med45)

    damping = get_effective_damping(hospital)
    projected, (confidence, label) = (
        project_wait(current_wait, momentum, damping),
        confidence_score(current_wait, momentum, los_pct, p90),
    )

    return {
        "site":                  hospital,
        "network":               network,
        "latest_obs_utc":        obs_utc,
        "waiting_count":         waiting_count,
        "treating_count":        treating_count,
        "current_wait_min":      round(current_wait, 1),
        "max_wait_min":          max_wait,
        "predicted_wait_min":    projected,
        "wait_momentum":         round(momentum, 1),
        "confidence":            confidence,
        "confidence_label":      label,
        "ctx_source":            ctx_source,
        "vahi_p90_mins":         None if pd.isna(p90) else int(p90),
        "vahi_median_cat123_mins": vahi_median_cat123,
        "vahi_median_cat45_mins":  vahi_median_cat45,
    }


def format_report(payload: dict) -> str:
    """Human-readable console summary."""
    lines = [
        f"  ED Wait Outlook — generated {payload['generated_utc']}",
        f"  Horizon: {payload['horizon_min']} min | "
        f"Damping: {MOMENTUM_DAMPING} | Target: {LOS_TARGET_PCT}% LOS<4hr",
        "",
    ]
    for s in payload["sites"]:
        trend = "↑" if s["wait_momentum"] > 0 else ("↓" if s["wait_momentum"] < 0 else "→")
        lines += [
            f"  {s['site']}",
            f"    Now:       {s['current_wait_min']:.0f} min  "
            f"(momentum {trend}{abs(s['wait_momentum']):.1f} min/15min)",
            f"    In 60 min: {s['predicted_wait_min']:.0f} min  "
            f"[{s['confidence_label']} confidence — {s['confidence']:.2f}]",
            f"    Context:   {s['ctx_source']}  "
            f"LOS<4hr {s['site']} baseline "
            f"(conf weight: 50% LOS proximity to {LOS_TARGET_PCT}%)",
            "",
        ]
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="60-minute ED wait-time outlook from Silver CSV."
    )
    parser.add_argument(
        "--silver", type=pathlib.Path, default=DEFAULT_SILVER,
        help="Path to Silver CSV (default: SSD path)",
    )
    parser.add_argument(
        "--out", type=pathlib.Path, default=None,
        help="Write JSON output to this path (optional)",
    )
    args = parser.parse_args()

    try:
        silver = load_latest_silver(args.silver)
    except FileNotFoundError:
        print(f"ERROR: Silver CSV not found at {args.silver}", file=sys.stderr)
        print("Run transform_silver.py first.", file=sys.stderr)
        sys.exit(1)

    if silver.empty:
        print("ERROR: No rows found for target hospitals in Silver CSV.", file=sys.stderr)
        sys.exit(1)

    sites = [build_outlook(row) for _, row in silver.iterrows()]

    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "horizon_min":   HORIZON_MIN,
        "sites":         sites,
    }

    print(format_report(payload))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2))
        print(f"  JSON written → {args.out}")


if __name__ == "__main__":
    main()
