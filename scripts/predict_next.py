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
from config.paths import (             # noqa: E402
    SILVER_CSV as DEFAULT_SILVER,
    ACCURACY_LOG as _ACCURACY_LOG,
    ANOMALY_LOG  as _ANOMALY_LOG,
)

# ── Model config (config/model_config.json) ───────────────────────────────────
_MODEL_CFG_PATH = pathlib.Path(__file__).resolve().parent.parent / "config" / "model_config.json"
_OVERRIDES_PATH = pathlib.Path(__file__).resolve().parent.parent / "config" / "overrides.json"

def _load_model_cfg() -> dict:
    try:
        return json.loads(_MODEL_CFG_PATH.read_text())
    except Exception:
        return {}

_mcfg = _load_model_cfg()

HORIZON_MIN       = int(_mcfg.get("horizon_min",       60))
CADENCE_MIN       = int(_mcfg.get("cadence_min",       15))
MOMENTUM_DAMPING  = float(_mcfg.get("momentum_damping", 0.50))
MOMENTUM_CEILING  = float(_mcfg.get("momentum_ceiling", 30.0))
MAX_WAIT_MIN      = int(_mcfg.get("max_wait_min",       480))
LOS_TARGET_PCT    = float(_mcfg.get("los_target_pct",   70.0))
DAMPING_MIN       = float(_mcfg.get("damping_min",      0.50))
DAMPING_MAX       = float(_mcfg.get("damping_max",      1.20))
ANOMALY_ERROR_PCT = float(_mcfg.get("anomaly_error_pct", 200.0))

# Confidence weights
_CW_LOS      = float(_mcfg.get("confidence_weight_los",      0.50))
_CW_MOMENTUM = float(_mcfg.get("confidence_weight_momentum", 0.30))
_CW_P90      = float(_mcfg.get("confidence_weight_p90",      0.20))
_CONF_HIGH   = float(_mcfg.get("confidence_high_threshold",  0.70))
_CONF_MOD    = float(_mcfg.get("confidence_moderate_threshold", 0.40))

# Per-hospital damping from evolve_model.py (written to model_config.json)
_PER_HOSPITAL_DAMPING: dict[str, float] = _mcfg.get("per_hospital_damping", {})


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
      3. model_config.json → per_hospital_damping[hospital]          (ML-evolved by evolve_model.py)
      4. model_config.json → momentum_damping                        (global default)
    """
    def _clamp(v: float) -> float:
        return float(min(DAMPING_MAX, max(DAMPING_MIN, v)))

    ov = _load_overrides()

    if hospital and "manual_damping_per_site" in ov:
        if hospital in ov["manual_damping_per_site"]:
            return _clamp(ov["manual_damping_per_site"][hospital])

    if "manual_damping" in ov:
        return _clamp(ov["manual_damping"])

    if hospital and hospital in _PER_HOSPITAL_DAMPING:
        return _clamp(_PER_HOSPITAL_DAMPING[hospital])

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

    score = round(_CW_LOS * los_score + _CW_MOMENTUM * momentum_score + _CW_P90 * p90_score, 3)

    if score >= _CONF_HIGH:
        label = "High"
    elif score >= _CONF_MOD:
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
