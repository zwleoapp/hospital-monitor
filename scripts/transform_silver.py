# data-class: public-aggregate
"""
transform_silver.py — Silver transform with VAHI / AIHW contextual enrichment.

Hospital-agnostic: processes every hospital present in the Bronze CSV.
Registry-driven: active hospitals are defined in config/hospitals.csv.

Reads:
  --bronze   Bronze CSV (all networks, default: SSD path)
  --out      Output Silver CSV (default: SSD path)
  VAHI_FILE  bronze/vahi_history_merged.csv (quarterly benchmarks, Oct 2024–)
  AIHW_FILE  bronze/eastern_hospital_historical_context.csv
             (annual AIHW backfill, 2011–2025; run fetch_aihw.py --append to
             extend coverage to all hospitals)

Join logic (LEFT — every Bronze row is preserved):
  1. VAHI quarterly match: quarter_start_utc ≤ ts < quarter_end_utc, same hospital.
  2. AIHW annual fallback (no VAHI match): period_start ≤ ts < period_end+1d, same hospital.
  3. ctx_source == "VAHI" | "AIHW" on every output row.

Semantic note:
  VAHI ctx columns measure *wait time to treatment start*.
  AIHW fallback columns measure *total ED episode time* (arrival → departure).
  Both correlate with load pressure but at different scales (~6 min vs ~280 min median).
  Always condition on ctx_source before comparing ctx values across rows.

Idempotency / duplicate protection:
  Silver is a full rebuild from Bronze on every run — never appended.
  Running twice on the same Bronze input produces byte-identical output.
  Within each run, consecutive rows per hospital with unchanged (min_wait_mins,
  load_ratio) are dropped — scraper no-change repeats do not accumulate.
"""

import re
import sys
import time
import argparse
import pathlib
import pandas as pd
import holidays
from datetime import timedelta
from zoneinfo import ZoneInfo

try:
    from status import update_status
except ImportError:
    def update_status(name, state):  # graceful no-op outside Pi environment
        pass

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = pathlib.Path(__file__).resolve().parent.parent
_SSD  = pathlib.Path("/mnt/router_ssd/Data_Hub/Waiting_Live_time")

DEFAULT_BRONZE = _SSD / "eastern_hospital.csv"
DEFAULT_OUTPUT = _SSD / "eastern_hospital_silver.csv"
VAHI_FILE      = _BASE / "bronze" / "vahi_history_merged.csv"
AIHW_FILE      = _BASE / "bronze" / "eastern_hospital_historical_context.csv"

# ── Constants ─────────────────────────────────────────────────────────────────
MELBOURNE = ZoneInfo("Australia/Melbourne")

# Maps calendar month → quarter-of-year (Melbourne-local)
_QUARTER_MONTH_TO_QOY = {
    1: 1, 2: 1, 3: 1,   # Jan–Mar
    4: 2, 5: 2, 6: 2,   # Apr–Jun
    7: 3, 8: 3, 9: 3,   # Jul–Sep
    10: 4, 11: 4, 12: 4, # Oct–Dec
}
VIC_HOLIDAYS = holidays.AU(subdiv="VIC")

# AIHW uses this long-form name; our live schema uses the short form.
AIHW_NAME_MAP = {
    "Maroondah Hospital [East Ringwood]": "Maroondah Hospital",
}

CTX_COLS = [
    "ctx_network",
    "ctx_wait_p90_mins", "ctx_wait_median_cat123_mins", "ctx_wait_median_cat45_mins",
    "ctx_los_pct_under_4hr", "ctx_los_pct_over_24hr", "ctx_non_admitted_los_pct_under_4hr",
    "ctx_source",
]

SILVER_COL_ORDER = [
    "timestamp", "hospital", "waiting", "treating", "wait_time",
    "min_wait_mins", "max_wait_mins", "load_ratio",
    "wait_momentum",
    "hour", "day_of_week", "is_weekend",
    "is_holiday", "is_eve", "day_type", "season",
    "ctx_network",
    "ctx_wait_p90_mins", "ctx_wait_median_cat123_mins", "ctx_wait_median_cat45_mins",
    "ctx_los_pct_under_4hr", "ctx_los_pct_over_24hr", "ctx_non_admitted_los_pct_under_4hr",
    "ctx_source",
]

# ── Loaders ───────────────────────────────────────────────────────────────────

def load_bronze(path: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["hospital"] = df["hospital"].str.strip()
    return df


def load_vahi(path: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["quarter_start_utc"] = pd.to_datetime(df["quarter_start_utc"], utc=True)
    df["quarter_end_utc"]   = pd.to_datetime(df["quarter_end_utc"],   utc=True)
    df["hospital"] = df["hospital"].str.strip()
    return df


def _compute_seasonal_benchmarks(vahi: pd.DataFrame) -> pd.DataFrame:
    """
    For each (hospital, quarter-of-year), average the three wait benchmark columns
    across all real VAHI years (source == 'VAHI').

    Returns a DataFrame with columns: hospital, _qoy, _seas_p90, _seas_med123, _seas_med45.
    Used by _join_vahi to replace exact-quarter benchmark values with seasonally-adjusted ones.
    """
    real = vahi[vahi["source"] == "VAHI"].copy()
    real["_qoy"] = (
        real["quarter_start_utc"].dt.tz_convert(MELBOURNE).dt.month
        .map(_QUARTER_MONTH_TO_QOY)
    )
    return (
        real.groupby(["hospital", "_qoy"])
        [["wait_p90_mins", "wait_median_cat123_mins", "wait_median_cat45_mins"]]
        .mean().round(1).reset_index()
        .rename(columns={
            "wait_p90_mins":           "_seas_p90",
            "wait_median_cat123_mins": "_seas_med123",
            "wait_median_cat45_mins":  "_seas_med45",
        })
    )


def load_aihw(path: pathlib.Path) -> pd.DataFrame:
    """
    Return a pivoted AIHW table: one row per (hospital, period) with
    p90_time_departed_min and median_time_departed_min as columns.
    Period bounds are converted to UTC midnight; period_end gets +1 day
    so the range check ts < period_end_utc is inclusive of the last day.
    """
    df = pd.read_csv(path)
    df["hospital"] = df["hospital"].str.strip().replace(AIHW_NAME_MAP)

    relevant = df[
        (df["triage_category"] == "All patients") &
        (df["measure_alias"].isin(["p90_time_departed_min", "median_time_departed_min"]))
    ].copy()

    pivoted = relevant.pivot_table(
        index=["hospital", "period_start", "period_end"],
        columns="measure_alias",
        values="value",
        aggfunc="first",
    ).reset_index()
    pivoted.columns.name = None
    pivoted = pivoted.rename(columns={
        "p90_time_departed_min":     "aihw_p90_depart_min",
        "median_time_departed_min":  "aihw_median_depart_min",
    })

    # Convert to UTC — period_end is inclusive, so add 1 day for the exclusive bound.
    pivoted["period_start_utc"] = pd.to_datetime(pivoted["period_start"], utc=True)
    pivoted["period_end_utc"]   = (
        pd.to_datetime(pivoted["period_end"], utc=True) + pd.Timedelta(days=1)
    )
    return pivoted

# ── Feature engineering ───────────────────────────────────────────────────────

def _parse_time_to_minutes(time_str) -> int:
    if pd.isna(time_str) or time_str == "N/A":
        return 0
    hours, minutes = 0, 0
    hr_match  = re.search(r"(\d+)\s*hr",  str(time_str))
    min_match = re.search(r"(\d+)\s*min", str(time_str))
    if hr_match:  hours   = int(hr_match.group(1))
    if min_match: minutes = int(min_match.group(1))
    return hours * 60 + minutes


def _advanced_features(dt) -> tuple:
    d = dt.date()
    is_h = int(d in VIC_HOLIDAYS)
    is_eve = int((d + timedelta(days=1)) in VIC_HOLIDAYS)
    if is_h:
        day_type = 2
    elif d.weekday() >= 5:
        day_type = 1
    else:
        day_type = 0
    m = d.month
    season = 1 if m in (12, 1, 2) else 2 if m in (3, 4, 5) else 3 if m in (6, 7, 8) else 4
    return is_h, is_eve, day_type, season


def add_silver_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ts_local = df["timestamp"].dt.tz_convert(MELBOURNE)

    df["hour"]       = ts_local.dt.hour
    df["day_of_week"] = ts_local.dt.weekday
    df["is_weekend"]  = (df["day_of_week"] >= 5).astype(int)

    adv = ts_local.apply(_advanced_features)
    df[["is_holiday", "is_eve", "day_type", "season"]] = pd.DataFrame(
        adv.tolist(), index=df.index
    )

    splits = df["wait_time"].fillna("").str.split(" - ", expand=True)
    df["min_wait_mins"] = splits[0].apply(_parse_time_to_minutes)
    df["max_wait_mins"] = splits[1].apply(_parse_time_to_minutes) if 1 in splits.columns else 0

    df["load_ratio"] = (df["waiting"] / df["treating"].replace(0, 1)).round(2)
    return df

# ── Context join ──────────────────────────────────────────────────────────────

def _join_vahi(bronze: pd.DataFrame, vahi: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Left-join VAHI quarterly benchmarks onto Bronze via timestamp interval match.
    Returns (vahi_matched_df, unmatched_df).
    """
    b = bronze.sort_values("timestamp").reset_index(drop=True)
    v = vahi.sort_values("quarter_start_utc")

    _VAHI_CTX = [
        "network",
        "wait_p90_mins", "wait_median_cat123_mins", "wait_median_cat45_mins",
        "los_pct_under_4hr", "los_pct_over_24hr", "non_admitted_los_pct_under_4hr",
    ]

    merged = pd.merge_asof(
        b,
        v[["hospital", "quarter_start_utc", "quarter_end_utc"] + _VAHI_CTX],
        left_on="timestamp",
        right_on="quarter_start_utc",
        by="hospital",
        direction="backward",
    )

    in_quarter = (
        merged["quarter_start_utc"].notna() &
        (merged["timestamp"] < merged["quarter_end_utc"])
    )

    matched = merged[in_quarter].copy().drop(columns=["quarter_start_utc", "quarter_end_utc"])
    matched = matched.rename(columns={
        "network":                          "ctx_network",
        "wait_p90_mins":                    "ctx_wait_p90_mins",
        "wait_median_cat123_mins":          "ctx_wait_median_cat123_mins",
        "wait_median_cat45_mins":           "ctx_wait_median_cat45_mins",
        "los_pct_under_4hr":               "ctx_los_pct_under_4hr",
        "los_pct_over_24hr":               "ctx_los_pct_over_24hr",
        "non_admitted_los_pct_under_4hr":  "ctx_non_admitted_los_pct_under_4hr",
    })
    matched["ctx_source"] = "VAHI"

    # Overwrite wait benchmark columns with seasonally-adjusted YoY averages.
    # The QOY is derived from the Bronze timestamp (not the VAHI quarter start),
    # so PROXY rows (which cover Q1+Q2 2026 with Q4-2025 values) are corrected
    # to reflect the observed seasonal level for that quarter of the year.
    seasonal = _compute_seasonal_benchmarks(vahi)
    matched["_qoy"] = (
        matched["timestamp"].dt.tz_convert(MELBOURNE).dt.month.map(_QUARTER_MONTH_TO_QOY)
    )
    matched = matched.merge(seasonal, on=["hospital", "_qoy"], how="left")
    for ctx_col, seas_col in [
        ("ctx_wait_p90_mins",           "_seas_p90"),
        ("ctx_wait_median_cat123_mins", "_seas_med123"),
        ("ctx_wait_median_cat45_mins",  "_seas_med45"),
    ]:
        matched[ctx_col] = matched[seas_col].combine_first(matched[ctx_col])
    matched = matched.drop(columns=["_qoy", "_seas_p90", "_seas_med123", "_seas_med45"])

    unmatched = merged[~in_quarter].copy().drop(
        columns=["quarter_start_utc", "quarter_end_utc"] + _VAHI_CTX
    )
    return matched, unmatched


def _join_aihw_fallback(
    unmatched: pd.DataFrame,
    aihw: pd.DataFrame,
    vahi_los_means: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach AIHW annual context to rows that had no VAHI quarter.
    Raises ValueError if any row has no coverage in either source.

    AIHW provides wait-time proxies (total departure times, ctx_source=="AIHW").
    LOS percentage columns have no AIHW equivalent; they are filled from
    per-hospital means across available VAHI quarters so no nulls reach training.
    ctx_source=="AIHW" downstream signals that ctx values are lower-fidelity.
    """
    if unmatched.empty:
        return unmatched

    u = unmatched.sort_values("timestamp").reset_index(drop=True)
    a = aihw.sort_values("period_start_utc")

    merged = pd.merge_asof(
        u,
        a[["hospital", "period_start_utc", "period_end_utc",
           "aihw_p90_depart_min", "aihw_median_depart_min"]],
        left_on="timestamp",
        right_on="period_start_utc",
        by="hospital",
        direction="backward",
    )

    in_period = (
        merged["period_start_utc"].notna() &
        (merged["timestamp"] < merged["period_end_utc"])
    )

    if not in_period.all():
        gap_rows = merged.loc[~in_period, ["hospital", "timestamp"]]
        raise ValueError(
            f"{(~in_period).sum()} Bronze row(s) have no coverage in either "
            f"VAHI or AIHW — add data or extend AIHW backfill:\n{gap_rows.head()}"
        )

    merged["ctx_source"] = "AIHW"
    merged = merged.rename(columns={
        "aihw_p90_depart_min":    "ctx_wait_p90_mins",
        "aihw_median_depart_min": "ctx_wait_median_cat123_mins",
    })
    merged["ctx_wait_median_cat45_mins"] = merged["ctx_wait_median_cat123_mins"]
    merged = merged.drop(columns=["period_start_utc", "period_end_utc"])

    # LOS pct columns have no AIHW equivalent — use per-hospital VAHI means.
    merged = merged.merge(vahi_los_means, on="hospital", how="left")
    merged = merged.rename(columns={
        "los_pct_under_4hr":               "ctx_los_pct_under_4hr",
        "los_pct_over_24hr":               "ctx_los_pct_over_24hr",
        "non_admitted_los_pct_under_4hr":  "ctx_non_admitted_los_pct_under_4hr",
    })
    return merged

# ── Dedup ─────────────────────────────────────────────────────────────────────

def dedup_consecutive(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop consecutive rows per hospital where min_wait_mins, max_wait_mins, and
    load_ratio are all unchanged — i.e., scraper ran but the website hadn't updated.
    max_wait_mins is included so rows where only the upper bound shifts are kept.
    Context columns are not considered: a VAHI quarter boundary alone does not
    constitute a meaningful change worth keeping.
    """
    df = df.sort_values(["hospital", "timestamp"]).reset_index(drop=True)
    prev_min  = df.groupby("hospital")["min_wait_mins"].shift(1)
    prev_max  = df.groupby("hospital")["max_wait_mins"].shift(1)
    prev_load = df.groupby("hospital")["load_ratio"].shift(1)
    mask = (
        (df["min_wait_mins"] == prev_min) &
        (df["max_wait_mins"] == prev_max) &
        (df["load_ratio"]    == prev_load)
    )
    return df[~mask].reset_index(drop=True)

# ── Wait momentum ────────────────────────────────────────────────────────────

def _add_wait_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-hospital change in min_wait_mins normalised to one 15-minute cadence unit.

      momentum = (current_wait - prev_wait) / (actual_gap_min / 15)

    Computed on the already-deduped Silver so every value reflects a real
    observable change between two consecutive kept rows.
    NaN for the first row per hospital (no prior observation to diff against).

    A momentum of +5 means waits rose 5 min per 15-min cycle at the observed
    rate of change; -5 means they fell.  predict_next.py multiplies by 4 to
    project across the 60-minute horizon.
    """
    df = df.sort_values(["hospital", "timestamp"]).reset_index(drop=True)

    prev_wait = df.groupby("hospital")["min_wait_mins"].shift(1)
    prev_ts   = df.groupby("hospital")["timestamp"].shift(1)

    gap_min   = (df["timestamp"] - prev_ts).dt.total_seconds() / 60
    raw_delta = df["min_wait_mins"] - prev_wait

    # Normalise: change per 15 min, regardless of actual gap (works at both 15-min and 30-min cadence)
    df["wait_momentum"] = (raw_delta / gap_min.clip(lower=1) * 15).round(1)
    return df


# ── QC assertions ─────────────────────────────────────────────────────────────

def _assert_no_ctx_nulls(df: pd.DataFrame) -> None:
    """Fail loudly rather than silently produce NaN-poisoned training rows."""
    null_counts = df[CTX_COLS].isna().sum()
    if null_counts.any():
        raise ValueError(f"Null values found in ctx columns:\n{null_counts[null_counts > 0]}")


def _assert_bounds(df: pd.DataFrame) -> None:
    bad_load = df[(df["load_ratio"] < 0) | (df["load_ratio"] > 5)]
    bad_wait = df[(df["min_wait_mins"] < 0) | (df["min_wait_mins"] > 720)]
    if not bad_load.empty:
        print(f"  WARNING: {len(bad_load)} rows with load_ratio outside [0,5]")
    if not bad_wait.empty:
        print(f"  WARNING: {len(bad_wait)} rows with min_wait_mins outside [0,720]")

# ── Resilient save ────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, path: pathlib.Path, max_retries: int = 5) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(max_retries):
        try:
            df.to_csv(path, index=False)
            return
        except OSError as e:
            if e.errno == 16 and attempt < max_retries - 1:
                print(f"  SSD busy (attempt {attempt + 1}/{max_retries}), retrying…")
                time.sleep(2 ** attempt)
            else:
                raise

# ── Main ──────────────────────────────────────────────────────────────────────

def build_silver(
    bronze_path: pathlib.Path,
    vahi_path: pathlib.Path,
    aihw_path: pathlib.Path,
    output_path: pathlib.Path,
) -> pd.DataFrame:
    print(f"Bronze  : {bronze_path}")
    print(f"VAHI    : {vahi_path}")
    print(f"AIHW    : {aihw_path}")
    print(f"Output  : {output_path}")

    bronze = load_bronze(bronze_path)
    if bronze.empty:
        print("Bronze is empty — nothing to transform.")
        return pd.DataFrame()
    print(f"  Loaded {len(bronze):,} Bronze rows across "
          f"{bronze['hospital'].nunique()} hospitals.")

    vahi = load_vahi(vahi_path)
    aihw = load_aihw(aihw_path)

    _LOS_COLS = ["los_pct_under_4hr", "los_pct_over_24hr", "non_admitted_los_pct_under_4hr"]
    vahi_los_means = vahi.groupby("hospital")[_LOS_COLS].mean().reset_index()

    # Feature engineering (no context yet)
    silver = add_silver_features(bronze)

    # Context join
    vahi_matched, unmatched = _join_vahi(silver, vahi)
    print(f"  VAHI match : {len(vahi_matched):,} rows")

    aihw_patched = _join_aihw_fallback(unmatched, aihw, vahi_los_means)
    if not aihw_patched.empty:
        print(f"  AIHW fallback applied to {len(aihw_patched):,} rows "
              f"(timestamps outside VAHI coverage)")

    silver = pd.concat([vahi_matched, aihw_patched], ignore_index=True)

    # Dedup consecutive no-change rows
    before = len(silver)
    silver = dedup_consecutive(silver)
    dropped = before - len(silver)
    if dropped:
        print(f"  Dedup removed {dropped:,} consecutive no-change rows.")

    # Momentum (computed on deduped Silver so gaps are real observable intervals)
    silver = _add_wait_momentum(silver)

    # QC
    _assert_no_ctx_nulls(silver)
    _assert_bounds(silver)

    # Final column order
    silver = silver[SILVER_COL_ORDER]

    _save(silver, output_path)
    print(f"\nSilver written: {len(silver):,} rows → {output_path}")
    print(f"ctx_source breakdown:\n{silver['ctx_source'].value_counts().to_string()}")
    return silver


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Silver CSV from Bronze + VAHI/AIHW context.")
    parser.add_argument("--bronze", type=pathlib.Path, default=DEFAULT_BRONZE)
    parser.add_argument("--out",    type=pathlib.Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    try:
        build_silver(
            bronze_path=args.bronze,
            vahi_path=VAHI_FILE,
            aihw_path=AIHW_FILE,
            output_path=args.out,
        )
        update_status("transform_silver", "PASS")
    except Exception as e:
        update_status("transform_silver", "FAIL")
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
