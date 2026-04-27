# data-class: public-aggregate
"""
fetch_vahi.py — Merge VAHI quarterly ED wait-time and LOS CSVs into Bronze.

Processes every hospital registered in config/hospitals.csv — add a new row
there to include its VAHI data in the next rebuild.

Sources (bronze/ — downloaded manually from the VAHI Data Portal):
  vahi_90th_Percentile_Waiting_minutes.csv      -> wait_p90_mins
  vahi_Median_Waiting_Cat123_minutes.csv        -> wait_median_cat123_mins
  vahi_Median_Waiting_Cat45_minutes.csv         -> wait_median_cat45_mins
  vahi_LOS_pct_under_4hr.csv                    -> los_pct_under_4hr
  vahi_LOS_pct_over_24hr.csv                    -> los_pct_over_24hr
  vahi_NonAdmitted_LOS_pct_under_4hr.csv        -> non_admitted_los_pct_under_4hr

Note: LOS files use "Period Axis" as the quarter column; wait-time files use
"Calendar Quarter Name". load_vahi handles both transparently.

Output:
  bronze/vahi_history_merged.csv

When a new quarter of raw CSVs arrives:
  1. Drop updated files into bronze/
  2. Add the new quarter to QUARTER_BOUNDS (only needed once per quarter)
  3. python3 scripts/fetch_vahi.py
"""
import re
import pathlib
import sys
import pandas as pd
from datetime import datetime, date
from zoneinfo import ZoneInfo

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from config.hospitals import HOSPITAL_META, HOSPITAL_NETWORK

BRONZE_DIR  = pathlib.Path(__file__).resolve().parent.parent / "bronze"
OUTPUT_FILE = BRONZE_DIR / "vahi_history_merged.csv"

MELBOURNE = ZoneInfo("Australia/Melbourne")
UTC       = ZoneInfo("UTC")

TARGETS = set(HOSPITAL_META)

# quarter_end is the midnight start of the *next* quarter (exclusive boundary).
# Add new entries here as each quarterly CSV drop arrives.
QUARTER_BOUNDS: dict[str, tuple[str, str]] = {
    "Oct - Dec 2024": ("2024-10-01", "2025-01-01"),
    "Jan - Mar 2025": ("2025-01-01", "2025-04-01"),
    "Apr - Jun 2025": ("2025-04-01", "2025-07-01"),
    "Jul - Sep 2025": ("2025-07-01", "2025-10-01"),
    "Oct - Dec 2025": ("2025-10-01", "2026-01-01"),
}


def local_midnight_to_utc(date_str: str) -> str:
    """YYYY-MM-DD Melbourne midnight → UTC ISO-8601 string ending in Z."""
    dt_local = datetime.fromisoformat(date_str).replace(tzinfo=MELBOURNE)
    return dt_local.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_value(raw: object) -> float:
    """'<5' censored values → NaN; any non-numeric string → NaN."""
    s = str(raw).strip()
    if re.fullmatch(r"<\s*[\d.]+", s):
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def load_vahi(path: pathlib.Path, value_col: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "Organisation Description": "hospital",
        "Table Value":              value_col,
        "Calendar Quarter Name":    "quarter",  # wait-time files
        "Period Axis":              "quarter",   # LOS files
    })
    df["hospital"] = df["hospital"].str.strip()
    df["quarter"]  = df["quarter"].str.strip()
    df = df[df["hospital"].isin(TARGETS)].copy()

    raw_series = df[value_col].copy()
    df[value_col] = df[value_col].apply(parse_value)
    censored = df[value_col].isna()
    if censored.any():
        bad = df.loc[censored, ["hospital", "quarter"]].copy()
        bad["raw"] = raw_series[censored].values
        print(f"  WARNING [{path.name}] {censored.sum()} value(s) coerced to NaN:")
        print(bad.to_string(index=False))

    return df[["hospital", "quarter", value_col]]


def main() -> None:
    print("Loading VAHI CSVs…")
    print(f"  Processing {len(TARGETS)} hospitals from config/hospitals.csv")

    p90        = load_vahi(BRONZE_DIR / "vahi_90th_Percentile_Waiting_minutes.csv",  "wait_p90_mins")
    cat123     = load_vahi(BRONZE_DIR / "vahi_Median_Waiting_Cat123_minutes.csv",    "wait_median_cat123_mins")
    cat45      = load_vahi(BRONZE_DIR / "vahi_Median_Waiting_Cat45_minutes.csv",     "wait_median_cat45_mins")
    los_u4     = load_vahi(BRONZE_DIR / "vahi_LOS_pct_under_4hr.csv",               "los_pct_under_4hr")
    los_o24    = load_vahi(BRONZE_DIR / "vahi_LOS_pct_over_24hr.csv",               "los_pct_over_24hr")
    los_nonadm = load_vahi(BRONZE_DIR / "vahi_NonAdmitted_LOS_pct_under_4hr.csv",   "non_admitted_los_pct_under_4hr")

    merged = (
        p90
        .merge(cat123,     on=["hospital", "quarter"], how="outer")
        .merge(cat45,      on=["hospital", "quarter"], how="outer")
        .merge(los_u4,     on=["hospital", "quarter"], how="outer")
        .merge(los_o24,    on=["hospital", "quarter"], how="outer")
        .merge(los_nonadm, on=["hospital", "quarter"], how="outer")
    )

    unknown = set(merged["quarter"]) - set(QUARTER_BOUNDS)
    if unknown:
        raise ValueError(f"Unmapped quarter(s) — add to QUARTER_BOUNDS: {unknown}")

    merged["quarter_start_utc"] = merged["quarter"].map(
        lambda q: local_midnight_to_utc(QUARTER_BOUNDS[q][0])
    )
    merged["quarter_end_utc"] = merged["quarter"].map(
        lambda q: local_midnight_to_utc(QUARTER_BOUNDS[q][1])
    )
    merged["network"]    = merged["hospital"].map(HOSPITAL_NETWORK)
    merged["source"]     = "VAHI"
    merged["data_class"] = "public-aggregate"

    merged = (
        merged[[
            "hospital", "network", "quarter", "quarter_start_utc", "quarter_end_utc",
            "wait_p90_mins", "wait_median_cat123_mins", "wait_median_cat45_mins",
            "los_pct_under_4hr", "los_pct_over_24hr", "non_admitted_los_pct_under_4hr",
            "source", "data_class",
        ]]
        .sort_values(["hospital", "quarter_start_utc"])
        .reset_index(drop=True)
    )

    # ── QC ────────────────────────────────────────────────────────────────────
    for col in ("quarter_start_utc", "quarter_end_utc"):
        bad = merged[~merged[col].str.endswith("Z")]
        if not bad.empty:
            raise ValueError(f"Non-UTC timestamp in {col}:\n{bad}")

    for q in sorted(merged["quarter"].unique()):
        found   = set(merged.loc[merged["quarter"] == q, "hospital"])
        missing = TARGETS - found
        if missing:
            print(f"  WARNING: {missing} absent from quarter '{q}'")

    unmapped = merged[merged["network"].isna()]["hospital"].unique()
    if len(unmapped):
        raise ValueError(f"Hospitals missing from HOSPITAL_NETWORK: {unmapped}")

    # ── Forward-fill proxy quarters to cover today ────────────────────────────
    # VAHI publishes with a ~3-month lag. Rows beyond the last real quarter_end
    # would otherwise match nothing in transform_silver. We tag them VAHI_PROXY.
    today       = date.today()
    q_end_month = ((today.month - 1) // 3 + 1) * 3 + 1
    q_end_year  = today.year + (1 if q_end_month > 12 else 0)
    q_end_month = q_end_month if q_end_month <= 12 else q_end_month - 12
    proxy_end   = local_midnight_to_utc(f"{q_end_year:04d}-{q_end_month:02d}-01")

    last_real_end = merged["quarter_end_utc"].max()
    if proxy_end > last_real_end:
        latest = merged[merged["quarter_end_utc"] == last_real_end].copy()
        latest["quarter_start_utc"] = last_real_end
        latest["quarter_end_utc"]   = proxy_end
        latest["quarter"]           = "PROXY"
        latest["source"]            = "VAHI_PROXY"
        merged = pd.concat([merged, latest], ignore_index=True)
        print(f"  Proxy quarter appended: {last_real_end} → {proxy_end}")

    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"\nWritten {len(merged)} rows → {OUTPUT_FILE}")
    print(f"Hospitals covered: {sorted(merged['hospital'].unique())}")


if __name__ == "__main__":
    main()
