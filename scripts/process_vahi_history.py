# data-class: public-aggregate
"""
Merge VAHI quarterly ED wait-time and LOS CSVs into a single Bronze-compatible file.

Sources (bronze/):
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
"""
import re
import pathlib
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

BRONZE_DIR = pathlib.Path(__file__).resolve().parent.parent / "bronze"
OUTPUT_FILE = BRONZE_DIR / "vahi_history_merged.csv"

MELBOURNE = ZoneInfo("Australia/Melbourne")
UTC = ZoneInfo("UTC")

TARGETS = {
    "Box Hill Hospital", "Angliss Hospital", "Maroondah Hospital",
    "Casey Hospital", "Dandenong Hospital", "Monash Medical Centre - Clayton",
}

HOSPITAL_NETWORK = {
    "Box Hill Hospital":              "Eastern Health",
    "Angliss Hospital":               "Eastern Health",
    "Maroondah Hospital":             "Eastern Health",
    "Casey Hospital":                 "Monash Health",
    "Dandenong Hospital":             "Monash Health",
    "Monash Medical Centre - Clayton":"Monash Health",
}

# quarter_end is the midnight start of the *next* quarter (exclusive boundary)
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
    """
    Convert a VAHI table value to float.
    '<5' style censored values are stored as NaN; the caller logs these.
    Any other non-numeric string is also NaN.
    """
    s = str(raw).strip()
    if re.fullmatch(r"<\s*[\d.]+", s):
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def load_vahi(path: pathlib.Path, value_col: str) -> pd.DataFrame:
    # utf-8-sig strips the BOM present on two of the three files
    df = pd.read_csv(path, encoding="utf-8-sig")

    # Normalise column names (strip leading/trailing whitespace)
    df.columns = [c.strip() for c in df.columns]

    df = df.rename(columns={
        "Organisation Description": "hospital",
        "Table Value": value_col,
        "Calendar Quarter Name": "quarter",  # wait-time files
        "Period Axis": "quarter",            # LOS files
    })

    # Strip stray whitespace from string fields
    df["hospital"] = df["hospital"].str.strip()
    df["quarter"] = df["quarter"].str.strip()

    df = df[df["hospital"].isin(TARGETS)].copy()

    # Safe numeric conversion — log any censored or unparseable values
    raw_series = df[value_col].copy()
    df[value_col] = df[value_col].apply(parse_value)
    censored_mask = df[value_col].isna()
    if censored_mask.any():
        bad = df.loc[censored_mask, ["hospital", "quarter"]].copy()
        bad["raw"] = raw_series[censored_mask].values
        print(f"  WARNING [{path.name}] {censored_mask.sum()} value(s) coerced to NaN:")
        print(bad.to_string(index=False))

    return df[["hospital", "quarter", value_col]]


def main() -> None:
    print("Loading VAHI CSVs...")
    p90       = load_vahi(BRONZE_DIR / "vahi_90th_Percentile_Waiting_minutes.csv",   "wait_p90_mins")
    cat123    = load_vahi(BRONZE_DIR / "vahi_Median_Waiting_Cat123_minutes.csv",     "wait_median_cat123_mins")
    cat45     = load_vahi(BRONZE_DIR / "vahi_Median_Waiting_Cat45_minutes.csv",      "wait_median_cat45_mins")
    los_u4    = load_vahi(BRONZE_DIR / "vahi_LOS_pct_under_4hr.csv",                "los_pct_under_4hr")
    los_o24   = load_vahi(BRONZE_DIR / "vahi_LOS_pct_over_24hr.csv",                "los_pct_over_24hr")
    los_nonadm = load_vahi(BRONZE_DIR / "vahi_NonAdmitted_LOS_pct_under_4hr.csv",   "non_admitted_los_pct_under_4hr")

    merged = (
        p90
        .merge(cat123,     on=["hospital", "quarter"], how="outer")
        .merge(cat45,      on=["hospital", "quarter"], how="outer")
        .merge(los_u4,     on=["hospital", "quarter"], how="outer")
        .merge(los_o24,    on=["hospital", "quarter"], how="outer")
        .merge(los_nonadm, on=["hospital", "quarter"], how="outer")
    )

    # Attach UTC timestamps — QC Bronze Timezone gate
    unknown = set(merged["quarter"]) - set(QUARTER_BOUNDS)
    if unknown:
        raise ValueError(f"Unmapped quarter(s) — add to QUARTER_BOUNDS: {unknown}")

    merged["quarter_start_utc"] = merged["quarter"].map(
        lambda q: local_midnight_to_utc(QUARTER_BOUNDS[q][0])
    )
    merged["quarter_end_utc"] = merged["quarter"].map(
        lambda q: local_midnight_to_utc(QUARTER_BOUNDS[q][1])
    )

    merged["network"]     = merged["hospital"].map(HOSPITAL_NETWORK)
    merged["source"]      = "VAHI"
    merged["data_class"]  = "public-aggregate"

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

    # --- QC assertions ---
    for col in ("quarter_start_utc", "quarter_end_utc"):
        bad = merged[~merged[col].str.endswith("Z")]
        if not bad.empty:
            raise ValueError(f"Non-UTC timestamp found in {col}:\n{bad}")

    for q in sorted(merged["quarter"].unique()):
        found = set(merged.loc[merged["quarter"] == q, "hospital"])
        missing = TARGETS - found
        if missing:
            print(f"  WARNING: {missing} absent from quarter '{q}'")

    # Confirm every hospital has a network assignment
    unmapped = merged[merged["network"].isna()]["hospital"].unique()
    if len(unmapped):
        raise ValueError(f"Hospitals missing from HOSPITAL_NETWORK: {unmapped}")

    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"\nWritten {len(merged)} rows → {OUTPUT_FILE}")
    print(merged.to_string(index=False))


if __name__ == "__main__":
    main()
