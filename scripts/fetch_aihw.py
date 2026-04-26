# data-class: public-aggregate
"""
fetch_aihw.py — Fetch ED measures from the AIHW MyHospitals public API.

⚠  RUN FROM LAPTOP — myhospitals.gov.au does not resolve from the Pi.

Outputs a CSV in the same schema as bronze/eastern_hospital_historical_context.csv
so the two files can be concatenated directly.

Usage (run from repo root on a laptop with internet access):
  python3 scripts/fetch_aihw.py --list-only         # verify H-codes first
  python3 scripts/fetch_aihw.py --out bronze/monash_aihw_context.csv
  python3 scripts/fetch_aihw.py --append            # merge into main file

After --append, copy the updated bronze/ file to the Pi and run:
  python3 scripts/transform_silver.py               # rebuild Silver with full context
"""

import sys
import json
import time
import argparse
import pathlib
from datetime import datetime, timezone

import pandas as pd
import requests

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from config.hospitals import HOSPITAL_CODES  # canonical name → H-code

# Display name overrides: AIHW returns its own name; we normalise to our canonical names.
# Keys = AIHW facility name (lowercased, stripped); values = our canonical name.
NAME_OVERRIDES = {
    "maroondah hospital [east ringwood]": "Maroondah Hospital",
    "maroondah hospital":                 "Maroondah Hospital",
    "monash medical centre":              "Monash Medical Centre - Clayton",
    "monash medical centre - clayton":    "Monash Medical Centre - Clayton",
}

# Measures to fetch — must match existing file exactly
MEASURES = {
    "MYH0005": "pct_depart_within_4hr",
    "MYH0010": "pct_seen_on_time",
    "MYH0011": "presentations_count",
    "MYH0012": "presentations_count",
    "MYH0013": "p90_time_departed_min",
    "MYH0036": "median_time_departed_min",
}

# ── API ───────────────────────────────────────────────────────────────────────
BASE = "https://myhospitals.gov.au/api/v1"   # no www — Pi can't resolve www. prefix
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "hospital-monitor/1.0"})


def api_get(path: str, params: dict | None = None) -> dict | list:
    url = f"{BASE}/{path.lstrip('/')}"
    r = SESSION.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def resolve_code(code: str) -> dict | None:
    """Return the API facility object for a given H-code, or None if not found."""
    try:
        return api_get(f"facilities/{code}")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise


def canonical_name(api_name: str, code: str) -> str:
    """Map AIHW facility name to our canonical hospital name."""
    key = api_name.strip().lower()
    if key in NAME_OVERRIDES:
        return NAME_OVERRIDES[key]
    # Fall back to reverse-lookup in HOSPITAL_CODES
    for canon, hcode in HOSPITAL_CODES.items():
        if hcode == code:
            return canon
    return api_name.strip()


def fetch_measures(code: str, hospital_name: str) -> list[dict]:
    """Fetch all target measures for one facility and return normalised rows."""
    rows = []
    fetched_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for measure_code, measure_alias in MEASURES.items():
        try:
            data = api_get(f"facilities/{code}/statistics/{measure_code}")
            time.sleep(0.15)  # be polite
        except requests.HTTPError as e:
            print(f"  WARN: {hospital_name} / {measure_code} → {e.response.status_code}, skipping")
            continue

        # API returns a list of annual records
        records = data if isinstance(data, list) else data.get("data", [])
        for rec in records:
            period_start = rec.get("periodStart") or rec.get("period_start") or rec.get("year")
            period_end   = rec.get("periodEnd")   or rec.get("period_end")
            triage_cat   = rec.get("patientType") or rec.get("triage_category") or "All patients"
            value        = rec.get("value")
            units        = rec.get("unit") or rec.get("units") or ""
            measure_name = rec.get("measureName") or rec.get("name") or ""

            if value is None or period_start is None:
                continue

            rows.append({
                "period_start":   period_start,
                "period_end":     period_end,
                "hospital_code":  code,
                "hospital":       hospital_name,
                "triage_category": triage_cat,
                "measure_code":   measure_code,
                "measure_name":   measure_name,
                "measure_alias":  measure_alias,
                "value":          value,
                "units":          units,
                "source":         "AIHW MyHospitals API",
                "fetched_utc":    fetched_utc,
            })

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

_BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_OUT    = _BASE_DIR / "bronze" / "monash_aihw_context.csv"
EXISTING_FILE  = _BASE_DIR / "bronze" / "eastern_hospital_historical_context.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch AIHW ED measures for target hospitals.")
    parser.add_argument("--out",       type=pathlib.Path, default=DEFAULT_OUT,
                        help=f"Output CSV path (default: {DEFAULT_OUT})")
    parser.add_argument("--list-only", action="store_true",
                        help="Resolve facility codes and print — do not fetch measure data")
    parser.add_argument("--append",    action="store_true",
                        help=f"Append fetched rows directly to {EXISTING_FILE}")
    args = parser.parse_args()

    # ── Step 1: resolve codes ────────────────────────────────────────────────
    print("Resolving facility codes…")
    resolved: dict[str, str] = {}   # canonical name → H-code
    for canon_name, code in HOSPITAL_CODES.items():
        facility = resolve_code(code)
        if facility is None:
            print(f"  ✗ {code} ({canon_name}) — not found; check HOSPITAL_CODES")
            continue
        api_name = facility.get("name") or facility.get("facilityName") or code
        norm = canonical_name(api_name, code)
        print(f"  ✓ {code} → '{api_name}' → canonical: '{norm}'")
        resolved[norm] = code
        time.sleep(0.1)

    if args.list_only:
        print("\nFacility codes resolved. Re-run without --list-only to fetch data.")
        return

    if not resolved:
        print("ERROR: no facilities resolved. Check HOSPITAL_CODES constants.", file=sys.stderr)
        sys.exit(1)

    # ── Step 2: fetch measures ───────────────────────────────────────────────
    all_rows: list[dict] = []
    for canon_name, code in resolved.items():
        print(f"\nFetching {canon_name} ({code})…")
        rows = fetch_measures(code, canon_name)
        print(f"  → {len(rows)} rows")
        all_rows.extend(rows)

    if not all_rows:
        print("ERROR: no data rows returned. Check API response structure.", file=sys.stderr)
        sys.exit(1)

    # ── Step 3: normalise and write ──────────────────────────────────────────
    df = pd.DataFrame(all_rows)

    # Normalise period dates to YYYY-MM-DD (API may return ISO timestamps)
    for col in ("period_start", "period_end"):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    df = df.sort_values(["hospital", "period_start", "measure_code", "triage_category"])

    # Column order must match existing file exactly
    col_order = [
        "period_start", "period_end", "hospital_code", "hospital",
        "triage_category", "measure_code", "measure_name", "measure_alias",
        "value", "units", "source", "fetched_utc",
    ]
    df = df[col_order]

    if args.append:
        existing = pd.read_csv(EXISTING_FILE)
        combined = (
            pd.concat([existing, df], ignore_index=True)
            .drop_duplicates(subset=["hospital", "period_start", "measure_code", "triage_category"])
            .sort_values(["hospital", "period_start", "measure_code"])
            .reset_index(drop=True)
        )
        combined.to_csv(EXISTING_FILE, index=False)
        print(f"\nAppended to {EXISTING_FILE}: {len(existing)} → {len(combined)} rows")
        new_hospitals = set(combined["hospital"].unique()) - set(existing["hospital"].unique())
        if new_hospitals:
            print(f"New hospitals added: {sorted(new_hospitals)}")
    else:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.out, index=False)
        print(f"\nWritten {len(df)} rows → {args.out}")
        print("Review output, then re-run with --append to merge into the main file.")
        print("\nRows per hospital:")
        print(df.groupby("hospital").size().to_string())


if __name__ == "__main__":
    main()
