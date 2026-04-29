# data-class: public-aggregate
"""
fetch_aihw.py — Fetch ED measures from the AIHW MyHospitals public API.

ℹ  Can be run from the Pi — myhospitalsapi.aihw.gov.au resolves fine (new domain).
   The old myhospitals.gov.au was blocked; this new domain is not.

The AIHW file is a backfill for Bronze rows older than Oct 2024.
All current Bronze data falls within VAHI quarterly coverage so transform_silver.py
runs fine without it (skips gracefully if file is missing or has wrong schema).

Usage (run from repo root on a laptop with internet access):
  python3 scripts/fetch_aihw.py --list-only         # verify H-codes still resolve
  python3 scripts/fetch_aihw.py --out bronze/check_aihw.csv   # preview before writing
  python3 scripts/fetch_aihw.py --append            # merge into eastern_hospital_historical_context.csv

After --append, copy bronze/ to the Pi and run:
  python3 scripts/transform_silver.py               # rebuild Silver with AIHW backfill

If the API URL or response schema changes (myhospitals.gov.au restructures periodically):
  1. Update the BASE constant below
  2. Run --list-only to confirm H-codes still resolve
  3. Check fetch_measures() key names match the new API (periodStart vs period_start, etc.)
  4. Update NAME_OVERRIDES if facility names changed
  5. Run --out to a temp file and inspect rows before --append

Current API base: https://myhospitalsapi.aihw.gov.au/api/v1
Endpoint: GET /reporting-units/{code}/data-items (bulk dump filtered locally by measure_code).
Period info fetched from GET /datasets/{dataset_id} (cached across hospitals).
API docs / Swagger: https://myhospitalsapi.aihw.gov.au/index.html
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
    "monash medical centre [clayton]":    "Monash Medical Centre - Clayton",
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
BASE = "https://myhospitalsapi.aihw.gov.au/api/v1"
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "hospital-monitor/1.0"})

_DATASET_CACHE: dict[int, dict] = {}  # dataset_id → {period_start, period_end, triage_category, measure_name}


def api_get(path: str, params: dict | None = None) -> dict | list:
    url = f"{BASE}/{path.lstrip('/')}"
    r = SESSION.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def resolve_code(code: str) -> dict | None:
    """Return the API reporting-unit object for a given H-code, or None if not found."""
    try:
        resp = api_get(f"reporting-units/{code}")
        return resp.get("result", resp) if isinstance(resp, dict) else resp
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise


def canonical_name(api_name: str, code: str) -> str:
    """Map AIHW facility name to our canonical hospital name."""
    key = api_name.strip().lower()
    if key in NAME_OVERRIDES:
        return NAME_OVERRIDES[key]
    for canon, hcode in HOSPITAL_CODES.items():
        if hcode == code:
            return canon
    return api_name.strip()


def _dataset_info(dataset_id: int) -> dict:
    """Fetch and cache period + triage info for a dataset_id."""
    if dataset_id not in _DATASET_CACHE:
        try:
            ds = api_get(f"datasets/{dataset_id}")["result"]
            rms = ds.get("reported_measure_summary", {})
            ms  = rms.get("measure_summary", {})
            _DATASET_CACHE[dataset_id] = {
                "period_start":    ds["reporting_start_date"],
                "period_end":      ds["reporting_end_date"],
                "triage_category": rms.get("reported_measure_name", ""),
                "measure_name":    ms.get("measure_name", ""),
            }
            time.sleep(0.1)
        except Exception as e:
            _DATASET_CACHE[dataset_id] = {}  # mark as failed so we don't retry
    return _DATASET_CACHE[dataset_id]


def fetch_measures(code: str, hospital_name: str) -> list[dict]:
    """
    Fetch all target measures for one reporting unit via the v1 bulk data-items endpoint.
    Filters to MEASURES codes, resolves period/triage via cached dataset lookups.
    """
    fetched_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []

    try:
        data = api_get(f"reporting-units/{code}/data-items")
    except requests.HTTPError as e:
        print(f"  WARN: {hospital_name} data-items → {e.response.status_code}, skipping")
        return rows

    items = data.get("result", []) if isinstance(data, dict) else data

    # Pre-collect unique dataset_ids for our target measures to show progress
    target_items = [i for i in items if i.get("measure_code") in MEASURES]
    unique_ds    = {i["data_set_id"] for i in target_items}
    print(f"  {len(target_items)} target items across {len(unique_ds)} datasets — resolving periods…")

    for item in target_items:
        measure_code = item["measure_code"]
        ds_id        = item["data_set_id"]
        value        = item.get("value")

        if value is None:
            continue

        ds = _dataset_info(ds_id)
        if not ds:
            continue

        rows.append({
            "period_start":    ds["period_start"],
            "period_end":      ds["period_end"],
            "hospital_code":   code,
            "hospital":        hospital_name,
            "triage_category": ds["triage_category"],
            "measure_code":    measure_code,
            "measure_name":    ds["measure_name"],
            "measure_alias":   MEASURES[measure_code],
            "value":           value,
            "units":           "",
            "source":          "AIHW MyHospitals API v1",
            "fetched_utc":     fetched_utc,
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
        api_name = facility.get("reporting_unit_name") or facility.get("name") or facility.get("facilityName") or code
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
        _DEDUP_COLS = ["hospital", "period_start", "measure_code", "triage_category"]
        existing = pd.DataFrame()
        if EXISTING_FILE.exists():
            try:
                _ex = pd.read_csv(EXISTING_FILE)
                if set(_DEDUP_COLS).issubset(_ex.columns):
                    existing = _ex
                else:
                    print(f"  NOTE: {EXISTING_FILE.name} has unexpected schema — starting fresh.")
            except Exception as e:
                print(f"  NOTE: could not read existing file ({e}) — starting fresh.")

        combined = (
            pd.concat([existing, df], ignore_index=True)
            .drop_duplicates(subset=_DEDUP_COLS)
            .sort_values(["hospital", "period_start", "measure_code"])
            .reset_index(drop=True)
        )
        combined.to_csv(EXISTING_FILE, index=False)
        prev = len(existing)
        print(f"\nWritten to {EXISTING_FILE}: {prev} existing → {len(combined)} rows")
        new_hospitals = set(combined["hospital"].unique()) - set(existing["hospital"].unique()) if not existing.empty else set(combined["hospital"].unique())
        if new_hospitals:
            print(f"Hospitals in file: {sorted(new_hospitals)}")
    else:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.out, index=False)
        print(f"\nWritten {len(df)} rows → {args.out}")
        print("Review output, then re-run with --append to merge into the main file.")
        print("\nRows per hospital:")
        print(df.groupby("hospital").size().to_string())


if __name__ == "__main__":
    main()
