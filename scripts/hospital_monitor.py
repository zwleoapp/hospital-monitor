# data-class: public-aggregate
import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from curl_cffi import requests
import csv
import os
import re
import json
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime
from status import update_status
from config.hospitals import SOURCES

CSV_PATH = "/mnt/router_ssd/Data_Hub/Waiting_Live_time/eastern_hospital.csv"
CSV_HEADER = ["timestamp", "hospital", "waiting", "treating",
              "wait_time", "min_wait_mins", "max_wait_mins"]
LAST_UPDATED_SIDECAR = "/mnt/router_ssd/Data_Hub/Waiting_Live_time/monash_last_updated.json"

# Clinical Raw persistence: Trust vs. Pulse dual-stream architecture
BRONZE_RAW_PATH = "/mnt/router_ssd/Data_Hub/Waiting_Live_time/bronze_raw_scrapes.csv"
BRONZE_RAW_HEADER = ["site", "scrape_timestamp_utc", "reported_timestamp_str",
                     "reported_waiting", "reported_wait_str",
                     "raw_query_waiting", "raw_query_treating", "raw_query_max_wait",
                     "is_adult_filtered"]


_MELB = ZoneInfo("Australia/Melbourne")


# ── Sidecar helpers ───────────────────────────────────────────────────────────

def _merge_last_updated_sidecar(updates: dict[str, str]) -> None:
    """Merge new {hospital: timestamp} entries into the shared sidecar (read→update→write)."""
    if not updates:
        return
    existing: dict = {}
    try:
        if os.path.exists(LAST_UPDATED_SIDECAR):
            existing = json.loads(pathlib.Path(LAST_UPDATED_SIDECAR).read_text())
    except Exception:
        pass
    existing.update(updates)
    try:
        os.makedirs(os.path.dirname(LAST_UPDATED_SIDECAR), exist_ok=True)
        with open(LAST_UPDATED_SIDECAR, "w") as _f:
            json.dump(existing, _f)
    except Exception as e:
        print(f"  Sidecar write failed: {e}")


def _extract_eh_page_timestamp(html: str, resp) -> str:
    """
    Try to extract a native 'Last Updated' timestamp from the Eastern Health page.
    Falls back to the HTTP Date response header (marked with ~ to signal approximate).
    """
    patterns = [
        r'lastUpdated\s*[=:]\s*["\']([^"\']+)["\']',
        r'last[_-]?updated[_-]?(?:at|time)?[\s:=]+["\']?(\d{1,2}[:/]\d{2}(?:[:/]\d{2,4})?\s*(?:[AP]M)?)["\']?',
        r'(?:data\s+as\s+at|updated)\s*[:\-]\s*(\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}\s*(?:[AP]M)?)',
        r'<[^>]*(?:last.?update|refresh.?time)[^>]*>([^<]{4,40})<',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if val:
                return val

    # Fall back to HTTP Date header — always available, marks as approximate with ~
    date_hdr = getattr(resp, "headers", {}).get("Date", "")
    if date_hdr:
        try:
            dt = parsedate_to_datetime(date_hdr).astimezone(_MELB)
            return "~" + dt.strftime("%H:%M")  # ~ = server clock, not hospital-published
        except Exception:
            pass
    return ""


# ── Shared helpers ────────────────────────────────────────────────────────────

def format_time(minutes: int) -> str:
    """Convert integer minutes → 'X hr Y min' string."""
    try:
        m = int(minutes)
        if m < 60:
            return f"{m} min"
        h, r = divmod(m, 60)
        return f"{h} hr" if r == 0 else f"{h} hr {r} min"
    except (ValueError, TypeError):
        return "N/A"


def _parse_wait_str(s) -> int:
    """Best-effort parse of any wait-time string to integer minutes. Returns 0 on failure."""
    if isinstance(s, (int, float)):
        return int(s)
    text = str(s).lower()
    h = re.search(r'(\d+)\s*h', text)
    m = re.search(r'(\d+)\s*m', text)
    return (int(h.group(1)) * 60 if h else 0) + (int(m.group(1)) if m else 0)


# ── Eastern Health scraper (HTML + embedded JS) ───────────────────────────────

def _scrape_html_source(source_key: str, cfg: dict, timestamp: str) -> tuple[list, list]:
    """
    GET the dashboard page; parse JS-embedded patientCounts + predictedWaitMinutes.
    Returns (bronze_rows, raw_scrape_rows) for dual persistence.
    """
    resp = requests.get(cfg["url"], impersonate="chrome120", timeout=20)
    if resp.status_code != 200:
        print(f"  [{source_key}] HTTP {resp.status_code}")
        return [], []

    html = resp.text
    counts_m = re.search(r'const patientCounts\s*=\s*(\{.*?\});', html, re.DOTALL)
    waits_m  = re.search(r'const predictedWaitMinutes\s*=\s*(\{.*?\});', html, re.DOTALL)
    if not counts_m or not waits_m:
        print(f"  [{source_key}] Data variables not found in HTML.")
        return [], []

    counts = json.loads(counts_m.group(1))
    waits  = json.loads(waits_m.group(1))

    page_ts = _extract_eh_page_timestamp(html, resp)

    rows = []
    raw_rows = []
    last_updated_map: dict[str, str] = {}
    for js_key, formal_name in cfg["hospitals"].items():
        c = counts.get(js_key, {})
        w = waits.get(js_key, {})
        waiting  = c.get("waiting",      0)
        treating = c.get("beingTreated", 0)
        min_raw  = int(w.get("min", 0))
        max_raw  = int(w.get("max", 0))
        min_fmt  = format_time(min_raw)
        max_fmt  = format_time(max_raw)
        wait_str = f"{min_fmt} - {max_fmt}" if min_fmt != "N/A" else "N/A"
        rows.append([timestamp, formal_name, waiting, treating,
                     wait_str, min_raw, max_raw])
        # Clinical raw: site, scrape_timestamp_utc, reported_timestamp_str,
        #               reported_waiting, reported_wait_str,
        #               raw_query_waiting, raw_query_treating, raw_query_max_wait,
        #               is_adult_filtered
        raw_rows.append([
            formal_name,
            timestamp,
            page_ts or "",
            waiting,        # reported_waiting
            wait_str,       # reported_wait_str
            waiting,        # raw_query_waiting (same as reported for html_js)
            treating,       # raw_query_treating (same as reported for html_js)
            max_raw,        # raw_query_max_wait
            "All"           # is_adult_filtered (Eastern Health doesn't split Adult/Paeds)
        ])
        if page_ts:
            last_updated_map[formal_name] = page_ts

    if last_updated_map:
        _merge_last_updated_sidecar(last_updated_map)
        suffix = " (native)" if not page_ts.startswith("~") else " (HTTP header fallback)"
        print(f"  [{source_key}] Page timestamp: {page_ts}{suffix}")
    else:
        print(f"  [{source_key}] No page timestamp found.")

    return rows, raw_rows


# ── Monash Health scraper (Power BI Embedded batch API) ───────────────────────

def _extract_dsr_value(result_obj: dict):
    """Navigate Power BI DSR envelope: result.data.dsr.DS[0].PH[0].DM0[0].M0"""
    try:
        return (result_obj["result"]["data"]["dsr"]
                          ["DS"][0]["PH"][0]["DM0"][0]["M0"])
    except (KeyError, IndexError, TypeError):
        return None


def _extract_dsr_timestamp(result_obj: dict) -> str | None:
    """
    Extract timestamp from Power BI DSR response for timestamp-only queries.

    Timestamp queries return: result.data.dsr.DS[0].PH[0].DM0[0].G0
    (different from metric queries which use M0)
    """
    try:
        return (result_obj["result"]["data"]["dsr"]
                          ["DS"][0]["PH"][0]["DM0"][0]["G0"])
    except (KeyError, IndexError, TypeError):
        return None


def _build_pbi_timestamp_query(job_id: str, entity: str, hospital_col: str,
                                hospital_filter: str, col_last_updated: str) -> dict:
    """
    Build a timestamp-only query for a specific campus.

    Separate from the grouped query to extract per-campus LastUpdatedDisplay
    without grouping by AdultPaed, which can cause report-level aggregation.
    """
    def _col(prop):
        return {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": prop}}

    return {
        "Query": {
            "Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [{"Name": "t", "Entity": entity, "Type": 0}],
                        "Select": [{**_col(col_last_updated), "Name": "T0"}],
                        "Where": [{
                            "Condition": {
                                "Comparison": {
                                    "ComparisonKind": 0,
                                    "Left": _col(hospital_col),
                                    "Right": {"Literal": {"Value": f"'{hospital_filter}'"}}
                                }
                            }
                        }],
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0]}]},
                        "DataReduction": {"DataVolume": 4, "Primary": {"Top": {}}},
                        "Version": 1,
                    },
                }
            }]
        },
        "QueryId": job_id,
    }


def _build_pbi_grouped_query(job_id: str, entity: str, hospital_col: str,
                              hospital_filter: str, group_col: str,
                              col_waiting: str, col_treating: str,
                              col_wait_str: str,
                              col_last_updated: str | None = None) -> dict:
    """
    Build a grouped SemanticQueryDataShapeCommand query for one campus.

    Groups by group_col (AdultPaed) and selects col_waiting, col_treating,
    col_wait_str columns, and optionally col_last_updated (G4) for native
    hospital data freshness. The response DSR contains one row per group value.
    We pick the target group row in _scrape_powerbi_source.
    """
    def _col(prop):
        return {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": prop}}

    select = [
        {**_col(group_col),    "Name": "G0"},
        {**_col(col_waiting),  "Name": "G1"},
        {**_col(col_treating), "Name": "G2"},
        {**_col(col_wait_str), "Name": "G3"},
    ]
    if col_last_updated:
        select.append({**_col(col_last_updated), "Name": "G4"})

    return {
        "Query": {
            "Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [{"Name": "t", "Entity": entity, "Type": 0}],
                        "Select": select,
                        "Where": [{"Condition": {"Comparison": {
                            "ComparisonKind": 0,
                            "Left":  _col(hospital_col),
                            "Right": {"Literal": {"Value": f"'{hospital_filter}'"}},
                        }}}],
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": list(range(len(select)))}]},
                        "DataReduction": {"DataVolume": 4, "Primary": {"Top": {}}},
                        "Version": 1,
                    },
                }
            }]
        },
        "QueryId": job_id,
    }


def _parse_grouped_dsr(result_obj: dict, group_target: str) -> dict | None:
    """
    Extract the target group row from a Power BI grouped DSR response.

    DSR format:
      DS[0].PH[0].DM0[i]  — row i, containing:
        S  — column schema (only on first row; entry has optional DN for dict-encoding)
        C  — values for non-repeated columns only
        R  — repeat bitmask: bit i set means col i is unchanged from the previous row
      DS[0].ValueDicts     — {dictName: [values]} for DN-encoded string columns

    Power BI uses delta/repeat compression: a row's C array may be shorter than
    n_cols because unchanged columns are omitted and flagged via R.  The old
    'if len(c) < 4: continue' check silently dropped these rows, causing the
    wrong group (e.g. Paed instead of Adult) to be returned.  We now reconstruct
    the full column vector before matching.
    """
    try:
        ds0    = result_obj["result"]["data"]["dsr"]["DS"][0]
        rows   = ds0["PH"][0]["DM0"]
        vdicts = ds0.get("ValueDicts", {})
        schema = rows[0]["S"]
    except (KeyError, IndexError, TypeError):
        return None

    n_cols = len(schema)

    def _decode(c_val, col_idx):
        if col_idx >= n_cols or c_val is None:
            return c_val
        s = schema[col_idx]
        if "DN" in s and isinstance(c_val, int):
            return vdicts.get(s["DN"], [])[c_val]
        return c_val

    prev_c      = [None] * n_cols
    first_valid = None

    for row in rows:
        c_raw  = row.get("C", [])
        r_mask = row.get("R", 0)   # bit i set → col i repeats from previous row

        # Reconstruct the full n_cols vector honouring the repeat bitmask
        full_c  = list(prev_c)
        raw_idx = 0
        for col_idx in range(n_cols):
            if r_mask & (1 << col_idx):
                pass  # keep prev_c[col_idx]
            else:
                if raw_idx < len(c_raw):
                    full_c[col_idx] = c_raw[raw_idx]
                raw_idx += 1
        prev_c = list(full_c)

        if full_c[0] is None:
            continue

        g0_raw = _decode(full_c[0], 0)
        decoded = {
            "group":        g0_raw,
            "waiting":      _decode(full_c[1], 1),
            "treating":     _decode(full_c[2], 2),
            "wait_str":     str(_decode(full_c[3], 3) or "").strip(),
            "last_updated": str(_decode(full_c[4], 4) or "").strip() if n_cols > 4 else "",
        }
        if first_valid is None:
            first_valid = decoded
        if str(g0_raw) == group_target:
            return decoded

    # Campus has no Adult/Paeds split — return the single row
    return first_valid


def _parse_grouped_dsr_maxwait(result_obj: dict) -> int | None:
    """
    Scan ALL groups (Adult + Paediatric + any others) in the same DSR response
    and return the highest wait upper-bound in minutes.

    The grouped query already returns every patient-category row for a campus.
    By taking the max across all of them we capture the true wait ceiling —
    e.g. an 8h 51m Paediatric outlier that the Adult-only filter would miss.
    Uses the same delta/repeat DSR reconstruction as _parse_grouped_dsr.
    G3 is always col_wait_str ("Estimated Time").
    """
    try:
        ds0    = result_obj["result"]["data"]["dsr"]["DS"][0]
        rows   = ds0["PH"][0]["DM0"]
        vdicts = ds0.get("ValueDicts", {})
        schema = rows[0]["S"]
    except (KeyError, IndexError, TypeError):
        return None

    n_cols = len(schema)

    def _decode(c_val, col_idx):
        if col_idx >= n_cols or c_val is None:
            return c_val
        s = schema[col_idx]
        if "DN" in s and isinstance(c_val, int):
            return vdicts.get(s["DN"], [])[c_val]
        return c_val

    prev_c   = [None] * n_cols
    max_mins = None

    for row in rows:
        c_raw  = row.get("C", [])
        r_mask = row.get("R", 0)
        full_c = list(prev_c)
        raw_idx = 0
        for col_idx in range(n_cols):
            if r_mask & (1 << col_idx):
                pass
            else:
                if raw_idx < len(c_raw):
                    full_c[col_idx] = c_raw[raw_idx]
                raw_idx += 1
        prev_c = list(full_c)

        if n_cols <= 3:
            continue
        wait_str = str(_decode(full_c[3], 3) or "").strip()
        if not wait_str:
            continue
        # Upper bound is after " - " separator, or the whole string if no range
        upper_str = wait_str.split(" - ")[-1]
        upper_mins = _parse_wait_str(upper_str)
        if upper_mins > 0:
            max_mins = max(max_mins or 0, upper_mins)

    return max_mins


def _scrape_powerbi_source(source_key: str, cfg: dict, timestamp: str) -> tuple[list, list]:
    """
    Hybrid scraper: Power BI API for metrics + HTML scraping for per-campus timestamps.

    ARCHITECTURE (Trust vs. Pulse):
    - Power BI API: raw_query_* columns (ML momentum, real-time pressure)
    - Webpage HTML: reported_* columns (UI truth, what users see)

    Power BI LastUpdatedDisplay (G4) is report-level, not per-campus. To match the
    webpage, we scrape HTML to extract the visual tile timestamps.

    Returns (bronze_rows, raw_scrape_rows) for dual persistence.
    """
    endpoint     = cfg.get("endpoint")
    model_id     = cfg.get("model_id")
    resource_key = cfg.get("resource_key")

    if not all([endpoint, model_id, resource_key]):
        missing = [k for k, v in {"endpoint": endpoint,
                                   "model_id": model_id,
                                   "resource_key": resource_key}.items() if not v]
        print(f"  [{source_key}] Power BI not configured — set {missing} in config/hospitals.py")
        return [], []

    entity           = cfg.get("entity",           "CurrentPatients")
    hospital_col     = cfg.get("hospital_col",     "Campus")
    group_col        = cfg.get("group_col",        "AdultPaed")
    group_target     = cfg.get("group_target",     "Adult")
    col_waiting      = cfg.get("col_waiting",      "TotalWaiting")
    col_treating     = cfg.get("col_treating",     "TotalBeingTreated")
    col_wait_str     = cfg.get("col_wait_str",     "Estimated Time")
    col_last_updated = cfg.get("col_last_updated")   # optional; None for non-PBI sources
    hospitals        = cfg["hospitals"]

    # Step 1: Query Power BI for per-campus timestamps (separate queries per campus)
    pbi_timestamps: dict[str, str] = {}
    if col_last_updated:
        timestamp_queries = []
        timestamp_order = []  # Track (campus_filter, formal_name) order

        for campus_filter, formal_name in hospitals.items():
            job_id = f"timestamp_{campus_filter}_{uuid.uuid4().hex[:8]}"
            timestamp_queries.append(_build_pbi_timestamp_query(
                job_id=job_id,
                entity=entity,
                hospital_col=hospital_col,
                hospital_filter=campus_filter,
                col_last_updated=col_last_updated
            ))
            timestamp_order.append((campus_filter, formal_name))

        # Send timestamp queries in a separate batch
        ts_payload = {
            "version": "1.0.0",
            "queries": timestamp_queries,
            "cancelQueries": [],
            "modelId": model_id,
            "clientRequestId": f"timestamps_{uuid.uuid4().hex}",
        }

        try:
            ts_resp = requests.post(
                endpoint, json=ts_payload,
                headers={"Content-Type": "application/json",
                         "X-PowerBI-ResourceKey": resource_key},
                impersonate="chrome120", timeout=30,
            )
            if ts_resp.status_code == 200:
                ts_results = ts_resp.json().get("results", [])
                for i, (campus_filter, formal_name) in enumerate(timestamp_order):
                    if i < len(ts_results):
                        ts_val = _extract_dsr_timestamp(ts_results[i])
                        if ts_val is not None:
                            pbi_timestamps[formal_name] = str(ts_val)
                if pbi_timestamps:
                    print(f"  [{source_key}] Per-campus timestamps extracted via separate queries: {len(pbi_timestamps)} campuses")
                else:
                    print(f"  [{source_key}] WARNING: Timestamp queries returned no values — using report-level fallback")
            else:
                print(f"  [{source_key}] Timestamp query HTTP {ts_resp.status_code} — using report-level fallback")
        except Exception as e:
            print(f"  [{source_key}] Timestamp query failed: {e} — using report-level fallback")

    # Step 2: Query Power BI for patient metrics (grouped by AdultPaed)
    queries      = []
    campus_order = []   # preserves (filter, formal_name) order for result mapping

    # Unique suffix per scrape cycle — Power BI uses QueryId for server-side
    # result caching; a different value each call forces a fresh execution.
    bust_id = uuid.uuid4().hex

    for campus_filter, formal_name in hospitals.items():
        queries.append(_build_pbi_grouped_query(
            job_id           = f"{campus_filter}_{bust_id}",
            entity           = entity,
            hospital_col     = hospital_col,
            hospital_filter  = campus_filter,
            group_col        = group_col,
            col_waiting      = col_waiting,
            col_treating     = col_treating,
            col_wait_str     = col_wait_str,
            col_last_updated = col_last_updated,
        ))
        campus_order.append((campus_filter, formal_name))

    payload = {
        "version": "1.0.0",
        "queries": queries,
        "cancelQueries": [],
        "modelId": model_id,
        "clientRequestId": bust_id,   # top-level UID — additional PBI dedup key
    }
    resp = requests.post(
        endpoint, json=payload,
        headers={"Content-Type": "application/json",
                 "X-PowerBI-ResourceKey": resource_key},
        impersonate="chrome120", timeout=30,
    )
    if resp.status_code != 200:
        print(f"  [{source_key}] Power BI API HTTP {resp.status_code}: {resp.text[:200]}")
        return [], []

    results = resp.json().get("results", [])

    # DEBUG: Save raw Power BI response to inspect schema
    if results and len(results) > 0:
        debug_path = pathlib.Path("/tmp/powerbi_debug_response.json")
        try:
            with open(debug_path, "w") as df:
                json.dump({"timestamp": timestamp, "results": results}, df, indent=2)
            print(f"  [DEBUG] Raw Power BI response → {debug_path}")
        except Exception:
            pass

    rows = []
    raw_rows = []
    last_updated_map: dict[str, str] = {}

    for i, (campus_filter, formal_name) in enumerate(campus_order):
        if i >= len(results):
            print(f"  [{source_key}] Missing result for {formal_name}")
            continue

        row = _parse_grouped_dsr(results[i], group_target)
        if row is None:
            print(f"  [{source_key}] No '{group_target}' row found for {formal_name}")
            continue

        waiting  = int(row["waiting"]  or 0)
        treating = int(row["treating"] or 0)
        wait_str = row["wait_str"]                  # e.g. "2 hr 46 min - 6 hr 50 min"
        min_mins = _parse_wait_str(wait_str.split(" - ")[0]) if " - " in wait_str else _parse_wait_str(wait_str)
        # Scan ALL patient groups (Adult + Paed) for the true wait ceiling
        all_max  = _parse_grouped_dsr_maxwait(results[i])
        adult_max = _parse_wait_str(wait_str.split(" - ")[1]) if " - " in wait_str else min_mins
        max_mins = all_max if (all_max and all_max >= adult_max) else adult_max
        if all_max and all_max > adult_max:
            print(f"   [MAX-WAIT] {formal_name}: all-group max {all_max}m > adult max {adult_max}m")

        # Per-campus Power BI timestamp (Trust Stream — from separate timestamp query)
        pbi_campus_ts = pbi_timestamps.get(formal_name, "")

        # Report-level timestamp from grouped query (fallback)
        pbi_report_ts = row.get("last_updated", "")

        # Use per-campus timestamp if available, else fall back to report-level
        reported_ts = pbi_campus_ts if pbi_campus_ts else pbi_report_ts

        if reported_ts:
            last_updated_map[formal_name] = reported_ts

        rows.append([timestamp, formal_name, waiting, treating,
                     wait_str, min_mins, max_mins])

        # Clinical raw: site, scrape_timestamp_utc, reported_timestamp_str (webpage),
        #               reported_waiting, reported_wait_str (for UI),
        #               raw_query_waiting, raw_query_treating, raw_query_max_wait (for ML),
        #               is_adult_filtered
        raw_rows.append([
            formal_name,
            timestamp,
            reported_ts,
            waiting,        # reported_waiting (same as raw for now)
            wait_str,       # reported_wait_str (same as raw for now)
            waiting,        # raw_query_waiting (from Power BI API)
            treating,       # raw_query_treating (from Power BI API)
            max_mins,       # raw_query_max_wait (all-group scan)
            "Adult"         # is_adult_filtered (group_target applied)
        ])

        source_label = "per-campus" if pbi_campus_ts else "report-level"
        print(f"   [SCRAPED] {formal_name}: waiting={waiting}, treating={treating}, wait={wait_str}, max={max_mins}m"
              + (f", timestamp={reported_ts} ({source_label})" if reported_ts else ", timestamp=∅"))

    # If every campus returned the same timestamp, LastUpdatedDisplay is report-global
    # (not per-campus). Tag with '^' so the frontend skips per-campus stale checks.
    # Self-correcting: once a real per-campus column is configured and returns differing
    # values, the '^' is not applied.
    values = list(last_updated_map.values())
    if len(values) > 1 and len(set(values)) == 1:
        last_updated_map = {k: "^" + v for k, v in last_updated_map.items()}
        print(f"  [{source_key}] LastUpdatedDisplay is report-global (all campuses same) — tagged '^'")

    _merge_last_updated_sidecar(last_updated_map)

    return rows, raw_rows


# ── Main ──────────────────────────────────────────────────────────────────────

def scrape_hospital():
    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        all_rows  = []
        all_raw_rows = []

        for source_key, cfg in SOURCES.items():
            parser = cfg.get("parser", "html_js")
            print(f"  Scraping {source_key} ({parser})…")

            if parser == "html_js":
                if not cfg.get("url"):
                    print(f"  [{source_key}] url not set — skipping.")
                    continue
                rows, raw_rows = _scrape_html_source(source_key, cfg, timestamp)

            elif parser == "powerbi":
                if not cfg.get("endpoint"):
                    print(f"  [{source_key}] Power BI endpoint not configured — skipping.")
                    continue
                rows, raw_rows = _scrape_powerbi_source(source_key, cfg, timestamp)

            else:
                print(f"  [{source_key}] Unknown parser '{parser}' — skipping.")
                continue

            all_rows.extend(rows)
            all_raw_rows.extend(raw_rows)

        if not all_rows:
            print("No data rows collected.")
            update_status("hospital_monitor", "FAIL")
            return

        # Write to Reported Truth CSV (existing Bronze for UI)
        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
        file_exists = os.path.isfile(CSV_PATH)
        with open(CSV_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(CSV_HEADER)
            writer.writerows(all_rows)

        # Write to Clinical Raw CSV (new persistence for ML momentum)
        os.makedirs(os.path.dirname(BRONZE_RAW_PATH), exist_ok=True)
        raw_exists = os.path.isfile(BRONZE_RAW_PATH)
        with open(BRONZE_RAW_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if not raw_exists:
                writer.writerow(BRONZE_RAW_HEADER)
            writer.writerows(all_raw_rows)

        print(f"[{timestamp}] Success! {len(all_rows)} rows written to Bronze (Reported Truth).")
        print(f"[{timestamp}] {len(all_raw_rows)} rows written to Bronze Raw (Clinical Stream).")
        for row in all_rows:
            print(f" -> {row[1]}: {row[2]} waiting, {row[3]} treating. Est wait: {row[4]}")
        update_status("hospital_monitor", "PASS")

    except Exception as e:
        print(f"Extraction failed: {e}")
        update_status("hospital_monitor", "FAIL")


if __name__ == "__main__":
    scrape_hospital()
