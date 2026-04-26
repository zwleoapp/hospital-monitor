# data-class: public-aggregate
import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from curl_cffi import requests
import csv
import os
import re
import json
from datetime import datetime, timezone
from status import update_status
from config.hospitals import SOURCES

CSV_PATH = "/mnt/router_ssd/Data_Hub/Waiting_Live_time/eastern_hospital.csv"
CSV_HEADER = ["timestamp", "hospital", "waiting", "treating",
              "wait_time", "min_wait_mins", "max_wait_mins"]


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

def _scrape_html_source(source_key: str, cfg: dict, timestamp: str) -> list:
    """GET the dashboard page; parse JS-embedded patientCounts + predictedWaitMinutes."""
    resp = requests.get(cfg["url"], impersonate="chrome120", timeout=20)
    if resp.status_code != 200:
        print(f"  [{source_key}] HTTP {resp.status_code}")
        return []

    html = resp.text
    counts_m = re.search(r'const patientCounts\s*=\s*(\{.*?\});', html)
    waits_m  = re.search(r'const predictedWaitMinutes\s*=\s*(\{.*?\});', html)
    if not counts_m or not waits_m:
        print(f"  [{source_key}] Data variables not found in HTML.")
        return []

    counts = json.loads(counts_m.group(1))
    waits  = json.loads(waits_m.group(1))

    rows = []
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
    return rows


# ── Monash Health scraper (Power BI Embedded batch API) ───────────────────────

def _extract_dsr_value(result_obj: dict):
    """Navigate Power BI DSR envelope: result.data.dsr.DS[0].PH[0].DM0[0].M0"""
    try:
        return (result_obj["result"]["data"]["dsr"]
                          ["DS"][0]["PH"][0]["DM0"][0]["M0"])
    except (KeyError, IndexError, TypeError):
        return None


def _build_pbi_query(job_id: str, entity: str, hospital_col: str,
                     hospital_filter: str, prop: str, fn: int) -> dict:
    """
    Build one SemanticQueryDataShapeCommand query for the batch payload.

    job_id          — unique string returned verbatim in the response
    entity          — Power BI table name (e.g. "CurrentPatients")
    hospital_col    — column used to filter by site (e.g. "Hospital")
    hospital_filter — literal value for the WHERE clause
    prop            — measure/column property name (e.g. "TotalWaiting")
    fn              — PBI aggregation function: 0=Sum 1=Avg 4=Min 5=Max
    """
    return {
        "Query": {
            "Commands": [
                {
                    "SemanticQueryDataShapeCommand": {
                        "Query": {
                            "Version": 2,
                            "From": [{"Name": "t", "Entity": entity, "Type": 0}],
                            "Select": [
                                {
                                    "Aggregation": {
                                        "Expression": {
                                            "Column": {
                                                "Expression": {"SourceRef": {"Source": "t"}},
                                                "Property": prop,
                                            }
                                        },
                                        "Function": fn,
                                    },
                                    "Name": f"Agg({entity}.{prop})",
                                }
                            ],
                            "Where": [
                                {
                                    "Condition": {
                                        "Comparison": {
                                            "ComparisonKind": 0,
                                            "Left": {
                                                "Column": {
                                                    "Expression": {"SourceRef": {"Source": "t"}},
                                                    "Property": hospital_col,
                                                }
                                            },
                                            "Right": {
                                                "Literal": {"Value": f"'{hospital_filter}'"}
                                            },
                                        }
                                    }
                                }
                            ],
                        },
                        "Binding": {
                            "Primary": {"Groupings": [{"Projections": [0]}]},
                            "Version": 1,
                        },
                    }
                }
            ]
        },
        "QueryId": job_id,
    }


def _scrape_powerbi_source(source_key: str, cfg: dict, timestamp: str) -> list:
    """
    Single POST to the Power BI batch API covering all hospitals × all measures.

    Response shape (per result):
      result.data.dsr.DS[0].PH[0].DM0[0].M0  →  the scalar value
    """
    endpoint     = cfg.get("endpoint")
    model_id     = cfg.get("model_id")
    resource_key = cfg.get("resource_key")
    entity       = cfg.get("entity",       "CurrentPatients")
    hospital_col = cfg.get("hospital_col", "Hospital")
    measures     = cfg.get("measures",     {})
    hospitals    = cfg["hospitals"]

    if not all([endpoint, model_id, resource_key]):
        missing = [k for k, v in {"endpoint": endpoint, "model_id": model_id,
                                   "resource_key": resource_key}.items() if not v]
        print(f"  [{source_key}] Power BI not configured — set {missing} in config/hospitals.py")
        return []

    # Build batch payload: one query per (hospital × measure)
    queries  = []
    job_map  = {}   # job_id → (formal_name, measure_key)

    for hospital_filter, formal_name in hospitals.items():
        for measure_key, m_cfg in measures.items():
            job_id = f"{formal_name}__{measure_key}"
            job_map[job_id] = (formal_name, measure_key)
            queries.append(
                _build_pbi_query(
                    job_id, entity, hospital_col,
                    hospital_filter, m_cfg["property"], m_cfg["function"],
                )
            )

    payload = {
        "version": "1.0.0",
        "queries": queries,
        "cancelQueries": [],
        "modelId": model_id,
    }
    headers = {
        "Content-Type":           "application/json",
        "X-PowerBI-ResourceKey":  resource_key,
    }

    resp = requests.post(endpoint, json=payload, headers=headers,
                         impersonate="chrome120", timeout=30)
    if resp.status_code != 200:
        print(f"  [{source_key}] Power BI API HTTP {resp.status_code}: {resp.text[:200]}")
        return []

    # Parse: collect all (hospital, measure) values
    collected = {}   # formal_name → {measure_key: raw_value}
    for result in resp.json().get("results", []):
        job_id  = result.get("jobId") or result.get("QueryId", "")
        mapping = job_map.get(job_id)
        if not mapping:
            continue
        formal_name, measure_key = mapping
        value = _extract_dsr_value(result)
        collected.setdefault(formal_name, {})[measure_key] = value

    # Build Bronze rows
    rows = []
    for formal_name, vals in collected.items():
        waiting  = int(vals.get("waiting")  or 0)
        treating = int(vals.get("treating") or 0)

        # Wait time: Power BI may return int minutes, float, or a formatted string
        wait_raw = vals.get("wait")
        if wait_raw is None:
            min_mins = max_mins = 0
        else:
            wait_mins = _parse_wait_str(wait_raw)
            min_mins  = max_mins = wait_mins   # single estimate; no range from PBI

        min_fmt  = format_time(min_mins)
        wait_str = min_fmt if min_mins == max_mins else f"{min_fmt} - {format_time(max_mins)}"

        rows.append([timestamp, formal_name, waiting, treating,
                     wait_str, min_mins, max_mins])

    print(f"  [{source_key}] {len(rows)} hospitals parsed from Power BI response.")
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def scrape_hospital():
    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        all_rows  = []

        for source_key, cfg in SOURCES.items():
            parser = cfg.get("parser", "html_js")
            print(f"  Scraping {source_key} ({parser})…")

            if parser == "html_js":
                if not cfg.get("url"):
                    print(f"  [{source_key}] url not set — skipping.")
                    continue
                rows = _scrape_html_source(source_key, cfg, timestamp)

            elif parser == "powerbi":
                if not cfg.get("endpoint"):
                    print(f"  [{source_key}] Power BI endpoint not configured — skipping.")
                    continue
                rows = _scrape_powerbi_source(source_key, cfg, timestamp)

            else:
                print(f"  [{source_key}] Unknown parser '{parser}' — skipping.")
                continue

            all_rows.extend(rows)

        if not all_rows:
            print("No data rows collected.")
            update_status("hospital_monitor", "FAIL")
            return

        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
        file_exists = os.path.isfile(CSV_PATH)
        with open(CSV_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(CSV_HEADER)
            writer.writerows(all_rows)

        print(f"[{timestamp}] Success! {len(all_rows)} rows written.")
        for row in all_rows:
            print(f" -> {row[1]}: {row[2]} waiting, {row[3]} treating. Est wait: {row[4]}")
        update_status("hospital_monitor", "PASS")

    except Exception as e:
        print(f"Extraction failed: {e}")
        update_status("hospital_monitor", "FAIL")


if __name__ == "__main__":
    scrape_hospital()
