from .base import (_MELB, format_time, _parse_wait_str, _calculate_cache_lag,
                   _merge_last_updated_sidecar, _write_ingest_alert)

from curl_cffi import requests
import re
import json
from email.utils import parsedate_to_datetime


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


def scrape_html_source(source_key: str, cfg: dict, timestamp: str, location_timestamp: str) -> tuple[list, list]:
    """
    GET the dashboard page; parse JS-embedded patientCounts + predictedWaitMinutes.
    Returns (bronze_rows, raw_scrape_rows) for dual persistence.
    """
    resp = requests.get(cfg["url"], impersonate="chrome120", timeout=20)
    if resp.status_code != 200:
        print(f"  [{source_key}] HTTP {resp.status_code}")
        _write_ingest_alert("Eastern Health", "All", "html_js",
                            "HTTP_ERROR", f"HTTP {resp.status_code} from {cfg['url']}",
                            timestamp, location_timestamp)
        return [], []

    html = resp.text

    # JS variable names and field mappings come from hospitals.json so they can be
    # updated without touching scraper code if Eastern Health renames them.
    js_vars = cfg.get("js_data_vars", {"counts": "patientCounts", "waits": "predictedWaitMinutes"})
    fm      = cfg.get("js_field_map",  {"waiting": "waiting", "treating": "beingTreated",
                                         "min_wait": "min", "max_wait": "max"})

    counts_var = js_vars.get("counts", "patientCounts")
    waits_var  = js_vars.get("waits",  "predictedWaitMinutes")

    counts_m = re.search(rf'const {re.escape(counts_var)}\s*=\s*(\{{.*?\}});', html, re.DOTALL)
    waits_m  = re.search(rf'const {re.escape(waits_var)}\s*=\s*(\{{.*?\}});',  html, re.DOTALL)
    if not counts_m or not waits_m:
        print(f"  [{source_key}] Data variables not found in HTML "
              f"(looked for '{counts_var}', '{waits_var}').")
        _write_ingest_alert("Eastern Health", "All", "html_js",
                            "PARSE_ERROR",
                            f"{counts_var} or {waits_var} not found in page HTML",
                            timestamp, location_timestamp)
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
        waiting  = c.get(fm["waiting"],  0)
        treating = c.get(fm["treating"], 0)
        min_raw  = int(w.get(fm["min_wait"], 0))
        max_raw  = int(w.get(fm["max_wait"], 0))
        min_fmt  = format_time(min_raw)
        max_fmt  = format_time(max_raw)
        wait_str = f"{min_fmt} - {max_fmt}" if min_fmt != "N/A" else "N/A"
        rows.append([timestamp, formal_name, waiting, treating,
                     wait_str, min_raw, max_raw, location_timestamp])
        # Calculate cache lag and fidelity status
        cache_lag, fidelity_status = _calculate_cache_lag(timestamp, page_ts or "")

        raw_rows.append([
            formal_name,
            timestamp,          # scrape_timestamp_utc  (Scrape Truth)
            location_timestamp, # location_timestamp     (Melbourne local)
            page_ts or "",      # reported_timestamp_str (Portal Truth)
            waiting,            # reported_waiting
            wait_str,           # reported_wait_str
            waiting,            # raw_query_waiting
            treating,           # raw_query_treating
            max_raw,            # raw_query_max_wait
            "All",              # cohort (Eastern Health doesn't split Adult/Paeds)
            cache_lag,          # cache_lag_minutes
            fidelity_status,    # fidelity_status
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
