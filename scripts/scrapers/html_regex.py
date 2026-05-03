from .base import (_MELB, _parse_wait_str, _calculate_cache_lag,
                   _merge_last_updated_sidecar, _write_ingest_alert)

from curl_cffi import requests
import re


def scrape_html_regex_source(source_key: str, cfg: dict, timestamp: str,
                              location_timestamp: str) -> tuple[list, list]:
    """
    Fetch a plain HTML page and extract ED data via configured regex_patterns.

    Bronze CSV output (full Silver pipeline) is written ONLY when a "wait_time"
    pattern is configured and successfully matched — meaning the hospital publishes
    a real wait time range.  Hospitals with only a "busy_index" pattern (e.g. RCH)
    produce an empty rows list and write to Bronze Raw only.

    regex_patterns keys (all optional, but at least one required):
      wait_time         — "00 hr 52 min - 01 hr 54 min" range string
      patients_waiting  — integer waiting count
      patients_treating — integer treating count
      updated_time      — portal "Last Updated" text (any format)
      busy_index        — floating-point busyness score (RCH only)

    cache_lag / fidelity_status:
      A time in HH:MM format is extracted from updated_time (if present) and
      treated as an approximate portal timestamp (~HH:MM) for lag calculation.
      This is the same "indirect" interpretation as Monash Health Power BI —
      the scraper clock vs portal display, not hospital system freshness.
    """
    url      = cfg["url"]
    patterns = cfg.get("regex_patterns", {})
    hospitals = cfg["hospitals"]

    try:
        resp = requests.get(url, impersonate="chrome120", timeout=20)
    except Exception as exc:
        for formal_name in hospitals.values():
            _write_ingest_alert(formal_name, "All", "html_regex",
                                "HTTP_ERROR", str(exc)[:80], timestamp, location_timestamp)
        print(f"  [{source_key}] Request failed: {exc}")
        return [], []

    if resp.status_code != 200:
        for formal_name in hospitals.values():
            _write_ingest_alert(formal_name, "All", "html_regex",
                                "HTTP_ERROR", f"HTTP {resp.status_code}", timestamp, location_timestamp)
        print(f"  [{source_key}] HTTP {resp.status_code}")
        return [], []

    html = resp.text

    # Apply all configured patterns once against the page HTML
    extracted: dict[str, str | None] = {}
    for field, pattern in patterns.items():
        m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        extracted[field] = m.group(1).strip() if m else None
        status = extracted[field] if extracted[field] is not None else "NOT FOUND"
        print(f"  [{source_key}] {field}: {status}")

    rows: list = []
    raw_rows: list = []

    for js_key, formal_name in hospitals.items():
        wait_time_raw = extracted.get("wait_time")        # e.g. "00 hr 52 min - 01 hr 54 min"
        waiting_raw   = extracted.get("patients_waiting")
        treating_raw  = extracted.get("patients_treating")
        updated_raw   = extracted.get("updated_time", "") or ""
        busy_idx      = extracted.get("busy_index")

        # Safe int conversion for counts
        def _to_int(s):
            try:
                return int(s) if s and str(s).strip().isdigit() else None
            except (TypeError, ValueError):
                return None

        waiting  = _to_int(waiting_raw)
        treating = _to_int(treating_raw)

        # Parse wait range → (min_wait_mins, max_wait_mins, display string)
        min_wait, max_wait, wait_str = 0, 0, ""
        if wait_time_raw:
            parts    = re.split(r"\s*-\s*", wait_time_raw, maxsplit=1)
            min_wait = _parse_wait_str(parts[0])
            max_wait = _parse_wait_str(parts[1]) if len(parts) > 1 else min_wait
            wait_str = wait_time_raw
        elif busy_idx:
            wait_str = f"Busy: {busy_idx}"

        # Normalize portal timestamp to 24h "~HH:MM" so:
        #   (a) _calculate_cache_lag uses the existing ~HH:MM branch
        #   (b) the UI's parseHospDataTime shows the correct hour (e.g. 17:45 not 5:45)
        # html_regex pages may use 12h am/pm format ("5:45pm") unlike Eastern Health
        # which always returns 24h from its HTTP Date header or native page clock.
        _ts_normalized = ""
        hm = re.search(r'(\d{1,2}):(\d{2})\s*([ap]m)?', updated_raw or '', re.IGNORECASE)
        if hm:
            h, m, ampm = int(hm.group(1)), int(hm.group(2)), (hm.group(3) or '').lower()
            if ampm == 'pm' and h != 12:
                h += 12
            elif ampm == 'am' and h == 12:
                h = 0
            _ts_normalized = f"~{h:02d}:{m:02d}"

        cache_lag, fidelity_status = _calculate_cache_lag(
            timestamp, _ts_normalized or updated_raw or ""
        )

        # Write to sidecar so publish_latest.py populates last_updated_display in Gold.
        # This keeps html_regex consistent with html_js (eastern.py) and powerbi.
        if _ts_normalized:
            _merge_last_updated_sidecar({formal_name: _ts_normalized})

        # ── Bronze CSV row (full Silver pipeline) ──────────────────────────────
        # Only written when a real wait time was extracted.
        if wait_time_raw:
            rows.append([
                timestamp, formal_name,
                waiting or 0, treating or 0,
                wait_str, min_wait, max_wait,
                location_timestamp,
            ])
        elif not busy_idx:
            _write_ingest_alert(formal_name, "All", "html_regex",
                                "PARSE_ERROR",
                                "Neither wait_time nor busy_index extracted from HTML",
                                timestamp, location_timestamp)

        # ── Bronze Raw row (always — captures busy_index and portal metadata) ──
        raw_rows.append([
            formal_name,
            timestamp,           # scrape_timestamp_utc  (Scrape Truth)
            location_timestamp,  # location_timestamp     (Melbourne local)
            updated_raw,         # reported_timestamp_str (Portal Truth)
            waiting,             # reported_waiting
            wait_str,            # reported_wait_str (wait range or "Busy: X.X")
            waiting,             # raw_query_waiting
            treating,            # raw_query_treating
            max_wait if wait_time_raw else None,  # raw_query_max_wait
            "All",               # cohort
            cache_lag,           # cache_lag_minutes
            fidelity_status,     # fidelity_status
        ])

    return rows, raw_rows
