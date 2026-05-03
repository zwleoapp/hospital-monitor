import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent))

import csv
import os
import re
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime
from config.paths import (
    BRONZE_CSV as CSV_PATH,
    BRONZE_RAW_CSV as BRONZE_RAW_PATH,
    INGEST_ALERTS_CSV,
    LAST_UPDATED_SIDECAR,
)

# ── Operational config (ui_config.json) ───────────────────────────────────────
_UI_CFG_PATH = pathlib.Path(__file__).resolve().parent.parent.parent / "config" / "ui_config.json"
try:
    _ui_cfg = json.loads(_UI_CFG_PATH.read_text())
except Exception:
    _ui_cfg = {}

FIDELITY_SYNCED_THRESHOLD_MINS = int(_ui_cfg.get("FIDELITY_SYNCED_THRESHOLD_MINS", 15))
FIDELITY_STALE_THRESHOLD_MINS  = int(_ui_cfg.get("FIDELITY_STALE_THRESHOLD_MINS",  60))

_MELB = ZoneInfo("Australia/Melbourne")

INGEST_ALERT_HEADER = [
    "alert_timestamp_utc", "location_timestamp", "hospital", "cohort",
    "source_type", "issue_type", "detail", "scrape_timestamp_utc",
]


def _write_ingest_alert(hospital: str, cohort: str, source_type: str,
                        issue_type: str, detail: str,
                        scrape_timestamp_utc: str, location_timestamp: str) -> None:
    """
    Append one row to ingest_alerts.csv whenever a data-quality issue is detected.

    issue_type values:
      NULL_ALL_MEASURES  — API returned null for waiting AND treating (not a valid ED state)
      HTTP_ERROR         — non-200 response from the source
      PARSE_ERROR        — could not extract data variables from HTML / DSR
      DSR_NO_GROUP       — Power BI DSR had no matching group row (e.g. Adult row missing)

    This file is the foundation for a future status page: green = no alerts in last
    60 min; amber = alert in last 60 min; red = alert in last 15 min.
    """
    now_utc  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    now_melb = datetime.now(_MELB).strftime("%Y-%m-%d %H:%M %Z")
    row = [now_utc, now_melb, hospital, cohort, source_type,
           issue_type, detail, scrape_timestamp_utc]
    file_exists = INGEST_ALERTS_CSV.exists()
    try:
        with open(INGEST_ALERTS_CSV, "a", newline="") as fh:
            writer = csv.writer(fh)
            if not file_exists:
                writer.writerow(INGEST_ALERT_HEADER)
            writer.writerow(row)
    except OSError:
        pass  # SSD unavailable — non-fatal


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


def _calculate_cache_lag(scrape_timestamp_utc: str, reported_timestamp_str: str) -> tuple[int, str]:
    """
    Calculate cache lag (Truth Gap) between scrape time and reported portal update.

    Args:
        scrape_timestamp_utc: ISO format UTC timestamp (e.g., "2026-04-30T14:51:02Z")
        reported_timestamp_str: Portal timestamp string (e.g., "Last Updated: 01 May 26 00:46" or "~00:32")

    Returns:
        (cache_lag_minutes, fidelity_status)

    Fidelity Status Logic:
        < 15 mins: "SYNCED"
        15-60 mins: "API_LEAD_ACTIVE"
        > 60 mins: "PORTAL_STALE_WARNING"
    """
    try:
        # Parse scrape timestamp (UTC) and convert to AEST
        scrape_utc = datetime.fromisoformat(scrape_timestamp_utc.replace('Z', '+00:00'))
        scrape_aest = scrape_utc.astimezone(_MELB)

        # Parse reported timestamp (AEST)
        if reported_timestamp_str.startswith("~"):
            # Format: "~00:32" (HTTP header fallback) - assume today
            time_str = reported_timestamp_str.lstrip("~")
            reported_aest = datetime.strptime(
                f"{scrape_aest.strftime('%Y-%m-%d')} {time_str}",
                "%Y-%m-%d %H:%M"
            ).replace(tzinfo=_MELB)
        elif "Last Updated:" in reported_timestamp_str:
            # Format: "Last Updated: 01 May 26 00:46"
            match = re.search(r'(\d+)\s+(\w+)\s+(\d+)\s+(\d+):(\d+)', reported_timestamp_str)
            if not match:
                return 0, "PARSE_ERROR"

            day, month_str, year_short, hour, minute = match.groups()

            # Map month abbreviations
            month_map = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            month = month_map.get(month_str.lower(), 1)
            year = 2000 + int(year_short)

            reported_aest = datetime(
                year=year, month=month, day=int(day),
                hour=int(hour), minute=int(minute),
                tzinfo=_MELB
            )
        else:
            # Unknown format
            return 0, "UNKNOWN_FORMAT"

        # Calculate lag in minutes
        lag_seconds = (scrape_aest - reported_aest).total_seconds()
        lag_minutes = int(lag_seconds / 60)

        # Determine fidelity status (thresholds from ui_config.json)
        if lag_minutes < 0:
            fidelity_status = "CLOCK_SKEW"
        elif lag_minutes < FIDELITY_SYNCED_THRESHOLD_MINS:
            fidelity_status = "SYNCED"
        elif lag_minutes < FIDELITY_STALE_THRESHOLD_MINS:
            fidelity_status = "API_LEAD_ACTIVE"
        else:
            fidelity_status = "PORTAL_STALE_WARNING"

        return lag_minutes, fidelity_status

    except Exception as e:
        # Fallback on parse error
        return 0, f"ERROR:{str(e)[:20]}"
