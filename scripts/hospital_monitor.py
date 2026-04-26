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


def format_time(minutes):
    """Convert raw minutes to 'X hr Y min' string."""
    try:
        mins = int(minutes)
        if mins < 60:
            return f"{mins} min"
        hrs, rem = divmod(mins, 60)
        return f"{hrs} hr" if rem == 0 else f"{hrs} hr {rem} min"
    except (ValueError, TypeError):
        return "N/A"


def _scrape_source(source_key: str, source_cfg: dict, timestamp: str) -> list:
    """Scrape one dashboard source; return rows ready for CSV append."""
    url     = source_cfg["url"]
    mapping = source_cfg["hospitals"]

    response = requests.get(url, impersonate="chrome120", timeout=20)
    if response.status_code != 200:
        print(f"  [{source_key}] Server rejected connection: {response.status_code}")
        return []

    html = response.text
    counts_match = re.search(r'const patientCounts\s*=\s*(\{.*?\});', html)
    waits_match  = re.search(r'const predictedWaitMinutes\s*=\s*(\{.*?\});', html)

    if not counts_match or not waits_match:
        print(f"  [{source_key}] Failed to locate data variables in HTML.")
        return []

    patient_counts  = json.loads(counts_match.group(1))
    predicted_waits = json.loads(waits_match.group(1))

    rows = []
    for key, formal_name in mapping.items():
        c_data   = patient_counts.get(key, {})
        waiting  = c_data.get("waiting", 0)
        treating = c_data.get("beingTreated", 0)

        w_data  = predicted_waits.get(key, {})
        min_raw = int(w_data.get("min", 0))
        max_raw = int(w_data.get("max", 0))
        min_fmt = format_time(min_raw)
        max_fmt = format_time(max_raw)
        wait_time_str = f"{min_fmt} - {max_fmt}" if min_fmt != "N/A" else "N/A"

        rows.append([timestamp, formal_name, waiting, treating,
                     wait_time_str, min_raw, max_raw])
    return rows


def scrape_hospital():
    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        all_rows  = []

        for source_key, source_cfg in SOURCES.items():
            if source_cfg["url"] is None:
                print(f"  [{source_key}] URL not configured — skipping.")
                continue
            print(f"  Scraping {source_key}…")
            rows = _scrape_source(source_key, source_cfg, timestamp)
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
                writer.writerow(["timestamp", "hospital", "waiting", "treating",
                                 "wait_time", "min_wait_mins", "max_wait_mins"])
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
