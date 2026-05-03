# data-class: public-aggregate
import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import csv
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from status import update_status
from config.hospitals import SOURCES
from config.paths import BRONZE_CSV as CSV_PATH, BRONZE_RAW_CSV as BRONZE_RAW_PATH
from scrapers.eastern    import scrape_html_source
from scrapers.monash     import scrape_powerbi_source
from scrapers.html_regex import scrape_html_regex_source

_MELB = ZoneInfo("Australia/Melbourne")

CSV_HEADER = ["timestamp", "hospital", "waiting", "treating",
              "wait_time", "min_wait_mins", "max_wait_mins", "location_timestamp"]

BRONZE_RAW_HEADER = ["site", "scrape_timestamp_utc", "location_timestamp",
                     "reported_timestamp_str",
                     "reported_waiting", "reported_wait_str",
                     "raw_query_waiting", "raw_query_treating", "raw_query_max_wait",
                     "cohort", "cache_lag_minutes", "fidelity_status"]


def scrape_hospital():
    try:
        now = datetime.now(timezone.utc)
        timestamp          = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        location_timestamp = now.astimezone(_MELB).strftime("%Y-%m-%d %H:%M %Z")

        all_rows     = []
        all_raw_rows = []

        for source_key, cfg in SOURCES.items():
            parser = cfg.get("parser", "html_js")
            print(f"  Scraping {source_key} ({parser})…")

            if parser == "html_js":
                if not cfg.get("url"):
                    print(f"  [{source_key}] url not set — skipping.")
                    continue
                rows, raw_rows = scrape_html_source(source_key, cfg, timestamp, location_timestamp)

            elif parser == "powerbi":
                if not cfg.get("endpoint"):
                    print(f"  [{source_key}] Power BI endpoint not configured — skipping.")
                    continue
                rows, raw_rows = scrape_powerbi_source(source_key, cfg, timestamp, location_timestamp)

            elif parser == "html_regex":
                if not cfg.get("url"):
                    print(f"  [{source_key}] url not set — skipping.")
                    continue
                rows, raw_rows = scrape_html_regex_source(source_key, cfg, timestamp, location_timestamp)

            else:
                print(f"  [{source_key}] Unknown parser '{parser}' — skipping.")
                continue

            all_rows.extend(rows)
            all_raw_rows.extend(raw_rows)

        if not all_rows:
            print("No data rows collected.")
            update_status("hospital_monitor", "FAIL")
            return

        # Write to Reported Truth CSV (Bronze, Adult, UI-facing)
        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
        file_exists = os.path.isfile(CSV_PATH)
        with open(CSV_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(CSV_HEADER)
            writer.writerows(all_rows)

        # Write to Clinical Raw CSV (all cohorts + metadata, for ML)
        os.makedirs(os.path.dirname(BRONZE_RAW_PATH), exist_ok=True)
        raw_exists = os.path.isfile(BRONZE_RAW_PATH)
        with open(BRONZE_RAW_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if not raw_exists:
                writer.writerow(BRONZE_RAW_HEADER)
            writer.writerows(all_raw_rows)

        print(f"[{timestamp}] Success! {len(all_rows)} rows written to Bronze.")
        print(f"[{timestamp}] {len(all_raw_rows)} rows written to Bronze Raw.")
        for row in all_rows:
            print(f" -> {row[1]}: {row[2]} waiting, {row[3]} treating. Est wait: {row[4]}")
        update_status("hospital_monitor", "PASS")

    except Exception as e:
        print(f"Extraction failed: {e}")
        update_status("hospital_monitor", "FAIL")


if __name__ == "__main__":
    scrape_hospital()
