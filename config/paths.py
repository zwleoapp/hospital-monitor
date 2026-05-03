# data-class: public-aggregate
"""
config/paths.py — Single source of truth for all file paths.

All scripts import from here. Moving the SSD mount point or renaming
a file is a one-line change in this file only.
"""

import pathlib

# ── Storage root ──────────────────────────────────────────────────────────────
SSD  = pathlib.Path("/mnt/router_ssd/Data_Hub/Waiting_Live_time")
REPO = pathlib.Path(__file__).resolve().parent.parent

# ── Bronze (raw scrape output — append-only, never filtered) ─────────────────
BRONZE_CSV       = SSD / "melbourne_southeast.csv"        # Adult main (UI-facing bronze)
BRONZE_RAW_CSV   = SSD / "bronze_raw_scrapes.csv"         # All cohorts + clinical metadata

# ── Silver (enriched, full-rebuild each cycle) ────────────────────────────────
SILVER_CSV       = SSD / "melbourne_southeast_silver.csv"

# ── ML / audit (never subject to UI display window) ──────────────────────────
FORECAST_AUDIT_CSV  = SSD / "forecast_audit.csv"
ACCURACY_LOG        = SSD / "accuracy_postmortem.jsonl"
ANOMALY_LOG         = SSD / "damping_anomalies.jsonl"
INGEST_ALERTS_CSV   = SSD / "ingest_alerts.csv"           # data-quality alert log

# ── Sidecars ──────────────────────────────────────────────────────────────────
LAST_UPDATED_SIDECAR = SSD / "monash_last_updated.json"   # per-campus portal timestamps

# ── Staging (ephemeral /tmp — never committed) ────────────────────────────────
LATEST_JSON_TMP      = pathlib.Path("/tmp/hospital_monitor_latest.json")
HISTORY_JSON_TMP     = pathlib.Path("/tmp/history_timeline.json")
PUBLISHER_TMPDIR     = pathlib.Path("/tmp/publisher")

# ── Bronze reference data ─────────────────────────────────────────────────────
VAHI_FILE  = REPO / "bronze" / "vahi_history_merged.csv"
AIHW_FILE  = REPO / "bronze" / "eastern_hospital_historical_context.csv"
