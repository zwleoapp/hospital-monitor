# data-class: public-aggregate
"""
get_history.py — Silver CSV → history_timeline.json + forecast_audit.csv

Reads the Silver CSV, extracts the last N hours of observations grouped into
15-minute buckets (one row per hospital per bucket), computes the 60-minute
forecast for each snapshot, and pre-computes forecast accuracy (predicted at T
vs actual at T+60).

history_timeline.json schema:
  {
    "generated_utc":  "...",
    "history_hours":  3,
    "snapshots": [
      {
        "bucket_utc": "2026-05-03T01:00:00Z",
        "sites": [
          {
            "site":                 "Box Hill Hospital",
            "timestamp_utc":        "...",
            "current_wait_min":     45.0,
            "max_wait_min":         90,
            "waiting_count":        20,
            "treating_count":       35,
            "wait_momentum":        5.0,
            "predicted_wait_min":   55.0,
            "confidence":           0.78,
            "confidence_label":     "High",
            "forecast_accuracy":    91.2,
            "actual_60m_wait_min":  48.0
          }, ...
        ]
      }, ...
    ]
  }

── Cache Lag Architecture ───────────────────────────────────────────────────

cache_lag_minutes measures the gap between the portal's claimed last-update
time and the moment our scraper ran. It is ALWAYS non-zero because:

  - Our scraper runs every 15 minutes
  - Hospitals update their data on their own schedule (typically every 5–30 min)
  - Even in the best case, there is irreducible latency between hospital system
    update → portal publish → our scrape

The lag means DIFFERENT things depending on the data source:

  SOURCE TYPE: html_js  (Eastern Health — Box Hill, Angliss, Maroondah)
  ─────────────────────────────────────────────────────────────────────────
  The hospital's own public webpage embeds a native "Last Updated" timestamp
  that reflects when the hospital pushed new patient-count data to their site.

    cache_lag_minutes = scrape_timestamp − hospital_page_last_updated

  This is a DIRECT measure of hospital publishing latency. A lag of 5 min
  means the hospital refreshed 5 min before we scraped. A lag of 45 min
  means the hospital's own system was slow to publish. Relatively trustworthy
  as an indicator of underlying data freshness.

  SOURCE TYPE: powerbi  (Monash Health — Casey, Clayton, Dandenong)
  ─────────────────────────────────────────────────────────────────────────
  Monash Health exposes data through Power BI Embedded. The "Last Updated"
  value (LastUpdatedDisplay) is a timestamp shown inside the Power BI visual,
  which reflects when Power BI REFRESHED ITS OWN DATASET — not when the
  underlying hospital system changed.

    cache_lag_minutes = scrape_timestamp − powerbi_dataset_refresh_time

  This is an INDIRECT measure: PBI caching latency, not hospital data freshness.
  The actual hospital data could be fresher or staler than PBI's display claims.
  When interpreting this column for Monash rows, treat it as "time since Power BI
  last synced" rather than "time since hospital updated."

  ML FORECAST NOTE:
  ─────────────────────────────────────────────────────────────────────────
  The ML forecast (predicted_wait_min) is always trained on the raw API-queried
  waiting time — the actual count returned by the scrape — regardless of what
  the portal claims to have last updated. cache_lag_minutes is a DIAGNOSTIC
  column in forecast_audit.csv to explain error spikes in hindsight; it is
  NOT an input feature to the forecast model.

forecast_audit.csv is written alongside accuracy_postmortem.jsonl and is
NEVER subject to the UI display window filter. It is a full historical record
for ML backtesting and model evolution.
"""

import csv
import sys
import json
import pathlib
import argparse
from datetime import datetime, timezone, timedelta

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from predict_next import project_wait, confidence_score, time_band, day_type  # noqa: E402
from config.hospitals import ALL_HOSPITALS, SOURCES       # noqa: E402
from config.paths import (                                # noqa: E402
    SILVER_CSV          as DEFAULT_SILVER,
    HISTORY_JSON_TMP    as DEFAULT_JSON_OUT,
    ACCURACY_LOG        as ACCURACY_LOG_PATH,
    ANOMALY_LOG         as ANOMALY_LOG_PATH,
    FORECAST_AUDIT_CSV  as FORECAST_AUDIT_PATH,
    BRONZE_RAW_CSV      as BRONZE_RAW_PATH,
)
ANOMALY_ERROR_PCT = 200.0

# Read UI_DISPLAY_WINDOW_MINS from ui_config.json so that running get_history.py
# directly from the CLI produces the same window as publish_latest.py.
# Falls back to 3 h (180 min) if the config file is unreadable.
_UI_CFG_PATH = pathlib.Path(__file__).resolve().parent.parent / "config" / "ui_config.json"
try:
    _ui_window_mins = int(json.loads(_UI_CFG_PATH.read_text()).get("UI_DISPLAY_WINDOW_MINS", 180))
except Exception:
    _ui_window_mins = 180
HISTORY_HOURS = _ui_window_mins / 60  # e.g. 180 min → 3.0 h

# Map each hospital formal name → scraper source_type ("html_js" or "powerbi").
# Used in forecast_audit.csv so cache_lag_minutes can be interpreted correctly:
#   html_js  → lag measures hospital publishing latency (direct)
#   powerbi  → lag measures Power BI dataset refresh latency (indirect/cached)
_SOURCE_TYPE_MAP: dict[str, str] = {
    formal_name: cfg.get("parser", "html_js")
    for cfg in SOURCES.values()
    for formal_name in cfg.get("hospitals", {}).values()
}

FORECAST_AUDIT_HEADER = [
    "bucket_utc", "hospital", "cohort", "source_type",
    "day_type", "time_band",
    "current_wait_min", "wait_momentum", "treating_count",
    "actual_wait_min", "predicted_wait_min", "error_pct", "forecast_accuracy",
    "cache_lag_minutes", "fidelity_status",
]


def _log_accuracy_postmortem(df: "pd.DataFrame") -> None:
    """
    Append completed accuracy records to accuracy_postmortem.jsonl.

    Only writes rows where actual_60m_wait_min is known (T+60 already observed).
    Rows with absolute error > ANOMALY_ERROR_PCT are also written to anomaly log
    for human review and are NOT used by evolve_damping_factors().
    """
    import json as _json
    completed = df[df["actual_60m_wait_min"].notna() & df["forecast_accuracy"].notna()].copy()
    if completed.empty:
        return

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    normal_lines = []
    anomaly_lines = []

    for _, row in completed.iterrows():
        predicted = float(row["predicted_wait_min"])
        actual    = float(row["actual_60m_wait_min"])
        error_pct = abs(predicted - actual) / max(actual, 1) * 100
        record = {
            "logged_utc":       now_str,
            "bucket_utc":       row["bucket"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "hospital":         row["hospital"],
            "predicted_wait":   round(predicted, 1),
            "actual_wait":      round(actual, 1),
            "error_pct":        round(error_pct, 1),
            "forecast_accuracy": float(row["forecast_accuracy"]),
            "momentum":         float(row.get("wait_momentum") or 0),
        }
        if error_pct > ANOMALY_ERROR_PCT:
            record["anomaly"] = True
            anomaly_lines.append(_json.dumps(record))
        else:
            normal_lines.append(_json.dumps(record))

    for path, lines in [(ACCURACY_LOG_PATH, normal_lines), (ANOMALY_LOG_PATH, anomaly_lines)]:
        if lines:
            try:
                with open(path, "a") as fh:
                    fh.write("\n".join(lines) + "\n")
            except OSError:
                pass  # SSD unavailable — non-fatal


def _write_forecast_audit(df: "pd.DataFrame") -> None:
    """
    Append completed forecast rows to forecast_audit.csv.

    Runs after _log_accuracy_postmortem on the same completed-rows subset.
    Only writes rows where actual_60m_wait_min is known (T+60 already observed).

    cache_lag_minutes / fidelity_status are joined from bronze_raw_scrapes.csv
    by matching scrape timestamps to 15-minute buckets. These columns are
    DIAGNOSTIC ONLY — they explain forecast error in hindsight but are not
    ML inputs. Interpretation differs by source_type:

      html_js (Eastern Health):
        cache_lag = time since hospital last published their page data.
        Directly reflects hospital publishing latency.

      powerbi (Monash Health):
        cache_lag = time since Power BI last refreshed its embedded dataset.
        Reflects PBI caching latency, NOT underlying hospital data freshness.
        The lag always exists (scraper runs every 15 min, PBI refreshes on its
        own cadence) — treat it as "PBI sync age" rather than "hospital data age."

    This file is NEVER subject to the UI display window filter.
    """
    completed = df[df["actual_60m_wait_min"].notna() & df["forecast_accuracy"].notna()].copy()
    if completed.empty:
        return

    # Build (hospital, bucket) → (cache_lag_minutes, fidelity_status) lookup
    # by flooring bronze_raw scrape timestamps into 15-minute buckets.
    cache_lag_lookup: dict[tuple, str] = {}
    fidelity_lookup: dict[tuple, str]  = {}
    if BRONZE_RAW_PATH.exists():
        try:
            br = pd.read_csv(BRONZE_RAW_PATH)
            br["scrape_dt"] = pd.to_datetime(
                br["scrape_timestamp_utc"], utc=True, errors="coerce"
            )
            br["bucket"] = br["scrape_dt"].dt.floor("15min")
            br = br.dropna(subset=["scrape_dt", "site"])
            # Keep last row per (site, bucket) — most recent scrape wins
            br = br.sort_values("scrape_dt").groupby(["site", "bucket"]).last().reset_index()
            for _, row in br.iterrows():
                key = (row["site"], row["bucket"])
                cache_lag_lookup[key] = row.get("cache_lag_minutes", "")
                fidelity_lookup[key]  = row.get("fidelity_status", "")
        except Exception:
            pass

    audit_rows = []
    for _, row in completed.iterrows():
        hospital = row["hospital"]
        bucket   = row["bucket"]
        key      = (hospital, bucket)

        predicted  = float(row["predicted_wait_min"])
        actual     = float(row["actual_60m_wait_min"])
        current    = float(row.get("min_wait_mins") or 0)
        momentum   = float(row.get("wait_momentum")  or 0)
        treating   = int(row.get("treating") or 0)
        error_pct  = abs(predicted - actual) / max(actual, 1) * 100

        # Temporal classification for the evolve_model.py demand segmentation
        from zoneinfo import ZoneInfo as _ZI
        bucket_melb = bucket.astimezone(_ZI("Australia/Melbourne"))
        d_type = day_type(bucket_melb)
        t_band = time_band(bucket_melb.hour)

        audit_rows.append([
            bucket.strftime("%Y-%m-%dT%H:%M:%SZ"),
            hospital,
            "Adult",                                     # cohort — Paed forecasts not yet built
            _SOURCE_TYPE_MAP.get(hospital, "unknown"),   # html_js or powerbi
            d_type,                                      # weekday | weekend | public_holiday
            t_band,                                      # overnight | morning | afternoon | evening
            round(current, 1),
            round(momentum, 1),
            treating,                                    # treating_count — capacity signal for regression
            round(actual, 1),
            round(predicted, 1),
            round(error_pct, 1),
            float(row["forecast_accuracy"]),
            cache_lag_lookup.get(key, ""),
            fidelity_lookup.get(key, ""),
        ])

    if not audit_rows:
        return

    file_exists = FORECAST_AUDIT_PATH.exists()
    try:
        with open(FORECAST_AUDIT_PATH, "a", newline="") as fh:
            writer = csv.writer(fh)
            if not file_exists:
                writer.writerow(FORECAST_AUDIT_HEADER)
            writer.writerows(audit_rows)
    except OSError:
        pass  # SSD unavailable — non-fatal


def build_timeline(silver_path: pathlib.Path, history_hours: int = HISTORY_HOURS) -> dict:
    df = pd.read_csv(silver_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=history_hours)
    df = df[df["timestamp"] >= cutoff].copy()
    df = df[df["hospital"].isin(ALL_HOSPITALS)].copy()

    if df.empty:
        return {
            "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "history_hours": history_hours,
            "snapshots": [],
        }

    # Round to 15-min buckets; within each bucket keep the latest row per hospital
    df["bucket"] = df["timestamp"].dt.floor("15min")
    df = (
        df.sort_values("timestamp")
          .groupby(["bucket", "hospital"], sort=False)
          .last()
          .reset_index()
    )

    # Compute 60-min projection for every row
    def _project(row):
        return project_wait(float(row["min_wait_mins"] or 0),
                            float(row.get("wait_momentum") or 0))

    df["predicted_wait_min"] = df.apply(_project, axis=1)

    # Build (bucket, hospital) → current_wait_min lookup for accuracy computation
    wait_at: dict = df.set_index(["bucket", "hospital"])["min_wait_mins"].to_dict()

    def _accuracy(row):
        target = row["bucket"] + pd.Timedelta(minutes=60)
        for delta in [0, -5, 5, -10, 10, -15, 15]:
            key = (target + pd.Timedelta(minutes=delta), row["hospital"])
            if key in wait_at:
                actual = float(wait_at[key] or 0)
                predicted = row["predicted_wait_min"]
                acc = (round(100 - min(abs(predicted - actual) / max(actual, 1) * 100, 100), 1)
                       if actual > 0 else None)
                return acc, round(actual, 1)
        return None, None

    accs     = df.apply(lambda r: _accuracy(r)[0], axis=1)
    actuals  = df.apply(lambda r: _accuracy(r)[1], axis=1)
    df["forecast_accuracy"]   = accs
    df["actual_60m_wait_min"] = actuals

    _log_accuracy_postmortem(df)
    _write_forecast_audit(df)

    snapshots = []
    for bucket, grp in df.groupby("bucket"):
        sites = []
        for _, row in grp.iterrows():
            current = float(row["min_wait_mins"] or 0)
            momentum = float(row.get("wait_momentum") or 0)
            try:
                conf, label = confidence_score(
                    current, momentum,
                    float(row.get("ctx_los_pct_under_4hr") or 50),
                    float(row.get("ctx_wait_p90_mins") or 60),
                )
            except Exception:
                conf, label = None, "—"

            raw_max  = row.get("max_wait_mins")
            raw_pred = row["predicted_wait_min"]
            raw_acc  = row["forecast_accuracy"]
            raw_act  = row["actual_60m_wait_min"]
            sites.append({
                "site":                 row["hospital"],
                "timestamp_utc":        row["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "current_wait_min":     round(current, 1),
                "max_wait_min":         None if pd.isna(raw_max)  else int(raw_max),
                "waiting_count":        int(row.get("waiting") or 0),
                "treating_count":       int(row.get("treating") or 0),
                "wait_momentum":        round(momentum, 1),
                "predicted_wait_min":   None if pd.isna(raw_pred) else round(float(raw_pred), 1),
                "confidence":           conf,
                "confidence_label":     label,
                "forecast_accuracy":    None if pd.isna(raw_acc)  else round(float(raw_acc),  1),
                "actual_60m_wait_min":  None if pd.isna(raw_act)  else round(float(raw_act),  1),
            })

        snapshots.append({
            "bucket_utc": bucket.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sites": sorted(sites, key=lambda s: s["site"]),
        })

    snapshots.sort(key=lambda s: s["bucket_utc"])

    return {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "history_hours": history_hours,
        "snapshots":     snapshots,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build 24h history timeline JSON from Silver CSV."
    )
    parser.add_argument("--silver", type=pathlib.Path, default=DEFAULT_SILVER)
    parser.add_argument("--out",    type=pathlib.Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--hours",  type=int,          default=HISTORY_HOURS)
    args = parser.parse_args()

    timeline = build_timeline(args.silver, args.hours)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(timeline, indent=2, allow_nan=False))
    n = len(timeline["snapshots"])
    print(f"  History timeline: {n} snapshots ({args.hours}h) → {args.out}")


if __name__ == "__main__":
    main()
