# data-class: public-aggregate
"""
get_history.py — Silver CSV → history_timeline.json (last 24 h)

Reads the Silver CSV, extracts the last 24 hours of observations grouped
into 15-minute buckets (one row per hospital per bucket), computes the
60-minute forecast for each snapshot, and pre-computes forecast accuracy
(predicted at T vs actual at T+60) so the UI can show a training comparison.

Output schema:
  {
    "generated_utc":  "...",
    "history_hours":  24,
    "snapshots": [
      {
        "bucket_utc": "2026-04-28T01:00:00Z",
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
            "forecast_accuracy":    91.2,   // % — null if T+60 not yet recorded
            "actual_60m_wait_min":  48.0    // null if T+60 not yet recorded
          },
          ...
        ]
      },
      ...
    ]
  }
"""

import sys
import json
import pathlib
import argparse
from datetime import datetime, timezone, timedelta

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from predict_next import project_wait, confidence_score  # noqa: E402
from config.hospitals import ALL_HOSPITALS               # noqa: E402

_SSD              = pathlib.Path("/mnt/router_ssd/Data_Hub/Waiting_Live_time")
DEFAULT_SILVER    = _SSD / "eastern_hospital_silver.csv"
DEFAULT_JSON_OUT  = pathlib.Path("/tmp/history_timeline.json")
ACCURACY_LOG_PATH = _SSD / "accuracy_postmortem.jsonl"
ANOMALY_LOG_PATH  = _SSD / "damping_anomalies.jsonl"
ANOMALY_ERROR_PCT = 200.0   # entries exceeding this are routed to anomaly log
HISTORY_HOURS     = 24


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

            raw_max = row.get("max_wait_mins")
            sites.append({
                "site":                 row["hospital"],
                "timestamp_utc":        row["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "current_wait_min":     round(current, 1),
                "max_wait_min":         None if pd.isna(raw_max) else int(raw_max),
                "waiting_count":        int(row.get("waiting") or 0),
                "treating_count":       int(row.get("treating") or 0),
                "wait_momentum":        round(momentum, 1),
                "predicted_wait_min":   row["predicted_wait_min"],
                "confidence":           conf,
                "confidence_label":     label,
                "forecast_accuracy":    row["forecast_accuracy"],
                "actual_60m_wait_min":  row["actual_60m_wait_min"],
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
    args.out.write_text(json.dumps(timeline, indent=2))
    n = len(timeline["snapshots"])
    print(f"  History timeline: {n} snapshots ({args.hours}h) → {args.out}")


if __name__ == "__main__":
    main()
