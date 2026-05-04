# data-class: public-aggregate
"""
publish_latest.py — Silver → JSON → Vercel (direct API) / GitHub data branch / dual

Pipeline (run after transform_silver.py):
  1. Load the most-recent Silver CSV row per hospital
  2. Compute outlook via predict_next logic (wait + momentum + VAHI baseline)
  3. Write latest.json to a staging path (/tmp/publisher/)
  4. --push: publish via method set in config/ui_config.json → publish_method
             'vercel_api'      — direct Vercel API deploy (no git)
             'git_data_branch' — push to GitHub data branch via SSH deploy key
             'dual'            — git every cycle + Vercel every vercel_deploy_interval_mins

Credentials for Vercel in .env (repo root, never committed):
  VERCEL_API_TOKEN, VERCEL_PROJECT_ID

Override method at runtime: PUBLISH_METHOD=git_data_branch python3 scripts/publish_latest.py --push

Usage:
  python3 scripts/publish_latest.py
  python3 scripts/publish_latest.py --silver /path/to/silver.csv
  python3 scripts/publish_latest.py --push
"""

import os
import sys
import json
import math
import shlex
import hashlib
import argparse
import pathlib
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))  # repo root for config
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from predict_next import load_latest_silver, build_outlook   # noqa: E402
from get_history import build_timeline                        # noqa: E402
from config.hospitals import VAHI_BENCHMARKS, RAW_ONLY_HOSPITALS  # noqa: E402
from config.ontology  import write_schema, SCHEMA_VERSION         # noqa: E402
from config.paths import (                                    # noqa: E402
    SILVER_CSV            as DEFAULT_SILVER,
    LATEST_JSON_TMP       as DEFAULT_JSON_OUT,
    HISTORY_JSON_TMP      as DEFAULT_HISTORY_OUT,
    PUBLISHER_TMPDIR,
    LAST_UPDATED_SIDECAR,
    BRONZE_RAW_CSV        as BRONZE_RAW_PATH,
    FORECAST_AUDIT_CSV,
    VERCEL_LAST_DEPLOY,
    GITHUB_LAST_PUSH,
)

# ── UI display window ─────────────────────────────────────────────────────────
# Loaded from config/ui_config.json so the threshold is adjustable without
# touching Python. Applied upstream before writing latest.json and
# history_timeline.json — the frontend renders whatever is in those files.
_UI_CONFIG_PATH = pathlib.Path(__file__).resolve().parent.parent / "config" / "ui_config.json"
try:
    _ui_cfg = json.loads(_UI_CONFIG_PATH.read_text())
    UI_DISPLAY_WINDOW_MINS: int = int(_ui_cfg.get("UI_DISPLAY_WINDOW_MINS", 180))
except Exception:
    UI_DISPLAY_WINDOW_MINS = 180  # safe fallback

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = pathlib.Path(__file__).resolve().parent.parent

_MELB               = ZoneInfo("Australia/Melbourne")
OPERATIONAL_START_H = int(_ui_cfg.get("OPERATIONAL_START_H", 6))
OPERATIONAL_END_H   = int(_ui_cfg.get("OPERATIONAL_END_H",   23))

# ── Traffic-light helper ──────────────────────────────────────────────────────

def traffic_light(predicted_wait: float, momentum: float) -> str:
    green_max  = float(_ui_cfg.get("TRAFFIC_LIGHT_GREEN_MAX_MINS", 30))
    mom_max    = float(_ui_cfg.get("TRAFFIC_LIGHT_MOMENTUM_MAX",   2))
    amber_max  = float(_ui_cfg.get("TRAFFIC_LIGHT_AMBER_MAX_MINS", 60))
    if predicted_wait <= green_max and momentum <= mom_max:
        return "green"
    if predicted_wait <= amber_max:
        return "amber"
    return "red"

# ── Vercel deploy interval check ─────────────────────────────────────────────

def _load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    env_file = _BASE / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def _due(sidecar: pathlib.Path, interval_mins: int, label: str) -> bool:
    try:
        last = float(sidecar.read_text().strip())
        elapsed = (datetime.now(timezone.utc).timestamp() - last) / 60
        if elapsed < interval_mins:
            print(f"  {label} skipped — {elapsed:.0f}/{interval_mins} min elapsed")
            return False
    except (FileNotFoundError, ValueError):
        pass
    return True


def _vercel_due(interval_mins: int) -> bool:
    return _due(VERCEL_LAST_DEPLOY, interval_mins, "Vercel deploy")


def _vercel_stamp() -> None:
    VERCEL_LAST_DEPLOY.write_text(str(datetime.now(timezone.utc).timestamp()))


def _git_due(interval_mins: int) -> bool:
    return _due(GITHUB_LAST_PUSH, interval_mins, "Git push")


def _git_stamp() -> None:
    GITHUB_LAST_PUSH.write_text(str(datetime.now(timezone.utc).timestamp()))


def deploy_to_vercel(json_path: pathlib.Path,
                     history_path: pathlib.Path | None = None) -> None:
    """Upload files directly to Vercel production via deployment API (no git)."""
    import shutil
    env        = _load_env()
    token      = os.environ.get("VERCEL_API_TOKEN")  or env.get("VERCEL_API_TOKEN", "")
    project_id = os.environ.get("VERCEL_PROJECT_ID") or env.get("VERCEL_PROJECT_ID", "")
    if not token or not project_id:
        raise RuntimeError("VERCEL_API_TOKEN and VERCEL_PROJECT_ID must be set in .env")

    PUBLISHER_TMPDIR.mkdir(parents=True, exist_ok=True)
    shutil.copy(json_path, PUBLISHER_TMPDIR / "latest.json")
    shutil.copy(_BASE / "docs" / "index.html", PUBLISHER_TMPDIR / "index.html")
    write_schema(PUBLISHER_TMPDIR / "schema.json")
    vercel_config = {
        "headers": [
            {"source": "/latest.json",          "headers": [{"key": "Cache-Control", "value": _ui_cfg.get("CACHE_LATEST_JSON", "no-cache, no-store, must-revalidate")}]},
            {"source": "/history_timeline.json", "headers": [{"key": "Cache-Control", "value": _ui_cfg.get("CACHE_HISTORY_JSON", "public, max-age=900")}]},
        ]
    }
    (PUBLISHER_TMPDIR / "vercel.json").write_text(json.dumps(vercel_config, indent=2))

    deploy_files = [
        (PUBLISHER_TMPDIR / "latest.json",          "latest.json"),
        (PUBLISHER_TMPDIR / "index.html",           "index.html"),
        (PUBLISHER_TMPDIR / "schema.json",          "schema.json"),
        (PUBLISHER_TMPDIR / "vercel.json",          "vercel.json"),
    ]
    if history_path and history_path.exists():
        shutil.copy(history_path, PUBLISHER_TMPDIR / "history_timeline.json")
        deploy_files.append((PUBLISHER_TMPDIR / "history_timeline.json", "history_timeline.json"))

    def _upload(content: bytes) -> str:
        sha = hashlib.sha1(content).hexdigest()
        req = urllib.request.Request(
            "https://api.vercel.com/v2/files", data=content, method="POST",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream",
                     "x-vercel-digest": sha, "Content-Length": str(len(content))},
        )
        try:
            with urllib.request.urlopen(req) as r:
                r.read()
        except urllib.error.HTTPError as e:
            if e.code not in (200, 201):
                raise
        return sha

    manifest = []
    for local, vercel_path in deploy_files:
        content = local.read_bytes()
        sha = _upload(content)
        manifest.append({"file": vercel_path, "sha": sha, "size": len(content)})
        print(f"  uploaded {vercel_path} ({len(content):,} bytes)")

    payload = json.dumps({"name": "hospital-monitor", "target": "production", "files": manifest}).encode()
    req = urllib.request.Request(
        f"https://api.vercel.com/v13/deployments?projectId={project_id}&skipAutoDetectionConfirmation=1",
        data=payload, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as r:
        result = json.loads(r.read())
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"  Deployed → https://{result.get('url', '?')} ({stamp})")


# ── Git push (data branch) ────────────────────────────────────────────────────

def _git(cmd: str, cwd: pathlib.Path) -> None:
    result = subprocess.run(
        shlex.split(cmd), cwd=cwd, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"git failed: {cmd}\n{result.stderr.strip()}")


def push_to_data_branch(json_path: pathlib.Path,
                        history_path: pathlib.Path | None = None) -> None:
    """Force-push data files to the GitHub data branch via SSH deploy key."""
    import shutil
    repo_url = subprocess.check_output(
        ["git", "remote", "get-url", "origin"], cwd=_BASE, text=True
    ).strip()

    if not (PUBLISHER_TMPDIR / ".git").exists():
        import shutil as _shutil
        if PUBLISHER_TMPDIR.exists():
            _shutil.rmtree(PUBLISHER_TMPDIR)
        print(f"  Cloning data branch → {PUBLISHER_TMPDIR} …")
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", "data",
             repo_url, str(PUBLISHER_TMPDIR)], check=True
        )
    else:
        _git("git fetch origin data", PUBLISHER_TMPDIR)
        _git("git reset --hard origin/data", PUBLISHER_TMPDIR)

    _git("git rm -rf --cached --quiet .", PUBLISHER_TMPDIR)
    subprocess.run(["git", "clean", "-fdx", "--quiet"],
                   cwd=PUBLISHER_TMPDIR, capture_output=True, check=False)

    import shutil
    shutil.copy(json_path, PUBLISHER_TMPDIR / "latest.json")
    shutil.copy(_BASE / "docs" / "index.html", PUBLISHER_TMPDIR / "index.html")
    write_schema(PUBLISHER_TMPDIR / "schema.json")

    history_file = "history_timeline.json"
    if history_path and history_path.exists():
        shutil.copy(history_path, PUBLISHER_TMPDIR / history_file)
    else:
        history_file = ""  # build_timeline failed this cycle; omit from commit

    vercel_config = {
        "headers": [
            {
                "source": "/latest.json",
                "headers": [{"key": "Cache-Control", "value": _ui_cfg.get("CACHE_LATEST_JSON", "no-cache, no-store, must-revalidate")}]
            },
            {
                "source": "/history_timeline.json",
                "headers": [{"key": "Cache-Control", "value": _ui_cfg.get("CACHE_HISTORY_JSON", "public, max-age=900")}]
            },
        ]
    }
    (PUBLISHER_TMPDIR / "vercel.json").write_text(json.dumps(vercel_config, indent=2))

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    extra_files = (" " + history_file) if history_file else ""
    try:
        _git(f"git add latest.json index.html vercel.json schema.json{extra_files}", PUBLISHER_TMPDIR)
        _git(f'git commit -m "data: outlook {stamp}"', PUBLISHER_TMPDIR)
    except RuntimeError as e:
        if "nothing to commit" in str(e):
            print("  data branch: nothing changed, skipping push.")
            return
        raise
    _git("git push --force origin HEAD:data", PUBLISHER_TMPDIR)
    print(f"  Force-pushed → data branch ({stamp})")

# ── Strain index ──────────────────────────────────────────────────────────────

def compute_strain_index(predicted_wait: float, p90: float) -> float:
    """Predicted wait normalised against historical p90. >1.0 means above normal load."""
    return round(predicted_wait / max(1.0, p90), 3)


# ── Recent forecast accuracy ─────────────────────────────────────────────────

def _load_recent_accuracy(n: int = 6) -> dict[str, float | None]:
    """
    Return {hospital: recent_accuracy} from the last n completed T+60 rows
    in forecast_audit.csv. n=6 ≈ last 90 min of resolved forecasts.
    Returns empty dict if file unavailable.
    """
    result: dict[str, list[float]] = {}
    if not FORECAST_AUDIT_CSV.exists():
        return {}
    try:
        import csv
        with open(FORECAST_AUDIT_CSV, newline="") as f:
            for row in csv.DictReader(f):
                hospital = row.get("hospital", "")
                acc_str  = row.get("forecast_accuracy", "")
                if not hospital or not acc_str:
                    continue
                try:
                    result.setdefault(hospital, []).append(float(acc_str))
                except ValueError:
                    pass
    except Exception:
        return {}
    return {
        h: round(sum(rows[-n:]) / len(rows[-n:]), 1)
        for h, rows in result.items() if rows
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish ED wait-time outlook JSON to the data branch."
    )
    parser.add_argument(
        "--silver", type=pathlib.Path, default=DEFAULT_SILVER,
        help="Silver CSV path (default: SSD path)",
    )
    parser.add_argument(
        "--out", type=pathlib.Path, default=DEFAULT_JSON_OUT,
        help=f"JSON output path (default: {DEFAULT_JSON_OUT})",
    )
    parser.add_argument(
        "--push", action="store_true",
        help="Publish via method in ui_config.json (vercel_api / git_data_branch / dual)",
    )
    args = parser.parse_args()

    # ── 0. Operational-hours gate (06:00–23:00 Melbourne) ─────────────────────
    now_melb = datetime.now(_MELB)
    if not (OPERATIONAL_START_H <= now_melb.hour < OPERATIONAL_END_H):
        print(
            f"Trial Mode: Sleeping "
            f"({now_melb.strftime('%H:%M')} AEST — outside "
            f"{OPERATIONAL_START_H:02d}:00–{OPERATIONAL_END_H:02d}:00)"
        )
        sys.exit(0)

    # ── 1. Load Silver + compute outlook ──────────────────────────────────────
    try:
        silver = load_latest_silver(args.silver)
    except FileNotFoundError:
        print(f"ERROR: Silver CSV not found at {args.silver}", file=sys.stderr)
        print("Run transform_silver.py first.", file=sys.stderr)
        sys.exit(1)

    if silver.empty:
        print("ERROR: No rows for target hospitals in Silver CSV.", file=sys.stderr)
        sys.exit(1)

    generated_utc_dt  = datetime.now(timezone.utc)
    generated_utc_str = generated_utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Hospital-native freshness timestamps from the PBI scrape sidecar
    last_updated_map: dict = {}
    if LAST_UPDATED_SIDECAR.exists():
        try:
            last_updated_map = json.loads(LAST_UPDATED_SIDECAR.read_text())
        except Exception:
            pass

    # Scraper sync times + metadata: how long ago did the Pi last successfully query the raw endpoint?
    scraper_sync_map: dict[str, int] = {}
    cache_lag_map: dict[str, int] = {}
    fidelity_status_map: dict[str, str] = {}
    last_portal_update_map: dict[str, str] = {}

    # latest_paed: {site -> row dict} — latest Paediatric scrape per hospital
    latest_paed: dict[str, dict] = {}

    if BRONZE_RAW_PATH.exists():
        try:
            import csv
            with open(BRONZE_RAW_PATH, "r", newline="") as f:
                reader = csv.DictReader(f)
                latest_adult: dict[str, dict] = {}
                # Separate Adult and Paediatric rows; keep last occurrence of each
                for row in reader:
                    site   = row.get("site", "")
                    cohort = row.get("cohort", "Adult")
                    scrape_ts_str = row.get("scrape_timestamp_utc", "")
                    if not site or not scrape_ts_str:
                        continue
                    if cohort not in ("Adult", "All"):
                        latest_paed[site] = row
                    else:
                        latest_adult[site] = {
                            "scrape_timestamp_utc": scrape_ts_str,
                            "cache_lag_minutes":    row.get("cache_lag_minutes", ""),
                            "fidelity_status":      row.get("fidelity_status", ""),
                            "reported_timestamp_str": row.get("reported_timestamp_str", "")
                        }

                # Process the latest Adult scrape for each site
                for site, data in latest_adult.items():
                    try:
                        scrape_dt = datetime.fromisoformat(data["scrape_timestamp_utc"].replace("Z", "+00:00"))
                        delta_mins = round((generated_utc_dt - scrape_dt).total_seconds() / 60)
                        scraper_sync_map[site] = delta_mins

                        cache_lag_str = data.get("cache_lag_minutes", "")
                        if cache_lag_str:
                            try:
                                cache_lag_map[site] = int(cache_lag_str)
                            except ValueError:
                                pass

                        fidelity = data.get("fidelity_status", "")
                        if fidelity:
                            fidelity_status_map[site] = fidelity

                        portal_ts = data.get("reported_timestamp_str", "")
                        if portal_ts:
                            last_portal_update_map[site] = portal_ts
                    except Exception as e:
                        print(f"  [DEBUG] Error processing {site}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"  [DEBUG] Error reading Bronze Raw: {e}", file=sys.stderr)

    recent_accuracy_map = _load_recent_accuracy(n=int(_ui_cfg.get("RECENT_ACCURACY_LOOKBACK", 6)))

    sites = []
    for _, row in silver.iterrows():
        outlook = build_outlook(row)
        outlook["color"] = traffic_light(
            outlook["predicted_wait_min"], outlook["wait_momentum"]
        )
        obs_dt = datetime.fromisoformat(outlook["latest_obs_utc"].replace("Z", "+00:00"))
        outlook["heartbeat_age_mins"] = round(
            (generated_utc_dt - obs_dt).total_seconds() / 60, 1
        )
        p90_val = row["ctx_wait_p90_mins"]
        outlook["strain_index"] = (
            compute_strain_index(outlook["predicted_wait_min"], float(p90_val))
            if p90_val is not None and not (isinstance(p90_val, float) and math.isnan(p90_val))
            else None
        )
        outlook["last_updated_display"] = last_updated_map.get(outlook["site"], "")
        outlook["scraper_sync_mins"]    = scraper_sync_map.get(outlook["site"])
        outlook["recent_accuracy"]      = recent_accuracy_map.get(outlook["site"])

        # Add unified metadata block for Clinical Pulse architecture
        cache_lag = cache_lag_map.get(outlook["site"])
        fidelity_status = fidelity_status_map.get(outlook["site"])
        last_portal_update = last_portal_update_map.get(outlook["site"], "")

        outlook["metadata"] = {
            "cache_lag_minutes": cache_lag,
            "fidelity_status": fidelity_status,
            "is_stale": fidelity_status == "PORTAL_STALE_WARNING" if fidelity_status else None,
            "last_portal_update": last_portal_update.replace("Last Updated: ", "").replace("~", "") if last_portal_update else None,
            "scrape_timestamp": outlook.get("latest_obs_utc")
        }

        sites.append(outlook)

    # ── UI window filter (upstream — keeps latest.json lean) ──────────────────
    before = len(sites)
    sites = [s for s in sites if (s.get("heartbeat_age_mins") or 0) <= UI_DISPLAY_WINDOW_MINS]
    dropped = before - len(sites)
    if dropped:
        print(f"  [UI filter] {dropped} site(s) dropped (scrape > {UI_DISPLAY_WINDOW_MINS} min old)")

    # ── Attach Paediatric sub-object to sites that have it ────────────────────
    # Paediatric data comes from bronze_raw_scrapes.csv (scraped alongside Adult
    # but not routed through Silver). Attached as s.paediatric for UI rendering.
    # Only included when the scrape is within the UI display window.
    for s in sites:
        paed_row = latest_paed.get(s["site"])
        if not paed_row:
            continue
        try:
            paed_dt    = datetime.fromisoformat(paed_row["scrape_timestamp_utc"].replace("Z", "+00:00"))
            paed_age   = (generated_utc_dt - paed_dt).total_seconds() / 60
            if paed_age > UI_DISPLAY_WINDOW_MINS:
                continue
            waiting  = int(paed_row.get("reported_waiting") or 0)
            treating = int(paed_row.get("raw_query_treating") or 0)
            wait_str = paed_row.get("reported_wait_str", "")
            s["paediatric"] = {
                "waiting":  waiting,
                "treating": treating,
                "wait_str": wait_str,
                "heartbeat_age_mins": round(paed_age, 1),
            }
            print(f"  [Paeds] {s['site']}: {waiting} waiting, {treating} treating, {wait_str}")
        except Exception as e:
            print(f"  [DEBUG] Paeds attach failed for {s['site']}: {e}", file=sys.stderr)

    # ── Status-only sites (raw_only pipeline — e.g. RCH busy index) ──────────────
    # Read the latest Bronze Raw row per raw-only hospital and attach as status_sites.
    # These hospitals have no Silver data; only Bronze Raw is written by their scraper.
    status_sites: list[dict] = []
    if RAW_ONLY_HOSPITALS and BRONZE_RAW_PATH.exists():
        try:
            import csv as _csv
            latest_raw_only: dict[str, dict] = {}
            with open(BRONZE_RAW_PATH, "r", newline="") as _f:
                for row in _csv.DictReader(_f):
                    if row.get("site", "") in RAW_ONLY_HOSPITALS:
                        latest_raw_only[row["site"]] = row
            for site, row in latest_raw_only.items():
                scrape_ts_str = row.get("scrape_timestamp_utc", "")
                try:
                    scrape_dt  = datetime.fromisoformat(scrape_ts_str.replace("Z", "+00:00"))
                    age_mins   = round((generated_utc_dt - scrape_dt).total_seconds() / 60, 1)
                except Exception:
                    age_mins = None
                if age_mins is not None and age_mins > UI_DISPLAY_WINDOW_MINS:
                    continue  # stale — skip
                wait_str  = row.get("reported_wait_str", "")
                # busy_label: human status label (e.g. "Extremely Busy", "Normal")
                # busy_index: numeric value (e.g. 95.793) — stored alongside label
                _known_labels = {"Normal", "Very Busy", "Extremely Busy"}
                busy_label: str | None = wait_str if wait_str in _known_labels else None
                busy_index: float | None = None
                if wait_str.startswith("Busy: "):
                    try:
                        busy_index = float(wait_str.replace("Busy: ", ""))
                    except ValueError:
                        pass
                status_sites.append({
                    "site":                site,
                    "scrape_timestamp_utc": scrape_ts_str,
                    "heartbeat_age_mins":  age_mins,
                    "reported_wait_str":   wait_str,
                    "busy_label":          busy_label,
                    "busy_index":          busy_index,
                    "fidelity_status":     row.get("fidelity_status", ""),
                    "last_portal_update":  row.get("reported_timestamp_str", ""),
                })
                if busy_index is not None:
                    print(f"  [Status] {site}: Busy Index {busy_index:.1f}")
        except Exception as _exc:
            print(f"  [DEBUG] status_sites build failed: {_exc}", file=sys.stderr)

    quarter = (generated_utc_dt.month - 1) // 3 + 1
    vahi_qly_label = f"Q{quarter} {generated_utc_dt.year}"

    payload = {
        "schema_version":   SCHEMA_VERSION,
        "generated_utc":    generated_utc_str,
        "horizon_min":      60,
        "vahi_p90_all_mins": VAHI_BENCHMARKS.get("p90_all_mins"),
        "vahi_qly_label":   vahi_qly_label,
        "sites":            sites,
        "status_sites":     status_sites,
        "ui_thresholds": {
            "HOSPITAL_STALE_MINS":         int(_ui_cfg.get("HOSPITAL_STALE_MINS",          60)),
            "TRAFFIC_LIGHT_GREEN_MAX_MINS": float(_ui_cfg.get("TRAFFIC_LIGHT_GREEN_MAX_MINS", 30)),
            "TRAFFIC_LIGHT_MOMENTUM_MAX":   float(_ui_cfg.get("TRAFFIC_LIGHT_MOMENTUM_MAX",   2)),
            "TRAFFIC_LIGHT_AMBER_MAX_MINS": float(_ui_cfg.get("TRAFFIC_LIGHT_AMBER_MAX_MINS", 60)),
            "CRISIS_AMBER_P90_RATIO":       float(_ui_cfg.get("CRISIS_AMBER_P90_RATIO",       0.80)),
            "CRISIS_CRITICAL_P90_RATIO":    float(_ui_cfg.get("CRISIS_CRITICAL_P90_RATIO",    1.00)),
            "MAX_WAIT_CRITICAL_P90_RATIO":  float(_ui_cfg.get("MAX_WAIT_CRITICAL_P90_RATIO",  5)),
            "RECENT_ACCURACY_LOOKBACK":     int(_ui_cfg.get("RECENT_ACCURACY_LOOKBACK",       6)),
        },
    }

    # ── 2. Write JSON ──────────────────────────────────────────────────────────
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2))

    # ── 3. Console summary ─────────────────────────────────────────────────────
    colour_icon = {"green": "🟢", "amber": "🟡", "red": "🔴"}
    print(f"\n  Outlook — {payload['generated_utc']}")
    print(f"  {'Hospital':<26} {'Now':>5}  {'60min':>5}  {'Momentum':>9}  Conf   Color")
    print(f"  {'─'*26} {'─'*5}  {'─'*5}  {'─'*9}  {'─'*5}  {'─'*5}")
    for s in sites:
        sign = "+" if s["wait_momentum"] >= 0 else ""
        strain_str = f"strain={s['strain_index']:.2f}" if s['strain_index'] is not None else "strain=—"
        print(
            f"  {s['site']:<26} {s['current_wait_min']:>4.0f}m "
            f" {s['predicted_wait_min']:>4.0f}m "
            f" {sign}{s['wait_momentum']:>+6.1f}/15m "
            f" {s['confidence_label']:<8} "
            f" {strain_str} "
            f" {colour_icon.get(s['color'], s['color'])}"
        )
    print(f"\n  latest.json → {args.out}")

    # ── 4. Build history timeline (UI window) ────────────────────────────────
    # history_timeline.json is trimmed to UI_DISPLAY_WINDOW_MINS (default 3h).
    # Full historical accuracy data lives in forecast_audit.csv (never filtered).
    history_path: pathlib.Path | None = None
    history_hours = UI_DISPLAY_WINDOW_MINS / 60  # e.g. 180 min → 3.0 h
    try:
        timeline = build_timeline(args.silver, history_hours=history_hours)
        history_path = DEFAULT_HISTORY_OUT
        history_path.write_text(json.dumps(timeline, indent=2, allow_nan=False))
        print(f"\n  History timeline: {len(timeline['snapshots'])} snapshots "
              f"({history_hours:.1f}h window) → {history_path}")
    except Exception as e:
        print(f"\n  Warning: history timeline skipped: {e}", file=sys.stderr)

    # ── 5. Publish (env PUBLISH_METHOD → ui_config.json → default vercel_api) ──
    if args.push:
        method         = os.environ.get("PUBLISH_METHOD") or _ui_cfg.get("publish_method", "vercel_api")
        git_interval   = int(_ui_cfg.get("git_push_interval_mins",    30))
        vercel_interval= int(_ui_cfg.get("vercel_deploy_interval_mins", 60))
        do_git    = method in ("git_data_branch", "dual")
        do_vercel = method in ("vercel_api",      "dual")

        if do_git and _git_due(git_interval):
            print("\n  Pushing to GitHub data branch …")
            try:
                push_to_data_branch(args.out, history_path)
                _git_stamp()
            except Exception as e:
                print(f"  Git push failed: {e}", file=sys.stderr)
                print("  Check SSH deploy key and data branch exist.", file=sys.stderr)
                if not do_vercel:
                    sys.exit(1)

        if do_vercel and _vercel_due(vercel_interval):
            print("\n  Deploying to Vercel …")
            try:
                deploy_to_vercel(args.out, history_path)
                _vercel_stamp()
            except Exception as e:
                print(f"  Vercel deploy failed: {e}", file=sys.stderr)
                if not do_git:
                    sys.exit(1)


if __name__ == "__main__":
    main()
