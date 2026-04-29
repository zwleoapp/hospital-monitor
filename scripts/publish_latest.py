# data-class: public-aggregate
"""
publish_latest.py — Silver → JSON → data branch

Pipeline (run after transform_silver.py):
  1. Load the most-recent Silver CSV row per hospital
  2. Compute outlook via predict_next logic (wait + momentum + VAHI baseline)
  3. Write latest.json to a staging path (/tmp by default, gitignored in docs/)
  4. --push: force-push latest.json to the `data` branch via SSH deploy key

docs/index.html is a static file committed to main; it fetches latest.json
from the data branch at runtime — no embedded payload, no local write needed.

Usage:
  python3 scripts/publish_latest.py
  python3 scripts/publish_latest.py --silver /path/to/silver.csv
  python3 scripts/publish_latest.py --push   # requires SSH deploy key configured
"""

import sys
import json
import shlex
import argparse
import pathlib
import subprocess
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))  # repo root for config
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from predict_next import load_latest_silver, build_outlook   # noqa: E402
from get_history import build_timeline                        # noqa: E402
from config.hospitals import VAHI_BENCHMARKS                  # noqa: E402

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = pathlib.Path(__file__).resolve().parent.parent
_SSD  = pathlib.Path("/mnt/router_ssd/Data_Hub/Waiting_Live_time")

DEFAULT_SILVER        = _SSD / "eastern_hospital_silver.csv"
DEFAULT_JSON_OUT      = pathlib.Path("/tmp/hospital_monitor_latest.json")
DEFAULT_HISTORY_OUT   = pathlib.Path("/tmp/history_timeline.json")
PUBLISHER_TMPDIR      = pathlib.Path("/tmp/publisher")   # staging clone for data branch
LAST_UPDATED_SIDECAR  = _SSD / "monash_last_updated.json"  # written by hospital_monitor.py

_MELB                  = ZoneInfo("Australia/Melbourne")
OPERATIONAL_START_H    = 6     # 06:00 Melbourne — before this, skip and exit
OPERATIONAL_END_H      = 23    # 23:00 Melbourne — after this, skip and exit

# ── Traffic-light helper ──────────────────────────────────────────────────────

def traffic_light(predicted_wait: float, momentum: float) -> str:
    """
    Patient-centric colour based on where the patient will likely wait in 60 min.
      Green : predicted ≤ 30 min and trend stable or improving
      Amber : predicted 31–60 min (or short but rising fast)
      Red   : predicted  > 60 min
    """
    if predicted_wait <= 30 and momentum <= 2:
        return "green"
    if predicted_wait <= 60:
        return "amber"
    return "red"

# ── Git push (data branch) ────────────────────────────────────────────────────

def _git(cmd: str, cwd: pathlib.Path) -> None:
    result = subprocess.run(
        shlex.split(cmd), cwd=cwd, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"git failed: {cmd}\n{result.stderr.strip()}")


def push_to_data_branch(json_path: pathlib.Path,
                        history_path: pathlib.Path | None = None) -> None:
    """
    Force-push data files to the `data` branch via SSH deploy key.
    Each commit is a clean slate containing exactly 4 files:
      index.html, latest.json, history_timeline.json, vercel.json
    This removes any source-code files that crept in via earlier manual pushes.
    """
    repo_url = subprocess.check_output(
        ["git", "remote", "get-url", "origin"],
        cwd=_BASE, text=True
    ).strip()

    if not PUBLISHER_TMPDIR.exists():
        print(f"  Cloning data branch → {PUBLISHER_TMPDIR} …")
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", "data",
             repo_url, str(PUBLISHER_TMPDIR)],
            check=True
        )
    else:
        _git("git fetch origin data", PUBLISHER_TMPDIR)
        _git("git reset --hard origin/data", PUBLISHER_TMPDIR)

    # Strip everything from the index and working tree so only our 4 files land in the commit.
    # Handles the case where manual pushes contaminated the data branch with source files.
    _git("git rm -rf --cached --quiet .", PUBLISHER_TMPDIR)
    subprocess.run(["git", "clean", "-fdx", "--quiet"],
                   cwd=PUBLISHER_TMPDIR, capture_output=True, check=False)

    import shutil
    shutil.copy(json_path, PUBLISHER_TMPDIR / "latest.json")
    shutil.copy(_BASE / "docs" / "index.html", PUBLISHER_TMPDIR / "index.html")

    history_file = "history_timeline.json"
    if history_path and history_path.exists():
        shutil.copy(history_path, PUBLISHER_TMPDIR / history_file)
    else:
        history_file = ""  # build_timeline failed this cycle; omit from commit

    # ignoreCommand: Vercel skips rebuild when only data JSON files change
    vercel_config = {
        "ignoreCommand": (
            "git diff HEAD^ HEAD --name-only"
            " | grep -qvE '(latest|history_timeline)\\.json$'"
            " && exit 1 || exit 0"
        ),
        "headers": [
            {
                "source": "/latest.json",
                "headers": [{"key": "Cache-Control", "value": "no-cache, no-store, must-revalidate"}]
            },
            {
                "source": "/history_timeline.json",
                "headers": [{"key": "Cache-Control", "value": "public, max-age=900"}]
            },
        ]
    }
    (PUBLISHER_TMPDIR / "vercel.json").write_text(json.dumps(vercel_config, indent=2))

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    extra_files = (" " + history_file) if history_file else ""
    try:
        _git(f"git add latest.json index.html vercel.json{extra_files}", PUBLISHER_TMPDIR)
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
        help="Force-push latest.json to the git data branch via SSH deploy key",
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
        outlook["strain_index"] = compute_strain_index(
            outlook["predicted_wait_min"], float(row["ctx_wait_p90_mins"])
        )
        outlook["last_updated_display"] = last_updated_map.get(outlook["site"], "")
        sites.append(outlook)

    quarter = (generated_utc_dt.month - 1) // 3 + 1
    vahi_qly_label = f"Q{quarter} {generated_utc_dt.year}"

    payload = {
        "generated_utc":    generated_utc_str,
        "horizon_min":      60,
        "vahi_p90_all_mins": VAHI_BENCHMARKS.get("p90_all_mins"),
        "vahi_qly_label":   vahi_qly_label,
        "sites":            sites,
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
        print(
            f"  {s['site']:<26} {s['current_wait_min']:>4.0f}m "
            f" {s['predicted_wait_min']:>4.0f}m "
            f" {sign}{s['wait_momentum']:>+6.1f}/15m "
            f" {s['confidence_label']:<8} "
            f" strain={s['strain_index']:.2f} "
            f" {colour_icon.get(s['color'], s['color'])}"
        )
    print(f"\n  latest.json → {args.out}")

    # ── 4. Build 24h history timeline ─────────────────────────────────────────
    history_path: pathlib.Path | None = None
    try:
        timeline = build_timeline(args.silver)
        history_path = DEFAULT_HISTORY_OUT
        history_path.write_text(json.dumps(timeline, indent=2))
        print(f"\n  History timeline: {len(timeline['snapshots'])} snapshots → {history_path}")
    except Exception as e:
        print(f"\n  Warning: history timeline skipped: {e}", file=sys.stderr)

    # ── 5. Optional git push ───────────────────────────────────────────────────
    if args.push:
        print("\n  Pushing to data branch …")
        try:
            push_to_data_branch(args.out, history_path)
        except Exception as e:
            print(f"  Push failed: {e}", file=sys.stderr)
            print("  Check SSH deploy key is configured.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
