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

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from predict_next import load_latest_silver, build_outlook   # noqa: E402

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = pathlib.Path(__file__).resolve().parent.parent
_SSD  = pathlib.Path("/mnt/router_ssd/Data_Hub/Waiting_Live_time")

DEFAULT_SILVER   = _SSD / "eastern_hospital_silver.csv"
DEFAULT_JSON_OUT = pathlib.Path("/tmp/hospital_monitor_latest.json")
PUBLISHER_TMPDIR = pathlib.Path("/tmp/publisher")   # staging clone for data branch

_MELB                  = ZoneInfo("Australia/Melbourne")
OPERATIONAL_START_H    = 6     # 06:00 Melbourne — before this, skip and exit
OPERATIONAL_END_H      = 23    # 23:00 Melbourne — after this, skip and exit
DIVERSION_STRAIN_DELTA = 0.40  # strain_index gap that triggers a diversion suggestion

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


def push_to_data_branch(json_path: pathlib.Path) -> None:
    """
    Force-push latest.json to the `data` branch via SSH deploy key.
    Uses a persistent shallow clone at PUBLISHER_TMPDIR to keep pushes fast.
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

    import shutil
    shutil.copy(json_path, PUBLISHER_TMPDIR / "latest.json")

    # Vercel: no-cache so the browser always gets the freshest file
    vercel_config = {
        "headers": [
            {
                "source": "/latest.json",
                "headers": [{"key": "Cache-Control", "value": "no-cache, no-store, must-revalidate"}]
            }
        ]
    }
    (PUBLISHER_TMPDIR / "vercel.json").write_text(json.dumps(vercel_config, indent=2))

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        _git("git add latest.json vercel.json", PUBLISHER_TMPDIR)
        _git(f'git commit -m "data: outlook {stamp}"', PUBLISHER_TMPDIR)
    except RuntimeError as e:
        if "nothing to commit" in str(e):
            print("  data branch: nothing changed, skipping push.")
            return
        raise
    _git("git push --force origin HEAD:data", PUBLISHER_TMPDIR)
    print(f"  Force-pushed latest.json → data branch ({stamp})")

# ── Strain index + diversion ──────────────────────────────────────────────────

def compute_strain_index(predicted_wait: float, p90: float) -> float:
    """Predicted wait normalised against historical p90. >1.0 means above normal load."""
    return round(predicted_wait / max(1.0, p90), 3)


def annotate_diversion(sites: list) -> None:
    """
    Mutates site dicts in place.
    Diversion is only meaningful within the same health network — comparisons
    are scoped per-network. Cross-network suggestions are never generated.
    """
    by_network: dict[str, list] = {}
    for s in sites:
        by_network.setdefault(s.get("network", ""), []).append(s)

    for network_sites in by_network.values():
        if len(network_sites) < 2:
            for s in network_sites:
                s["suggest_diversion"] = False
            continue
        least = min(network_sites, key=lambda s: s["strain_index"])
        for s in network_sites:
            gap = s["strain_index"] - least["strain_index"]
            if s is not least and gap >= DIVERSION_STRAIN_DELTA:
                s["suggest_diversion"] = True
                s["diversion_to"]      = least["site"]
            else:
                s["suggest_diversion"] = False


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
        sites.append(outlook)

    annotate_diversion(sites)

    payload = {
        "generated_utc": generated_utc_str,
        "horizon_min":   60,
        "sites":         sites,
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
        sign   = "+" if s["wait_momentum"] >= 0 else ""
        divert = f"  → divert to {s['diversion_to']}" if s.get("suggest_diversion") else ""
        print(
            f"  {s['site']:<26} {s['current_wait_min']:>4.0f}m "
            f" {s['predicted_wait_min']:>4.0f}m "
            f" {sign}{s['wait_momentum']:>+6.1f}/15m "
            f" {s['confidence_label']:<8} "
            f" strain={s['strain_index']:.2f} "
            f" {colour_icon.get(s['color'], s['color'])}{divert}"
        )
    print(f"\n  latest.json → {args.out}")

    # ── 4. Optional git push ───────────────────────────────────────────────────
    if args.push:
        print("\n  Pushing to data branch …")
        try:
            push_to_data_branch(args.out)
        except Exception as e:
            print(f"  Push failed: {e}", file=sys.stderr)
            print("  Check SSH deploy key is configured.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
