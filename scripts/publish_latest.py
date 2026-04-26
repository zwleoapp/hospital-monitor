# data-class: public-aggregate
"""
publish_latest.py — Phase 1 publisher: Silver → JSON → docs/index.html

Pipeline (run after transform_silver.py):
  1. Load the most-recent Silver CSV row per hospital
  2. Compute outlook via predict_next logic (wait + momentum + VAHI baseline)
  3. Write docs/latest.json  (JSON payload for the data branch / git push)
  4. Write docs/index.html   (standalone traffic-light dashboard, data embedded —
                               works as file:// on the Pi with no server required)
  5. --push: force-push latest.json to the `data` branch via SSH deploy key

Usage:
  python3 scripts/publish_latest.py
  python3 scripts/publish_latest.py --silver /path/to/silver.csv --out /path/to/docs/
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

# Resolve sibling scripts without pip-installing them
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from predict_next import load_latest_silver, build_outlook   # noqa: E402

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = pathlib.Path(__file__).resolve().parent.parent
_SSD  = pathlib.Path("/mnt/router_ssd/Data_Hub/Waiting_Live_time")

DEFAULT_SILVER   = _SSD / "eastern_hospital_silver.csv"
DEFAULT_SITE_DIR = _BASE / "docs"
PUBLISHER_TMPDIR = pathlib.Path("/tmp/publisher")   # staging clone for data branch

_MELB                  = ZoneInfo("Australia/Melbourne")
OPERATIONAL_START_H    = 6     # 06:00 Melbourne — before this, skip and exit
OPERATIONAL_END_H      = 23    # 23:00 Melbourne — after this, skip and exit
DIVERSION_STRAIN_DELTA = 0.40  # strain_index gap that triggers a diversion suggestion

# ── Traffic-light helper ──────────────────────────────────────────────────────

def traffic_light(predicted_wait: float, momentum: float) -> str:
    """
    Patient-centric colour: based on where they'll likely wait IN 60 MINUTES.
      Green : predicted ≤ 30 min and trend is stable or improving
      Amber : predicted 31-60 min  (or short but rising fast)
      Red   : predicted  > 60 min
    """
    if predicted_wait <= 30 and momentum <= 2:
        return "green"
    if predicted_wait <= 60:
        return "amber"
    return "red"

# ── HTML template (Pi-local embedded version — mirrors docs/index.html layout) ─

_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Eastern Health ED — Wait Time Outlook</title>
  <style>
    :root {{
      --green:#43a047; --green-bg:#e8f5e9; --green-dim:#c8e6c9;
      --amber:#fb8c00; --amber-bg:#fff3e0;
      --red:#e53935;   --red-bg:#ffebee;
      --indigo:#3949ab; --indigo-bg:#e8eaf6; --indigo-dim:#c5cae9;
    }}
    *{{box-sizing:border-box;margin:0;padding:0}}
    .disclaimer-bar{{
      position:sticky;top:0;z-index:100;
      background:#1a1a2e;color:#fff;
      text-align:center;padding:.55rem 1rem;
      font-size:.78rem;line-height:1.5;
    }}
    .disclaimer-bar strong{{color:#ef9a9a}}
    .disclaimer-bar .emg{{color:#ffcc02;font-weight:700}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
          background:#f0f2f5;color:#222;min-height:100vh;padding:1.2rem}}
    header{{text-align:center;margin-bottom:1.6rem}}
    h1{{font-size:1.25rem;font-weight:700;color:#1a1a2e}}
    .subtitle{{font-size:.76rem;color:#999;margin-top:.3rem}}
    #stale-banner{{
      display:none;background:#ffebee;border:2px solid #e53935;
      border-radius:10px;padding:.9rem 1.2rem;
      text-align:center;max-width:920px;margin:0 auto 1.2rem;color:#c62828;
    }}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));
           gap:1rem;max-width:920px;margin:0 auto}}
    .card{{background:#fff;border-radius:14px;
           box-shadow:0 2px 12px rgba(0,0,0,.07);
           border-left:5px solid #ddd;overflow:hidden}}
    .card.green{{border-color:var(--green)}}
    .card.amber{{border-color:var(--amber)}}
    .card.red{{border-color:var(--red)}}
    .patient-headline{{padding:1.2rem 1.3rem .9rem}}
    .card-header{{display:flex;align-items:center;gap:.5rem;margin-bottom:.85rem}}
    .dot{{width:11px;height:11px;border-radius:50%;flex-shrink:0}}
    .green .dot{{background:var(--green);box-shadow:0 0 7px var(--green)}}
    .amber .dot{{background:var(--amber);box-shadow:0 0 7px var(--amber)}}
    .red   .dot{{background:var(--red);  box-shadow:0 0 7px var(--red)}}
    .hosp{{font-weight:700;font-size:.96rem}}
    .waits{{display:flex;gap:1.8rem;align-items:flex-end;margin-bottom:.7rem}}
    .wb label{{font-size:.65rem;text-transform:uppercase;letter-spacing:.06em;
               color:#aaa;display:block;margin-bottom:.15rem}}
    .wv{{font-size:2rem;font-weight:800;line-height:1}}
    .wu{{font-size:.7rem;color:#bbb;margin-left:1px}}
    .current .wv{{color:#1a1a2e}}
    .predicted .wv{{color:#777}}
    .trend{{font-size:.8rem;color:#666;margin-bottom:.65rem}}
    .up{{color:var(--red)}} .dn{{color:var(--green)}} .fl{{color:#bbb}}
    .obs-meta{{display:flex;gap:.5rem;flex-wrap:wrap;align-items:center;
               font-size:.65rem;color:#bbb}}
    .badge{{font-size:.67rem;font-weight:600;padding:.18rem .55rem;border-radius:99px}}
    .bh{{background:var(--green-bg);color:var(--green)}}
    .bm{{background:var(--amber-bg);color:var(--amber)}}
    .bl{{background:var(--red-bg);  color:var(--red)}}
    .diversion-banner{{
      margin:0 1.3rem .85rem;
      background:var(--indigo-bg);border:1px solid var(--indigo-dim);
      border-radius:8px;padding:.65rem .9rem;
      display:flex;align-items:flex-start;gap:.65rem;
    }}
    .diversion-icon{{font-size:1.2rem;line-height:1.1;flex-shrink:0;color:var(--indigo)}}
    .diversion-body{{font-size:.78rem;color:#283593;line-height:1.4}}
    .diversion-body strong{{display:block;margin-bottom:.15rem;font-size:.8rem}}
    .divert-to{{font-weight:700;font-size:.85rem}}
    .divert-note{{font-size:.69rem;opacity:.75}}
    .system-insights{{
      background:#f7f9fc;border-top:1px solid #eef0f4;
      padding:.65rem 1.3rem .75rem;
    }}
    .insights-label{{
      font-size:.6rem;text-transform:uppercase;letter-spacing:.08em;
      color:#bbb;font-weight:600;margin-bottom:.45rem;
    }}
    .insights-row{{display:flex;gap:1.4rem;flex-wrap:wrap}}
    .insight{{display:flex;flex-direction:column;gap:.1rem;min-width:60px}}
    .insight .il{{font-size:.6rem;text-transform:uppercase;letter-spacing:.05em;color:#bbb}}
    .insight .iv{{font-size:.82rem;font-weight:700;color:#444}}
    .strain-low{{color:var(--green)}}
    .strain-mid{{color:var(--amber)}}
    .strain-high{{color:var(--red)}}
    .wait-range{{font-size:.72rem;color:#aaa;margin-top:.2rem}}
    footer{{text-align:center;font-size:.67rem;color:#ccc;margin-top:1.5rem}}
    footer a{{color:#ccc}}
  </style>
</head>
<body>

<div class="disclaimer-bar">
  <strong>NOT CLINICAL ADVICE</strong> &mdash;
  Statistical estimates from public data only. Always verify with hospital staff.
  &nbsp; Life-threatening emergency? Call <span class="emg">000</span> immediately.
</div>

<header>
  <h1>Eastern Health Emergency &mdash; 60-min Wait Outlook</h1>
  <div class="subtitle">
    Updated <span id="gt"></span> &nbsp;&middot;&nbsp;
    Public-aggregate data only &nbsp;&middot;&nbsp; Auto-refreshes every 5 min
  </div>
</header>

<div id="stale-banner">
  <strong>&#9888; DATA STALE</strong> &mdash;
  Last update was <span id="stale-age">?</span> minutes ago.
  <div style="font-weight:400;font-size:.78rem;margin-top:.25rem;color:#b71c1c;">
    The Pi may be offline or the scraper may have failed.
    Do not rely on these predictions &mdash; verify directly with the hospital.
  </div>
</div>

<div class="grid" id="grid"></div>

<footer>
  Phase 1 heuristic baseline &mdash; accuracy improves as the ML model trains.
  &nbsp;&middot;&nbsp; Raspberry Pi &middot;
  <a href="https://github.com/zwleoapp/hospital-monitor">github.com/zwleoapp/hospital-monitor</a>
</footer>

<script>
const OUTLOOK    = {json_payload};
const STALE_MINS = 30;

function light(p, m) {{
  if (p <= 30 && m <= 2) return "green";
  if (p <= 60)           return "amber";
  return "red";
}}
function arrow(m) {{
  if (m >  2) return {{cls:"up", sym:"&#8593;", lbl:"Rising"}};
  if (m < -2) return {{cls:"dn", sym:"&#8595;", lbl:"Improving"}};
  return             {{cls:"fl", sym:"&#8594;", lbl:"Stable"}};
}}
function confCls(lbl) {{
  return {{High:"bh", Moderate:"bm", Low:"bl"}}[lbl] || "bm";
}}
function strainCls(v) {{
  if (v < 0.70) return "strain-low";
  if (v < 1.00) return "strain-mid";
  return "strain-high";
}}
function localTime(utc) {{
  return new Date(utc).toLocaleTimeString("en-AU",
    {{timeZone:"Australia/Melbourne", hour:"2-digit", minute:"2-digit"}});
}}
function fmtMins(m) {{
  if (!m || m <= 0) return null;
  const h = Math.floor(m / 60), r = m % 60;
  if (h === 0) return r + " min";
  if (r === 0) return h + " hr";
  return h + " hr " + r + " min";
}}
function fmtShort(m) {{
  if (m == null || m <= 0) return null;
  const h = Math.floor(m / 60), r = m % 60;
  if (h === 0) return r + "m";
  return r === 0 ? h + "h" : h + "h " + r + "m";
}}

function renderCard(s) {{
  const c    = light(s.predicted_wait_min, s.wait_momentum);
  const a    = arrow(s.wait_momentum);
  const sign = s.wait_momentum >= 0 ? "+" : "";

  const diversionHtml = s.suggest_diversion ? `
    <div class="diversion-banner">
      <span class="diversion-icon">&#8644;</span>
      <div class="diversion-body">
        <strong>Consider directing to</strong>
        <span class="divert-to">${{s.diversion_to}}</span>
        <span class="divert-note">Lower current system strain</span>
      </div>
    </div>` : "";

  const strainHtml = s.strain_index != null ? `
    <div class="insight">
      <span class="il">Strain Index</span>
      <span class="iv ${{strainCls(s.strain_index)}}">${{s.strain_index.toFixed(2)}}&times;</span>
    </div>` : "";

  return `
    <div class="card ${{c}}">
      <div class="patient-headline">
        <div class="card-header">
          <div class="dot"></div>
          <div class="hosp">${{s.site}}</div>
        </div>
        <div class="waits">
          <div class="wb current">
            <label>Now</label>
            <span class="wv">${{Math.round(s.current_wait_min)}}</span>
            <span class="wu">min</span>
            ${{fmtShort(s.max_wait_min) ? `<div class="wait-range">&ndash;&nbsp;${{fmtShort(s.max_wait_min)}}</div>` : ""}}
          </div>
          <div class="wb predicted">
            <label>In 60 min</label>
            <span class="wv">${{Math.round(s.predicted_wait_min)}}</span>
            <span class="wu">min</span>
          </div>
        </div>
        <div class="trend">
          <span class="${{a.cls}}">${{a.sym}}</span>
          ${{a.lbl}} &nbsp;&middot;&nbsp; ${{sign}}${{s.wait_momentum.toFixed(1)}} min / 15 min
        </div>
        <div class="obs-meta">
          <span class="badge ${{confCls(s.confidence_label)}}">
            ${{s.confidence_label}} confidence &nbsp;${{(s.confidence*100).toFixed(0)}}%
          </span>
          <span>Last obs: ${{localTime(s.latest_obs_utc)}} AEST</span>
        </div>
      </div>
      ${{diversionHtml}}
      <div class="system-insights">
        <div class="insights-label">System Insights</div>
        <div class="insights-row">
          ${{strainHtml}}
          <div class="insight">
            <span class="il">Momentum</span>
            <span class="iv ${{a.cls}}">${{sign}}${{s.wait_momentum.toFixed(1)}}<small style="font-weight:400;font-size:.65rem"> min/15m</small></span>
          </div>
          <div class="insight">
            <span class="il">Source</span>
            <span class="iv">${{s.ctx_source}}</span>
          </div>
        </div>
      </div>
    </div>`;
}}

document.getElementById("gt").textContent =
  new Date(OUTLOOK.generated_utc).toLocaleString("en-AU",
    {{timeZone:"Australia/Melbourne"}});

document.getElementById("grid").innerHTML =
  OUTLOOK.sites.map(renderCard).join("");

function checkStale() {{
  const genMs            = new Date(OUTLOOK.generated_utc).getTime();
  const minsSincePublish = (Date.now() - genMs) / 60000;
  const dataAge          = Math.max(...OUTLOOK.sites.map(s => s.heartbeat_age_mins ?? 0));
  const totalAge         = dataAge + minsSincePublish;
  const isStale          = totalAge > STALE_MINS;

  const banner = document.getElementById("stale-banner");
  const grid   = document.getElementById("grid");
  banner.style.display = isStale ? "block" : "none";
  if (isStale) {{
    document.getElementById("stale-age").textContent = Math.round(totalAge);
    grid.style.opacity       = "0.3";
    grid.style.filter        = "grayscale(1)";
    grid.style.pointerEvents = "none";
  }} else {{
    grid.style.opacity = grid.style.filter = grid.style.pointerEvents = "";
  }}
}}

checkStale();
setInterval(checkStale, 60_000);
setTimeout(() => location.reload(), 5 * 60 * 1000);
</script>
</body>
</html>
"""

# ── Git push (Phase 1 — data branch) ─────────────────────────────────────────

def _git(cmd: str, cwd: pathlib.Path) -> None:
    """Run a git command, raise on failure."""
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

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        _git("git add latest.json", PUBLISHER_TMPDIR)
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
    Diversion is only meaningful within the same health network, so comparisons
    are scoped per-network. Cross-network suggestions are never generated.
    """
    from itertools import groupby
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
        description="Publish ED wait-time outlook to docs/index.html + latest.json."
    )
    parser.add_argument(
        "--silver", type=pathlib.Path, default=DEFAULT_SILVER,
        help="Silver CSV path (default: SSD path)",
    )
    parser.add_argument(
        "--out", type=pathlib.Path, default=DEFAULT_SITE_DIR,
        help="Output directory for index.html + latest.json (default: docs/)",
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
        # How old is the source observation at publish time?
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

    # ── 2. Write outputs ───────────────────────────────────────────────────────
    args.out.mkdir(parents=True, exist_ok=True)

    json_path = args.out / "latest.json"
    json_path.write_text(json.dumps(payload, indent=2))

    html_path = args.out / "index.html"
    html_path.write_text(
        _HTML.format(json_payload=json.dumps(payload, indent=2))
    )

    # ── 3. Console summary ─────────────────────────────────────────────────────
    colour_icon = {"green": "🟢", "amber": "🟡", "red": "🔴"}
    print(f"\n  Outlook — {payload['generated_utc']}")
    print(f"  {'Hospital':<26} {'Now':>5}  {'60min':>5}  {'Momentum':>9}  Conf   Color")
    print(f"  {'─'*26} {'─'*5}  {'─'*5}  {'─'*9}  {'─'*5}  {'─'*5}")
    for s in sites:
        sign     = "+" if s["wait_momentum"] >= 0 else ""
        divert   = f"  → divert to {s['diversion_to']}" if s.get("suggest_diversion") else ""
        print(
            f"  {s['site']:<26} {s['current_wait_min']:>4.0f}m "
            f" {s['predicted_wait_min']:>4.0f}m "
            f" {sign}{s['wait_momentum']:>+6.1f}/15m "
            f" {s['confidence_label']:<8} "
            f" strain={s['strain_index']:.2f} "
            f" {colour_icon.get(s['color'], s['color'])}{divert}"
        )

    print(f"\n  index.html → {html_path}")
    print(f"  latest.json → {json_path}")
    print(f"\n  Open in browser: file://{html_path.resolve()}")

    # ── 4. Optional git push ───────────────────────────────────────────────────
    if args.push:
        print("\n  Pushing to data branch …")
        try:
            push_to_data_branch(json_path)
        except Exception as e:
            print(f"  Push failed: {e}", file=sys.stderr)
            print("  Check SSH deploy key is configured.", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
