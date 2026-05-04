# Dataflow — Hospital Monitor Pipeline

**Updated:** 2026-05-04

> Companion to [DESIGN.md](DESIGN.md). This page covers *how data moves* — scraper types, Bronze → Silver → Gold pipeline, timestamp provenance, publish methods, and Vercel configuration.

---

## Hospital Registry

| Hospital | Network | Parser | Pipeline |
|---|---|---|---|
| Box Hill Hospital | Eastern Health | `html_js` | full |
| Angliss Hospital | Eastern Health | `html_js` | full |
| Maroondah Hospital | Eastern Health | `html_js` | full |
| Casey Hospital | Monash Health | `powerbi` | full |
| Dandenong Hospital | Monash Health | `powerbi` | full |
| Monash Medical Centre - Clayton | Monash Health | `powerbi` | full |
| Royal Melbourne Hospital | Melbourne Health | `html_regex` | full |
| Royal Childrens Hospital | Royal Childrens Hospital | `html_regex` | raw_only |

**full pipeline** → Bronze CSV + Bronze Raw → Silver → Gold (`latest.json`)
**raw_only** → Bronze Raw only → `status_sites[]` in Gold (no forecast)

All hospital names, URLs, credentials, and extraction patterns live in **`config/hospitals.json`** and **`config/hospitals.csv`**. No hospital-specific values are hardcoded in scraper scripts.

---

## Ops Guide — Adding, Renaming, or Removing a Hospital

### Adding a new hospital

1. **`config/hospitals.json`** — add a source entry with parser type, URL, and patterns/credentials.
   For `html_regex` sources also add `status_map` (if categorical) and optionally `ctx_defaults`
   (temporary proxy VAHI benchmarks — remove once step 4 is done).

2. **`config/hospitals.csv`** — add one row:
   ```
   name, network_type, scraper_type, vahi_id, aihw_id, is_active, pipeline
   ```
   - `pipeline`: `full` (has wait times) or `raw_only` (status/index only, e.g. RCH)

3. **Set `vahi_id`** — look up the hospital's exact "Organisation Description":
   ```bash
   python3 scripts/fetch_vahi.py --list-orgs
   ```
   Lists all 42 Victorian hospitals; `✓` marks those already mapped in `hospitals.csv`.
   - If the VAHI name **matches** the formal name exactly → leave `vahi_id` blank.
   - If it **differs** → set `vahi_id` to the VAHI name.

4. **Rebuild VAHI merged file**:
   ```bash
   python3 scripts/fetch_vahi.py
   ```

5. **Rebuild Silver**:
   ```bash
   python3 scripts/transform_silver.py
   ```

6. **Test scrape + publish**:
   ```bash
   python3 scripts/hospital_monitor.py
   python3 scripts/publish_latest.py --push
   ```

### Renaming a hospital

1. Update `name` in `hospitals.csv`.
2. Update the `hospitals` mapping in `hospitals.json`.
3. Check `vahi_id` — if blank because names matched, set it to the old VAHI name.
4. Run `python3 scripts/fetch_vahi.py` and `python3 scripts/transform_silver.py`.

### Deactivating a hospital

Set `is_active=false` in `hospitals.csv`. Historical Silver data is preserved.

---

## Scraper Types

### `html_js` — Eastern Health
Extracts two JS objects embedded in the page `<script>` block. One HTTP fetch covers all three campuses. JS variable names and JSON field names are configured in `hospitals.json` under `js_data_vars` and `js_field_map`.

### `powerbi` — Monash Health
Power BI Embedded DSR batch API. One authenticated POST per campus. Per-campus per-cohort visual IDs configured in `hospitals.json → visual_ids`.

### `html_regex` — Royal Melbourne Hospital, RCH
Generic regex extraction from plain HTML. Patterns configured in `hospitals.json → regex_patterns`.

If `wait_time` matched → full Silver/Gold pipeline.
If only `busy_index` or `updated_time` → Bronze Raw row only → `raw_only` pipeline.

---

## Timestamp Provenance

| Field | When set | What it measures |
|---|---|---|
| `scrape_timestamp_utc` | When the Pi executed the scrape | Scrape Truth — always accurate |
| `reported_timestamp_str` | Value parsed from the source portal | Portal Truth — measures portal freshness |

### `cache_lag_minutes` and `fidelity_status`

Fidelity thresholds (configurable in `config/ui_config.json`):

| `fidelity_status` | Lag | Meaning |
|---|---|---|
| `SYNCED` | < 15 min | Portal recently refreshed |
| `API_LEAD_ACTIVE` | 15–60 min | Portal lagging behind scrape cadence |
| `PORTAL_STALE_WARNING` | > 60 min | Portal significantly stale |

---

## Bronze Layer

### `bronze_raw_scrapes.csv` — primary audit trail, all scrapers, all cohorts
### `melbourne_southeast.csv` — full-pipeline hospitals only, Adult/All, Silver input

---

## Silver Layer

`melbourne_southeast_silver.csv` — full rebuild each cycle from Bronze + reference benchmarks.

**3-tier context join:**
1. VAHI quarterly → `ctx_source = "VAHI"` (best)
2. AIHW annual fallback → `ctx_source = "AIHW"`
3. `ctx_defaults` → `ctx_source = "ESTIMATE"` (proxy for newly onboarded hospitals)

---

## Gold Layer

`latest.json` is built by `publish_latest.py` and deployed to Vercel each cycle.
`history_timeline.json` covers the last 3 hours of 15-min snapshots.
`forecast_audit.csv` (SSD) is the ML input — never filtered by UI window.

---

## Pi Pipeline (every 15 min via systemd)

```
run_monitor.sh
  │
  ├── 1. hospital_monitor.py       — scrape → Bronze CSV + Bronze Raw + sidecar
  │
  ├── 2. transform_silver.py       — Bronze → Silver (full rebuild)
  │
  └── 3. publish_latest.py --push
          a. Load latest Silver row per hospital
          b. Read Bronze Raw for Paeds + status_sites
          c. Compute 60-min outlook (predict_next.py)
          d. Apply UI_DISPLAY_WINDOW_MINS filter
          e. Write /tmp/hospital_monitor_latest.json
          f. Build 3h history timeline (get_history.py)
          g. Stage 5 files into /tmp/publisher:
             index.html, latest.json, history_timeline.json, schema.json, vercel.json
          h. Publish via method in config/ui_config.json → publish_method (see below)
```

Operational hours gate: steps a–h only run 06:00–23:00 Melbourne time. Outside those hours `publish_latest.py` exits 0 with no deploy.

---

## Publish Method

Controlled by **environment variable → config file → default**. No Python edits required.

**Priority order:**
1. `PUBLISH_METHOD` env var — one-off override, no file changes needed
2. `"publish_method"` in `config/ui_config.json` — permanent default
3. Falls back to `"vercel_api"` if neither is set

**Available methods:**

| Value | Behaviour | Vercel deploys/day |
|---|---|---|
| `vercel_api` | Direct Vercel API every cycle | ~68 (68% of quota) |
| `git_data_branch` | GitHub data branch every cycle | 0 (unlimited) |
| `dual` | GitHub every cycle + Vercel every `vercel_deploy_interval_mins` | ~24 at 60 min interval ✓ recommended |

**`vercel_deploy_interval_mins`** (default 60) — used only in `dual` mode. Last deploy time tracked in `/tmp/vercel_last_deploy.txt` (resets on reboot → immediate deploy then throttled).

**One-off override (no file edits):**
```bash
PUBLISH_METHOD=git_data_branch python3 scripts/publish_latest.py --push
```

**Permanent change:**
```json
"publish_method": "dual",
"vercel_deploy_interval_mins": 60
```

---

## Vercel Configuration

**Deploy method:** Direct API — `publish_latest.py` calls Vercel REST API, no git branch required.
**Production URL:** `https://hospital-monitor-zwleoapps-projects.vercel.app`
**Project ID:** `prj_bHVauKu3cdZTy5dovv1clnArXuq4`

**Credentials (`.env` in repo root — never committed):**

| Key | Purpose |
|---|---|
| `VERCEL_API_TOKEN` | Account-level token for `zwleoapps-projects` |
| `VERCEL_PROJECT_ID` | `prj_bHVauKu3cdZTy5dovv1clnArXuq4` |

**Files deployed each cycle (staged in `/tmp/publisher/`):**

| File | Description |
|---|---|
| `index.html` | UI — copied from `docs/index.html` on main |
| `latest.json` | Current 7-hospital outlook + RCH status_sites |
| `history_timeline.json` | Last 3h of 15-min snapshots |
| `schema.json` | Gold API schema |
| `vercel.json` | Cache-Control headers |

**Cache-Control:**

| File | Header |
|---|---|
| `/latest.json` | `no-cache, no-store, must-revalidate` |
| `/history_timeline.json` | `public, max-age=900` |

---

## Vercel Quota (Hobby Plan — confirmed 2026-05-04)

| Limit | Value | Scope |
|---|---|---|
| Deployments per day | **100** | **Account-wide** (all projects share this) |
| Deployments from CLI per week | 2,000 | Account-wide |
| Static file uploads | 100 MB | Per deployment |
| Concurrent builds | 1 | Account-wide |

**Current usage in `dual` mode:** ~24 deploys/day — uses 24% of quota, leaves 76 headroom.
**Warning threshold:** if daily count consistently reaches 90+, switch to `git_data_branch`.

**Check today's count:**
```bash
source .env
curl -s "https://api.vercel.com/v6/deployments?projectId=$VERCEL_PROJECT_ID&limit=100" \
  -H "Authorization: Bearer $VERCEL_API_TOKEN" | python3 -c "
import json,sys
from datetime import datetime, timezone
d=json.load(sys.stdin)
today = datetime.now(timezone.utc).date()
count = sum(1 for dep in d.get('deployments',[])
            if datetime.fromtimestamp(dep['createdAt']/1000, tz=timezone.utc).date() == today)
print(f'Deployments today: {count}/100')
"
```

---

## Switching to `git_data_branch` (step-by-step)

**Step 1 — Change config** (`config/ui_config.json`):
```json
"publish_method": "git_data_branch"
```

**Step 2 — Recreate the data branch on GitHub:**
```bash
git checkout --orphan data-new
git rm -rf --cached .
git clean -fdx
echo '{}' > latest.json
git add latest.json
git commit -m "data: init"
git push origin data-new:data
git checkout main
git branch -D data-new
```

**Step 3 — Verify SSH deploy key:**
```bash
ssh -T git@github.com   # should print: Hi zwleoapp! You've authenticated...
ls ~/.ssh/hospital_monitor_deploy
```

**Step 4 — In Vercel dashboard** for `hospital-monitor` project:
- Production Branch → `data`
- Root Directory → *(blank)*
- Build Command → *(blank)*

**Step 5 — Test:**
```bash
python3 scripts/publish_latest.py --push
# Should print: Force-pushed → data branch (...)
```

**Switching back to `vercel_api` or `dual`:**
```bash
# Edit config/ui_config.json → "publish_method": "dual"
# Verify .env has VERCEL_API_TOKEN and VERCEL_PROJECT_ID
python3 scripts/publish_latest.py --push
```

---

## Branch Responsibilities

| Branch | Purpose |
|---|---|
| `main` | Source code, config, docs — version history. Push to GitHub as usual. |
| `data` | Live data output — only exists when `publish_method` is `git_data_branch` or `dual`. Machine-written. |

## SSH Deploy Key

```
~/.ssh/hospital_monitor_deploy   (mode 600)
```

Routes ALL GitHub connections on this Pi (both main branch pushes and data branch pushes). Do not delete — required for `git push` to main.
