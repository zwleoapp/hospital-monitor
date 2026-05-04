# Dataflow — Hospital Monitor Pipeline

**Updated:** 2026-05-04

> Companion to [DESIGN.md](DESIGN.md). This page covers *how data moves* — scraper types, Bronze → Silver → Gold pipeline, timestamp provenance, branch structure, and Vercel configuration.

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

No Python changes needed. All steps are config edits + manual script runs.

#### Step 1 — Config: `config/hospitals.json`

Add a source entry with parser type, URL, and extraction patterns/credentials.

- For `html_js` sources: set `js_data_vars` and `js_field_map`.
- For `powerbi` sources: set `endpoint` and `visual_ids` (per-campus per-cohort).
- For `html_regex` sources: set `regex_patterns`. Add `status_map` if the portal uses categorical labels.
- Add `ctx_defaults` (temporary proxy VAHI benchmarks) if the hospital has no VAHI/AIHW data yet — remove after step 5 confirms VAHI coverage.

#### Step 2 — Config: `config/hospitals.csv`

Add one row:
```
name, network_type, scraper_type, vahi_id, aihw_id, is_active, pipeline
```
- `pipeline`: `full` (has numeric wait times → Silver + Gold) or `raw_only` (status/index only → `status_sites` card in UI, no forecast)
- Leave `vahi_id` and `aihw_id` blank for now — filled in steps 3 and 6.

#### Step 3 — Look up `vahi_id`

```bash
python3 scripts/fetch_vahi.py --list-orgs
```

Lists all 42 Victorian hospitals; `✓` marks those already mapped in `hospitals.csv`.

- If the VAHI name **matches** the formal name exactly → leave `vahi_id` blank.
- If it **differs** (e.g. `"The Royal Melbourne Hospital - City Campus"` vs `"Royal Melbourne Hospital"`) → set `vahi_id` in `hospitals.csv` to the exact VAHI name.
- If left blank when names differ, `fetch_vahi.py` silently drops the hospital. It prints a WARNING — treat that as a required fix.

#### Step 4 — Rebuild VAHI merged file

```bash
python3 scripts/fetch_vahi.py
```

- Automatically picks up the new hospital from `hospitals.csv`.
- Rebuilds `bronze/vahi_history_merged.csv`.
- **Auto-backs up all `bronze/*.csv` to SSD** (`/mnt/router_ssd/Data_Hub/bronze_backup/`) — no manual copy needed.
- Verify output: new hospital should appear in "Hospitals covered".

#### Step 5 — Rebuild Silver

```bash
python3 scripts/transform_silver.py
```

Check `ctx_source breakdown` in the output:
- `VAHI` → real quarterly benchmarks, ready to go. Remove `ctx_defaults` from `hospitals.json`.
- `ESTIMATE` → VAHI name mismatch or hospital not yet in VAHI file — fix `vahi_id` and re-run step 4.

#### Step 6 — Load AIHW historical data (optional, `full` pipeline only)

Provides historical annual benchmarks for pre-VAHI rows. Skip for `raw_only` hospitals.

```bash
# Verify the hospital's H-code resolves
python3 scripts/fetch_aihw.py --list-only

# Preview — fetch to a temp file and check row counts
python3 scripts/fetch_aihw.py --out bronze/check_aihw.csv

# Append to the main file once rows look correct
python3 scripts/fetch_aihw.py --append

# Backup the AIHW file to SSD (fetch_aihw.py does not auto-backup)
cp bronze/eastern_hospital_historical_context.csv /mnt/router_ssd/Data_Hub/bronze_backup/

# Set aihw_id in hospitals.csv to the H-code (e.g. H0330)
```

**H-code lookup:** See CLAUDE.md → "H-codes (verified 2026-04-29)". If the hospital is new, use `--list-only` to find its code.

#### Step 7 — Test scrape

```bash
python3 scripts/hospital_monitor.py
```

Check:
- New hospital appears in Bronze Raw CSV output.
- No `PARSE_ERROR` or `HTTP_ERROR` in `ingest_alerts.csv`:
  ```bash
  tail -20 /mnt/router_ssd/Data_Hub/Waiting_Live_time/ingest_alerts.csv
  ```

#### Step 8 — Test publish

```bash
python3 scripts/publish_latest.py --push
```

Check:
- New hospital appears in the `sites` array of the published `latest.json`.
- `ctx_source` is `"VAHI"` (not `"ESTIMATE"`) on the site object.
- UI card renders correctly on the dashboard.

#### Step 9 — Cleanup

- Remove `ctx_defaults` from `config/hospitals.json` if added in step 1 and VAHI is now confirmed.
- Commit the config changes to `main`:
  ```bash
  git add config/hospitals.json config/hospitals.csv
  git commit -m "feat: add <Hospital Name>"
  git push origin main
  ```

---

### Renaming a hospital (formal name change)

1. Update `name` in `hospitals.csv`.
2. Update the `hospitals` mapping in `hospitals.json` (parser key → formal name).
3. Check `vahi_id` — if it was blank because names matched, set it to the old VAHI name explicitly so the mapping survives the rename.
4. Update `vahi_benchmarks.per_hospital_p90_mins` key in `hospitals.json` to the new name.
5. Run `python3 scripts/fetch_vahi.py` then `python3 scripts/transform_silver.py`.

### Renaming/updating a VAHI organisation name

The VAHI portal occasionally renames hospitals. If `fetch_vahi.py` WARNING appears:

1. Re-download the affected raw VAHI CSV from the VAHI Data Portal into `bronze/`.
2. Update `vahi_id` in `hospitals.csv` to match the new VAHI name.
3. Run `python3 scripts/fetch_vahi.py`.

### Deactivating a hospital

Set `is_active=false` in `hospitals.csv`. The hospital is immediately removed from scraping and Gold output. Historical Silver data is preserved (Silver is rebuilt from Bronze, Bronze is append-only).

---

## Scraper Types

### `html_js` — Eastern Health
Extracts two JS objects embedded in the page `<script>` block:
```
const patientCounts        = { BoxHill: {waiting, beingTreated}, ... }
const predictedWaitMinutes = { BoxHill: {min, max}, ... }
```
One HTTP fetch covers all three campuses. JS variable names (`patientCounts`, `predictedWaitMinutes`) and JSON field names (`waiting`, `beingTreated`, `min`, `max`) are configured in `hospitals.json` under `js_data_vars` and `js_field_map`.

**Portal timestamp:** Native "Last Updated" extracted from page HTML; falls back to HTTP `Date` response header (`~HH:MM`, marked approximate). Always 24h format.

### `powerbi` — Monash Health
Power BI Embedded DSR batch API. One authenticated POST per campus, returning typed result columns. Per-campus per-cohort visual IDs (timestamp, waiting, treating, wait_str) configured in `hospitals.json → visual_ids`.

**Portal timestamp:** `LastUpdatedDisplay` field from the DSR response. Report-level (same value returned for all three campuses), reflects Power BI dataset refresh time — **not** underlying hospital system update time.

### `html_regex` — Royal Melbourne Hospital, RCH
Generic regex extraction from plain HTML. Patterns configured in `hospitals.json → regex_patterns`.

| Pattern key | RMH | RCH |
|---|---|---|
| `wait_time` | ✓ "00 hr 34 min - 01 hr 52 min" | — |
| `patients_waiting` | ✓ sibling-div pattern | — |
| `patients_treating` | ✓ sibling-div pattern | — |
| `updated_time` | ✓ "5:45pm on 03 May 2026" | ✓ timestamp only |
| `busy_index` | — | — (removed from RCH page 2026-05) |

If `wait_time` matched → Bronze CSV row written → full Silver/Gold pipeline.  
If only `busy_index` or `updated_time` → Bronze Raw row only → `raw_only` pipeline.

**Portal timestamp normalization:** RMH uses 12h am/pm format. The scraper normalizes "5:45pm" → "~17:45" (24h, approximate prefix) before passing to `_calculate_cache_lag` and writing to the sidecar. This ensures the UI displays "17:45 AEST" correctly.

---

## Timestamp Provenance

Every row captured by the scraper carries two timestamps with distinct meanings:

| Field | When set | What it measures |
|---|---|---|
| `scrape_timestamp_utc` | When the Pi executed the scrape | **Scrape Truth** — always accurate, set by our clock |
| `reported_timestamp_str` | Value parsed from the source portal | **Portal Truth** — measures how fresh the portal's own data is |

These are recorded separately so error analysis can distinguish "our scraper was slow" from "the hospital didn't update their portal".

### `cache_lag_minutes` and `fidelity_status`

`cache_lag = scrape_timestamp − reported_timestamp` (in minutes, Melbourne local).

| Source | What lag actually measures |
|---|---|
| `html_js` (Eastern Health) | Time since hospital last pushed new data to their public page. Direct measure of hospital publishing latency. |
| `powerbi` (Monash Health) | Time since Power BI last refreshed its embedded dataset. **Indirect** — reflects PBI caching layer, not hospital system freshness. Always non-zero (PBI refreshes on its own schedule). |
| `html_regex` (RMH) | Time since RMH last updated the "Wait time updated at" display on their page. Direct measure, same semantics as Eastern Health. |

Fidelity thresholds (configurable in `config/ui_config.json`):

| `fidelity_status` | Lag | Meaning |
|---|---|---|
| `SYNCED` | < 15 min | Portal recently refreshed |
| `API_LEAD_ACTIVE` | 15–60 min | Portal lagging behind our scrape cadence |
| `PORTAL_STALE_WARNING` | > 60 min | Portal significantly stale; forecast may use old data |

`cache_lag_minutes` is a **diagnostic column only** — it explains forecast errors in hindsight but is NOT an input to the forecast model.

### Timestamp → Sidecar → `last_updated_display`

All three scraper types write the normalized portal timestamp to a shared JSON sidecar (`monash_last_updated.json` on the SSD). `publish_latest.py` reads this sidecar into `last_updated_display` on each site's Gold payload. The UI card uses `parseHospDataTime(last_updated_display)` to show "⏰ 17:45 AEST" next to the wait time.

```
Scraper                          SSD sidecar                    Gold (latest.json)
─────────                        ───────────                    ──────────────────
html_js   ──► "~17:45"          ──► last_updated_sidecar.json  ──► last_updated_display
powerbi   ──► "^18:31"          ──►       (per site)           ──► (rendered by UI card)
html_regex ──► "~17:45" (norm'd) ──►
```

`^` prefix = report-level Power BI refresh time (not per-campus data time).  
`~` prefix = approximate time (from HTTP header or normalized am/pm time).

---

## Bronze Layer

### Reference files (`bronze/` — VAHI and AIHW benchmarks)

`bronze/` is in `.gitignore` — these files are **never committed to `main`**. They live on disk and are backed up to the SSD automatically.

| File | Source | Rebuilt by |
|---|---|---|
| `vahi_history_merged.csv` | VAHI Data Portal CSVs (manually downloaded) | `fetch_vahi.py` |
| `eastern_hospital_historical_context.csv` | AIHW API | `fetch_aihw.py --append` |

**SSD backup:** `fetch_vahi.py` automatically copies all `bronze/*.csv` to `/mnt/router_ssd/Data_Hub/bronze_backup/` after rebuilding the merged file. `fetch_aihw.py` requires a manual copy step (see `CLAUDE.md`).

**Restore if `bronze/` is wiped:**
```bash
cp /mnt/router_ssd/Data_Hub/bronze_backup/* /home/pi-zwapp/hospital-monitor/bronze/
python3 scripts/transform_silver.py
```

---

### `bronze_raw_scrapes.csv` (all scrapers — Adult + Paeds + raw_only)

The primary audit trail per scrape event. Every row represents one scraper call for one hospital/cohort.

| Column | Source | Notes |
|---|---|---|
| `site` | Hospital formal name | |
| `scrape_timestamp_utc` | Pi clock at scrape time | Scrape Truth |
| `location_timestamp` | Pi clock, Melbourne local | Display only |
| `reported_timestamp_str` | Portal "Last Updated" raw text | Portal Truth |
| `reported_waiting` | Extracted patient count | As shown on portal |
| `reported_wait_str` | Extracted wait range string | "00 hr 34 min - 01 hr 52 min" |
| `raw_query_waiting` | API/scrape waiting count | May differ from reported |
| `raw_query_treating` | API/scrape treating count | |
| `raw_query_max_wait` | API/scrape max wait | |
| `cohort` | "Adult" / "Paeds" / "All" | "All" = no split (Eastern, RMH) |
| `cache_lag_minutes` | Computed from above two timestamps | Diagnostic only |
| `fidelity_status` | SYNCED / API_LEAD_ACTIVE / PORTAL_STALE_WARNING / UNKNOWN_FORMAT | |

### `melbourne_southeast.csv` (full-pipeline hospitals only — Adult/All)

Append-only Bronze CSV used as input to Silver. Only `full` pipeline hospitals write here.

| Column | Notes |
|---|---|
| `timestamp` | Scrape UTC |
| `hospital` | Formal name |
| `waiting`, `treating` | Patient counts |
| `wait_time` | Range string (e.g. "00 hr 34 min - 01 hr 52 min") |
| `min_wait_mins`, `max_wait_mins` | Parsed minutes |
| `location_timestamp` | Melbourne local time |

Paediatric cohort (Monash only) goes to `bronze_raw_scrapes.csv` only — not to this file.

---

## Silver Layer

`melbourne_southeast_silver.csv` — full rebuild from Bronze on every pipeline cycle.

Silver enriches Bronze with three tiers of contextual benchmarks (LEFT join — every Bronze row is preserved):

1. **VAHI quarterly** — `ctx_source = "VAHI"` — quarterly wait benchmarks (Oct 2024–, extended with VAHI_PROXY rows)
2. **AIHW annual fallback** — `ctx_source = "AIHW"` — for Bronze rows before VAHI coverage
3. **ctx_defaults** — `ctx_source = "ESTIMATE"` — proxy values from `config/hospitals.json` for newly onboarded hospitals without VAHI/AIHW data yet (e.g. Royal Melbourne Hospital)

Additional Silver columns: `wait_momentum` (change per 15-min cadence), `load_ratio`, `hour`, `day_of_week`, `is_weekend`, `is_holiday`, `day_type`, `season`.

`wait_momentum` is NaN for the **first-ever row** per hospital (no prior row to diff against). All downstream consumers use `pd.notna` guards on this field.

---

## Gold Layer

### `latest.json` schema

```json
{
  "generated_utc": "...",
  "horizon_min": 60,
  "vahi_p90_all_mins": 89,
  "vahi_qly_label": "Q2 2026",
  "sites": [
    {
      "site":                    "Royal Melbourne Hospital",
      "network":                 "Melbourne Health",
      "latest_obs_utc":          "...",
      "waiting_count":           10,
      "treating_count":          34,
      "current_wait_min":        34.0,
      "max_wait_min":            112,
      "predicted_wait_min":      32.0,
      "wait_momentum":           -1.0,
      "confidence":              0.81,
      "confidence_label":        "High",
      "color":                   "amber",
      "heartbeat_age_mins":      0.4,
      "strain_index":            0.36,
      "last_updated_display":    "~17:45",
      "scraper_sync_mins":       0,
      "ctx_source":              "ESTIMATE",
      "vahi_p90_mins":           90,
      "vahi_median_cat123_mins": 30,
      "vahi_median_cat45_mins":  60,
      "paediatric": {
        "waiting": 3, "treating": 9,
        "wait_str": "0 hr 46 min - 0 hr 56 min",
        "heartbeat_age_mins": 0.4
      },
      "metadata": {
        "cache_lag_minutes":  15,
        "fidelity_status":    "API_LEAD_ACTIVE",
        "is_stale":           false,
        "last_portal_update": "17:45",
        "scrape_timestamp":   "2026-05-03T08:00:03Z"
      }
    }
  ],
  "status_sites": [
    {
      "site":                  "Royal Childrens Hospital",
      "scrape_timestamp_utc":  "...",
      "heartbeat_age_mins":    0.4,
      "reported_wait_str":     "",
      "busy_index":            null,
      "fidelity_status":       "API_LEAD_ACTIVE",
      "last_portal_update":    "03 May 2026 17:03:06 +AEST"
    }
  ]
}
```

`paediatric` sub-object is present only for Monash campuses (Casey, Clayton) — Dandenong has no Paeds ward. RCH (paediatric specialist hospital) is in `status_sites`, not `sites`.

`ctx_source` on each site:
- `"VAHI"` — real VAHI quarterly benchmarks (best quality)
- `"AIHW"` — AIHW annual data (pre-VAHI coverage)
- `"ESTIMATE"` — proxy from `ctx_defaults` (Royal Melbourne Hospital, pending VAHI onboarding)

### `history_timeline.json` schema

Last 3 hours of 15-min snapshots (window configurable via `UI_DISPLAY_WINDOW_MINS` in `config/ui_config.json`). Used by the UI time-navigation arrows.

```json
{
  "generated_utc": "...",
  "history_hours": 3.0,
  "snapshots": [
    {
      "bucket_utc": "2026-05-03T05:00:00Z",
      "sites": [{ "site": "...", "current_wait_min": 45.0, ... }]
    }
  ]
}
```

Full historical accuracy data lives in `forecast_audit.csv` (SSD, never filtered by UI window).

### `forecast_audit.csv` (SSD, ML input)

Written by `get_history.py` whenever a T+60 observation is available to compare against a prior forecast. Powers `evolve_model.py`'s per-hospital per-segment damping optimization.

| Key columns | Notes |
|---|---|
| `bucket_utc` | Forecast time |
| `hospital`, `cohort`, `source_type` | Identifiers |
| `day_type`, `time_band` | Segmentation for ML (weekday/weekend/public_holiday × overnight/morning/afternoon/evening) |
| `current_wait_min`, `wait_momentum`, `treating_count` | Model inputs |
| `actual_wait_min`, `predicted_wait_min`, `error_pct` | Accuracy |
| `cache_lag_minutes`, `fidelity_status` | Diagnostic — correlate errors with data staleness |

---

## Pi Pipeline (every 15 min via systemd)

```
run_monitor.sh
  │
  ├── 1. hospital_monitor.py
  │       Dispatches to scrapers/ package by parser type:
  │         html_js    → scrapers/eastern.py
  │         powerbi    → scrapers/monash.py
  │         html_regex → scrapers/html_regex.py
  │       Writes:
  │         • Bronze CSV (full-pipeline hospitals, Adult/All cohort)
  │         • Bronze Raw CSV (all hospitals, all cohorts, includes Paeds + raw_only)
  │         • Sidecar JSON (portal timestamps, read by publish_latest.py)
  │         • ingest_alerts.csv (data quality issues: PARSE_ERROR, HTTP_ERROR, NULL_ALL_MEASURES)
  │
  ├── 2. transform_silver.py
  │       Full rebuild of Silver CSV from Bronze + VAHI/AIHW/ctx_defaults reference files
  │       Adds: temporal features, wait_momentum, load_ratio, ctx benchmarks
  │       Deduplicates consecutive no-change rows
  │
  └── 3. publish_latest.py --push
          a. Load latest Silver row per hospital
          b. Read Bronze Raw for Paeds data (Monash campuses) and status_sites (raw_only)
          c. Compute 60-min outlook via predict_next.py (effective damping from model_config.json)
          d. Apply UI_DISPLAY_WINDOW_MINS filter (default 180 min)
          e. Write /tmp/hospital_monitor_latest.json
          f. Build 3h history timeline via get_history.py
             → write /tmp/history_timeline.json
             → append completed forecasts to forecast_audit.csv
          g. Check git_push_interval_mins (default 1 min — effectively every cycle)
          h. Clone/fetch data branch into /tmp/publisher
          i. Strip data branch clean (git rm --cached + git clean)
          j. Copy 4 files: index.html, latest.json, history_timeline.json, vercel.json
          k. Commit "data: outlook <UTC stamp>"
             → if JSON unchanged: "nothing to commit" — no push, no Vercel deploy
          l. Force-push → origin/data → Vercel auto-deploys via GitHub integration
```

**Timing notes:**
- The systemd timer fires every 15 min. Steps 1–3 take ~2–4 min (Monash PowerBI is the variable leg).
- `git_push_interval_mins` is set to 1 (not 15) intentionally. If it matched the 15-min timer period, a slow previous cycle (pushes at T+4) followed by a fast current cycle (checks at T+15+1) could read an elapsed of only ~12 min and skip the push. The real guard against redundant pushes is git's own `nothing to commit` exit at step k — if `latest.json` hasn't changed, no push and no Vercel rebuild occur.

Operational hours gate: steps a–l only run 06:00–23:00 Melbourne time (configurable in `config/ui_config.json`). Outside those hours `publish_latest.py` exits 0 with no push.

---

## Branch Responsibilities

| Branch | Purpose | What lives here |
|---|---|---|
| `main` | Source code | All Python scripts, `docs/index.html`, config, CLAUDE.md, docs/ |
| `data` | Live data output | **Exactly 4 files** (see below) |

```
data/
  index.html            ← copied from docs/index.html on main at publish time
  latest.json           ← current 7-hospital outlook + RCH status_sites
  history_timeline.json ← last 3h of 15-min snapshots
  vercel.json           ← Vercel cache-control headers
```

The `data` branch is machine-written on every publish cycle. Never commit source code to `data` manually.

## SSH Deploy Key

```
~/.ssh/hospital_monitor_deploy   (mode 600)
```

Routes **all** GitHub connections on this Pi — both `main` branch pushes and `data` branch force-pushes. Do not delete or replace without updating `~/.ssh/config`.

---

## Vercel Configuration

**Production branch:** `data`  
Vercel serves `index.html` from the data branch root. It never reads `main`.

**Deploy method:** `git_data_branch` (configured in `config/ui_config.json → publish_method`).  
Every force-push to `origin/data` triggers a Vercel rebuild automatically via the GitHub integration. No explicit Vercel API calls are made. This avoids double-deploys (API + GitHub integration) and removes the 60-min Vercel API cadence as a bottleneck.

**Cache-Control headers (set in `vercel.json`, values from `config/ui_config.json`):**

| File | Config key | Header | Effect |
|---|---|---|---|
| `/latest.json` | `CACHE_LATEST_JSON` | `no-cache, no-store, must-revalidate` | Browser always fetches fresh on every dashboard poll |
| `/history_timeline.json` | `CACHE_HISTORY_JSON` | `public, max-age=900` | 15-min browser cache; stable between pushes |

`vercel.json` is generated by `publish_latest.py` at publish time from these config values — not hand-edited.

**Vercel Settings:**

| Setting | Value |
|---|---|
| Production Branch | `data` |
| Ignored Build Step | *(leave blank — Vercel must build on every push to serve fresh JSON)* |
| Root Directory | *(leave blank — files are at the repo root on the `data` branch)* |

---

## SSH Deploy Key

```
~/.ssh/hospital_monitor_deploy   (mode 600)
```

Write access to the `data` branch only. Cannot push to `main`. `publish_latest.py` uses the system SSH config; no `GIT_SSH_COMMAND` override is needed if `~/.ssh/config` routes `github.com` to this key.
