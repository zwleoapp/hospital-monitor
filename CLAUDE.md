# Hospital Monitor — Claude Context

## Project overview
Raspberry Pi scrapes Eastern Health, Monash Health, and Royal Melbourne Hospital ED dashboards every 15 min, plus Royal Children's Hospital status page.
Data flows Bronze (raw CSV on Pi SSD) → Silver (enriched, rebuilt each cycle) → Gold (`latest.json` published to Vercel via data branch).
Bronze stays local and private. Only Gold is published. ML feedback loop active (`evolve_model.py`).

## Repo layout
```
scripts/            Pi-side pipeline scripts
scripts/scrapers/   Parser modules (eastern.py, monash.py, html_regex.py, base.py)
bronze/             VAHI + AIHW reference files (context for Silver enrichment)
config/             hospitals.json, hospitals.csv, hospitals.py, paths.py, model_config.json, ui_config.json
docs/               GitHub Pages site (index.html fetches latest.json from data branch)
systemd/            Timer + service units
run_monitor.sh      Full pipeline: scrape → silver → publish (called by systemd)
```

## Networks and hospitals

| Network | Hospitals | Parser | Pipeline |
|---|---|---|---|
| Eastern Health | Box Hill Hospital, Angliss Hospital, Maroondah Hospital | `html_js` | full |
| Monash Health | Casey Hospital, Dandenong Hospital, Monash Medical Centre - Clayton | `powerbi` | full |
| Melbourne Health | Royal Melbourne Hospital | `html_regex` | full |
| Royal Childrens Hospital | Royal Childrens Hospital | `html_regex` | raw_only |

**Parser types** (all config-driven via `hospitals.json` — no hospital names hardcoded in scripts):
- `html_js` — Eastern Health: extracts JS-embedded JSON objects (`patientCounts`, `predictedWaitMinutes`). JS variable names in `js_data_vars`; field names in `js_field_map`.
- `powerbi` — Monash Health: Power BI DSR batch API with per-campus, per-cohort visual IDs.
- `html_regex` — Plain HTML regex extraction. If `wait_time` pattern present → full Bronze CSV + Silver pipeline. If only `busy_index` pattern → `raw_only` (Bronze Raw only, shown as status card in UI).

**`raw_only` pipeline:** hospital writes to `bronze_raw_scrapes.csv` only (no Bronze CSV → no Silver → no forecast). Appears in UI as a status card via `status_sites` payload field. Controlled by `pipeline=raw_only` in `hospitals.csv`.

## Adding a new hospital (no Python changes needed)
1. Add source entry to `config/hospitals.json` with parser type, URL, and patterns/credentials
2. Add row to `config/hospitals.csv` (name, network_type, scraper_type, aihw_id, is_active, pipeline)
3. Set `vahi_id` to the exact "Organisation Description" string from the bronze VAHI source CSVs.
   If it matches the formal name exactly, leave blank. If it differs (e.g. RMH: "The Royal Melbourne
   Hospital - City Campus" ≠ "Royal Melbourne Hospital"), populate it — otherwise `fetch_vahi.py`
   silently drops the hospital. Missing vahi_id now prints a WARNING with a hint.
4. `python3 scripts/fetch_vahi.py` — automatically picks up the new hospital and rebuilds
   `bronze/vahi_history_merged.csv`. No code changes needed.
5. `python3 scripts/transform_silver.py` — Silver uses real VAHI context (ctx_source="VAHI")

## ED Data Pipeline Workflow

### Reference data files (bronze/)
- `vahi_history_merged.csv` — quarterly VAHI benchmarks for all 6 original hospitals (Oct 2024–). Rebuilt by `scripts/fetch_vahi.py` from the 6 raw VAHI source CSVs in `bronze/`. 2026 proxy quarters (VAHI_PROXY) are forward-filled from the last real quarter.
- `eastern_hospital_historical_context.csv` — AIHW annual baseline (backfill for pre-Oct-2024 rows only). All current Bronze data falls within VAHI coverage so this file is **optional** — `transform_silver.py` skips it gracefully if absent or malformed.

**Silver context resolution (3-tier LEFT join):**
1. VAHI quarterly match (best) — `ctx_source = "VAHI"`
2. AIHW annual fallback — `ctx_source = "AIHW"`
3. `ctx_defaults` in `hospitals.json` (proxy for newly onboarded hospitals) — `ctx_source = "ESTIMATE"`

**Bronze backup:** All bronze files are mirrored to the SSD at `/mnt/router_ssd/Data_Hub/bronze_backup/`. If `bronze/` is ever wiped (git clean, etc.), restore with:
```bash
cp /mnt/router_ssd/Data_Hub/bronze_backup/* /home/pi-zwapp/hospital-monitor/bronze/
python3 scripts/transform_silver.py
```
Refresh the SSD backup after any VAHI update: `cp bronze/* /mnt/router_ssd/Data_Hub/bronze_backup/`

### Fetch Script (can run from Pi or laptop — new API domain resolves from Pi)
```bash
# Step 1 — verify H-codes still resolve
python3 scripts/fetch_aihw.py --list-only

# Step 2 — fetch to a temp file for review
python3 scripts/fetch_aihw.py --out bronze/check_aihw.csv

# Step 3 — merge into main file once row counts look sane
python3 scripts/fetch_aihw.py --append

# After --append, refresh the SSD backup:
cp bronze/eastern_hospital_historical_context.csv /mnt/router_ssd/Data_Hub/bronze_backup/
```

`--append` handles a wrong-schema existing file (starts fresh with a note).
Deduplicates on `(hospital, period_start, measure_code, triage_category)` so re-running is safe.
Current file: **2,688 rows**, all 6 original hospitals, 2011–2025, all triage categories.

**H-codes (verified 2026-04-29 against live API):**
| Hospital | Code | API name |
|---|---|---|
| Box Hill Hospital | H0330 | Box Hill Hospital |
| Maroondah Hospital | H0332 | Maroondah Hospital [East Ringwood] |
| Angliss Hospital | H0333 | Angliss Hospital |
| Casey Hospital | **H0353** | Casey Hospital |
| Dandenong Hospital | **H0348** | Dandenong Hospital |
| Monash Medical Centre - Clayton | **H0331** | Monash Medical Centre [Clayton] |
| Royal Melbourne Hospital | H0081 | Royal Melbourne Hospital (AIHW not yet loaded) |

Previous codes H0326/H0329/H0345 were wrong (pointed to unrelated regional hospitals).

**API (updated 2026-04-29):**
- Base: `https://myhospitalsapi.aihw.gov.au/api/v1` — migrated from defunct `myhospitals.gov.au`
- Endpoint: `GET /reporting-units/{code}/data-items` (bulk dump, filtered locally by measure code)
- Swagger: `https://myhospitalsapi.aihw.gov.au/index.html`

### Silver Transform
Run after any change to Bronze or VAHI/AIHW reference files:
```bash
python3 scripts/transform_silver.py
```
Silver is a full rebuild each run — never appended. Safe to run repeatedly.

### ML Feedback Loop
`evolve_model.py` reads `forecast_audit.csv`, computes per-hospital per-segment optimal damping, and writes to `config/model_config.json` under `per_hospital_damping`. Segmented by `day_type` × `time_band` (12 segments). Run weekly:
```bash
python3 scripts/evolve_model.py --audit  # per-segment breakdown (dry-run)
python3 scripts/evolve_model.py          # write evolved damping to model_config.json
```
Minimum 24 rows per hospital before evolution runs. First meaningful run expected ~7 days after initial scraping of each hospital.

## Common Commands

| Task | Command |
|---|---|
| Full pipeline (manual) | `bash run_monitor.sh` |
| Scrape only | `python3 scripts/hospital_monitor.py` |
| Rebuild Silver | `python3 scripts/transform_silver.py` |
| Publish + push to data branch | `python3 scripts/publish_latest.py --push` |
| Rebuild VAHI merged file | `python3 scripts/process_vahi_history.py` |
| Ingest new hospital AIHW data | `python3 scripts/fetch_aihw.py --append` |
| Check Silver output | `head -2 /mnt/router_ssd/Data_Hub/Waiting_Live_time/melbourne_southeast_silver.csv` |
| Audit ML damping | `python3 scripts/evolve_model.py --audit` |
| Evolve ML damping (write) | `python3 scripts/evolve_model.py` |
| Check ingest alerts | `tail -20 /mnt/router_ssd/Data_Hub/Waiting_Live_time/ingest_alerts.csv` |

## Operational hours gate
`publish_latest.py` enforces 06:00–23:00 Melbourne time (read from `config/ui_config.json`). Outside those hours it logs `Trial Mode: Sleeping` and exits 0 — systemd timer fires unconditionally, gate is inside the script.

## Config files — what lives where
| File | Contents |
|---|---|
| `config/hospitals.json` | Scraper URLs, credentials, regex patterns, Power BI IDs, `ctx_defaults`, `vahi_benchmarks` |
| `config/hospitals.csv` | Per-hospital registry: name, network, scraper_type, aihw_id, is_active, pipeline |
| `config/model_config.json` | ML constants (damping bounds, horizon, cadence) + evolved `per_hospital_damping` |
| `config/ui_config.json` | UI constants: display window, operational hours, fidelity thresholds |
| `config/paths.py` | Single source of truth for all SSD paths |
| `config/overrides.json` | Manual damping overrides (takes priority over evolved values) |

## Key constants (adjust in config files, not scripts)
| Constant | File | Purpose |
|---|---|---|
| `UI_DISPLAY_WINDOW_MINS` | `config/ui_config.json` | History window shown in UI (default 180 min) |
| `OPERATIONAL_START_H / END_H` | `config/ui_config.json` | Operational hours window |
| `momentum_damping` | `config/model_config.json` | Global damping default (overridden by per_hospital_damping) |
| `ctx_defaults` | `config/hospitals.json` | Proxy VAHI benchmarks for hospitals not yet in VAHI file |
| `NETWORK_ORDER` | `docs/index.html` | Display order of hospital networks in UI |
| `HOSPITAL_STALE_MINS` | `docs/index.html` | Age threshold for stale-data banner |

## Network notes
- Pi has egress-only internet. SSH deploy key scoped to this repo at `~/.ssh/hospital_monitor_deploy`.
- `myhospitalsapi.aihw.gov.au` resolves from the Pi (new domain since 2026-04).
- Dashboard fetches `/latest.json` from Vercel (no-cache header) — browsers get fresh data within seconds, not the 5-min CDN lag of raw GitHub URLs.
