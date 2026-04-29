# Hospital Monitor — Claude Context

## Project overview
Raspberry Pi scrapes Eastern Health and Monash Health ED dashboards every 15 min.
Data flows Bronze (raw CSV on Pi SSD) → Silver (enriched, rebuilt each cycle) → Gold (`latest.json` published to GitHub Pages).
Bronze stays local and private. Only Gold is published. ML baseline in progress.

## Repo layout
```
scripts/            Pi-side pipeline scripts
bronze/             VAHI + AIHW reference files (context for Silver enrichment)
docs/               GitHub Pages site (index.html fetches latest.json from data branch)
systemd/            Timer + service units
run_monitor.sh      Full pipeline: scrape → silver → publish (called by systemd)
```

## Networks and hospitals
| Network | Hospitals |
|---|---|
| Eastern Health | Box Hill Hospital, Angliss Hospital, Maroondah Hospital |
| Monash Health | Casey Hospital, Dandenong Hospital, Monash Medical Centre - Clayton |

Diversion logic is scoped within-network only (`publish_latest.py::annotate_diversion`).

## ED Data Pipeline Workflow

### Reference data files (bronze/)
- `vahi_history_merged.csv` — quarterly VAHI benchmarks for all 6 hospitals (Oct 2024–). Rebuilt by `scripts/fetch_vahi.py` from the 6 raw VAHI source CSVs in `bronze/`. 2026 proxy quarters (VAHI_PROXY) are forward-filled from the last real quarter.
- `eastern_hospital_historical_context.csv` — AIHW annual baseline (backfill for pre-Oct-2024 rows only). All current Bronze data falls within VAHI coverage so this file is **optional** — `transform_silver.py` skips it gracefully if absent or malformed.

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
Current file: **2,688 rows**, all 6 hospitals, 2011–2025, all triage categories.

**H-codes (verified 2026-04-29 against live API):**
| Hospital | Code | API name |
|---|---|---|
| Box Hill Hospital | H0330 | Box Hill Hospital |
| Maroondah Hospital | H0332 | Maroondah Hospital [East Ringwood] |
| Angliss Hospital | H0333 | Angliss Hospital |
| Casey Hospital | **H0353** | Casey Hospital |
| Dandenong Hospital | **H0348** | Dandenong Hospital |
| Monash Medical Centre - Clayton | **H0331** | Monash Medical Centre [Clayton] |

Previous codes H0326/H0329/H0345 were wrong (pointed to unrelated regional hospitals).

**API (updated 2026-04-29):**
- Base: `https://myhospitalsapi.aihw.gov.au/api/v1` — migrated from defunct `myhospitals.gov.au`
- Endpoint: `GET /reporting-units/{code}/data-items` (bulk dump, filtered locally by measure code)
- Period/triage: `GET /datasets/{dataset_id}` per unique dataset ID (cached across hospitals)
- Swagger: `https://myhospitalsapi.aihw.gov.au/index.html`
- Old `myhospitals.gov.au` is DNS-dead; new domain resolves from both Pi and laptop

### Silver Transform
Run after any change to Bronze or VAHI/AIHW reference files:
```bash
python3 scripts/transform_silver.py
```
Silver is a full rebuild each run — never appended. Safe to run repeatedly.

## Common Commands

| Task | Command |
|---|---|
| Full pipeline (manual) | `bash run_monitor.sh` |
| Scrape only | `python3 scripts/hospital_monitor.py` |
| Rebuild Silver | `python3 scripts/transform_silver.py` |
| Publish + push to data branch | `python3 scripts/publish_latest.py --push` |
| Rebuild VAHI merged file | `python3 scripts/process_vahi_history.py` |
| Ingest new Monash AIHW data | `python3 scripts/fetch_aihw.py --append` |
| Check Silver output | `head -2 /mnt/router_ssd/Data_Hub/Waiting_Live_time/eastern_hospital_silver.csv` |

## Operational hours gate
`publish_latest.py` enforces 07:00–23:00 Melbourne time. Outside those hours it logs `Trial Mode: Sleeping` and exits 0 — systemd timer fires unconditionally, gate is inside the script.

## Network notes
- Pi has egress-only internet. SSH deploy key scoped to this repo at `~/.ssh/hospital_monitor_deploy`.
- `myhospitals.gov.au` (old domain) does not resolve from the Pi — but `myhospitalsapi.aihw.gov.au` (new API domain) does. `fetch_aihw.py` can now run directly on the Pi.
- `raw.githubusercontent.com` (data branch) is reachable from browsers but has ~5 min CDN cache. Dashboard fetches `/latest.json` from Vercel (no-cache) instead.

## Key constants (adjust at top of each script)
| Constant | File | Purpose |
|---|---|---|
| `OPERATIONAL_START_H / END_H` | `publish_latest.py` | Operational hours window |
| `DIVERSION_STRAIN_DELTA` | `publish_latest.py` | Strain gap threshold for diversion flag |
| `STALE_MINS` | `docs/index.html` | Age threshold for stale-data banner |
| `MOMENTUM_DAMPING` | `predict_next.py` | Dampens trend extrapolation |
