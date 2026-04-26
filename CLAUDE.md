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
- `vahi_history_merged.csv` — quarterly VAHI benchmarks for all 6 hospitals (Oct 2024–). Rebuilt by `scripts/process_vahi_history.py`. 2026 proxy quarters (VAHI_PROXY) are forward-filled from the last real quarter.
- `eastern_hospital_historical_context.csv` — annual AIHW baseline 2011–2025. Currently covers Eastern Health only; Monash backfill pending via `fetch_aihw.py`.

### Fetch Script (run from laptop — Pi cannot reach myhospitals.gov.au)
```bash
# Step 1 — verify H-codes resolve correctly before fetching any data
python3 scripts/fetch_aihw.py --list-only

# Step 2 — fetch to a separate file for review
python3 scripts/fetch_aihw.py --out bronze/monash_aihw_context.csv

# Step 3 — merge into main file once row counts look sane
python3 scripts/fetch_aihw.py --append
```

**H-codes to verify (run --list-only first):**
| Hospital | Code in script | Status |
|---|---|---|
| Box Hill Hospital | H0330 | Confirmed from existing file |
| Maroondah Hospital | H0332 | Confirmed from existing file |
| Angliss Hospital | H0333 | Confirmed from existing file |
| Monash Medical Centre - Clayton | H0326 | Unverified — check with --list-only |
| Dandenong Hospital | H0329 | Unverified — check with --list-only |
| Casey Hospital | H0345 | Unverified — check with --list-only |

**Validation**: `--append` deduplicates on `(hospital, period_start, measure_code, triage_category)` so re-running is safe.

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
- `myhospitals.gov.au` does not resolve from the Pi (DNS/firewall). Run `fetch_aihw.py` from a laptop.
- `raw.githubusercontent.com` (data branch) is reachable from browsers but has ~5 min CDN cache.

## Key constants (adjust at top of each script)
| Constant | File | Purpose |
|---|---|---|
| `OPERATIONAL_START_H / END_H` | `publish_latest.py` | Operational hours window |
| `DIVERSION_STRAIN_DELTA` | `publish_latest.py` | Strain gap threshold for diversion flag |
| `STALE_MINS` | `docs/index.html` | Age threshold for stale-data banner |
| `MOMENTUM_DAMPING` | `predict_next.py` | Dampens trend extrapolation |
