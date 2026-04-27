# DESIGN — Predictive ED Wait Time Engine

**Version:** 1.0 · **Updated:** 2026-04-25 · **Owner:** G

> This document is the **Single Source of Truth (SSOT)**. Every architectural change updates this doc *before* the code is merged. Both Gemini (strategy) and Claude (DS co-design / tactical execution) read it on every session.

---

## 1. Mission

Build a small, low-maintenance pipeline that turns Eastern Health's public ED dashboard into a clean, ML-ready dataset, and — when ready — serves predicted wait times back to the public via a tiny static page. The four focus areas, in priority order:

1. **Data safety** — public-aggregate only, append-only Bronze, schema-validated, no PII.
2. **Accurate ingest & transform** — every layer is reproducible from the previous one.
3. **ML lifecycle** — versioned datasets, frozen holdouts, monitored drift.
4. **Output for use** — a static, free-to-host status / map page.

---

## 2. Roles (Rules of Engagement)

| Role | Who | Scope |
|---|---|---|
| Strategy & DS theory | Gemini | Architecture, feature theory, model choice, results analysis |
| DS co-designer | Claude (chat) | Design review, ML patterns, doc maintenance, second-opinion |
| Tactical execution | Claude Code (terminal) | Writes scripts, runs migrations, handles PySpark/Delta syntax |
| Product owner | G | Final decisions on scope and architecture |

**Soft rule:** Either AI can propose architectural changes; no code merges before this doc reflects the change.

---

## 3. Phased plan

The project moves through two phases. Phase 1 is the current operating mode and stays viable indefinitely at $0. Phase 2 is unlocked by need, not by date.

### Phase 1 — Edge-only (current)

Everything runs on the Raspberry Pi. No cloud spend.

```
[Eastern Health dashboard]   [Open-Meteo API]
        │ curl_cffi                 │ requests
        ▼ (30 min, systemd)         ▼ (hourly, systemd)
[Raspberry Pi]
   ├── Bronze: eastern_hospital.csv         (append-only, on /mnt/router_ssd/)
   ├── Bronze: weather.csv                  (append-only, on /mnt/router_ssd/)
   ├── Silver: eastern_hospital_cleaned.csv (rebuilt each cycle from Bronze)
   ├── Model:  /models/v<n>.pkl             (trained locally, weekly)
   └── Export: /tmp/latest.json
            │
            ▼  (force-push to `data` branch)
   [GitHub repo]  ──►  [GitHub Pages]  static map page reads `latest.json`
```

**Phase 1 deliverables:**

- Bronze ingester (existing): `hospital_monitor.py`
- Silver transformer (existing): `transform_split_1.py`
- Weather ingester (new, small): `weather_monitor.py` — pulls Open-Meteo on the Pi.
- Publisher (new, small): `publish_latest.py` — writes `latest.json`, force-pushes to `data` branch via SSH key.
- Trainer (new, small): `train_local.py` — pickles a model weekly into `/models/v<n>.pkl`.
- Static page: `docs/index.html` — vanilla JS, fetches `latest.json`, renders three pins.

### Phase 2 — Cloud uplift (when justified)

Triggered when *any* of these are true:

- Bronze exceeds ~200k rows or ~6 months of history (Pi-side training gets slow).
- Time-series cross-validation needs more compute than the Pi can give.
- A grant or modest paid tier becomes available.

**What changes:**

- Pi gains a **second push** target: incremental Bronze rows → Databricks Unity Catalog Volume (one-way, PAT-auth).
- Silver and Gold are **recomputed in Databricks** from Bronze. Pi-side Silver becomes redundant for ML purposes (kept for the publisher).
- ML training moves to Databricks notebooks; model registry replaces local pickles.
- Publisher's `latest.json` source flips from Pi-side Silver to a Databricks export job.

**What does *not* change:**

- The Pi remains the **sole sensor**. Databricks Free Edition restricts outbound internet, so the Pi keeps owning all external API calls.
- The static page and its URL stay identical — only the data source behind `latest.json` changes.
- Bronze is still append-only.
- All security boundaries (Pi → cloud is push-only, no inbound) are preserved.

---

## 4. Data safety posture

| Concern | Phase 1 control | Phase 2 addition |
|---|---|---|
| PII | Top-of-file `# data-class: public-aggregate` stamp; CI grep gate fails merge if removed | Same |
| Schema drift | JSON validated against `/config/bronze_schema_v*.json` before Bronze append | Same check applied at Volume → Delta hand-off |
| Tampering | Bronze append-only; nightly tarball of Bronze to a second medium | Add: Delta time-travel + hash manifest of training snapshots |
| Network surface | Pi has egress only; no inbound. SSH keys for git push stored at `~/.ssh/` (mode 600) | Add: Databricks PAT in `~/.databrickscfg` (mode 600); rotation procedure documented in this file when token created |
| Data loss | Bronze on SSD + nightly tarball | Add: Delta in cloud is the durable copy |

**Token / credential rules:**

- Never commit a token. `.gitignore` covers `*.cfg`, `*.token`, `.env`.
- SSH keys for the publisher use a **deploy key** scoped to this repo only.
- Databricks PAT (Phase 2) gets minimum scope and a documented rotation date.

---

## 5. ML lifecycle

Phase 1 is deliberately small, but the lifecycle shape is set now so Phase 2 is a swap-out, not a rewrite.

| Stage | Phase 1 (Pi) | Phase 2 (Databricks) |
|---|---|---|
| Training data | Silver CSV, last N days | Gold Delta snapshot (content-hashed) |
| Holdout | Last 14 days frozen, never trained on | Same; enforced in code |
| Model store | `/models/v<n>.pkl` + `manifest.csv` | MLflow model registry |
| Cadence | Weekly cron on Pi | Weekly Databricks job |
| Evaluation | MAE on holdout; logged to `models/eval_log.csv` | Same; logged to MLflow runs |
| Drift monitor | Population Stability Index (PSI) on `load_ratio` and `temp_c` between latest week and training window; flag if PSI > 0.2 | Same; alerts via Databricks job email |
| Promotion | New model only replaces champion if MAE on holdout is ≥ 5% better | Same, gated in MLflow |

**Leakage rule (both phases):** every feature must be a function only of `t' ≤ t`. Unit-tested.

---

## 6. Output for use (the public-facing piece)

**Phase 1 (zero-cost path):**

- Pi runs `publish_latest.py` after each Silver rebuild.
- Script writes a small JSON with one record per site: `{site, latest_obs_utc, current_wait_min, predicted_wait_min, confidence, heartbeat_age_mins}`.
- Force-pushes that single file to the `data` branch of the repo via deploy-key SSH. Force-push keeps git history small.
- A static `docs/index.html` (vanilla JS + Leaflet or just an HTML table) is served from GitHub Pages on `main`. It fetches `https://raw.githubusercontent.com/zwleoapp/hospital-monitor/data/latest.json` and renders.
- Cache-busting via `?t=<timestamp>` query string.

This is the simplest possible public surface: no servers, no DNS, no inbound on the Pi, no spend. Petrol Spy Australia is a useful reference for *what* (live, public-good, map-driven) but the implementation here is far smaller — we don't need accounts, comments, or a backend.

**Phase 2 swap:** Databricks job writes the same JSON shape to the same branch. Frontend untouched.

---

## 7. Decisions log (append-only)

| Date | Decision | Rationale |
|---|---|---|
| 2026-04-25 | Pi pushes; cloud never pulls. | Pi has egress-only network; Databricks Free Edition restricts inbound. |
| 2026-04-25 | Bronze append-only end-to-end. | Reproducibility from raw is the foundation of the medallion. |
| 2026-04-25 | Silver recomputed in cloud (Phase 2), not synced from Pi. | Decouples Pi-side bugs from cloud Silver; preserves idempotency. |
| 2026-04-25 | SSOT in markdown, in this repo. | Both AIs read deterministically; git diffs make decisions auditable. |
| 2026-04-25 | Holiday lib pinned to `state='VIC'`. | Eastern Health is Victoria; default `holidays.Australia()` is wrong. |
| 2026-04-25 | One weather pull at metro-Melbourne centroid for v1. | All three sites within ~15 km. Documented as approximation. |
| 2026-04-25 | Databricks tier (when reached) = Free Edition. | Serverless + UC Volumes; outbound restricted, so Pi keeps all external I/O. |
| 2026-04-25 | Publisher = force-push `latest.json` to `data` branch. | Zero-cost public surface; git history stays small. |
| 2026-04-25 | Pi → GitHub auth = deploy-key SSH (scoped). | One-way push, key-rotatable, minimum blast radius. |
| 2026-04-25 | Pi → Databricks auth (Phase 2) = PAT for v1; OAuth M2M deferred. | OAuth is overkill at hobby scale; revisit if a second sensor or service joins. |

---

## 8. Open questions

1. **Sync cadence to GitHub `data` branch:** every 30 min (matches scrape) vs hourly. Default 30 min until file count or push noise is a problem.
2. **Weather endpoint:** Open-Meteo archive (backfill) vs forecast (live). Need both; the join key is the archive value once it's available.
3. **Phase-2 trigger:** which threshold trips first — row count, training time, or grant?

---

## 9. Change log

- **1.0 (2026-04-25)** — Restructured around Phase 1 / Phase 2. Added data-safety posture and ML lifecycle. Replaces SSOT v0.2.
- **1.1 (2026-04-27)** — Merged dashboard design reference (Dual-Clock, Tiered Stale, Vercel). Removed diversion UI.

---

## 10. Dashboard & Operational Design

### Dual-Clock Freshness System

Two independent clocks govern data freshness. They measure different things and can fail independently.

| Clock | Source field | What it measures |
|---|---|---|
| **Global Pi Heartbeat** | `generated_utc` (top-level) | When the Pi last ran `publish_latest.py` and pushed a new JSON |
| **Per-Hospital Observation** | `heartbeat_age_mins` (per site) | How old each hospital's latest scraped observation is at publish time |

**Why two clocks?** The Pi can publish a fresh JSON (heartbeat alive) but still carry stale observations if a single hospital's dashboard was temporarily unavailable, a per-hospital scraper failed, or the scraper ran but recorded no new value. Conversely, per-hospital observations can be recent while the Pi has not yet published (outside operational hours or mid-cycle). `heartbeat_age_mins` is always computed relative to `generated_utc`, not wall-clock time, so it stays stable once published.

### Tiered Stale Thresholds

| Tier | Condition | UI Effect |
|---|---|---|
| **Fresh** | `heartbeat_age_mins ≤ 60` | Normal card rendering |
| **Hospital Stale** | `heartbeat_age_mins > 60` per site | `.stale-card`: 0.7 opacity, light-red border, ⚠️ STALE badge in footer |
| **Network Stale (global)** | **All** hospitals `> 60 mins` | Top red banner: "ALL DATA STALE — Pi may be offline" |

**Why 60 minutes?** The Pi scrapes every 15 min. A 60-min threshold tolerates up to 3 missed cycles before flagging. If the Pi is offline entirely, all hospitals cross 60 min together and the global banner fires. If only one hospital is stale, its card is individually marked but the dashboard stays usable.

The global banner is a "Pi offline" signal, not a "some data is old" signal — individual stale cards handle the latter.

### Network Layout

Hospitals are grouped by network and rendered in fixed order:

1. **Monash Health** — Casey Hospital, Dandenong Hospital, Monash Medical Centre - Clayton
2. **Eastern Health** — Box Hill Hospital, Angliss Hospital, Maroondah Hospital

### Trend Leaderboard

A sidebar panel (desktop) / top horizontal slider (mobile) ranks the top 3 hospitals with the highest negative `wait_momentum` (fastest-improving waits). Hospitals with `wait_momentum ≥ −1` are excluded as noise. The panel is labelled **⚡ Quickest Improvements**.

### Vercel Deployment

- Production branch: `data`
- Pi force-pushes `latest.json` + `index.html` + `vercel.json` to `data` on every publish cycle
- `vercel.json` sets `Cache-Control: no-cache, no-store, must-revalidate` on `/latest.json`
- Browser receives fresh JSON within ~30 s of Pi push, not the 5-min CDN lag of raw GitHub URLs
- Dashboard auto-refreshes every 5 min; stale-check re-runs every 60 s against the in-memory object

### Key UI Constants

| Constant | File | Purpose |
|---|---|---|
| `HOSPITAL_STALE_MINS` | `docs/index.html` | Per-hospital stale threshold (60 min) |
| `NETWORK_ORDER` | `docs/index.html` | Fixed network display order |
| `OPERATIONAL_START_H / END_H` | `scripts/publish_latest.py` | Publish hours gate (06:00–23:00 Melbourne) |
| `MOMENTUM_DAMPING` | `scripts/predict_next.py` | Dampens trend extrapolation |
