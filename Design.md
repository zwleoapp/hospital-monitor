# Hospital Monitor — Design Reference

## Architecture Overview

Raspberry Pi scrapes Eastern Health and Monash Health ED dashboards every 15 min.
Data flows: **Bronze** (raw CSV on Pi SSD) → **Silver** (enriched) → **Gold** (`latest.json` on data branch).
The Vercel dashboard (`index.html`) fetches `latest.json` from the same deployment at runtime.

---

## Data Freshness Logic

### Dual-Clock System

Two independent clocks govern freshness. They measure different things and fail independently.

| Clock | Source field | What it measures |
|---|---|---|
| **Global Pi Heartbeat** | `generated_utc` (top-level) | When the Pi last ran `publish_latest.py` and pushed a new JSON |
| **Per-Hospital Observation** | `heartbeat_age_mins` (per site) | How old each hospital's latest scraped observation is at publish time |

**Why two clocks?**

The Pi can publish a fresh JSON (heartbeat alive) but still carry stale observations if:
- The hospital's live dashboard was temporarily unavailable during the scrape cycle
- A single hospital scraper failed while others succeeded
- The scraper ran but no new observation was recorded (same value repeated)

Conversely, a site's observations can be recent but the Pi may not have published a new JSON yet
(outside operational hours or mid-cycle). The per-hospital age is always relative to `generated_utc`,
not wall-clock time, so it stays stable once published.

### Tiered Stale Thresholds

| Tier | Condition | UI Effect |
|---|---|---|
| **Fresh** | `heartbeat_age_mins ≤ 60` | Normal card rendering |
| **Hospital Stale** | `heartbeat_age_mins > 60` per site | `.stale-card` class: 0.7 opacity, light-red border, ⚠️ STALE badge, greyed-out footer |
| **Network Stale (global)** | **All** hospitals `> 60 mins` | Top red banner: "DATA STALE — Pi may be offline" |

**Why 60 minutes?**

- The Pi scrapes every 15 min. A 60-min threshold tolerates up to 3 missed cycles before flagging.
- A single missed cycle (15 min) or brief dashboard outage should not alarm users.
- If the Pi is offline entirely, all hospitals will cross 60 min together → global banner fires.
- If only one hospital is stale, its card is individually marked but the dashboard remains usable.

### Global Banner Rule

The top red banner only appears when **every** hospital in the payload has `heartbeat_age_mins > 60`.
This means: one fresh hospital proves the Pi is alive; the banner is suppressed.
The banner is a "Pi offline" signal, not a "some data is old" signal — cards handle that locally.

---

## Network Layout

Hospitals are grouped by network and rendered in fixed order:

1. **Monash Health** — Casey Hospital, Dandenong Hospital, Monash Medical Centre - Clayton
2. **Eastern Health** — Box Hill Hospital, Angliss Hospital, Maroondah Hospital

Diversion suggestions are scoped within-network only (never cross-network).

---

## Vercel Deployment

- Vercel production branch: `data`
- Pi force-pushes `latest.json` + `index.html` + `vercel.json` to the `data` branch on every publish cycle
- `vercel.json` sets `Cache-Control: no-cache, no-store, must-revalidate` on `/latest.json`
- Browser receives fresh JSON within ~30 s of Pi push (Vercel deploy time), not 5-min CDN lag
- Dashboard auto-refreshes every 5 min; stale-check runs every 60 s against the cached JS object

---

## Key Constants

| Constant | File | Purpose |
|---|---|---|
| `STALE_MINS` (JS) | `docs/index.html` | Legacy global threshold (now superseded by per-card 60-min check) |
| `HOSPITAL_STALE_MINS` (JS) | `docs/index.html` | Per-hospital stale threshold (60 min) |
| `OPERATIONAL_START_H / END_H` | `scripts/publish_latest.py` | Hours gate (06:00–23:00 Melbourne) |
| `DIVERSION_STRAIN_DELTA` | `scripts/publish_latest.py` | Strain gap for diversion suggestion |
| `MOMENTUM_DAMPING` | `scripts/predict_next.py` | Dampens trend extrapolation |
