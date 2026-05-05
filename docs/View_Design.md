# View Design — Melbourne ED Monitor

**Updated:** 2026-05-04

> Describes the current UI layout in `docs/index.html`. For data schemas that drive content, see [dataflow.md](dataflow.md).

---

## Page Layout

```
┌─ Sticky disclaimer bar ──────────────────────────────────────────────┐
├─ Left sidebar (fixed 250px) ────── Right: hospital card grid ────────┤
│  Leaderboard                        ┌─ Network block ──────────────┐ │
│  ├─ Shortest Wait                   │  network label               │ │
│  └─ Best for Minor                  │  ┌─ card ─┐ ┌─ card ─┐     │ │
│                                     │  └────────┘ └────────┘     │ │
│                                     └──────────────────────────────┘ │
│                                     ┌─ Status-only sites ──────────┐ │
│                                     │  RCH status card             │ │
│                                     └──────────────────────────────┘ │
├─ History nav (sticky, shows after data loads) ───────────────────────┤
│  ← Earlier    18:00 AEST    Later →    Live                          │
└──────────────────────────────────────────────────────────────────────┘
```

**Network display order** (`NETWORK_ORDER`): Monash Health → Eastern Health → Melbourne Health. Other networks follow alphabetically.

---

## Hospital Card Structure (top → bottom)

### 1. Cat 1 chip
```
⚡ Cat 1 — Always seen immediately, no wait     [green chip]
```
CSS: `.cat1-chip`

### 2. Patient counts
"Adult" label shown only when Paeds data present.
```
22 waiting · 57 in treatment          [1.75rem .count-num]
```
CSS: `.counts-block .count-row .count-cohort .count-num`

### 3. Wait time row
```
All · Now  49m–3h41m  →  60min ⏱ 117m  ⏰ 17:45
```
- "All ·" prefix only when Paeds present
- Portal timestamp (⏰ HH:MM AEST) from `last_updated_display` via sidecar
- CSS: `.wait-time-row .wt-label .wt-now .wait-range-sep .wait-range-max .wt-arrow .wt-fc .wt-time`

### 4. Triage 60-min row (when VAHI medians available)
```
In 60min  Cat 2–3  35m  |  Cat 4–5  39m–43m    [grey chip]
```
CSS: `.triage60-row .triage60-lbl .triage60-item .triage60-cat .triage60-val`

### 5. Trend + confidence + accuracy
```
↑ Worsening  High confidence 82%

FORECAST ACCURACY
04:30pm  ⏱ 49m → ✓ 05:30pm 48m   98%
04:15pm  ⏱ 51m → ✓ 05:15pm 48m   93%
04:00pm  ⏱ 53m → ✓ 05:00pm 50m   96%
```
- Trend arrow and label from `wait_momentum` (Rising / Improving / Stable)
- Confidence label from `confidence_label` (High / Moderate / Low)
- **Forecast Accuracy block** — 3 rows, newest first, from `history_timeline.json` snapshots loaded in-browser. Each row shows:
  - **Left:** forecast time (Melbourne local, when the prediction was made)
  - **⏱ Xm:** the 60-min prediction made at that time
  - **→ ✓ T+60 Ym:** actual wait observed at T+60 (Melbourne local), with ✓ icon
  - **Right:** accuracy % (bold blue)
  - ⏱ = prediction icon &nbsp; ✓ = actual/confirmed icon
- Falls back to `s.recent_accuracy` mean badge if history not yet loaded; `Calibrating…` if no data
- CSS: `.acc-hist-block` (wrapper) · `.acc-hist-label` (header) · `.acc-hist-row` (each row)
  - `.ah-time` (forecast time, grey, fixed width) · `.ah-val` (icons + values) · `.ah-pct` (%, bold blue)
- Historical mode (time-nav): single row using the snapshot's own `_forecast_accuracy` + `_actual_60m`
- Data source: `_recentForecasts(siteName, 3)` — walks `_histTimeline.snapshots` newest-first,
  returns completed forecasts where `forecast_accuracy != null && actual_60m_wait_min != null`

### 6. Paediatric section (Monash Casey + Clayton only)
```
┃ PAEDIATRIC                              [blue left-border, tinted bg]
┃ 4 waiting · 11 in treatment             [1.3rem]
┃ Now  48m–2h15m  No 60min forecast yet
```
CSS: `.paeds-section .paeds-section-hdr .count-num-paeds .wt-now-paeds .wt-no-fc`

### 7. Metrics & Index Insights (accordion, collapsed by default)
- Strain Index, Clearing Speed, Scraper Sync
- VAHI Benchmarks: p90, Cat 2–3 median, Cat 4–5 median, quarterly label

---

## Status-only Sites Section

Below the main grid when `status_sites[]` is non-empty. Currently: Royal Children's Hospital.

```
STATUS-ONLY SITES
┌─────────────────────────┐
│ Royal Children's Hosp.  │
│ PAEDIATRIC ED           │
│ 🔴 Extremely Busy       │
│ Updated 17:03 AEST · 0m │
└─────────────────────────┘
```

Badge colours (`_STATUS_BADGE` constant):

| Level | Dot | CSS class | Background |
|---|---|---|---|
| Normal | 🟡 | `.status-badge-normal` | Yellow tint (#fef9c3) |
| Very Busy | 🟠 | `.status-badge-verybusy` | Orange tint (#fff3e0) |
| Extremely Busy | 🔴 | `.status-badge-extremebusy` | Red tint (#fee2e2) |
| No data | — | `.status-badge-unknown` | Grey (#f3f4f6) |

CSS: `.status-sites-block .status-sites-grid .status-card .status-card-name .status-card-sub .status-badge`

---

## Typography Scale

| Element | Size | Weight | Colour |
|---|---|---|---|
| Patient counts (Adult) | 1.75rem | 800 | #6b7280 |
| Patient counts (Paeds) | 1.3rem | 800 | #6b7280 |
| Current wait | 1.75rem | 800 | #1a1a2e dark navy |
| 60-min forecast | 1.75rem | 800 | #4338ca indigo |
| Wait range / max | 0.9rem | 700 | #9ca3af grey |
| Portal timestamp | 0.62rem | 500 | #9ca3af |
| Confidence label | 0.65rem | 600 | #374151 |
| Triage chip | 0.7rem | 600 | #374151 |
| Status badge | 0.75rem | 700 | per-level |

---

## History Navigation

`#hist-nav` sticky below disclaimer. Buttons: ← Earlier | timestamp | Later → | Live.

- Only available for snapshots within last 3 hours (`UI_DISPLAY_WINDOW_MINS = 180`)
- "Only available within the last 3 hours" note shown at earliest snapshot
- History mode: network labels turn indigo, status-sites hidden (`renderStatusSites([])`)

---

## Stale Data

`HOSPITAL_STALE_MINS` (default 60, from `outlook.ui_thresholds`). When all sites have `heartbeat_age_mins > 60`:
- Stale banner appears with total age
- Grid opacity 0.3, grayscale filter, pointer-events none

---

## Key JS Constants (all in the `<script>` block of index.html)

| Constant | Purpose |
|---|---|
| `NETWORK_ORDER` | Display order of network groups |
| `_uiT` | UI thresholds read from `outlook.ui_thresholds` — replaces hardcoded values |
| `UI_CAT1_NOTE` | Text in Cat 1 chip |
| `UI_NO_FC_PAEDS` | Text when no Paeds 60-min forecast |
| `_STATUS_BADGE` | Label → dot + CSS class for status cards |
| `DATA_URL` | `/latest.json` (Vercel root-relative, no-cache) |
| `HISTORY_URL` | `/history_timeline.json` (15-min cache) |

Accuracy is no longer computed client-side. `s.recent_accuracy` from `latest.json` is used directly — no `localStorage`, no `FORECAST_KEY`, no calibration wait per device.
