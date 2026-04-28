# View Design — Melbourne ED Monitor

## Layout Overview

Single-page dashboard hosted on GitHub Pages. Fetches `latest.json` + `history_timeline.json` from the `data` branch at runtime. No server-side rendering.

```
┌─ Sticky disclaimer bar (emergency warning) ───────────────────┐
│                                                                 │
│  ┌─ Leaderboard (250px fixed) ──┐  ┌─ Hospital cards grid ──┐ │
│  │  Shortest Wait               │  │  Card: Box Hill         │ │
│  │  Fastest Clearing            │  │  Card: Angliss          │ │
│  │  Best Minor Wait             │  │  Card: Maroondah        │ │
│  └──────────────────────────────┘  │  Card: Casey            │ │
│                                     │  Card: Dandenong        │ │
│                                     │  Card: Monash Clayton   │ │
│                                     └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

Max-width: 1160px. Left sidebar is sticky at 52px top offset.

---

## Hospital Card Structure

Each card renders top-to-bottom in this order:

```
1.  Hospital name + status dot
2.  Hero split: URGENT | MINOR | Waiting | In Treatment
3.  Command-centre row: All · Now [Xm] → 60m [🔮 Xm]  [HH:MM AEST]
4.  Confidence badge + 72h accuracy badge
5.  9-in-10 row: [P90 badge · VAHI Qly] [Max wait pairing]
6.  Crisis headline OR trend arrow
7.  ← breathing gap → Triage benchmark chips (Urgent LEFT, Minor RIGHT)
8.  History accuracy badge (history mode only)
────────────────────────────────────────
9.  ▸ Metrics & Index Insights (collapsible)
────────────────────────────────────────
10. Data Heartbeat footer
```

The **Command-centre row** (row 3) is the primary "Brain" signal. Current and 60-minute forecast values render at `1.5rem/800` weight alongside the hospital data timestamp, anchoring freshness to the forecast in a single glance.

---

## Typography Scale

| Element | CSS class | Font size | Weight | Notes |
|---------|-----------|-----------|--------|-------|
| Hero wait value | `.hero-val` | 1.75rem | 800 | Urgent/Minor wait time |
| Crisis headline | `.crisis-headline` | 1.75rem | 800 | LONG WAIT / VERY LONG WAIT |
| **Command-centre current** | `.cmd-val` | **1.5rem** | **800** | All-categories current wait — command row |
| **Command-centre forecast** | `.cmd-forecast-val` | **1.5rem** | **800** | 60m forecast — indigo, command row |
| **Median anchor value** | `.tb-median-val` | **1.3rem** | **900** | e.g., "10m" in triage chip — visual anchor |
| Count value (Waiting/Treating) | `.count-hero` | 1.75rem | 800 | Grey (#6b7280) to distinguish from times |
| Hospital name | `.hosp` | .96rem | 700 | |
| 9-in-10 badge text | `.p90-badge` | .75rem | 700 | Dark pill with P90 value + VAHI Qly label |
| P90 numeric value | `.p90-badge-val` | .85rem | 900 | Inside dark pill |
| Max wait pairing | `.max-wait-pairing` | .85rem | 700 | "Max Xm (Y×)" next to P90 badge |
| Command-centre label | `.cmd-label` | .62rem | 600 | "All · Now" label in command row |
| Command-centre timestamp | `.cmd-time` | .62rem | 400 | Hospital data time, right-aligned in cmd row |
| Triage chip label | `.tb-cat` | .52rem | 700 | "URGENT (CAT 1–3)" |
| Triage median row | `.tb-usual` | .65rem | 700 | "Median [anchor] [VAHI label]" |
| Hero column label | `.hero-sub` | .6rem | 600 | "URGENT", "MINOR", "Waiting" etc. |
| Sub-category label | `.hero-sub-cat` | .5rem | — | "Cat 1–3", "Cat 4–5" |
| VAHI source label | `.tb-qsrc` | .5rem | 400 | "(YoY VAHI Q4 2025)" |

---

## Hero Column Lane Order

Left → Right, always consistent:

| Position | Column | Sub-label |
|----------|--------|-----------|
| 1 (left) | **URGENT** | Cat 1–3 |
| 2 | **MINOR** | Cat 4–5 |
| 3 | Waiting | patient count |
| 4 (right) | In Treatment | patient count |

This is also the order of the triage benchmark chips below the hero. Urgent is always on the LEFT lane.

---

## Triage Benchmark Chips

Two chips, side-by-side:

```
┌─ Urgent (Cat 1–3) ──────┐  ┌─ Minor (Cat 4–5) ────────┐
│  Median                  │  │  Median                   │
│  [10m]  ← visual anchor  │  │  [34m]  ← visual anchor   │
│  (YoY VAHI Q4 2025)      │  │  (YoY VAHI Q4 2025)       │
└──────────────────────────┘  └───────────────────────────┘
```

The numeric value (e.g., `10m`, `34m`) uses `.tb-median-val` at 1.3rem/900 weight, making it the dominant visual element in the chip. The "Median" label and VAHI source caption are small supporting text.

**Chip colour states:**
| Class | Meaning | Background | Text |
|-------|---------|------------|------|
| `.tb-above` | Current wait above median | `#fff7ed` | `#b36b10` (ochre) |
| `.tb-below` | Current wait below median | `#eaf6f4` | `#2a8a7e` (teal) |
| `.tb-near` | Within ±10% of median | `#f1f4fc` | `#6a82b0` (slate blue) |

---

## Command-Centre Row

```
All · Now  45m  →  60m  🔮 52m                         🕐 07:56 AEST
```

The primary "Brain" signal. Replaces the old "All-categories row" at a much larger size:
- Current wait: `.cmd-val` at `1.5rem/800`, dark navy (`#1a1a2e`)
- 60-minute forecast: `.cmd-forecast-val` at `1.5rem/800`, indigo (`#5b72b5`)
- Hospital data timestamp: `.cmd-time` at `.62rem`, pushed right via `margin-left:auto`

Positioned immediately after the hero split (row 3), making the forecast the first data signal after triage times.

---

## 9-in-10 Row

```
[🛡 89m · VAHI Qly]    Max 3hr 12m (2.2×)
```

- P90 badge: dark navy pill, `.75rem` text, P90 value in `.p90-badge-val` at `.85rem`/900
- Badge label: "(9-in-10 · VAHI Qly)" — attributes the benchmark to VAHI quarterly data
- Max wait pairing: inline, `.85rem`/700, immediately to the right of the badge
- If Max wait is > 5× P90: critical pulse animation on the warning chip

---

## Crisis Headline

Triggered by **current wait vs P90**, not max wait. Logic:

| Condition | Class | Label |
|-----------|-------|-------|
| `current_wait >= P90 × 0.80` | `.crisis-long` | ⚠ LONG WAIT (amber) |
| `current_wait >= P90` | `.crisis-very-long` | ⚠ VERY LONG WAIT (red) |

Rationale: P90 represents the point where the system is officially backed up per VAHI quarterly history. Reaching 80% of P90 is an early amber warning; exceeding P90 outright is an objective crisis state. This avoids false positives from occasional high max-wait outliers.

---

## Card Status Colours

| State | Border / dot | Background |
|-------|-------------|------------|
| Green (wait ≤ 30m, stable) | `#2e9e90` | `#e8f7f5` |
| Amber (31–60m) | `#c47d15` | `#fdf5e2` |
| Red (> 60m) | `#c05a4a` | `#fdf1ee` |
| Stale (data age > threshold) | `#d9a89f` | `#fdf6f5` |

---

## Timeline Navigation (History Mode)

When the user steps back into history:
- `#hist-nav` bar appears sticky below disclaimer, showing `← Earlier` / `Later →` / `Live`
- In active history: bar turns indigo (`#3730a3`) with pulse animation on `#hist-banner`
- `updateHistNav()` is called at the end of `renderDashboard()` to ensure button state is always in sync after any data refresh

---

## Collapsible Insights Section

**"Metrics & Index Insights"** — collapsed by default.

Contains:
- Strain Index (Waiting + Treating / capacity)
- Clearing Speed (momentum value with directional arrow)

Accessible via `<details class="system-insights">`. Opens with a 90° rotation on the `▸` chevron.

---

## Stale Data Handling

`STALE_MINS` constant in `index.html` controls the staleness threshold. When `heartbeat_age_mins > STALE_MINS`:
- Card gets `.stale-card` class (muted opacity, pink border)
- STALE badge appears in the Data Heartbeat footer
- Status dot turns grey

---

## Key Constants (in `index.html`)

| Constant | Default | Purpose |
|----------|---------|---------|
| `STALE_MINS` | configurable | Minutes after which a card is marked stale |
| `HOSPITAL_STALE_MINS` | configurable | Per-hospital stale threshold |
| `HISTORY_WINDOW_H` | 24 | Hours of history available in timeline |
