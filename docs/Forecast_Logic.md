# Forecast Logic — Melbourne ED Monitor

## Overview

60-minute wait-time forecast engine using a Hybrid Momentum-Damping Model with a self-evolving calibration loop. The model has three tiers: a rule-based heuristic (live), an ML damping loop (Phase 2), and a human override layer (always available).

---

## Tier 1: Core Forecast Model (`predict_next.py`)

### 60-Minute Projection Formula

```
W60 = Wnow + (M15 × 4 × D)
```

| Symbol | Meaning |
|--------|---------|
| `W60` | Projected minimum wait in 60 minutes |
| `Wnow` | Current minimum wait (minutes, from live Bronze) |
| `M15` | Wait-time momentum per 15-minute cadence (computed from **Clinical Stream**) |
| `4` | Horizon steps: 60 min ÷ 15 min cadence |
| `D` | Damping Factor (bounded [0.5, 1.2], self-evolving via ML loop) |

### Momentum Calculation — Clinical Stream Formula

Momentum is calculated from the **Clinical Stream** (`bronze_raw_scrapes.csv`), using system scrape timestamps rather than hospital-reported timestamps:

```
M15 = (Wscrape_t − Wscrape_t−15) / 15
```

**Rationale:** The hospital's `LastUpdatedDisplay` timestamp reflects when the hospital refreshed their public dashboard, not when real-time system pressure changed. To capture actual ED dynamics, momentum must be computed from the **scrape timestamp** (when we queried the raw Power BI endpoint). This ensures momentum reflects the true rate of change in wait times, not the hospital's publishing cadence.

**Constraints:**
- Floor: `W60 ≥ Wnow × 0.50` — a single momentum spike cannot predict near-zero wait when the system is clearly still busy
- Ceiling: `W60 ≤ 480 min` (8-hour hard cap)

### Triage Split

Derived from VAHI quarterly median ratios:

```
Urgent60 = W60 × (Median_Cat1-3 / (Median_Cat1-3 + Median_Cat4-5))
Minor60  = W60 × (Median_Cat4-5 / (Median_Cat1-3 + Median_Cat4-5))
```

Where `Median_Cat1-3` and `Median_Cat4-5` are the VAHI quarterly medians from `vahi_history_merged.csv`. When VAHI data is unavailable, the current all-category wait is used as the fallback.

### Confidence Score

Composite of three signals, each grounded in the 14-year AIHW baseline:

```
confidence = 0.50 × LOS_score + 0.30 × momentum_score + 0.20 × p90_score
```

| Signal | Formula | Weight | Rationale |
|--------|---------|--------|-----------|
| `LOS_score` | `min(1, LOS_pct_under_4hr / 70)` | 0.50 | Hospital near 70% national target = predictable regime |
| `momentum_score` | `max(0, 1 − \|M15\| / 30)` | 0.30 | Stable trend = more extrapolable |
| `p90_score` | `max(0, 1 − max(0, Wnow − P90) / P90)` | 0.20 | Within historical norms = reliable baseline |

Labels: **High** (≥ 0.70) | **Moderate** (≥ 0.40) | **Low** (< 0.40)

### Strain Index

```
Strain = (Waiting + Treating) / Institutional_Capacity
```

Where `Institutional_Capacity` is derived from AIHW 10-year baseline annual presentations. A Strain Index > 1.0 indicates the hospital is operating above its typical load.

### Forecast Accuracy Formula

```
Accuracy = 100 − (|Predicted − Actual| / Actual × 100)
```

Computed in `get_history.py` for each snapshot where the T+60 observation exists (±15-minute tolerance for cadence drift). Results are stored in `accuracy_postmortem.jsonl`.

---

## Tier 2: Self-Evolving Damping — ML Loop (`evolve_damping_factors()`)

**Status: Phase 2 placeholder — not yet active.**

### How It Works

`evolve_damping_factors()` reads the last 72 hours of accuracy post-mortem entries from:

```
/mnt/router_ssd/Data_Hub/Waiting_Live_time/accuracy_postmortem.jsonl
```

For each hospital, it searches for the damping value `D` that would have minimised mean-absolute-error over that window. The evolved value is written to the pipeline for use on the next run cycle.

### Safety Constraints

All ML-evolved damping values are hard-clamped before use:

```python
DAMPING_MIN = 0.50   # D cannot go below this
DAMPING_MAX = 1.20   # D cannot exceed this
```

This is enforced in both `get_effective_damping()` and `evolve_damping_factors()`.

### Anomaly Exclusion

Snapshots where the absolute error exceeds 200% of actual are **excluded from ML training** and appended to the anomaly log for human review:

```
/mnt/router_ssd/Data_Hub/Waiting_Live_time/damping_anomalies.jsonl
```

Typical causes: system outages, data feed interruptions, major surge events. These should not influence the damping calibration.

---

## Tier 3: Human Intervention

### Override File

`config/overrides.json` provides manual control over the forecast engine. Values here take **priority over all ML-evolved values**. The file is read every pipeline cycle — no restart required.

**Supported keys:**

| Key | Type | Effect |
|-----|------|--------|
| `manual_damping` | float | Override damping for all hospitals |
| `manual_damping_per_site` | object | Per-hospital damping, keyed by exact hospital name |

Keys prefixed with `_` (e.g., `_manual_damping`) are treated as comments and ignored.

**Example — global reset to default:**
```json
{
  "manual_damping": 0.50,
  "_comment": "Manually set 2026-05-01 — Box Hill maintenance surge"
}
```

**Example — per-hospital:**
```json
{
  "manual_damping_per_site": {
    "Box Hill Hospital": 0.80
  }
}
```

### When to Intervene

- A major infrastructure event at a hospital (ward closures, surge capacity activation)
- The ML-evolved damping has drifted outside expected behaviour
- Resetting to a known-good baseline after an anomalous period

### How to Reset

1. Open `config/overrides.json`
2. Set `manual_damping` to the desired value (nominal default: `0.50`)
3. Run the pipeline: `bash run_monitor.sh`
4. Confirm the dashboard reflects the reset
5. Remove the `manual_damping` key once the anomalous period has passed

### Reviewing Anomalies

```bash
# View recent anomaly entries
tail -20 /mnt/router_ssd/Data_Hub/Waiting_Live_time/damping_anomalies.jsonl

# Count anomalies by hospital
grep -o '"hospital":"[^"]*"' /mnt/router_ssd/Data_Hub/Waiting_Live_time/damping_anomalies.jsonl | sort | uniq -c
```

---

## Data Integrity

### Monash Health — Per-Campus Timestamp Extraction

Monash Health requires per-campus timestamp extraction due to independent Power BI report refresh cycles. Casey, Dandenong, and Clayton each run separate Power BI refresh jobs and may show different `LastUpdatedDisplay` values at any given scrape.

**Power BI Data Source:** `CurrentPatients` entity

**Clinical Properties Extracted:**
| Property | Power BI Column | Scraper Mapping | Purpose |
|----------|----------------|-----------------|---------|
| Total Waiting | `TotalWaiting` | `col_waiting` (G1) | Patient count waiting to be seen |
| Total Being Treated | `TotalBeingTreated` | `col_treating` (G2) | Patient count currently in treatment |
| Estimated Time | `Estimated Time` | `col_wait_str` (G3) | Wait range string (e.g., "2 hr 46 min - 6 hr 50 min") |
| Last Updated Display | `LastUpdatedDisplay` | `col_last_updated` (G4) | Per-campus data freshness timestamp (e.g., "20:31") |
| Adult/Paed Group | `AdultPaed` | `group_col` (G0) | Patient category grouping — scraper targets "Adult" group |
| Campus Filter | `Campus` | `hospital_col` (WHERE clause) | Campus name filter (Casey/Clayton/Dandenong) |

The scraper sends one grouped query per campus in the batch POST (`_scrape_powerbi_source()`). Each query groups by `AdultPaed` and selects columns G0–G4. The DSR response returns all patient-category rows (Adult + Paediatric) for that campus.

The scraper:
1. Extracts G4 (`LastUpdatedDisplay`) from each campus's DSR result
2. Writes per-campus timestamps to sidecar JSON
3. Scans ALL patient groups (Adult + Paed) for the highest wait upper-bound — this becomes the campus's `max_wait_mins`

**KNOWN LIMITATION (2026-04-30):** `LastUpdatedDisplay` (G4) is a **report-level property**, not per-campus. Power BI returns the same timestamp for all three Monash campuses (e.g., all show "18:31"), but the webpage visual tiles display different per-campus timestamps (Casey 18:26, Clayton 18:06, Dandenong 17:26). Evidence: The Power BI API's `ValueDicts.D2` array contains only **one timestamp value**, confirming all campuses reference the same report-level refresh time. The visual tiles either compute timestamps client-side from a hidden measure or query a different entity not exposed in the `CurrentPatients` table. Current scraper extracts the report-level timestamp as the best available proxy.

`publish_latest.py` reads this sidecar and maps each timestamp to the matching site's `last_updated_display` field in `latest.json`. When all campuses return identical timestamps, the scraper tags them with `^` prefix to signal report-level (not per-campus) freshness.

**Eastern Health** uses an HTTP `Date` response header fallback (prefixed `~`) since its pages do not embed a native published timestamp.

### Data Validation Policy — The "Published Truth" Standard

**Primary source:** Data values must align with what is visually published on the hospital's public-facing webpage, not with raw internal Power BI query output.

**Why this matters:** The internal Power BI query engine and the public dashboard UI operate on independent refresh cycles. At any moment the raw `SemanticQueryDataShapeCommand` response may reflect a model state that has not yet propagated to the rendered tiles a patient would see. If the public webpage shows 40 minutes and this monitor shows 32 minutes sourced from an internal query, users lose trust in the tool — and rightly so, because the hospital's official public statement is 40 minutes.

**Implications for scraper design:**

| Source | Standard | Notes |
|--------|----------|-------|
| Eastern Health | UI-scraped (`patientCounts` + `predictedWaitMinutes` JS variables embedded by the page renderer) | Matches exactly what the public page displays |
| Monash Health | Power BI batch API (`/querydata`) — the same data that populates the rendered tiles | Acceptable because the API is the render-time data source for the public tiles, not a pre-render internal feed |

When discrepancies appear between monitor values and values a user sees on the hospital website, the website value is Ground Truth. Investigate whether the scraper is hitting a pre-render endpoint, a cached layer, or a different entity/column than the one driving the public tile.

---

## Data Flow Summary

```
Bronze CSV (live scrape)
    │
    ▼
transform_silver.py → Silver CSV (momentum, VAHI context enriched)
    │
    ▼
predict_next.py → outlook JSON
    │  get_effective_damping()
    │    ├─ config/overrides.json (Tier 3 — human override)
    │    ├─ evolve_damping_factors() (Tier 2 — ML loop, Phase 2)
    │    └─ MOMENTUM_DAMPING constant (Tier 1 — default 0.50)
    │
    ▼
get_history.py → history_timeline.json + accuracy_postmortem.jsonl
    │
    ▼
publish_latest.py → latest.json → data branch → GitHub Pages
```
