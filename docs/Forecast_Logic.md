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
| `M15` | Wait-time momentum per 15-minute cadence (computed in `transform_silver.py`) |
| `4` | Horizon steps: 60 min ÷ 15 min cadence |
| `D` | Damping Factor (bounded [0.5, 1.2], self-evolving via ML loop) |

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
