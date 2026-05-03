# Forecast Logic — Melbourne ED Monitor

**Updated:** 2026-05-03

## Overview

60-minute wait-time forecast engine using a Hybrid Momentum-Damping Model with a self-evolving calibration loop. Three tiers: rule-based heuristic (always active), ML damping loop (active — runs weekly via `evolve_model.py`), and human override layer (always available).

Covers **7 full-pipeline hospitals**: Eastern Health (Box Hill, Angliss, Maroondah) and Monash Health (Casey, Clayton, Dandenong) and Melbourne Health (Royal Melbourne Hospital). Royal Children's Hospital is `raw_only` — status card only, no forecast.

---

## Tier 1: Core Forecast Model (`predict_next.py`)

### 60-Minute Projection Formula

```
W60 = clamp(max(Wnow × 0.50,  Wnow + M15 × 4 × D),  max=480)
```

| Symbol | Meaning |
|---|---|
| `W60` | Projected minimum wait in 60 minutes |
| `Wnow` | Current minimum wait (minutes, from Silver) |
| `M15` | Wait-time momentum per 15-min cadence (Silver column `wait_momentum`) |
| `4` | Horizon steps: 60 min ÷ 15 min cadence |
| `D` | Effective damping factor — resolved by `get_effective_damping()` |

Floor `Wnow × 0.50`: a single momentum spike cannot predict near-zero wait when the system is clearly still busy.  
Ceiling `480 min`: 8-hour hard cap.

### Momentum (`wait_momentum`)

Computed in `transform_silver.py` on the deduped Silver CSV:

```
wait_momentum = (min_wait_mins_t − min_wait_mins_t−1) / (gap_minutes / 15)
```

Normalised to one 15-min cadence unit regardless of actual gap (handles both 15-min and 30-min intervals). `NaN` for the first-ever row per hospital (no prior observation to diff against); all downstream consumers use `pd.notna` guards.

### Effective Damping (`get_effective_damping()`)

Priority order — highest wins:

1. `config/overrides.json → manual_damping_per_site[hospital]` (human override, per-site)
2. `config/overrides.json → manual_damping` (human override, global)
3. `config/model_config.json → per_hospital_damping[hospital][{day_type}_{time_band}]` (ML-evolved, segmented)
4. `config/model_config.json → per_hospital_damping[hospital]` (ML-evolved, mean across segments)
5. `config/model_config.json → momentum_damping` (global default, 0.50)

All values clamped to `[damping_min, damping_max]` = `[0.50, 1.20]`.

### Triage Split

Derived from VAHI quarterly median ratios:

```
ratio_urgent = Median_Cat1-3 / (Median_Cat1-3 + Median_Cat4-5)
Urgent60 = W60 × ratio_urgent
Minor60  = W60 × (1 − ratio_urgent)
```

VAHI medians come from Silver `ctx_wait_median_cat123_mins` / `ctx_wait_median_cat45_mins`. For Royal Melbourne Hospital (no VAHI data yet), these are proxy values from `ctx_defaults` in `hospitals.json` (`ctx_source="ESTIMATE"`).

### Confidence Score

```
confidence = 0.50 × LOS_score + 0.30 × momentum_score + 0.20 × p90_score
```

| Signal | Formula | Weight |
|---|---|---|
| `LOS_score` | `min(1, LOS_pct_under_4hr / 70)` | 0.50 — proximity to 70% national LOS target |
| `momentum_score` | `max(0, 1 − |M15| / 30)` | 0.30 — stable trend = more extrapolable |
| `p90_score` | `max(0, 1 − max(0, Wnow − P90) / P90)` | 0.20 — within historical norms |

Labels: **High** (≥ 0.70) | **Moderate** (≥ 0.40) | **Low** (< 0.40)

For hospitals with `ctx_source="ESTIMATE"`, confidence uses proxy ctx values — treat as indicative only until real VAHI benchmarks are onboarded.

### Strain Index

```
strain_index = predicted_wait_min / p90_wait_min
```

Where `p90_wait_min` = VAHI quarterly 90th-percentile wait (Silver `ctx_wait_p90_mins`). Values: < 0.70 = low load, 0.70–1.00 = moderate, > 1.00 = above historical normal. A strain of 1.5 means the predicted wait is 1.5× the hospital's normal p90.

*Note: Earlier versions defined strain as `(Waiting + Treating) / Institutional_Capacity`. That formula was replaced — the predicted_wait / p90 ratio is more directly comparable across hospitals with different absolute capacities.*

### Forecast Accuracy

```
accuracy = 100 − (|W60_predicted − W60_actual| / W60_actual × 100)
```

Computed in `get_history.py` for each snapshot where the T+60 observation is available (±15-min tolerance). Written to `forecast_audit.csv` on the SSD for ML backtesting.

---

## Tier 2: Self-Evolving Damping — `evolve_model.py`

**Status: Active — runs weekly (Sunday 08:00 AEST local cron).**

### Segmentation

Damping is evolved separately for each `(hospital, day_type, time_band)` combination:

- `day_type`: `weekday` | `weekend` | `public_holiday` (Victorian calendar via `holidays` package)
- `time_band`: `overnight` (00–06) | `morning` (06–12) | `afternoon` (12–18) | `evening` (18–24) (Melbourne local)

Compound key: `"weekday_morning"`, `"weekend_evening"`, etc. — 12 segments maximum per hospital.

### Algorithm

For each `(hospital, segment)` with ≥ 4 rows in `forecast_audit.csv`:

1. Grid-search `d ∈ [damping_min, damping_max]` in 0.05 steps
2. For each candidate: compute `MAE = mean(|project_wait(current, momentum, d) − actual|)`
3. Accept `best_d` only if its MAE beats the current damping's MAE — never regress
4. Write `{hospital: {segment: best_d}}` to `config/model_config.json` — **merge, never replace** (hospitals below minimum data threshold keep their previous evolved values)

**Minimum data requirement:** 24 rows per hospital (across all segments) before evolution runs.

### Running Manually

```bash
python3 scripts/evolve_model.py --audit   # per-segment breakdown, no write
python3 scripts/evolve_model.py           # compute and write to model_config.json
python3 scripts/evolve_model.py --dry-run # compute only, no write
```

### Anomaly Exclusion

Snapshots where `|error_pct| > anomaly_error_pct` (default 200%) are excluded from ML training and logged to `accuracy_postmortem.jsonl` anomaly section for human review.

### Future Path

Once ~500 rows per hospital (≈6 months), replace grid search with multivariate Ridge regression using `current_wait`, `momentum`, `treating_count`, `hour_sin/cos`, `is_weekend`, `is_holiday` as features. `forecast_audit.csv` schema already captures all required inputs.

---

## Tier 3: Human Override (`config/overrides.json`)

Read every pipeline cycle — no restart required. Takes priority over all ML-evolved values.

```json
{
  "manual_damping": 0.50,
  "_comment": "Global reset — 2026-05-01 Box Hill maintenance surge"
}
```

```json
{
  "manual_damping_per_site": {
    "Box Hill Hospital": 0.80
  }
}
```

Keys prefixed `_` are ignored.

---

## Silver Context Tiers for Forecast

Silver enriches Bronze with context benchmarks in three tiers:

| `ctx_source` | Data | Quality |
|---|---|---|
| `"VAHI"` | Quarterly VAHI benchmarks (Oct 2024–) | Best — hospital-specific quarterly actuals |
| `"AIHW"` | Annual AIHW episode data (2011–2025) | Lower fidelity — total ED episode time vs wait-to-treatment |
| `"ESTIMATE"` | Proxy values from `ctx_defaults` in `hospitals.json` | Indicative only — used for newly onboarded hospitals without VAHI history |

Royal Melbourne Hospital currently uses `"ESTIMATE"`. Confidence scores and triage splits for RMH should be treated as approximate until real VAHI data is loaded.

---

## Data Flow Summary

```
Bronze CSV (scrape every 15 min)
    │
    ▼
transform_silver.py
    │  Context join: VAHI → AIHW → ctx_defaults (3-tier LEFT join)
    │  Adds: wait_momentum, load_ratio, temporal features
    │
    ▼
publish_latest.py
    │  predict_next.py → W60 via get_effective_damping()
    │     ├─ overrides.json      (Tier 3)
    │     ├─ per_hospital_damping (Tier 2, per-segment, from evolve_model.py)
    │     └─ momentum_damping    (Tier 1, global default)
    │  get_history.py → history_timeline.json
    │                 → forecast_audit.csv (for evolve_model.py)
    │
    ▼
latest.json → data branch → Vercel
```

**Not in the forecast pipeline:** Royal Children's Hospital (`raw_only`) — status card only.
