# Forecast Logic — Melbourne ED Monitor

**Updated:** 2026-05-04

## Overview

60-minute wait-time forecast engine with a self-evolving ML loop. Three tiers: Ridge regression (Phase 2, active), damped linear extrapolation fallback (Phase 1), and human override layer. All dispatch is transparent via `predict_wait()`.

**Self-evolving loop:** every 15-min scrape appends a completed T+60 row to `forecast_audit.csv` → weekly `evolve_model.py` re-fits Ridge coefficients from all accumulated history → auto-promotes or demotes each hospital → `model_config.json` updated → next pipeline cycle picks up new coefficients. No human intervention required.

Covers **7 full-pipeline hospitals**: Eastern Health (Box Hill, Angliss, Maroondah), Monash Health (Casey, Clayton, Dandenong), Melbourne Health (Royal Melbourne Hospital). Royal Children's Hospital is `raw_only` — status card only, no forecast.

**Current model status (2026-05-05):** All 7 hospitals on Ridge regression. Weekly re-evaluation every Sunday 08:00 AEST.

---

## Tier 1: Core Forecast Model (`predict_next.py`)

### 60-Minute Projection Formula

```
W60 = clamp(max(Wnow × F,  Wnow + M15 × 4 × D),  max=480)
```

| Symbol | Meaning | Config key |
|---|---|---|
| `W60` | Projected minimum wait in 60 minutes | — |
| `Wnow` | Current minimum wait (minutes, from Silver) | — |
| `M15` | Wait-time momentum per 15-min cadence (Silver column `wait_momentum`) | — |
| `4` | Horizon steps: 60 min ÷ 15 min cadence | `horizon_min` / `cadence_min` |
| `D` | Effective damping factor — resolved by `get_effective_damping()` | `momentum_damping` |
| `F` | Floor ratio — projected wait cannot fall below F × Wnow | `momentum_floor_ratio` |

Floor `Wnow × F` (`momentum_floor_ratio`, default 0.50, in `config/model_config.json`): a single improving-momentum spike cannot predict near-zero wait when the ED is clearly still busy. Reduce `F` for hospitals that genuinely clear fast; increase it to make the model more conservative.  
Ceiling `480 min`: 8-hour hard cap (`max_wait_min`).

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

Computed in `get_history.py` for each snapshot where the T+60 observation is available (±15-min tolerance). Written to `forecast_audit.csv` on the SSD.

**Recent Accuracy (UI badge):** `publish_latest.py` reads `forecast_audit.csv` at publish time and computes two values per hospital, embedded in `latest.json`:
- `recent_accuracy` — mean over the last `RECENT_ACCURACY_LOOKBACK` (12) same-segment distinct rows
- `recent_accuracy_trend` — the last 3 individual values, oldest-first

The badge displays the trend inline: `Recent Accuracy ↑ 88% · 91% · 90%`. The direction arrow (↑ ↓ →) is derived from first vs last value. If fewer than 3 trend values exist, the mean is shown as fallback. Every device sees the same server-computed values immediately — no client-side calibration period.

**Segment-aware:** the `n` comparisons are drawn from rows whose `day_type + time_band` match the *current* Melbourne moment (e.g. `weekday_morning`). This gives a like-for-like view — Box Hill's 9am weekday badge reflects weekday morning forecasts only, not weekend evenings or public holidays. Falls back to the last `n` rows across all segments if the matching segment has fewer than `n` completed comparisons (early calibration, infrequent segments such as `public_holiday`).

**Deduplication:** `forecast_audit.csv` re-appends the same completed bucket on every pipeline cycle while it remains in the 3h Silver window (~4 writes per forecast). `_load_recent_accuracy` deduplicates on `(hospital, bucket_utc)` — same as `evolve_model.py` — so "last n" always means n distinct forecast snapshots, not n raw CSV rows.

- **All triage categories combined** — per-category breakdown is not available from source portals.
- **"Calibrating"** shown when fewer than `RECENT_ACCURACY_LOOKBACK` completed comparisons exist (e.g. first 90 min after a new hospital is added, or `forecast_audit.csv` is absent).
- `RECENT_ACCURACY_LOOKBACK` (default 12) set in `config/ui_config.json`.

### `forecast_audit.csv` schema

| Column | Source | Notes |
|---|---|---|
| `bucket_utc` | Scrape bucket | UTC, floored to 15 min |
| `hospital` | Hospital name | |
| `cohort` | "Adult" | Paediatric not yet forecast |
| `source_type` | `html_js` / `powerbi` | For interpreting `cache_lag_minutes` |
| `day_type` | `weekday` / `weekend` / `public_holiday` | Victorian calendar |
| `time_band` | `overnight` / `morning` / `afternoon` / `evening` | Melbourne local |
| `current_wait_min` | Silver `min_wait_mins` | Model input |
| `wait_momentum` | Silver `wait_momentum` | Model input |
| `treating_count` | Silver `treating` | Capacity signal — active Ridge feature |
| `actual_wait_min` | T+60 observation | Ground truth |
| `predicted_wait_min` | Live forecast at T | Frozen — what was actually published |
| `error_pct` | Live error | Based on `predicted_wait_min` |
| `forecast_accuracy` | Live accuracy | 100 − min(error_pct, 100) |
| `cache_lag_minutes` | Diagnostic | See `dataflow.md` |
| `fidelity_status` | Diagnostic | `SYNCED` / `API_LEAD_ACTIVE` / `PORTAL_STALE_WARNING` |
| `bucket_local_melb` | `bucket_utc` in Melbourne local | ISO 8601 with UTC offset — AEST `+10:00`, AEDT `+11:00` |
| `backtest_predicted_wait_min` | `backtest_model.py` | Current-formula prediction for this row |
| `backtest_error_pct` | `backtest_model.py` | \|backtest − actual\| / actual × 100 |
| `backtest_accuracy` | `backtest_model.py` | 100 − min(backtest_error_pct, 100) |

---

## Tier 1b: Backtesting — `backtest_model.py`

Run after any model change (evolved damping, formula tweak, `momentum_floor_ratio` adjustment) to recompute what the *current* model would have predicted on all historical rows.

```bash
python3 scripts/backtest_model.py --dry-run  # preview — no file write
python3 scripts/backtest_model.py            # rewrite forecast_audit.csv in-place
python3 scripts/backtest_model.py --audit    # per-hospital per-segment live vs backtest table
```

**What it does:** For every row in `forecast_audit.csv`, calls `predict_wait(hospital, current_wait_min, wait_momentum, treating_count, bucket_melb)` — the same dispatcher used at live prediction time. This routes to Ridge regression or damping per `hospital_model_type`, using current `model_config.json` + `overrides.json`. Writes `backtest_predicted_wait_min`, `backtest_error_pct`, `backtest_accuracy` columns. Fills `bucket_local_melb` for any older rows missing the field.

**What it preserves:** `predicted_wait_min`, `error_pct`, `forecast_accuracy` are **never modified** — they remain the frozen live-at-the-time record.

**DST handling:** `bucket_local_melb` uses `ZoneInfo("Australia/Melbourne")`, which switches automatically between AEST (+10:00) and AEDT (+11:00). The UTC offset embedded in the ISO 8601 string makes historical rows unambiguous across DST transitions.

**Preferred workflow — use the batch script:**

```bash
bash run_backtest.sh   # evolve → recompute backtest columns → print audit (all in one)
```

Or step by step:

```bash
python3 scripts/evolve_model.py          # re-fit Ridge (GCV) + damping, write model_config.json
python3 scripts/backtest_model.py        # recompute backtest columns
python3 scripts/backtest_model.py --audit  # review improvement table
```

---

## Tier 2: Ridge Regression — Phase 2 (`evolve_model.py`)

**Status: Active — auto-promoted hospitals use Ridge from next pipeline cycle.**

### Feature vector (9 features including intercept)

| Feature | Encoding | Why |
|---|---|---|
| intercept | 1.0 | baseline |
| `current_wait` | raw minutes | anchor — level signal |
| `momentum` | raw min/15min | trend direction |
| `treating_count` | raw count | capacity pressure — high treating + rising wait = sustained pressure |
| `hour_sin`, `hour_cos` | `sin/cos(2π × hour / 24)` | continuous daily cycle — hour 23 adjacent to hour 0 |
| `dow_sin`, `dow_cos` | `sin/cos(2π × dow / 7)` | continuous weekly cycle — Sunday adjacent to Monday |
| `is_holiday` | 0 or 1 | Victorian public holiday flag |

`hour_sin/cos` and `dow_sin/cos` outperform the coarse `time_band` and `day_type` buckets used by Phase 1 because they capture the smooth cyclical shape of ED demand rather than sharp step-function boundaries.

### Ridge solver — GCV alpha selection (numpy, no external ML library)

Alpha is selected automatically per hospital via **GCV (Generalised Cross-Validation)**, analytically equivalent to leave-one-out CV and computed via SVD — no iteration, no held-out splits, no external dependency beyond numpy.

```
X = U S Vᵀ                              ← SVD of feature matrix
d_α = s² / (s² + α)                     ← shrinkage per singular component
ŷ   = U @ (d_α × Uᵀy)                  ← Ridge predictions for candidate α
GCV(α) = MSE(y, ŷ) / (1 − mean(d_α))²  ← penalises over-fitted models
```

The alpha with the lowest GCV score is selected from `regression_alpha_candidates` in `model_config.json`. `gcv_denom_floor` (also in `model_config.json`) prevents division by zero numerically.

The chosen alpha per hospital is written to `regression_chosen_alphas` in `model_config.json` after each run — useful for diagnosing data quality (high alpha = noisy data needs heavy shrinkage; low alpha = smooth, predictable data).

**No hardcoded values** — all tuning parameters (`regression_alpha_candidates`, `gcv_denom_floor`, `min_rows_regression`) live in `model_config.json`. See DESIGN.md §10.

### Auto-promotion logic

`evolve_model.py` runs Ridge for every hospital with ≥ `min_rows_regression` rows (default 50). It compares `ridge_mae` vs `damping_mae` on the same rows:
- If `ridge_mae < damping_mae` → writes coefficients to `regression_coefficients`, sets `hospital_model_type[hospital] = "ridge"`
- If not → keeps "damping", demotes back if previously promoted

Promotion is fully automatic — no manual config. `overrides.json → model_type_per_site` overrides for emergency reset.

### Current promotion status (first run — 2026-05-05)

| Hospital | Ridge MAE | Damping MAE | Δ | Status |
|---|---|---|---|---|
| Angliss Hospital | 14.4 m | 17.8 m | −3.4 m | ridge |
| Box Hill Hospital | 15.4 m | 18.7 m | −3.3 m | ridge |
| Casey Hospital | 9.3 m | 14.3 m | −5.0 m | ridge |
| Dandenong Hospital | 9.4 m | 13.8 m | −4.4 m | ridge |
| Maroondah Hospital | 11.0 m | 15.0 m | −4.0 m | ridge |
| Monash Medical Centre - Clayton | 8.4 m | 10.7 m | −2.3 m | ridge |
| Royal Melbourne Hospital | 17.0 m | 30.3 m | −13.2 m | ridge |

RMH shows the largest gain (−13.2 m MAE) — it has `ctx_source="ESTIMATE"` (no real VAHI benchmarks), meaning the damping model had weak context, while Ridge learns directly from observed patterns.

---

## Tier 2b: Self-Evolving Damping — `evolve_model.py` (Phase 1, retained as fallback)

**Status: Active — runs weekly (Sunday 08:00 AEST local cron). Used when Ridge is not promoted.**

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
