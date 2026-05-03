# Clinical Pulse Architecture

**Updated:** 2026-05-03

> Records **architectural decisions** behind the scraper design — why each source uses the parser it does, what the Truth Gap means, and how the dual-timestamp model preserves data integrity. For the full pipeline dataflow (file schemas, pipeline steps, Vercel config), see [dataflow.md](dataflow.md).

---

## Decision: API-only over headless browser (Monash Health)

Early prototypes for Monash Health tried headless scraping of the Power BI iframe. All failed:

| Attempt | Problem |
|---|---|
| requests-html + pyppeteer | Architecture mismatch — targets Chromium directly, not the requests session |
| `chromium --dump-dom` | Does not execute iframe JavaScript; DOM is static |
| Selenium | Too heavy and fragile on Raspberry Pi hardware |

The Power BI **query API** (`/public/reports/querydata`) is publicly accessible with the `X-PowerBI-ResourceKey` header (embedded in the public embed URL). It returns **live database state**, bypassing any visual-layer cache.

**Accepted consequence:** the API may return a more current count than the browser visual shows. This is documented as "API Lead Active" in the fidelity model below.

---

## Scraper Type Decisions per Source

### Eastern Health — `html_js`

Eastern Health (`waittime.easternhealth.org.au`) embeds patient counts and predicted wait ranges as JavaScript constants:

```javascript
const patientCounts        = { BoxHill: {waiting, beingTreated}, ... }
const predictedWaitMinutes = { BoxHill: {min, max}, ... }
```

One HTTP fetch covers all three campuses. JS variable names and field names are configured in `hospitals.json → js_data_vars` and `js_field_map` — no hospital-specific strings in scraper code.

**Portal timestamp:** Native "Last Updated" from page HTML; falls back to HTTP `Date` header (`~HH:MM`, approximate). **Direct** measure of hospital publishing latency.

### Monash Health — `powerbi`

14 separate HTTP POSTs per scrape cycle (Casey: 5, Clayton: 5, Dandenong: 4). Per-campus per-cohort visual IDs in `hospitals.json → visual_ids`. Adult and Paeds cohorts fetched separately for Casey and Clayton.

**Portal timestamp:** `LastUpdatedDisplay` from DSR response — report-level value (same for all three campuses). Reflects Power BI dataset refresh, **not** hospital system update time. **Indirect** measure with one caching layer between hospital data and our scraper.

### Royal Melbourne Hospital — `html_regex`

RMH (`thermh.org.au`) renders data in plain HTML. Patient count and label are in adjacent sibling `<div>` elements. Patterns in `hospitals.json → regex_patterns` extract each field. Portal timestamp ("5:45pm on 03 May 2026") is normalized to 24h `~HH:MM` so the UI and cache lag both work correctly.

**Portal timestamp:** **Direct** measure — reflects when RMH last updated their public page. Same semantics as Eastern Health.

### Royal Children's Hospital — `html_regex` (raw_only)

RCH (`rch.org.au`) publishes a 3-level categorical status via image filename (`ED-wait-times-graph1/2/3.png`). A hidden white-text number provides a numeric busy index. **No patient counts or wait times are published.**

`pipeline=raw_only` in `hospitals.csv` — writes to `bronze_raw_scrapes.csv` only. Appears in UI as a status card (`status_sites[]`), not in main `sites[]`. Label mapping is config-driven via `status_map` in `hospitals.json`:

```json
{"1": "Normal", "2": "Very Busy", "3": "Extremely Busy"}
```

---

## Dual-Timestamp Model (Truth Gap)

Every scrape event captures two timestamps:

| Field | Set by | Measures |
|---|---|---|
| `scrape_timestamp_utc` | Pi clock | **Scrape Truth** — when we asked |
| `reported_timestamp_str` | Portal | **Portal Truth** — when portal claims it last updated |

`cache_lag_minutes = scrape_timestamp − reported_timestamp`

| Source | What lag measures |
|---|---|
| Eastern Health | Hospital page publishing latency (direct) |
| Monash Health | Power BI dataset refresh latency (indirect — PBI caching layer) |
| RMH | Hospital page publishing latency (direct) |
| RCH | Time since page's "Last updated" stamp. Often PORTAL_STALE_WARNING due to infrequent updates. |

**Fidelity statuses** (thresholds in `config/ui_config.json`):

| Status | Lag | Meaning |
|---|---|---|
| `SYNCED` | < 15 min | Portal recently refreshed |
| `API_LEAD_ACTIVE` | 15–60 min | Our data is fresher than portal display (normal for Monash) |
| `PORTAL_STALE_WARNING` | > 60 min | Portal significantly behind; forecast may use stale inputs |
| `UNKNOWN_FORMAT` | — | Timestamp couldn't be parsed; lag treated as 0 |

`cache_lag_minutes` is a **diagnostic column only** — never an ML model input.

---

## Bronze Raw Schema

`bronze_raw_scrapes.csv` — full audit trail per scrape event, all hospitals and cohorts.

| Column | Notes |
|---|---|
| `site` | Hospital formal name |
| `scrape_timestamp_utc` | Scrape Truth |
| `location_timestamp` | Melbourne local (display only) |
| `reported_timestamp_str` | Portal Truth — raw extracted string |
| `reported_waiting` | Count as shown on portal |
| `reported_wait_str` | Wait range string or status label (e.g. "Extremely Busy") |
| `raw_query_waiting` | API/scrape waiting count |
| `raw_query_treating` | API/scrape treating count |
| `raw_query_max_wait` | Max wait minutes |
| `cohort` | "Adult" / "Paeds" / "All" |
| `cache_lag_minutes` | Computed lag (diagnostic) |
| `fidelity_status` | SYNCED / API_LEAD_ACTIVE / PORTAL_STALE_WARNING / UNKNOWN_FORMAT |

**Paeds:** Monash Casey + Clayton scrape both Adult and Paeds rows. Paeds rows go to Bronze Raw only; `publish_latest.py` attaches them as `paediatric{}` on the relevant sites in `latest.json`.

---

## Adding a New Hospital — No Python Changes Required

1. Add source entry to `config/hospitals.json` (parser, URL, patterns/credentials, optional `status_map`, optional `ctx_defaults`)
2. Add row to `config/hospitals.csv` (name, network_type, scraper_type, aihw_id, is_active, pipeline)
3. If `pipeline=raw_only`: hospital appears in `status_sites[]` only
4. If no VAHI data yet: add `ctx_defaults` to `hospitals.json` (Silver uses `ctx_source="ESTIMATE"`)
