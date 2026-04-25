# QC — Quality Control Guardrail

**Version:** 1.0 · **Updated:** 2026-04-25 · **Companion to:** [DESIGN.md](DESIGN.md)

> Each gate is either a **merge-blocker** or a **runtime check**. Phase tags indicate when a gate becomes active. P1 = applies on the Pi today. P2 = adds when Databricks comes online.

---

## 1. How to use this doc

- Before merging any change to a script in this repo, run the **change-control checklist** (§5) and tick or waive each item in the PR description.
- When something breaks, jump to the **runbook** (§4) — symptom-first.
- When you add a new feature column or a new data source, add a row to the **gates** in §2 and §3 in the same PR.

---

## 2. Bronze gates (raw capture)

| Gate | Phase | Check | Pass criterion |
|---|---|---|---|
| Schema contract | P1 | Validate scraped JSON against `config/bronze_schema_v*.json` before append. | Zero rows appended on mismatch; rejected payload written to `bronze/_rejected/` with error code. |
| Freshness | P1 | Newest Bronze row timestamp within last 35 minutes. | `bronze_freshness_seconds < 2100`. |
| Append integrity | P1 | Row count strictly increasing across runs. | `wc -l` before run < `wc -l` after run, every run. |
| Retry policy | P1 | Network errors (incl. `Errno 16` SSD lock) retried with exponential backoff. | Max 5 attempts: 1s, 2s, 4s, 8s, 16s. On exhaustion → `bronze/_dlq/` and failure metric. |
| Timezone | P1 | All Bronze timestamps stored as UTC ISO-8601 with explicit offset. | Regex on every appended row: ends with `Z` or `±HH:MM`. |
| Data class | P1 | Top-of-file `# data-class: public-aggregate` stamp present and unchanged. | CI grep gate fails the merge if line is missing or modified without privacy review. |
| Volume hand-off | P2 | Pi → Unity Catalog Volume push is idempotent. | Re-running yesterday's push produces zero new files. |

---

## 3. Silver / Gold gates

| Gate | Phase | Check | Pass criterion |
|---|---|---|---|
| Idempotency | P1 | Re-running Silver on the same Bronze produces a byte-identical CSV. | `diff(silver_run_1.csv, silver_run_2.csv)` is empty. |
| Schema contract | P1 | Output schema validated against `config/silver_schema_v*.json`. | All declared columns present, types match, no extras. |
| Value bounds | P1 | `load_ratio ∈ [0, 5]`; `wait_minutes ∈ [0, 720]`; `treating ≥ 0`; `waiting ≥ 0`. | Out-of-bounds rows quarantined, not silently clamped. |
| Null-rate | P1 | Null rate per column ≤ 1% over rolling 7-day window. | Heartbeat metric `silver_null_rate{col=...} ≤ 0.01`. |
| Holiday calendar | P1 | `is_holiday` derived from `config/holidays_vic_<yyyy>.yaml`; pinned per year. | Audit script compares flagged dates to the YAML; mismatch fails the gate. |
| DST integrity | P1 | No duplicate or missing 30-minute slot at AEST/AEDT boundary. | `scripts/qc/dst_audit.py` reports zero anomalies for the last DST event. |
| Snapshot immutability | P2 (Gold) | Each training snapshot is content-hashed; never overwritten. | SHA-256 of snapshot recorded in `gold/_manifest.csv` with model run-ID. |
| Holdout isolation | P2 (Gold) | Frozen 14-day holdout never used for training. | Training script rejects rows whose date ≥ holdout-start. |
| Leakage | P1 (now) / P2 (formal) | No feature derived from the future. | Unit test asserts every feature is a function only of `t' ≤ t`. |
| Distribution monitor | P2 (Gold) | PSI between current month and training window < 0.2. | CI emits PSI per feature; > 0.2 raises a drift ticket. |

---

## 4. Runbook (failure → fix)

Triage table for on-call. Symptom-first so the entry point is what you observe, not what you hypothesise.

| Failure | Phase | Symptom | Diagnostic | Remediation |
|---|---|---|---|---|
| Ingestion (HTTP) | P1 | No new rows in Bronze; service in `failed`. | `journalctl -u hospital_monitor.service -n 50` | If 4xx/5xx: check `curl_cffi` UA + TLS profile. If JSON shape changed: bump `bronze_schema_v*` and update extractor; replay last 24h from raw cache. |
| Silent drift | P1 | Rows present, `load_ratio` NaN or out-of-bounds. | `python3 scripts/qc/check_silver_bounds.py --window 24h` | Quarantine offending Bronze rows; raise schema version; re-run Silver. |
| SSD / mount | P1 | `Errno 16` (Device Busy) repeating in journal. | `ls -l /mnt/router_ssd/`; `mount \| grep router_ssd` | Confirm retry caught it; if permanent, remount; if remount fails, fall back to `/var/lib/hospital_monitor/_local_dlq`. |
| Transformation | P1 | Silver missing columns / row drop. | `python3 scripts/transform_split_1.py --dry-run` | Compare to `silver_schema_v*`. If intentional: bump version + migration note. If regression: revert. |
| Holiday calendar | P1 | `is_holiday` wrong on a known public holiday. | `python3 scripts/qc/audit_holidays.py --year <yyyy> --state VIC` | Update `config/holidays_vic_<yyyy>.yaml`; rerun Silver for affected window. |
| DST boundary | P1 | Duplicate / missing 30-min slot near AEST/AEDT switch. | `python3 scripts/qc/dst_audit.py` | All Bronze timestamps must be UTC; if local time leaked in, patch extractor and replay the day. |
| Pipeline drift | P1 | Heartbeat row count deviates >10% from rolling 7-day mean. | `python3 scripts/qc/heartbeat.py --report` | If hospital redefined triage categories, freeze model and open ticket to retrain on post-change data. |
| Storage failure | P1 | SSD unmounted or read-only. | `dmesg \| tail`; `smartctl -a /dev/sda` | Switch service to local fallback path; restore Bronze from latest nightly tarball; open hardware ticket. |
| Publisher failure | P1 | `latest.json` not refreshing on the static page. | `git -C /tmp/publisher log -1 origin/data` | Re-run `publish_latest.py`; check deploy-key SSH is unlocked; if rate-limited, back off. |
| Cloud landing | P2 | Files not appearing in UC Volume. | `databricks fs ls dbfs:/Volumes/.../bronze/` | Verify PAT not expired; check egress connectivity; replay from Pi-side staging dir. |
| Delta append | P2 | Bronze Delta row count diverges from Volume file count. | Notebook: count rows per landing file vs Delta. | Run dedup MERGE; investigate any duplicated file landings. |

---

## 5. Change-control checklist (paste into every PR)

```
- [ ] Schema version bumped if shape changed
- [ ] Idempotency test passes (Silver byte-identical on rerun)
- [ ] Bounds test passes (load_ratio, wait_minutes within bounds)
- [ ] Replay test on a 7-day window passes
- [ ] Heartbeat metric still emits
- [ ] Runbook updated if a new failure mode is now possible
- [ ] data-class stamp unchanged (or privacy-reviewed)
- [ ] DESIGN.md decisions log updated if the change is architectural
```

Tick each box or write a one-line rationale for waiving it.

---

## 6. Operational appendix

### 6.1 One-line health probe (Phase 1)

```bash
python3 scripts/qc/healthcheck.py --layers bronze,silver --window 24h
```

Returns a single PASS/FAIL with the failing gate name. Drop in a cron or run before any post-mortem.

### 6.2 Manual Silver replay

```bash
python3 scripts/transform_split_1.py \
  --bronze /mnt/router_ssd/.../eastern_hospital.csv \
  --since 2026-04-20 --until 2026-04-21 \
  --out /tmp/silver_replay.csv
```

### 6.3 Nightly Bronze backup

```bash
tar czf /mnt/usb_backup/bronze_$(date +%F).tar.gz \
  /mnt/router_ssd/.../eastern_hospital.csv
```

### 6.4 Doc maintenance

- Re-issue this doc at every sprint close. Bump the version in the header.
- Any new failure mode discovered in production must produce a runbook row in the same PR that fixes it.
- Any new Silver feature must add a row to the Silver value-bounds gate.
