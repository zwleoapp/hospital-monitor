[README.md](https://github.com/user-attachments/files/27081262/README.md)
# hospital-monitor

A small, ML-ready data pipeline that scrapes Eastern Health's public ED dashboard (Box Hill, Angliss, Maroondah), engineers temporal and pressure features, and — eventually — predicts wait times.

**Status:** v2.0 — Hybrid Edge-Cloud · **Cost:** $0 · **Maintainer:** [@zwleoapp](https://github.com/zwleoapp)

## What it does

- Every 15 minutes, a Raspberry Pi scrapes the public ED wait-time dashboard.
- Raw rows append to a Bronze CSV (the unalterable source of truth).
- A Silver CSV is rebuilt each cycle with cleaned numeric features (`load_ratio`, `is_holiday`, `is_eve`, `season`, `hour`, `day_of_week`).
- When warranted, the Pi additionally pushes Bronze to Databricks for ML training and historical analysis.

## Repo layout

```
hospital-monitor/
├── README.md           ← you are here
├── scripts/            ← Pi-side ingest + transform (existing)
│   ├── hospital_monitor.py
│   └── transform_split_1.py
├── docs/
│   ├── DESIGN.md       ← architecture, phases, decisions log
│   └── QC.md           ← QC guardrail: per-layer gates, runbook, change-control
├── data/               ← (Phase 1) `latest.json` published to `data` branch only
└── docs/               ← (Phase 1) static GitHub Pages UI (served from /docs on main)
```

## Quick links

- 📐 [Design & SSOT](docs/DESIGN.md) — architecture, edge-cloud hybrid, decisions log
- 🛡️ [QC guardrail](docs/QC.md) — gates, runbook, change-control checklist
- 🌏 Live status: _not published yet — see DESIGN §6_

## Conventions

- **Data is public-aggregate only.** No PII passes through this pipeline. If that ever changes, `docs/DESIGN.md` updates first.
- **Bronze is append-only**, end-to-end, in every phase.
- **All timestamps are UTC.** Local-time features (Victoria) are derived in Silver+.
- **`docs/DESIGN.md` is the contract.** Architectural changes update the doc *before* the code merges.

## License

MIT (suggested — add a `LICENSE` file when ready).
