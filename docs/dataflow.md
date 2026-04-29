# Dataflow — Hospital Monitor Pipeline

**Updated:** 2026-04-29

> Companion to [DESIGN.md](DESIGN.md). This page covers *how data moves* — branch structure, file inventory, Pi pipeline steps, and Vercel configuration. For architecture decisions and ML lifecycle, see DESIGN.md.

---

## Branch Responsibilities

| Branch | Purpose | What lives here |
|---|---|---|
| `main` | Source code | All Python scripts, `docs/index.html`, config, CLAUDE.md |
| `data` | Live data output | **Exactly 4 files** (see below) |

The `data` branch is machine-written on every publish cycle. Never commit source code to `data` manually.

---

## Data Branch File Inventory

```
data/
  index.html            ← copied from docs/index.html on main at publish time
  latest.json           ← current 6-hospital outlook (generated_utc, sites[])
  history_timeline.json ← last 24 h of 15-min snapshots (96 buckets × 6 sites)
  vercel.json           ← Vercel cache-control headers + ignoreCommand
```

`latest.json` and `history_timeline.json` are **gitignored on main** — they only exist on the `data` branch.

---

## Pi Pipeline (every 15 min via systemd)

```
run_monitor.sh
  │
  ├── 1. hospital_monitor.py
  │       Scrapes Eastern Health (HTML/JS) and Monash Health (Power BI API)
  │       Appends raw rows → Bronze CSV on /mnt/router_ssd/
  │
  ├── 2. transform_silver.py
  │       Full rebuild of Silver CSV from Bronze + VAHI/AIHW reference files
  │       Adds seasonal benchmarks, triage splits, momentum columns
  │
  └── 3. publish_latest.py --push
          a. Load latest Silver row per hospital
          b. Compute 60-min outlook via predict_next.py
          c. Write /tmp/hospital_monitor_latest.json
          d. Build 24h history timeline via get_history.py
             → write /tmp/history_timeline.json
          e. Clone/fetch data branch into /tmp/publisher
          f. Strip index clean (git rm --cached + git clean)
          g. Copy 4 files into /tmp/publisher
          h. Commit "data: outlook <UTC stamp>"
          i. Force-push → origin/data
```

Operational hours gate: steps a–i only run 06:00–23:00 Melbourne time. Outside those hours `publish_latest.py` exits 0 with no push.

---

## Vercel Configuration

**Production branch:** `data`
Vercel serves `index.html` directly from the data branch root. It never reads `main`.

**Ignored Build Step (`ignoreCommand` in `vercel.json`):**
```
git diff HEAD^ HEAD --name-only | grep -qvE '(latest|history_timeline)\.json$' && exit 1 || exit 0
```
- Pi pushes (JSON-only) → Vercel exits 0 → **build skipped**
- Code pushes (index.html or vercel.json changed) → Vercel exits 1 → **build runs**

This means roughly 95 daily Pi pushes produce zero Vercel builds. Only a code deploy triggers one.

**Cache-Control headers (set in `vercel.json`):**

| File | Header | Effect |
|---|---|---|
| `/latest.json` | `no-cache, no-store, must-revalidate` | Browser always fetches fresh on every dashboard poll |
| `/history_timeline.json` | `public, max-age=900` | 15-min browser cache; stable between pushes |

---

## Browser Fetch Paths

`index.html` uses root-relative URLs served by Vercel — **not** raw GitHub CDN:

```js
const DATA_URL    = "/latest.json";           // no-cache — always fresh
const HISTORY_URL = "/history_timeline.json"; // 15-min cache — stable
```

Using Vercel's served URLs (not `raw.githubusercontent.com`) ensures the `Cache-Control: no-cache` header on `latest.json` is respected. The raw GitHub CDN has a ~5 min cache and ignores custom headers.

---

## Vercel Settings Checklist

Go to **Vercel → Project Settings → Git**:

| Setting | Value |
|---|---|
| Production Branch | `data` |
| Ignored Build Step | *(leave blank — handled by `ignoreCommand` in `vercel.json`)* |
| Root Directory | *(leave blank — files are at the repo root on the `data` branch)* |

---

## SSH Deploy Key

The Pi pushes via a deploy key scoped to this repo only:

```
~/.ssh/hospital_monitor_deploy   (mode 600)
```

The key has **write access to the data branch only**. It cannot push to `main`. `publish_latest.py` uses the system SSH config; no `GIT_SSH_COMMAND` override is needed if `~/.ssh/config` routes `github.com` to this key.
