# data-class: public-aggregate
"""
Single source of truth for hospital configuration.

Two config files combine here:
  hospitals.csv  — per-hospital registry (name, network, codes, is_active).
                   Adding a new hospital requires only a new row.
  hospitals.json — per-source-group connection details (URLs, Power BI keys,
                   entity/column names).  Edit to update credentials or add a
                   new scraper type without touching Python.

SOURCES is the merged, active-filtered view consumed by hospital_monitor.py.
"""

import csv
import json
import pathlib

_CSV  = pathlib.Path(__file__).resolve().parent / "hospitals.csv"
_JSON = pathlib.Path(__file__).resolve().parent / "hospitals.json"

# ── Registry (all hospitals, active and inactive) ─────────────────────────────
with open(_CSV, newline="") as _f:
    REGISTRY: list[dict] = list(csv.DictReader(_f))

ACTIVE_HOSPITALS: list[str] = [
    r["name"] for r in REGISTRY if r["is_active"].strip().lower() == "true"
]

# Hospitals whose pipeline column is "raw_only": scraper writes Bronze Raw only —
# they skip Bronze CSV → Silver → predict pipeline and appear via status_sites.
RAW_ONLY_HOSPITALS: set[str] = {
    r["name"] for r in REGISTRY
    if r.get("pipeline", "full").strip() == "raw_only"
    and r["is_active"].strip().lower() == "true"
}

# ── Per-hospital metadata (all hospitals — used for historical joins) ──────────
HOSPITAL_META: dict[str, dict] = {
    r["name"]: {"network": r["network_type"], "aihw_code": r["aihw_id"],
                "vahi_id": r.get("vahi_id", "")}
    for r in REGISTRY
}

# Maps VAHI "Organisation Description" → our formal hospital name.
# Used by fetch_vahi.py so VAHI source CSVs can use long-form names while
# the pipeline uses short formal names throughout.
VAHI_NAME_MAP: dict[str, str] = {
    r.get("vahi_id", "").strip(): r["name"]
    for r in REGISTRY
    if r.get("vahi_id", "").strip() and r.get("vahi_id", "").strip() != r["name"]
}

ALL_HOSPITALS    = list(HOSPITAL_META)
HOSPITAL_NETWORK = {h: m["network"]   for h, m in HOSPITAL_META.items()}
HOSPITAL_CODES   = {h: m["aihw_code"] for h, m in HOSPITAL_META.items()}


# ── Per-source scraper configuration (loaded from hospitals.json) ─────────────
# Parser types:
#   "html_js"    — page embeds `const patientCounts` + `const predictedWaitMinutes`
#                  (Eastern Health pattern). Requires "url".
#   "powerbi"    — Power BI Embedded batch API. Requires "endpoint", "model_id",
#                  "resource_key". Fill from browser DevTools → Network tab.
#   "html_regex" — Standard HTML page with regex_patterns configured in hospitals.json.
#                  If "wait_time" pattern present → full Bronze CSV + Silver pipeline.
#                  If only "busy_index" → Bronze Raw only (set pipeline=raw_only in CSV).
#
# To add a new source: add an entry to hospitals.json and rows to hospitals.csv.
# No Python changes required.
with open(_JSON) as _jf:
    _full_json: dict = json.load(_jf)

# Source entries must have a "hospitals" sub-key; everything else (e.g. vahi_benchmarks) is metadata.
_SOURCES_RAW: dict = {
    k: v for k, v in _full_json.items()
    if not k.startswith("_") and isinstance(v, dict) and "hospitals" in v
}

VAHI_BENCHMARKS: dict = _full_json.get("vahi_benchmarks", {})

# Filter each source's hospital list to only active entries from the registry.
# Disabling a hospital in hospitals.csv removes it from scraping automatically.
_active_set = set(ACTIVE_HOSPITALS)
SOURCES: dict = {}
for _key, _cfg in _SOURCES_RAW.items():
    _cfg = dict(_cfg)
    _cfg["hospitals"] = {k: v for k, v in _cfg["hospitals"].items() if v in _active_set}
    if _cfg["hospitals"]:   # skip source entirely if all its hospitals are deactivated
        SOURCES[_key] = _cfg
