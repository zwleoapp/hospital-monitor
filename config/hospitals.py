# data-class: public-aggregate
"""
Single source of truth for hospital configuration.

Hospitals are registered in config/hospitals.csv (columns: name, network_type,
scraper_type, vahi_id, aihw_id, is_active). This module loads that CSV at
import time and rebuilds all derived views — adding a new hospital requires
only a new row in the CSV.

Scraper connection details (URLs, Power BI keys, entity/column names) live in
SOURCES below: they are per-source-group, not per-hospital, and are too
complex for a flat CSV row.
"""

import csv
import pathlib

_CSV = pathlib.Path(__file__).resolve().parent / "hospitals.csv"

# ── Registry (all hospitals, active and inactive) ─────────────────────────────
with open(_CSV, newline="") as _f:
    REGISTRY: list[dict] = list(csv.DictReader(_f))

ACTIVE_HOSPITALS: list[str] = [
    r["name"] for r in REGISTRY if r["is_active"].strip().lower() == "true"
]

# ── Per-hospital metadata (all hospitals — used for historical joins) ──────────
HOSPITAL_META: dict[str, dict] = {
    r["name"]: {"network": r["network_type"], "aihw_code": r["aihw_id"]}
    for r in REGISTRY
}

ALL_HOSPITALS    = list(HOSPITAL_META)
HOSPITAL_NETWORK = {h: m["network"]   for h, m in HOSPITAL_META.items()}
HOSPITAL_CODES   = {h: m["aihw_code"] for h, m in HOSPITAL_META.items()}


# ── Per-source scraper configuration ─────────────────────────────────────────
# Parser types:
#   "html_js"  — page embeds `const patientCounts` + `const predictedWaitMinutes`
#               (Eastern Health pattern). Requires "url".
#   "powerbi"  — Power BI Embedded batch API. Requires "endpoint", "model_id",
#               "resource_key". Fill from browser DevTools → Network tab:
#               filter for the POST to /querydata; copy the URL, modelId from
#               the request body, and X-PowerBI-ResourceKey from request headers.
#               Verify entity/column names from SemanticQueryDataShapeCommand.
_SOURCES_RAW: dict = {
    "eastern_health": {
        "parser": "html_js",
        "url": "https://waittime.easternhealth.org.au/",
        # key = JS object key in patientCounts/predictedWaitMinutes
        "hospitals": {
            "BoxHill":   "Box Hill Hospital",
            "Angliss":   "Angliss Hospital",
            "Maroondah": "Maroondah Hospital",
        },
    },

    "monash_health": {
        "parser":       "powerbi",
        "endpoint":     "https://wabi-australia-southeast-api.analysis.windows.net/public/reports/querydata?synchronous=true",
        "model_id":     2556929,           # integer confirmed from /conceptualschema
        "resource_key": "da2bf0d9-bd8a-41b5-a572-ad5c35acdf7e",

        # ── Data model (confirmed from /conceptualschema + live probe) ────────
        "entity":       "CurrentPatients",
        "hospital_col": "Campus",          # WHERE Campus = '{filter_key}'
        # Each campus has two rows (Adult / Paeds). We want Adult only.
        "group_col":    "AdultPaed",
        "group_target": "Adult",
        # Columns to SELECT in the grouped query (order defines C array indices):
        # [group_col, col_waiting, col_treating, col_wait_str]
        "col_waiting":     "TotalWaiting",
        "col_treating":    "TotalBeingTreated",
        "col_wait_str":    "Estimated Time",     # returns "X hr Y min - X hr Y min"
        "col_last_updated":"LastUpdatedDisplay", # returns "Last Updated: DD Mmm YY HH:MM"

        # key = Campus filter value  →  value = canonical hospital name
        "hospitals": {
            "Casey":     "Casey Hospital",
            "Clayton":   "Monash Medical Centre - Clayton",
            "Dandenong": "Dandenong Hospital",
        },
    },
}

# Filter each source's hospital list to only active entries from the registry.
# Disabling a hospital in hospitals.csv removes it from scraping automatically.
_active_set = set(ACTIVE_HOSPITALS)
SOURCES: dict = {}
for _key, _cfg in _SOURCES_RAW.items():
    _cfg = dict(_cfg)
    _cfg["hospitals"] = {k: v for k, v in _cfg["hospitals"].items() if v in _active_set}
    if _cfg["hospitals"]:   # skip source entirely if all its hospitals are deactivated
        SOURCES[_key] = _cfg
