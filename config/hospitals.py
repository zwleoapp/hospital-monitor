# data-class: public-aggregate
"""
Single source of truth for hospital configuration.

To add a hospital:
  1. Add an entry to SOURCES under the right source (key = dashboard filter
     value, value = canonical hospital name).
  2. Add a row to HOSPITAL_META (network + aihw_code).
  3. Nothing else needs editing — every pipeline script imports from here.

Parser types
  "html_js"  — page embeds `const patientCounts` + `const predictedWaitMinutes`
               (Eastern Health pattern). Requires "url".
  "powerbi"  — Power BI Embedded batch API. Requires "endpoint", "model_id",
               "resource_key". Fill these from browser DevTools → Network tab:
               filter for the POST to `/querydata`, copy the URL, modelId from
               the request body, and X-PowerBI-ResourceKey from request headers.
               Verify entity/column names from the SemanticQueryDataShapeCommand
               Commands array in that same POST body.
"""

# ── Per-source scraper configuration ─────────────────────────────────────────
SOURCES = {
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
        "col_waiting":  "TotalWaiting",
        "col_treating": "TotalBeingTreated",
        "col_wait_str": "Estimated Time",  # returns "X hr Y min - X hr Y min" string

        # key = Campus filter value  →  value = canonical hospital name
        "hospitals": {
            "Casey":     "Casey Hospital",
            "Clayton":   "Monash Medical Centre - Clayton",
            "Dandenong": "Dandenong Hospital",
        },
    },
}

# ── Per-hospital metadata ─────────────────────────────────────────────────────
# aihw_code: MyHospitals H-code — verify with `python3 scripts/fetch_aihw.py --list-only`
HOSPITAL_META = {
    "Box Hill Hospital":               {"network": "Eastern Health", "aihw_code": "H0330"},
    "Angliss Hospital":                {"network": "Eastern Health", "aihw_code": "H0333"},
    "Maroondah Hospital":              {"network": "Eastern Health", "aihw_code": "H0332"},
    "Casey Hospital":                  {"network": "Monash Health",  "aihw_code": "H0345"},
    "Dandenong Hospital":              {"network": "Monash Health",  "aihw_code": "H0329"},
    "Monash Medical Centre - Clayton": {"network": "Monash Health",  "aihw_code": "H0326"},
}

# ── Derived views (computed — never edit these directly) ──────────────────────
ALL_HOSPITALS    = list(HOSPITAL_META)
HOSPITAL_NETWORK = {h: m["network"]   for h, m in HOSPITAL_META.items()}
HOSPITAL_CODES   = {h: m["aihw_code"] for h, m in HOSPITAL_META.items()}
