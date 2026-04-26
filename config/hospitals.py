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
        "parser": "powerbi",

        # ── Set from DevTools Network Inspector ───────────────────────────────
        # endpoint: POST URL — still needed, paste from network trace
        #   e.g. "https://wabi-australia-east-b-api.analysis.windows.net/public/reports/querydata?synchronous=true"
        "endpoint":     None,
        "model_id":     "70ffff0b-d2c6-456d-9c2e-b0f259d3b30d",  # Dataset ID
        "resource_key": "da2bf0d9-bd8a-41b5-a572-ad5c35acdf7e",

        # ── Data-model names — verify from the Commands array in the POST body ─
        "entity":       "CurrentPatients",  # table / entity name in the semantic model
        "hospital_col": "Hospital",         # column used to filter by hospital

        # measures: key used internally → {"property": column/measure name, "function": PBI agg code}
        # PBI aggregation function codes: 0=Sum  1=Avg  4=Min  5=Max
        "measures": {
            "waiting":  {"property": "TotalWaiting",      "function": 0},
            "treating": {"property": "TotalBeingTreated", "function": 0},
            "wait":     {"property": "EstimatedWaitMins", "function": 4},
        },

        # key = literal value used in the WHERE filter (may differ from formal name)
        # value = canonical hospital name matching HOSPITAL_META
        "hospitals": {
            "Casey Hospital":                  "Casey Hospital",
            "Dandenong Hospital":              "Dandenong Hospital",
            "Monash Medical Centre - Clayton": "Monash Medical Centre - Clayton",
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
