# data-class: public-aggregate
"""
Single source of truth for hospital configuration.

To add a hospital:
  1. Add its scrape key + formal name under the correct source in SOURCES.
  2. Add its metadata row to HOSPITAL_META.
  3. If the source URL is new, set its "url" in SOURCES (None = scraper skips it).

Every pipeline script imports from here — no other file needs editing.
"""

# ── Per-source scraper configuration ─────────────────────────────────────────
# "url": None means the dashboard URL is not yet confirmed; scraper skips silently.
# "hospitals": maps the JS key from the dashboard JSON to the canonical hospital name.
SOURCES = {
    "eastern_health": {
        "url": "https://waittime.easternhealth.org.au/",
        "hospitals": {
            "BoxHill":   "Box Hill Hospital",
            "Angliss":   "Angliss Hospital",
            "Maroondah": "Maroondah Hospital",
        },
    },
    "monash_health": {
        "url": None,  # TBC — set once Monash dashboard URL is confirmed
        "hospitals": {
            "Casey":     "Casey Hospital",
            "Dandenong": "Dandenong Hospital",
            "Clayton":   "Monash Medical Centre - Clayton",
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
