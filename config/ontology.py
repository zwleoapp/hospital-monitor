# data-class: public-aggregate
"""
config/ontology.py — Typed object model for the hospital monitor pipeline.

This module defines the core object types as Python dataclasses, mimicking the
Palantir Foundry Ontology pattern:

  Object Type       ↔  Python dataclass
  Property          ↔  typed field
  Primary Key       ↔  `name` on Hospital
  Link              ↔  foreign-key field (hospital_name → Hospital)
  Object Set        ↔  filtered list comprehension on HOSPITALS
  Action / Write    ↔  config file edit + pipeline re-run
  Schema Discovery  ↔  schema.json published to data branch

Object hierarchy:
  HealthNetwork (1) ──< Hospital (many)
  Hospital      (1) ──< VahiBenchmark (many, one per quarter)
  Hospital      (1) ──< EDSnapshot (many, one per scrape cycle)

Usage:
  from config.ontology import Hospital, HOSPITALS, get_hospital
"""

from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Literal
import json
import pathlib

Pipeline    = Literal["full", "raw_only"]
ScraperType = Literal["html_js", "powerbi", "html_regex"]
CtxSource   = Literal["VAHI", "AIHW", "ESTIMATE", "NONE"]
Fidelity    = Literal["SYNCED", "API_LEAD_ACTIVE", "PORTAL_STALE_WARNING",
                       "UNKNOWN_FORMAT", "CLOCK_SKEW"]


# ── Object Types ──────────────────────────────────────────────────────────────

@dataclass
class Hospital:
    """
    Primary object — one row in config/hospitals.csv.

    Primary key: name (must be unique across the registry).
    Links: network → HealthNetwork.name
    """
    name:         str
    network:      str
    scraper_type: ScraperType
    pipeline:     Pipeline
    vahi_id:      str  = ""   # VAHI "Organisation Description" when it differs from name
    aihw_id:      str  = ""   # AIHW H-code (e.g. "H0081")
    is_active:    bool = True

    # ── Computed / derived properties ─────────────────────────────────────────
    @property
    def is_full_pipeline(self) -> bool:
        return self.pipeline == "full"

    @property
    def is_raw_only(self) -> bool:
        return self.pipeline == "raw_only"

    @property
    def vahi_lookup_name(self) -> str:
        """Name to use when joining against VAHI source CSVs."""
        return self.vahi_id if self.vahi_id else self.name

    @property
    def has_aihw(self) -> bool:
        return bool(self.aihw_id)

    # ── Validation ────────────────────────────────────────────────────────────
    def validate(self) -> list[str]:
        """Return a list of validation error strings (empty = valid)."""
        errors: list[str] = []
        if not self.name.strip():
            errors.append("name is required")
        if not self.network.strip():
            errors.append(f"{self.name}: network is required")
        if self.scraper_type not in ("html_js", "powerbi", "html_regex"):
            errors.append(f"{self.name}: invalid scraper_type '{self.scraper_type}'")
        if self.pipeline not in ("full", "raw_only"):
            errors.append(f"{self.name}: invalid pipeline '{self.pipeline}'")
        if self.aihw_id and not self.aihw_id.startswith("H"):
            errors.append(f"{self.name}: aihw_id should start with 'H' (got '{self.aihw_id}')")
        return errors


@dataclass
class EDSnapshot:
    """
    One scrape-cycle observation for one full-pipeline hospital.
    Written to latest.json → sites[]. Not persisted as a standalone object
    (Silver CSV holds the historical record).

    Links: site → Hospital.name
    """
    site:               str
    network:            str
    timestamp_utc:      str
    current_wait_min:   float
    predicted_wait_min: float
    wait_momentum:      float
    confidence:         float | None
    confidence_label:   str
    ctx_source:         CtxSource
    fidelity_status:    Fidelity | str
    strain_index:       float | None
    color:              str   # "green" | "amber" | "red"


@dataclass
class VahiBenchmark:
    """
    One quarterly VAHI benchmark row for one hospital.
    Stored in bronze/vahi_history_merged.csv.

    Links: hospital → Hospital.name
    """
    hospital:                       str
    quarter:                        str
    quarter_start_utc:              str
    quarter_end_utc:                str
    wait_p90_mins:                  float
    wait_median_cat123_mins:        float
    wait_median_cat45_mins:         float
    los_pct_under_4hr:              float
    los_pct_over_24hr:              float
    non_admitted_los_pct_under_4hr: float
    source:                         str   # "VAHI" | "VAHI_PROXY"


# ── Registry ──────────────────────────────────────────────────────────────────

def _load_hospitals() -> list[Hospital]:
    """Load and validate the hospital registry from hospitals.csv."""
    import csv
    csv_path = pathlib.Path(__file__).resolve().parent / "hospitals.csv"
    hospitals: list[Hospital] = []
    with open(csv_path, newline="") as fh:
        for row in csv.DictReader(fh):
            h = Hospital(
                name         = row["name"].strip(),
                network      = row["network_type"].strip(),
                scraper_type = row["scraper_type"].strip(),    # type: ignore[arg-type]
                pipeline     = row.get("pipeline", "full").strip(),  # type: ignore[arg-type]
                vahi_id      = row.get("vahi_id",  "").strip(),
                aihw_id      = row.get("aihw_id",  "").strip(),
                is_active    = row.get("is_active", "true").strip().lower() == "true",
            )
            errors = h.validate()
            if errors:
                raise ValueError(f"hospitals.csv validation failed:\n" + "\n".join(errors))
            hospitals.append(h)
    return hospitals


# Module-level registry — validated at import time
HOSPITALS: list[Hospital] = _load_hospitals()

# Convenience lookups
HOSPITAL_MAP:    dict[str, Hospital] = {h.name: h for h in HOSPITALS}
ACTIVE_HOSPITALS = [h for h in HOSPITALS if h.is_active]
FULL_PIPELINE    = [h for h in HOSPITALS if h.is_active and h.is_full_pipeline]
RAW_ONLY         = [h for h in HOSPITALS if h.is_active and h.is_raw_only]


def get_hospital(name: str) -> Hospital | None:
    """Look up a Hospital by formal name. Returns None if not found."""
    return HOSPITAL_MAP.get(name)


# ── Schema generation (for schema.json published to data branch) ──────────────

SCHEMA_VERSION = "2.1"

LATEST_JSON_SCHEMA = {
    "schema_version": SCHEMA_VERSION,
    "description": "ED wait-time outlook — Royal Children's Hospital, Melbourne",
    "object_types": {
        "EDSite": {
            "description": "Full-pipeline hospital: 60-min forecast + VAHI context",
            "primary_key": "site",
            "properties": {
                "site":                  {"type": "string",  "description": "Formal hospital name"},
                "network":               {"type": "string",  "description": "Health network"},
                "latest_obs_utc":        {"type": "string",  "format": "datetime"},
                "current_wait_min":      {"type": "number",  "unit": "minutes"},
                "predicted_wait_min":    {"type": "number",  "unit": "minutes",
                                          "description": "60-min damped momentum forecast"},
                "wait_momentum":         {"type": "number",  "unit": "min/15min-cadence"},
                "confidence":            {"type": "number",  "range": [0, 1]},
                "confidence_label":      {"type": "string",  "enum": ["High", "Moderate", "Low"]},
                "color":                 {"type": "string",  "enum": ["green", "amber", "red"]},
                "strain_index":          {"type": "number",  "description": "predicted_wait / VAHI p90"},
                "ctx_source":            {"type": "string",  "enum": ["VAHI", "AIHW", "ESTIMATE"],
                                          "description": "Quality of benchmark context"},
                "fidelity_status":       {"type": "string",
                                          "enum": ["SYNCED", "API_LEAD_ACTIVE",
                                                   "PORTAL_STALE_WARNING", "UNKNOWN_FORMAT"]},
                "heartbeat_age_mins":    {"type": "number",  "unit": "minutes"},
                "last_updated_display":  {"type": "string",  "description": "Portal timestamp (sidecar)"},
                "paediatric":            {"type": "object",  "nullable": True,
                                          "description": "Paeds sub-object (Monash Casey/Clayton only)"},
                "metadata":              {"type": "object",
                                          "description": "cache_lag, fidelity detail, scrape_timestamp"},
            },
        },
        "StatusSite": {
            "description": "Raw-only hospital: categorical status, no forecast",
            "primary_key": "site",
            "properties": {
                "site":               {"type": "string"},
                "busy_label":         {"type": "string",  "nullable": True,
                                       "enum": ["Normal", "Very Busy", "Extremely Busy"]},
                "busy_index":         {"type": "number",  "nullable": True},
                "fidelity_status":    {"type": "string"},
                "last_portal_update": {"type": "string"},
                "heartbeat_age_mins": {"type": "number"},
            },
        },
    },
    "payload": {
        "generated_utc":     {"type": "string", "format": "datetime"},
        "schema_version":    {"type": "string"},
        "horizon_min":       {"type": "integer", "value": 60},
        "vahi_p90_all_mins": {"type": "number"},
        "vahi_qly_label":    {"type": "string"},
        "sites":             {"type": "array", "items": "$EDSite"},
        "status_sites":      {"type": "array", "items": "$StatusSite"},
    },
}


def write_schema(path: pathlib.Path) -> None:
    """Write the Gold API schema to a JSON file (for the data branch)."""
    path.write_text(json.dumps(LATEST_JSON_SCHEMA, indent=2))
