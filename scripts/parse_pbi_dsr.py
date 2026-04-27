# data-class: public-aggregate
"""
parse_pbi_dsr.py — Extract wait-time metrics from a raw Power BI DSR batch response.

Navigation path (per Power BI batch API):
  results[i] → result → data → dsr → DS[0] → PH[0] → DM0

Column names come from one of two places (tried in order):
  1. result → data → descriptor → Select   (rich metadata; preferred)
  2. DM0[0] → S schema                     (aliases only; fallback)

T-type constants (from S schema entry {"N":"M0","T":4}):
  T:1  Text / String   — the time-range display, e.g. "2 hr 30 min - 4 hr 45 min"
  T:2  Decimal
  T:3  Fixed Decimal
  T:4  Whole Number    — integer patient counts (TotalWaiting, TotalBeingTreated)
  T:5  Date/Time (string repr)
  T:6  Date
  T:7  Time
  T:8  DateTime UTC
  T:9  Boolean

Row value formats handled:
  • M-key  — DM0[0] = {"S":[{"N":"M0","T":4}], "M0":23}
             (single-row measure query; T on the same row)
  • C-array — DM0[i] = {"S":[...], "C":[...], "R": bitmask}
              (grouped/tabular query; delta + repeat-encoded; T on first row)

descriptor.Select alias detection:
  The real Power BI response uses Value as the DSR alias ("M0") and Name as the
  human label ("Sum(CurrentPatients.TotalWaiting)").  Some query builders write it
  the other way round (Name="G1", Value="TotalWaiting").  We detect the alias by
  matching the short [MG]\\d+ pattern so both conventions work automatically.

Classification (keyword matching on the resolved column name):
  "estimated_time"       — name contains "EstimatedTime" or "Estimated" + "Time"
  "total_waiting"        — name contains "TotalWaiting"
  "total_treating"       — name contains "TotalBeingTreated", "Treating", "Treated"
  "last_updated_display" — name contains "LastUpdated"
  "timestamp"            — result → data → timestamp  (ISO string)

Usage (CLI):
  python3 scripts/parse_pbi_dsr.py response.json
  python3 scripts/parse_pbi_dsr.py response.json --all-rows
  python3 scripts/parse_pbi_dsr.py response.json --raw

Usage (module):
  from parse_pbi_dsr import parse_response
  results = parse_response(json.loads(raw_text))
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from typing import Any

# ── T-type coercion ───────────────────────────────────────────────────────────

# Maps S-schema T value → Python type constructor.
# T:1=text, T:4=integer are the two types confirmed in the real Monash response.
_PBI_T_COERCE: dict[int, type] = {
    1: str,    # Text
    2: float,  # Decimal
    3: float,  # Fixed Decimal
    4: int,    # Whole Number (patient counts)
    5: str,    # DateTime string
    6: str,    # Date string
    7: str,    # Time string
    8: str,    # DateTime UTC string
    9: bool,   # Boolean
}


def _coerce_by_t(val: Any, t: int | None) -> Any:
    """Apply the Power BI T-type coercion to a raw DSR value."""
    if val is None or t is None:
        return val
    fn = _PBI_T_COERCE.get(t)
    if fn is None:
        return val
    try:
        return fn(val)
    except (ValueError, TypeError):
        return val


# ── Name resolution ───────────────────────────────────────────────────────────

# A DSR alias is a short token like "M0", "G1", "G3".
_ALIAS_RE = re.compile(r'^[MGC]\d+$')


def _descriptor_name_map(result_data: dict) -> dict[str, str]:
    """
    Build {dsr_alias → human_label} from result.data.descriptor.Select.

    The real Power BI batch API returns:
      {"Kind":2, "Value":"M0", "Name":"Sum(CurrentPatients.TotalWaiting)"}
    where Value is the DSR alias and Name is the human label.

    Some query builders write it the other way (Name="G1", Value="TotalWaiting").
    We detect the alias field by matching the [MG]\\d+ pattern so both work.

    Returns e.g. {"M0": "Sum(CurrentPatients.TotalWaiting)"}
                 {"G1": "TotalWaiting", "G3": "Estimated Time"}
    """
    try:
        entries = result_data["descriptor"]["Select"]
    except (KeyError, TypeError):
        return {}

    name_map: dict[str, str] = {}
    for e in entries:
        val  = str(e.get("Value") or "")
        name = str(e.get("Name")  or "")

        if _ALIAS_RE.match(val):
            alias, label = val, name        # real PBI API: Value=alias, Name=label
        elif _ALIAS_RE.match(name):
            alias, label = name, val        # query-builder convention: Name=alias, Value=label
        else:
            alias, label = name, val or name

        if alias:
            name_map[alias] = label or alias

    return name_map


def _schema_name_map(schema: list[dict]) -> dict[str, str]:
    """
    Build {alias → alias} from DM0[0].S schema (fallback when no descriptor).

    S schema carries only aliases and T-type info — no human-readable names.
    Identity map; keyword matching still works for meaningful alias names.
    """
    return {
        col.get("N") or col.get("Name") or f"G{i}": col.get("N") or f"G{i}"
        for i, col in enumerate(schema)
    }


# ── Row extraction ─────────────────────────────────────────────────────────────

def _extract_m_key_row(
    dm0_row: dict,
    name_map: dict[str, str],
) -> dict[str, Any]:
    """
    Extract values from a flat M-key row.

    Real PBI format (single-row measure queries):
      {"S": [{"N": "M0", "T": 4}], "M0": 23}

    The S schema lives on the same row as the M-keys (unlike C-array format
    where S only appears on DM0[0]).  T-type is read per column for coercion.

    name_map is keyed on the DSR alias ("M0", not "G0") so we look up the
    column name as name_map.get("M0", "M0").
    """
    schema: list[dict] = dm0_row.get("S", [])
    row: dict[str, Any] = {}

    for i in range(32):
        m_key = f"M{i}"
        if m_key not in dm0_row:
            break
        val = dm0_row[m_key]

        # T-type coercion using the inline S schema
        t = schema[i].get("T") if i < len(schema) else None
        val = _coerce_by_t(val, t)

        # name_map is keyed on the alias ("M0"); fall back to the key itself
        col_name = name_map.get(m_key, m_key)
        row[col_name] = val

    return row


def _extract_c_array_rows(
    dsr: dict,
    name_map: dict[str, str],
) -> list[dict[str, Any]]:
    """
    Extract all rows from the C-array / S-schema DSR format.

    Power BI delta + repeat compression:
      S   — column schema on first row only: {"N":"G0","T":1} etc.
      C   — values for *new* columns only (omitted columns are unchanged)
      R   — bitmask: bit i set → column i repeats the previous row's value
      DS[0].ValueDicts — {dictName: [values]} for DN dict-encoded string columns
    """
    try:
        ds0    = dsr["DS"][0]
        rows   = ds0["PH"][0]["DM0"]
        vdicts = ds0.get("ValueDicts", {})
        schema = rows[0]["S"]
    except (KeyError, IndexError, TypeError):
        return []

    n_cols = len(schema)
    schema_aliases = [col.get("N") or col.get("Name") or f"G{i}"
                      for i, col in enumerate(schema)]
    schema_t       = [col.get("T") for col in schema]

    def _decode(val: Any, col_idx: int) -> Any:
        if col_idx >= n_cols or val is None:
            return val
        dn = schema[col_idx].get("DN")
        if dn and isinstance(val, int):
            bucket = vdicts.get(dn, [])
            return bucket[val] if val < len(bucket) else val
        return _coerce_by_t(val, schema_t[col_idx])

    prev_c: list[Any] = [None] * n_cols
    result_rows: list[dict[str, Any]] = []

    for row in rows:
        c_raw  = row.get("C", [])
        r_mask = row.get("R", 0)

        full_c = list(prev_c)
        raw_idx = 0
        for col_idx in range(n_cols):
            if r_mask & (1 << col_idx):
                pass
            else:
                if raw_idx < len(c_raw):
                    full_c[col_idx] = c_raw[raw_idx]
                raw_idx += 1
        prev_c = list(full_c)

        row_dict: dict[str, Any] = {}
        for col_idx, alias in enumerate(schema_aliases):
            col_name = name_map.get(alias, alias)
            row_dict[col_name] = _decode(full_c[col_idx], col_idx)
        result_rows.append(row_dict)

    return result_rows


# ── Classification ─────────────────────────────────────────────────────────────

def _classify(raw_row: dict[str, Any]) -> dict[str, Any]:
    """
    Map resolved column names → standardised metric keys by keyword matching.

    Works on both short names ("TotalWaiting") and full DAX expressions
    ("Sum(CurrentPatients.TotalWaiting)") — substring match on the normalised
    lower-case name with spaces and punctuation stripped.

    Returns any subset of:
      estimated_time        str   — "2 hr 30 min - 4 hr 45 min"
      total_waiting         int   — patients waiting (T:4)
      total_treating        int   — patients being treated (T:4)
      last_updated_display  str   — "Last Updated: 27 Apr 26 19:17" (T:1)
    """
    out: dict[str, Any] = {}
    for col_name, val in raw_row.items():
        # Normalise: lowercase, strip spaces + punctuation so DAX expressions
        # like "Sum(CurrentPatients.TotalWaiting)" match the keyword "totalwaiting"
        key = re.sub(r'[^a-z0-9]', '', col_name.lower())

        if "estimatedtime" in key or ("estimated" in key and "time" in key):
            out["estimated_time"] = str(val).strip() if val is not None else None

        elif "totalwaiting" in key:
            try:
                out["total_waiting"] = int(val) if val is not None else None
            except (ValueError, TypeError):
                out["total_waiting"] = val

        elif "totalbeingtreated" in key or "treatingcount" in key:
            try:
                out["total_treating"] = int(val) if val is not None else None
            except (ValueError, TypeError):
                out["total_treating"] = val

        elif "lastupdated" in key:
            out["last_updated_display"] = str(val).strip() if val is not None else None

    return out


# ── Public API ─────────────────────────────────────────────────────────────────

def parse_response(
    response: dict,
    all_rows: bool = False,
    include_raw: bool = False,
) -> list[dict]:
    """
    Parse a Power BI batch API response dict.

    Parameters
    ----------
    response    : parsed JSON from the PBI batch endpoint
    all_rows    : if True, return every decoded row; otherwise only the first
    include_raw : if True, attach "raw_rows" list to each result record

    Returns
    -------
    List of dicts, one per result entry:
      {
        "query_id":            str | None,
        "timestamp":           str | None,   # from result.data.timestamp
        "estimated_time":      str | None,
        "total_waiting":       int | None,
        "total_treating":      int | None,
        "last_updated_display":str | None,
        "raw_rows":            list | None,  # present only when include_raw=True
      }
    """
    raw_results = response.get("results", [])
    output = []

    for entry in raw_results:
        query_id   = entry.get("jobId") or entry.get("QueryId")
        result_obj = entry.get("result", {})
        data       = result_obj.get("data", {})
        dsr        = data.get("dsr", {})
        timestamp  = data.get("timestamp")

        name_map = _descriptor_name_map(data)

        try:
            dm0       = dsr["DS"][0]["PH"][0]["DM0"]
            first_row = dm0[0] if dm0 else {}
        except (KeyError, IndexError, TypeError):
            first_row = {}

        # M-key format: row has Mx keys directly (single-row measure queries)
        has_m_keys = any(k.startswith("M") and k[1:].isdigit() for k in first_row)

        if has_m_keys:
            if not name_map:
                name_map = {f"M{i}": f"M{i}" for i in range(32)}
            raw_rows = [_extract_m_key_row(first_row, name_map)]
        else:
            if not name_map:
                try:
                    schema   = dsr["DS"][0]["PH"][0]["DM0"][0]["S"]
                    name_map = _schema_name_map(schema)
                except (KeyError, IndexError, TypeError):
                    name_map = {}
            raw_rows = _extract_c_array_rows(dsr, name_map)

        target_rows = raw_rows if all_rows else raw_rows[:1]
        classified  = [_classify(r) for r in target_rows]

        merged: dict[str, Any] = {}
        for c in classified:
            for k, v in c.items():
                if k not in merged:
                    merged[k] = v

        record: dict[str, Any] = {
            "query_id":             query_id,
            "timestamp":            timestamp,
            "estimated_time":       merged.get("estimated_time"),
            "total_waiting":        merged.get("total_waiting"),
            "total_treating":       merged.get("total_treating"),
            "last_updated_display": merged.get("last_updated_display"),
        }
        if include_raw:
            record["raw_rows"] = raw_rows

        output.append(record)

    return output


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract wait-time metrics from a Power BI DSR response JSON file."
    )
    parser.add_argument("file", type=pathlib.Path,
                        help="Path to the raw PBI JSON response file")
    parser.add_argument("--all-rows", action="store_true",
                        help="Include every decoded row, not just the first per result")
    parser.add_argument("--raw", action="store_true",
                        help="Attach full decoded row data to output")
    args = parser.parse_args()

    if not args.file.exists():
        print(f"ERROR: file not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    try:
        response = json.loads(args.file.read_text())
    except json.JSONDecodeError as e:
        print(f"ERROR: invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    records = parse_response(response, all_rows=args.all_rows, include_raw=args.raw)

    if not records:
        print("No results found in response.", file=sys.stderr)
        sys.exit(1)

    print(json.dumps(records, indent=2))


if __name__ == "__main__":
    main()
