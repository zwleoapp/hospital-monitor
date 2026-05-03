from .base import (_MELB, format_time, _parse_wait_str, _calculate_cache_lag,
                   _merge_last_updated_sidecar, _write_ingest_alert)

from curl_cffi import requests
import re
import json
import uuid
import pathlib


def _extract_dsr_value(result_obj: dict):
    """Navigate Power BI DSR envelope: result.data.dsr.DS[0].PH[0].DM0[0].M0"""
    try:
        return (result_obj["result"]["data"]["dsr"]
                          ["DS"][0]["PH"][0]["DM0"][0]["M0"])
    except (KeyError, IndexError, TypeError):
        return None


def _extract_dsr_timestamp(result_obj: dict) -> str | None:
    """
    Extract timestamp from Power BI DSR response for MIN aggregation queries.

    MIN(LastUpdatedDisplay) queries return: result.data.dsr.DS[0].PH[0].DM0[0].M0
    (aggregation result, not grouped key)
    """
    try:
        return (result_obj["result"]["data"]["dsr"]
                          ["DS"][0]["PH"][0]["DM0"][0]["M0"])
    except (KeyError, IndexError, TypeError):
        return None


def _build_pbi_single_measure_query(job_id: str, entity: str, hospital_col: str,
                                      hospital_filter: str, group_col: str, group_target: str,
                                      measure_col: str, measure_name: str,
                                      dataset_id: str, report_id: str, visual_id: str,
                                      apply_adult_filter: bool = True) -> dict:
    """
    Build a single-measure aggregation query for one visual.

    This replicates how the browser queries Power BI: each visual (waiting, treating, wait_str)
    makes a separate query with its own VisualId in ApplicationContext.

    WHERE Campus IN ('Casey') [AND AdultPaed IN ('Adult')]
    SELECT SUM(TotalWaiting) or MIN(Estimated Time)

    Args:
        apply_adult_filter: If False, omits AdultPaed filter (for Dandenong which has no Paeds ward)

    Returns DSR with M0 value (single measure result).
    """
    def _col(prop):
        return {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": prop}}

    # Determine aggregation function based on measure type
    if "Estimated Time" in measure_col or "LastUpdatedDisplay" in measure_col:
        agg_function = 3  # MIN for strings/timestamps
    else:
        agg_function = 0  # SUM for numeric counts

    # Build WHERE clause - Campus filter always, Adult filter conditionally
    where_conditions = [
        {
            "Condition": {
                "In": {
                    "Expressions": [_col(hospital_col)],
                    "Values": [[{"Literal": {"Value": f"'{hospital_filter}'"}}]]
                }
            }
        }
    ]

    # Only add Adult filter for campuses with Paediatric wards (Casey, Clayton)
    if apply_adult_filter:
        where_conditions.append({
            "Condition": {
                "In": {
                    "Expressions": [_col(group_col)],
                    "Values": [[{"Literal": {"Value": f"'{group_target}'"}}]]
                }
            }
        })

    query_obj = {
        "Query": {
            "Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [{"Name": "c", "Entity": entity, "Type": 0}],
                        "Select": [{
                            "Aggregation": {
                                "Expression": _col(measure_col),
                                "Function": agg_function,
                            },
                            "Name": measure_name
                        }],
                        "Where": where_conditions,
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0]}]},
                        "DataReduction": {"DataVolume": 3, "Primary": {"Top": {}}},
                        "Version": 1,
                    },
                    "ExecutionMetricsKind": 1,
                }
            }]
        },
        "QueryId": job_id,
        "ApplicationContext": {
            "DatasetId": dataset_id,
            "Sources": [{
                "ReportId": report_id,
                "VisualId": visual_id,
            }]
        }
    }

    query_obj["CacheKey"] = json.dumps({"Commands": query_obj["Query"]["Commands"]})
    return query_obj


def _extract_dsr_measure(result_obj: dict, measure_idx: int) -> int | None:
    """
    Extract a measure value (M0-M4) from Power BI measure query DSR response.

    Gauge queries return: result.data.dsr.DS[0].PH[0].DM0[0].C[measure_idx]
    The C array contains raw values for M0, M1, M2, M3, M4 in order.

    Power BI null-marker: If all measures are null, C=[] and a special "Ø" field appears.
    This indicates the query returned no matching rows (e.g., no Adult patients at that campus).

    Args:
        measure_idx: Index in C array (0=M0, 1=M1, ..., 4=M4)

    Type-agnostic: Handles both T:3 (gauge) and T:4 (integer) without distinction.
    """
    try:
        dm0_row = result_obj["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"][0]

        # Check for null marker (Ø field means C array is empty/all values null)
        if "Ø" in dm0_row or not dm0_row.get("C"):
            return None

        c_array = dm0_row["C"]
        if measure_idx < len(c_array):
            val = c_array[measure_idx]
            return int(val) if val is not None else None
        return None
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def _build_pbi_measure_query(job_id: str, entity: str, hospital_col: str,
                               hospital_filter: str, group_col: str, group_target: str) -> dict:
    """
    Build a gauge measure query for M0-M4 (Startpoint, Endpoint, Range2/3, MaxForRange).

    This query returns aggregate measures (not grouped rows) filtered by Campus AND AdultPaed.
    Used to extract the clinical raw MaxForRange (M4) integer for ML momentum calculation.

    WHERE Campus='Casey' AND AdultPaed='Adult'
    SELECT SUM(Startpoint), SUM(Endpoint), ..., SUM(MaxForRange)

    Returns DSR with C array: [M0_val, M1_val, M2_val, M3_val, M4_val]
    M4 uses type T:3 (gauge) instead of T:4 (integer).
    """
    def _col(prop):
        return {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": prop}}

    def _agg(prop, name):
        return {
            "Aggregation": {"Expression": _col(prop), "Function": 0},  # SUM
            "Name": name,
        }

    # Build WHERE clause: Campus=filter AND AdultPaed=target
    where_conditions = [
        {"Condition": {"Comparison": {
            "ComparisonKind": 0,
            "Left": _col(hospital_col),
            "Right": {"Literal": {"Value": f"'{hospital_filter}'"}},
        }}},
        {"Condition": {"Comparison": {
            "ComparisonKind": 0,
            "Left": _col(group_col),
            "Right": {"Literal": {"Value": f"'{group_target}'"}},
        }}},
    ]

    return {
        "Query": {
            "Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [{"Name": "t", "Entity": entity, "Type": 0}],
                        "Select": [
                            _agg("Startpoint",    "M0"),
                            _agg("Endpoint",      "M1"),
                            _agg("Range2start",   "M2"),
                            _agg("Range3start",   "M3"),
                            _agg("MaxForRange",   "M4"),
                        ],
                        "Where": where_conditions,
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3, 4]}]},
                        "DataReduction": {"DataVolume": 4, "Primary": {"Top": {}}},
                        "Version": 1,
                    },
                }
            }]
        },
        "QueryId": job_id,
    }


def _build_pbi_timestamp_query(job_id: str, entity: str, hospital_col: str,
                                hospital_filter: str, col_last_updated: str,
                                dataset_id: str, report_id: str, visual_id: str,
                                group_col: str = "", group_target: str = "",
                                apply_adult_filter: bool = False) -> dict:
    """
    Build a per-campus LastUpdatedDisplay query with ApplicationContext.

    CRITICAL: Each campus timestamp visual has a unique VisualId. Including this in
    ApplicationContext.Sources forces Power BI to use the visual-specific query context,
    returning truly per-campus timestamp values instead of report-level aggregates.

    CLINICAL LOCK UPDATE: Timestamp queries MUST match the dashboard's active filter state.
    For Casey/Clayton (which have Paediatric wards), we filter by AdultPaed='Adult' to get
    the timestamp for Adult patients only (matching the dashboard). Dandenong is Adults-only,
    so we omit the filter entirely.

    WHERE Campus IN ('Dandenong') [AND AdultPaed IN ('Adult')]
    SELECT MIN(LastUpdatedDisplay)
    ApplicationContext: { DatasetId, ReportId, VisualId }

    Returns DSR with M0 value (aggregation result), not G0 (group key).

    Args:
        apply_adult_filter: If True, adds AdultPaed = group_target filter (for Casey/Clayton)
                           If False, omits Adult filter (for Dandenong which has no Paeds ward)
    """
    def _col(prop):
        return {"Column": {"Expression": {"SourceRef": {"Source": "c"}}, "Property": prop}}

    # Build WHERE clause with Campus filter + optional Adult filter
    where_conditions = [
        {
            "Condition": {
                "In": {
                    "Expressions": [_col(hospital_col)],
                    "Values": [[{"Literal": {"Value": f"'{hospital_filter}'"}}]]
                }
            }
        }
    ]

    # CLINICAL LOCK: Add Adult filter for Casey/Clayton to match dashboard active state
    if apply_adult_filter and group_col and group_target:
        where_conditions.append({
            "Condition": {
                "In": {
                    "Expressions": [_col(group_col)],
                    "Values": [[{"Literal": {"Value": f"'{group_target}'"}}]]
                }
            }
        })

    query_obj = {
        "Query": {
            "Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [{"Name": "c", "Entity": entity, "Type": 0}],
                        "Select": [{
                            "Aggregation": {
                                "Expression": _col(col_last_updated),
                                "Function": 3,  # MIN function
                            },
                            "Name": f"Min({entity}.{col_last_updated})"
                        }],
                        "Where": where_conditions,
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0]}]},
                        "DataReduction": {"DataVolume": 3, "Primary": {"Top": {}}},
                        "Version": 1,
                    },
                    "ExecutionMetricsKind": 1,
                }
            }]
        },
        "QueryId": job_id,
        "ApplicationContext": {
            "DatasetId": dataset_id,
            "Sources": [{
                "ReportId": report_id,
                "VisualId": visual_id,
            }]
        }
    }

    # Add CacheKey (JSON-stringified Commands section) to match browser behavior
    query_obj["CacheKey"] = json.dumps({"Commands": query_obj["Query"]["Commands"]})

    return query_obj


def _build_pbi_grouped_query(job_id: str, entity: str, hospital_col: str,
                              hospital_filter: str, group_col: str,
                              col_waiting: str, col_treating: str,
                              col_wait_str: str,
                              col_last_updated: str | None = None,
                              dataset_id: str = "", report_id: str = "",
                              visual_id: str = "") -> dict:
    """
    Build a grouped SemanticQueryDataShapeCommand query for one campus.

    Groups by group_col (AdultPaed) and selects col_waiting, col_treating,
    col_wait_str columns, and optionally col_last_updated (G4) for native
    hospital data freshness. The response DSR contains one row per group value.
    We pick the target group row in _scrape_powerbi_source.
    """
    def _col(prop):
        return {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": prop}}

    select = [
        {**_col(group_col),    "Name": "G0"},
        {**_col(col_waiting),  "Name": "G1"},
        {**_col(col_treating), "Name": "G2"},
        {**_col(col_wait_str), "Name": "G3"},
    ]
    if col_last_updated:
        select.append({**_col(col_last_updated), "Name": "G4"})

    query_obj = {
        "Query": {
            "Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [{"Name": "t", "Entity": entity, "Type": 0}],
                        "Select": select,
                        "Where": [{"Condition": {"Comparison": {
                            "ComparisonKind": 0,
                            "Left":  _col(hospital_col),
                            "Right": {"Literal": {"Value": f"'{hospital_filter}'"}},
                        }}}],
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": list(range(len(select)))}]},
                        "DataReduction": {"DataVolume": 4, "Primary": {"Top": {}}},
                        "Version": 1,
                    },
                }
            }]
        },
        "QueryId": job_id,
    }

    # Add ApplicationContext if VisualId provided
    if dataset_id and report_id and visual_id:
        query_obj["ApplicationContext"] = {
            "DatasetId": dataset_id,
            "Sources": [{
                "ReportId": report_id,
                "VisualId": visual_id,
            }]
        }

    return query_obj


def _parse_grouped_dsr(result_obj: dict, group_target: str) -> dict | None:
    """
    Extract the target group row from a Power BI grouped DSR response.

    DSR format:
      DS[0].PH[0].DM0[i]  — row i, containing:
        S  — column schema (only on first row; entry has optional DN for dict-encoding)
        C  — values for non-repeated columns only
        R  — repeat bitmask: bit i set means col i is unchanged from the previous row
      DS[0].ValueDicts     — {dictName: [values]} for DN-encoded string columns

    Power BI uses delta/repeat compression: a row's C array may be shorter than
    n_cols because unchanged columns are omitted and flagged via R.  The old
    'if len(c) < 4: continue' check silently dropped these rows, causing the
    wrong group (e.g. Paed instead of Adult) to be returned.  We now reconstruct
    the full column vector before matching.
    """
    try:
        ds0    = result_obj["result"]["data"]["dsr"]["DS"][0]
        rows   = ds0["PH"][0]["DM0"]
        vdicts = ds0.get("ValueDicts", {})
        schema = rows[0]["S"]
    except (KeyError, IndexError, TypeError):
        return None

    n_cols = len(schema)

    def _decode(c_val, col_idx):
        if col_idx >= n_cols or c_val is None:
            return c_val
        s = schema[col_idx]
        if "DN" in s and isinstance(c_val, int):
            return vdicts.get(s["DN"], [])[c_val]
        return c_val

    prev_c      = [None] * n_cols
    first_valid = None

    for row in rows:
        c_raw  = row.get("C", [])
        r_mask = row.get("R", 0)   # bit i set → col i repeats from previous row

        # Reconstruct the full n_cols vector honouring the repeat bitmask
        full_c  = list(prev_c)
        raw_idx = 0
        for col_idx in range(n_cols):
            if r_mask & (1 << col_idx):
                pass  # keep prev_c[col_idx]
            else:
                if raw_idx < len(c_raw):
                    full_c[col_idx] = c_raw[raw_idx]
                raw_idx += 1
        prev_c = list(full_c)

        if full_c[0] is None:
            continue

        g0_raw = _decode(full_c[0], 0)
        decoded = {
            "group":        g0_raw,
            "waiting":      _decode(full_c[1], 1),
            "treating":     _decode(full_c[2], 2),
            "wait_str":     str(_decode(full_c[3], 3) or "").strip(),
            "last_updated": str(_decode(full_c[4], 4) or "").strip() if n_cols > 4 else "",
        }
        if first_valid is None:
            first_valid = decoded
        if str(g0_raw) == group_target:
            return decoded

    # Campus has no Adult/Paeds split — return the single row
    return first_valid


def _parse_grouped_dsr_maxwait(result_obj: dict) -> int | None:
    """
    Scan ALL groups (Adult + Paediatric + any others) in the same DSR response
    and return the highest wait upper-bound in minutes.

    The grouped query already returns every patient-category row for a campus.
    By taking the max across all of them we capture the true wait ceiling —
    e.g. an 8h 51m Paediatric outlier that the Adult-only filter would miss.
    Uses the same delta/repeat DSR reconstruction as _parse_grouped_dsr.
    G3 is always col_wait_str ("Estimated Time").
    """
    try:
        ds0    = result_obj["result"]["data"]["dsr"]["DS"][0]
        rows   = ds0["PH"][0]["DM0"]
        vdicts = ds0.get("ValueDicts", {})
        schema = rows[0]["S"]
    except (KeyError, IndexError, TypeError):
        return None

    n_cols = len(schema)

    def _decode(c_val, col_idx):
        if col_idx >= n_cols or c_val is None:
            return c_val
        s = schema[col_idx]
        if "DN" in s and isinstance(c_val, int):
            return vdicts.get(s["DN"], [])[c_val]
        return c_val

    prev_c   = [None] * n_cols
    max_mins = None

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

        if n_cols <= 3:
            continue
        wait_str = str(_decode(full_c[3], 3) or "").strip()
        if not wait_str:
            continue
        # Upper bound is after " - " separator, or the whole string if no range
        upper_str = wait_str.split(" - ")[-1]
        upper_mins = _parse_wait_str(upper_str)
        if upper_mins > 0:
            max_mins = max(max_mins or 0, upper_mins)

    return max_mins


def scrape_powerbi_source(source_key: str, cfg: dict, timestamp: str, location_timestamp: str) -> tuple[list, list]:
    """
    Hybrid scraper: Power BI API for metrics + HTML scraping for per-campus timestamps.

    ARCHITECTURE (Trust vs. Pulse):
    - Power BI API: raw_query_* columns (ML momentum, real-time pressure)
    - Webpage HTML: reported_* columns (UI truth, what users see)

    Power BI LastUpdatedDisplay (G4) is report-level, not per-campus. To match the
    webpage, we scrape HTML to extract the visual tile timestamps.

    Returns (bronze_rows, raw_scrape_rows) for dual persistence.
    """
    endpoint     = cfg.get("endpoint")
    model_id     = cfg.get("model_id")
    resource_key = cfg.get("resource_key")

    if not all([endpoint, model_id, resource_key]):
        missing = [k for k, v in {"endpoint": endpoint,
                                   "model_id": model_id,
                                   "resource_key": resource_key}.items() if not v]
        print(f"  [{source_key}] Power BI not configured — set {missing} in config/hospitals.py")
        return [], []

    entity           = cfg.get("entity",           "CurrentPatients")
    hospital_col     = cfg.get("hospital_col",     "Campus")
    group_col        = cfg.get("group_col",        "AdultPaed")
    group_target     = cfg.get("group_target",     "Adult")
    col_waiting      = cfg.get("col_waiting",      "TotalWaiting")
    col_treating     = cfg.get("col_treating",     "TotalBeingTreated")
    col_wait_str     = cfg.get("col_wait_str",     "Estimated Time")
    col_last_updated = cfg.get("col_last_updated")   # optional; None for non-PBI sources
    hospitals        = cfg["hospitals"]

    # Step 1: Query Power BI for per-campus timestamps (LastUpdatedDisplay with campus filter)
    pbi_timestamps: dict[str, str] = {}

    # ApplicationContext parameters for visual-specific queries
    dataset_id = cfg.get("dataset_id", "")
    report_id = cfg.get("report_id", "")
    visual_ids_map = cfg.get("visual_ids", {})

    # Extract Adult timestamp VisualId per campus from the nested cohort structure.
    # Timestamp visuals are shared between Adult and Paediatric (same visual_id), so
    # we always use the Adult entry. Supports legacy flat structure as fallback.
    timestamp_visual_ids = {}
    for campus_filter, cohort_cfg in visual_ids_map.items():
        adult_cfg = cohort_cfg.get("Adult", cohort_cfg)  # fall back to flat if no "Adult" key
        ts_id = adult_cfg.get("timestamp")
        if ts_id:
            timestamp_visual_ids[campus_filter] = ts_id

    if col_last_updated and dataset_id and report_id and timestamp_visual_ids:
        timestamp_queries = []
        timestamp_order = []  # Track (campus_filter, formal_name) order

        print(f"  [{source_key}] Querying per-campus {col_last_updated} with VisualId context (Trust Stream)...")
        for campus_filter, formal_name in hospitals.items():
            visual_id = timestamp_visual_ids.get(campus_filter)
            if not visual_id:
                print(f"  [{source_key}] WARNING: No VisualId for {campus_filter}, skipping timestamp query")
                continue

            # CLINICAL LOCK: Apply Adult filter for Casey/Clayton (which have Paeds wards)
            # Dandenong is Adults-only, so no filter needed
            apply_adult_filter = (campus_filter != "Dandenong")

            job_id = f"timestamp_{campus_filter}_{uuid.uuid4().hex[:8]}"
            timestamp_queries.append(_build_pbi_timestamp_query(
                job_id=job_id,
                entity=entity,
                hospital_col=hospital_col,
                hospital_filter=campus_filter,
                col_last_updated=col_last_updated,
                dataset_id=dataset_id,
                report_id=report_id,
                visual_id=visual_id,
                group_col=group_col,
                group_target=group_target,
                apply_adult_filter=apply_adult_filter,
            ))
            timestamp_order.append((campus_filter, formal_name))

        # Send each timestamp query as a SEPARATE HTTP request (not batched)
        # Power BI visual context may not work correctly in batch mode
        print(f"  [{source_key}] Sending {len(timestamp_queries)} individual timestamp queries...")

        try:
            for i, (campus_filter, formal_name) in enumerate(timestamp_order):
                if i >= len(timestamp_queries):
                    continue

                # Individual query payload
                single_payload = {
                    "version": "1.0.0",
                    "queries": [timestamp_queries[i]],  # Single query only
                    "cancelQueries": [],
                    "modelId": model_id,
                    "clientRequestId": f"ts_{campus_filter}_{uuid.uuid4().hex[:8]}",
                }

                # CLINICAL LOCK DEBUG: Verify Campus filter, Adult filter, and VisualId are unique per request
                query_obj = timestamp_queries[i]
                where_clause = query_obj["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Where"]
                visual_id = query_obj["ApplicationContext"]["Sources"][0]["VisualId"]
                campus_literal = where_clause[0]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"]

                # Check if Adult filter is present (2nd condition)
                adult_filter_str = ""
                if len(where_clause) > 1:
                    adult_literal = where_clause[1]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"]
                    adult_filter_str = f", Adult={adult_literal}"
                else:
                    adult_filter_str = ", Adult=NONE"

                print(f"    [DEBUG {campus_filter}] Campus={campus_literal}{adult_filter_str}, VisualId={visual_id[:16]}...")

                ts_resp = requests.post(
                    endpoint, json=single_payload,
                    headers={"Content-Type": "application/json",
                             "X-PowerBI-ResourceKey": resource_key},
                    impersonate="chrome120", timeout=15,
                )

                if ts_resp.status_code == 200:
                    ts_results = ts_resp.json().get("results", [])

                    # DEBUG: Save individual campus response
                    ts_campus_path = pathlib.Path(f"/tmp/powerbi_ts_{campus_filter}.json")
                    try:
                        with open(ts_campus_path, "w") as tcf:
                            json.dump({"campus": campus_filter, "results": ts_results}, tcf, indent=2)
                    except Exception:
                        pass

                    if ts_results:
                        ts_val = _extract_dsr_timestamp(ts_results[0])
                        if ts_val is not None:
                            pbi_timestamps[formal_name] = str(ts_val)
                            print(f"    [{campus_filter}] → {ts_val}")
                        else:
                            print(f"    [{campus_filter}] → NULL (M0 extraction failed - check /tmp/powerbi_ts_{campus_filter}.json)")
                    else:
                        print(f"    [{campus_filter}] → NO RESULTS")
                else:
                    print(f"    [{campus_filter}] HTTP {ts_resp.status_code}")

            if pbi_timestamps:
                print(f"  [{source_key}] Per-campus timestamps extracted: {len(pbi_timestamps)} campuses")
            else:
                print(f"  [{source_key}] WARNING: All timestamp queries failed")

        except Exception as e:
            print(f"  [{source_key}] Timestamp query failed: {e}")

    # Step 2: Query Power BI gauge measures (M0-M4) for clinical raw MaxForRange
    gauge_max_wait: dict[str, int] = {}
    measure_queries = []
    measure_order = []

    print(f"  [{source_key}] Querying gauge measures (M4=MaxForRange) for ML momentum...")
    bust_id_measure = uuid.uuid4().hex

    for campus_filter, formal_name in hospitals.items():
        job_id = f"gauge_{campus_filter}_{bust_id_measure}"
        measure_queries.append(_build_pbi_measure_query(
            job_id=job_id,
            entity=entity,
            hospital_col=hospital_col,
            hospital_filter=campus_filter,
            group_col=group_col,
            group_target=group_target,
        ))
        measure_order.append((campus_filter, formal_name))

    measure_payload = {
        "version": "1.0.0",
        "queries": measure_queries,
        "cancelQueries": [],
        "modelId": model_id,
        "clientRequestId": f"gauge_{bust_id_measure}",
    }

    try:
        measure_resp = requests.post(
            endpoint, json=measure_payload,
            headers={"Content-Type": "application/json",
                     "X-PowerBI-ResourceKey": resource_key},
            impersonate="chrome120", timeout=30,
        )
        if measure_resp.status_code == 200:
            measure_results = measure_resp.json().get("results", [])

            # DEBUG: Save gauge response
            gauge_debug_path = pathlib.Path("/tmp/powerbi_gauge_response.json")
            try:
                with open(gauge_debug_path, "w") as gf:
                    json.dump({"timestamp": timestamp, "results": measure_results}, gf, indent=2)
            except Exception:
                pass

            for i, (campus_filter, formal_name) in enumerate(measure_order):
                if i < len(measure_results):
                    # M4 is index 4 in the C array (M0, M1, M2, M3, M4)
                    m4_val = _extract_dsr_measure(measure_results[i], measure_idx=4)
                    if m4_val is not None:
                        gauge_max_wait[formal_name] = m4_val
                    else:
                        print(f"  [{source_key}] WARNING: M4 extraction failed for {formal_name}")
            if gauge_max_wait:
                print(f"  [{source_key}] Gauge M4 (MaxForRange) extracted: {gauge_max_wait}")
            else:
                print(f"  [{source_key}] WARNING: Gauge queries returned no M4 values")
        else:
            print(f"  [{source_key}] Gauge query HTTP {measure_resp.status_code}")
    except Exception as e:
        print(f"  [{source_key}] Gauge query failed: {e}")

    # Step 3: Query Power BI per-measure queries (per campus, per cohort).
    # visual_ids schema: {campus: {cohort: {measure: visual_id}}}
    # Adult → main bronze CSV + bronze_raw. Paediatric → bronze_raw only.
    visual_ids_map = cfg.get("visual_ids", {})

    # Keys: (campus_filter, cohort_name). Values: {formal_name, cohort, metrics dict}
    campus_cohort_metrics: dict[tuple, dict] = {}

    measure_types = [
        ("waiting", col_waiting,  "TotalWaiting"),
        ("treating", col_treating, "TotalBeingTreated"),
        ("wait_str", col_wait_str, "Estimated Time"),
    ]

    print(f"  [{source_key}] Querying per-measure visuals (waiting, treating, wait_str)...")

    for campus_filter, formal_name in hospitals.items():
        campus_cohort_cfg = visual_ids_map.get(campus_filter, {})

        # Build the list of cohorts to fetch for this campus.
        # Nested schema: {"Adult": {...}, "Paeds": {...}} — non-Adult key is used
        # verbatim as the Power BI AdultPaed WHERE filter value, so it must match
        # the exact string in the data model (confirmed "Paeds" for Monash Health).
        # Legacy flat schema {"waiting": id, ...} treated as Adult-only.
        if "Adult" in campus_cohort_cfg:
            cohorts_to_fetch = [("Adult", campus_cohort_cfg["Adult"])]
            for cohort_key, cohort_cfg in campus_cohort_cfg.items():
                if cohort_key != "Adult":
                    cohorts_to_fetch.append((cohort_key, cohort_cfg))
        elif campus_cohort_cfg:
            cohorts_to_fetch = [("Adult", campus_cohort_cfg)]  # legacy flat
        else:
            print(f"  [{source_key}] WARNING: No VisualIds for {campus_filter}, using grouped query fallback")
            continue

        for cohort_name, cohort_visuals in cohorts_to_fetch:
            if not cohort_visuals or cohort_visuals.get("waiting") == "TBD":
                if cohort_name == "Adult":
                    print(f"  [{source_key}] WARNING: No Adult VisualIds for {campus_filter}, using grouped query fallback")
                continue

            # Dandenong Adult: no AdultPaed filter (Adults-only ward, no split in Power BI).
            # All other campus+cohort combinations: filter by cohort_name.
            apply_cohort_filter = not (campus_filter == "Dandenong" and cohort_name == "Adult")

            metrics: dict = {}
            for measure_key, measure_col, measure_name in measure_types:
                visual_id = cohort_visuals.get(measure_key)
                if not visual_id or visual_id == "TBD":
                    print(f"    [{campus_filter}/{cohort_name}] Skipping {measure_key} (no VisualId)")
                    continue

                query = _build_pbi_single_measure_query(
                    job_id=f"{campus_filter}_{cohort_name}_{measure_key}_{uuid.uuid4().hex[:8]}",
                    entity=entity,
                    hospital_col=hospital_col,
                    hospital_filter=campus_filter,
                    group_col=group_col,
                    group_target=cohort_name,
                    measure_col=measure_col,
                    measure_name=measure_name,
                    dataset_id=dataset_id,
                    report_id=report_id,
                    visual_id=visual_id,
                    apply_adult_filter=apply_cohort_filter,
                )

                payload = {
                    "version": "1.0.0",
                    "queries": [query],
                    "cancelQueries": [],
                    "modelId": model_id,
                    "clientRequestId": f"{campus_filter}_{cohort_name}_{measure_key}_{uuid.uuid4().hex[:8]}",
                }

                try:
                    resp = requests.post(
                        endpoint, json=payload,
                        headers={"Content-Type": "application/json",
                                 "X-PowerBI-ResourceKey": resource_key},
                        impersonate="chrome120", timeout=15,
                    )

                    if resp.status_code == 200:
                        results = resp.json().get("results", [])

                        debug_path = pathlib.Path(f"/tmp/powerbi_{campus_filter.lower()}_{cohort_name.lower()}_{measure_key}.json")
                        try:
                            with open(debug_path, "w") as df:
                                json.dump({
                                    "campus": campus_filter, "cohort": cohort_name,
                                    "measure": measure_key, "visual_id": visual_id,
                                    "apply_cohort_filter": apply_cohort_filter,
                                    "query": query, "results": results
                                }, df, indent=2)
                        except Exception:
                            pass

                        if results:
                            val = _extract_dsr_timestamp(results[0])
                            if val is not None:
                                metrics[measure_key] = val
                                print(f"    [{campus_filter}/{cohort_name}] {measure_key}={val}")
                            else:
                                print(f"    [{campus_filter}/{cohort_name}] {measure_key}=NULL")
                    else:
                        print(f"    [{campus_filter}/{cohort_name}] {measure_key} HTTP {resp.status_code}")
                except Exception as e:
                    print(f"    [{campus_filter}/{cohort_name}] {measure_key} error: {e}")

            if metrics:
                campus_cohort_metrics[(campus_filter, cohort_name)] = {
                    "formal_name": formal_name,
                    "cohort": cohort_name,
                    "metrics": metrics,
                }

    # Fallback: grouped query for campuses with no VisualIds configured at all.
    # Only Adult data is fetched via fallback.
    bust_id = uuid.uuid4().hex
    fallback_queries = []
    fallback_order = []

    adult_scraped = {campus for (campus, cohort) in campus_cohort_metrics if cohort == "Adult"}
    for campus_filter, formal_name in hospitals.items():
        if campus_filter in adult_scraped:
            continue  # Already got Adult data via per-measure queries

        fallback_queries.append(_build_pbi_grouped_query(
            job_id=f"{campus_filter}_{bust_id}",
            entity=entity,
            hospital_col=hospital_col,
            hospital_filter=campus_filter,
            group_col=group_col,
            col_waiting=col_waiting,
            col_treating=col_treating,
            col_wait_str=col_wait_str,
            col_last_updated=col_last_updated,
        ))
        fallback_order.append((campus_filter, formal_name))

    fallback_results = []
    if fallback_queries:
        payload = {
            "version": "1.0.0",
            "queries": fallback_queries,
            "cancelQueries": [],
            "modelId": model_id,
            "clientRequestId": bust_id,
        }
        resp = requests.post(
            endpoint, json=payload,
            headers={"Content-Type": "application/json",
                     "X-PowerBI-ResourceKey": resource_key},
            impersonate="chrome120", timeout=30,
        )
        if resp.status_code == 200:
            fallback_results = resp.json().get("results", [])

    results = fallback_results  # For compatibility with existing parsing code below

    # DEBUG: Save raw Power BI response to inspect schema
    if results and len(results) > 0:
        debug_path = pathlib.Path("/tmp/powerbi_debug_response.json")
        try:
            with open(debug_path, "w") as df:
                json.dump({"timestamp": timestamp, "results": results}, df, indent=2)
            print(f"  [DEBUG] Raw Power BI response → {debug_path}")
        except Exception:
            pass

    rows = []
    raw_rows = []
    last_updated_map: dict[str, str] = {}

    # Process per-measure query results (all cohorts: Adult → bronze CSV + bronze_raw;
    # Paediatric → bronze_raw only for ML/audit, not the UI-facing CSV).
    for (campus_filter, cohort_name), data in campus_cohort_metrics.items():
        formal_name = data["formal_name"]
        metrics     = data["metrics"]

        waiting_raw  = metrics.get("waiting")
        treating_raw = metrics.get("treating")
        wait_str     = metrics.get("wait_str", "")

        if waiting_raw is None and treating_raw is None:
            print(f"  [{source_key}] WARNING: All measures NULL for {formal_name}/{cohort_name}, skipping")
            _write_ingest_alert(formal_name, cohort_name, "powerbi",
                                "NULL_ALL_MEASURES",
                                "waiting and treating both null from per-measure visual queries",
                                timestamp, location_timestamp)
            continue

        try:
            waiting = int(waiting_raw) if waiting_raw is not None else 0
        except (ValueError, TypeError):
            waiting = 0

        try:
            treating = int(treating_raw) if treating_raw is not None else 0
        except (ValueError, TypeError):
            treating = 0

        min_mins = _parse_wait_str(wait_str.split(" - ")[0]) if " - " in wait_str else _parse_wait_str(wait_str)
        max_mins = _parse_wait_str(wait_str.split(" - ")[1]) if " - " in wait_str else min_mins

        reported_ts = pbi_timestamps.get(formal_name, "")
        # Track reported timestamp per campus for sidecar (Adult only — matches dashboard view)
        if reported_ts and cohort_name == "Adult":
            last_updated_map[formal_name] = reported_ts

        # Gauge M4 queries are Adult-only (gauge_max_wait keyed by formal_name = Adult result)
        gauge_m4 = gauge_max_wait.get(formal_name) if cohort_name == "Adult" else None
        ml_max_wait = gauge_m4 if gauge_m4 is not None else max_mins

        cache_lag, fidelity_status = _calculate_cache_lag(timestamp, reported_ts)

        if fidelity_status == "PORTAL_STALE_WARNING":
            print(f"  ⚠️  [{source_key}] {formal_name}/{cohort_name}: PORTAL_STALE_WARNING — Data is {cache_lag} minutes old!")

        # Main bronze CSV (UI-facing): Adult cohort only, default dashboard view
        if cohort_name == "Adult":
            rows.append([timestamp, formal_name, waiting, treating,
                         wait_str, min_mins, max_mins, location_timestamp])

        # Bronze raw (ML / audit): all cohorts
        raw_rows.append([
            formal_name,
            timestamp,          # scrape_timestamp_utc  — Python execution time (Scrape Truth)
            location_timestamp, # location_timestamp     — Melbourne local at scrape
            reported_ts,        # reported_timestamp_str — "Last Updated" from portal (Portal Truth)
            waiting,            # reported_waiting
            wait_str,           # reported_wait_str
            waiting,            # raw_query_waiting
            treating,           # raw_query_treating
            ml_max_wait,        # raw_query_max_wait (M4 gauge for Adult; parsed upper for Paed)
            cohort_name,        # cohort: "Adult" | "Paediatric"
            cache_lag,          # cache_lag_minutes = scrape_timestamp − reported_timestamp
            fidelity_status,    # fidelity_status
        ])

        gauge_label = f", M4={gauge_m4}m" if gauge_m4 is not None else ""
        print(f"   [SCRAPED] {formal_name}/{cohort_name}: waiting={waiting}, treating={treating}, wait={wait_str}, max={max_mins}m{gauge_label}"
              + (f", ts={reported_ts}" if reported_ts else ", ts=∅"))

    # Process fallback grouped query results for campuses with no VisualIds (Adult only)
    for i, (campus_filter, formal_name) in enumerate(fallback_order):
        if i >= len(results):
            print(f"  [{source_key}] Missing fallback result for {formal_name}")
            _write_ingest_alert(formal_name, "Adult", "powerbi",
                                "NULL_ALL_MEASURES",
                                f"No DSR result in fallback batch (index {i}, results len {len(results)})",
                                timestamp, location_timestamp)
            continue

        row = _parse_grouped_dsr(results[i], group_target)
        if row is None:
            print(f"  [{source_key}] No '{group_target}' row found for {formal_name}")
            _write_ingest_alert(formal_name, "Adult", "powerbi",
                                "DSR_NO_GROUP",
                                f"No '{group_target}' group row in DSR response",
                                timestamp, location_timestamp)
            continue

        waiting   = int(row["waiting"]  or 0)
        treating  = int(row["treating"] or 0)
        # Guard: skip and alert if both measures are zero-from-null (not a valid ED state)
        if row.get("waiting") is None and row.get("treating") is None:
            _write_ingest_alert(formal_name, "Adult", "powerbi",
                                "NULL_ALL_MEASURES",
                                "waiting and treating both null in DSR grouped row",
                                timestamp, location_timestamp)
            continue
        wait_str  = row["wait_str"]
        min_mins  = _parse_wait_str(wait_str.split(" - ")[0]) if " - " in wait_str else _parse_wait_str(wait_str)
        all_max   = _parse_grouped_dsr_maxwait(results[i])
        adult_max = _parse_wait_str(wait_str.split(" - ")[1]) if " - " in wait_str else min_mins
        max_mins  = all_max if (all_max and all_max >= adult_max) else adult_max
        if all_max and all_max > adult_max:
            print(f"   [MAX-WAIT] {formal_name}: all-group max {all_max}m > adult max {adult_max}m")

        pbi_campus_ts = pbi_timestamps.get(formal_name, "")
        pbi_report_ts = row.get("last_updated", "")
        reported_ts   = pbi_campus_ts if pbi_campus_ts else pbi_report_ts

        if reported_ts:
            last_updated_map[formal_name] = reported_ts

        gauge_m4    = gauge_max_wait.get(formal_name)
        ml_max_wait = gauge_m4 if gauge_m4 is not None else max_mins

        cache_lag, fidelity_status = _calculate_cache_lag(timestamp, reported_ts)

        if fidelity_status == "PORTAL_STALE_WARNING":
            print(f"  ⚠️  [{source_key}] {formal_name}: PORTAL_STALE_WARNING — Data is {cache_lag} minutes old!")

        rows.append([timestamp, formal_name, waiting, treating,
                     wait_str, min_mins, max_mins, location_timestamp])

        raw_rows.append([
            formal_name,
            timestamp,          # scrape_timestamp_utc  (Scrape Truth)
            location_timestamp, # location_timestamp     (Melbourne local)
            reported_ts,        # reported_timestamp_str (Portal Truth)
            waiting,
            wait_str,
            waiting,
            treating,
            ml_max_wait,
            "Adult",            # cohort: fallback path is always Adult
            cache_lag,
            fidelity_status,
        ])

        source_label = "per-campus" if pbi_campus_ts else "report-level"
        gauge_label  = f", M4={gauge_m4}m" if gauge_m4 is not None else ""
        print(f"   [SCRAPED] {formal_name}/Adult: waiting={waiting}, treating={treating}, wait={wait_str}, max={max_mins}m{gauge_label}"
              + (f", ts={reported_ts} ({source_label})" if reported_ts else ", ts=∅"))

    # If every campus returned the same timestamp, LastUpdatedDisplay is report-global
    # (not per-campus). Tag with '^' so the frontend skips per-campus stale checks.
    # Self-correcting: once a real per-campus column is configured and returns differing
    # values, the '^' is not applied.
    values = list(last_updated_map.values())
    if len(values) > 1 and len(set(values)) == 1:
        last_updated_map = {k: "^" + v for k, v in last_updated_map.items()}
        print(f"  [{source_key}] LastUpdatedDisplay is report-global (all campuses same) — tagged '^'")

    _merge_last_updated_sidecar(last_updated_map)

    return rows, raw_rows
