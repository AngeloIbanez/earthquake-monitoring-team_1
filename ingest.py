#!/usr/bin/env python3
"""
Command-Line Ingestion Worker for Natural Hazard Monitoring System.
Team 1: Earthquake Activity Monitoring

Usage:
    python ingest.py --mode historical [--start 2025-10-01 --end 2025-11-01] [--source usgs_fdsn] [--use-samples] [--dry-run]
    python ingest.py --mode incremental [--source usgs_realtime] [--use-samples] [--dry-run]
    python ingest.py --mode status
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple, Optional

from src.config import load_sources_config
from src.models import CanonicalEvent, EarthquakeDetail
from src.parsers import parse_usgs_geojson, parse_emsc_json, parse_gdacs_xml
from src.extractors import fetch_historical_source, fetch_incremental_source, save_raw_payload
from src.matching import build_cross_source_event_groups
from src.db import (
    get_db_connection,
    ensure_schema,
    sync_sources,
    start_ingestion_run,
    finish_ingestion_run,
    upsert_events_and_earthquakes,
    insert_rejected_records,
    update_watermark,
    get_watermark,
    load_all_canonical_events,
    upsert_event_groups,
    get_pipeline_status,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingest")

SAMPLES_DIR = Path(__file__).resolve().parent / "workshop_1" / "samples"


def _load_sample_and_parse(
    s_id: str, mode: str, save_raw: bool = True
) -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """Loads local sample files from workshop_1/samples/ and parses them into canonical models."""
    if "usgs" in s_id:
        sample_file = SAMPLES_DIR / (
            "usgs_historical_raw.json" if mode == "historical" else "usgs_incremental_raw.json"
        )
        payload = json.loads(sample_file.read_text(encoding="utf-8"))
        if save_raw:
            save_raw_payload(s_id, payload, ext="geojson")
        return parse_usgs_geojson(payload, source_id=s_id)
    elif "emsc" in s_id:
        sample_file = SAMPLES_DIR / "emsc_historical_raw.json"
        payload = json.loads(sample_file.read_text(encoding="utf-8"))
        if save_raw:
            save_raw_payload(s_id, payload, ext="json")
        return parse_emsc_json(payload, source_id=s_id)
    elif "gdacs" in s_id:
        sample_file = SAMPLES_DIR / "gdacs_incremental_raw.xml"
        xml_text = sample_file.read_text(encoding="utf-8")
        if save_raw:
            save_raw_payload(s_id, xml_text, ext="xml")
        return parse_gdacs_xml(xml_text, source_id=s_id)
    else:
        raise ValueError(f"Unrecognized source ID for sample loading: {s_id}")


def _fetch_live_and_parse(
    s_id: str,
    s_cfg: dict,
    mode: str,
    start_date: Optional[str],
    end_date: Optional[str],
    save_raw: bool = True,
) -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """Fetches live data from external seismic endpoints and parses into canonical models."""
    events: List[CanonicalEvent] = []
    earthquakes: List[EarthquakeDetail] = []

    if mode == "historical":
        eff_start = start_date or os.getenv("INGESTION_HISTORICAL_START", "2025-10-01")
        eff_end = end_date or os.getenv("INGESTION_HISTORICAL_END", "2025-11-01")
        payloads = fetch_historical_source(
            source_id=s_id,
            source_cfg=s_cfg,
            start_date=eff_start,
            end_date=eff_end,
            save_raw=save_raw,
        )
        for p in payloads:
            if "usgs" in s_id:
                evs, eqs = parse_usgs_geojson(p, source_id=s_id)
            elif "emsc" in s_id:
                evs, eqs = parse_emsc_json(p, source_id=s_id)
            else:
                continue
            events.extend(evs)
            earthquakes.extend(eqs)
    else:
        raw_payload = fetch_incremental_source(source_id=s_id, source_cfg=s_cfg, save_raw=save_raw)
        if "usgs" in s_id and isinstance(raw_payload, dict):
            events, earthquakes = parse_usgs_geojson(raw_payload, source_id=s_id)
        elif "emsc" in s_id:
            events, earthquakes = parse_emsc_json(raw_payload, source_id=s_id)
        elif "gdacs" in s_id and isinstance(raw_payload, str):
            events, earthquakes = parse_gdacs_xml(raw_payload, source_id=s_id)

    return events, earthquakes


def _filter_by_watermark_window(
    events: List[CanonicalEvent],
    earthquakes: List[EarthquakeDetail],
    watermark_str: Optional[str],
    overlap_hours: int = 24,
) -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """
    Filters incremental events to the [watermark - overlap_hours, NOW] window.
    Preserves invalid records so they still reach the validation & quarantine gate.
    """
    if not watermark_str:
        return events, earthquakes
    try:
        wm_dt = datetime.fromisoformat(watermark_str.replace("Z", "+00:00"))
        if wm_dt.tzinfo is None:
            wm_dt = wm_dt.replace(tzinfo=timezone.utc)
        cutoff = wm_dt - timedelta(hours=overlap_hours)
    except Exception:
        return events, earthquakes

    eq_map = {eq.event_id: eq for eq in earthquakes}
    filtered_evs: List[CanonicalEvent] = []
    filtered_eqs: List[EarthquakeDetail] = []

    for ev in events:
        ref_dt = ev.updated_at or ev.timestamp
        if ref_dt is None or ref_dt >= cutoff:
            filtered_evs.append(ev)
            if ev.event_id in eq_map:
                filtered_eqs.append(eq_map[ev.event_id])

    logger.info(
        "Applied incremental watermark filter (watermark=%s, overlap=%dh): %d/%d events within window",
        watermark_str,
        overlap_hours,
        len(filtered_evs),
        len(events),
    )
    return filtered_evs, filtered_eqs


def run_status() -> None:
    """Queries PostgreSQL and displays watermarks, last runs, and canonical table row counts."""
    conn = get_db_connection()
    try:
        ensure_schema(conn)
        config = load_sources_config()
        sync_sources(conn, config.get("sources", {}))
        status = get_pipeline_status(conn)
    finally:
        conn.close()

    print("\n======================================================================")
    print("  EARTHQUAKE MONITORING SYSTEM — PIPELINE STATUS")
    print("======================================================================")
    print("\n[1] Canonical Table Row Counts:")
    for tbl, cnt in status["counts"].items():
        print(f"    - {tbl:<18}: {cnt:>8,d} rows")

    print("\n[2] Events by Source:")
    if not status["source_breakdown"]:
        print("    (No events stored yet)")
    else:
        for sb in status["source_breakdown"]:
            print(
                f"    - {sb['source_id']:<16}: {sb['event_count']:>7,d} events "
                f"(Range: {sb['min_timestamp']} -> {sb['max_timestamp']})"
            )

    print("\n[3] Source Watermarks (ingestion_state):")
    if not status["watermarks"]:
        print("    (No watermarks recorded yet)")
    else:
        for wm in status["watermarks"]:
            print(
                f"    - {wm['source_id']:<16}: watermark={wm['watermark']} "
                f"| last_success={wm['last_success']}"
            )

    print("\n[4] Recent Ingestion Runs (ingestion_run):")
    if not status["recent_runs"]:
        print("    (No ingestion runs recorded yet)")
    else:
        for r in status["recent_runs"]:
            print(
                f"    - Run #{r['run_id']:<4} | {r['source_id']:<15} | mode={r['mode']:<11} "
                f"| status={r['status']:<7} | in={r['rows_in']:<5} new={r['rows_new']:<5} "
                f"rejected={r['rows_rejected']:<4} | started={r['started_at']}"
            )
    print("======================================================================\n")


def run_ingestion(
    mode: str,
    source_filter: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    use_samples: bool = False,
    dry_run: bool = False,
) -> int:
    """Executes the ingestion pipeline for the specified mode and sources, followed by cross-source matching."""
    logger.info(
        "Initializing Earthquake Ingestion Pipeline (Mode: %s, Live: %s, Dry-run: %s)",
        mode,
        not use_samples,
        dry_run,
    )
    config = load_sources_config()
    sources = config.get("sources", {})

    conn = None
    if not dry_run:
        conn = get_db_connection()
        ensure_schema(conn)
        sync_sources(conn, sources)

    total_processed = 0
    total_valid = 0
    total_new = 0
    total_rejected = 0
    session_valid_events: List[CanonicalEvent] = []

    try:
        for s_key, s_cfg in sources.items():
            s_id = s_cfg.get("id", s_key)
            s_type = s_cfg.get("type")
            s_format = s_cfg.get("format")

            if s_type != mode:
                continue
            if source_filter and s_id != source_filter and s_key != source_filter:
                continue

            logger.info("Processing source [%s] (%s | Format: %s)", s_id, s_cfg.get("name"), s_format)

            run_id = None
            prev_watermark = None
            if conn is not None:
                prev_watermark = get_watermark(conn, s_id)
                run_id = start_ingestion_run(conn, s_id, mode)

            try:
                if use_samples:
                    events, earthquakes = _load_sample_and_parse(s_id, mode, save_raw=not dry_run)
                else:
                    events, earthquakes = _fetch_live_and_parse(
                        s_id=s_id,
                        s_cfg=s_cfg,
                        mode=mode,
                        start_date=start_date,
                        end_date=end_date,
                        save_raw=not dry_run,
                    )

                if mode == "incremental" and prev_watermark and not use_samples:
                    overlap_h = int(s_cfg.get("watermark", {}).get("overlap_window_hours", 24))
                    events, earthquakes = _filter_by_watermark_window(
                        events, earthquakes, prev_watermark, overlap_hours=overlap_h
                    )

                eq_by_id = {eq.event_id: eq for eq in earthquakes}
                valid_events: List[CanonicalEvent] = []
                valid_earthquakes: List[EarthquakeDetail] = []
                rejected_items: List[Tuple[CanonicalEvent, str]] = []

                for ev in events:
                    is_ok, reason = ev.validate()
                    if is_ok:
                        valid_events.append(ev)
                        if ev.event_id in eq_by_id:
                            valid_earthquakes.append(eq_by_id[ev.event_id])
                    else:
                        rejected_items.append((ev, reason or "validation_failed"))

                session_valid_events.extend(valid_events)
                rows_in = len(events)
                rows_rejected = len(rejected_items)
                rows_new = 0

                if conn is not None and run_id is not None:
                    rows_new = upsert_events_and_earthquakes(conn, valid_events, valid_earthquakes)
                    insert_rejected_records(conn, run_id, s_id, rejected_items)

                    if valid_events:
                        latest_dt = max(
                            (ev.updated_at or ev.timestamp)
                            for ev in valid_events
                            if (ev.updated_at or ev.timestamp) is not None
                        )
                        if latest_dt is not None:
                            update_watermark(conn, s_id, latest_dt.isoformat())

                    conn.commit()
                    finish_ingestion_run(
                        conn,
                        run_id=run_id,
                        status="SUCCESS",
                        rows_in=rows_in,
                        rows_new=rows_new,
                        rows_rejected=rows_rejected,
                    )
                else:
                    rows_new = len(valid_events)

                logger.info(
                    "Source [%s] Summary: Rows In: %d | Valid: %d | New Inserted: %d | Quarantined: %d",
                    s_id,
                    rows_in,
                    len(valid_events),
                    rows_new,
                    rows_rejected,
                )

                total_processed += rows_in
                total_valid += len(valid_events)
                total_new += rows_new
                total_rejected += rows_rejected

            except Exception as exc:
                logger.error("Failed to ingest source [%s]: %s", s_id, str(exc), exc_info=True)
                if conn is not None:
                    conn.rollback()
                    if run_id is not None:
                        finish_ingestion_run(
                            conn,
                            run_id=run_id,
                            status="FAILED",
                            rows_in=0,
                            rows_new=0,
                            rows_rejected=0,
                        )

        # Cross-source event identification and grouping (event_group)
        events_for_matching = load_all_canonical_events(conn) if conn is not None else session_valid_events
        group_rows, match_metrics = build_cross_source_event_groups(events_for_matching)
        if conn is not None and group_rows:
            upsert_event_groups(conn, group_rows)
        logger.info(
            "Cross-Source Event Matching Summary: Matched Pairs=%d | Multi-Source Groups=%d | Grouped Events=%d | Median dt=%.2fs | Median dist=%.2fkm",
            match_metrics["matched_pairs"],
            match_metrics["total_groups"],
            match_metrics["total_grouped_events"],
            match_metrics["median_time_diff_sec"],
            match_metrics["median_dist_km"],
        )

    finally:
        if conn is not None:
            conn.close()

    logger.info("=== Ingestion Run Completed ===")
    logger.info(
        "Total Rows In: %d | Valid Rows: %d | New Rows: %d | Quarantined Rows: %d",
        total_processed,
        total_valid,
        total_new,
        total_rejected,
    )
    return total_valid


def main():
    parser = argparse.ArgumentParser(description="Natural Hazard Ingestion CLI (Team 1: Earthquakes)")
    parser.add_argument(
        "--mode",
        choices=["historical", "incremental", "status"],
        required=True,
        help="Pipeline mode: historical (bulk partitioned), incremental (latest feed), or status",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Optional specific source ID to execute (e.g. usgs_fdsn, emsc_fdsn, usgs_realtime, gdacs_realtime)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Historical start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="Historical end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--use-samples",
        action="store_true",
        help="Read from local verified samples in workshop_1/samples/ instead of live HTTP endpoints",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute parsing, validation, and cross-source matching without writing to PostgreSQL",
    )

    args = parser.parse_args()
    if args.mode == "status":
        run_status()
    else:
        run_ingestion(
            mode=args.mode,
            source_filter=args.source,
            start_date=args.start,
            end_date=args.end,
            use_samples=args.use_samples,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
