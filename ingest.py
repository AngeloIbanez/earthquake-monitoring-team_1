#!/usr/bin/env python3
"""
Command-Line Ingestion Worker for Natural Hazard Monitoring System.
Team 1: Earthquake Activity Monitoring

Usage:
    python ingest.py --mode historical [--source usgs_fdsn] [--dry-run]
    python ingest.py --mode incremental [--source usgs_realtime] [--dry-run]
"""

import argparse
import json
import logging
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone

from src.config import load_sources_config
from src.parsers import parse_usgs_geojson, parse_emsc_json, parse_gdacs_xml
from src.models import IngestionRunRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ingest")

SAMPLES_DIR = Path(__file__).resolve().parent / "workshop_1" / "samples"

def run_ingestion(mode: str, source_filter: str = None, dry_run: bool = False):
    """Executes the ingestion pipeline for the specified mode and sources."""
    logger.info("Initializing Earthquake Ingestion Pipeline (Mode: %s, Dry-run: %s)", mode, dry_run)
    config = load_sources_config()
    sources = config.get("sources", {})
    
    total_processed = 0
    total_valid = 0
    total_rejected = 0
    
    for s_key, s_cfg in sources.items():
        s_id = s_cfg.get("id", s_key)
        s_type = s_cfg.get("type")
        s_format = s_cfg.get("format")
        
        # Filter by mode and source if specified
        if s_type != mode:
            continue
        if source_filter and s_id != source_filter and s_key != source_filter:
            continue
            
        logger.info("Processing source [%s] (%s | Format: %s)", s_id, s_cfg.get("name"), s_format)
        
        # In offline/dry-run mode, read from workshop_1/samples
        events = []
        earthquakes = []
        
        try:
            if "usgs" in s_id:
                sample_file = SAMPLES_DIR / ("usgs_historical_raw.json" if mode == "historical" else "usgs_incremental_raw.json")
                with open(sample_file, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                events, earthquakes = parse_usgs_geojson(payload, source_id=s_id)
            elif "emsc" in s_id:
                sample_file = SAMPLES_DIR / "emsc_historical_raw.json"
                with open(sample_file, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                events, earthquakes = parse_emsc_json(payload, source_id=s_id)
            elif "gdacs" in s_id:
                sample_file = SAMPLES_DIR / "gdacs_incremental_raw.xml"
                with open(sample_file, "r", encoding="utf-8") as f:
                    xml_text = f.read()
                events, earthquakes = parse_gdacs_xml(xml_text, source_id=s_id)
            else:
                logger.warning("Unrecognized source ID: %s. Skipping.", s_id)
                continue
                
            # Validation gating
            valid_events = []
            rejected_events = []
            for ev in events:
                if ev.is_valid():
                    valid_events.append(ev)
                else:
                    rejected_events.append(ev)
                    
            logger.info("Source [%s] Summary: Parsed %d records | Valid: %d | Quarantined: %d",
                        s_id, len(events), len(valid_events), len(rejected_events))
            
            total_processed += len(events)
            total_valid += len(valid_events)
            total_rejected += len(rejected_events)
            
            if not dry_run:
                logger.info("Persisting %d records to PostgreSQL canonical tables (mock/dry-run enabled).", len(valid_events))
                
        except Exception as e:
            logger.error("Failed to ingest source [%s]: %s", s_id, str(e), exc_info=True)
            
    logger.info("=== Ingestion Run Completed ===")
    logger.info("Total Rows In: %d | Valid Rows: %d | Quarantined Rows: %d", total_processed, total_valid, total_rejected)
    return total_valid

def main():
    parser = argparse.ArgumentParser(description="Natural Hazard Ingestion CLI (Team 1: Earthquakes)")
    parser.add_argument("--mode", choices=["historical", "incremental"], required=True,
                        help="Ingestion mode: historical (bulk partitioned) or incremental (real-time stream)")
    parser.add_argument("--source", type=str, default=None,
                        help="Optional specific source ID to execute (e.g. usgs_fdsn, emsc_fdsn, gdacs_realtime)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Execute parsing and validation gates without writing to the database")
    
    args = parser.parse_args()
    run_ingestion(mode=args.mode, source_filter=args.source, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
