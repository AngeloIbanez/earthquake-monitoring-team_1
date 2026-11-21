"""
Bronze Ingestion DAG — Team 1: Earthquake Activity Monitoring
Data Analysis Programming — Semester 2026-II

Extracts raw earthquake event data from multiple heterogeneous sources:
1. USGS FDSN Web Service API (GeoJSON)
2. EMSC Seismic Portal FDSN Web Service API (JSON)
3. GDACS Real-Time Disaster Alert Feed (RSS 2.0 XML)

Validates raw payloads and persists timestamped files into datalake_bronze/.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
EMSC_URL = "https://www.seismicportal.eu/fdsnws/event/1/query"
GDACS_URL = "https://www.gdacs.org/xml/rss_24h.xml"
BRONZE_DIR = Path("/opt/airflow/datalake_bronze")


def _resolve_time_window(data_interval_start=None, data_interval_end=None, dag_run=None):
    conf = (dag_run.conf or {}) if dag_run else {}
    min_mag = conf.get("minmagnitude", "2.5")
    now = datetime.now(timezone.utc)
    if "starttime" in conf and "endtime" in conf:
        start_str = conf["starttime"]
        end_str = conf["endtime"]
    elif data_interval_start and data_interval_end and (data_interval_end - data_interval_start) >= timedelta(hours=1):
        start_str = data_interval_start.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = data_interval_end.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        start_str = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
        end_str = now.strftime("%Y-%m-%dT%H:%M:%S")
    return start_str, end_str, str(min_mag)


@dag(
    dag_id="bronze_ingestion_dag",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["bronze", "ingestion", "team_1", "earthquake", "multi-source"],
    description="Fetch multi-source USGS, EMSC, and GDACS earthquake telemetry into datalake_bronze",
)
def bronze_ingestion_pipeline():

    @task
    def extract_raw_earthquakes(data_interval_start=None, data_interval_end=None, dag_run=None, **context) -> dict:
        """Fetch raw earthquake events from USGS FDSN API for the execution window."""
        start_str, end_str, min_mag = _resolve_time_window(data_interval_start, data_interval_end, dag_run)
        params = {
            "format": "geojson",
            "starttime": start_str,
            "endtime": end_str,
            "minmagnitude": min_mag,
        }
        logger.info("Querying USGS FDSN endpoint: %s [%s -> %s] (min_mag=%s)", USGS_URL, start_str, end_str, min_mag)
        response = requests.get(USGS_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        logger.info("Successfully fetched %d earthquake features from USGS", len(payload.get("features", [])))
        return payload

    @task
    def extract_emsc_earthquakes(data_interval_start=None, data_interval_end=None, dag_run=None, **context) -> dict:
        """Fetch raw earthquake events from EMSC FDSN API for the execution window."""
        start_str, end_str, min_mag = _resolve_time_window(data_interval_start, data_interval_end, dag_run)
        params = {
            "format": "json",
            "starttime": start_str,
            "endtime": end_str,
            "minmag": min_mag,
            "limit": "2000",
        }
        logger.info("Querying EMSC FDSN endpoint: %s [%s -> %s] (minmag=%s)", EMSC_URL, start_str, end_str, min_mag)
        response = requests.get(EMSC_URL, params=params, timeout=30)
        if response.status_code == 204 or not response.text.strip():
            logger.info("EMSC returned 204 No Content for window.")
            return {"type": "FeatureCollection", "features": []}
        response.raise_for_status()
        payload = response.json()
        logger.info("Successfully fetched %d earthquake features from EMSC", len(payload.get("features", [])))
        return payload

    @task
    def extract_gdacs_alerts(**context) -> str:
        """Fetch active disaster alert RSS 2.0 XML feed from GDACS."""
        logger.info("Querying GDACS RSS 24h XML feed: %s", GDACS_URL)
        response = requests.get(GDACS_URL, timeout=30)
        response.raise_for_status()
        xml_text = response.text
        logger.info("Successfully fetched GDACS XML feed (%d bytes)", len(xml_text))
        return xml_text

    @task
    def validate_bronze_payload(payload: dict) -> dict:
        """Validate the structural schema of a GeoJSON/JSON FeatureCollection response."""
        if not isinstance(payload, dict):
            raise ValueError("Payload is not a valid JSON dictionary")
        features = payload.get("features", [])
        if not isinstance(features, list):
            raise ValueError("'features' field must be a list")
        logger.info("Schema validation passed: %d features found in collection", len(features))
        return payload

    @task
    def persist_to_bronze(
        usgs_payload: dict,
        emsc_payload: dict,
        gdacs_xml: str,
        **context,
    ) -> list:
        """Persist validated USGS, EMSC, and GDACS raw files into datalake_bronze/."""
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        persisted_files = []

        # 1. USGS file (preserves earthquakes_YYYYMMDD_HHMMSS.json convention for FileSensor)
        usgs_path = BRONZE_DIR / f"earthquakes_{stamp}.json"
        with usgs_path.open("w", encoding="utf-8") as f:
            json.dump(usgs_payload, f, ensure_ascii=False, indent=2)
        persisted_files.append(str(usgs_path))
        logger.info("Persisted USGS Bronze file: %s (%d features)", usgs_path, len(usgs_payload.get("features", [])))

        # 2. EMSC file
        emsc_path = BRONZE_DIR / f"emsc_{stamp}.json"
        with emsc_path.open("w", encoding="utf-8") as f:
            json.dump(emsc_payload, f, ensure_ascii=False, indent=2)
        persisted_files.append(str(emsc_path))
        logger.info("Persisted EMSC Bronze file: %s (%d features)", emsc_path, len(emsc_payload.get("features", [])))

        # 3. GDACS XML file
        if gdacs_xml:
            gdacs_path = BRONZE_DIR / f"gdacs_{stamp}.xml"
            gdacs_path.write_text(gdacs_xml, encoding="utf-8")
            persisted_files.append(str(gdacs_path))
            logger.info("Persisted GDACS Bronze file: %s", gdacs_path)

        return persisted_files

    usgs_raw = extract_raw_earthquakes()
    emsc_raw = extract_emsc_earthquakes()
    gdacs_raw = extract_gdacs_alerts()

    usgs_valid = validate_bronze_payload(usgs_raw)
    emsc_valid = validate_bronze_payload(emsc_raw)
    persist_to_bronze(usgs_valid, emsc_valid, gdacs_raw)


bronze_dag = bronze_ingestion_pipeline()
