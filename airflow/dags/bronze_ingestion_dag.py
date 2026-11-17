"""
Bronze Ingestion DAG — Team 1: Earthquake Activity Monitoring
Data Analysis Programming — Semester 2026-II

Extracts raw earthquake event data from the USGS FDSN Web Service API,
validates the raw JSON schema payload, and persists timestamped JSON files
into the datalake_bronze/ directory.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.decorators import dag, task

logger = logging.getLogger("airflow.task")

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
BRONZE_DIR = Path("/opt/airflow/datalake_bronze")


@dag(
    dag_id="bronze_ingestion_dag",
    schedule="*/15 * * * *",  # Runs every 15 minutes (or manual trigger)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["bronze", "ingestion", "team_1", "earthquake"],
    description="Fetch real-time USGS earthquake telemetry into datalake_bronze",
)
def bronze_ingestion_pipeline():

    @task
    def extract_raw_earthquakes(data_interval_start=None, data_interval_end=None, dag_run=None, **context) -> dict:
        """Fetch raw earthquake events from USGS FDSN API for the execution window."""
        conf = (dag_run.conf or {}) if dag_run else {}
        min_mag = conf.get("minmagnitude", "1.5")
        
        now = datetime.now(timezone.utc)
        if "starttime" in conf and "endtime" in conf:
            start_str = conf["starttime"]
            end_str = conf["endtime"]
        elif data_interval_start and data_interval_end and (data_interval_end - data_interval_start) >= timedelta(hours=1):
            start_str = data_interval_start.strftime("%Y-%m-%dT%H:%M:%S")
            end_str = data_interval_end.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            # Default to rolling 24-hour window
            start_str = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
            end_str = now.strftime("%Y-%m-%dT%H:%M:%S")

        params = {
            "format": "geojson",
            "starttime": start_str,
            "endtime": end_str,
            "minmagnitude": str(min_mag),
        }
        logger.info("Querying USGS FDSN endpoint: %s with window [%s, %s] (min_mag: %s)", USGS_URL, start_str, end_str, min_mag)
        response = requests.get(USGS_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        feature_count = len(payload.get("features", []))
        logger.info("Successfully fetched %d earthquake features from USGS", feature_count)
        return payload

    @task
    def validate_bronze_payload(payload: dict) -> dict:
        """Validate the structural schema of the raw GeoJSON response."""
        if not isinstance(payload, dict):
            raise ValueError("Payload is not a valid JSON dictionary")

        if payload.get("type") != "FeatureCollection":
            logger.warning("Payload type is '%s', expected 'FeatureCollection'", payload.get("type"))

        features = payload.get("features", [])
        if not isinstance(features, list):
            raise ValueError("'features' field must be a list")

        logger.info("Schema validation passed: %d features found in collection", len(features))
        return payload

    @task
    def persist_to_bronze(payload: dict, data_interval_start=None, **context) -> str:
        """Persist the validated raw JSON payload into datalake_bronze/."""
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        filename = f"earthquakes_{stamp}.json"
        target_path = BRONZE_DIR / filename

        with target_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        file_size_kb = target_path.stat().st_size / 1024.0
        logger.info("Persisted Bronze file: %s (%.2f KB, %d features)", target_path, file_size_kb, len(payload.get("features", [])))
        return str(target_path)

    # DAG Dependency Flow
    raw_data = extract_raw_earthquakes()
    validated_data = validate_bronze_payload(raw_data)
    persist_to_bronze(validated_data)


bronze_dag = bronze_ingestion_pipeline()
