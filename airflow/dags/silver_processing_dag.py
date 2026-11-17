"""
Silver Processing DAG — Team 1: Earthquake Activity Monitoring
Data Analysis Programming — Semester 2026-II

Monitors datalake_bronze/ for raw earthquake JSON payloads,
applies comprehensive data cleansing, normalization, deduplication, and validation,
and outputs:
1. Curated relational records in PostgreSQL (earthquakes_curated).
2. Traceable Parquet files in datalake_silver/ (earthquakes_silver_YYYYMMDD_HHMMSS.parquet).
"""

import json
import logging
import math
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from psycopg2.extras import execute_values

logger = logging.getLogger("airflow.task")

BRONZE_DIR = Path("/opt/airflow/datalake_bronze")
SILVER_DIR = Path("/opt/airflow/datalake_silver")
CONN_ID = "postgres_etl"
FILE_PATTERN = re.compile(r"earthquakes_(\d{8}_\d{6})\.json$")

DDL_CURATED = """
CREATE TABLE IF NOT EXISTS earthquakes_curated (
    event_id        TEXT PRIMARY KEY,
    event_time      TIMESTAMPTZ NOT NULL,
    magnitude       DOUBLE PRECISION NOT NULL,
    magnitude_type  TEXT,
    place           TEXT,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    depth_km        DOUBLE PRECISION,
    energy_joules   DOUBLE PRECISION,
    tsunami_alert   BOOLEAN DEFAULT FALSE,
    source_file     TEXT NOT NULL,
    curated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_curated_time ON earthquakes_curated (event_time);
CREATE INDEX IF NOT EXISTS idx_curated_mag ON earthquakes_curated (magnitude);
"""


@dag(
    dag_id="silver_processing_dag",
    schedule="*/20 * * * *",  # Runs every 20 minutes (or manual trigger)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["silver", "curation", "parquet", "postgres", "team_1"],
    description="Clean, validate, and curate bronze earthquake data to silver Parquet & PostgreSQL",
)
def silver_processing_pipeline():

    @task
    def prepare_database():
        """Ensure PostgreSQL schema and destination tables exist."""
        try:
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            hook.run(DDL_CURATED)
            logger.info("Verified PostgreSQL earthquakes_curated table definition.")
        except Exception as e:
            logger.warning("PostgreSQL setup skipped or pending connection: %s", e)

    @task
    def find_and_load_bronze_records() -> list:
        """Scan datalake_bronze/ for raw JSON files and extract event features."""
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        bronze_files = sorted(BRONZE_DIR.glob("earthquakes_*.json"))
        if not bronze_files:
            logger.info("No Bronze files found in %s", BRONZE_DIR)
            return []

        all_records = []
        for file_path in bronze_files:
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    payload = json.load(f)
                features = payload.get("features", [])
                for feat in features:
                    props = feat.get("properties") or {}
                    geom = feat.get("geometry") or {}
                    coords = geom.get("coordinates") or [None, None, None]

                    all_records.append({
                        "event_id": feat.get("id"),
                        "raw_time": props.get("time"),
                        "magnitude": props.get("mag"),
                        "mag_type": props.get("magType"),
                        "place": props.get("place"),
                        "longitude": coords[0] if len(coords) > 0 else None,
                        "latitude": coords[1] if len(coords) > 1 else None,
                        "depth_km": coords[2] if len(coords) > 2 else None,
                        "tsunami": bool(props.get("tsunami", 0)),
                        "source_file": file_path.name,
                    })
            except Exception as e:
                logger.error("Error reading file %s: %s", file_path, e)

        logger.info("Extracted %d raw records across %d Bronze files", len(all_records), len(bronze_files))
        return all_records

    @task
    def clean_and_normalize(records: list) -> list:
        """Apply data cleansing, coordinate bounds checking, null handling, and deduplication."""
        if not records:
            logger.info("No records to process.")
            return []

        df = pd.DataFrame(records)
        initial_count = len(df)

        # 1. Null handling on mandatory primary keys and measurements
        df = df.dropna(subset=["event_id", "magnitude", "raw_time", "latitude", "longitude"])
        logger.info("After dropping missing mandatory fields: %d records (dropped %d)", len(df), initial_count - len(df))

        # 2. Type conversions
        df["magnitude"] = pd.to_numeric(df["magnitude"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["depth_km"] = pd.to_numeric(df["depth_km"], errors="coerce").fillna(10.0)

        # 3. Validation Bounds Checking
        valid_coords = (
            (df["latitude"] >= -90.0) & (df["latitude"] <= 90.0) &
            (df["longitude"] >= -180.0) & (df["longitude"] <= 180.0)
        )
        valid_mag = (df["magnitude"] >= -2.0) & (df["magnitude"] <= 10.0)
        df = df[valid_coords & valid_mag]

        # 4. Standardization: Time to ISO-8601 UTC string (JSON/XCom serializable)
        df["event_time"] = pd.to_datetime(df["raw_time"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # 5. Text Normalization: trim, title case, null replacement
        df["place"] = df["place"].fillna("Unknown Location").astype(str).str.strip()
        df["magnitude_type"] = df["mag_type"].fillna("unknown").astype(str).str.lower().str.strip()

        # 6. Geophysical Feature Engineering: Radiated Seismic Energy in Joules (Gutenberg-Richter)
        # E = 10^(4.8 + 1.5 * M)
        df["energy_joules"] = df["magnitude"].apply(lambda m: 10 ** (4.8 + 1.5 * float(m)))

        # 7. Deduplication: Keep most recent record per event_id
        df = df.drop_duplicates(subset=["event_id"], keep="last")

        logger.info("Cleansing complete: %d curated records ready for Silver persistence", len(df))
        return df.to_dict(orient="records")

    @task
    def persist_to_silver_parquet(curated_records: list) -> str:
        """Write curated records to a timestamped Parquet file in datalake_silver/."""
        if not curated_records:
            logger.info("No curated records to write to Parquet.")
            return ""

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        parquet_filename = f"earthquakes_silver_{stamp}.parquet"
        target_path = SILVER_DIR / parquet_filename

        df = pd.DataFrame(curated_records)
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        df.to_parquet(target_path, engine="pyarrow", index=False)

        logger.info("Wrote Silver Parquet file: %s (%d rows, %.2f KB)", target_path, len(df), target_path.stat().st_size / 1024.0)
        return str(target_path)

    @task
    def persist_to_postgres(curated_records: list):
        """Upsert curated records into PostgreSQL table earthquakes_curated."""
        if not curated_records:
            return

        try:
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            cursor = conn.cursor()

            insert_query = """
            INSERT INTO earthquakes_curated (
                event_id, event_time, magnitude, magnitude_type, place,
                latitude, longitude, depth_km, energy_joules, tsunami_alert, source_file
            ) VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET
                event_time = EXCLUDED.event_time,
                magnitude = EXCLUDED.magnitude,
                magnitude_type = EXCLUDED.magnitude_type,
                place = EXCLUDED.place,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                depth_km = EXCLUDED.depth_km,
                energy_joules = EXCLUDED.energy_joules,
                tsunami_alert = EXCLUDED.tsunami_alert,
                source_file = EXCLUDED.source_file,
                curated_at = NOW();
            """

            rows = [
                (
                    r["event_id"],
                    r["event_time"],
                    r["magnitude"],
                    r["magnitude_type"],
                    r["place"],
                    r["latitude"],
                    r["longitude"],
                    r["depth_km"],
                    r["energy_joules"],
                    r["tsunami"],
                    r["source_file"],
                )
                for r in curated_records
            ]

            execute_values(cursor, insert_query, rows)
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("Successfully upserted %d records into PostgreSQL earthquakes_curated", len(rows))
        except Exception as e:
            logger.warning("PostgreSQL upsert skipped or encountered issue: %s", e)

    # FileSensor operator detecting new raw JSON files in datalake_bronze/
    wait_for_bronze_files = FileSensor(
        task_id="wait_for_bronze_files",
        filepath="datalake_bronze/earthquakes_*.json",
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=300,
        mode="poke",
    )

    # DAG Task Dependency Flow
    db_prep = prepare_database()
    raw_records = find_and_load_bronze_records()
    clean_data = clean_and_normalize(raw_records)
    write_parquet = persist_to_silver_parquet(clean_data)
    write_db = persist_to_postgres(clean_data)

    wait_for_bronze_files >> db_prep >> raw_records >> clean_data >> [write_parquet, write_db]


silver_dag = silver_processing_pipeline()
