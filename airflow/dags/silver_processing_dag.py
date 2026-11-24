"""
Silver Processing DAG — Team 1: Earthquake Activity Monitoring
Data Analysis Programming — Semester 2026-II

Monitors datalake_bronze/ for raw earthquake payloads (USGS GeoJSON, EMSC JSON, GDACS XML),
applies comprehensive data cleansing, normalization, deduplication, validation, and quarantine,
outputs:
1. Canonical relational records in PostgreSQL with per-source ingestion_run auditing.
2. Curated relational records in PostgreSQL (earthquakes_curated) for backward compatibility.
3. Traceable Parquet files in datalake_silver/ (earthquakes_silver_YYYYMMDD_HHMMSS.parquet).
4. Chained trigger to gold_processing_dag (gated by ShortCircuitOperator cooldown).
"""

from collections import defaultdict
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from psycopg2.extras import execute_values, Json

from src.parsers import parse_usgs_geojson, parse_emsc_json, parse_gdacs_xml

logger = logging.getLogger("airflow.task")

BRONZE_DIR = Path("/opt/airflow/datalake_bronze")
SILVER_DIR = Path("/opt/airflow/datalake_silver")
GOLD_DIR = Path("/opt/airflow/datalake_gold")
SCHEMA_FILE = Path("/opt/airflow/sql/schema.sql")
CONN_ID = "postgres_etl"
DEFAULT_GOLD_MIN_INTERVAL_MINUTES = 15

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

DEFAULT_SOURCES = [
    ("usgs_fdsn", "USGS FDSN Event Web Service", "https://earthquake.usgs.gov/fdsnws/event/1/query", "geojson", "Public Domain"),
    ("emsc_fdsn", "EMSC Seismic Portal FDSN Web Service", "https://www.seismicportal.eu/fdsnws/event/1/query", "json", "Open Data"),
    ("usgs_realtime", "USGS Real-time GeoJSON Feed", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson", "geojson", "Public Domain"),
    ("gdacs_realtime", "GDACS RSS 24h Alert Feed", "https://www.gdacs.org/xml/rss_24h.xml", "xml-rss2", "Open Access"),
]


def _resolve_bronze_source_id(filename: str) -> str:
    """Maps a Bronze filename prefix to its canonical `source_id`."""
    if filename.startswith(("usgs_realtime_", "usgs_incremental_")):
        return "usgs_realtime"
    if filename.startswith("earthquakes_") or filename.startswith("usgs_fdsn_"):
        return "usgs_fdsn"
    if filename.startswith("emsc_"):
        return "emsc_fdsn"
    if filename.startswith("gdacs_"):
        return "gdacs_realtime"
    return ""


def _check_gold_cooldown(**context) -> bool:
    """
    Short-circuit gate controlling when Silver triggers `gold_processing_dag`:
    - Manual DAG runs (`run_type == 'manual'`) or explicit `{"force_gold": true}` always
      trigger Gold immediately for deterministic end-to-end verification.
    - Scheduled runs respect `GOLD_MIN_INTERVAL_MINUTES` (Airflow Variable
      `gold_min_interval_minutes` or env `GOLD_MIN_INTERVAL_MINUTES`, default 15 min)
      against the latest `governance_YYYYMMDD_HHMMSS.parquet` timestamp so PySpark
      local[*] does not launch redundant heavy JVMs back-to-back.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    run_type = getattr(dag_run, "run_type", "") if dag_run else ""

    if run_type == "manual" or bool(conf.get("force_gold")):
        logger.info("Manual/forced Silver run detected (run_type=%s); triggering Gold DAG immediately.", run_type)
        return True

    raw_min = Variable.get(
        "gold_min_interval_minutes",
        default_var=os.getenv("GOLD_MIN_INTERVAL_MINUTES", str(DEFAULT_GOLD_MIN_INTERVAL_MINUTES)),
    )
    try:
        min_interval = max(0, int(raw_min))
    except (TypeError, ValueError):
        min_interval = DEFAULT_GOLD_MIN_INTERVAL_MINUTES

    if min_interval <= 0 or not GOLD_DIR.exists():
        return True

    gov_files = sorted(GOLD_DIR.glob("governance_*.parquet"))
    if not gov_files:
        return True

    latest_name = gov_files[-1].name
    m = re.search(r"(\d{8}_\d{6})", latest_name)
    if not m:
        return True

    try:
        last_dt = datetime.strptime(m.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60.0
        if age_min < min_interval:
            logger.info(
                "Skipping Gold trigger: latest Gold (%s) is %.1f min old (< %d min cooldown).",
                latest_name,
                age_min,
                min_interval,
            )
            return False
    except ValueError:
        pass

    return True


@dag(
    dag_id="silver_processing_dag",
    schedule="*/20 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["silver", "curation", "parquet", "postgres", "team_1", "canonical"],
    description="Clean, validate, quarantine, and curate multi-source bronze earthquake data to silver Parquet & PostgreSQL",
)
def silver_processing_pipeline():

    @task
    def prepare_database():
        """Ensure PostgreSQL canonical schema (sql/schema.sql) and earthquakes_curated exist."""
        try:
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            with conn.cursor() as cur:
                if SCHEMA_FILE.exists():
                    cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))
                cur.execute(DDL_CURATED)
                execute_values(
                    cur,
                    """
                    INSERT INTO source (source_id, name, url, format, license)
                    VALUES %s
                    ON CONFLICT (source_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        url = EXCLUDED.url,
                        format = EXCLUDED.format,
                        license = EXCLUDED.license;
                    """,
                    DEFAULT_SOURCES,
                )
            conn.commit()
            conn.close()
            logger.info("Verified canonical PostgreSQL schema, source catalog, and earthquakes_curated table.")
        except Exception as e:
            logger.warning("PostgreSQL setup skipped or pending connection: %s", e)

    @task
    def find_and_parse_bronze_records() -> dict:
        """Scan datalake_bronze/ for raw USGS, EMSC, and GDACS files, parse, and run validation gate."""
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        bronze_files = sorted(
            list(BRONZE_DIR.glob("earthquakes_*.json"))
            + list(BRONZE_DIR.glob("usgs_realtime_*.json"))
            + list(BRONZE_DIR.glob("emsc_*.json"))
            + list(BRONZE_DIR.glob("gdacs_*.xml"))
        )
        if not bronze_files:
            logger.info("No Bronze files found in %s", BRONZE_DIR)
            return {"valid": [], "rejected": []}

        valid_records = []
        rejected_records = []

        for file_path in bronze_files:
            fname = file_path.name
            source_id = _resolve_bronze_source_id(fname)
            if not source_id:
                continue
            try:
                if source_id in ("usgs_fdsn", "usgs_realtime"):
                    payload = json.loads(file_path.read_text(encoding="utf-8"))
                    events, earthquakes = parse_usgs_geojson(payload, source_id=source_id)
                elif source_id == "emsc_fdsn":
                    payload = json.loads(file_path.read_text(encoding="utf-8"))
                    events, earthquakes = parse_emsc_json(payload, source_id=source_id)
                elif source_id == "gdacs_realtime":
                    xml_text = file_path.read_text(encoding="utf-8")
                    events, earthquakes = parse_gdacs_xml(xml_text, source_id=source_id)
                else:
                    continue

                eq_map = {eq.event_id: eq for eq in earthquakes}
                for ev in events:
                    is_ok, reason = ev.validate()
                    eq = eq_map.get(ev.event_id)
                    if is_ok:
                        raw_ms = int(ev.timestamp.timestamp() * 1000)
                        mag_t = (eq.magnitude_type if eq else ev.severity_unit) or "unk"
                        valid_records.append(
                            {
                                "event_id": ev.event_id,
                                "raw_time": raw_ms,
                                "magnitude": float(ev.severity),
                                "mag_type": mag_t,
                                "place": (eq.place if eq else "Unknown Location") or "Unknown Location",
                                "longitude": float(ev.longitude),
                                "latitude": float(ev.latitude),
                                "depth_km": float(eq.depth_km) if (eq and eq.depth_km is not None) else 10.0,
                                "tsunami": bool(eq.tsunami_flag) if eq else False,
                                "source_file": fname,
                                "event_time": ev.timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                "magnitude_type": mag_t,
                                "source_id": ev.source_id,
                                "event_type": ev.event_type,
                                "updated_at": (ev.updated_at or ev.timestamp).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                "raw_reference": ev.raw_reference,
                            }
                        )
                    else:
                        rejected_records.append(
                            {
                                "source_id": ev.source_id,
                                "reason": reason or "validation_failed",
                                "payload": ev.raw_payload or {"event_id": ev.event_id, "source_file": fname},
                            }
                        )
            except Exception as e:
                logger.error("Error reading Bronze file %s: %s", file_path, e)

        logger.info(
            "Processed %d Bronze files: %d valid records, %d rejected records",
            len(bronze_files),
            len(valid_records),
            len(rejected_records),
        )
        return {"valid": valid_records, "rejected": rejected_records}

    @task
    def clean_and_normalize(parsed_bundle: dict) -> dict:
        """Apply deduplication and geophysical feature engineering (radiated energy in Joules)."""
        records = parsed_bundle.get("valid", [])
        rejected = parsed_bundle.get("rejected", [])
        if not records:
            return {"valid": [], "rejected": rejected}

        df = pd.DataFrame(records)
        df["energy_joules"] = df["magnitude"].apply(lambda m: 10 ** (4.8 + 1.5 * float(m)))
        df = df.drop_duplicates(subset=["event_id"], keep="last")

        # Deduplicate rejected records in-memory by (source_id, reason, canonical JSON digest)
        seen_rej = set()
        dedup_rejected = []
        for r in rejected:
            digest = hashlib.md5(json.dumps(r["payload"], sort_keys=True, default=str).encode("utf-8")).hexdigest()
            key = (r["source_id"], r["reason"], digest)
            if key not in seen_rej:
                seen_rej.add(key)
                dedup_rejected.append(r)

        logger.info("Cleansing complete: %d unique curated records ready for Silver persistence", len(df))
        return {"valid": df.to_dict(orient="records"), "rejected": dedup_rejected}

    @task
    def persist_to_silver_parquet(curated_bundle: dict) -> str:
        """Write curated records to a timestamped Parquet file in datalake_silver/ with uniform 13-column schema."""
        curated_records = curated_bundle.get("valid", [])
        if not curated_records:
            logger.info("No curated records to write to Parquet.")
            return ""

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        target_path = SILVER_DIR / f"earthquakes_silver_{stamp}.parquet"

        df = pd.DataFrame(curated_records)
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        silver_cols = [
            "event_id",
            "raw_time",
            "magnitude",
            "mag_type",
            "place",
            "longitude",
            "latitude",
            "depth_km",
            "tsunami",
            "source_file",
            "event_time",
            "magnitude_type",
            "energy_joules",
        ]
        df = df[silver_cols]
        df.to_parquet(
            target_path,
            engine="pyarrow",
            index=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

        logger.info("Wrote Silver Parquet file: %s (%d rows)", target_path, len(df))
        return str(target_path)

    @task
    def persist_to_postgres(curated_bundle: dict):
        """
        Upserts curated records, tracks per-source `ingestion_run` audit rows with
        exact new/updated/rejected counts, and quarantines rejected records idempotently.
        """
        curated_records = curated_bundle.get("valid", [])
        rejected_records = curated_bundle.get("rejected", [])
        if not curated_records and not rejected_records:
            return

        by_source_valid = defaultdict(list)
        for r in curated_records:
            by_source_valid[r["source_id"]].append(r)

        by_source_rej = defaultdict(list)
        for r in rejected_records:
            by_source_rej[r["source_id"]].append(r)

        active_sources = sorted(set(by_source_valid.keys()) | set(by_source_rej.keys()))

        try:
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            cur = conn.cursor()

            # Check existing event_ids prior to upsert for accurate per-source rows_new tracking
            all_ids = [r["event_id"] for r in curated_records]
            existing_ids = set()
            if all_ids:
                cur.execute("SELECT event_id FROM event WHERE event_id = ANY(%s);", (all_ids,))
                existing_ids = {row[0] for row in cur.fetchall()}

            # 1. Create per-source `ingestion_run` audit entries
            run_ids_by_source = {}
            rows_new_by_source = {}
            for s_id in active_sources:
                s_valid = by_source_valid[s_id]
                s_rej = by_source_rej[s_id]
                s_in = len(s_valid) + len(s_rej)
                s_new = sum(1 for r in s_valid if r["event_id"] not in existing_ids)
                rows_new_by_source[s_id] = s_new

                cur.execute(
                    """
                    INSERT INTO ingestion_run (source_id, mode, started_at, status, rows_in, rows_new, rows_rejected)
                    VALUES (%s, 'incremental', CURRENT_TIMESTAMP, 'RUNNING', %s, 0, %s)
                    RETURNING run_id;
                    """,
                    (s_id, s_in, len(s_rej)),
                )
                run_ids_by_source[s_id] = cur.fetchone()[0]

            # 2. Upsert into canonical 'event' and 'earthquake' tables
            if curated_records:
                event_rows = [
                    (
                        r["event_id"],
                        r["source_id"],
                        r.get("event_type", "earthquake"),
                        r["event_time"],
                        r["latitude"],
                        r["longitude"],
                        r["magnitude"],
                        r["magnitude_type"][:20],
                        r.get("raw_reference"),
                        r.get("updated_at") or r["event_time"],
                    )
                    for r in curated_records
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO event (
                        event_id, source_id, event_type, timestamp,
                        latitude, longitude, severity, severity_unit, raw_reference, updated_at
                    ) VALUES %s
                    ON CONFLICT (event_id) DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        severity = EXCLUDED.severity,
                        severity_unit = EXCLUDED.severity_unit,
                        raw_reference = EXCLUDED.raw_reference,
                        updated_at = EXCLUDED.updated_at;
                    """,
                    event_rows,
                )

                eq_rows = [
                    (
                        r["event_id"],
                        r["depth_km"],
                        r["magnitude_type"][:20],
                        r["place"],
                        r["tsunami"],
                    )
                    for r in curated_records
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO earthquake (
                        event_id, depth_km, magnitude_type, place, tsunami_flag
                    ) VALUES %s
                    ON CONFLICT (event_id) DO UPDATE SET
                        depth_km = EXCLUDED.depth_km,
                        magnitude_type = EXCLUDED.magnitude_type,
                        place = EXCLUDED.place,
                        tsunami_flag = EXCLUDED.tsunami_flag;
                    """,
                    eq_rows,
                )

                # Backward-compatible upsert into earthquakes_curated
                curated_rows = [
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
                execute_values(
                    cur,
                    """
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
                    """,
                    curated_rows,
                )

            # 3. Update ingestion_state watermarks per source
            by_src_max_ts = {}
            for r in curated_records:
                s_id = r["source_id"]
                ts_val = r.get("updated_at") or r["event_time"]
                if s_id not in by_src_max_ts or ts_val > by_src_max_ts[s_id]:
                    by_src_max_ts[s_id] = ts_val

            for s_id, max_ts in by_src_max_ts.items():
                cur.execute(
                    """
                    INSERT INTO ingestion_state (source_id, watermark, last_success)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (source_id) DO UPDATE SET
                        watermark = EXCLUDED.watermark,
                        last_success = CURRENT_TIMESTAMP;
                    """,
                    (s_id, str(max_ts)[:100]),
                )

            # 4. Cross-source event matching into event_group
            from src.models import CanonicalEvent
            from src.matching import build_cross_source_event_groups

            canonical_objs = [
                CanonicalEvent(
                    event_id=r["event_id"],
                    source_id=r["source_id"],
                    event_type=r.get("event_type", "earthquake"),
                    timestamp=datetime.fromisoformat(r["event_time"].replace("Z", "+00:00")),
                    latitude=float(r["latitude"]),
                    longitude=float(r["longitude"]),
                    severity=float(r["magnitude"]),
                    severity_unit=r["magnitude_type"],
                )
                for r in curated_records
            ]
            group_rows, _ = build_cross_source_event_groups(canonical_objs)
            if group_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO event_group (group_id, event_id)
                    VALUES %s
                    ON CONFLICT (group_id, event_id) DO NOTHING;
                    """,
                    group_rows,
                )

            # 5. Idempotently quarantine rejected records linked to their source's run_id
            if rejected_records:
                cur.execute("SELECT source_id, reason, payload::text FROM rejected_record;")
                existing_rej = {
                    (row[0], row[1], json.dumps(json.loads(row[2]), sort_keys=True))
                    for row in cur.fetchall()
                    if row[2]
                }
                new_rej_rows = []
                for r in rejected_records:
                    norm_json = json.dumps(r["payload"], sort_keys=True, default=str)
                    if (r["source_id"], r["reason"], norm_json) not in existing_rej:
                        new_rej_rows.append(
                            (run_ids_by_source[r["source_id"]], r["source_id"], r["reason"], Json(r["payload"]))
                        )
                if new_rej_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO rejected_record (run_id, source_id, reason, payload)
                        VALUES %s;
                        """,
                        new_rej_rows,
                    )

            # 6. Complete each source's ingestion_run audit entry
            for s_id, s_run_id in run_ids_by_source.items():
                s_valid_cnt = len(by_source_valid[s_id])
                s_new_cnt = rows_new_by_source[s_id]
                s_rej_cnt = len(by_source_rej[s_id])
                cur.execute(
                    """
                    UPDATE ingestion_run
                    SET finished_at = CURRENT_TIMESTAMP,
                        status = 'SUCCESS',
                        rows_in = %s,
                        rows_new = %s,
                        rows_rejected = %s
                    WHERE run_id = %s;
                    """,
                    (s_valid_cnt + s_rej_cnt, s_new_cnt, s_rej_cnt, s_run_id),
                )
                logger.info(
                    "Audit ingestion_run #%d [%s]: in=%d, new=%d, updated=%d, rejected=%d",
                    s_run_id,
                    s_id,
                    s_valid_cnt + s_rej_cnt,
                    s_new_cnt,
                    max(0, s_valid_cnt - s_new_cnt),
                    s_rej_cnt,
                )

            conn.commit()
            cur.close()
            conn.close()
            logger.info(
                "Successfully persisted %d canonical events across %d sources (%d event_group links)",
                len(curated_records),
                len(active_sources),
                len(group_rows),
            )
        except Exception as e:
            logger.warning("PostgreSQL upsert encountered issue: %s", e)

    wait_for_bronze_files = FileSensor(
        task_id="wait_for_bronze_files",
        filepath="datalake_bronze/earthquakes_*.json",
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=300,
        mode="poke",
    )

    gold_cooldown_gate = ShortCircuitOperator(
        task_id="check_gold_trigger_cooldown",
        python_callable=_check_gold_cooldown,
    )

    trigger_gold_dag = TriggerDagRunOperator(
        task_id="trigger_gold_processing",
        trigger_dag_id="gold_processing_dag",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    db_prep = prepare_database()
    parsed_bundle = find_and_parse_bronze_records()
    clean_bundle = clean_and_normalize(parsed_bundle)
    write_parquet = persist_to_silver_parquet(clean_bundle)
    write_db = persist_to_postgres(clean_bundle)

    wait_for_bronze_files >> db_prep >> parsed_bundle >> clean_bundle >> [write_parquet, write_db] >> gold_cooldown_gate >> trigger_gold_dag


silver_dag = silver_processing_pipeline()
