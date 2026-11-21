"""
Database persistence layer for the Natural Hazard Monitoring System (PostgreSQL).
Manages schema initialization, source catalog synchronization, audit runs,
idempotent canonical event/earthquake upserts, quarantine storage, and status reporting.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

try:
    import psycopg2
    from psycopg2.extras import execute_values, Json
except ImportError:
    psycopg2 = None
    execute_values = None
    Json = lambda x: json.dumps(x)

from .config import get_db_connection_params
from .models import CanonicalEvent, EarthquakeDetail

logger = logging.getLogger("src.db")

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def get_db_connection():
    """Creates and returns a new PostgreSQL connection using environment/config parameters."""
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is not installed in the active Python environment. "
            "Install dependencies via 'pip install -r requirements.txt' or run with '--dry-run'."
        )
    params = get_db_connection_params()
    return psycopg2.connect(**params)


def ensure_schema(conn) -> None:
    """Executes sql/schema.sql idempotently to ensure all canonical tables exist."""
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"DDL schema file not found at {SCHEMA_PATH}")
    sql_ddl = SCHEMA_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_ddl)
    conn.commit()


def sync_sources(conn, sources_cfg: Dict[str, Any]) -> None:
    """Synchronizes all configured data sources from config/sources.yaml into the 'source' table."""
    rows = []
    for s_key, s_data in sources_cfg.items():
        s_id = s_data.get("id", s_key)
        name = s_data.get("name", s_id)
        url = s_data.get("feed_url") or s_data.get("base_url") or ""
        fmt = s_data.get("format", "unknown")
        lic = s_data.get("license", "Open Data")
        rows.append((s_id, name[:150], url[:255], fmt[:20], lic[:100]))

    if not rows:
        return

    query = """
    INSERT INTO source (source_id, name, url, format, license)
    VALUES %s
    ON CONFLICT (source_id) DO UPDATE SET
        name = EXCLUDED.name,
        url = EXCLUDED.url,
        format = EXCLUDED.format,
        license = EXCLUDED.license;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, rows)
    conn.commit()


def start_ingestion_run(conn, source_id: str, mode: str) -> int:
    """Creates an audit record in 'ingestion_run' with status RUNNING and returns run_id."""
    query = """
    INSERT INTO ingestion_run (source_id, mode, started_at, status, rows_in, rows_new, rows_rejected)
    VALUES (%s, %s, CURRENT_TIMESTAMP, 'RUNNING', 0, 0, 0)
    RETURNING run_id;
    """
    with conn.cursor() as cur:
        cur.execute(query, (source_id, mode))
        run_id = cur.fetchone()[0]
    conn.commit()
    return int(run_id)


def finish_ingestion_run(
    conn,
    run_id: int,
    status: str,
    rows_in: int,
    rows_new: int,
    rows_rejected: int,
) -> None:
    """Updates an existing 'ingestion_run' audit record upon completion or failure."""
    query = """
    UPDATE ingestion_run
    SET finished_at = CURRENT_TIMESTAMP,
        status = %s,
        rows_in = %s,
        rows_new = %s,
        rows_rejected = %s
    WHERE run_id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (status, rows_in, rows_new, rows_rejected, run_id))
    conn.commit()


def upsert_events_and_earthquakes(
    conn,
    events: List[CanonicalEvent],
    earthquakes: List[EarthquakeDetail],
) -> int:
    """
    Idempotently upserts valid CanonicalEvent and EarthquakeDetail rows into PostgreSQL.
    Deduplicates within the in-memory batch by event_id before executing bulk SQL upserts.
    Returns the number of genuinely new events inserted (rows_new).
    """
    if not events:
        return 0

    # Deduplicate by event_id within the batch (keep last occurrence)
    dedup_events: Dict[str, CanonicalEvent] = {}
    for ev in events:
        dedup_events[ev.event_id] = ev

    dedup_eq: Dict[str, EarthquakeDetail] = {}
    for eq in earthquakes:
        if eq.event_id in dedup_events:
            dedup_eq[eq.event_id] = eq

    event_ids = list(dedup_events.keys())

    with conn.cursor() as cur:
        # Check which event_ids already exist to compute accurate rows_new count
        cur.execute(
            "SELECT event_id FROM event WHERE event_id = ANY(%s);",
            (event_ids,),
        )
        existing_ids = {row[0] for row in cur.fetchall()}
        rows_new = len(event_ids) - len(existing_ids)

        event_rows = [
            (
                ev.event_id,
                ev.source_id,
                ev.event_type,
                ev.timestamp,
                float(ev.latitude),
                float(ev.longitude),
                float(ev.severity),
                (ev.severity_unit or "unk")[:20],
                ev.raw_reference,
                ev.updated_at or datetime.now(timezone.utc),
            )
            for ev in dedup_events.values()
        ]

        upsert_event_sql = """
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
        """
        execute_values(cur, upsert_event_sql, event_rows, page_size=500)

        if dedup_eq:
            eq_rows = [
                (
                    eq.event_id,
                    eq.depth_km,
                    (eq.magnitude_type or "unk")[:20],
                    eq.place,
                    bool(eq.tsunami_flag),
                )
                for eq in dedup_eq.values()
            ]
            upsert_eq_sql = """
            INSERT INTO earthquake (
                event_id, depth_km, magnitude_type, place, tsunami_flag
            ) VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET
                depth_km = EXCLUDED.depth_km,
                magnitude_type = EXCLUDED.magnitude_type,
                place = EXCLUDED.place,
                tsunami_flag = EXCLUDED.tsunami_flag;
            """
            execute_values(cur, upsert_eq_sql, eq_rows, page_size=500)

    return max(0, rows_new)


def insert_rejected_records(
    conn,
    run_id: int,
    source_id: str,
    rejected_items: List[Tuple[CanonicalEvent, str]],
) -> int:
    """
    Persists validation failures into the 'rejected_record' quarantine table.
    Each item is a tuple of (CanonicalEvent, reason).
    """
    if not rejected_items:
        return 0

    rows = []
    for ev, reason in rejected_items:
        payload = ev.raw_payload if isinstance(ev.raw_payload, dict) else {
            "event_id": ev.event_id,
            "source_id": ev.source_id,
            "timestamp": ev.timestamp.isoformat() if ev.timestamp else None,
            "latitude": ev.latitude,
            "longitude": ev.longitude,
            "severity": ev.severity,
        }
        rows.append((run_id, source_id, reason, Json(payload)))

    query = """
    INSERT INTO rejected_record (run_id, source_id, reason, payload)
    VALUES %s;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, rows, page_size=500)

    return len(rows)


def update_watermark(conn, source_id: str, watermark: str) -> None:
    """Upserts the incremental high-water mark for a source after a successful commit."""
    query = """
    INSERT INTO ingestion_state (source_id, watermark, last_success)
    VALUES (%s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (source_id) DO UPDATE SET
        watermark = EXCLUDED.watermark,
        last_success = CURRENT_TIMESTAMP;
    """
    with conn.cursor() as cur:
        cur.execute(query, (source_id, str(watermark)[:100]))


def get_watermark(conn, source_id: str) -> Optional[str]:
    """Retrieves the stored high-water mark timestamp string for a source from ingestion_state."""
    with conn.cursor() as cur:
        cur.execute("SELECT watermark FROM ingestion_state WHERE source_id = %s;", (source_id,))
        row = cur.fetchone()
    return row[0] if row else None


def load_all_canonical_events(conn) -> List[CanonicalEvent]:
    """Loads all stored canonical events from PostgreSQL for cross-source event matching."""
    query = """
    SELECT event_id, source_id, event_type, timestamp, latitude, longitude, severity, severity_unit, raw_reference, updated_at
    FROM event
    ORDER BY timestamp ASC;
    """
    events: List[CanonicalEvent] = []
    with conn.cursor() as cur:
        cur.execute(query)
        for row in cur.fetchall():
            events.append(
                CanonicalEvent(
                    event_id=row[0],
                    source_id=row[1],
                    event_type=row[2],
                    timestamp=row[3],
                    latitude=row[4],
                    longitude=row[5],
                    severity=row[6],
                    severity_unit=row[7],
                    raw_reference=row[8],
                    updated_at=row[9],
                )
            )
    return events


def upsert_event_groups(conn, group_rows: List[Tuple[str, str]]) -> int:
    """Idempotently persists cross-source event_group (group_id, event_id) pairs."""
    if not group_rows:
        return 0

    dedup_rows = sorted(set(group_rows))
    query = """
    INSERT INTO event_group (group_id, event_id)
    VALUES %s
    ON CONFLICT (group_id, event_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, dedup_rows, page_size=500)
    conn.commit()
    return len(dedup_rows)


def get_pipeline_status(conn) -> Dict[str, Any]:
    """Retrieves row counts, watermarks, and recent ingestion runs for '--mode status'."""
    status_info: Dict[str, Any] = {
        "counts": {},
        "watermarks": [],
        "recent_runs": [],
        "source_breakdown": [],
    }
    tables = [
        "source",
        "event",
        "earthquake",
        "event_group",
        "rejected_record",
        "ingestion_run",
        "ingestion_state",
    ]
    with conn.cursor() as cur:
        for tbl in tables:
            cur.execute(f"SELECT COUNT(*) FROM {tbl};")
            status_info["counts"][tbl] = cur.fetchone()[0]

        cur.execute(
            """
            SELECT source_id, COUNT(*), MIN(timestamp), MAX(timestamp)
            FROM event
            GROUP BY source_id
            ORDER BY source_id;
            """
        )
        for row in cur.fetchall():
            status_info["source_breakdown"].append(
                {
                    "source_id": row[0],
                    "event_count": row[1],
                    "min_timestamp": row[2].isoformat() if row[2] else None,
                    "max_timestamp": row[3].isoformat() if row[3] else None,
                }
            )

        cur.execute(
            """
            SELECT source_id, watermark, last_success
            FROM ingestion_state
            ORDER BY source_id;
            """
        )
        for row in cur.fetchall():
            status_info["watermarks"].append(
                {
                    "source_id": row[0],
                    "watermark": row[1],
                    "last_success": row[2].isoformat() if row[2] else None,
                }
            )

        cur.execute(
            """
            SELECT run_id, source_id, mode, status, rows_in, rows_new, rows_rejected, started_at, finished_at
            FROM ingestion_run
            ORDER BY run_id DESC
            LIMIT 10;
            """
        )
        for row in cur.fetchall():
            status_info["recent_runs"].append(
                {
                    "run_id": row[0],
                    "source_id": row[1],
                    "mode": row[2],
                    "status": row[3],
                    "rows_in": row[4],
                    "rows_new": row[5],
                    "rows_rejected": row[6],
                    "started_at": row[7].isoformat() if row[7] else None,
                    "finished_at": row[8].isoformat() if row[8] else None,
                }
            )

    return status_info
