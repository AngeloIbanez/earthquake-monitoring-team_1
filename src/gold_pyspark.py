"""
Silver-to-Gold Distributed Analytical Engine using PySpark (Local Mode).
Reads all accumulated Silver Parquet files from datalake_silver/ as a unified
distributed DataFrame (spark.read.parquet("datalake_silver/")) and produces two
Gold Parquet summary datasets in datalake_gold/:
1. governance_YYYYMMDD_HHMMSS.parquet — Data Governance & Structural Health KPIs.
2. storytelling_YYYYMMDD_HHMMSS.parquet — Functional User Storytelling & SAAI Aggregations.
"""

import argparse
from collections import Counter
import json
import logging
import math
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional
from urllib.parse import urlparse, unquote

import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    import psycopg2
except ImportError:
    psycopg2 = None

from .config import get_db_connection_params
from .models import CanonicalEvent
from .parsers import parse_usgs_geojson, parse_emsc_json, parse_gdacs_xml
from .matching import build_cross_source_event_groups

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("src.gold_pyspark")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DIR = PROJECT_ROOT / "datalake_silver"
GOLD_DIR = PROJECT_ROOT / "datalake_gold"
SAMPLES_DIR = PROJECT_ROOT / "workshop_1" / "samples"

# Policy & matching parameters (documented in docs/hazard_definition.md)
MATCH_MAX_DIST_KM: float = 100.0
MATCH_MAX_TIME_SEC: float = 900.0
PINNED_DEPTH_WARN_PCT: float = 25.0
QUARANTINE_WARN_RECORDS: float = 50.0

GOVERNANCE_SCHEMA = T.StructType(
    [
        T.StructField("kpi_category", T.StringType(), False),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("source_id", T.StringType(), False),
        T.StructField("period_date", T.StringType(), False),
        T.StructField("metric_value", T.DoubleType(), True),
        T.StructField("secondary_value", T.DoubleType(), True),
        T.StructField("min_value", T.DoubleType(), True),
        T.StructField("median_value", T.DoubleType(), True),
        T.StructField("max_value", T.DoubleType(), True),
        T.StructField("threshold_value", T.DoubleType(), True),
        T.StructField("compliance_status", T.StringType(), False),
        T.StructField("unit", T.StringType(), False),
        T.StructField("computed_at", T.StringType(), False),
    ]
)


def _resolve_db_params_with_airflow_fallback() -> Dict[str, Any]:
    """
    Resolves PostgreSQL connection parameters via src.config.get_db_connection_params().
    When running inside an Airflow Docker container where POSTGRES_HOST is unset
    but AIRFLOW_CONN_POSTGRES_ETL is configured, parses host/port/db/user/password
    from AIRFLOW_CONN_POSTGRES_ETL without logging credentials.
    """
    params = get_db_connection_params()
    airflow_uri = os.getenv("AIRFLOW_CONN_POSTGRES_ETL")
    if airflow_uri and "POSTGRES_HOST" not in os.environ:
        parsed = urlparse(airflow_uri)
        if parsed.hostname:
            params["host"] = parsed.hostname
        if parsed.port:
            params["port"] = int(parsed.port)
        if parsed.path and parsed.path.lstrip("/"):
            params["database"] = parsed.path.lstrip("/")
        if parsed.username:
            params["user"] = unquote(parsed.username)
        if parsed.password:
            params["password"] = unquote(parsed.password)
    return params


def query_quarantined_records_count(connect_timeout: int = 3) -> Tuple[Optional[int], str]:
    """
    Queries the cumulative count of quarantined validation failures from PostgreSQL
    table `rejected_record` (matching the full-corpus cumulative scope of all Gold
    summary KPIs).

    Returns:
        (count, status) where status is "AVAILABLE" when PostgreSQL responded,
        or (None, "UNAVAILABLE") when PostgreSQL is offline or unreachable.
    """
    if psycopg2 is None:
        logger.warning("psycopg2 not installed; marking quarantined_records_detected as UNAVAILABLE.")
        return None, "UNAVAILABLE"

    params = _resolve_db_params_with_airflow_fallback()
    conn = None
    try:
        conn = psycopg2.connect(connect_timeout=connect_timeout, **params)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM rejected_record;")
            row = cur.fetchone()
            total_rejected = int(row[0]) if row and row[0] is not None else 0
        return total_rejected, "AVAILABLE"
    except Exception as exc:
        logger.warning(
            "PostgreSQL unavailable while querying rejected_record (%s); "
            "marking quarantined_records_detected as UNAVAILABLE.",
            type(exc).__name__,
        )
        return None, "UNAVAILABLE"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def compute_dynamic_matching_metrics(
    events: List[CanonicalEvent],
    max_dist_km: float = MATCH_MAX_DIST_KM,
    max_time_sec: float = MATCH_MAX_TIME_SEC,
) -> Dict[str, Any]:
    """
    Computes dynamic cross-source event reconciliation metrics over deduplicated
    CanonicalEvent instances using `build_cross_source_event_groups`.

    Metric definitions:
    - `cross_source_matched_pairs`: 1-to-1 nearest-neighbor pairs of events from
      distinct seismic agencies (within `max_time_sec` and `max_dist_km`, excluding
      intra-USGS `usgs_fdsn` vs `usgs_realtime` self-agency comparisons) united in
      an `event_group`.
    - `event_groups_total`: Total connected components (`event_group` clusters)
      containing >= 2 matched agency detections.
    - `group_size_distribution`: Count of groups by cluster size (e.g. 2 agencies,
      3+ agencies).
    """
    t0 = time.perf_counter()
    ordered_events = sorted(
        events,
        key=lambda e: e.timestamp.timestamp() if e.timestamp is not None else 0.0,
    )
    group_rows, match_metrics = build_cross_source_event_groups(
        ordered_events,
        max_dist_km=max_dist_km,
        max_time_sec=max_time_sec,
    )
    elapsed_sec = time.perf_counter() - t0

    members_per_group = Counter(group_id for group_id, _ in group_rows)
    size_counts = Counter(members_per_group.values())
    size_2_count = int(size_counts.get(2, 0))
    size_3plus_count = int(sum(cnt for sz, cnt in size_counts.items() if sz >= 3))

    result = {
        "cross_source_matched_pairs": int(match_metrics["matched_pairs"]),
        "event_groups_total": int(match_metrics["total_groups"]),
        "total_grouped_events": int(match_metrics["total_grouped_events"]),
        "group_size_2_count": size_2_count,
        "group_size_3plus_count": size_3plus_count,
        "group_size_distribution": {int(k): int(v) for k, v in sorted(size_counts.items())},
        "median_time_diff_sec": float(match_metrics["median_time_diff_sec"]),
        "median_dist_km": float(match_metrics["median_dist_km"]),
        "mean_mag_diff": float(match_metrics["mean_mag_diff"]),
        "elapsed_sec": round(elapsed_sec, 4),
    }
    logger.info(
        "Dynamic cross-source matching finished in %.3fs: pairs=%d, groups=%d (2-agency=%d, 3+-agency=%d)",
        elapsed_sec,
        result["cross_source_matched_pairs"],
        result["event_groups_total"],
        size_2_count,
        size_3plus_count,
    )
    return result


def _normalize_legacy_nanos_parquets(silver_dir: Path) -> None:
    """Normalizes any legacy TIMESTAMP(NANOS, true) Parquet files to TIMESTAMP(MICROS, true) for PySpark."""
    import pyarrow.parquet as pq
    for pf in sorted(silver_dir.glob("*.parquet")):
        schema = pq.read_schema(pf)
        if any("timestamp[ns" in str(field.type) for field in schema):
            legacy_df = pd.read_parquet(pf)
            legacy_df.to_parquet(
                pf,
                engine="pyarrow",
                index=False,
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )
            logger.info("Normalized legacy NANOS timestamp to MICROS in Silver file: %s", pf.name)


def ensure_multisource_silver_parquet(silver_dir: Path = SILVER_DIR) -> Path:
    """
    Ensures datalake_silver/ contains a multi-source Silver Parquet partition
    (USGS FDSN, EMSC FDSN, USGS Real-Time, GDACS XML) with the uniform 13-column
    Silver schema so PySpark folder reads cover all validated agencies.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    target = silver_dir / "earthquakes_silver_20261120_120000.parquet"
    if target.exists():
        _normalize_legacy_nanos_parquets(silver_dir)
        return target

    sample_Configs = [
        ("usgs_fdsn", "usgs_historical_raw.json", "usgs"),
        ("emsc_fdsn", "emsc_historical_raw.json", "emsc"),
        ("usgs_realtime", "usgs_incremental_raw.json", "usgs"),
        ("gdacs_realtime", "gdacs_incremental_raw.xml", "gdacs"),
    ]

    records: List[Dict[str, Any]] = []
    for s_id, fname, kind in sample_Configs:
        fpath = SAMPLES_DIR / fname
        if not fpath.exists():
            continue
        if kind == "usgs":
            evs, eqs = parse_usgs_geojson(json.loads(fpath.read_text(encoding="utf-8")), source_id=s_id)
        elif kind == "emsc":
            evs, eqs = parse_emsc_json(json.loads(fpath.read_text(encoding="utf-8")), source_id=s_id)
        else:
            evs, eqs = parse_gdacs_xml(fpath.read_text(encoding="utf-8"), source_id=s_id)

        eq_map = {eq.event_id: eq for eq in eqs}
        for ev in evs:
            is_ok, _ = ev.validate()
            if not is_ok or ev.timestamp is None:
                continue
            eq = eq_map.get(ev.event_id)
            mag = float(ev.severity)
            mag_t = (eq.magnitude_type if eq else ev.severity_unit) or "unk"
            depth = float(eq.depth_km) if (eq and eq.depth_km is not None) else 10.0
            records.append(
                {
                    "event_id": ev.event_id,
                    "raw_time": int(ev.timestamp.timestamp() * 1000),
                    "magnitude": mag,
                    "mag_type": mag_t,
                    "place": (eq.place if eq else "Unknown Location") or "Unknown Location",
                    "longitude": float(ev.longitude),
                    "latitude": float(ev.latitude),
                    "depth_km": depth,
                    "tsunami": bool(eq.tsunami_flag) if eq else False,
                    "source_file": fname,
                    "event_time": pd.Timestamp(ev.timestamp),
                    "magnitude_type": mag_t,
                    "energy_joules": float(10 ** (4.8 + 1.5 * mag)),
                }
            )

    df = pd.DataFrame(records)
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
        target,
        engine="pyarrow",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    logger.info("Created multi-source Silver Parquet partition: %s (%d rows)", target.name, len(df))
    _normalize_legacy_nanos_parquets(silver_dir)
    return target


def create_spark_session(app_name: str = "Team1_Earthquake_Gold_Pipeline") -> SparkSession:
    """Initializes a local[*] PySpark session optimized for containerized folder Parquet reads."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _write_single_parquet_via_spark(spark_df: DataFrame, target_file: Path) -> Path:
    """
    Persists a Spark DataFrame via spark_df.coalesce(1).write.parquet() and moves the
    resulting part-*.parquet file to target_file so it is a standard single Parquet file
    readable by both spark.read.parquet() and pd.read_parquet().
    """
    target_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = target_file.parent / f".tmp_{target_file.name}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    spark_df.coalesce(1).write.mode("overwrite").parquet(str(tmp_dir))
    part_files = list(tmp_dir.glob("part-*.parquet"))
    if not part_files:
        raise RuntimeError(f"PySpark did not produce a part-*.parquet file in {tmp_dir}")

    if target_file.exists():
        if target_file.is_dir():
            shutil.rmtree(target_file)
        else:
            target_file.unlink()

    shutil.move(str(part_files[0]), str(target_file))
    shutil.rmtree(tmp_dir)
    return target_file


def run_gold_pipeline(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
) -> Tuple[Path, Path]:
    """
    Executes the full Silver-to-Gold PySpark transformation:
    1. Reads all Parquet files in datalake_silver/ via spark.read.parquet().
    2. Computes Data Governance KPIs -> datalake_gold/governance_YYYYMMDD_HHMMSS.parquet
    3. Computes Storytelling & SAAI Aggregations -> datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet
    """
    ensure_multisource_silver_parquet(silver_dir)
    parquet_files = sorted(silver_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Silver Parquet files found in {silver_dir}")

    spark = create_spark_session()
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    try:
        logger.info("Reading %d Silver Parquet files from %s with PySpark...", len(parquet_files), silver_dir)
        raw_df = spark.read.parquet(str(silver_dir))

        # Derive source_id, event_date, log10_energy, place_length, and depth_pinned_10km in Spark
        enriched_df = (
            raw_df.withColumn(
                "source_id",
                F.when(F.col("event_id").startswith("usgs_fdsn_"), F.lit("usgs_fdsn"))
                .when(F.col("event_id").startswith("emsc_fdsn_"), F.lit("emsc_fdsn"))
                .when(F.col("event_id").startswith("usgs_realtime_"), F.lit("usgs_realtime"))
                .when(F.col("event_id").startswith("gdacs_realtime_"), F.lit("gdacs_realtime"))
                .when(F.col("source_file").contains("emsc"), F.lit("emsc_fdsn"))
                .when(F.col("source_file").contains("gdacs"), F.lit("gdacs_realtime"))
                .otherwise(F.lit("usgs_fdsn")),
            )
            .withColumn(
                "event_ts",
                F.to_timestamp(F.from_unixtime(F.col("raw_time") / 1000.0)),
            )
            .withColumn("period_date", F.date_format(F.col("event_ts"), "yyyy-MM-dd"))
            .withColumn("log10_energy", F.lit(4.8) + F.lit(1.5) * F.col("magnitude"))
            .withColumn("place_len", F.length(F.coalesce(F.col("place"), F.lit(""))))
            .withColumn(
                "is_pinned_10km",
                F.when(F.abs(F.col("depth_km") - F.lit(10.0)) < 1e-4, F.lit(1)).otherwise(F.lit(0)),
            )
        )

        enriched_df.cache()
        total_raw_rows = enriched_df.count()

        # Deterministic deduplication across accumulated Silver Parquet snapshots by event_id
        dedup_window = Window.partitionBy("event_id").orderBy(
            F.desc("source_file"),
            F.desc("raw_time"),
        )
        dedup_df = (
            enriched_df.withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .cache()
        )
        unique_rows = dedup_df.count()
        dup_rate_pct = ((total_raw_rows - unique_rows) / max(total_raw_rows, 1)) * 100.0

        # ==================================================================
        # PART 1: GOVERNANCE SUMMARY KPIs (PySpark Aggregations + Dynamic Reconciliation)
        # ==================================================================
        gov_rows: List[Dict[str, Any]] = []

        # 1A. Null rate per field & pinned 10km depth rate
        tracked_cols = [
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
            "magnitude_type",
            "energy_joules",
        ]
        null_exprs = [
            F.sum(
                F.when(
                    F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
                    1,
                ).otherwise(0)
            ).alias(c)
            for c in tracked_cols
        ]
        null_exprs.append(F.sum("is_pinned_10km").alias("pinned_10km_count"))
        null_counts_row = dedup_df.agg(*null_exprs).collect()[0].asDict()

        for c in tracked_cols:
            n_cnt = int(null_counts_row[c] or 0)
            n_pct = (n_cnt / max(unique_rows, 1)) * 100.0
            gov_rows.append(
                {
                    "kpi_category": "null_rate_per_field",
                    "metric_name": c,
                    "source_id": "ALL",
                    "period_date": "ALL",
                    "metric_value": round(n_pct, 4),
                    "secondary_value": float(n_cnt),
                    "min_value": 0.0,
                    "median_value": 0.0,
                    "max_value": 100.0,
                    "threshold_value": 5.0,
                    "compliance_status": "PASS" if n_pct <= 5.0 else "WARN",
                    "unit": "percent",
                    "computed_at": now_iso,
                }
            )

        pinned_10km_cnt = int(null_counts_row["pinned_10km_count"] or 0)
        pinned_10km_pct = (pinned_10km_cnt / max(unique_rows, 1)) * 100.0
        gov_rows.append(
            {
                "kpi_category": "null_rate_per_field",
                "metric_name": "depth_km_pinned_default_10km",
                "source_id": "ALL",
                "period_date": "ALL",
                "metric_value": round(pinned_10km_pct, 4),
                "secondary_value": float(pinned_10km_cnt),
                "min_value": 0.0,
                "median_value": 0.0,
                "max_value": 100.0,
                "threshold_value": PINNED_DEPTH_WARN_PCT,
                "compliance_status": "WARN" if pinned_10km_pct > PINNED_DEPTH_WARN_PCT else "PASS",
                "unit": "percent",
                "computed_at": now_iso,
            }
        )

        # 1B. Volume & text length statistics per source
        vol_src_rows = (
            dedup_df.groupBy("source_id")
            .agg(
                F.count("*").alias("cnt"),
                F.avg("place_len").alias("mean_len"),
                F.expr("percentile_approx(place_len, 0.5)").alias("median_len"),
                F.min("place_len").alias("min_len"),
                F.max("place_len").alias("max_len"),
                F.sum("is_pinned_10km").alias("pinned_cnt"),
            )
            .orderBy("source_id")
            .collect()
        )
        for r in vol_src_rows:
            cnt = int(r["cnt"])
            share = (cnt / max(unique_rows, 1)) * 100.0
            min_vol_thresh = 10.0 if r["source_id"] == "gdacs_realtime" else 100.0
            gov_rows.append(
                {
                    "kpi_category": "volume_by_source",
                    "metric_name": f"source_volume_{r['source_id']}",
                    "source_id": r["source_id"],
                    "period_date": "ALL",
                    "metric_value": float(cnt),
                    "secondary_value": round(share, 2),
                    "min_value": float(r["min_len"] or 0),
                    "median_value": float(r["median_len"] or 0),
                    "max_value": float(r["max_len"] or 0),
                    "threshold_value": min_vol_thresh,
                    "compliance_status": "PASS" if cnt >= min_vol_thresh else "WARN",
                    "unit": "records",
                    "computed_at": now_iso,
                }
            )
            mean_len_val = round(float(r["mean_len"] or 0.0), 2)
            gov_rows.append(
                {
                    "kpi_category": "text_length_statistics",
                    "metric_name": f"place_text_length_{r['source_id']}",
                    "source_id": r["source_id"],
                    "period_date": "ALL",
                    "metric_value": mean_len_val,
                    "secondary_value": float(cnt),
                    "min_value": float(r["min_len"] or 0),
                    "median_value": float(r["median_len"] or 0),
                    "max_value": float(r["max_len"] or 0),
                    "threshold_value": 120.0,
                    "compliance_status": "PASS" if mean_len_val <= 120.0 else "WARN",
                    "unit": "characters",
                    "computed_at": now_iso,
                }
            )

        # 1C. Daily volume over time per source
        daily_vol_rows = (
            dedup_df.groupBy("period_date", "source_id")
            .agg(F.count("*").alias("cnt"))
            .orderBy("period_date", "source_id")
            .collect()
        )
        running_total = 0
        for r in daily_vol_rows:
            if not r["period_date"]:
                continue
            c = int(r["cnt"])
            running_total += c
            gov_rows.append(
                {
                    "kpi_category": "volume_over_time",
                    "metric_name": "daily_ingested_records",
                    "source_id": r["source_id"],
                    "period_date": r["period_date"],
                    "metric_value": float(c),
                    "secondary_value": float(running_total),
                    "min_value": 0.0,
                    "median_value": 0.0,
                    "max_value": float(unique_rows),
                    "threshold_value": 1.0,
                    "compliance_status": "PASS" if c >= 1 else "WARN",
                    "unit": "records",
                    "computed_at": now_iso,
                }
            )

        # 1D. Outlier rate per numeric field (IQR method in PySpark)
        numeric_fields = ["magnitude", "depth_km", "log10_energy", "energy_joules"]
        for nf in numeric_fields:
            q_row = dedup_df.select(
                F.expr(f"percentile_approx({nf}, 0.25)").alias("q1"),
                F.expr(f"percentile_approx({nf}, 0.50)").alias("med"),
                F.expr(f"percentile_approx({nf}, 0.75)").alias("q3"),
            ).collect()[0]
            q1 = float(q_row["q1"] or 0.0)
            med = float(q_row["med"] or 0.0)
            q3 = float(q_row["q3"] or 0.0)
            iqr = q3 - q1
            low_b = q1 - 1.5 * iqr
            high_b = q3 + 1.5 * iqr

            out_cnt = dedup_df.filter((F.col(nf) < low_b) | (F.col(nf) > high_b)).count()
            out_pct = (out_cnt / max(unique_rows, 1)) * 100.0
            gov_rows.append(
                {
                    "kpi_category": "outlier_rate_per_numeric_field",
                    "metric_name": nf,
                    "source_id": "ALL",
                    "period_date": "ALL",
                    "metric_value": round(out_pct, 3),
                    "secondary_value": float(out_cnt),
                    "min_value": round(q1, 3),
                    "median_value": round(med, 3),
                    "max_value": round(q3, 3),
                    "threshold_value": 15.0,
                    "compliance_status": "PASS" if out_pct <= 15.0 else "WARN",
                    "unit": "percent",
                    "computed_at": now_iso,
                }
            )

        # 1E. Dynamic cross-source matching over deduplicated corpus (minimal column projection)
        match_subset_rows = (
            dedup_df.select(
                "event_id",
                "source_id",
                "raw_time",
                "latitude",
                "longitude",
                "magnitude",
                "mag_type",
            )
            .orderBy("raw_time")
            .toPandas()
        )
        canonical_for_matching = [
            CanonicalEvent(
                event_id=str(row.event_id),
                source_id=str(row.source_id),
                event_type="earthquake",
                timestamp=datetime.fromtimestamp(float(row.raw_time) / 1000.0, tz=timezone.utc),
                latitude=float(row.latitude),
                longitude=float(row.longitude),
                severity=float(row.magnitude),
                severity_unit=str(row.mag_type or "unk"),
            )
            for row in match_subset_rows.itertuples(index=False)
        ]
        matching_stats = compute_dynamic_matching_metrics(canonical_for_matching)

        # 1F. Live PostgreSQL quarantine count with safe offline fallback
        quarantined_cnt, quarantine_db_status = query_quarantined_records_count()
        if quarantine_db_status == "AVAILABLE" and quarantined_cnt is not None:
            q_val: Optional[float] = float(quarantined_cnt)
            q_rate_pct: Optional[float] = round(
                (quarantined_cnt / max(unique_rows + quarantined_cnt, 1)) * 100.0, 4
            )
            q_compliance = "PASS" if quarantined_cnt <= QUARANTINE_WARN_RECORDS else "WARN"
            q_src_flag = 1.0
            q_src_status = "PASS"
        else:
            q_val = None
            q_rate_pct = None
            q_compliance = "UNAVAILABLE"
            q_src_flag = 0.0
            q_src_status = "UNAVAILABLE"

        # 1G. Schema compliance & executive summary KPIs
        compliant_cnt = dedup_df.filter(
            (F.col("latitude").between(-90.0, 90.0))
            & (F.col("longitude").between(-180.0, 180.0))
            & (F.col("magnitude").between(-1.0, 10.0))
            & F.col("event_id").isNotNull()
        ).count()
        compliance_pct = (compliant_cnt / max(unique_rows, 1)) * 100.0

        # `cross_source_matched_pairs`: 1-to-1 cross-agency pairs united in an `event_group`
        matched_pairs_val = float(matching_stats["cross_source_matched_pairs"])
        groups_total_val = float(matching_stats["event_groups_total"])
        grouped_events_val = float(matching_stats["total_grouped_events"])
        group_sz2_val = float(matching_stats["group_size_2_count"])
        group_sz3_val = float(matching_stats["group_size_3plus_count"])

        summary_kpis = [
            (
                "total_silver_records_read",
                float(total_raw_rows),
                float(len(parquet_files)),
                100.0,
                "PASS" if total_raw_rows >= 100 else "WARN",
                "records",
            ),
            (
                "unique_deduplicated_events",
                float(unique_rows),
                float(total_raw_rows),
                100.0,
                "PASS" if unique_rows >= 100 else "WARN",
                "records",
            ),
            (
                "intra_lake_duplicate_rate_pct",
                round(dup_rate_pct, 2),
                float(total_raw_rows - unique_rows),
                50.0,
                "PASS" if dup_rate_pct <= 50.0 else "WARN",
                "percent",
            ),
            (
                "schema_compliance_rate_pct",
                round(compliance_pct, 4),
                float(compliant_cnt),
                99.0,
                "PASS" if compliance_pct >= 99.0 else "WARN",
                "percent",
            ),
            (
                "pinned_10km_depth_rate_pct",
                round(pinned_10km_pct, 2),
                float(pinned_10km_cnt),
                PINNED_DEPTH_WARN_PCT,
                "WARN" if pinned_10km_pct > PINNED_DEPTH_WARN_PCT else "PASS",
                "percent",
            ),
            (
                "quarantined_records_detected",
                q_val,
                q_rate_pct,
                QUARANTINE_WARN_RECORDS,
                q_compliance,
                "records",
            ),
            (
                "quarantine_source_status",
                q_src_flag,
                q_val,
                1.0,
                q_src_status,
                "flag",
            ),
            (
                "cross_source_matched_pairs",
                matched_pairs_val,
                groups_total_val,
                50.0,
                "PASS" if matched_pairs_val >= 50.0 else "WARN",
                "pairs",
            ),
            (
                "event_groups_total",
                groups_total_val,
                grouped_events_val,
                50.0,
                "PASS" if groups_total_val >= 50.0 else "WARN",
                "groups",
            ),
            (
                "event_group_size_2_count",
                group_sz2_val,
                matching_stats["median_dist_km"],
                1.0,
                "PASS" if group_sz2_val >= 1.0 else "WARN",
                "groups",
            ),
            (
                "event_group_size_3plus_count",
                group_sz3_val,
                matching_stats["median_time_diff_sec"],
                0.0,
                "PASS",
                "groups",
            ),
        ]
        for k_name, m_val, s_val, thresh, status_str, unit_str in summary_kpis:
            gov_rows.append(
                {
                    "kpi_category": "summary",
                    "metric_name": k_name,
                    "source_id": "ALL",
                    "period_date": "ALL",
                    "metric_value": m_val,
                    "secondary_value": s_val,
                    "min_value": 0.0 if m_val is not None else None,
                    "median_value": m_val,
                    "max_value": m_val,
                    "threshold_value": thresh,
                    "compliance_status": status_str,
                    "unit": unit_str,
                    "computed_at": now_iso,
                }
            )

        gov_spark_df = spark.createDataFrame(gov_rows, schema=GOVERNANCE_SCHEMA)
        gov_path = gold_dir / f"governance_{stamp}.parquet"
        _write_single_parquet_via_spark(gov_spark_df, gov_path)
        logger.info("Persisted Gold Governance Parquet: %s (%d KPI rows)", gov_path.name, len(gov_rows))

        # ==================================================================
        # PART 2: STORYTELLING SUMMARY AGGREGATIONS (PySpark DataFrame + Spark SQL)
        # ==================================================================
        story_df = (
            dedup_df.withColumn(
                "severity_tier",
                F.when(F.col("magnitude") < 3.0, F.lit("Micro/Minor (<3.0)"))
                .when(F.col("magnitude") < 4.5, F.lit("Light (3.0-4.4)"))
                .when(F.col("magnitude") < 5.5, F.lit("Moderate (4.5-5.4)"))
                .when(F.col("magnitude") < 6.5, F.lit("Strong (5.5-6.4)"))
                .otherwise(F.lit("Major/Great (>=6.5)")),
            )
            .withColumn(
                "depth_regime",
                F.when(F.col("depth_km") <= 70.0, F.lit("Shallow (0-70 km)"))
                .when(F.col("depth_km") <= 300.0, F.lit("Intermediate (70-300 km)"))
                .otherwise(F.lit("Deep (>300 km)")),
            )
            .withColumn(
                "region_name",
                F.trim(
                    F.when(
                        F.col("place").contains(" of "),
                        F.element_at(F.split(F.col("place"), " of "), -1),
                    )
                    .when(
                        F.col("place").contains(") in "),
                        F.regexp_extract(F.col("place"), r"\)\s+in\s+([A-Za-z\.\s]+?)\s+\d{2}/\d{2}/\d{4}", 1),
                    )
                    .otherwise(F.col("place"))
                ),
            )
        )
        story_df.createOrReplaceTempView("silver_earthquakes")

        story_rows: List[Dict[str, Any]] = []

        # 2A. Severity distribution via Spark SQL
        sev_rows = spark.sql(
            """
            SELECT
                severity_tier,
                COUNT(*) AS cnt,
                AVG(magnitude) AS mean_mag,
                MAX(magnitude) AS max_mag,
                AVG(depth_km) AS mean_depth,
                AVG(log10_energy) AS mean_log10_e
            FROM silver_earthquakes
            GROUP BY severity_tier
            ORDER BY mean_mag ASC
            """
        ).collect()
        for r in sev_rows:
            cnt = int(r["cnt"])
            pct = (cnt / max(unique_rows, 1)) * 100.0
            story_rows.append(
                {
                    "aggregation_type": "severity_distribution",
                    "dimension_key": r["severity_tier"],
                    "secondary_dimension": "ALL",
                    "period_date": "ALL",
                    "event_count": cnt,
                    "percentage": round(pct, 2),
                    "mean_magnitude": round(float(r["mean_mag"] or 0.0), 3),
                    "max_magnitude": round(float(r["max_mag"] or 0.0), 2),
                    "mean_depth_km": round(float(r["mean_depth"] or 0.0), 2),
                    "log10_energy": round(float(r["mean_log10_e"] or 0.0), 3),
                    "saai_score": 0.0,
                    "alert_tier": "INFO",
                    "narrative_note": "Gutenberg-Richter magnitude class distribution",
                    "computed_at": now_iso,
                }
            )

        # 2B. Temporal trend over time & Seismic Activity Anomaly Index (SAAI)
        daily_trend_rows = (
            story_df.groupBy("period_date")
            .agg(
                F.count("*").alias("cnt"),
                F.avg("magnitude").alias("mean_mag"),
                F.max("magnitude").alias("max_mag"),
                F.avg("depth_km").alias("mean_depth"),
                F.sum("energy_joules").alias("sum_energy"),
            )
            .orderBy("period_date")
            .collect()
        )

        valid_daily = [r for r in daily_trend_rows if r["period_date"] is not None]
        counts_vec = [float(r["cnt"]) for r in valid_daily]
        log_e_vec = [math.log10(max(float(r["sum_energy"] or 1.0), 1.0)) for r in valid_daily]

        mu_n = sum(counts_vec) / max(len(counts_vec), 1)
        sd_n = math.sqrt(sum((x - mu_n) ** 2 for x in counts_vec) / max(len(counts_vec), 1)) or 1.0
        mu_e = sum(log_e_vec) / max(len(log_e_vec), 1)
        sd_e = math.sqrt(sum((x - mu_e) ** 2 for x in log_e_vec) / max(len(log_e_vec), 1)) or 1.0

        for r, log_e in zip(valid_daily, log_e_vec):
            cnt = int(r["cnt"])
            z_n = (cnt - mu_n) / sd_n
            z_e = (log_e - mu_e) / sd_e
            saai = 0.4 * z_n + 0.6 * z_e
            if saai >= 3.5:
                tier = "CRITICAL"
            elif saai >= 2.0:
                tier = "HIGH"
            elif saai >= 1.0:
                tier = "ELEVATED"
            else:
                tier = "NORMAL"

            story_rows.append(
                {
                    "aggregation_type": "temporal_trend_saai",
                    "dimension_key": r["period_date"],
                    "secondary_dimension": tier,
                    "period_date": r["period_date"],
                    "event_count": cnt,
                    "percentage": round((cnt / max(unique_rows, 1)) * 100.0, 2),
                    "mean_magnitude": round(float(r["mean_mag"] or 0.0), 3),
                    "max_magnitude": round(float(r["max_mag"] or 0.0), 2),
                    "mean_depth_km": round(float(r["mean_depth"] or 0.0), 2),
                    "log10_energy": round(log_e, 3),
                    "saai_score": round(saai, 3),
                    "alert_tier": tier,
                    "narrative_note": f"Daily SAAI={saai:.2f} ({tier})",
                    "computed_at": now_iso,
                }
            )

        # 2C. Top seismic regions / keywords (with associated severity & energy)
        top_reg_rows = (
            story_df.groupBy("region_name")
            .agg(
                F.count("*").alias("cnt"),
                F.avg("magnitude").alias("mean_mag"),
                F.max("magnitude").alias("max_mag"),
                F.avg("depth_km").alias("mean_depth"),
                F.sum("energy_joules").alias("sum_energy"),
            )
            .orderBy(F.desc("cnt"))
            .limit(15)
            .collect()
        )
        for r in top_reg_rows:
            cnt = int(r["cnt"])
            log_e = math.log10(max(float(r["sum_energy"] or 1.0), 1.0))
            m_max = float(r["max_mag"] or 0.0)
            tier = "HIGH" if m_max >= 6.0 else ("ELEVATED" if m_max >= 5.0 else "NORMAL")
            story_rows.append(
                {
                    "aggregation_type": "top_regions_keywords",
                    "dimension_key": str(r["region_name"] or "Unknown"),
                    "secondary_dimension": tier,
                    "period_date": "ALL",
                    "event_count": cnt,
                    "percentage": round((cnt / max(unique_rows, 1)) * 100.0, 2),
                    "mean_magnitude": round(float(r["mean_mag"] or 0.0), 3),
                    "max_magnitude": round(m_max, 2),
                    "mean_depth_km": round(float(r["mean_depth"] or 0.0), 2),
                    "log10_energy": round(log_e, 3),
                    "saai_score": 0.0,
                    "alert_tier": tier,
                    "narrative_note": "Top active tectonic region in Silver corpus",
                    "computed_at": now_iso,
                }
            )

        # 2D. Source comparison (volume & severity distribution per agency)
        src_comp_rows = (
            story_df.groupBy("source_id", "severity_tier")
            .agg(
                F.count("*").alias("cnt"),
                F.avg("magnitude").alias("mean_mag"),
                F.max("magnitude").alias("max_mag"),
                F.avg("depth_km").alias("mean_depth"),
                F.sum("energy_joules").alias("sum_energy"),
            )
            .orderBy("source_id", "mean_mag")
            .collect()
        )
        for r in src_comp_rows:
            cnt = int(r["cnt"])
            log_e = math.log10(max(float(r["sum_energy"] or 1.0), 1.0))
            story_rows.append(
                {
                    "aggregation_type": "source_comparison",
                    "dimension_key": r["source_id"],
                    "secondary_dimension": r["severity_tier"],
                    "period_date": "ALL",
                    "event_count": cnt,
                    "percentage": round((cnt / max(unique_rows, 1)) * 100.0, 2),
                    "mean_magnitude": round(float(r["mean_mag"] or 0.0), 3),
                    "max_magnitude": round(float(r["max_mag"] or 0.0), 2),
                    "mean_depth_km": round(float(r["mean_depth"] or 0.0), 2),
                    "log10_energy": round(log_e, 3),
                    "saai_score": 0.0,
                    "alert_tier": "INFO",
                    "narrative_note": f"Catalog comparison for {r['source_id']}",
                    "computed_at": now_iso,
                }
            )

        # 2E. Depth regime breakdown (Shallow / Intermediate / Deep)
        depth_rows = (
            story_df.groupBy("depth_regime")
            .agg(
                F.count("*").alias("cnt"),
                F.avg("magnitude").alias("mean_mag"),
                F.max("magnitude").alias("max_mag"),
                F.avg("depth_km").alias("mean_depth"),
                F.sum("energy_joules").alias("sum_energy"),
            )
            .orderBy("mean_depth")
            .collect()
        )
        for r in depth_rows:
            cnt = int(r["cnt"])
            log_e = math.log10(max(float(r["sum_energy"] or 1.0), 1.0))
            story_rows.append(
                {
                    "aggregation_type": "depth_regime_breakdown",
                    "dimension_key": r["depth_regime"],
                    "secondary_dimension": "ALL",
                    "period_date": "ALL",
                    "event_count": cnt,
                    "percentage": round((cnt / max(unique_rows, 1)) * 100.0, 2),
                    "mean_magnitude": round(float(r["mean_mag"] or 0.0), 3),
                    "max_magnitude": round(float(r["max_mag"] or 0.0), 2),
                    "mean_depth_km": round(float(r["mean_depth"] or 0.0), 2),
                    "log10_energy": round(log_e, 3),
                    "saai_score": 0.0,
                    "alert_tier": "INFO",
                    "narrative_note": "Focal depth regime attenuation breakdown",
                    "computed_at": now_iso,
                }
            )

        story_spark_df = spark.createDataFrame(story_rows)
        story_path = gold_dir / f"storytelling_{stamp}.parquet"
        _write_single_parquet_via_spark(story_spark_df, story_path)
        logger.info("Persisted Gold Storytelling Parquet: %s (%d aggregation rows)", story_path.name, len(story_rows))

        return gov_path, story_path
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver-to-Gold PySpark Pipeline")
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    args = parser.parse_args()
    g_path, s_path = run_gold_pipeline(silver_dir=args.silver_dir, gold_dir=args.gold_dir)
    print(f"Generated Gold files:\n  - {g_path}\n  - {s_path}")


if __name__ == "__main__":
    main()
