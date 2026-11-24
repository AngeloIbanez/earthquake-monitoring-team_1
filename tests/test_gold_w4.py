"""
Unit and integration tests for Workshop #4 Gold Layer enrichments (T-03):
- Dynamic cross-source matching over synthetic multi-agency events
- Safe offline fallback when PostgreSQL is unreachable (monkeypatch)
- Additive Gold Parquet schema preservation (legacy W3 columns + new W4 columns & categories)
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import pytest

import src.gold_pyspark as gold_mod
from src.models import CanonicalEvent


def test_dynamic_matching_synthetic_corpus():
    """
    Verifies dynamic cross-source matching:
    - 1 pair (USGS + EMSC) within 15 min / 100 km -> matched
    - 1 pair far outside window -> unmatched
    - 1 intra-agency USGS Real-Time duplicate -> NOT self-matched with USGS FDSN
    """
    t0 = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        CanonicalEvent(
            event_id="usgs_fdsn_ev1",
            source_id="usgs_fdsn",
            timestamp=t0,
            latitude=36.20,
            longitude=141.80,
            severity=5.6,
            severity_unit="mww",
        ),
        CanonicalEvent(
            event_id="emsc_fdsn_ev1",
            source_id="emsc_fdsn",
            timestamp=t0 + timedelta(seconds=4),
            latitude=36.22,
            longitude=141.82,
            severity=5.5,
            severity_unit="mb",
        ),
        # Same agency prefix ("usgs") at California location without EMSC: must NOT self-match
        CanonicalEvent(
            event_id="usgs_fdsn_ca",
            source_id="usgs_fdsn",
            timestamp=t0 + timedelta(minutes=45),
            latitude=38.80,
            longitude=-122.80,
            severity=3.2,
            severity_unit="ml",
        ),
        CanonicalEvent(
            event_id="usgs_realtime_ca",
            source_id="usgs_realtime",
            timestamp=t0 + timedelta(minutes=45, seconds=1),
            latitude=38.80,
            longitude=-122.80,
            severity=3.2,
            severity_unit="ml",
        ),
        # Outside spatio-temporal window (> 15 min and > 100 km)
        CanonicalEvent(
            event_id="emsc_fdsn_far",
            source_id="emsc_fdsn",
            timestamp=t0 + timedelta(hours=3),
            latitude=-20.0,
            longitude=-70.0,
            severity=4.9,
            severity_unit="mb",
        ),
    ]

    stats = gold_mod.compute_dynamic_matching_metrics(events)
    assert stats["cross_source_matched_pairs"] == 1
    assert stats["event_groups_total"] == 1
    assert stats["group_size_2_count"] == 1
    assert stats["group_size_3plus_count"] == 0
    assert stats["total_grouped_events"] == 2


def test_quarantine_fallback_when_postgres_offline(monkeypatch):
    """Verifies safe UNAVAILABLE fallback when PostgreSQL connection fails."""
    if gold_mod.psycopg2 is not None:
        def _boom(*args, **kwargs):
            raise RuntimeError("Simulated connection refused")

        monkeypatch.setattr(gold_mod.psycopg2, "connect", _boom)

    cnt, status = gold_mod.query_quarantined_records_count(connect_timeout=1)
    assert cnt is None
    assert status == "UNAVAILABLE"


def test_airflow_conn_uri_fallback_parsing(monkeypatch):
    """Verifies AIRFLOW_CONN_POSTGRES_ETL host/port parsing when POSTGRES_HOST is unset."""
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.setenv("AIRFLOW_CONN_POSTGRES_ETL", "postgresql://etl_ud:secret@postgres:5432/earthquakes_ud")
    params = gold_mod._resolve_db_params_with_airflow_fallback()
    assert params["host"] == "postgres"
    assert params["port"] == 5432
    assert params["database"] == "earthquakes_ud"
    assert params["user"] == "etl_ud"


def test_gold_pipeline_additive_schema_and_offline_quarantine(tmp_path: Path, monkeypatch):
    """
    Runs the full PySpark Silver-to-Gold pipeline over a compact synthetic multi-source
    Silver partition with PostgreSQL simulated offline, validating:
    1. Backward compatibility with all W3 Governance (13 cols, 6 categories) and
       Storytelling (14 original cols, 5 original categories).
    2. New W4 categories (`recent_significant_events`, `historical_vs_current`) and
       nullable columns (`latitude`, `longitude`, `energy_share_pct`, `tsunami_flag`).
    3. Offline quarantine KPI recorded with `compliance_status == 'UNAVAILABLE'` and NaN value.
    """
    if gold_mod.psycopg2 is not None:
        monkeypatch.setattr(
            gold_mod.psycopg2,
            "connect",
            lambda *a, **kw: (_ for _ in ()).throw(ConnectionError("Postgres offline")),
        )

    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    silver_dir.mkdir()
    gold_dir.mkdir()

    # Create pre-seeded sentinel name so ensure_multisource_silver_parquet uses our synthetic fixture
    base_ts = datetime(2026, 11, 21, 0, 0, 0, tzinfo=timezone.utc)
    rows = [
        {
            "event_id": "usgs_fdsn_ev100",
            "raw_time": int(base_ts.timestamp() * 1000),
            "magnitude": 6.2,
            "mag_type": "mww",
            "place": "45 km E of Sendai, Japan",
            "longitude": 141.5,
            "latitude": 38.2,
            "depth_km": 24.0,
            "tsunami": True,
            "source_file": "earthquakes_20261121_000000.json",
            "event_time": pd.Timestamp(base_ts),
            "magnitude_type": "mww",
            "energy_joules": float(10 ** (4.8 + 1.5 * 6.2)),
        },
        {
            "event_id": "emsc_fdsn_ev100",
            "raw_time": int((base_ts + timedelta(seconds=3)).timestamp() * 1000),
            "magnitude": 6.1,
            "mag_type": "mw",
            "place": "NEAR EAST COAST OF HONSHU, JAPAN",
            "longitude": 141.52,
            "latitude": 38.21,
            "depth_km": 10.0,
            "tsunami": False,
            "source_file": "emsc_20261121_000000.json",
            "event_time": pd.Timestamp(base_ts + timedelta(seconds=3)),
            "magnitude_type": "mw",
            "energy_joules": float(10 ** (4.8 + 1.5 * 6.1)),
        },
        {
            "event_id": "usgs_realtime_ev200",
            "raw_time": int((base_ts - timedelta(hours=6)).timestamp() * 1000),
            "magnitude": 2.4,
            "mag_type": "ml",
            "place": "8 km NW of The Geysers, CA",
            "longitude": -122.8,
            "latitude": 38.8,
            "depth_km": 3.5,
            "tsunami": False,
            "source_file": "usgs_incremental_raw.json",
            "event_time": pd.Timestamp(base_ts - timedelta(hours=6)),
            "magnitude_type": "ml",
            "energy_joules": float(10 ** (4.8 + 1.5 * 2.4)),
        },
        {
            "event_id": "gdacs_realtime_ev300",
            "raw_time": int((base_ts - timedelta(hours=2)).timestamp() * 1000),
            "magnitude": 5.6,
            "mag_type": "mw",
            "place": "Green earthquake (Magnitude 5.6M, Depth:35km) in Chile 20/11/2026",
            "longitude": -71.2,
            "latitude": -30.5,
            "depth_km": 35.0,
            "tsunami": False,
            "source_file": "gdacs_20261121_000000.xml",
            "event_time": pd.Timestamp(base_ts - timedelta(hours=2)),
            "magnitude_type": "mw",
            "energy_joules": float(10 ** (4.8 + 1.5 * 5.6)),
        },
    ]
    fixture_df = pd.DataFrame(rows)
    fixture_df.to_parquet(
        silver_dir / "earthquakes_silver_20261120_120000.parquet",
        engine="pyarrow",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    gov_path, story_path = gold_mod.run_gold_pipeline(silver_dir=silver_dir, gold_dir=gold_dir)
    gov_df = pd.read_parquet(gov_path)
    story_df = pd.read_parquet(story_path)

    # 1. Governance backward compatibility + dynamic matching + offline quarantine
    legacy_gov_cols = {
        "kpi_category", "metric_name", "source_id", "period_date", "metric_value",
        "secondary_value", "min_value", "median_value", "max_value",
        "threshold_value", "compliance_status", "unit", "computed_at",
    }
    assert legacy_gov_cols.issubset(set(gov_df.columns))

    q_row = gov_df[gov_df["metric_name"] == "quarantined_records_detected"].iloc[0]
    assert q_row["compliance_status"] == "UNAVAILABLE"
    assert pd.isna(q_row["metric_value"])

    m_row = gov_df[gov_df["metric_name"] == "cross_source_matched_pairs"].iloc[0]
    assert m_row["metric_value"] == 1.0

    # 2. Storytelling backward compatibility + W4 additive columns & categories
    legacy_story_cols = {
        "aggregation_type", "dimension_key", "secondary_dimension", "period_date",
        "event_count", "percentage", "mean_magnitude", "max_magnitude",
        "mean_depth_km", "log10_energy", "saai_score", "alert_tier",
        "narrative_note", "computed_at",
    }
    assert legacy_story_cols.issubset(set(story_df.columns))
    assert {"latitude", "longitude", "energy_share_pct", "tsunami_flag", "daily_rate"}.issubset(set(story_df.columns))

    expected_story_cats = {
        "severity_distribution",
        "temporal_trend_saai",
        "top_regions_keywords",
        "source_comparison",
        "depth_regime_breakdown",
        "recent_significant_events",
        "historical_vs_current",
    }
    assert expected_story_cats.issubset(set(story_df["aggregation_type"].unique()))

    sig_df = story_df[story_df["aggregation_type"] == "recent_significant_events"]
    assert len(sig_df) == 3  # M6.2, M6.1, M5.6 (all >= 4.5); M2.4 excluded
    assert sig_df["latitude"].between(-90.0, 90.0).all()
    assert sig_df["longitude"].between(-180.0, 180.0).all()

    usgs_sig = sig_df[sig_df["source_id"] == "usgs_fdsn"].iloc[0]
    emsc_sig = sig_df[sig_df["source_id"] == "emsc_fdsn"].iloc[0]
    assert bool(usgs_sig["tsunami_flag"]) is True
    assert pd.isna(emsc_sig["tsunami_flag"])

    sev_df = story_df[story_df["aggregation_type"] == "severity_distribution"]
    assert pytest.approx(sev_df["energy_share_pct"].sum(), abs=0.05) == 100.0
