"""
Unit and integration tests for Workshop #3:
- Transitive cross-source event grouping (USGS + EMSC + GDACS -> event_group)
- Incremental watermark window filtering with overlap window
- Silver-to-Gold Parquet summary verification
"""

import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.models import CanonicalEvent, EarthquakeDetail
from src.matching import build_cross_source_event_groups
from ingest import _filter_by_watermark_window

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_ROOT / "datalake_gold"


class TestWorkshop3(unittest.TestCase):

    def test_transitive_three_catalog_grouping(self):
        """Verifies that a major earthquake reported by USGS, EMSC, and GDACS is merged into one group_id."""
        t0 = datetime(2026, 11, 9, 4, 52, 50, tzinfo=timezone.utc)
        ev_usgs = CanonicalEvent(
            event_id="usgs_fdsn_us7000tx1f",
            source_id="usgs_fdsn",
            timestamp=t0,
            latitude=36.2471,
            longitude=141.8299,
            severity=5.7,
        )
        ev_emsc = CanonicalEvent(
            event_id="emsc_fdsn_20261109_0000123",
            source_id="emsc_fdsn",
            timestamp=datetime(2026, 11, 9, 4, 52, 51, tzinfo=timezone.utc),
            latitude=36.2500,
            longitude=141.8350,
            severity=5.6,
        )
        ev_gdacs = CanonicalEvent(
            event_id="gdacs_realtime_1576372",
            source_id="gdacs_realtime",
            timestamp=datetime(2026, 11, 9, 4, 52, 51, tzinfo=timezone.utc),
            latitude=36.2471,
            longitude=141.8299,
            severity=5.7,
        )

        group_rows, metrics = build_cross_source_event_groups([ev_usgs, ev_emsc, ev_gdacs])
        self.assertEqual(metrics["total_groups"], 1)
        self.assertEqual(len(group_rows), 3)
        group_ids = {g_id for g_id, _ in group_rows}
        self.assertEqual(len(group_ids), 1)
        self.assertIn("grp_usgs_fdsn_us7000tx1f", group_ids)

    def test_watermark_overlap_window_filter(self):
        """Verifies that _filter_by_watermark_window drops old records outside overlap while retaining recent revisions."""
        wm = "2026-11-10T12:00:00+00:00"
        old_ev = CanonicalEvent(
            event_id="usgs_old",
            source_id="usgs_realtime",
            timestamp=datetime(2026, 11, 8, 0, 0, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 11, 8, 0, 0, 0, tzinfo=timezone.utc),
            latitude=10.0,
            longitude=20.0,
            severity=4.5,
        )
        overlap_ev = CanonicalEvent(
            event_id="usgs_revised_in_overlap",
            source_id="usgs_realtime",
            timestamp=datetime(2026, 11, 9, 20, 0, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 11, 10, 2, 0, 0, tzinfo=timezone.utc),
            latitude=10.0,
            longitude=20.0,
            severity=4.8,
        )
        new_ev = CanonicalEvent(
            event_id="usgs_new",
            source_id="usgs_realtime",
            timestamp=datetime(2026, 11, 10, 14, 0, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 11, 10, 14, 0, 0, tzinfo=timezone.utc),
            latitude=12.0,
            longitude=22.0,
            severity=5.1,
        )

        eqs = [
            EarthquakeDetail(event_id="usgs_old", depth_km=10.0),
            EarthquakeDetail(event_id="usgs_revised_in_overlap", depth_km=15.0),
            EarthquakeDetail(event_id="usgs_new", depth_km=25.0),
        ]

        filtered_evs, filtered_eqs = _filter_by_watermark_window(
            [old_ev, overlap_ev, new_ev], eqs, watermark_str=wm, overlap_hours=24
        )
        retained_ids = [e.event_id for e in filtered_evs]
        self.assertEqual(retained_ids, ["usgs_revised_in_overlap", "usgs_new"])
        self.assertEqual(len(filtered_eqs), 2)

    def test_gold_parquet_outputs_if_present(self):
        """Verifies that generated Gold Parquet files contain required KPI and storytelling categories."""
        gov_files = sorted(GOLD_DIR.glob("governance_*.parquet"))
        story_files = sorted(GOLD_DIR.glob("storytelling_*.parquet"))
        if not gov_files or not story_files:
            return

        gov_df = pd.read_parquet(gov_files[-1])
        story_df = pd.read_parquet(story_files[-1])

        expected_gov_cats = {
            "null_rate_per_field",
            "volume_by_source",
            "volume_over_time",
            "outlier_rate_per_numeric_field",
            "text_length_statistics",
            "summary",
        }
        self.assertTrue(expected_gov_cats.issubset(set(gov_df["kpi_category"].unique())))

        expected_story_types = {
            "severity_distribution",
            "temporal_trend_saai",
            "top_regions_keywords",
            "source_comparison",
            "depth_regime_breakdown",
        }
        self.assertTrue(expected_story_types.issubset(set(story_df["aggregation_type"].unique())))


if __name__ == "__main__":
    unittest.main()
