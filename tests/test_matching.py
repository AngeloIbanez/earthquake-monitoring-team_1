"""
Unit tests for cross-source event matching algorithms.
"""

import unittest
from datetime import datetime, timezone
from src.matching import haversine_distance, compute_spatio_temporal_cost, match_events_nearest_neighbor
from src.models import CanonicalEvent

class TestMatching(unittest.TestCase):
    
    def test_haversine_distance_known_points(self):
        # Quito (0.1807 S, 78.4678 W) to Guayaquil (2.1894 S, 79.8891 W) ~ 269 km
        dist = haversine_distance(-0.1807, -78.4678, -2.1894, -79.8891)
        self.assertGreater(dist, 260.0)
        self.assertLess(dist, 280.0)
        
    def test_spatio_temporal_cost(self):
        # Exact match (0 km, 0 sec) -> cost 0
        self.assertEqual(compute_spatio_temporal_cost(0.0, 0.0), 0.0)
        
        # Max boundary (100 km, 900 sec) -> sqrt(1 + 1) = sqrt(2) ~ 1.4142
        cost_max = compute_spatio_temporal_cost(100.0, 900.0, 100.0, 900.0)
        self.assertAlmostEqual(cost_max, 1.4142, places=3)
        
    def test_nearest_neighbor_collision_resolution(self):
        # Test case: 1 EMSC event within matching range of 2 USGS events (1-to-N collision)
        t_base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        emsc_event = CanonicalEvent(
            event_id="emsc_1",
            source_id="emsc_fdsn",
            timestamp=t_base,
            latitude=35.0,
            longitude=140.0,
            severity=5.5
        )
        
        # USGS Event 1: very close (Δt = 1s, Δd = 5 km) -> low cost
        usgs_event_close = CanonicalEvent(
            event_id="usgs_close",
            source_id="usgs_fdsn",
            timestamp=datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
            latitude=35.04,
            longitude=140.02,
            severity=5.6
        )
        
        # USGS Event 2: farther (Δt = 400s, Δd = 60 km) -> higher cost
        usgs_event_far = CanonicalEvent(
            event_id="usgs_far",
            source_id="usgs_fdsn",
            timestamp=datetime(2026, 1, 1, 12, 6, 40, tzinfo=timezone.utc),
            latitude=35.45,
            longitude=140.50,
            severity=4.9
        )
        
        matches = match_events_nearest_neighbor(
            events_primary=[usgs_event_close, usgs_event_far],
            events_secondary=[emsc_event]
        )
        
        # Must resolve to exactly ONE match, selecting the nearest neighbor (usgs_close)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["primary_id"], "usgs_close")
        self.assertEqual(matches[0]["secondary_id"], "emsc_1")

if __name__ == "__main__":
    unittest.main()
