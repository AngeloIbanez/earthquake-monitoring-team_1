"""
Unit tests for data source payload parsers.
"""

import unittest
from pathlib import Path
from src.parsers import parse_usgs_geojson, parse_emsc_json, parse_gdacs_xml

class TestParsers(unittest.TestCase):
    
    def test_parse_usgs_sample(self):
        sample = {
            "features": [
                {
                    "id": "us7000tx1f",
                    "properties": {
                        "mag": 5.7,
                        "magType": "mww",
                        "time": 1794199971519,
                        "place": "106 km ENE of Hasaki, Japan",
                        "tsunami": 0,
                        "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000tx1f"
                    },
                    "geometry": {
                        "coordinates": [141.8299, 36.2471, 24.17]
                    }
                }
            ]
        }
        events, earthquakes = parse_usgs_geojson(sample, source_id="usgs_test")
        self.assertEqual(len(events), 1)
        self.assertEqual(len(earthquakes), 1)
        
        ev = events[0]
        self.assertEqual(ev.event_id, "usgs_test_us7000tx1f")
        self.assertEqual(ev.severity, 5.7)
        self.assertEqual(ev.severity_unit, "mww")
        self.assertAlmostEqual(ev.latitude, 36.2471)
        self.assertAlmostEqual(ev.longitude, 141.8299)
        self.assertTrue(ev.is_valid())
        
        eq = earthquakes[0]
        self.assertAlmostEqual(eq.depth_km, 24.17)
        self.assertEqual(eq.magnitude_type, "mww")
        self.assertFalse(eq.tsunami_flag)

    def test_parse_emsc_sample(self):
        sample = {
            "features": [
                {
                    "id": "20260101_0000428",
                    "properties": {
                        "source_id": "20260101_0000428",
                        "mag": 4.8,
                        "magtype": "mb",
                        "time": "2026-01-01T01:25:20.926Z",
                        "flynn_region": "SOUTH SANDWICH ISLANDS REGION",
                        "lat": -56.32,
                        "lon": -27.45,
                        "depth": 35.0
                    },
                    "geometry": {
                        "coordinates": [-27.45, -56.32, 35.0]
                    }
                }
            ]
        }
        events, earthquakes = parse_emsc_json(sample, source_id="emsc_test")
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev.event_id, "emsc_test_20260101_0000428")
        self.assertEqual(ev.severity, 4.8)
        self.assertEqual(ev.severity_unit, "mb")
        self.assertTrue(ev.is_valid())

    def test_parse_gdacs_xml_sample(self):
        xml_sample = """<?xml version="1.0" encoding="utf-8"?>
        <rss version="2.0" xmlns:gdacs="http://www.gdacs.org" xmlns:georss="http://www.georss.org/georss">
            <channel>
                <item>
                    <gdacs:eventtype>EQ</gdacs:eventtype>
                    <gdacs:eventid>1576372</gdacs:eventid>
                    <gdacs:fromdate>Mon, 09 Nov 2026 04:52:51 GMT</gdacs:fromdate>
                    <georss:point>36.2471 141.8299</georss:point>
                    <gdacs:severity>Magnitude 5.7M, Depth:24.2km</gdacs:severity>
                    <gdacs:calculationtype>earthquakeonly</gdacs:calculationtype>
                    <title>Green earthquake (Magnitude 5.7M, Depth:24.2km) in Japan</title>
                    <link>http://www.gdacs.org/report.aspx?eventtype=EQ&amp;eventid=1576372</link>
                </item>
                <item>
                    <gdacs:eventtype>TC</gdacs:eventtype>
                    <title>Tropical Cyclone</title>
                </item>
            </channel>
        </rss>"""
        events, earthquakes = parse_gdacs_xml(xml_sample, source_id="gdacs_test")
        # TC event must be filtered out; exactly 1 EQ event must be parsed
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev.event_id, "gdacs_test_1576372")
        self.assertEqual(ev.severity, 5.7)
        self.assertAlmostEqual(ev.latitude, 36.2471)
        self.assertAlmostEqual(ev.longitude, 141.8299)
        self.assertTrue(ev.is_valid())
        
        eq = earthquakes[0]
        self.assertAlmostEqual(eq.depth_km, 24.2)
        self.assertFalse(eq.tsunami_flag)

if __name__ == "__main__":
    unittest.main()
