"""
Unit tests validating the canonical database DDL schema.
"""

import unittest
from pathlib import Path

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"

class TestSchema(unittest.TestCase):
    
    def setUp(self):
        self.assertTrue(SCHEMA_FILE.exists(), f"schema.sql not found at {SCHEMA_FILE}")
        with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
            self.sql_content = f.read()

    def test_required_tables_exist(self):
        expected_tables = [
            "TABLE IF NOT EXISTS source",
            "TABLE IF NOT EXISTS ingestion_run",
            "TABLE IF NOT EXISTS ingestion_state",
            "TABLE IF NOT EXISTS event",
            "TABLE IF NOT EXISTS event_group",
            "TABLE IF NOT EXISTS rejected_record",
            "TABLE IF NOT EXISTS earthquake"
        ]
        for tbl in expected_tables:
            self.assertIn(tbl, self.sql_content, f"Missing table definition: {tbl}")

    def test_event_table_constraints(self):
        self.assertIn("latitude BETWEEN -90.0 AND 90.0", self.sql_content)
        self.assertIn("longitude BETWEEN -180.0 AND 180.0", self.sql_content)
        self.assertIn("updated_at", self.sql_content)

if __name__ == "__main__":
    unittest.main()
