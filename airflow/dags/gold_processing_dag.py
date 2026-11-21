"""
Gold Processing DAG — Team 1: Earthquake Activity Monitoring
Data Analysis Programming — Semester 2026-II (Workshop #3)

Reads all accumulated Silver Parquet files from datalake_silver/ using PySpark
in local[*] mode, computes Data Governance KPIs and Storytelling / SAAI analytical
aggregations, and persists two structured Gold Parquet files in datalake_gold/:
  - datalake_gold/governance_YYYYMMDD_HHMMSS.parquet
  - datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task

from src.gold_pyspark import run_gold_pipeline

logger = logging.getLogger("airflow.task")

SILVER_DIR = Path("/opt/airflow/datalake_silver")
GOLD_DIR = Path("/opt/airflow/datalake_gold")


@dag(
    dag_id="gold_processing_dag",
    schedule="0 */6 * * *",  # Runs every 6 hours (or manual trigger)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["gold", "pyspark", "governance", "storytelling", "saai", "team_1"],
    description="PySpark Silver-to-Gold aggregation pipeline producing governance and storytelling Parquet datasets",
)
def gold_processing_pipeline():

    @task
    def verify_silver_inputs() -> int:
        """Verify that datalake_silver/ contains readable Parquet files before launching PySpark."""
        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        files = sorted(SILVER_DIR.glob("*.parquet"))
        logger.info("Found %d Silver Parquet partitions in %s", len(files), SILVER_DIR)
        return len(files)

    @task
    def execute_pyspark_gold_aggregations(silver_file_count: int) -> dict:
        """Run PySpark local[*] transformations over datalake_silver/ and persist Gold Parquet summaries."""
        logger.info("Launching PySpark Silver-to-Gold pipeline over %d input files...", silver_file_count)
        gov_path, story_path = run_gold_pipeline(silver_dir=SILVER_DIR, gold_dir=GOLD_DIR)
        return {
            "governance_parquet": str(gov_path),
            "storytelling_parquet": str(story_path),
        }

    @task
    def validate_gold_outputs(gold_paths: dict) -> dict:
        """Verify that both Gold Parquet files exist, are non-empty, and conform to expected schemas."""
        gov_file = Path(gold_paths["governance_parquet"])
        story_file = Path(gold_paths["storytelling_parquet"])

        if not gov_file.exists() or not story_file.exists():
            raise FileNotFoundError("One or both Gold Parquet files were not created.")

        gov_df = pd.read_parquet(gov_file)
        story_df = pd.read_parquet(story_file)

        if gov_df.empty or story_df.empty:
            raise ValueError("Gold Parquet files cannot be empty.")

        logger.info(
            "Validated Gold outputs: %s (%d rows) | %s (%d rows)",
            gov_file.name,
            len(gov_df),
            story_file.name,
            len(story_df),
        )
        return {
            "governance_rows": len(gov_df),
            "storytelling_rows": len(story_df),
            "governance_file": gov_file.name,
            "storytelling_file": story_file.name,
        }

    cnt = verify_silver_inputs()
    paths = execute_pyspark_gold_aggregations(cnt)
    validate_gold_outputs(paths)


gold_dag = gold_processing_pipeline()
