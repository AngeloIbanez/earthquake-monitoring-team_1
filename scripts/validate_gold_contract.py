#!/usr/bin/env python3
"""
Gold Layer Data Contract Validator — Workshop #4 (`scripts/validate_gold_contract.py`).

Locates the latest `governance_YYYYMMDD_HHMMSS.parquet` and
`storytelling_YYYYMMDD_HHMMSS.parquet` files in `datalake_gold/` ordered strictly
by the UTC timestamp embedded in the filename (never by filesystem mtime) and
verifies all schema, nullability, physical range, and cross-file conservation
invariants defined in `docs/gold_contract_w4.md`.

Exits with code 0 on full compliance or code 1 if any invariant fails.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GOLD_DIR = Path(os.getenv("GOLD_DIR", str(PROJECT_ROOT / "datalake_gold")))
STAMP_RE = re.compile(r"(\d{8}_\d{6})")

REQUIRED_GOV_COLUMNS = {
    "kpi_category",
    "metric_name",
    "source_id",
    "period_date",
    "metric_value",
    "secondary_value",
    "min_value",
    "median_value",
    "max_value",
    "threshold_value",
    "compliance_status",
    "unit",
    "computed_at",
}

REQUIRED_GOV_CATEGORIES = {
    "null_rate_per_field",
    "volume_by_source",
    "volume_over_time",
    "outlier_rate_per_numeric_field",
    "text_length_statistics",
    "summary",
}

REQUIRED_SUMMARY_METRICS = {
    "total_silver_records_read",
    "unique_deduplicated_events",
    "intra_lake_duplicate_rate_pct",
    "schema_compliance_rate_pct",
    "pinned_10km_depth_rate_pct",
    "quarantined_records_detected",
    "quarantine_source_status",
    "cross_source_matched_pairs",
    "event_groups_total",
    "event_group_size_2_count",
    "event_group_size_3plus_count",
}

REQUIRED_STORY_COLUMNS = {
    # Legacy 14 W3 columns
    "aggregation_type",
    "dimension_key",
    "secondary_dimension",
    "period_date",
    "event_count",
    "percentage",
    "mean_magnitude",
    "max_magnitude",
    "mean_depth_km",
    "log10_energy",
    "saai_score",
    "alert_tier",
    "narrative_note",
    "computed_at",
    # Additive 11 W4 columns
    "latitude",
    "longitude",
    "energy_share_pct",
    "event_timestamp_utc",
    "source_id",
    "tsunami_flag",
    "is_pinned_depth",
    "daily_rate",
    "median_depth_km",
    "daily_energy_joules",
    "m55_plus_share_pct",
}

REQUIRED_STORY_CATEGORIES = {
    "severity_distribution",
    "temporal_trend_saai",
    "top_regions_keywords",
    "source_comparison",
    "depth_regime_breakdown",
    "recent_significant_events",
    "historical_vs_current",
}


def _extract_stamp(path: Path) -> str:
    match = STAMP_RE.search(path.name)
    return match.group(1) if match else path.name


def find_latest_parquet(gold_dir: Path, prefix: str) -> Path:
    candidates = sorted(gold_dir.glob(f"{prefix}_*.parquet"), key=_extract_stamp)
    if not candidates:
        raise FileNotFoundError(f"No {prefix}_*.parquet files found in {gold_dir}")
    return candidates[-1]


def validate_gold_pair(gold_dir: Path) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    try:
        gov_path = find_latest_parquet(gold_dir, "governance")
        story_path = find_latest_parquet(gold_dir, "storytelling")
    except FileNotFoundError as exc:
        return False, [str(exc)]

    gov_df = pd.read_parquet(gov_path)
    story_df = pd.read_parquet(story_path)

    if gov_df.empty:
        errors.append(f"{gov_path.name} is empty.")
    if story_df.empty:
        errors.append(f"{story_path.name} is empty.")

    # 1. Schema checks
    missing_gov_cols = REQUIRED_GOV_COLUMNS - set(gov_df.columns)
    if missing_gov_cols:
        errors.append(f"Governance missing columns: {sorted(missing_gov_cols)}")

    missing_story_cols = REQUIRED_STORY_COLUMNS - set(story_df.columns)
    if missing_story_cols:
        errors.append(f"Storytelling missing columns: {sorted(missing_story_cols)}")

    if errors:
        return False, errors

    # 2. Category & summary KPI checks
    gov_cats = set(gov_df["kpi_category"].dropna().unique())
    if gov_cats != REQUIRED_GOV_CATEGORIES:
        missing_c = REQUIRED_GOV_CATEGORIES - gov_cats
        extra_c = gov_cats - REQUIRED_GOV_CATEGORIES
        if missing_c:
            errors.append(f"Governance missing kpi_category values: {sorted(missing_c)}")
        if extra_c:
            errors.append(f"Governance unexpected kpi_category values: {sorted(extra_c)}")

    summary_df = gov_df[gov_df["kpi_category"] == "summary"].set_index("metric_name")
    missing_sum_metrics = REQUIRED_SUMMARY_METRICS - set(summary_df.index)
    if missing_sum_metrics:
        errors.append(f"Governance summary missing metrics: {sorted(missing_sum_metrics)}")

    valid_statuses = {"PASS", "WARN", "UNAVAILABLE"}
    bad_statuses = set(gov_df["compliance_status"].dropna().unique()) - valid_statuses
    if bad_statuses:
        errors.append(f"Invalid compliance_status values: {sorted(bad_statuses)}")

    story_cats = set(story_df["aggregation_type"].dropna().unique())
    missing_s_cats = REQUIRED_STORY_CATEGORIES - story_cats
    if missing_s_cats:
        errors.append(f"Storytelling missing aggregation_type values: {sorted(missing_s_cats)}")

    if errors:
        return False, errors

    # 3. Quarantine status invariant
    q_row = summary_df.loc["quarantined_records_detected"]
    q_src_row = summary_df.loc["quarantine_source_status"]
    if float(q_src_row["metric_value"]) == 1.0:
        if pd.isna(q_row["metric_value"]) or float(q_row["metric_value"]) < 0:
            errors.append("quarantined_records_detected must be >= 0 when quarantine_source_status == 1.0")
    else:
        if not pd.isna(q_row["metric_value"]) or q_row["compliance_status"] != "UNAVAILABLE":
            errors.append("quarantined_records_detected must be NaN and UNAVAILABLE when DB is offline")

    # 4. Cross-file & intra-file conservation checks
    unique_events = int(round(float(summary_df.loc["unique_deduplicated_events", "metric_value"])))
    vol_by_src_sum = int(round(float(gov_df[gov_df["kpi_category"] == "volume_by_source"]["metric_value"].sum())))
    if vol_by_src_sum != unique_events:
        errors.append(f"Volume mismatch: sum(volume_by_source)={vol_by_src_sum} != unique_events={unique_events}")

    sev_df = story_df[story_df["aggregation_type"] == "severity_distribution"]
    sev_sum = int(sev_df["event_count"].sum())
    if sev_sum != unique_events:
        errors.append(f"Severity count mismatch: sum(severity_distribution)={sev_sum} != unique_events={unique_events}")

    energy_pct_sum = float(sev_df["energy_share_pct"].sum())
    if abs(energy_pct_sum - 100.0) > 0.15:
        errors.append(f"Severity energy_share_pct sum={energy_pct_sum:.4f}% deviates from 100.0%")

    depth_df = story_df[story_df["aggregation_type"] == "depth_regime_breakdown"]
    depth_sum = int(depth_df["event_count"].sum())
    if depth_sum != unique_events:
        errors.append(f"Depth regime count mismatch: sum(depth_regime_breakdown)={depth_sum} != unique_events={unique_events}")

    # 5. Geospatial & physical range checks on recent_significant_events
    sig_df = story_df[story_df["aggregation_type"] == "recent_significant_events"]
    if sig_df.empty:
        errors.append("recent_significant_events cannot be empty.")
    else:
        for req_col in ("latitude", "longitude", "event_timestamp_utc", "source_id"):
            if sig_df[req_col].isna().any():
                errors.append(f"recent_significant_events contains nulls in mandatory column '{req_col}'")
        if not sig_df["latitude"].between(-90.0, 90.0).all():
            errors.append("recent_significant_events latitude out of [-90, 90] bounds")
        if not sig_df["longitude"].between(-180.0, 180.0).all():
            errors.append("recent_significant_events longitude out of [-180, 180] bounds")
        if not sig_df["mean_magnitude"].between(-1.0, 10.0).all():
            errors.append("recent_significant_events magnitude out of [-1, 10] bounds")
        if not sig_df["mean_depth_km"].between(-5.0, 800.0).all():
            errors.append("recent_significant_events depth_km out of [-5, 800] bounds")

        usgs_mask = sig_df["source_id"].astype(str).str.startswith("usgs_")
        if sig_df.loc[usgs_mask, "tsunami_flag"].isna().any():
            errors.append("USGS rows in recent_significant_events must have boolean tsunami_flag (not null)")
        if sig_df.loc[~usgs_mask, "tsunami_flag"].notna().any():
            errors.append("Non-USGS rows in recent_significant_events must have null tsunami_flag")

    # 6. Historical vs. current cohort checks
    hvc_df = story_df[story_df["aggregation_type"] == "historical_vs_current"]
    expected_cohorts = {"historical_baseline", "current_7d", "current_24h"}
    missing_cohorts = expected_cohorts - set(hvc_df["dimension_key"].unique())
    if missing_cohorts:
        errors.append(f"historical_vs_current missing cohorts: {sorted(missing_cohorts)}")
    for c_col in ("daily_rate", "median_depth_km", "daily_energy_joules", "m55_plus_share_pct"):
        if hvc_df[c_col].isna().any():
            errors.append(f"historical_vs_current contains nulls in '{c_col}'")

    return len(errors) == 0, errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate latest Gold Parquet files against W4 contract")
    parser.add_argument("--gold-dir", type=Path, default=DEFAULT_GOLD_DIR)
    args = parser.parse_args()

    ok, errors = validate_gold_pair(args.gold_dir)
    if ok:
        gov_p = find_latest_parquet(args.gold_dir, "governance")
        st_p = find_latest_parquet(args.gold_dir, "storytelling")
        gov_df = pd.read_parquet(gov_p)
        st_df = pd.read_parquet(st_p)
        print(
            f"[PASS] Gold Contract Validated:\n"
            f"  - Governance:   {gov_p.name} ({gov_df.shape[0]} rows x {gov_df.shape[1]} cols, computed_at={gov_df['computed_at'].iloc[0]})\n"
            f"  - Storytelling: {st_p.name} ({st_df.shape[0]} rows x {st_df.shape[1]} cols, computed_at={st_df['computed_at'].iloc[0]})"
        )
        return 0

    print("[FAIL] Gold Contract Validation Errors:", file=sys.stderr)
    for err in errors:
        print(f"  - {err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
