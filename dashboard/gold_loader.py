"""
Exclusive Gold Layer data loader and cache manager (`dashboard/gold_loader.py`).

Enforces Workshop #4 Golden Rule R1: all dashboard data is loaded strictly from
`datalake_gold/governance_*.parquet` and `datalake_gold/storytelling_*.parquet`
using `pandas.read_parquet()`, selecting the latest snapshot by the UTC timestamp
token embedded in the filename (`YYYYMMDD_HHMMSS`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import re
from typing import Dict, Literal, Optional, Set, Tuple

import pandas as pd

GoldKind = Literal["governance", "storytelling"]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD_DIR = PROJECT_ROOT / "datalake_gold"
STAMP_PATTERN = re.compile(r"(\d{8}_\d{6})")

REQUIRED_COLUMNS: Dict[GoldKind, Set[str]] = {
    "governance": {
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
    },
    "storytelling": {
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
    },
}

CATEGORY_COLUMN: Dict[GoldKind, str] = {
    "governance": "kpi_category",
    "storytelling": "aggregation_type",
}


class GoldNotFoundError(FileNotFoundError):
    """Raised when no matching Gold file exists in the target directory."""


class GoldContractError(ValueError):
    """Raised when a Gold file violates the minimum schema contract."""


@dataclass(frozen=True)
class GoldBundle:
    """Immutable container holding a loaded Gold dataset and pre-indexed slices."""

    kind: GoldKind
    df: pd.DataFrame
    path: Path
    filename: str
    computed_at: str
    loaded_at: str
    mtime_ns: int
    by_category: Dict[str, pd.DataFrame]

    @property
    def signature(self) -> str:
        """Unique file-version token combining canonical path and mtime_ns."""
        return f"{self.path.resolve()}::{self.mtime_ns}"


_BUNDLE_CACHE: Dict[Tuple[GoldKind, Path], GoldBundle] = {}


def resolve_gold_dir(gold_dir: Optional[Path] = None) -> Path:
    """Resolves the Gold directory from explicit argument, `GOLD_DIR` env var, or repo root."""
    if gold_dir is not None:
        return Path(gold_dir)
    env_override = os.getenv("GOLD_DIR")
    if env_override:
        return Path(env_override)
    return DEFAULT_GOLD_DIR


def _filename_sort_key(path: Path) -> Tuple[str, str]:
    """Extracts `(YYYYMMDD_HHMMSS, filename)` so sorting is strictly by filename timestamp."""
    match = STAMP_PATTERN.search(path.name)
    stamp = match.group(1) if match else ""
    return (stamp, path.name)


def find_latest(kind: GoldKind, gold_dir: Optional[Path] = None) -> Path:
    """
    Locates the latest `governance_*.parquet` or `storytelling_*.parquet` file
    ordered by the UTC timestamp in the filename (never by filesystem mtime).
    """
    if kind not in ("governance", "storytelling"):
        raise ValueError(f"Unsupported Gold dataset kind: {kind!r}")

    target_dir = resolve_gold_dir(gold_dir)
    if not target_dir.exists() or not target_dir.is_dir():
        raise GoldNotFoundError(
            f"Gold directory not found at '{target_dir}'. "
            "Run the Airflow DAGs (Bronze -> Silver -> Gold) or generate Gold outputs first."
        )

    candidates = sorted(target_dir.glob(f"{kind}_*.parquet"), key=_filename_sort_key)
    if not candidates:
        raise GoldNotFoundError(
            f"No '{kind}_*.parquet' files found in '{target_dir}'. "
            "Run the Airflow DAGs in sequence to populate datalake_gold/ before opening the dashboard."
        )
    return candidates[-1]


def peek_latest_signature(kind: GoldKind, gold_dir: Optional[Path] = None) -> str:
    """Returns `resolved_path::mtime_ns` for the latest Gold file without reading its rows."""
    latest = find_latest(kind, gold_dir=gold_dir)
    stat = latest.stat()
    return f"{latest.resolve()}::{stat.st_mtime_ns}"


def load_gold(
    kind: GoldKind,
    gold_dir: Optional[Path] = None,
    force_reload: bool = False,
) -> GoldBundle:
    """
    Loads the latest Gold file using `pd.read_parquet()`, validates mandatory
    columns against the Workshop #4 contract, and caches by `(path, mtime_ns)`.
    """
    target_dir = resolve_gold_dir(gold_dir).resolve()
    latest_path = find_latest(kind, gold_dir=target_dir).resolve()
    mtime_ns = latest_path.stat().st_mtime_ns
    cache_key = (kind, target_dir)

    if not force_reload and cache_key in _BUNDLE_CACHE:
        cached = _BUNDLE_CACHE[cache_key]
        if cached.path == latest_path and cached.mtime_ns == mtime_ns:
            return cached

    df = pd.read_parquet(latest_path)
    if df.empty:
        raise GoldContractError(f"Gold file '{latest_path.name}' is empty (0 rows).")

    missing_cols = REQUIRED_COLUMNS[kind] - set(df.columns)
    if missing_cols:
        raise GoldContractError(
            f"Gold file '{latest_path.name}' is missing required contract columns: "
            f"{sorted(missing_cols)}"
        )

    cat_col = CATEGORY_COLUMN[kind]
    by_cat: Dict[str, pd.DataFrame] = {
        str(cat_val): sub_df.copy()
        for cat_val, sub_df in df.groupby(cat_col, dropna=True, sort=False)
    }

    comp_series = df["computed_at"].dropna()
    computed_at = str(comp_series.iloc[0]) if not comp_series.empty else "Unknown"
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bundle = GoldBundle(
        kind=kind,
        df=df,
        path=latest_path,
        filename=latest_path.name,
        computed_at=computed_at,
        loaded_at=loaded_at,
        mtime_ns=mtime_ns,
        by_category=by_cat,
    )
    _BUNDLE_CACHE[cache_key] = bundle
    return bundle


def reload(
    kind: Optional[GoldKind] = None,
    gold_dir: Optional[Path] = None,
) -> Optional[GoldBundle]:
    """Invalidates cached Gold bundles and optionally reloads the specified kind immediately."""
    if kind is None:
        _BUNDLE_CACHE.clear()
        return None

    target_dir = resolve_gold_dir(gold_dir).resolve()
    _BUNDLE_CACHE.pop((kind, target_dir), None)
    return load_gold(kind, gold_dir=target_dir, force_reload=True)
