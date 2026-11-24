"""
Comprehensive offline automated test suite for Workshop #4 (`tests/test_workshop4.py`).

Covers all 7 verification groups:
1. Gold data contract, cross-dataset conservation invariants, and non-literal KPIs
2. Both Dash apps (`:8050` Governance, `:8051` Storytelling) HTTP routes & graceful empty-Gold callbacks
3. Pure figure builders (`figures_governance.py`, `figures_storytelling.py`) on real & empty DataFrames
4. Deterministic narrative engine (`narrative.py`) & zero-jargon checks on Storytelling UI/figures
5. Static AST & regex audit of `dashboard/**/*.py` enforcing Golden Rules R1 & R2
6. `gold_loader` error handling, filename-timestamp ordering (independent of mtime), and cache invalidation
7. `requirements.txt` dependency manifest completeness
"""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import re
from typing import Any, List

import pandas as pd
import plotly.graph_objects as go
import pytest

from dashboard import gold_loader
from dashboard.figures_governance import (
    build_null_rate_fig,
    build_outlier_quartile_spread_fig,
    build_outlier_rate_fig,
    build_text_length_fig,
    build_volume_over_time_fig,
    extract_governance_kpis,
    prepare_governance_table_records,
)
from dashboard.figures_storytelling import (
    build_comparison_panel_fig,
    build_daily_volume_swarms_fig,
    build_depth_attenuation_fig,
    build_epicenter_map_fig,
    build_saai_trend_fig,
    build_severity_energy_fig,
    build_top_regions_fig,
)
from dashboard.governance_app import DEFAULT_GOVERNANCE_PORT, create_governance_app
from dashboard.narrative import build_narrative
from dashboard.storytelling_app import DEFAULT_STORYTELLING_PORT, create_storytelling_app
from scripts.validate_gold_contract import validate_gold_pair

REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = REPO_ROOT / "dashboard"
GOLD_DIR = REPO_ROOT / "datalake_gold"

BANNED_STORY_JARGON = re.compile(
    r"\b(parquet|dataframe|dedup|deduplication|iqr|pipeline|schema|raw|silver|gold|spark|pyspark|bronze|medallion|etl|aggregation_type)\b",
    re.IGNORECASE,
)

FORBIDDEN_R1_TOKENS = (
    "datalake_silver",
    "datalake_bronze",
    "workshop_1",
    "requests.get",
    "urllib",
    "psycopg2",
    "pyspark",
    "read_json",
    "sqlalchemy",
    "sqlite3",
)

FORBIDDEN_R1_IMPORTS = {
    "requests",
    "urllib",
    "psycopg2",
    "pyspark",
    "sqlalchemy",
    "sqlite3",
}


@pytest.fixture(scope="module")
def gov_df() -> pd.DataFrame:
    return gold_loader.load_gold("governance", gold_dir=GOLD_DIR).df


@pytest.fixture(scope="module")
def story_df() -> pd.DataFrame:
    return gold_loader.load_gold("storytelling", gold_dir=GOLD_DIR).df


# ---------------------------------------------------------------------------
# Group 1: Gold Data Contract & Non-Literal KPIs
# ---------------------------------------------------------------------------
def test_gold_contract_validator_and_invariants(gov_df: pd.DataFrame, story_df: pd.DataFrame):
    ok, errors = validate_gold_pair(GOLD_DIR)
    assert ok and errors == [], f"Gold contract validation failed: {errors}"

    # Verify non-literal dynamic KPIs in governance
    summary = gov_df[gov_df["kpi_category"] == "summary"].set_index("metric_name")
    matched = float(summary.loc["cross_source_matched_pairs", "metric_value"])
    assert matched >= 0.0
    assert matched != 568.0, "cross_source_matched_pairs must not be legacy hardcoded 568.0"

    q_source = float(summary.loc["quarantine_source_status", "metric_value"])
    q_status = str(summary.loc["quarantined_records_detected", "compliance_status"])
    q_val = summary.loc["quarantined_records_detected", "metric_value"]
    if q_source == 1.0:
        assert pd.notna(q_val) and float(q_val) >= 0.0
        assert q_status in {"PASS", "WARN"}
    else:
        assert pd.isna(q_val)
        assert q_status == "UNAVAILABLE"

    # Verify src/gold_pyspark.py has zero legacy hardcoded assignment literals
    gold_src = (REPO_ROOT / "src" / "gold_pyspark.py").read_text(encoding="utf-8")
    assert "= 568.0" not in gold_src
    assert "= 3.0" not in gold_src

    # Verify storytelling conservation & bounds
    uniq_events = float(summary.loc["unique_deduplicated_events", "metric_value"])
    sev = story_df[story_df["aggregation_type"] == "severity_distribution"]
    dep = story_df[story_df["aggregation_type"] == "depth_regime_breakdown"]
    assert float(sev["event_count"].sum()) == pytest.approx(uniq_events)
    assert float(dep["event_count"].sum()) == pytest.approx(uniq_events)
    assert float(sev["energy_share_pct"].sum()) == pytest.approx(100.0, abs=0.1)


# ---------------------------------------------------------------------------
# Group 2: Dash Layouts, HTTP Routes & Graceful Empty-Gold Degradation
# ---------------------------------------------------------------------------
def test_both_dash_apps_http_200_routes():
    gov_app = create_governance_app()
    story_app = create_storytelling_app()

    for app_instance in (gov_app, story_app):
        client = app_instance.server.test_client()
        for endpoint in ("/", "/_dash-layout", "/_dash-dependencies"):
            resp = client.get(endpoint)
            assert resp.status_code == 200, f"{app_instance.title} failed on {endpoint}"


def test_dash_apps_graceful_degradation_when_gold_missing(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))
    gold_loader.reload()

    gov_app = create_governance_app()
    story_app = create_storytelling_app()

    for app_instance in (gov_app, story_app):
        client = app_instance.server.test_client()
        assert client.get("/").status_code == 200
        assert client.get("/_dash-layout").status_code == 200


# ---------------------------------------------------------------------------
# Group 3: Pure Figure Builders (Real & Empty DataFrames)
# ---------------------------------------------------------------------------
def test_governance_figure_builders_and_kpis(gov_df: pd.DataFrame):
    kpis = extract_governance_kpis(gov_df)
    assert len(kpis) == 7
    assert extract_governance_kpis(pd.DataFrame()) == []

    rows = prepare_governance_table_records(gov_df)
    assert len(rows) == len(gov_df)
    assert prepare_governance_table_records(pd.DataFrame()) == []

    builders = [
        build_null_rate_fig,
        build_volume_over_time_fig,
        build_outlier_rate_fig,
        build_outlier_quartile_spread_fig,
        build_text_length_fig,
    ]
    for fn in builders:
        fig = fn(gov_df)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1, f"{fn.__name__} returned empty traces"
        assert fig.layout.title.text, f"{fn.__name__} missing title"
        assert fig.layout.xaxis.title.text, f"{fn.__name__} missing x-axis label"
        assert fig.layout.yaxis.title.text, f"{fn.__name__} missing y-axis label"
        assert any("Source:" in str(a.text) for a in (fig.layout.annotations or []))

        empty_fig = fn(pd.DataFrame())
        assert isinstance(empty_fig, go.Figure)
        assert len(empty_fig.layout.annotations or []) >= 2

    # Cumulative mode & empty source selection
    fig_cum = build_volume_over_time_fig(gov_df, selected_sources=["usgs_fdsn"], view_mode="cumulative")
    assert len(fig_cum.data) == 1
    fig_none = build_volume_over_time_fig(gov_df, selected_sources=[])
    assert len(fig_none.data) == 0


def test_storytelling_figure_builders(story_df: pd.DataFrame):
    cartesian_builders = [
        lambda d: build_severity_energy_fig(d),
        lambda d: build_saai_trend_fig(d),
        lambda d: build_daily_volume_swarms_fig(d),
        lambda d: build_top_regions_fig(d),
        lambda d: build_comparison_panel_fig(d, mode="agency"),
        lambda d: build_comparison_panel_fig(d, mode="historical_vs_current"),
    ]
    for idx, fn in enumerate(cartesian_builders):
        fig = fn(story_df)
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1, f"Story cartesian builder {idx} returned empty traces"
        assert fig.layout.title.text
        assert fig.layout.xaxis.title.text
        assert fig.layout.yaxis.title.text
        assert any("Source:" in str(a.text) for a in (fig.layout.annotations or []))

        empty_fig = fn(pd.DataFrame())
        assert isinstance(empty_fig, go.Figure)
        assert len(empty_fig.layout.annotations or []) >= 2

    # Donut builder (Panel A Right)
    fig_depth = build_depth_attenuation_fig(story_df)
    assert isinstance(fig_depth, go.Figure)
    assert len(fig_depth.data) == 1
    assert isinstance(fig_depth.data[0], go.Pie)
    assert len(fig_depth.data[0].labels) == 3

    # Geospatial Map builder (Panel F)
    fig_map = build_epicenter_map_fig(story_df)
    assert isinstance(fig_map, go.Figure)
    assert len(fig_map.data) >= 1
    scatter_trace = fig_map.data[0]
    assert isinstance(scatter_trace, go.Scattergeo)
    assert all(float(sz) > 0 for sz in scatter_trace.marker.size)
    assert "Depth (km)" in str(scatter_trace.marker.colorbar.title.text)

    # Empty filter edge cases
    assert len(build_epicenter_map_fig(story_df, selected_agencies=[]).data) == 0
    assert len(build_saai_trend_fig(story_df, alert_tiers=[]).data) == 0
    assert len(build_top_regions_fig(story_df, alert_tiers=[]).data) == 0


# ---------------------------------------------------------------------------
# Group 4: Deterministic Narrative Engine & Zero Technical Jargon
# ---------------------------------------------------------------------------
def _extract_visible_layout_strings(node: Any, sink: List[str]) -> None:
    if isinstance(node, dict):
        props = node.get("props", {})
        if isinstance(props, dict):
            for prop_key in ("children", "label", "placeholder", "title"):
                val = props.get(prop_key)
                if isinstance(val, str):
                    sink.append(val)
        for child in node.values():
            _extract_visible_layout_strings(child, sink)
    elif isinstance(node, list):
        for item in node:
            _extract_visible_layout_strings(item, sink)


def test_narrative_responsiveness_and_zero_jargon(story_df: pd.DataFrame):
    full_brief = build_narrative(story_df)
    assert full_brief["headline"]
    assert "does NOT predict" in full_brief["disclaimer"]
    assert len(full_brief["bullets"]) == 4

    filtered_brief = build_narrative(
        story_df,
        filters={
            "start_date": "2026-11-21",
            "end_date": "2026-11-24",
            "alert_tiers": ["NORMAL"],
            "agencies": ["usgs_realtime"],
        },
    )
    assert filtered_brief["full_text"] != full_brief["full_text"]

    empty_brief = build_narrative(pd.DataFrame())
    assert "does NOT predict" in empty_brief["disclaimer"]

    for label, brief in (("full", full_brief), ("filtered", filtered_brief), ("empty", empty_brief)):
        hits = BANNED_STORY_JARGON.findall(brief["full_text"])
        assert hits == [], f"Banned jargon in {label} narrative: {hits}"

    # Check Storytelling app layout & figure annotations/titles/legends for banned jargon
    story_app = create_storytelling_app()
    layout_payload = json.loads(
        story_app.server.test_client().get("/_dash-layout").get_data(as_text=True)
    )
    visible_ui_strings: List[str] = []
    _extract_visible_layout_strings(layout_payload, visible_ui_strings)
    ui_hits = BANNED_STORY_JARGON.findall(" ".join(visible_ui_strings))
    assert ui_hits == [], f"Banned engineering jargon in Storytelling UI layout: {ui_hits}"

    story_figs = [
        build_severity_energy_fig(story_df),
        build_depth_attenuation_fig(story_df),
        build_saai_trend_fig(story_df),
        build_daily_volume_swarms_fig(story_df),
        build_top_regions_fig(story_df),
        build_comparison_panel_fig(story_df, mode="agency"),
        build_comparison_panel_fig(story_df, mode="historical_vs_current"),
        build_epicenter_map_fig(story_df),
    ]
    for fig in story_figs:
        chunks = [str(fig.layout.title.text)]
        for tr in fig.data:
            if getattr(tr, "name", None):
                chunks.append(str(tr.name))
        for ann in (fig.layout.annotations or []):
            chunks.append(str(ann.text))
        fig_hits = BANNED_STORY_JARGON.findall(" ".join(chunks))
        assert fig_hits == [], f"Banned jargon in Storytelling figure '{fig.layout.title.text}': {fig_hits}"


# ---------------------------------------------------------------------------
# Group 5: Static AST & Regex Enforcement of Golden Rules R1 & R2
# ---------------------------------------------------------------------------
def _imported_module_names(py_path: Path) -> List[str]:
    tree = ast.parse(py_path.read_text(encoding="utf-8"), filename=str(py_path))
    names: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.append(node.module)
            for alias in node.names:
                names.append(alias.name)
    return names


def test_static_golden_rules_r1_and_r2():
    py_files = sorted(DASHBOARD_DIR.rglob("*.py"))
    assert len(py_files) >= 8

    for py_file in py_files:
        src = py_file.read_text(encoding="utf-8")
        for token in FORBIDDEN_R1_TOKENS:
            assert token not in src, f"R1 violation: forbidden token '{token}' in {py_file.name}"

        imports = _imported_module_names(py_file)
        for mod_name in imports:
            root_pkg = mod_name.split(".")[0]
            assert root_pkg not in FORBIDDEN_R1_IMPORTS, (
                f"R1 AST violation: forbidden import '{mod_name}' in {py_file.name}"
            )

    # R2: Zero cross-imports between Governance and Storytelling modules
    gov_modules = [DASHBOARD_DIR / "governance_app.py", DASHBOARD_DIR / "figures_governance.py"]
    story_modules = [
        DASHBOARD_DIR / "storytelling_app.py",
        DASHBOARD_DIR / "figures_storytelling.py",
        DASHBOARD_DIR / "narrative.py",
    ]
    for g_file in gov_modules:
        g_imps = " ".join(_imported_module_names(g_file))
        for forbidden_story in ("storytelling_app", "figures_storytelling", "narrative"):
            assert forbidden_story not in g_imps, f"R2 cross-import in {g_file.name}: {forbidden_story}"

    for s_file in story_modules:
        s_imps = " ".join(_imported_module_names(s_file))
        for forbidden_gov in ("governance_app", "figures_governance"):
            assert forbidden_gov not in s_imps, f"R2 cross-import in {s_file.name}: {forbidden_gov}"

    # Distinct default ports (:8050 vs :8051)
    assert DEFAULT_GOVERNANCE_PORT == 8050
    assert DEFAULT_STORYTELLING_PORT == 8051
    assert DEFAULT_GOVERNANCE_PORT != DEFAULT_STORYTELLING_PORT


# ---------------------------------------------------------------------------
# Group 6: gold_loader Error Handling, Filename Timestamp Selection & Cache
# ---------------------------------------------------------------------------
def test_gold_loader_errors_timestamp_order_and_cache(tmp_path: Path, gov_df: pd.DataFrame):
    # Empty directory -> GoldNotFoundError
    with pytest.raises(gold_loader.GoldNotFoundError):
        gold_loader.load_gold("governance", gold_dir=tmp_path)

    # Missing directory -> GoldNotFoundError
    with pytest.raises(gold_loader.GoldNotFoundError):
        gold_loader.find_latest("storytelling", gold_dir=tmp_path / "does_not_exist")

    # Corrupt schema Parquet -> GoldContractError
    bad_dir = tmp_path / "bad_gold"
    bad_dir.mkdir()
    pd.DataFrame({"wrong_col": [1, 2]}).to_parquet(bad_dir / "governance_20261124_000000.parquet")
    with pytest.raises(gold_loader.GoldContractError):
        gold_loader.load_gold("governance", gold_dir=bad_dir)

    # Filename timestamp strictly wins over filesystem mtime
    multi_dir = tmp_path / "multi_gold"
    multi_dir.mkdir()
    older_file = multi_dir / "governance_20261120_090000.parquet"
    newer_file = multi_dir / "governance_20261124_183000.parquet"

    df_old = gov_df.copy()
    df_old["computed_at"] = "2026-11-20T09:00:00Z"
    df_new = gov_df.copy()
    df_new["computed_at"] = "2026-11-24T18:30:00Z"

    df_old.to_parquet(older_file, index=False)
    df_new.to_parquet(newer_file, index=False)
    # Artificially give older_file a NEWER filesystem mtime than newer_file
    os.utime(newer_file, (1_700_000_000, 1_700_000_000))
    os.utime(older_file, (1_800_000_000, 1_800_000_000))

    b1 = gold_loader.load_gold("governance", gold_dir=multi_dir)
    assert b1.filename == "governance_20261124_183000.parquet"
    assert b1.computed_at == "2026-11-24T18:30:00Z"

    # Cache hit returns identical object until reload()
    b2 = gold_loader.load_gold("governance", gold_dir=multi_dir)
    assert b1 is b2
    b3 = gold_loader.reload("governance", gold_dir=multi_dir)
    assert b3 is not None and b3 is not b1


# ---------------------------------------------------------------------------
# Group 7: Dependency Manifest Completeness
# ---------------------------------------------------------------------------
def test_requirements_txt_contains_dashboard_dependencies():
    req_text = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8").lower()
    for pkg in ("dash", "plotly", "dash-bootstrap-components", "pandas"):
        assert re.search(rf"^{re.escape(pkg)}\b", req_text, flags=re.MULTILINE), (
            f"Missing required package '{pkg}' in requirements.txt"
        )
