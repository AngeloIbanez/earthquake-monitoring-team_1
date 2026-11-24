"""
Pure figure and KPI-extraction functions for the Governance Dashboard (`dashboard/figures_governance.py`).

All functions accept a `pandas.DataFrame` loaded from `governance_*.parquet` and
return `plotly.graph_objects.Figure` or plain Python records, enabling deterministic
offline unit testing and direct reuse in design-lab notebooks without running a server.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import plotly.graph_objects as go

from .theme import (
    GOV_SOURCE_NOTE,
    SOURCE_COLORS,
    SOURCE_DISPLAY_NAMES,
    STATUS_BADGE_TEXT,
    STATUS_COLORS,
    empty_figure,
    style_figure,
)

FIELD_LABELS: Dict[str, str] = {
    "depth_km_pinned_default_10km": "Depth Pinned at Default 10 km",
    "event_id": "Event Identifier (event_id)",
    "raw_time": "Origin Epoch ms (raw_time)",
    "magnitude": "Magnitude Value",
    "mag_type": "Magnitude Scale Short Code",
    "place": "Location Descriptor (place)",
    "longitude": "Longitude (°E)",
    "latitude": "Latitude (°N)",
    "depth_km": "Hypocentral Depth (km)",
    "tsunami": "Tsunami Bulletin Flag",
    "source_file": "Source Partition File",
    "magnitude_type": "Magnitude Scale Full",
    "energy_joules": "Radiated Energy (J)",
}

NUMERIC_FIELD_LABELS: Dict[str, str] = {
    "depth_km": "Focal Depth (km)",
    "energy_joules": "Linear Energy (J)",
    "magnitude": "Magnitude (Mw eq.)",
    "log10_energy": "Log10 Energy (log10 J)",
}


def _get_summary_lookup(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if df is None or df.empty or "kpi_category" not in df.columns:
        return {}
    sub = df[df["kpi_category"] == "summary"]
    return {str(row["metric_name"]): row.to_dict() for _, row in sub.iterrows()}


def _fmt_int(val: Any) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    return f"{int(round(float(val))):,d}"


def _fmt_pct(val: Any, decimals: int = 2) -> str:
    if val is None or pd.isna(val):
        return "N/A"
    return f"{float(val):.{decimals}f}%"


def extract_governance_kpis(df: pd.DataFrame) -> List[Dict[str, str]]:
    """Extracts the 7 executive governance KPI card specifications from `summary` rows."""
    lookup = _get_summary_lookup(df)
    if not lookup:
        return []

    def _row(key: str) -> Dict[str, Any]:
        return lookup.get(key, {})

    r_total = _row("total_silver_records_read")
    r_uniq = _row("unique_deduplicated_events")
    r_dup = _row("intra_lake_duplicate_rate_pct")
    r_comp = _row("schema_compliance_rate_pct")
    r_pin = _row("pinned_10km_depth_rate_pct")
    r_quar = _row("quarantined_records_detected")
    r_match = _row("cross_source_matched_pairs")
    r_grp = _row("event_groups_total")

    q_status = str(r_quar.get("compliance_status", "UNAVAILABLE"))
    q_val = r_quar.get("metric_value")
    if q_status == "UNAVAILABLE" or q_val is None or pd.isna(q_val):
        quar_value_txt = "N/A"
        quar_sub_txt = "Audit database offline (safe fallback active)"
        quar_status = "UNAVAILABLE"
    else:
        quar_value_txt = _fmt_int(q_val)
        quar_rate = r_quar.get("secondary_value")
        quar_sub_txt = (
            f"{float(quar_rate):.3f}% of total evaluated records"
            if quar_rate is not None and not pd.isna(quar_rate)
            else "Malformed records isolated in quarantine"
        )
        quar_status = q_status

    grp_total_txt = _fmt_int(r_grp.get("metric_value", r_match.get("secondary_value")))

    return [
        {
            "title": "Curated Records Read",
            "value": _fmt_int(r_total.get("metric_value")),
            "subtitle": f"Across {_fmt_int(r_total.get('secondary_value'))} ingested Parquet partitions",
            "status": str(r_total.get("compliance_status", "PASS")),
            "threshold": f"SLA: ≥ {_fmt_int(r_total.get('threshold_value'))} records",
        },
        {
            "title": "Unique Canonical Events",
            "value": _fmt_int(r_uniq.get("metric_value")),
            "subtitle": "Deduplicated by canonical event_id",
            "status": str(r_uniq.get("compliance_status", "PASS")),
            "threshold": f"SLA: ≥ {_fmt_int(r_uniq.get('threshold_value'))} events",
        },
        {
            "title": "Cross-Snapshot Duplicate Rate",
            "value": _fmt_pct(r_dup.get("metric_value"), 2),
            "subtitle": f"{_fmt_int(r_dup.get('secondary_value'))} overlapping snapshot rows consolidated",
            "status": str(r_dup.get("compliance_status", "PASS")),
            "threshold": f"SLA: ≤ {_fmt_pct(r_dup.get('threshold_value'), 0)} warn ceiling",
        },
        {
            "title": "Physical & Bound Compliance",
            "value": _fmt_pct(r_comp.get("metric_value"), 2),
            "subtitle": f"{_fmt_int(r_comp.get('secondary_value'))} valid coordinate & magnitude rows",
            "status": str(r_comp.get("compliance_status", "PASS")),
            "threshold": f"SLA: ≥ {_fmt_pct(r_comp.get('threshold_value'), 1)}",
        },
        {
            "title": "Pinned 10 km Depth Rate",
            "value": _fmt_pct(r_pin.get("metric_value"), 2),
            "subtitle": f"{_fmt_int(r_pin.get('secondary_value'))} events assigned default 10 km depth",
            "status": str(r_pin.get("compliance_status", "WARN")),
            "threshold": f"SLA: ≤ {_fmt_pct(r_pin.get('threshold_value'), 0)} fixed-depth limit",
        },
        {
            "title": "Quarantined Records",
            "value": quar_value_txt,
            "subtitle": quar_sub_txt,
            "status": quar_status,
            "threshold": f"SLA: ≤ {_fmt_int(r_quar.get('threshold_value'))} rejected rows",
        },
        {
            "title": "Cross-Agency Matched Pairs",
            "value": _fmt_int(r_match.get("metric_value")),
            "subtitle": f"Linked into {grp_total_txt} multi-agency event groups",
            "status": str(r_match.get("compliance_status", "PASS")),
            "threshold": f"SLA: ≥ {_fmt_int(r_match.get('threshold_value'))} matched pairs",
        },
    ]


def build_null_rate_fig(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar chart of explicit field null rates & default 10 km depth pinning, sorted by severity."""
    if df is None or df.empty or "kpi_category" not in df.columns:
        return empty_figure(
            "No null-rate metrics available in Gold dataset.",
            title="Explicit Null & Pinned-Depth Rate by Field (%)",
            source_note=GOV_SOURCE_NOTE,
            height=390,
        )

    sub = df[df["kpi_category"] == "null_rate_per_field"].copy()
    if sub.empty:
        return empty_figure(
            "No null_rate_per_field rows present.",
            title="Explicit Null & Pinned-Depth Rate by Field (%)",
            source_note=GOV_SOURCE_NOTE,
            height=390,
        )

    sub["display_label"] = sub["metric_name"].map(lambda k: FIELD_LABELS.get(str(k), str(k)))
    sub = sub.sort_values(["metric_value", "display_label"], ascending=[True, False])

    fig = go.Figure()
    for status_key in ("PASS", "WARN"):
        part = sub[sub["compliance_status"] == status_key]
        if part.empty:
            continue
        color = STATUS_COLORS.get(status_key, STATUS_COLORS["PASS"])
        badge = STATUS_BADGE_TEXT.get(status_key, status_key)
        fig.add_trace(
            go.Bar(
                x=part["metric_value"],
                y=part["display_label"],
                orientation="h",
                name=badge,
                marker_color=color,
                text=[f"{v:.2f}% ({int(round(c)):,d})" for v, c in zip(part["metric_value"], part["secondary_value"])],
                textposition="outside",
                cliponaxis=False,
                customdata=list(zip(part["secondary_value"], part["threshold_value"], part["compliance_status"])),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Rate: %{x:.2f}% (%{customdata[0]:,.0f} events)<br>"
                    "SLA Threshold: ≤ %{customdata[1]:.1f}%<br>"
                    "Status: %{customdata[2]}<extra></extra>"
                ),
            )
        )

    fig.add_vline(
        x=5.0,
        line_dash="dot",
        line_color="#475569",
        annotation_text="Null SLA (5%)",
        annotation_position="top",
    )
    fig.add_vline(
        x=25.0,
        line_dash="dash",
        line_color=STATUS_COLORS["WARN"],
        annotation_text="Pinned-Depth Warn (25%)",
        annotation_position="top right",
    )

    max_x = max(float(sub["metric_value"].max() or 0.0) * 1.28, 30.0)
    fig.update_xaxes(range=[0, max_x])
    fig.update_layout(yaxis={"categoryorder": "array", "categoryarray": sub["display_label"].tolist()})

    explicit_cols = sub[sub["metric_name"] != "depth_km_pinned_default_10km"]
    zero_null_cnt = int((explicit_cols["metric_value"] == 0.0).sum())
    total_exp_cnt = int(len(explicit_cols))
    chart_title = (
        f"Explicit Null Rate ({zero_null_cnt}/{total_exp_cnt} Columns at 0.00%) vs. Default 10 km Depth Pinning"
        if total_exp_cnt > 0
        else "Explicit Null Rate vs. Agency Default 10 km Depth Pinning"
    )

    return style_figure(
        fig,
        title=chart_title,
        x_title="Affected Record Share (%)",
        y_title="Canonical Attribute / Check",
        source_note=GOV_SOURCE_NOTE,
        height=400,
    )


def build_volume_over_time_fig(
    df: pd.DataFrame,
    selected_sources: Optional[Sequence[str]] = None,
    view_mode: str = "daily",
) -> go.Figure:
    """Line chart of daily or cumulative ingested earthquake volume per seismic source."""
    if df is None or df.empty or "kpi_category" not in df.columns:
        return empty_figure(
            "No temporal ingestion volume records available.",
            title="Ingested Event Volume Over Time",
            source_note=GOV_SOURCE_NOTE,
            height=400,
        )

    sub = df[df["kpi_category"] == "volume_over_time"].copy()
    if sub.empty:
        return empty_figure(
            "No volume_over_time rows found.",
            title="Ingested Event Volume Over Time",
            source_note=GOV_SOURCE_NOTE,
            height=400,
        )

    if selected_sources is not None:
        if len(selected_sources) == 0:
            return empty_figure(
                "Select at least one seismic source in the dropdown filter above.",
                title="Ingested Event Volume Over Time",
                source_note=GOV_SOURCE_NOTE,
                height=400,
            )
        sub = sub[sub["source_id"].isin(list(selected_sources))].copy()

    if sub.empty:
        return empty_figure(
            "No ingestion records match the selected source filter.",
            title="Ingested Event Volume Over Time",
            source_note=GOV_SOURCE_NOTE,
            height=400,
        )

    sub = sub.sort_values(["period_date", "source_id"])
    is_cum = str(view_mode).lower() == "cumulative"

    fig = go.Figure()
    for src_id, grp in sub.groupby("source_id", sort=True):
        grp_sorted = grp.sort_values("period_date")
        y_vals = grp_sorted["metric_value"].cumsum() if is_cum else grp_sorted["metric_value"]
        label = SOURCE_DISPLAY_NAMES.get(str(src_id), str(src_id))
        color = SOURCE_COLORS.get(str(src_id), "#3b6998")
        fig.add_trace(
            go.Scatter(
                x=grp_sorted["period_date"],
                y=y_vals,
                mode="lines+markers",
                name=label,
                line={"color": color, "width": 2.3},
                marker={"size": 6},
                hovertemplate=(
                    f"<b>{label}</b> ({src_id})<br>"
                    "Date (UTC): %{x}<br>"
                    + ("Cumulative Events: %{y:,.0f} records<extra></extra>" if is_cum else "Daily Events: %{y:,.0f} records<extra></extra>")
                ),
            )
        )

    if sub["source_id"].nunique() > 1:
        daily_tot = sub.groupby("period_date", as_index=False)["metric_value"].sum().sort_values("period_date")
        tot_y = daily_tot["metric_value"].cumsum() if is_cum else daily_tot["metric_value"]
        fig.add_trace(
            go.Scatter(
                x=daily_tot["period_date"],
                y=tot_y,
                mode="lines",
                name="Selected Sources Total",
                line={"color": "#0f172a", "width": 2.0, "dash": "dash"},
                hovertemplate=(
                    "<b>Selected Sources Total</b><br>"
                    "Date (UTC): %{x}<br>"
                    "Events: %{y:,.0f} records<extra></extra>"
                ),
            )
        )

    mode_title = "Cumulative Deduplicated Event Growth by Agency" if is_cum else "Daily Ingested Event Volume by Agency"
    y_label = "Cumulative Unique Events (records)" if is_cum else "Daily Ingested Events (records/day)"
    return style_figure(
        fig,
        title=mode_title,
        x_title="Observation Date (UTC)",
        y_title=y_label,
        source_note=GOV_SOURCE_NOTE,
        height=400,
        hovermode="x unified",
    )


def build_outlier_rate_fig(df: pd.DataFrame) -> go.Figure:
    """Bar chart of Tukey IQR outlier rates (%) across numeric fields with 15% SLA threshold."""
    if df is None or df.empty or "kpi_category" not in df.columns:
        return empty_figure(
            "No numeric outlier KPIs available.",
            title="Tukey IQR Outlier Rate per Numeric Field (%)",
            source_note=GOV_SOURCE_NOTE,
            height=360,
        )

    sub = df[df["kpi_category"] == "outlier_rate_per_numeric_field"].copy()
    if sub.empty:
        return empty_figure(
            "No outlier_rate_per_numeric_field rows present.",
            title="Tukey IQR Outlier Rate per Numeric Field (%)",
            source_note=GOV_SOURCE_NOTE,
            height=360,
        )

    sub["display_label"] = sub["metric_name"].map(lambda k: NUMERIC_FIELD_LABELS.get(str(k), str(k)))
    sub = sub.sort_values("metric_value", ascending=False)

    fig = go.Figure()
    for status_key in ("WARN", "PASS"):
        part = sub[sub["compliance_status"] == status_key]
        if part.empty:
            continue
        color = STATUS_COLORS.get(status_key, STATUS_COLORS["PASS"])
        fig.add_trace(
            go.Bar(
                x=part["display_label"],
                y=part["metric_value"],
                name=STATUS_BADGE_TEXT.get(status_key, status_key),
                marker_color=color,
                text=[f"{v:.2f}%<br>({int(round(c)):,d})" for v, c in zip(part["metric_value"], part["secondary_value"])],
                textposition="outside",
                cliponaxis=False,
                customdata=list(
                    zip(
                        part["secondary_value"],
                        part["min_value"],
                        part["median_value"],
                        part["max_value"],
                        part["threshold_value"],
                    )
                ),
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "Outlier Share: %{y:.2f}% (%{customdata[0]:,.0f} events)<br>"
                    "Quartiles: Q1=%{customdata[1]:,.3f} | Median=%{customdata[2]:,.3f} | Q3=%{customdata[3]:,.3f}<br>"
                    "SLA Threshold: ≤ %{customdata[4]:.1f}%<extra></extra>"
                ),
            )
        )

    fig.add_hline(
        y=15.0,
        line_dash="dash",
        line_color=STATUS_COLORS["WARN"],
        annotation_text="IQR Outlier SLA (15%)",
        annotation_position="top right",
    )
    max_y = max(float(sub["metric_value"].max() or 0.0) * 1.32, 20.0)
    fig.update_yaxes(range=[0, max_y])

    return style_figure(
        fig,
        title="Tukey IQR Outlier Rate (%) — Deep Subduction & Linear Energy Skewness",
        x_title="Evaluated Numeric Attribute",
        y_title="Outlier Share (%)",
        source_note=GOV_SOURCE_NOTE,
        height=360,
    )


def build_outlier_quartile_spread_fig(df: pd.DataFrame) -> go.Figure:
    """
    Visualizes Q1, Median, Q3, and Upper Tukey Fence (Q3 + 1.5*IQR) per numeric field.
    Note on scale: `energy_joules` spans >10 orders of magnitude in linear Joules,
    so its quartiles are projected onto `log10(J)` alongside `magnitude`, `log10_energy`,
    and `depth_km` to preserve visual legibility without masking extreme skewness.
    """
    if df is None or df.empty or "kpi_category" not in df.columns:
        return empty_figure(
            "No quartile spread metrics available.",
            title="Interquartile Spread & Upper Tukey Fence",
            source_note=GOV_SOURCE_NOTE,
            height=360,
        )

    sub = df[df["kpi_category"] == "outlier_rate_per_numeric_field"].copy()
    if sub.empty:
        return empty_figure(
            "No outlier_rate_per_numeric_field rows present.",
            title="Interquartile Spread & Upper Tukey Fence",
            source_note=GOV_SOURCE_NOTE,
            height=360,
        )

    labels: List[str] = []
    q1_vals: List[float] = []
    med_vals: List[float] = []
    q3_vals: List[float] = []
    fence_vals: List[float] = []
    hover_notes: List[str] = []

    for _, row in sub.iterrows():
        m_name = str(row["metric_name"])
        raw_q1 = float(row["min_value"] or 0.0)
        raw_med = float(row["median_value"] or 0.0)
        raw_q3 = float(row["max_value"] or 0.0)
        raw_fence = raw_q3 + 1.5 * (raw_q3 - raw_q1)

        if m_name == "energy_joules":
            lbl = "Linear Energy (projected as log10 J)"
            q1 = math.log10(max(raw_q1, 1.0))
            med = math.log10(max(raw_med, 1.0))
            q3 = math.log10(max(raw_q3, 1.0))
            fence = math.log10(max(raw_fence, 1.0))
            note = f"Raw J: Q1={raw_q1:.2e}, Med={raw_med:.2e}, Q3={raw_q3:.2e}"
        else:
            lbl = NUMERIC_FIELD_LABELS.get(m_name, m_name)
            q1, med, q3, fence = raw_q1, raw_med, raw_q3, raw_fence
            note = f"Q1={raw_q1:.2f}, Med={raw_med:.2f}, Q3={raw_q3:.2f}, Upper Fence={raw_fence:.2f}"

        labels.append(lbl)
        q1_vals.append(round(q1, 3))
        med_vals.append(round(med, 3))
        q3_vals.append(round(q3, 3))
        fence_vals.append(round(fence, 3))
        hover_notes.append(note)

    fig = go.Figure()
    for lbl, q1, q3, note in zip(labels, q1_vals, q3_vals, hover_notes):
        fig.add_trace(
            go.Scatter(
                x=[q1, q3],
                y=[lbl, lbl],
                mode="lines",
                line={"color": "#3b6998", "width": 10},
                showlegend=False,
                hoverinfo="skip",
            )
        )

    fig.add_trace(
        go.Scatter(
            x=q1_vals,
            y=labels,
            mode="markers",
            name="Q1 (25th pct)",
            marker={"color": "#1f77b4", "size": 10, "symbol": "line-ns-open", "line": {"width": 3}},
            customdata=hover_notes,
            hovertemplate="<b>%{y}</b><br>Q1: %{x:.2f}<br>%{customdata}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=med_vals,
            y=labels,
            mode="markers",
            name="Median (Q2)",
            marker={"color": "#1b9e77", "size": 11, "symbol": "circle"},
            customdata=hover_notes,
            hovertemplate="<b>%{y}</b><br>Median: %{x:.2f}<br>%{customdata}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=q3_vals,
            y=labels,
            mode="markers",
            name="Q3 (75th pct)",
            marker={"color": "#3b6998", "size": 10, "symbol": "diamond"},
            customdata=hover_notes,
            hovertemplate="<b>%{y}</b><br>Q3: %{x:.2f}<br>%{customdata}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=fence_vals,
            y=labels,
            mode="markers",
            name="Upper Fence (Q3+1.5·IQR)",
            marker={"color": "#d95f02", "size": 10, "symbol": "triangle-up"},
            customdata=hover_notes,
            hovertemplate="<b>%{y}</b><br>Upper Tukey Fence: %{x:.2f}<br>%{customdata}<extra></extra>",
        )
    )

    return style_figure(
        fig,
        title="Quartile Spread (Q1–Median–Q3) & Upper Tukey Fence (Energy Projected on log10 J)",
        x_title="Attribute Value (Mw / km / log10 J)",
        y_title="Numeric Field",
        source_note=GOV_SOURCE_NOTE,
        height=360,
    )


def build_text_length_fig(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar + min/max range whiskers of `place` descriptor length per agency."""
    if df is None or df.empty or "kpi_category" not in df.columns:
        return empty_figure(
            "No text-length statistics available.",
            title="Location Descriptor Length by Agency (characters)",
            source_note=GOV_SOURCE_NOTE,
            height=360,
        )

    sub = df[df["kpi_category"] == "text_length_statistics"].copy()
    if sub.empty:
        return empty_figure(
            "No text_length_statistics rows present.",
            title="Location Descriptor Length by Agency (characters)",
            source_note=GOV_SOURCE_NOTE,
            height=360,
        )

    sub["agency_label"] = sub["source_id"].map(lambda s: SOURCE_DISPLAY_NAMES.get(str(s), str(s)))
    sub = sub.sort_values("metric_value", ascending=True)

    colors = [SOURCE_COLORS.get(str(s), "#3b6998") for s in sub["source_id"]]
    err_plus = (sub["max_value"] - sub["metric_value"]).clip(lower=0.0)
    err_minus = (sub["metric_value"] - sub["min_value"]).clip(lower=0.0)

    fig = go.Figure(
        go.Bar(
            x=sub["metric_value"],
            y=sub["agency_label"],
            orientation="h",
            name="Mean Length (min–max whiskers)",
            marker_color=colors,
            error_x={
                "type": "data",
                "symmetric": False,
                "array": err_plus,
                "arrayminus": err_minus,
                "color": "#334155",
                "thickness": 1.6,
                "width": 5,
            },
            text=[f"{m:.1f} chars (med {med:.0f})" for m, med in zip(sub["metric_value"], sub["median_value"])],
            textposition="inside",
            insidetextanchor="middle",
            customdata=list(zip(sub["min_value"], sub["median_value"], sub["max_value"], sub["secondary_value"])),
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Mean Length: %{x:.2f} characters<br>"
                "Min / Median / Max: %{customdata[0]:.0f} / %{customdata[1]:.0f} / %{customdata[2]:.0f} chars<br>"
                "Evaluated Events: %{customdata[3]:,.0f}<extra></extra>"
            ),
        )
    )
    fig.add_vline(
        x=120.0,
        line_dash="dash",
        line_color=STATUS_COLORS["WARN"],
        annotation_text="Mean SLA Limit (120 chars)",
        annotation_position="top right",
    )
    fig.update_xaxes(range=[0, max(float(sub["max_value"].max() or 0.0) * 1.12, 135.0)])

    return style_figure(
        fig,
        title="Location String Length by Agency (Mean ± Min/Max Range vs. 120-Char SLA)",
        x_title="Descriptor Length (characters)",
        y_title="Seismic Source",
        source_note=GOV_SOURCE_NOTE,
        height=360,
        show_legend=False,
    )


def prepare_governance_table_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Prepares human-readable, sortable KPI rows for `dash_table.DataTable`.
    Orders executive summary, completeness, outlier, source-volume, and text-length SLAs
    ahead of granular daily series rows so page 1 immediately surfaces primary SLAs.
    """
    if df is None or df.empty:
        return []

    cat_priority = {
        "summary": 0,
        "null_rate_per_field": 1,
        "outlier_rate_per_numeric_field": 2,
        "volume_by_source": 3,
        "text_length_statistics": 4,
        "volume_over_time": 5,
    }
    work = df.copy()
    work["_prio"] = work["kpi_category"].map(lambda c: cat_priority.get(str(c), 99))
    work["_status_prio"] = work["compliance_status"].map({"UNAVAILABLE": 0, "WARN": 1, "PASS": 2}).fillna(3)
    work = work.sort_values(["_prio", "_status_prio", "metric_name", "period_date"])

    records: List[Dict[str, Any]] = []
    for _, row in work.iterrows():
        status_raw = str(row["compliance_status"])
        m_val = row["metric_value"]
        s_val = row["secondary_value"]
        t_val = row["threshold_value"]
        records.append(
            {
                "Category": str(row["kpi_category"]),
                "KPI Metric": str(row["metric_name"]),
                "Source": str(row["source_id"]),
                "Period": str(row["period_date"]),
                "Current Value": "N/A" if pd.isna(m_val) else f"{float(m_val):,.2f}",
                "Secondary": "N/A" if pd.isna(s_val) else f"{float(s_val):,.2f}",
                "Unit": str(row["unit"]),
                "SLA Threshold": "N/A" if pd.isna(t_val) else f"{float(t_val):,.2f}",
                "Status": STATUS_BADGE_TEXT.get(status_raw, status_raw),
                "raw_status": status_raw,
            }
        )
    return records
