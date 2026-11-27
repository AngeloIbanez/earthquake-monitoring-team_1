"""
Pure figure-builder functions for the Seismic Hazard Storytelling Dashboard
(`dashboard/figures_storytelling.py`).

Audience: Dr. Elena Vance, Chief Seismic Risk Officer.
Every builder accepts a `pandas.DataFrame` loaded from the curated Storytelling
summary table and returns a `plotly.graph_objects.Figure` with zero engineering jargon,
accessible color+symbol+text encodings, explicit physical units, and graceful empty states.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .theme import (
    ALERT_COLORS,
    ALERT_LABELS_WITH_ICON,
    ALERT_MARKER_SYMBOLS,
    DEPTH_COLORSCALE,
    DEPTH_REGIME_COLORS,
    DEPTH_REGIME_ORDER,
    FONT_FAMILY,
    NEUTRAL_DARK,
    NEUTRAL_MUTED,
    SAAI_THRESHOLDS,
    SEVERITY_ORDER,
    SOURCE_DISPLAY_NAMES,
    STORY_SOURCE_NOTE,
    empty_figure,
    style_figure,
)

SEVERITY_CLASS_COLORS: Dict[str, str] = {
    "Micro/Minor (<3.0)": "#94a3b8",
    "Light (3.0-4.4)": "#3b6998",
    "Moderate (4.5-5.4)": "#1b9e77",
    "Strong (5.5-6.4)": "#d95f02",
    "Major/Great (>=6.5)": "#7b0d1e",
}

COHORT_LABELS: Dict[str, str] = {
    "historical_baseline": "Archival Baseline (38d)",
    "incremental_active_window": "Active Rolling Feeds (12d)",
    "current_7d": "Recent 7-Day Window",
    "current_24h": "Latest 24-Hour Window",
}

COHORT_ORDER = [
    "historical_baseline",
    "incremental_active_window",
    "current_7d",
    "current_24h",
]


def get_temporal_date_bounds(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    """Returns `(min_date, max_date)` ISO strings from `temporal_trend_saai`."""
    if df is None or df.empty or "aggregation_type" not in df.columns:
        return None, None
    ts = df[df["aggregation_type"] == "temporal_trend_saai"]
    if ts.empty:
        return None, None
    dates = sorted(str(d) for d in ts["period_date"].dropna().unique())
    if not dates:
        return None, None
    return dates[0], dates[-1]


# ---------------------------------------------------------------------------
# Panel A (Left): Magnitude Distribution — % of Events vs. % of Radiated Energy
# ---------------------------------------------------------------------------
def build_severity_energy_fig(df: pd.DataFrame) -> go.Figure:
    """
    Grouped bar chart contrasting earthquake frequency share (%) against radiated
    seismic energy share (%) across the 5 Gutenberg-Richter magnitude classes.
    """
    title = "Volume vs. Energy Paradox Across Magnitude Classes"
    if df is None or df.empty or "aggregation_type" not in df.columns:
        return empty_figure(
            "Magnitude distribution summaries are unavailable.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=380,
        )

    sub = df[df["aggregation_type"] == "severity_distribution"].copy()
    if sub.empty:
        return empty_figure(
            "No magnitude class distribution records available.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=380,
        )

    order_map = {k: idx for idx, k in enumerate(SEVERITY_ORDER)}
    sub["_ord"] = sub["dimension_key"].map(lambda k: order_map.get(str(k), 99))
    sub = sub.sort_values("_ord")

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=sub["dimension_key"],
            y=sub["percentage"],
            name="Share of Recorded Earthquakes (%)",
            marker_color="#3b6998",
            text=[f"{v:.1f}%" for v in sub["percentage"]],
            textposition="outside",
            cliponaxis=False,
            customdata=list(zip(sub["event_count"], sub["mean_magnitude"], sub["max_magnitude"])),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Frequency Share: %{y:.2f}% (%{customdata[0]:,.0f} earthquakes)<br>"
                "Mean / Peak Magnitude: M %{customdata[1]:.2f} / M %{customdata[2]:.1f}<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            x=sub["dimension_key"],
            y=sub["energy_share_pct"],
            name="Share of Radiated Seismic Energy (%)",
            marker_color="#d95f02",
            text=[f"{v:.1f}%" for v in sub["energy_share_pct"]],
            textposition="outside",
            cliponaxis=False,
            customdata=list(zip(sub["event_count"], sub["log10_energy"])),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Radiated Energy Share: %{y:.2f}%<br>"
                "Mean Class Energy: 10^%{customdata[1]:.2f} J (%{customdata[0]:,.0f} earthquakes)<extra></extra>"
            ),
        )
    )

    max_y = max(
        float(sub["percentage"].max() or 0.0),
        float(sub["energy_share_pct"].max() or 0.0),
    )
    fig.update_layout(barmode="group")
    fig.update_yaxes(range=[0, max(max_y * 1.24, 65.0)])

    return style_figure(
        fig,
        title=title,
        x_title="Gutenberg-Richter Magnitude Class (Mw eq.)",
        y_title="Share of Monitored Catalog (%)",
        source_note=STORY_SOURCE_NOTE,
        height=390,
    )


# ---------------------------------------------------------------------------
# Panel A (Right): 3-Class Hypocentral Depth Attenuation Breakdown
# ---------------------------------------------------------------------------
def build_depth_attenuation_fig(df: pd.DataFrame) -> go.Figure:
    """
    3-segment focal depth regime donut chart (Shallow 0–70 km, Intermediate 70–300 km,
    Deep >300 km) illustrating surface shaking exposure vs. mantle attenuation.
    """
    title = "Focal Depth Regimes & Crustal Exposure (km)"
    if df is None or df.empty or "aggregation_type" not in df.columns:
        return empty_figure(
            "Focal depth regime summaries are unavailable.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=390,
        )

    sub = df[df["aggregation_type"] == "depth_regime_breakdown"].copy()
    if sub.empty:
        return empty_figure(
            "No focal depth regime records available.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=390,
        )

    order_map = {k: idx for idx, k in enumerate(DEPTH_REGIME_ORDER)}
    sub["_ord"] = sub["dimension_key"].map(lambda k: order_map.get(str(k), 99))
    sub = sub.sort_values("_ord")

    colors = [DEPTH_REGIME_COLORS.get(str(k), "#3b6998") for k in sub["dimension_key"]]
    total_events = int(round(float(sub["event_count"].sum())))
    shallow_row = sub[sub["dimension_key"] == "Shallow (0-70 km)"]
    shallow_pct = float(shallow_row["percentage"].iloc[0]) if not shallow_row.empty else 0.0

    fig = go.Figure(
        go.Pie(
            labels=sub["dimension_key"],
            values=sub["event_count"],
            hole=0.54,
            sort=False,
            direction="clockwise",
            marker={"colors": colors, "line": {"color": "#ffffff", "width": 2}},
            textinfo="percent",
            textposition="inside",
            insidetextorientation="horizontal",
            customdata=list(
                zip(
                    sub["percentage"],
                    sub["energy_share_pct"],
                    sub["mean_depth_km"],
                    sub["max_magnitude"],
                )
            ),
            hovertemplate=(
                "<b>%{label}</b><br>"
                "Earthquakes: %{value:,.0f} (%{customdata[0]:.2f}%)<br>"
                "Radiated Energy Share: %{customdata[1]:.2f}%<br>"
                "Mean Depth: %{customdata[2]:.1f} km · Peak M %{customdata[3]:.1f}<extra></extra>"
            ),
        )
    )

    fig.add_annotation(
        text=f"<b>{shallow_pct:.1f}%</b><br>Shallow<br>({total_events:,d} total)",
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font={"size": 13, "color": NEUTRAL_DARK, "family": FONT_FAMILY},
    )

    fig = style_figure(
        fig,
        title=title,
        source_note=STORY_SOURCE_NOTE,
        height=390,
        show_legend=True,
    )
    fig.update_layout(
        legend={"orientation": "h", "yanchor": "bottom", "y": -0.11, "xanchor": "center", "x": 0.5},
        margin={"l": 28, "r": 28, "t": 54, "b": 82},
    )
    return fig


# ---------------------------------------------------------------------------
# Panel B: Daily SAAI Anomaly Score, Policy Thresholds & Radiated Energy Trend
# ---------------------------------------------------------------------------
def build_saai_trend_fig(
    df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    alert_tiers: Optional[Sequence[str]] = None,
) -> go.Figure:
    """
    Dual-axis temporal chart displaying daily Seismic Activity Anomaly Index (SAAI)
    against policy alert thresholds (1.0, 2.0, 3.5) and daily log10 radiated energy.
    """
    title = "Daily Seismic Anomaly Index (SAAI) & Radiated Energy Trajectory"
    if df is None or df.empty or "aggregation_type" not in df.columns:
        return empty_figure(
            "Daily anomaly observations are unavailable.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=410,
        )

    ts = df[df["aggregation_type"] == "temporal_trend_saai"].copy()
    if start_date:
        ts = ts[ts["period_date"] >= str(start_date)]
    if end_date:
        ts = ts[ts["period_date"] <= str(end_date)]
    ts = ts.sort_values("period_date")

    if ts.empty:
        return empty_figure(
            "No daily anomaly observations fall within the selected date range.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=410,
        )

    if alert_tiers is not None:
        if len(alert_tiers) == 0:
            return empty_figure(
                "Select at least one alert posture level in the filter bar above.",
                title=title,
                source_note=STORY_SOURCE_NOTE,
                height=410,
            )
        ts_markers = ts[ts["alert_tier"].isin(list(alert_tiers))].copy()
    else:
        ts_markers = ts.copy()

    if ts_markers.empty:
        return empty_figure(
            "Zero days in the selected date window match the chosen alert posture levels.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=410,
        )

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Baseline continuous trajectory for temporal context
    fig.add_trace(
        go.Scatter(
            x=ts["period_date"],
            y=ts["saai_score"],
            mode="lines",
            name="SAAI Trajectory",
            line={"color": "#475569", "width": 2.0},
            hoverinfo="skip",
            showlegend=False,
        ),
        secondary_y=False,
    )

    # Alert-posture markers with redundant color + symbol + text label
    for tier in ("NORMAL", "ELEVATED", "HIGH", "CRITICAL"):
        part = ts_markers[ts_markers["alert_tier"] == tier]
        if part.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=part["period_date"],
                y=part["saai_score"],
                mode="markers",
                name=ALERT_LABELS_WITH_ICON.get(tier, tier),
                marker={
                    "color": ALERT_COLORS.get(tier, "#3b6998"),
                    "symbol": ALERT_MARKER_SYMBOLS.get(tier, "circle"),
                    "size": 10 if tier == "NORMAL" else 13,
                    "line": {"color": "#ffffff", "width": 1.2},
                },
                customdata=list(
                    zip(
                        part["event_count"],
                        part["max_magnitude"],
                        part["log10_energy"],
                        part["alert_tier"],
                    )
                ),
                hovertemplate=(
                    "<b>Date (UTC): %{x}</b><br>"
                    "SAAI Score: %{y:+.2f} (%{customdata[3]})<br>"
                    "Daily Earthquakes: %{customdata[0]:,.0f} · Peak M %{customdata[1]:.1f}<br>"
                    "Daily Energy: 10^%{customdata[2]:.2f} J<extra></extra>"
                ),
            ),
            secondary_y=False,
        )

    # Secondary axis: Daily log10 radiated seismic energy
    fig.add_trace(
        go.Scatter(
            x=ts["period_date"],
            y=ts["log10_energy"],
            mode="lines",
            name="Daily Radiated Energy (log10 J)",
            line={"color": "#3b6998", "width": 1.8, "dash": "dot"},
            opacity=0.82,
            hovertemplate=(
                "<b>Date (UTC): %{x}</b><br>"
                "Daily Radiated Energy: %{y:.2f} log10(J)<extra></extra>"
            ),
        ),
        secondary_y=True,
    )

    # Policy Thresholds (1.0 Elevated, 2.0 High, 3.5 Critical)
    threshold_specs = [
        ("ELEVATED", SAAI_THRESHOLDS["ELEVATED"], "▲ Elevated (1.0)"),
        ("HIGH", SAAI_THRESHOLDS["HIGH"], "■ High (2.0)"),
        ("CRITICAL", SAAI_THRESHOLDS["CRITICAL"], "◆ Critical (3.5)"),
    ]
    for tier_key, y_val, label_txt in threshold_specs:
        fig.add_hline(
            y=y_val,
            line_dash="dash",
            line_width=1.3,
            line_color=ALERT_COLORS[tier_key],
            annotation_text=label_txt,
            annotation_position="top left",
            annotation_font={"size": 11, "color": ALERT_COLORS[tier_key]},
            secondary_y=False,
        )

    min_saai = min(float(ts["saai_score"].min() or 0.0), -1.0)
    max_saai = max(float(ts["saai_score"].max() or 0.0) + 0.5, 3.85)
    fig.update_yaxes(
        title_text="Daily SAAI Score (standardized index)",
        range=[min_saai - 0.25, max_saai],
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="Daily Radiated Energy (log10 J)",
        showgrid=False,
        secondary_y=True,
    )

    fig = style_figure(
        fig,
        title=title,
        x_title="Observation Date (UTC)",
        source_note=STORY_SOURCE_NOTE,
        height=410,
        hovermode="x unified",
    )
    fig.update_layout(margin={"l": 60, "r": 64, "t": 56, "b": 70})
    return fig


# ---------------------------------------------------------------------------
# Panel C: Daily Seismic Volume & Swarm Peak Activity
# ---------------------------------------------------------------------------
def build_daily_volume_swarms_fig(
    df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    alert_tiers: Optional[Sequence[str]] = None,
) -> go.Figure:
    """
    Daily earthquake count bar chart colored by alert posture with callout annotations
    on anomalous days (SAAI >= 1.0) and volumetric swarm peaks.
    """
    title = "Daily Earthquake Count & Swarm Peak Activity"
    if df is None or df.empty or "aggregation_type" not in df.columns:
        return empty_figure(
            "Daily earthquake counts are unavailable.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=410,
        )

    ts = df[df["aggregation_type"] == "temporal_trend_saai"].copy()
    if start_date:
        ts = ts[ts["period_date"] >= str(start_date)]
    if end_date:
        ts = ts[ts["period_date"] <= str(end_date)]
    if alert_tiers is not None:
        if len(alert_tiers) == 0:
            return empty_figure(
                "Select at least one alert posture level in the filter bar above.",
                title=title,
                source_note=STORY_SOURCE_NOTE,
                height=410,
            )
        ts = ts[ts["alert_tier"].isin(list(alert_tiers))]

    ts = ts.sort_values("period_date")
    if ts.empty:
        return empty_figure(
            "No daily earthquake counts match the selected date and alert filters.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=410,
        )

    fig = go.Figure()
    for tier in ("NORMAL", "ELEVATED", "HIGH", "CRITICAL"):
        part = ts[ts["alert_tier"] == tier]
        if part.empty:
            continue
        fig.add_trace(
            go.Bar(
                x=part["period_date"],
                y=part["event_count"],
                name=ALERT_LABELS_WITH_ICON.get(tier, tier),
                marker_color=ALERT_COLORS.get(tier, "#3b6998"),
                customdata=list(
                    zip(part["max_magnitude"], part["saai_score"], part["mean_depth_km"])
                ),
                hovertemplate=(
                    "<b>Date (UTC): %{x}</b><br>"
                    "Recorded Earthquakes: %{y:,.0f} events/day<br>"
                    "Peak Magnitude: M %{customdata[0]:.1f} · SAAI: %{customdata[1]:+.2f}<br>"
                    "Mean Depth: %{customdata[2]:.1f} km<extra></extra>"
                ),
            )
        )

    # Annotate anomalous days (SAAI >= 1.0) plus peak swarm day
    highlight_dates = set(ts[ts["saai_score"] >= 1.0]["period_date"].astype(str).tolist())
    peak_vol_row = ts.sort_values("event_count", ascending=False).iloc[0]
    highlight_dates.add(str(peak_vol_row["period_date"]))

    annotated = ts[ts["period_date"].astype(str).isin(highlight_dates)].sort_values(
        "event_count", ascending=False
    )
    for idx, (_, arow) in enumerate(annotated.head(3).iterrows()):
        ay_offset = -28 - (idx % 2) * 18
        fig.add_annotation(
            x=arow["period_date"],
            y=float(arow["event_count"]),
            text=(
                f"<b>{arow['period_date']}</b>: {int(arow['event_count']):,d} eq "
                f"(Mmax {float(arow['max_magnitude']):.1f})"
            ),
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowcolor="#334155",
            ax=0,
            ay=ay_offset,
            bgcolor="rgba(255,255,255,0.92)",
            bordercolor="#cbd5e1",
            borderwidth=1,
            font={"size": 11, "color": NEUTRAL_DARK, "family": FONT_FAMILY},
        )

    max_vol = float(ts["event_count"].max() or 0.0)
    fig.update_layout(barmode="stack")
    fig.update_xaxes(categoryorder="array", categoryarray=ts["period_date"].tolist())
    fig.update_yaxes(range=[0, max(max_vol * 1.28, 25.0)])

    return style_figure(
        fig,
        title=title,
        x_title="Observation Date (UTC)",
        y_title="Recorded Earthquakes (events/day)",
        source_note=STORY_SOURCE_NOTE,
        height=410,
    )


# ---------------------------------------------------------------------------
# Panel D: Top 15 Active Tectonic Regions
# ---------------------------------------------------------------------------
def build_top_regions_fig(
    df: pd.DataFrame,
    alert_tiers: Optional[Sequence[str]] = None,
) -> go.Figure:
    """
    Horizontal ranked bar chart of the Top 15 most active tectonic localities,
    colored and badged by regional alert posture (`alert_tier`).
    """
    title = "Top 15 Active Tectonic Regions (Full Observation Period)"
    if df is None or df.empty or "aggregation_type" not in df.columns:
        return empty_figure(
            "Regional seismicity rankings are unavailable.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=430,
        )

    reg = df[df["aggregation_type"] == "top_regions_keywords"].copy()
    if alert_tiers is not None:
        if len(alert_tiers) == 0:
            return empty_figure(
                "Select at least one alert posture level to display active regions.",
                title=title,
                source_note=STORY_SOURCE_NOTE,
                height=430,
            )
        reg = reg[reg["alert_tier"].isin(list(alert_tiers))]

    if reg.empty:
        return empty_figure(
            "No top-15 tectonic regions match the selected alert posture levels.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=430,
        )

    reg = reg.sort_values(["event_count", "max_magnitude"], ascending=[True, True])
    ordered_regions = reg["dimension_key"].tolist()

    fig = go.Figure()
    for tier in ("NORMAL", "ELEVATED", "HIGH", "CRITICAL"):
        part = reg[reg["alert_tier"] == tier]
        if part.empty:
            continue
        fig.add_trace(
            go.Bar(
                x=part["event_count"],
                y=part["dimension_key"],
                orientation="h",
                name=ALERT_LABELS_WITH_ICON.get(tier, tier),
                marker_color=ALERT_COLORS.get(tier, "#3b6998"),
                text=[
                    f"{int(cnt):,d} (Mmax {float(mm):.1f})"
                    for cnt, mm in zip(part["event_count"], part["max_magnitude"])
                ],
                textposition="outside",
                cliponaxis=False,
                customdata=list(
                    zip(
                        part["max_magnitude"],
                        part["mean_magnitude"],
                        part["mean_depth_km"],
                        part["alert_tier"],
                    )
                ),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Recorded Earthquakes: %{x:,.0f}<br>"
                    "Peak / Mean Magnitude: M %{customdata[0]:.1f} / M %{customdata[1]:.2f}<br>"
                    "Mean Focal Depth: %{customdata[2]:.1f} km · Posture: %{customdata[3]}<extra></extra>"
                ),
            )
        )

    max_x = float(reg["event_count"].max() or 0.0)
    fig.update_xaxes(range=[0, max(max_x * 1.30, 20.0)])
    fig.update_yaxes(categoryorder="array", categoryarray=ordered_regions)

    fig = style_figure(
        fig,
        title=title,
        x_title="Recorded Earthquakes (events)",
        y_title="Tectonic Region / Locality",
        source_note=STORY_SOURCE_NOTE,
        height=430,
    )
    fig.update_layout(margin={"l": 210, "r": 36, "t": 54, "b": 68})
    return fig


# ---------------------------------------------------------------------------
# Panel E: Multi-Network Coverage OR Historical vs. Current Cohort Comparison
# ---------------------------------------------------------------------------
def build_comparison_panel_fig(
    df: pd.DataFrame,
    mode: str = "agency",
    selected_agencies: Optional[Sequence[str]] = None,
) -> go.Figure:
    """
    Single toggleable comparison panel:
    - `mode == "agency"`: Magnitude profile across the 4 seismic monitoring networks
      with detection-threshold caveats.
    - `mode == "historical_vs_current"`: Daily earthquake rate (events/day) and mean
      magnitude across Archival Baseline vs. Rolling Real-Time Windows (12d, 7d, 24h).
    """
    is_cohort = str(mode).lower() == "historical_vs_current"
    if df is None or df.empty or "aggregation_type" not in df.columns:
        return empty_figure(
            "Comparative network and temporal cohort summaries are unavailable.",
            title="Monitoring Network & Temporal Cohort Comparison",
            source_note=STORY_SOURCE_NOTE,
            height=430,
        )

    if not is_cohort:
        title = "Earthquake Volume & Magnitude Profile by Monitoring Network"
        sub = df[df["aggregation_type"] == "source_comparison"].copy()
        if selected_agencies is not None:
            if len(selected_agencies) == 0:
                return empty_figure(
                    "Select at least one monitoring network in the filter bar above.",
                    title=title,
                    source_note=STORY_SOURCE_NOTE,
                    height=430,
                )
            sub = sub[sub["dimension_key"].isin(list(selected_agencies))]

        if sub.empty:
            return empty_figure(
                "No monitoring network records match the active network filter.",
                title=title,
                source_note=STORY_SOURCE_NOTE,
                height=430,
            )

        sub["network_label"] = sub["dimension_key"].map(
            lambda s: SOURCE_DISPLAY_NAMES.get(str(s), str(s))
        )
        totals = (
            sub.groupby("network_label", as_index=False)["event_count"]
            .sum()
            .sort_values("event_count", ascending=True)
        )
        ordered_nets = totals["network_label"].tolist()

        fig = go.Figure()
        for sev_class in SEVERITY_ORDER:
            part = sub[sub["secondary_dimension"] == sev_class]
            if part.empty:
                continue
            fig.add_trace(
                go.Bar(
                    x=part["event_count"],
                    y=part["network_label"],
                    orientation="h",
                    name=sev_class,
                    marker_color=SEVERITY_CLASS_COLORS.get(sev_class, "#3b6998"),
                    customdata=list(
                        zip(part["percentage"], part["mean_magnitude"], part["max_magnitude"])
                    ),
                    hovertemplate=(
                        "<b>%{y}</b> — " + sev_class + "<br>"
                        "Earthquakes: %{x:,.0f} (%{customdata[0]:.2f}% of catalog)<br>"
                        "Mean / Peak Magnitude: M %{customdata[1]:.2f} / M %{customdata[2]:.1f}<extra></extra>"
                    ),
                )
            )

        fig.update_layout(barmode="stack")
        fig.update_yaxes(categoryorder="array", categoryarray=ordered_nets)
        fig.add_annotation(
            text=(
                "Coverage note: USGS Real-Time captures U.S. micro-tremors (<3.0); "
                "USGS/EMSC Archives focus on regional catalogs; GDACS reports humanitarian alerts only."
            ),
            xref="paper",
            yref="paper",
            x=0.0,
            y=1.08,
            showarrow=False,
            xanchor="left",
            font={"size": 11, "color": NEUTRAL_MUTED, "family": FONT_FAMILY},
        )
        fig = style_figure(
            fig,
            title=title,
            x_title="Recorded Earthquakes (events)",
            y_title="Monitoring Network",
            source_note=STORY_SOURCE_NOTE,
            height=430,
        )
        fig.update_layout(margin={"l": 195, "r": 28, "t": 68, "b": 68})
        return fig

    # Mode 2: Historical Baseline vs. Rolling Real-Time Windows
    title = "Historical Archive Baseline vs. Rolling Real-Time Windows"
    coh = df[df["aggregation_type"] == "historical_vs_current"].copy()
    if coh.empty:
        return empty_figure(
            "Historical vs. current cohort summaries are unavailable.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=430,
        )

    coh["_ord"] = coh["dimension_key"].map(
        lambda k: COHORT_ORDER.index(str(k)) if str(k) in COHORT_ORDER else 99
    )
    coh = coh.sort_values("_ord")
    coh["cohort_label"] = coh["dimension_key"].map(lambda k: COHORT_LABELS.get(str(k), str(k)))

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=coh["cohort_label"],
            y=coh["daily_rate"],
            name="Daily Earthquake Rate (events/day)",
            marker_color="#3b6998",
            text=[f"{v:.1f}/day" for v in coh["daily_rate"]],
            textposition="outside",
            cliponaxis=False,
            customdata=list(
                zip(
                    coh["event_count"],
                    coh["max_magnitude"],
                    coh["median_depth_km"],
                    coh["m55_plus_share_pct"],
                    coh["narrative_note"],
                )
            ),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Daily Rate: %{y:.2f} events/day (%{customdata[0]:,.0f} total events)<br>"
                "Peak Magnitude: M %{customdata[1]:.1f} · M≥5.5 Share: %{customdata[3]:.2f}%<br>"
                "Median Depth: %{customdata[2]:.1f} km<br>"
                "<i>%{customdata[4]}</i><extra></extra>"
            ),
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=coh["cohort_label"],
            y=coh["mean_magnitude"],
            mode="lines+markers+text",
            name="Mean Magnitude (Mw)",
            line={"color": "#d95f02", "width": 2.6},
            marker={"color": "#d95f02", "size": 11, "symbol": "diamond"},
            text=[f"M {v:.2f}" for v in coh["mean_magnitude"]],
            textposition="top center",
            hovertemplate="<b>%{x}</b><br>Mean Magnitude: M %{y:.2f}<extra></extra>",
        ),
        secondary_y=True,
    )

    max_rate = float(coh["daily_rate"].max() or 0.0)
    fig.update_yaxes(
        title_text="Daily Observation Rate (events/day)",
        range=[0, max(max_rate * 1.28, 25.0)],
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="Mean Magnitude (Mw eq.)",
        range=[0, max(float(coh["mean_magnitude"].max() or 0.0) * 1.28, 6.2)],
        showgrid=False,
        secondary_y=True,
    )
    fig.add_annotation(
        text=(
            "Comparability note: Active rolling feeds include local micro-seismicity, "
            "raising daily event rates while lowering mean magnitude vs. archival baselines."
        ),
        xref="paper",
        yref="paper",
        x=0.0,
        y=1.08,
        showarrow=False,
        xanchor="left",
        font={"size": 11, "color": NEUTRAL_MUTED, "family": FONT_FAMILY},
    )

    fig = style_figure(
        fig,
        title=title,
        x_title="Observation Window Cohort",
        source_note=STORY_SOURCE_NOTE,
        height=430,
    )
    fig.update_layout(margin={"l": 60, "r": 64, "t": 68, "b": 68})
    return fig


# ---------------------------------------------------------------------------
# Panel F: Global Interactive Significant Epicenter & Hypocentral Depth Map
# ---------------------------------------------------------------------------
def build_epicenter_map_fig(
    df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    alert_tiers: Optional[Sequence[str]] = None,
    selected_agencies: Optional[Sequence[str]] = None,
) -> go.Figure:
    """
    Global Natural Earth geospatial scatter map of recent significant earthquakes
    (M >= 4.5 or tsunami evaluation issued). Marker size encodes magnitude (Mw);
    sequential Viridis_r color encodes focal depth (km); crimson rings highlight
    USGS coastal tsunami evaluation bulletins.
    """
    title = "Global Significant Earthquake Epicenters — Magnitude (Size) & Focal Depth (Color)"
    if df is None or df.empty or "aggregation_type" not in df.columns:
        return empty_figure(
            "Recent significant earthquake coordinates are unavailable.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=470,
        )

    ev = df[df["aggregation_type"] == "recent_significant_events"].copy()
    ev = ev.dropna(subset=["latitude", "longitude", "mean_magnitude", "mean_depth_km"])

    if start_date:
        ev = ev[ev["period_date"] >= str(start_date)]
    if end_date:
        ev = ev[ev["period_date"] <= str(end_date)]
    if alert_tiers is not None:
        if len(alert_tiers) == 0:
            return empty_figure(
                "Select at least one alert posture level to display significant epicenters.",
                title=title,
                source_note=STORY_SOURCE_NOTE,
                height=470,
            )
        ev = ev[ev["alert_tier"].isin(list(alert_tiers))]
    if selected_agencies is not None:
        if len(selected_agencies) == 0:
            return empty_figure(
                "Select at least one monitoring network to display significant epicenters.",
                title=title,
                source_note=STORY_SOURCE_NOTE,
                height=470,
            )
        ev = ev[ev["source_id"].isin(list(selected_agencies))]

    if ev.empty:
        return empty_figure(
            "No recent significant earthquakes match the active date, alert, and network filters.",
            title=title,
            source_note=STORY_SOURCE_NOTE,
            height=470,
        )

    ev = ev.sort_values("mean_magnitude", ascending=True)

    def _tsunami_text(val: object) -> str:
        if val is True or str(val).lower() == "true":
            return "USGS coastal evaluation issued (advisory check, not confirmed wave)"
        if val is False or str(val).lower() == "false":
            return "No tsunami evaluation issued (USGS)"
        return "Not reported by this monitoring network"

    def _depth_note(depth_val: float, pinned: object) -> str:
        is_pin = pinned is True or str(pinned).lower() == "true"
        return f"{depth_val:.1f} km (agency default 10 km)" if is_pin else f"{depth_val:.1f} km"

    marker_sizes: List[float] = [
        max(7.0, (float(m) - 3.8) * 6.0) for m in ev["mean_magnitude"]
    ]
    network_names = [SOURCE_DISPLAY_NAMES.get(str(s), str(s)) for s in ev["source_id"]]
    tsunami_labels = [_tsunami_text(v) for v in ev["tsunami_flag"]]
    depth_labels = [
        _depth_note(float(d), p) for d, p in zip(ev["mean_depth_km"], ev["is_pinned_depth"])
    ]

    max_depth = max(300.0, float(ev["mean_depth_km"].max() or 300.0))

    fig = go.Figure()
    fig.add_trace(
        go.Scattergeo(
            lat=ev["latitude"],
            lon=ev["longitude"],
            mode="markers",
            name="Significant Earthquake (M ≥ 4.5)",
            marker={
                "size": marker_sizes,
                "color": ev["mean_depth_km"],
                "colorscale": DEPTH_COLORSCALE,
                "cmin": 0.0,
                "cmax": max_depth,
                "opacity": 0.88,
                "line": {"color": "#0f172a", "width": 0.9},
                "colorbar": {
                    "title": {"text": "Depth (km)", "font": {"size": 12}},
                    "thickness": 14,
                    "len": 0.80,
                },
            },
            customdata=list(
                zip(
                    ev["secondary_dimension"],
                    ev["event_timestamp_utc"],
                    ev["mean_magnitude"],
                    depth_labels,
                    network_names,
                    ev["alert_tier"],
                    tsunami_labels,
                )
            ),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Origin Time (UTC): %{customdata[1]}<br>"
                "Magnitude: M %{customdata[2]:.1f} · Focal Depth: %{customdata[3]}<br>"
                "Coordinates: %{lat:.2f}°, %{lon:.2f}°<br>"
                "Monitoring Network: %{customdata[4]} · Day Posture: %{customdata[5]}<br>"
                "Tsunami Bulletin: %{customdata[6]}<extra></extra>"
            ),
        )
    )

    # Highlight USGS coastal tsunami evaluation events with a distinct crimson outer halo
    tsu_ev = ev[ev["tsunami_flag"].apply(lambda v: v is True or str(v).lower() == "true")]
    if not tsu_ev.empty:
        tsu_sizes = [max(14.0, (float(m) - 3.8) * 6.0 + 7.0) for m in tsu_ev["mean_magnitude"]]
        fig.add_trace(
            go.Scattergeo(
                lat=tsu_ev["latitude"],
                lon=tsu_ev["longitude"],
                mode="markers",
                name="USGS Coastal Tsunami Evaluation Issued",
                marker={
                    "size": tsu_sizes,
                    "symbol": "circle-open",
                    "color": "#c0392b",
                    "line": {"color": "#c0392b", "width": 2.6},
                },
                hoverinfo="skip",
            )
        )

    fig.update_geos(
        projection_type="natural earth",
        showland=True,
        landcolor="#f8fafc",
        showocean=True,
        oceancolor="#eef4fa",
        showcoastlines=True,
        coastlinecolor="#94a3b8",
        coastlinewidth=0.8,
        showcountries=True,
        countrycolor="#cbd5e1",
        countrywidth=0.5,
        lataxis_showgrid=True,
        lonaxis_showgrid=True,
         framecolor="#cbd5e1",
    )

    fig = style_figure(
        fig,
        title=f"{title} ({len(ev):,d} Displayed Events)",
        source_note=STORY_SOURCE_NOTE,
        height=470,
        show_legend=True,
    )
    fig.update_layout(
        legend={"orientation": "h", "yanchor": "bottom", "y": 0.99, "xanchor": "right", "x": 0.98},
        margin={"l": 16, "r": 24, "t": 56, "b": 62},
    )
    return fig
