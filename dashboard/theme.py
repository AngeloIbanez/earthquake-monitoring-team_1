"""
Unified visual theme, colorblind-safe palettes, policy constants, and figure
styling helpers for Team 1 Earthquake Activity Monitoring dashboards (`dashboard/theme.py`).
"""

from __future__ import annotations

from typing import Dict, Optional

import dash_bootstrap_components as dbc
import plotly.graph_objects as go

# Institutional Bootstrap stylesheet (logged in decisions_log.md D-11)
DBC_THEME = dbc.themes.FLATLY

FONT_FAMILY = "Inter, Segoe UI, system-ui, -apple-system, sans-serif"

# Source attribution footers (tailored per audience)
GOV_SOURCE_NOTE = "Source: Gold Layer — USGS, EMSC & GDACS Curated Telemetry"
STORY_SOURCE_NOTE = "Source: USGS, EMSC & GDACS Global Seismic Monitoring Networks"

# Policy thresholds documented in docs/hazard_definition.md (Section 4)
SAAI_THRESHOLDS: Dict[str, float] = {
    "ELEVATED": 1.0,
    "HIGH": 2.0,
    "CRITICAL": 3.5,
}

# Colorblind-safe semantic palette (ColorBrewer Dark2 / Okabe-Ito inspired; >= 4.5:1 text contrast)
ALERT_COLORS: Dict[str, str] = {
    "NORMAL": "#1b9e77",    # Deep teal-green
    "ELEVATED": "#d95f02",  # Burnt amber-orange
    "HIGH": "#c0392b",      # Vermilion crimson
    "CRITICAL": "#7b0d1e",  # Dark burgundy
    "INFO": "#3b6998",      # Slate steel blue
}

STATUS_COLORS: Dict[str, str] = {
    "PASS": "#1b9e77",
    "WARN": "#d95f02",
    "UNAVAILABLE": "#6c757d",
}

# Redundant symbol & text encodings for color-vision deficiency (CVD) accessibility
ALERT_MARKER_SYMBOLS: Dict[str, str] = {
    "NORMAL": "circle",
    "ELEVATED": "triangle-up",
    "HIGH": "square",
    "CRITICAL": "diamond",
    "INFO": "circle",
}

ALERT_LABELS_WITH_ICON: Dict[str, str] = {
    "NORMAL": "● Normal (< 1.0)",
    "ELEVATED": "▲ Elevated (1.0–1.99)",
    "HIGH": "■ High (2.0–3.49)",
    "CRITICAL": "◆ Critical (≥ 3.5)",
    "INFO": "● Informational",
}

STATUS_BADGE_TEXT: Dict[str, str] = {
    "PASS": "● PASS",
    "WARN": "▲ WARN",
    "UNAVAILABLE": "○ N/A (DB Offline)",
}

# Consistent colorblind-safe agency palette & human-friendly labels
SOURCE_COLORS: Dict[str, str] = {
    "usgs_fdsn": "#1f77b4",
    "emsc_fdsn": "#009e73",
    "usgs_realtime": "#e69f00",
    "gdacs_realtime": "#cc79a7",
}

SOURCE_DISPLAY_NAMES: Dict[str, str] = {
    "usgs_fdsn": "USGS Historical Archive",
    "emsc_fdsn": "EMSC Euro-Med Archive",
    "usgs_realtime": "USGS Real-Time Feed",
    "gdacs_realtime": "GDACS Global Disaster Alerts",
}

SEVERITY_ORDER = [
    "Micro/Minor (<3.0)",
    "Light (3.0-4.4)",
    "Moderate (4.5-5.4)",
    "Strong (5.5-6.4)",
    "Major/Great (>=6.5)",
]

DEPTH_REGIME_ORDER = [
    "Shallow (0-70 km)",
    "Intermediate (70-300 km)",
    "Deep (>300 km)",
]

DEPTH_REGIME_COLORS: Dict[str, str] = {
    "Shallow (0-70 km)": "#d95f02",        # High surface shaking & tsunami hazard
    "Intermediate (70-300 km)": "#3b6998", # Subduction zone moderate attenuation
    "Deep (>300 km)": "#1b9e77",           # Strong mantle attenuation
}

# Sequential perceptually uniform scale for hypocentral focal depth (km)
DEPTH_COLORSCALE = "Viridis_r"

NEUTRAL_DARK = "#1e293b"
NEUTRAL_MUTED = "#64748b"
GRID_COLOR = "#e2e8f0"

PLOTLY_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        font={"family": FONT_FAMILY, "size": 12, "color": NEUTRAL_DARK},
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        title={"font": {"family": FONT_FAMILY, "size": 15, "color": "#0f172a"}, "x": 0.01, "xanchor": "left"},
        xaxis={
            "gridcolor": GRID_COLOR,
            "linecolor": "#cbd5e1",
            "zerolinecolor": GRID_COLOR,
            "tickfont": {"size": 12},
            "title": {"font": {"size": 12}},
        },
        yaxis={
            "gridcolor": GRID_COLOR,
            "linecolor": "#cbd5e1",
            "zerolinecolor": GRID_COLOR,
            "tickfont": {"size": 12},
            "title": {"font": {"size": 12}},
        },
        legend={
            "bgcolor": "rgba(255,255,255,0.90)",
            "bordercolor": "#e2e8f0",
            "borderwidth": 1,
            "font": {"size": 12},
        },
    )
)


def style_figure(
    fig: go.Figure,
    title: str,
    x_title: Optional[str] = None,
    y_title: Optional[str] = None,
    source_note: str = GOV_SOURCE_NOTE,
    height: int = 380,
    show_legend: bool = True,
    hovermode: str = "closest",
    bottom_margin: int = 68,
) -> go.Figure:
    """
    Applies mandatory Workshop #4 visualization anatomy to a Plotly figure:
    - Descriptive title (>= 15px)
    - Labeled axes with physical units (>= 12px)
    - Accessible legend
    - Bottom-left source attribution annotation
    """
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        title={"text": f"<b>{title}</b>", "x": 0.01, "xanchor": "left"},
        height=height,
        showlegend=show_legend,
        hovermode=hovermode,
        margin={"l": 60, "r": 28, "t": 54, "b": bottom_margin},
    )
    if x_title is not None:
        fig.update_xaxes(title_text=x_title)
    if y_title is not None:
        fig.update_yaxes(title_text=y_title)

    existing_annotations = list(fig.layout.annotations) if fig.layout.annotations else []
    existing_annotations.append(
        go.layout.Annotation(
            text=source_note,
            xref="paper",
            yref="paper",
            x=0.0,
            y=-0.19 if bottom_margin >= 65 else -0.15,
            xanchor="left",
            yanchor="top",
            showarrow=False,
            font={"size": 11, "color": NEUTRAL_MUTED, "family": FONT_FAMILY},
        )
    )
    fig.update_layout(annotations=existing_annotations)
    return fig


def empty_figure(
    message: str = "No seismic records match the active filter selection.",
    title: str = "No Matching Data",
    source_note: str = GOV_SOURCE_NOTE,
    height: int = 340,
) -> go.Figure:
    """Builds a clean, informative placeholder figure when a filter yields 0 rows."""
    fig = go.Figure()
    fig.add_annotation(
        text=f"<b>{message}</b>",
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.52,
        showarrow=False,
        font={"size": 14, "color": NEUTRAL_MUTED, "family": FONT_FAMILY},
    )
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return style_figure(
        fig,
        title=title,
        source_note=source_note,
        height=height,
        show_legend=False,
    )
