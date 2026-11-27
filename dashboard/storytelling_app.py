#!/usr/bin/env python3
"""
Seismic Hazard & Anomaly Storytelling Dashboard (`dashboard/storytelling_app.py`).

Independent Plotly Dash web application running on `http://localhost:8051`
(configurable via `STORYTELLING_PORT`), tailored for Dr. Elena Vance
(Chief Seismic Risk Officer) with zero visible data-engineering jargon.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dash import Dash, Input, Output, State, ctx, dcc, html, no_update
import dash_bootstrap_components as dbc
import pandas as pd

from dashboard.components import app_header, error_banner
from dashboard.figures_storytelling import (
    build_comparison_panel_fig,
    build_daily_volume_swarms_fig,
    build_depth_attenuation_fig,
    build_epicenter_map_fig,
    build_saai_trend_fig,
    build_severity_energy_fig,
    build_top_regions_fig,
    get_temporal_date_bounds,
)
from dashboard.gold_loader import (
    GoldContractError,
    GoldNotFoundError,
    load_gold,
    peek_latest_signature,
)
from dashboard.narrative import RESPONSIBLE_USE_DISCLAIMER, build_narrative
from dashboard.theme import (
    ALERT_COLORS,
    ALERT_LABELS_WITH_ICON,
    DBC_THEME,
    SOURCE_DISPLAY_NAMES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard.storytelling")

DEFAULT_STORYTELLING_PORT = 8051
REFRESH_INTERVAL_MS = 60_000
ALL_ALERT_TIERS = ["NORMAL", "ELEVATED", "HIGH", "CRITICAL"]


def _render_narrative_card(brief: Dict[str, Any]) -> dbc.Card:
    """Renders the executive situational briefing card and permanent responsible-use notice."""
    tier = str(brief.get("posture_tier", "INFO"))
    accent = ALERT_COLORS.get(tier, ALERT_COLORS["INFO"])
    badge_label = ALERT_LABELS_WITH_ICON.get(tier, tier.title())

    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Span(
                                    "EXECUTIVE SEISMIC SITUATIONAL BRIEFING",
                                    className="small fw-bold text-uppercase text-secondary me-2",
                                ),
                                html.Span(
                                    badge_label,
                                    className="w4-status-pill",
                                    style={
                                        "backgroundColor": f"{accent}1a",
                                        "color": accent,
                                        "border": f"1px solid {accent}",
                                    },
                                ),
                            ],
                            className="d-flex flex-wrap align-items-center gap-1",
                        ),
                        html.Span(
                            brief.get("filter_scope_note", ""),
                            className="small text-muted",
                        ),
                    ],
                    className="d-flex flex-wrap justify-content-between align-items-center mb-2",
                ),
                html.H5(brief.get("headline", ""), className="fw-bold mb-2 text-dark"),
                html.Ul(
                    [
                        html.Li(brief.get("anomaly_summary", ""), className="mb-1"),
                        html.Li(brief.get("energy_paradox", ""), className="mb-1"),
                        html.Li(brief.get("regional_and_map_brief", ""), className="mb-0"),
                    ],
                    className="mb-2 ps-3 small text-dark",
                ),
                html.Div(
                    brief.get("disclaimer", RESPONSIBLE_USE_DISCLAIMER),
                    className="w4-disclaimer-box mt-2",
                ),
            ],
            className="p-3",
        ),
        className="w4-narrative-card border-0 shadow-sm mb-3",
        style={"borderLeft": f"5px solid {accent}"},
    )


def create_storytelling_app() -> Dash:
    """Constructs and wires the standalone Seismic Hazard Storytelling Dash application (:8051)."""
    app = Dash(
        __name__,
        external_stylesheets=[DBC_THEME],
        title="Seismic Hazard Briefing — Earthquake Monitoring (Team 1)",
        suppress_callback_exceptions=False,
    )

    alert_options = [
        {"label": ALERT_LABELS_WITH_ICON[t], "value": t}
        for t in ALL_ALERT_TIERS
    ]
    network_options = [
        {"label": label, "value": src_id}
        for src_id, label in SOURCE_DISPLAY_NAMES.items()
    ]
    default_networks = list(SOURCE_DISPLAY_NAMES.keys())

    def serve_layout() -> dbc.Container:
        return dbc.Container(
            fluid=True,
            className="w4-page-container",
            children=[
                dcc.Store(id="story-signature-store", data=""),
                dcc.Store(id="story-bundle-store", data={}),
                dcc.Interval(id="story-refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
                app_header(
                    project_title="Global Earthquake Activity & Seismic Hazard Situational Briefing",
                    topic_angle="Prepared for Dr. Elena Vance · Chief Seismic Risk Officer (Civil Protection & Preparedness)",
                    team_label="TEAM 1",
                    updated_id="story-last-updated",
                    refresh_btn_id="story-refresh-btn",
                    refresh_label="↻ Refresh Briefing Data",
                    subtitle_note=(
                        "Operational hazard overview synthesizing global seismic anomaly posture (SAAI), "
                        "radiated energy concentration, hypocentral depth exposure, and regional alert hotspots."
                    ),
                ),
                html.Div(id="story-alert-container"),
                # Dynamic Executive Narrative Card
                html.Div(id="story-narrative-container"),
                # Global Filter Bar + Explicit Panel Scope Declaration
                dbc.Card(
                    dbc.CardBody(
                        [
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            html.Label(
                                                "Observation Date Window (UTC):",
                                                className="fw-semibold small text-secondary mb-1 d-block",
                                            ),
                                            dcc.DatePickerRange(
                                                id="story-date-range",
                                                display_format="YYYY-MM-DD",
                                                minimum_nights=0,
                                                className="w-100",
                                            ),
                                        ],
                                        xl=3,
                                        lg=4,
                                        md=12,
                                    ),
                                    dbc.Col(
                                        [
                                            html.Label(
                                                "Daily & Regional Alert Posture:",
                                                className="fw-semibold small text-secondary mb-1",
                                            ),
                                            dcc.Dropdown(
                                                id="story-alert-filter",
                                                options=alert_options,
                                                value=ALL_ALERT_TIERS,
                                                multi=True,
                                                placeholder="Select alert levels...",
                                            ),
                                        ],
                                        xl=4,
                                        lg=4,
                                        md=6,
                                    ),
                                    dbc.Col(
                                        [
                                            html.Label(
                                                "Seismic Monitoring Networks:",
                                                className="fw-semibold small text-secondary mb-1",
                                            ),
                                            dcc.Dropdown(
                                                id="story-agency-filter",
                                                options=network_options,
                                                value=default_networks,
                                                multi=True,
                                                placeholder="Select monitoring networks...",
                                            ),
                                        ],
                                        xl=4,
                                        lg=3,
                                        md=6,
                                    ),
                                    dbc.Col(
                                        dbc.Button(
                                            "Reset Filters",
                                            id="story-reset-btn",
                                            color="outline-secondary",
                                            size="sm",
                                            className="w-100 fw-semibold",
                                        ),
                                        xl=1,
                                        lg=1,
                                        md=12,
                                        className="d-flex align-items-end",
                                    ),
                                ],
                                className="g-2 align-items-end",
                            ),
                            html.Div(
                                [
                                    html.Strong("Interactive Filter Scope: "),
                                    html.Span(
                                        "Date Window filters the Executive Briefing, Daily SAAI Trajectory, Daily Earthquake Count, and Global Epicenter Map · "
                                        "Alert Posture filters the Briefing, Daily Views, Top 15 Active Regions, and Epicenter Map · "
                                        "Monitoring Network filters Network Profile Comparison and the Epicenter Map · "
                                        "Magnitude/Depth Baselines and Historical vs. Rolling Cohort View summarize the full catalog for stable reference."
                                    ),
                                ],
                                className="small text-muted mt-2 pt-2 border-top",
                            ),
                        ],
                        className="p-3",
                    ),
                    className="w4-filter-card border-0 shadow-sm mb-3",
                ),
                # Row 1: Panel B (Daily SAAI & Energy) + Panel C (Daily Earthquake Count & Swarms)
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="story-saai-graph",
                                        config={"displaylogo": False, "responsive": True},
                                    ),
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=6,
                            lg=12,
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="story-volume-graph",
                                        config={"displaylogo": False, "responsive": True},
                                    ),
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=6,
                            lg=12,
                        ),
                    ],
                    className="g-3 mb-3",
                ),
                # Row 2: Panel F (Global Interactive Epicenter & Hypocentral Depth Map)
                dbc.Row(
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                dcc.Graph(
                                    id="story-map-graph",
                                    config={"displaylogo": False, "responsive": True},
                                ),
                                className="p-2",
                            ),
                            className="w4-section-card border-0 shadow-sm",
                        ),
                        width=12,
                    ),
                    className="g-3 mb-3",
                ),
                # Row 3: Panel A (Left: Volume vs. Energy Paradox · Right: 3-Class Focal Depth Donut)
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="story-severity-graph",
                                        config={"displaylogo": False, "responsive": True},
                                    ),
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=7,
                            lg=7,
                            md=12,
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="story-depth-graph",
                                        config={"displaylogo": False, "responsive": True},
                                    ),
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=5,
                            lg=5,
                            md=12,
                        ),
                    ],
                    className="g-3 mb-3",
                ),
                # Row 4: Panel D (Top 15 Active Regions) + Panel E (Toggle: By Network <-> Historical vs. Current)
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="story-regions-graph",
                                        config={"displaylogo": False, "responsive": True},
                                    ),
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=6,
                            lg=12,
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    [
                                        html.Div(
                                            [
                                                html.Span(
                                                    "Comparative View:",
                                                    className="fw-semibold small text-secondary me-2",
                                                ),
                                                dbc.RadioItems(
                                                    id="story-panel-e-mode",
                                                    options=[
                                                        {"label": "By Monitoring Network", "value": "agency"},
                                                        {
                                                            "label": "Historical vs. Current Windows",
                                                            "value": "historical_vs_current",
                                                        },
                                                    ],
                                                    value="agency",
                                                    inline=True,
                                                ),
                                            ],
                                            className="d-flex flex-wrap align-items-center justify-content-end px-2 pt-1",
                                        ),
                                        dcc.Graph(
                                            id="story-comparison-graph",
                                            config={"displaylogo": False, "responsive": True},
                                        ),
                                    ],
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=6,
                            lg=12,
                        ),
                    ],
                    className="g-3",
                ),
            ],
        )

    app.layout = serve_layout

    @app.callback(
        Output("story-signature-store", "data"),
        Output("story-bundle-store", "data"),
        Input("story-refresh-btn", "n_clicks"),
        Input("story-refresh-interval", "n_intervals"),
        State("story-signature-store", "data"),
    )
    def sync_storytelling_store(
        n_clicks: Optional[int],
        n_intervals: int,
        current_signature: str,
    ):
        """Loads latest curated Storytelling summary when updated or manually requested."""
        triggered = ctx.triggered_id
        is_manual = triggered == "story-refresh-btn"

        try:
            latest_sig = peek_latest_signature("storytelling")
            if not is_manual and current_signature and latest_sig == current_signature:
                return no_update, no_update

            bundle = load_gold("storytelling", force_reload=is_manual)
            payload = {
                "ok": True,
                "computed_at": bundle.computed_at,
                "loaded_at": bundle.loaded_at,
                "records": bundle.df.to_dict(orient="records"),
            }
            return bundle.signature, payload
        except (GoldNotFoundError, GoldContractError) as exc:
            logger.warning("Storytelling dataset unavailable: %s", exc)
            return (
                "ERROR",
                {
                    "ok": False,
                    "computed_at": "N/A",
                    "records": [],
                },
            )

    @app.callback(
        Output("story-date-range", "min_date_allowed"),
        Output("story-date-range", "max_date_allowed"),
        Output("story-date-range", "start_date"),
        Output("story-date-range", "end_date"),
        Output("story-alert-filter", "value"),
        Output("story-agency-filter", "value"),
        Input("story-bundle-store", "data"),
        Input("story-reset-btn", "n_clicks"),
        State("story-date-range", "start_date"),
        State("story-date-range", "end_date"),
    )
    def sync_filter_defaults(
        bundle_data: Dict[str, Any],
        reset_clicks: Optional[int],
        current_start: Optional[str],
        current_end: Optional[str],
    ):
        """Initializes date picker bounds from available observations and handles filter resets."""
        if not bundle_data or not bundle_data.get("ok"):
            return None, None, None, None, ALL_ALERT_TIERS, default_networks

        df = pd.DataFrame(bundle_data.get("records", []))
        min_d, max_d = get_temporal_date_bounds(df)
        is_reset = ctx.triggered_id == "story-reset-btn"

        if is_reset or not current_start or not current_end:
            return min_d, max_d, min_d, max_d, ALL_ALERT_TIERS, default_networks

        return min_d, max_d, no_update, no_update, no_update, no_update

    @app.callback(
        Output("story-last-updated", "children"),
        Output("story-alert-container", "children"),
        Output("story-narrative-container", "children"),
        Output("story-saai-graph", "figure"),
        Output("story-volume-graph", "figure"),
        Output("story-map-graph", "figure"),
        Output("story-severity-graph", "figure"),
        Output("story-depth-graph", "figure"),
        Output("story-regions-graph", "figure"),
        Output("story-comparison-graph", "figure"),
        Input("story-bundle-store", "data"),
        Input("story-date-range", "start_date"),
        Input("story-date-range", "end_date"),
        Input("story-alert-filter", "value"),
        Input("story-agency-filter", "value"),
        Input("story-panel-e-mode", "value"),
    )
    def render_storytelling_views(
        bundle_data: Dict[str, Any],
        start_date: Optional[str],
        end_date: Optional[str],
        alert_tiers: Optional[List[str]],
        selected_agencies: Optional[List[str]],
        panel_e_mode: str,
    ):
        """Renders executive narrative and all 7 storytelling visualizations."""
        if not bundle_data or not bundle_data.get("ok"):
            empty_df = pd.DataFrame()
            empty_brief = build_narrative(empty_df)
            unavailable_msg = (
                "Curated seismic hazard summaries are not currently available on disk. "
                "Please ask the operations team to refresh the scheduled monitoring summary."
            )
            return (
                [html.Div("Briefing Snapshot: Unavailable"), html.Div("Status: Awaiting scheduled update")],
                error_banner(unavailable_msg, title="Seismic Hazard Briefing Temporarily Unavailable"),
                _render_narrative_card(empty_brief),
                build_saai_trend_fig(empty_df),
                build_daily_volume_swarms_fig(empty_df),
                build_epicenter_map_fig(empty_df),
                build_severity_energy_fig(empty_df),
                build_depth_attenuation_fig(empty_df),
                build_top_regions_fig(empty_df),
                build_comparison_panel_fig(empty_df, mode=panel_e_mode or "agency"),
            )

        df = pd.DataFrame(bundle_data.get("records", []))
        min_d, max_d = get_temporal_date_bounds(df)
        eff_start = start_date or min_d
        eff_end = end_date or max_d
        eff_tiers = alert_tiers if alert_tiers is not None else ALL_ALERT_TIERS
        eff_agencies = selected_agencies if selected_agencies is not None else default_networks

        brief = build_narrative(
            df,
            filters={
                "start_date": eff_start,
                "end_date": eff_end,
                "alert_tiers": eff_tiers,
                "agencies": eff_agencies,
            },
        )

        stamp_block = [
            html.Div(
                [html.Strong("Briefing Computed (UTC): "), html.Span(str(bundle_data.get("computed_at", "N/A")))]
            ),
            html.Div(
                [html.Strong("Catalog Span: "), html.Span(f"{min_d or 'N/A'} to {max_d or 'N/A'} UTC")]
            ),
        ]

        return (
            stamp_block,
            None,
            _render_narrative_card(brief),
            build_saai_trend_fig(df, start_date=eff_start, end_date=eff_end, alert_tiers=eff_tiers),
            build_daily_volume_swarms_fig(df, start_date=eff_start, end_date=eff_end, alert_tiers=eff_tiers),
            build_epicenter_map_fig(
                df,
                start_date=eff_start,
                end_date=eff_end,
                alert_tiers=eff_tiers,
                selected_agencies=eff_agencies,
            ),
            build_severity_energy_fig(df),
            build_depth_attenuation_fig(df),
            build_top_regions_fig(df, alert_tiers=eff_tiers),
            build_comparison_panel_fig(
                df,
                mode=panel_e_mode or "agency",
                selected_agencies=eff_agencies,
            ),
        )

    return app


app = create_storytelling_app()
server = app.server


def main() -> None:
    port = int(os.getenv("STORYTELLING_PORT", str(DEFAULT_STORYTELLING_PORT)))
    host = os.getenv("STORYTELLING_HOST", "0.0.0.0")
    logger.info("Starting Storytelling Dashboard on http://%s:%d", host, port)
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
