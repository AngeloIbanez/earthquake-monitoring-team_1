#!/usr/bin/env python3
"""
Governance & Data Quality Monitoring Dashboard (`dashboard/governance_app.py`).

Independent Plotly Dash web application running on `http://localhost:8050`
(configurable via `GOVERNANCE_PORT`), designed for data engineering stakeholders
and powered exclusively by `datalake_gold/governance_*.parquet`.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dash import Dash, Input, Output, State, ctx, dash_table, dcc, html, no_update
import dash_bootstrap_components as dbc
import pandas as pd

from dashboard.components import app_header, error_banner, kpi_card
from dashboard.figures_governance import (
    build_null_rate_fig,
    build_outlier_quartile_spread_fig,
    build_outlier_rate_fig,
    build_text_length_fig,
    build_volume_over_time_fig,
    extract_governance_kpis,
    prepare_governance_table_records,
)
from dashboard.gold_loader import (
    GoldContractError,
    GoldNotFoundError,
    load_gold,
    peek_latest_signature,
)
from dashboard.theme import DBC_THEME, SOURCE_DISPLAY_NAMES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard.governance")

DEFAULT_GOVERNANCE_PORT = 8050
REFRESH_INTERVAL_MS = 60_000


def create_governance_app() -> Dash:
    """Constructs and wires the standalone Data Governance Dash application (:8050)."""
    app = Dash(
        __name__,
        external_stylesheets=[DBC_THEME],
        title="Data Governance — Earthquake Monitoring (Team 1)",
        suppress_callback_exceptions=False,
    )

    source_options = [
        {"label": label, "value": src_id}
        for src_id, label in SOURCE_DISPLAY_NAMES.items()
    ]
    default_sources = list(SOURCE_DISPLAY_NAMES.keys())

    def serve_layout() -> dbc.Container:
        return dbc.Container(
            fluid=True,
            className="w4-page-container",
            children=[
                dcc.Store(id="gov-signature-store", data=""),
                dcc.Store(id="gov-bundle-store", data={}),
                dcc.Interval(id="gov-refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),
                app_header(
                    project_title="Earthquake Activity Monitoring — Data Governance & Structural Health",
                    topic_angle="Medallion Gold Layer Telemetry Audit · SLA & Reconciliation Monitor",
                    team_label="TEAM 1",
                    updated_id="gov-last-updated",
                    refresh_btn_id="gov-refresh-btn",
                    refresh_label="↻ Refresh Gold Data",
                    subtitle_note=(
                        "Technical audit view tracking multi-catalog completeness, default-depth pinning, "
                        "cross-snapshot deduplication, Tukey outlier bounds, quarantine status, and 1-to-1 event matching."
                    ),
                ),
                html.Div(id="gov-alert-container"),
                # Top Row: 7 KPI Summary Cards (responsive 12-col grid without horizontal scroll)
                dbc.Row(id="gov-kpi-cards-row", className="g-2 mb-3"),
                # Row 2: Null & Pinned-Depth Rate (Left 6) + Volume Over Time with Controls (Right 6)
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="gov-null-rate-graph",
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
                                        dbc.Row(
                                            [
                                                dbc.Col(
                                                    [
                                                        html.Label(
                                                            "Filter Seismic Sources:",
                                                            className="fw-semibold small text-secondary mb-1",
                                                        ),
                                                        dcc.Dropdown(
                                                            id="gov-source-filter",
                                                            options=source_options,
                                                            value=default_sources,
                                                            multi=True,
                                                            placeholder="Select seismic sources...",
                                                        ),
                                                    ],
                                                    md=8,
                                                ),
                                                dbc.Col(
                                                    [
                                                        html.Label(
                                                            "Aggregation Mode:",
                                                            className="fw-semibold small text-secondary mb-1",
                                                        ),
                                                        dbc.RadioItems(
                                                            id="gov-volume-mode",
                                                            options=[
                                                                {"label": "Daily", "value": "daily"},
                                                                {"label": "Cumulative", "value": "cumulative"},
                                                            ],
                                                            value="daily",
                                                            inline=True,
                                                            className="pt-1",
                                                        ),
                                                    ],
                                                    md=4,
                                                ),
                                            ],
                                            className="g-2 px-2 pt-1 mb-1 align-items-end",
                                        ),
                                        dcc.Graph(
                                            id="gov-volume-graph",
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
                    className="g-3 mb-3",
                ),
                # Row 3: Outlier Rate (%) + Quartile Spread + Place Text Length by Agency
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="gov-outlier-rate-graph",
                                        config={"displaylogo": False, "responsive": True},
                                    ),
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=4,
                            lg=6,
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="gov-outlier-spread-graph",
                                        config={"displaylogo": False, "responsive": True},
                                    ),
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=4,
                            lg=6,
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody(
                                    dcc.Graph(
                                        id="gov-text-length-graph",
                                        config={"displaylogo": False, "responsive": True},
                                    ),
                                    className="p-2",
                                ),
                                className="w4-section-card border-0 shadow-sm h-100",
                            ),
                            xl=4,
                            lg=12,
                        ),
                    ],
                    className="g-3 mb-3",
                ),
                # Row 4: Structured Data Quality Summary Table
                dbc.Row(
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.Div(
                                        [
                                            html.H5(
                                                "Data Quality & Governance SLA Summary Table",
                                                className="fw-bold mb-0",
                                            ),
                                            html.Span(
                                                "Filterable & sortable audit ledger across all 6 Gold KPI categories "
                                                "(status badges combine symbol + text + color for CVD accessibility).",
                                                className="small text-muted",
                                            ),
                                        ],
                                        className="d-flex flex-wrap justify-content-between align-items-baseline mb-2 px-1",
                                    ),
                                    dash_table.DataTable(
                                        id="gov-quality-table",
                                        columns=[
                                            {"name": "KPI Category", "id": "Category"},
                                            {"name": "Metric Identifier", "id": "KPI Metric"},
                                            {"name": "Source Scope", "id": "Source"},
                                            {"name": "Period", "id": "Period"},
                                            {"name": "Current Value", "id": "Current Value"},
                                            {"name": "Supporting Stat", "id": "Secondary"},
                                            {"name": "Unit", "id": "Unit"},
                                            {"name": "SLA Threshold", "id": "SLA Threshold"},
                                            {"name": "Compliance Status", "id": "Status"},
                                        ],
                                        data=[],
                                        filter_action="native",
                                        sort_action="native",
                                        page_action="native",
                                        page_size=12,
                                        style_table={"overflowX": "auto"},
                                        style_header={
                                            "backgroundColor": "#0f2942",
                                            "color": "#ffffff",
                                            "fontWeight": "700",
                                            "fontSize": "12px",
                                            "border": "1px solid #0f2942",
                                        },
                                        style_cell={
                                            "fontFamily": "Inter, Segoe UI, sans-serif",
                                            "fontSize": "12px",
                                            "padding": "7px 10px",
                                            "textAlign": "left",
                                            "border": "1px solid #e2e8f0",
                                            "maxWidth": "280px",
                                            "overflow": "hidden",
                                            "textOverflow": "ellipsis",
                                        },
                                        style_data_conditional=[
                                            {
                                                "if": {
                                                    "filter_query": '{Status} contains "PASS"',
                                                    "column_id": "Status",
                                                },
                                                "backgroundColor": "#e6f5f0",
                                                "color": "#11694e",
                                                "fontWeight": "700",
                                            },
                                            {
                                                "if": {
                                                    "filter_query": '{Status} contains "WARN"',
                                                    "column_id": "Status",
                                                },
                                                "backgroundColor": "#fff3e6",
                                                "color": "#9a3f00",
                                                "fontWeight": "700",
                                            },
                                            {
                                                "if": {
                                                    "filter_query": '{Status} contains "N/A"',
                                                    "column_id": "Status",
                                                },
                                                "backgroundColor": "#f1f5f9",
                                                "color": "#475569",
                                                "fontWeight": "700",
                                            },
                                        ],
                                    ),
                                ],
                                className="p-3",
                            ),
                            className="w4-section-card border-0 shadow-sm",
                        ),
                        width=12,
                    ),
                    className="g-3",
                ),
            ],
        )

    app.layout = serve_layout

    @app.callback(
        Output("gov-signature-store", "data"),
        Output("gov-bundle-store", "data"),
        Input("gov-refresh-btn", "n_clicks"),
        Input("gov-refresh-interval", "n_intervals"),
        State("gov-signature-store", "data"),
    )
    def sync_governance_store(
        n_clicks: Optional[int],
        n_intervals: int,
        current_signature: str,
    ):
        """Loads the latest Governance Gold file only when new data arrives or manual refresh is clicked."""
        triggered = ctx.triggered_id
        is_manual = triggered == "gov-refresh-btn"

        try:
            latest_sig = peek_latest_signature("governance")
            if not is_manual and current_signature and latest_sig == current_signature:
                return no_update, no_update

            bundle = load_gold("governance", force_reload=is_manual)
            payload = {
                "ok": True,
                "filename": bundle.filename,
                "computed_at": bundle.computed_at,
                "loaded_at": bundle.loaded_at,
                "records": bundle.df.to_dict(orient="records"),
            }
            return bundle.signature, payload
        except (GoldNotFoundError, GoldContractError) as exc:
            logger.warning("Governance Gold unavailable: %s", exc)
            return (
                "ERROR",
                {
                    "ok": False,
                    "error": str(exc),
                    "filename": "None",
                    "computed_at": "N/A",
                    "records": [],
                },
            )

    @app.callback(
        Output("gov-last-updated", "children"),
        Output("gov-alert-container", "children"),
        Output("gov-kpi-cards-row", "children"),
        Output("gov-null-rate-graph", "figure"),
        Output("gov-outlier-rate-graph", "figure"),
        Output("gov-outlier-spread-graph", "figure"),
        Output("gov-text-length-graph", "figure"),
        Output("gov-quality-table", "data"),
        Input("gov-bundle-store", "data"),
    )
    def render_static_governance_views(bundle_data: Dict[str, Any]):
        """Renders header metadata, KPI cards, quality charts, and SLA table from cached Gold store."""
        if not bundle_data or not bundle_data.get("ok"):
            err_msg = (bundle_data or {}).get(
                "error",
                "No Governance Gold file available. Run the Airflow pipeline to generate Gold summaries.",
            )
            empty_df = pd.DataFrame()
            return (
                [html.Div("Gold Snapshot: Unavailable"), html.Div("Status: Awaiting pipeline run")],
                error_banner(err_msg, title="Governance Gold Dataset Not Found"),
                [],
                build_null_rate_fig(empty_df),
                build_outlier_rate_fig(empty_df),
                build_outlier_quartile_spread_fig(empty_df),
                build_text_length_fig(empty_df),
                [],
            )

        df = pd.DataFrame(bundle_data.get("records", []))
        kpis = extract_governance_kpis(df)
        # Layout 7 cards neatly across 12 columns (first 4 as lg=3, next 3 as lg=4 — zero horizontal overflow!)
        card_cols: List[dbc.Col] = []
        for idx, item in enumerate(kpis):
            lg_width = 3 if idx < 4 else 4
            card_cols.append(
                dbc.Col(
                    kpi_card(
                        title=item["title"],
                        value_text=item["value"],
                        subtitle=item["subtitle"],
                        status=item["status"],
                        threshold_label=item["threshold"],
                    ),
                    lg=lg_width,
                    md=6,
                    sm=12,
                )
            )

        stamp_block = [
            html.Div([html.Strong("Gold Computed (UTC): "), html.Span(str(bundle_data.get("computed_at", "N/A")))]),
            html.Div([html.Strong("Active File: "), html.Code(str(bundle_data.get("filename", "")), className="text-light")]),
        ]

        return (
            stamp_block,
            None,
            card_cols,
            build_null_rate_fig(df),
            build_outlier_rate_fig(df),
            build_outlier_quartile_spread_fig(df),
            build_text_length_fig(df),
            prepare_governance_table_records(df),
        )

    @app.callback(
        Output("gov-volume-graph", "figure"),
        Input("gov-bundle-store", "data"),
        Input("gov-source-filter", "value"),
        Input("gov-volume-mode", "value"),
    )
    def render_volume_view(
        bundle_data: Dict[str, Any],
        selected_sources: Optional[List[str]],
        view_mode: str,
    ):
        """Updates only the Volume Over Time figure when source filter or Daily/Cumulative mode changes."""
        if not bundle_data or not bundle_data.get("ok"):
            return build_volume_over_time_fig(pd.DataFrame())
        df = pd.DataFrame(bundle_data.get("records", []))
        return build_volume_over_time_fig(df, selected_sources=selected_sources, view_mode=view_mode or "daily")

    return app


app = create_governance_app()
server = app.server


def main() -> None:
    port = int(os.getenv("GOVERNANCE_PORT", str(DEFAULT_GOVERNANCE_PORT)))
    host = os.getenv("GOVERNANCE_HOST", "127.0.0.1")
    logger.info("Starting Governance Dashboard on http://%s:%d", host, port)
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
