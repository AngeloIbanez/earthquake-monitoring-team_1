"""
Shared Dash Bootstrap UI components for Team 1 dashboards (`dashboard/components.py`).
"""

from __future__ import annotations

from dash import html
import dash_bootstrap_components as dbc

from .theme import STATUS_BADGE_TEXT, STATUS_COLORS


def app_header(
    project_title: str,
    topic_angle: str,
    team_label: str,
    updated_id: str,
    refresh_btn_id: str,
    refresh_label: str = "Refresh Data",
    subtitle_note: str = "",
) -> dbc.Card:
    """Institutional dashboard header with project title, angle, Team 1, timestamp, and refresh trigger."""
    return dbc.Card(
        dbc.CardBody(
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.Span(team_label, className="w4-team-pill me-2"),
                                    html.Span(topic_angle, className="w4-angle-subtitle"),
                                ],
                                className="mb-1",
                            ),
                            html.H1(project_title, className="w4-header-title mb-1"),
                            html.P(subtitle_note, className="w4-header-note mb-0") if subtitle_note else None,
                        ],
                        lg=8,
                        md=12,
                    ),
                    dbc.Col(
                        html.Div(
                            [
                                html.Div(
                                    id=updated_id,
                                    className="w4-updated-stamp text-md-end mb-2",
                                ),
                                dbc.Button(
                                    refresh_label,
                                    id=refresh_btn_id,
                                    color="light",
                                    size="sm",
                                    className="fw-semibold shadow-sm",
                                ),
                            ],
                            className="d-flex flex-column align-items-md-end justify-content-center h-100",
                        ),
                        lg=4,
                        md=12,
                    ),
                ],
                className="align-items-center g-2",
            ),
            className="py-3 px-4",
        ),
        className="w4-header-card border-0 shadow-sm mb-3",
    )


def kpi_card(
    title: str,
    value_text: str,
    subtitle: str,
    status: str = "PASS",
    threshold_label: str = "",
) -> dbc.Card:
    """Renders an accessible governance/operational KPI metric card with status border and text badge."""
    norm_status = status if status in STATUS_COLORS else "PASS"
    accent = STATUS_COLORS[norm_status]
    badge_txt = STATUS_BADGE_TEXT.get(norm_status, norm_status)

    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    [
                        html.Span(title, className="w4-kpi-title"),
                        html.Span(
                            badge_txt,
                            className="w4-status-pill",
                            style={
                                "backgroundColor": f"{accent}1a",
                                "color": accent,
                                "border": f"1px solid {accent}",
                            },
                        ),
                    ],
                    className="d-flex justify-content-between align-items-start gap-1 mb-2",
                ),
                html.Div(value_text, className="w4-kpi-value mb-1"),
                html.Div(subtitle, className="w4-kpi-subtitle"),
                (
                    html.Div(threshold_label, className="w4-kpi-threshold mt-1")
                    if threshold_label
                    else None
                ),
            ],
            className="p-3",
        ),
        className="w4-kpi-card h-100 shadow-sm border-0",
        style={"borderTop": f"4px solid {accent}"},
    )


def error_banner(message: str, title: str = "Data Source Unavailable") -> dbc.Alert:
    """User-friendly actionable alert banner displayed instead of raw Python tracebacks."""
    return dbc.Alert(
        [
            html.H5(f"▲ {title}", className="alert-heading fw-bold mb-1"),
            html.P(message, className="mb-0"),
        ],
        color="warning",
        className="shadow-sm border-warning my-2",
    )
