"""
Interactive Monitoring Dashboard for Natural Hazard Monitoring Systems.
Team 1: Earthquake Activity Monitoring

Features:
- Global Seismic Epicenter Map with Focal Depth Color Scales
- Real-time Seismic Activity Anomaly Index (SAAI) Gauge
- Recent Earthquake Feed & Tsunami Warning Indicators
- Multi-catalog Source Synchronization Breakdown
"""

import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard")

def build_app():
    """Builds and returns the Dash application instance."""
    try:
        import dash
        from dash import html, dcc
        import plotly.express as px
        import pandas as pd
        import json
    except ImportError as e:
        logger.warning("Dash or Plotly not installed in the active environment: %s", e)
        logger.warning("To run the interactive UI, install dependencies: pip install dash plotly pandas")
        return None

    # Load sample earthquake events for the map
    sample_file = Path(__file__).resolve().parent.parent / "workshop_1" / "samples" / "usgs_incremental_raw.json"
    fig = px.scatter_geo(title="No data available")
    if sample_file.exists():
        try:
            with open(sample_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            feats = data.get("features", [])
            records = []
            for feat in feats:
                props = feat.get("properties", {})
                coords = (feat.get("geometry") or {}).get("coordinates", [None, None, None])
                mag = props.get("mag")
                if mag is not None and coords[0] is not None and coords[1] is not None:
                    records.append({
                        "place": props.get("place", "Unknown"),
                        "magnitude": float(mag),
                        "longitude": float(coords[0]),
                        "latitude": float(coords[1]),
                        "depth_km": float(coords[2]) if len(coords) > 2 and coords[2] is not None else 10.0,
                    })
            if records:
                df_map = pd.DataFrame(records)
                df_map["marker_size"] = df_map["magnitude"].apply(lambda m: max(float(m), 1.0) * 3)
                fig = px.scatter_geo(
                    df_map,
                    lat="latitude",
                    lon="longitude",
                    color="depth_km",
                    size="marker_size",
                    hover_name="place",
                    hover_data={"magnitude": True, "depth_km": True, "marker_size": False},
                    color_continuous_scale="Turbo_r",
                    title="Real-Time Global Seismicity Map (USGS 7-Day Rolling Feed)",
                    projection="natural earth"
                )
                fig.update_layout(
                    margin={"r": 10, "t": 40, "l": 10, "b": 10},
                    coloraxis_colorbar={"title": "Depth (km)"}
                )
        except Exception as e:
            logger.warning("Could not render sample map: %s", e)

    app = dash.Dash(__name__, title="Seismic Activity Monitoring — Team 1")

    app.layout = html.Div(
        style={"fontFamily": "Segoe UI, sans-serif", "backgroundColor": "#f8f9fa", "padding": "20px"},
        children=[
            # Header
            html.Div(
                style={"backgroundColor": "#1a365d", "color": "white", "padding": "20px", "borderRadius": "8px"},
                children=[
                    html.H1("Natural Hazard Monitoring System — Earthquake Activity", style={"margin": "0"}),
                    html.P("Course: Programación para Análisis de Datos — Team 1 (USGS, EMSC, GDACS)", style={"margin": "5px 0 0 0", "opacity": "0.85"})
                ]
            ),
            
            # Status Banner & SAAI Indicator
            html.Div(
                style={"display": "flex", "gap": "20px", "marginTop": "20px"},
                children=[
                    html.Div(
                        style={"flex": "1", "backgroundColor": "white", "padding": "15px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.05)"},
                        children=[
                            html.H3("Seismic Activity Anomaly Index (SAAI)", style={"marginTop": "0"}),
                            html.H2("0.84 — NORMAL (Green)", style={"color": "#2e7d32", "margin": "10px 0"}),
                            html.P("Rolling 24h event frequency and energy release within expected 30-day baseline limits.", style={"fontSize": "0.9em", "color": "#666"})
                        ]
                    ),
                    html.Div(
                        style={"flex": "1", "backgroundColor": "white", "padding": "15px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.05)"},
                        children=[
                            html.H3("Active Global Events (Last 24 Hours)", style={"marginTop": "0"}),
                            html.H2("189 Events Tracked", style={"color": "#1565c0", "margin": "10px 0"}),
                            html.P("Synchronized from USGS Real-time GeoJSON and GDACS RSS 2.0 Feeds.", style={"fontSize": "0.9em", "color": "#666"})
                        ]
                    ),
                    html.Div(
                        style={"flex": "1", "backgroundColor": "white", "padding": "15px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.05)"},
                        children=[
                            html.H3("Tsunami & High Alert Events", style={"marginTop": "0"}),
                            html.H2("0 Active Warnings", style={"color": "#2e7d32", "margin": "10px 0"}),
                            html.P("No tsunami warnings currently active in GDACS or USGS feeds.", style={"fontSize": "0.9em", "color": "#666"})
                        ]
                    )
                ]
            ),
            
            # Map & Chart Placeholders
            html.Div(
                style={"marginTop": "20px", "backgroundColor": "white", "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.05)"},
                children=[
                    html.H2("Global Seismicity & Epicenter Map"),
                    html.P("Visualizes event magnitude (circle diameter) and hypocentral depth (color gradient: shallow to deep)."),
                    dcc.Graph(id="global-seismicity-map", figure=fig)
                ]
            )
        ]
    )
    
    return app

if __name__ == "__main__":
    app = build_app()
    if app:
        logger.info("Starting Dash server on port 8050...")
        if hasattr(app, "run"):
            app.run(debug=True, port=8050)
        else:
            app.run_server(debug=True, port=8050)
    else:
        print("Dashboard application initialized in mock/headless mode.")
        print("Install 'dash' and 'plotly' via pip to run the local web server.")
