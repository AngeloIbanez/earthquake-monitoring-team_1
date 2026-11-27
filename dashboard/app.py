#!/usr/bin/env python3
"""
Informational Entry-Point Directory for Team 1 Dash Applications (`dashboard/app.py`).

Workshop #4 replaces the initial static prototype with two strictly independent
Plotly Dash web applications powered exclusively by curated Gold Parquet summaries:

1. Data Governance Dashboard (Technical Data Engineering View):
   - Module: `dashboard/governance_app.py`
   - Default URL: `http://localhost:8050` (`GOVERNANCE_PORT`)

2. Seismic Hazard Storytelling Dashboard (Dr. Elena Vance — Chief Seismic Risk Officer):
   - Module: `dashboard/storytelling_app.py`
   - Default URL: `http://localhost:8051` (`STORYTELLING_PORT`)
"""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
from typing import Dict, List

DASHBOARD_ENTRYPOINTS: Dict[str, Dict[str, str]] = {
    "governance": {
        "name": "Data Governance & Structural Health Dashboard",
        "script": "dashboard/governance_app.py",
        "url": "http://localhost:8050",
        "audience": "Data Engineering & Quality Operations",
    },
    "storytelling": {
        "name": "Seismic Hazard & Anomaly Storytelling Dashboard",
        "script": "dashboard/storytelling_app.py",
        "url": "http://localhost:8051",
        "audience": "Dr. Elena Vance — Chief Seismic Risk Officer",
    },
}


def get_dashboard_entrypoints() -> Dict[str, Dict[str, str]]:
    """Returns metadata for the two independent Workshop #4 Dash applications."""
    return {k: dict(v) for k, v in DASHBOARD_ENTRYPOINTS.items()}


def format_launcher_banner() -> str:
    """Formats human-readable instructions for launching both independent apps."""
    lines: List[str] = [
        "========================================================================",
        " Team 1 — Earthquake Activity Monitoring (Workshop #4 Dash Applications)",
        "========================================================================",
        "Two independent Plotly Dash applications are provided:",
        "",
    ]
    for key, meta in DASHBOARD_ENTRYPOINTS.items():
        lines.extend(
            [
                f"  [{key.upper()}] {meta['name']}",
                f"    Audience : {meta['audience']}",
                f"    Command  : python {meta['script']}",
                f"    URL      : {meta['url']}",
                "",
            ]
        )
    lines.append("Tip: Run both in separate terminals or pass `--launch both` to spawn both processes.")
    return "\n".join(lines)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Informational launcher for Team 1 Workshop #4 Dash applications."
    )
    parser.add_argument(
        "--launch",
        choices=["none", "governance", "storytelling", "both"],
        default="none",
        help="Optionally launch one or both independent dashboard processes (default: print instructions and exit 0).",
    )
    args = parser.parse_args(argv)

    print(format_launcher_banner())
    if args.launch == "none":
        return 0

    repo_root = Path(__file__).resolve().parents[1]
    targets = (
        ["governance", "storytelling"] if args.launch == "both" else [args.launch]
    )
    procs: List[subprocess.Popen[bytes]] = []
    try:
        for key in targets:
            script_path = repo_root / DASHBOARD_ENTRYPOINTS[key]["script"]
            procs.append(subprocess.Popen([sys.executable, str(script_path)], cwd=str(repo_root)))
        for proc in procs:
            proc.wait()
    except KeyboardInterrupt:
        for proc in procs:
            proc.terminate()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
