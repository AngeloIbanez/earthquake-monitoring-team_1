"""
Deterministic, template-based narrative generator for the Seismic Hazard
Storytelling Dashboard (`dashboard/narrative.py`).

Audience: Dr. Elena Vance, Chief Seismic Risk Officer (Civil Protection & Emergency Preparedness).
All numbers are computed dynamically from the curated Storytelling summary dataset and active UI filters.
Zero external LLM calls, zero hardcoded statistics, and zero data-engineering jargon.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from .theme import SOURCE_DISPLAY_NAMES

RESPONSIBLE_USE_DISCLAIMER = (
    "Responsible Use Notice: The Seismic Activity Anomaly Index (SAAI) is strictly a "
    "retrospective observational monitoring indicator designed for situational awareness "
    "and post-event prioritization. It does NOT predict future earthquakes, aftershock "
    "timing, or tsunami wave arrival, and must never be used to issue public evacuation orders."
)

ALERT_POSTURE_LABELS: Dict[str, str] = {
    "NORMAL": "Normal (< 1.0)",
    "ELEVATED": "Elevated (1.0–1.99)",
    "HIGH": "High (2.0–3.49)",
    "CRITICAL": "Critical (≥ 3.5)",
    "INFO": "Informational",
}


def _filter_temporal_rows(
    df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    alert_tiers: Optional[List[str]] = None,
) -> pd.DataFrame:
    ts = df[df["aggregation_type"] == "temporal_trend_saai"].copy()
    if ts.empty:
        return ts
    if start_date:
        ts = ts[ts["period_date"] >= str(start_date)]
    if end_date:
        ts = ts[ts["period_date"] <= str(end_date)]
    if alert_tiers is not None:
        ts = ts[ts["alert_tier"].isin(alert_tiers)]
    return ts.sort_values("period_date")


def _filter_significant_events(
    df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    alert_tiers: Optional[List[str]] = None,
    agencies: Optional[List[str]] = None,
) -> pd.DataFrame:
    ev = df[df["aggregation_type"] == "recent_significant_events"].copy()
    if ev.empty:
        return ev
    if start_date:
        ev = ev[ev["period_date"] >= str(start_date)]
    if end_date:
        ev = ev[ev["period_date"] <= str(end_date)]
    if alert_tiers is not None:
        ev = ev[ev["alert_tier"].isin(alert_tiers)]
    if agencies is not None and "source_id" in ev.columns:
        ev = ev[ev["source_id"].isin(agencies)]
    return ev.sort_values(["mean_magnitude", "event_timestamp_utc"], ascending=[False, False])


def build_narrative(
    story_df: pd.DataFrame,
    filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Builds a structured executive seismic briefing from `story_df` and `filters`.

    Parameters
    ----------
    story_df : pd.DataFrame
        Curated Storytelling summary table.
    filters : dict, optional
        Keys: `start_date` (str), `end_date` (str), `alert_tiers` (list[str]),
        `agencies` (list[str]).

    Returns
    -------
    dict
        Keys:
        - `headline`: str
        - `posture_tier`: str
        - `anomaly_summary`: str
        - `energy_paradox`: str
        - `regional_and_map_brief`: str
        - `filter_scope_note`: str
        - `disclaimer`: str
        - `bullets`: list[str]
        - `full_text`: str
    """
    active_filters = filters or {}
    start_date: Optional[str] = active_filters.get("start_date")
    end_date: Optional[str] = active_filters.get("end_date")
    alert_tiers: Optional[List[str]] = active_filters.get("alert_tiers")
    agencies: Optional[List[str]] = active_filters.get("agencies")

    if story_df is None or story_df.empty or "aggregation_type" not in story_df.columns:
        empty_msg = (
            "No seismic monitoring observations are currently loaded for executive briefing generation."
        )
        return {
            "headline": empty_msg,
            "posture_tier": "INFO",
            "anomaly_summary": "No daily anomaly observations match the current selection.",
            "energy_paradox": "Magnitude and radiated energy distributions are unavailable.",
            "regional_and_map_brief": "Regional hotspot and significant epicenter records are unavailable.",
            "filter_scope_note": "Active scope: awaiting monitoring summary load.",
            "disclaimer": RESPONSIBLE_USE_DISCLAIMER,
            "bullets": [empty_msg],
            "full_text": f"{empty_msg} {RESPONSIBLE_USE_DISCLAIMER}",
        }

    # 1. Temporal SAAI & Latest Daily Posture
    ts_filtered = _filter_temporal_rows(story_df, start_date, end_date, alert_tiers)
    if ts_filtered.empty:
        posture_tier = "INFO"
        headline = (
            "No daily seismic anomaly observations fall within the currently selected "
            "date window and alert posture filter."
        )
        anomaly_summary = (
            "Zero days match the active temporal and alert-posture selection. "
            "Expand the observation date range or re-enable additional alert levels to inspect historical activity."
        )
    else:
        latest_row = ts_filtered.iloc[-1]
        latest_date = str(latest_row["period_date"])
        latest_saai = float(latest_row["saai_score"])
        latest_tier = str(latest_row["alert_tier"])
        latest_count = int(round(float(latest_row["event_count"])))
        latest_mmax = float(latest_row["max_magnitude"])
        posture_tier = latest_tier
        posture_label = ALERT_POSTURE_LABELS.get(latest_tier, latest_tier.title())

        total_window_days = int(len(ts_filtered))
        total_window_events = int(round(float(ts_filtered["event_count"].sum())))
        headline = (
            f"Latest Monitored Day ({latest_date} UTC): Global SAAI stands at {latest_saai:+.2f} "
            f"[{posture_label}] with {latest_count:,d} recorded earthquakes (peak M {latest_mmax:.1f}) "
            f"across {total_window_days} observed days ({total_window_events:,d} total events in window)."
        )

        anom_days = ts_filtered[ts_filtered["saai_score"] >= 1.0].sort_values(
            "saai_score", ascending=False
        )
        if anom_days.empty:
            peak_day = ts_filtered.sort_values("saai_score", ascending=False).iloc[0]
            anomaly_summary = (
                f"Anomaly Surveillance: 0 days exceeded the Elevated threshold (SAAI ≥ 1.00) in the selected "
                f"window; the highest index occurred on {peak_day['period_date']} "
                f"(SAAI {float(peak_day['saai_score']):+.2f}, {int(peak_day['event_count']):,d} events, "
                f"peak M {float(peak_day['max_magnitude']):.1f})."
            )
        else:
            details: List[str] = []
            for _, arow in anom_days.head(4).iterrows():
                a_tier_lbl = str(arow["alert_tier"]).title()
                details.append(
                    f"{arow['period_date']} ({a_tier_lbl}, SAAI {float(arow['saai_score']):.2f}, "
                    f"{int(arow['event_count']):,d} events, peak M {float(arow['max_magnitude']):.1f})"
                )
            anomaly_summary = (
                f"Anomalous Seismic Days Detected ({len(anom_days)} day{'s' if len(anom_days) != 1 else ''} "
                f"with SAAI ≥ 1.00): " + "; ".join(details) + "."
            )

    # 2. Volume vs. Radiated Energy Paradox & Shallow Crustal Exposure (Corpus Baseline)
    sev = story_df[story_df["aggregation_type"] == "severity_distribution"].copy()
    dep = story_df[story_df["aggregation_type"] == "depth_regime_breakdown"].copy()
    if not sev.empty:
        total_catalog_events = int(round(float(sev["event_count"].sum())))
        strong_plus = sev[sev["dimension_key"].isin(["Strong (5.5-6.4)", "Major/Great (>=6.5)"])]
        m55_events = int(round(float(strong_plus["event_count"].sum())))
        m55_event_pct = (
            (100.0 * m55_events / total_catalog_events) if total_catalog_events > 0 else 0.0
        )
        m55_energy_pct = float(strong_plus["energy_share_pct"].sum())

        shallow_row = dep[dep["dimension_key"] == "Shallow (0-70 km)"]
        shallow_pct = float(shallow_row["percentage"].iloc[0]) if not shallow_row.empty else 0.0
        shallow_energy_pct = (
            float(shallow_row["energy_share_pct"].iloc[0]) if not shallow_row.empty else 0.0
        )

        energy_paradox = (
            f"Volume vs. Energy Paradox: Earthquakes of M ≥ 5.5 represent only {m55_event_pct:.2f}% "
            f"of cataloged events ({m55_events:,d} of {total_catalog_events:,d}), yet account for "
            f"{m55_energy_pct:.2f}% of total radiated seismic energy. Meanwhile, shallow-focus events "
            f"(0–70 km depth) comprise {shallow_pct:.2f}% of observations and {shallow_energy_pct:.2f}% "
            f"of released energy, concentrating potential ground-shaking exposure near the surface."
        )
    else:
        energy_paradox = "Magnitude and focal-depth baseline distributions are currently unavailable."

    # 3. Regional Alert Hotspots & Significant Epicenters in Active Filter Scope
    reg = story_df[story_df["aggregation_type"] == "top_regions_keywords"].copy()
    if alert_tiers is not None and not reg.empty:
        reg = reg[reg["alert_tier"].isin(alert_tiers)]

    sig = _filter_significant_events(story_df, start_date, end_date, alert_tiers, agencies)

    reg_clause = "No top-15 tectonic regions match the selected alert levels."
    if not reg.empty:
        elevated_reg = reg[reg["alert_tier"] != "NORMAL"].sort_values(
            ["max_magnitude", "event_count"], ascending=[False, False]
        )
        if not elevated_reg.empty:
            top_hotspots = [
                f"{r['dimension_key']} ({int(r['event_count'])} events, peak M {float(r['max_magnitude']):.1f})"
                for _, r in elevated_reg.head(3).iterrows()
            ]
            reg_clause = (
                f"{len(elevated_reg)} of the top 15 active regions carry Elevated-or-higher alert status, "
                f"led by " + ", ".join(top_hotspots) + "."
            )
        else:
            lead_reg = reg.sort_values("event_count", ascending=False).iloc[0]
            reg_clause = (
                f"All {len(reg)} displayed top-volume regions remain in Normal posture, led by "
                f"micro-seismic swarming at {lead_reg['dimension_key']} "
                f"({int(lead_reg['event_count'])} events, peak M {float(lead_reg['max_magnitude']):.1f})."
            )

    if sig.empty:
        map_clause = "Zero recent significant epicenters match the active date, alert, and network filters."
    else:
        top_ev = sig.iloc[0]
        tsunami_count = int(
            sig["tsunami_flag"].apply(lambda v: v is True or str(v).lower() == "true").sum()
        )
        tsunami_note = (
            f"{tsunami_count} event{'s' if tsunami_count != 1 else ''} triggered a USGS coastal tsunami evaluation bulletin"
            if tsunami_count > 0
            else "0 events triggered a USGS coastal tsunami evaluation bulletin"
        )
        map_clause = (
            f"On the global significant-event map, {len(sig):,d} earthquakes match active filters "
            f"(largest: M {float(top_ev['mean_magnitude']):.1f} at {top_ev['secondary_dimension']} on "
            f"{top_ev['period_date']}, focal depth {float(top_ev['mean_depth_km']):.1f} km; {tsunami_note})."
        )

    regional_and_map_brief = f"{reg_clause} {map_clause}"

    # 4. Active Filter Scope Awareness Note
    date_txt = (
        f"{start_date or 'start'} to {end_date or 'latest'}"
        if (start_date or end_date)
        else "Full observation period"
    )
    tier_txt = (
        ", ".join(t.title() for t in alert_tiers)
        if alert_tiers is not None and len(alert_tiers) > 0
        else ("None selected" if alert_tiers is not None else "All alert levels")
    )
    if agencies is None:
        net_txt = "All 4 monitoring networks"
    elif len(agencies) == 0:
        net_txt = "No networks selected"
    elif len(agencies) == len(SOURCE_DISPLAY_NAMES):
        net_txt = "All 4 monitoring networks"
    else:
        net_txt = ", ".join(SOURCE_DISPLAY_NAMES.get(a, a) for a in agencies)

    filter_scope_note = (
        f"Active Briefing Scope — Date window: {date_txt} UTC · "
        f"Alert levels: {tier_txt} · Monitoring networks: {net_txt}."
    )

    bullets = [
        headline,
        anomaly_summary,
        energy_paradox,
        regional_and_map_brief,
    ]
    full_text = " ".join(bullets + [filter_scope_note, RESPONSIBLE_USE_DISCLAIMER])

    return {
        "headline": headline,
        "posture_tier": posture_tier,
        "anomaly_summary": anomaly_summary,
        "energy_paradox": energy_paradox,
        "regional_and_map_brief": regional_and_map_brief,
        "filter_scope_note": filter_scope_note,
        "disclaimer": RESPONSIBLE_USE_DISCLAIMER,
        "bullets": bullets,
        "full_text": full_text,
    }
