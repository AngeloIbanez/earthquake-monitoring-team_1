"""
Cross-source event matching and deduplication algorithms.
Implements the Haversine distance formula, normalized spatio-temporal cost metric,
greedy 1-to-1 nearest-neighbor collision resolution, and transitive connected-component
grouping across multiple seismic catalogs (USGS, EMSC, GDACS).
"""

import math
import statistics
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any, Tuple
from .models import CanonicalEvent


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates great-circle distance between two geospatial coordinates in kilometers."""
    R = 6371.0  # WGS-84 volumetric mean radius of Earth in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c


def compute_spatio_temporal_cost(
    dist_km: float,
    time_diff_sec: float,
    max_dist_km: float = 100.0,
    max_time_sec: float = 900.0,
) -> float:
    """Computes dimensionless normalized Euclidean cost in spatio-temporal space."""
    return math.sqrt((dist_km / max_dist_km) ** 2 + (time_diff_sec / max_time_sec) ** 2)


def match_events_nearest_neighbor(
    events_primary: List[CanonicalEvent],
    events_secondary: List[CanonicalEvent],
    max_dist_km: float = 100.0,
    max_time_sec: float = 900.0,
) -> List[Dict[str, Any]]:
    """
    Finds candidate matches within tolerances and applies 1-to-1 nearest-neighbor
    assignment to eliminate multi-match collisions during seismic swarms.
    """
    candidates = []

    for e1 in events_primary:
        if not e1.timestamp or not e1.is_valid():
            continue
        for e2 in events_secondary:
            if not e2.timestamp or not e2.is_valid():
                continue

            time_diff = abs((e1.timestamp - e2.timestamp).total_seconds())
            if time_diff <= max_time_sec:
                dist = haversine_distance(float(e1.latitude), float(e1.longitude), float(e2.latitude), float(e2.longitude))
                if dist <= max_dist_km:
                    cost = compute_spatio_temporal_cost(dist, time_diff, max_dist_km, max_time_sec)
                    candidates.append(
                        {
                            "primary_id": e1.event_id,
                            "secondary_id": e2.event_id,
                            "dist_km": dist,
                            "time_diff_sec": time_diff,
                            "cost": cost,
                            "mag_diff": abs(float(e1.severity) - float(e2.severity)),
                        }
                    )

    # Sort candidate pairs by ascending spatio-temporal cost
    candidates.sort(key=lambda x: x["cost"])

    # Greedy nearest-neighbor selection to guarantee 1-to-1 bijection
    matched_primary = set()
    matched_secondary = set()
    unique_matches = []

    for match in candidates:
        p_id = match["primary_id"]
        s_id = match["secondary_id"]
        if p_id not in matched_primary and s_id not in matched_secondary:
            matched_primary.add(p_id)
            matched_secondary.add(s_id)
            unique_matches.append(match)

    return unique_matches


def build_cross_source_event_groups(
    events: List[CanonicalEvent],
    max_dist_km: float = 100.0,
    max_time_sec: float = 900.0,
) -> Tuple[List[Tuple[str, str]], Dict[str, Any]]:
    """
    Performs pairwise 1-to-1 nearest-neighbor matching across all distinct data sources
    and consolidates connected components via Union-Find into canonical event_group rows.

    Returns:
        (group_rows, metrics)
        - group_rows: list of (group_id, event_id) tuples for matched multi-source events.
        - metrics: evaluation statistics (matched_pairs, total_groups, median_dt, median_dist, mean_mag_diff).
    """
    by_source: Dict[str, List[CanonicalEvent]] = defaultdict(list)
    for ev in events:
        if ev.is_valid():
            by_source[ev.source_id].append(ev)

    source_ids = sorted(by_source.keys())
    all_matches: List[Dict[str, Any]] = []

    # Compare distinct catalog pairs (avoiding USGS historical vs USGS incremental self-agency comparison)
    for i in range(len(source_ids)):
        for j in range(i + 1, len(source_ids)):
            s1, s2 = source_ids[i], source_ids[j]
            agency1 = s1.split("_")[0]
            agency2 = s2.split("_")[0]
            if agency1 == agency2:
                continue
            pair_matches = match_events_nearest_neighbor(
                by_source[s1],
                by_source[s2],
                max_dist_km=max_dist_km,
                max_time_sec=max_time_sec,
            )
            all_matches.extend(pair_matches)

    # Disjoint-Set (Union-Find) for transitive component grouping
    parent: Dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            # Deterministic canonical root (prefer usgs_* if present, else lexicographical)
            if rb.startswith("usgs_") and not ra.startswith("usgs_"):
                parent[ra] = rb
            elif ra.startswith("usgs_") and not rb.startswith("usgs_"):
                parent[rb] = ra
            elif ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

    for m in all_matches:
        union(m["primary_id"], m["secondary_id"])

    components: Dict[str, List[str]] = defaultdict(list)
    for ev_id in list(parent.keys()):
        root = find(ev_id)
        components[root].append(ev_id)

    group_rows: List[Tuple[str, str]] = []
    for root_id, member_ids in sorted(components.items()):
        if len(member_ids) > 1:
            group_id = f"grp_{root_id}"
            for m_id in sorted(set(member_ids)):
                group_rows.append((group_id, m_id))

    dt_list = [m["time_diff_sec"] for m in all_matches]
    dist_list = [m["dist_km"] for m in all_matches]
    mag_list = [m["mag_diff"] for m in all_matches]

    metrics = {
        "matched_pairs": len(all_matches),
        "total_groups": len([c for c in components.values() if len(c) > 1]),
        "total_grouped_events": len(group_rows),
        "median_time_diff_sec": round(statistics.median(dt_list), 3) if dt_list else 0.0,
        "median_dist_km": round(statistics.median(dist_list), 3) if dist_list else 0.0,
        "mean_mag_diff": round(statistics.mean(mag_list), 3) if mag_list else 0.0,
    }
    return group_rows, metrics
