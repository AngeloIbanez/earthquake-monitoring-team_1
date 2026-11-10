"""
Cross-source event matching and deduplication algorithms.
Implements the Haversine distance formula, normalized spatio-temporal cost metric,
and greedy 1-to-1 nearest-neighbor collision resolution.
"""

import math
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
    max_time_sec: float = 900.0
) -> float:
    """Computes dimensionless normalized Euclidean cost in spatio-temporal space."""
    return math.sqrt((dist_km / max_dist_km) ** 2 + (time_diff_sec / max_time_sec) ** 2)

def match_events_nearest_neighbor(
    events_primary: List[CanonicalEvent],
    events_secondary: List[CanonicalEvent],
    max_dist_km: float = 100.0,
    max_time_sec: float = 900.0
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
                dist = haversine_distance(e1.latitude, e1.longitude, e2.latitude, e2.longitude)
                if dist <= max_dist_km:
                    cost = compute_spatio_temporal_cost(dist, time_diff, max_dist_km, max_time_sec)
                    candidates.append({
                        "primary_id": e1.event_id,
                        "secondary_id": e2.event_id,
                        "dist_km": dist,
                        "time_diff_sec": time_diff,
                        "cost": cost,
                        "mag_diff": abs(e1.severity - e2.severity)
                    })
                    
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
