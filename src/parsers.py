"""
Payload parsers for heterogeneous natural hazard sources.
Supports USGS GeoJSON, EMSC JSON, and GDACS RSS 2.0 XML feeds.
"""

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Optional
from .models import CanonicalEvent, EarthquakeDetail


def _compute_content_hash(
    event_id: str,
    timestamp: Optional[datetime],
    lat: Optional[float],
    lon: Optional[float],
    severity: Optional[float],
    depth: Optional[float],
) -> str:
    """Computes a deterministic SHA-256 content hash for change detection."""
    ts_str = timestamp.isoformat() if timestamp else "None"
    lat_str = f"{lat:.4f}" if lat is not None else "None"
    lon_str = f"{lon:.4f}" if lon is not None else "None"
    sev_str = f"{severity:.2f}" if severity is not None else "None"
    dep_str = f"{depth:.2f}" if depth is not None else "None"
    fingerprint = f"{event_id}|{ts_str}|{lat_str}|{lon_str}|{sev_str}|{dep_str}"
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:16]


def _normalize_depth(raw_depth: Any) -> Optional[float]:
    """
    Missing-value and physical-bound strategy for focal depth (km):
    - Missing / non-numeric -> None (documented nullable domain attribute)
    - Negative depths above highest topography (< -5.0 km) or > 800.0 km -> clamped/None
    """
    if raw_depth is None:
        return None
    try:
        d = float(raw_depth)
    except (TypeError, ValueError):
        return None
    if d < -5.0 or d > 800.0:
        return None
    return d


def _normalize_place(raw_place: Any) -> str:
    """Missing-value strategy for location description: fallback to 'Unknown Location'."""
    if raw_place is None:
        return "Unknown Location"
    cleaned = str(raw_place).strip()
    return cleaned if cleaned else "Unknown Location"


def _normalize_mag_type(raw_mag_type: Any, default: str = "unk") -> str:
    """Missing-value strategy for magnitude scale unit."""
    if raw_mag_type is None:
        return default
    cleaned = str(raw_mag_type).strip().lower()
    return cleaned if cleaned else default


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def parse_usgs_geojson(
    raw_data: Dict[str, Any], source_id: str = "usgs_fdsn"
) -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """Parses USGS GeoJSON payload into canonical event and domain earthquake models."""
    events: List[CanonicalEvent] = []
    earthquakes: List[EarthquakeDetail] = []

    features = raw_data.get("features", []) if isinstance(raw_data, dict) else []
    for feat in features:
        if not isinstance(feat, dict):
            continue
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") if isinstance(geom.get("coordinates"), list) else []

        raw_id = str(feat.get("id") or "").strip()
        event_id = f"{source_id}_{raw_id}" if raw_id else f"{source_id}_"

        # Convert Unix epoch milliseconds to UTC datetime
        epoch_ms = props.get("time")
        event_time = None
        if epoch_ms is not None:
            try:
                event_time = datetime.fromtimestamp(float(epoch_ms) / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                event_time = None

        updated_ms = props.get("updated")
        updated_time = event_time
        if updated_ms is not None:
            try:
                updated_time = datetime.fromtimestamp(float(updated_ms) / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                updated_time = event_time

        lon = _safe_float(coords[0]) if len(coords) > 0 else None
        lat = _safe_float(coords[1]) if len(coords) > 1 else None
        depth = _normalize_depth(coords[2] if len(coords) > 2 else None)

        severity = _safe_float(props.get("mag"))
        mag_type = _normalize_mag_type(props.get("magType"), default="unk")

        base_url = props.get("url") or f"https://earthquake.usgs.gov/earthquakes/eventpage/{raw_id}"
        content_hash = _compute_content_hash(event_id, event_time, lat, lon, severity, depth)
        raw_ref = f"{base_url}#sha256={content_hash}" if props.get("url") else base_url

        event = CanonicalEvent(
            event_id=event_id,
            source_id=source_id,
            event_type="earthquake",
            timestamp=event_time,
            latitude=lat,
            longitude=lon,
            severity=severity,
            severity_unit=mag_type,
            raw_reference=raw_ref,
            updated_at=updated_time,
            raw_payload=feat,
        )

        detail = EarthquakeDetail(
            event_id=event_id,
            depth_km=depth,
            magnitude_type=mag_type,
            place=_normalize_place(props.get("place")),
            tsunami_flag=bool(props.get("tsunami") == 1),
        )

        events.append(event)
        earthquakes.append(detail)

    return events, earthquakes


def parse_emsc_json(
    raw_data: Any, source_id: str = "emsc_fdsn"
) -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """Parses EMSC JSON / GeoJSON payload into canonical event and domain earthquake models."""
    events: List[CanonicalEvent] = []
    earthquakes: List[EarthquakeDetail] = []

    features = raw_data.get("features", []) if isinstance(raw_data, dict) else (raw_data or [])
    for feat in features:
        if not isinstance(feat, dict):
            continue
        props = feat.get("properties") if isinstance(feat.get("properties"), dict) else feat
        geom = feat.get("geometry") if isinstance(feat.get("geometry"), dict) else {}
        coords = geom.get("coordinates") if isinstance(geom.get("coordinates"), list) else []

        raw_id = str(props.get("source_id") or feat.get("id") or props.get("unid") or "").strip()
        event_id = f"{source_id}_{raw_id}" if raw_id else f"{source_id}_"

        # Parse ISO-8601 string
        time_str = props.get("time")
        event_time = None
        if time_str:
            try:
                clean_time = str(time_str).replace("Z", "+00:00")
                event_time = datetime.fromisoformat(clean_time)
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
            except Exception:
                event_time = None

        update_str = props.get("lastupdate")
        updated_time = event_time
        if update_str:
            try:
                clean_upd = str(update_str).replace("Z", "+00:00")
                updated_time = datetime.fromisoformat(clean_upd)
                if updated_time.tzinfo is None:
                    updated_time = updated_time.replace(tzinfo=timezone.utc)
            except Exception:
                updated_time = event_time

        raw_lon = coords[0] if len(coords) > 0 and coords[0] is not None else props.get("lon")
        raw_lat = coords[1] if len(coords) > 1 and coords[1] is not None else props.get("lat")
        raw_depth = coords[2] if len(coords) > 2 and coords[2] is not None else props.get("depth")

        lon = _safe_float(raw_lon)
        lat = _safe_float(raw_lat)
        depth = _normalize_depth(raw_depth)

        severity = _safe_float(props.get("mag"))
        mag_type = _normalize_mag_type(props.get("magtype"), default="unk")

        unid = props.get("unid") or raw_id
        base_url = f"https://www.seismicportal.eu/eventdetails.html?unid={unid}"
        content_hash = _compute_content_hash(event_id, event_time, lat, lon, severity, depth)

        event = CanonicalEvent(
            event_id=event_id,
            source_id=source_id,
            event_type="earthquake",
            timestamp=event_time,
            latitude=lat,
            longitude=lon,
            severity=severity,
            severity_unit=mag_type,
            raw_reference=f"{base_url}#sha256={content_hash}",
            updated_at=updated_time,
            raw_payload=feat,
        )

        detail = EarthquakeDetail(
            event_id=event_id,
            depth_km=depth,
            magnitude_type=mag_type,
            place=_normalize_place(props.get("flynn_region") or props.get("place")),
            tsunami_flag=False,
        )

        events.append(event)
        earthquakes.append(detail)

    return events, earthquakes


def parse_gdacs_xml(
    xml_content: str, source_id: str = "gdacs_realtime"
) -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """
    Parses GDACS RSS 2.0 XML payload using proper XML namespaces.
    Extracts coordinates from <georss:point> and regex-parses magnitude and depth from <gdacs:severity>.
    """
    events: List[CanonicalEvent] = []
    earthquakes: List[EarthquakeDetail] = []

    root = ET.fromstring(xml_content) if isinstance(xml_content, str) else xml_content
    ns = {
        "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
        "georss": "http://www.georss.org/georss",
        "gdacs": "http://www.gdacs.org",
    }

    for item in root.findall(".//item"):
        ev_type = item.findtext("gdacs:eventtype", namespaces=ns)
        if ev_type != "EQ":
            continue

        raw_id = (item.findtext("gdacs:eventid", namespaces=ns) or item.findtext("guid") or "").strip()
        event_id = f"{source_id}_{raw_id}" if raw_id else f"{source_id}_"

        from_date = item.findtext("gdacs:fromdate", namespaces=ns) or item.findtext("pubDate")
        event_time = None
        if from_date:
            try:
                event_time = datetime.strptime(from_date.strip(), "%a, %d %b %Y %H:%M:%S %Z").replace(
                    tzinfo=timezone.utc
                )
            except Exception:
                event_time = None

        # Parse coordinates from georss:point or geo:lat / geo:long
        point = item.findtext("georss:point", namespaces=ns)
        lat: Optional[float] = None
        lon: Optional[float] = None
        if point:
            pts = point.strip().split()
            if len(pts) == 2:
                lat = _safe_float(pts[0])
                lon = _safe_float(pts[1])
        else:
            lat = _safe_float(item.findtext("geo:lat", namespaces=ns))
            lon = _safe_float(item.findtext("geo:long", namespaces=ns))

        severity_str = item.findtext("gdacs:severity", namespaces=ns) or ""
        mag_m = re.search(r"Magnitude\s*([-\d.]+)\s*M?", severity_str)
        severity = _safe_float(mag_m.group(1)) if mag_m else None

        depth_m = re.search(r"Depth:\s*([-\d.]+)\s*km", severity_str)
        depth = _normalize_depth(depth_m.group(1) if depth_m else None)

        is_tsunami = item.findtext("gdacs:calculationtype", namespaces=ns) == "tsunami"
        title = item.findtext("title")
        link = item.findtext("link") or ""
        content_hash = _compute_content_hash(event_id, event_time, lat, lon, severity, depth)

        raw_item_dict = {
            "eventid": raw_id,
            "eventtype": ev_type,
            "fromdate": from_date,
            "point": point,
            "severity": severity_str,
            "title": title,
            "link": link,
        }

        event = CanonicalEvent(
            event_id=event_id,
            source_id=source_id,
            event_type="earthquake",
            timestamp=event_time,
            latitude=lat,
            longitude=lon,
            severity=severity,
            severity_unit="mw",
            raw_reference=f"{link}#sha256={content_hash}" if link else None,
            updated_at=event_time,
            raw_payload=raw_item_dict,
        )

        detail = EarthquakeDetail(
            event_id=event_id,
            depth_km=depth,
            magnitude_type="mw",
            place=_normalize_place(title),
            tsunami_flag=is_tsunami,
        )

        events.append(event)
        earthquakes.append(detail)

    return events, earthquakes
