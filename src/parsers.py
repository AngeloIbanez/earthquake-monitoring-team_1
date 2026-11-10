"""
Payload parsers for heterogeneous natural hazard sources.
Supports USGS GeoJSON, EMSC JSON, and GDACS RSS 2.0 XML feeds.
"""

import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Optional
from .models import CanonicalEvent, EarthquakeDetail

def parse_usgs_geojson(raw_data: Dict[str, Any], source_id: str = "usgs_fdsn") -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """Parses USGS GeoJSON payload into canonical event and domain earthquake models."""
    events = []
    earthquakes = []
    
    features = raw_data.get("features", [])
    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [None, None, None])
        
        raw_id = feat.get("id", "")
        event_id = f"{source_id}_{raw_id}"
        
        # Convert Unix epoch milliseconds to UTC datetime
        epoch_ms = props.get("time")
        event_time = None
        if epoch_ms is not None:
            event_time = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
            
        lon = float(coords[0]) if coords[0] is not None else 0.0
        lat = float(coords[1]) if coords[1] is not None else 0.0
        depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else None
        
        mag = props.get("mag")
        severity = float(mag) if mag is not None else 0.0
        mag_type = str(props.get("magType", "unk")).lower()
        
        event = CanonicalEvent(
            event_id=event_id,
            source_id=source_id,
            event_type="earthquake",
            timestamp=event_time,
            latitude=lat,
            longitude=lon,
            severity=severity,
            severity_unit=mag_type,
            raw_reference=props.get("url")
        )
        
        detail = EarthquakeDetail(
            event_id=event_id,
            depth_km=depth,
            magnitude_type=mag_type,
            place=props.get("place"),
            tsunami_flag=bool(props.get("tsunami") == 1)
        )
        
        events.append(event)
        earthquakes.append(detail)
        
    return events, earthquakes

def parse_emsc_json(raw_data: Any, source_id: str = "emsc_fdsn") -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """Parses EMSC JSON / GeoJSON payload into canonical event and domain earthquake models."""
    events = []
    earthquakes = []
    
    features = raw_data.get("features", []) if isinstance(raw_data, dict) else raw_data
    for feat in features:
        props = feat.get("properties", feat)
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [props.get("lon"), props.get("lat"), props.get("depth")])
        
        raw_id = props.get("source_id") or feat.get("id") or props.get("unid", "")
        event_id = f"{source_id}_{raw_id}"
        
        # Parse ISO-8601 string
        time_str = props.get("time")
        event_time = None
        if time_str:
            try:
                # Handle ISO format with trailing Z
                clean_time = time_str.replace("Z", "+00:00")
                event_time = datetime.fromisoformat(clean_time)
            except Exception:
                pass
                
        lon = float(coords[0]) if coords[0] is not None else float(props.get("lon", 0.0))
        lat = float(coords[1]) if coords[1] is not None else float(props.get("lat", 0.0))
        depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else props.get("depth")
        
        mag = props.get("mag")
        severity = float(mag) if mag is not None else 0.0
        mag_type = str(props.get("magtype", "unk")).lower()
        
        event = CanonicalEvent(
            event_id=event_id,
            source_id=source_id,
            event_type="earthquake",
            timestamp=event_time,
            latitude=lat,
            longitude=lon,
            severity=severity,
            severity_unit=mag_type,
            raw_reference=f"https://www.seismicportal.eu/eventdetails.html?unid={props.get('unid', raw_id)}"
        )
        
        detail = EarthquakeDetail(
            event_id=event_id,
            depth_km=float(depth) if depth is not None else None,
            magnitude_type=mag_type,
            place=props.get("flynn_region") or props.get("place"),
            tsunami_flag=False
        )
        
        events.append(event)
        earthquakes.append(detail)
        
    return events, earthquakes

def parse_gdacs_xml(xml_content: str, source_id: str = "gdacs_realtime") -> Tuple[List[CanonicalEvent], List[EarthquakeDetail]]:
    """
    Parses GDACS RSS 2.0 XML payload using proper XML namespaces.
    Extracts coordinates from <georss:point> and regex-parses magnitude and depth from <gdacs:severity>.
    """
    events = []
    earthquakes = []
    
    root = ET.fromstring(xml_content) if isinstance(xml_content, str) else xml_content
    ns = {
        "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
        "georss": "http://www.georss.org/georss",
        "gdacs": "http://www.gdacs.org"
    }
    
    for item in root.findall(".//item"):
        ev_type = item.findtext("gdacs:eventtype", namespaces=ns)
        if ev_type != "EQ":
            continue
            
        raw_id = item.findtext("gdacs:eventid", namespaces=ns) or item.findtext("guid") or ""
        event_id = f"{source_id}_{raw_id}"
        
        from_date = item.findtext("gdacs:fromdate", namespaces=ns) or item.findtext("pubDate")
        event_time = None
        if from_date:
            try:
                # GDACS uses RFC-822 formatted timestamps e.g. 'Mon, 09 Nov 2026 04:52:51 GMT'
                event_time = datetime.strptime(from_date, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
            except Exception:
                pass
                
        # Parse coordinates from georss:point
        point = item.findtext("georss:point", namespaces=ns)
        lat, lon = 0.0, 0.0
        if point:
            pts = point.strip().split()
            if len(pts) == 2:
                lat, lon = float(pts[0]), float(pts[1])
                
        severity_str = item.findtext("gdacs:severity", namespaces=ns) or ""
        mag_m = re.search(r"Magnitude\s*([\d.]+)\s*M?", severity_str)
        severity = float(mag_m.group(1)) if mag_m else 0.0
        
        depth_m = re.search(r"Depth:\s*([\d.]+)\s*km", severity_str)
        depth = float(depth_m.group(1)) if depth_m else None
        
        is_tsunami = (item.findtext("gdacs:calculationtype", namespaces=ns) == "tsunami")
        title = item.findtext("title")
        
        event = CanonicalEvent(
            event_id=event_id,
            source_id=source_id,
            event_type="earthquake",
            timestamp=event_time,
            latitude=lat,
            longitude=lon,
            severity=severity,
            severity_unit="mw",
            raw_reference=item.findtext("link")
        )
        
        detail = EarthquakeDetail(
            event_id=event_id,
            depth_km=depth,
            magnitude_type="mw",
            place=title,
            tsunami_flag=is_tsunami
        )
        
        events.append(event)
        earthquakes.append(detail)
        
    return events, earthquakes
