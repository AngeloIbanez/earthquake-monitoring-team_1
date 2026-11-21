"""
Data models and canonical schemas for the Natural Hazard Monitoring System.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any

@dataclass
class CanonicalEvent:
    """Represents the standardized generic natural hazard event."""
    event_id: str
    source_id: str
    event_type: str = "earthquake"
    timestamp: Optional[datetime] = None
    latitude: Optional[float] = 0.0
    longitude: Optional[float] = 0.0
    severity: Optional[float] = 0.0
    severity_unit: str = "unk"
    raw_reference: Optional[str] = None
    updated_at: Optional[datetime] = None
    raw_payload: Optional[Dict[str, Any]] = None

    def validate(self) -> tuple[bool, Optional[str]]:
        """Validates mandatory attributes and physical bounds, returning (is_valid, rejection_reason)."""
        if not self.event_id or self.event_id.endswith("_"):
            return False, "missing_event_id"
        if self.timestamp is None:
            return False, "missing_timestamp"
        if self.latitude is None or self.longitude is None:
            return False, "missing_coordinates"
        try:
            lat = float(self.latitude)
            lon = float(self.longitude)
        except (TypeError, ValueError):
            return False, "invalid_coordinate_type"
        if not (-90.0 <= lat <= 90.0):
            return False, f"latitude_out_of_bounds ({lat})"
        if not (-180.0 <= lon <= 180.0):
            return False, f"longitude_out_of_bounds ({lon})"
        if self.severity is None:
            return False, "missing_severity"
        try:
            sev = float(self.severity)
        except (TypeError, ValueError):
            return False, "invalid_severity_type"
        if sev < -1.0 or sev > 10.0:
            return False, f"severity_out_of_bounds ({sev})"
        return True, None

    def is_valid(self) -> bool:
        """Validates that coordinates and severity fall within valid physical bounds."""
        valid, _ = self.validate()
        return valid

@dataclass
class EarthquakeDetail:
    """Represents earthquake-specific domain attributes."""
    event_id: str
    depth_km: Optional[float] = None
    magnitude_type: Optional[str] = None
    place: Optional[str] = None
    tsunami_flag: bool = False

@dataclass
class IngestionRunRecord:
    """Audit record for pipeline execution tracking."""
    source_id: str
    mode: str
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    status: str = "RUNNING"
    rows_in: int = 0
    rows_new: int = 0
    rows_rejected: int = 0
