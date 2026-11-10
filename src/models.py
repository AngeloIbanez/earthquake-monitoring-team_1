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
    latitude: float = 0.0
    longitude: float = 0.0
    severity: float = 0.0
    severity_unit: str = "unk"
    raw_reference: Optional[str] = None
    updated_at: Optional[datetime] = None

    def is_valid(self) -> bool:
        """Validates that coordinates and severity fall within valid physical bounds."""
        if not (-90.0 <= self.latitude <= 90.0):
            return False
        if not (-180.0 <= self.longitude <= 180.0):
            return False
        if self.timestamp is None:
            return False
        if self.severity is None or self.severity < -1.0 or self.severity > 10.0:
            return False
        return True

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
