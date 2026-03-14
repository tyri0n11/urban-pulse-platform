"""Domain model definitions for traffic route observations."""

from datetime import datetime

from pydantic import BaseModel


class CongestionMetrics(BaseModel):
    heavy_ratio: float = 0.0
    moderate_ratio: float = 0.0
    low_ratio: float = 0.0
    severe_segments: int = 0
    total_segments: int = 0


class TrafficRouteObservation(BaseModel):
    route_id: str
    origin: str
    destination: str
    distance_meters: float
    duration_ms: float
    duration_minutes: float
    congestion: CongestionMetrics | None = None
    timestamp_utc: datetime
    source: str = "ingestion.vietmap"
