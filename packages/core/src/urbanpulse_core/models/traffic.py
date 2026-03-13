"""Domain model definitions for traffic route observations."""

from datetime import datetime

from pydantic import BaseModel


class CongestionMetrics(BaseModel):
    heavy_ratio: float = 0.0
    moderate_ratio: float = 0.0
    low_ratio: float = 0.0
    severe_segments: int = 0
    total_segments: int = 0


class TrafficRoutePath(BaseModel):
    duration_s: float
    distance_m: float
    congestion: CongestionMetrics | None = None


class TrafficRouteObservation(BaseModel):
    route_id: str
    origin: str
    destination: str
    origin_anchor: list[float]
    destination_anchor: list[float]
    fetched_at: datetime
    paths: list[TrafficRoutePath]
    api_status: str = "ok"
