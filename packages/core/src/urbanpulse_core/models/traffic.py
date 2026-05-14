"""Domain model definitions for traffic route observations."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class CongestionMetrics(BaseModel):
    heavy_ratio: float = 0.0
    moderate_ratio: float = 0.0
    low_ratio: float = 0.0
    severe_segments: int = 0
    total_segments: int = 0


class CongestionSegment(BaseModel):
    value: str  # "low" | "moderate" | "heavy" | "severe"
    first: int
    last: int


class TrafficRouteObservation(BaseModel):
    route_id: str
    origin: str
    destination: str
    distance_meters: float
    duration_ms: float
    duration_minutes: float
    congestion: CongestionMetrics | None = None
    geometry: list[list[float]] | None = None  # [[lng, lat], ...] GeoJSON order
    congestion_segments: list[CongestionSegment] | None = None
    timestamp_utc: datetime
    source: str = "ingestion.vietmap"


class VietmapRawEnvelope(BaseModel):
    """Thin wrapper around a raw VietMap API response preserving route identity."""

    route_id: str
    origin: str
    destination: str
    polled_at_ms: int
    timestamp_utc: datetime
    raw_response: dict[str, Any]


def _calc_congestion(segments: list[dict[str, object]]) -> CongestionMetrics:
    if not segments:
        return CongestionMetrics()
    total = len(segments)
    counts: dict[str, int] = {"heavy": 0, "moderate": 0, "low": 0, "severe": 0}
    for seg in segments:
        v = seg.get("value", "")
        if isinstance(v, str) and v in counts:
            counts[v] += 1
    return CongestionMetrics(
        heavy_ratio=round(counts["heavy"] / total, 3),
        moderate_ratio=round(counts["moderate"] / total, 3),
        low_ratio=round(counts["low"] / total, 3),
        severe_segments=counts["severe"],
        total_segments=total,
    )


def parse_vietmap_response(envelope: VietmapRawEnvelope) -> TrafficRouteObservation:
    """Convert a raw VietMap API envelope to a typed TrafficRouteObservation."""
    first: dict[str, Any] = (envelope.raw_response.get("paths") or [{}])[0]
    duration_ms = float(first.get("time", 0.0))
    congestion_segs: list[dict[str, object]] = (
        first.get("annotations", {}).get("congestion", [])  # type: ignore[union-attr]
    )
    points_geom: dict[str, Any] = first.get("points", {})  # type: ignore[assignment]
    geometry: list[list[float]] | None = points_geom.get("coordinates")  # type: ignore[assignment]
    segments: list[CongestionSegment] | None = (
        [CongestionSegment(**s) for s in congestion_segs]  # type: ignore[arg-type]
        if congestion_segs else None
    )
    return TrafficRouteObservation(
        route_id=envelope.route_id,
        origin=envelope.origin,
        destination=envelope.destination,
        distance_meters=float(first.get("distance", 0.0)),
        duration_ms=duration_ms,
        duration_minutes=round(duration_ms / 60000, 1),
        congestion=_calc_congestion(congestion_segs) if congestion_segs else None,
        geometry=geometry,
        congestion_segments=segments,
        timestamp_utc=envelope.timestamp_utc,
    )
