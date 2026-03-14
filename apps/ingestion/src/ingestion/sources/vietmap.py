"""VietMap traffic route API source connector.

Response shape (relevant parts):
  paths[n].annotations.congestion → [{value: "low"|"moderate"|"heavy"|"severe"|"unknown", first, last}]
"""

from datetime import datetime, timezone

import requests

from urbanpulse_core.models.traffic import (
    CongestionMetrics,
    TrafficRouteObservation,
    TrafficRoutePath,
)

_BASE_URL = "https://maps.vietmap.vn/api/route/v3"
_VEHICLE = "car"
_ANNOTATIONS = "congestion"


def _calc_congestion(segments: list[dict]) -> CongestionMetrics:
    if not segments:
        return CongestionMetrics()
    total = len(segments)
    counts: dict[str, int] = {"heavy": 0, "moderate": 0, "low": 0, "severe": 0}
    for seg in segments:
        v = seg.get("value", "")
        if v in counts:
            counts[v] += 1
    return CongestionMetrics(
        heavy_ratio=round(counts["heavy"] / total, 3),
        moderate_ratio=round(counts["moderate"] / total, 3),
        low_ratio=round(counts["low"] / total, 3),
        severe_segments=counts["severe"],
        total_segments=total,
    )


def fetch_route(
    route_id: str,
    origin: str,
    destination: str,
    origin_anchor: list[float],
    destination_anchor: list[float],
    api_key: str,
) -> TrafficRouteObservation:
    """Fetch a single route from the VietMap API and return a typed observation."""
    url = (
        f"{_BASE_URL}"
        f"?apikey={api_key}"
        f"&point={origin_anchor[0]},{origin_anchor[1]}"
        f"&point={destination_anchor[0]},{destination_anchor[1]}"
        f"&points_encoded=false&vehicle={_VEHICLE}&annotations={_ANNOTATIONS}"
    )
    timestamp_utc = datetime.now(timezone.utc)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    paths: list[TrafficRoutePath] = []
    for p in data.get("paths", []):
        congestion_segs: list[dict] = p.get("annotations", {}).get("congestion", [])
        paths.append(
            TrafficRoutePath(
                duration_s=p.get("time", 0) / 1000.0,
                distance_m=p.get("distance", 0.0),
                congestion=_calc_congestion(congestion_segs) if congestion_segs else None,
            )
        )

    return TrafficRouteObservation(
        route_id=route_id,
        origin=origin,
        destination=destination,
        origin_anchor=origin_anchor,
        destination_anchor=destination_anchor,
        paths=paths,
        timestamp_utc=timestamp_utc,
        api_status="ok",
    )
