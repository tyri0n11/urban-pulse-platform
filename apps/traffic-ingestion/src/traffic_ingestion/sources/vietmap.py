"""VietMap traffic route API source connector.

Response shape (relevant parts):
  paths[n].annotations.congestion → [{value: "low"|"moderate"|"heavy"|"severe"|"unknown", first, last}]
"""

import time
from datetime import datetime, timezone

import requests

from urbanpulse_core.models.traffic import VietmapRawEnvelope

_BASE_URL = "https://maps.vietmap.vn/api/route/v3"
_VEHICLE = "car"
_ANNOTATIONS = "congestion"


def fetch_route_raw(
    route_id: str,
    origin: str,
    destination: str,
    origin_anchor: list[float],
    destination_anchor: list[float],
    api_key: str,
) -> tuple[VietmapRawEnvelope, int]:
    """Fetch a single route and return the raw API response envelope + poll timestamp.

    Returns (envelope, polled_at_ms) where polled_at_ms is recorded before the
    HTTP call so downstream consumers can measure true end-to-end pipeline latency.
    """
    url = (
        f"{_BASE_URL}"
        f"?apikey={api_key}"
        f"&point={origin_anchor[0]},{origin_anchor[1]}"
        f"&point={destination_anchor[0]},{destination_anchor[1]}"
        f"&points_encoded=false&vehicle={_VEHICLE}&annotations={_ANNOTATIONS}"
    )
    polled_at_ms = int(time.time() * 1000)
    timestamp_utc = datetime.now(timezone.utc)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    return VietmapRawEnvelope(
        route_id=route_id,
        origin=origin,
        destination=destination,
        polled_at_ms=polled_at_ms,
        timestamp_utc=timestamp_utc,
        raw_response=resp.json(),
    ), polled_at_ms
