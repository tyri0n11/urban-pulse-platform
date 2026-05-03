"""VietMap traffic route API source connector."""

import time
from datetime import datetime, timezone
from typing import Any

import requests

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
) -> tuple[dict[str, Any], int, datetime]:
    """Fetch a single route and return the raw API response, poll timestamp, and UTC time."""
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
    return resp.json(), polled_at_ms, timestamp_utc
