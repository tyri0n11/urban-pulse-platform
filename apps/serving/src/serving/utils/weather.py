"""HCMC weather fetcher with 15-minute in-process cache (Open-Meteo, no API key).

Supports per-zone fetching: each of the 6 HCMC zones has its own coordinates
(matching the weather-ingestion service). Explain endpoints pass the route's
origin zone_id so weather reflects the actual local conditions, not just the
city centre.
"""

import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
_CACHE_TTL = 900  # 15 min

# Zone coordinates — mirrors apps/weather-ingestion/src/weather_ingestion/sources/openmeteo.py
_ZONE_COORDS: dict[str, tuple[float, float, str]] = {
    "zone1_urban_core":          (10.7755334, 106.6989747, "TP.HCM — Quận 1/3/4/5"),
    "zone2_eastern_innovation":  (10.8666897, 106.7793007, "Thủ Đức / Dĩ An / Thuận An"),
    "zone3_northern_industrial": (11.1031199, 106.5495773, "Bình Dương (KCN phía Bắc)"),
    "zone4_southern_port":       (10.7629962, 106.7722453, "Quận 7 / Nhà Bè / Cát Lái"),
    "zone5_western_periurban":   (10.6866869, 106.5773591, "Bình Chánh / Hóc Môn / Củ Chi"),
    "zone6_southern_coastal":    (10.5842275, 107.0367637, "Bà Rịa – Vũng Tàu (Cái Mép)"),
}

# Fallback: city centre (Bến Thành, Quận 1)
_DEFAULT_LAT = 10.7757
_DEFAULT_LON = 106.7009

_WMO_CODES: dict[int, str] = {
    0: "trời quang", 1: "ít mây", 2: "nửa nhiều mây", 3: "u ám",
    45: "sương mù", 48: "sương mù đóng băng",
    51: "mưa phùn nhẹ", 53: "mưa phùn vừa", 55: "mưa phùn dày",
    61: "mưa nhỏ", 63: "mưa vừa", 65: "mưa to",
    80: "mưa rào nhẹ", 81: "mưa rào vừa", 82: "mưa rào mạnh",
    95: "giông bão", 96: "giông bão kèm mưa đá", 99: "giông bão mạnh",
}

# Cache keyed by zone_id (or "__city__" for the generic city-centre fetch)
_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _origin_zone(route_id: str) -> str | None:
    """Extract the origin zone_id from a route_id like zone3_foo_to_zone6_bar."""
    parts = route_id.split("_to_")
    if len(parts) == 2:
        return parts[0]
    return None


async def fetch_current_weather(zone_id: str | None = None) -> dict[str, Any] | None:
    """Fetch current weather for a specific zone (or city centre if zone_id is None).

    Results are cached 15 min per zone so repeated calls within a request cycle
    are free.
    """
    cache_key = zone_id or "__city__"
    now = time.monotonic()
    cached = _cache.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    if zone_id and zone_id in _ZONE_COORDS:
        lat, lon, location_name = _ZONE_COORDS[zone_id]
    else:
        lat, lon = _DEFAULT_LAT, _DEFAULT_LON
        location_name = "TP.HCM (trung tâm)"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                _OPENMETEO_URL,
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "current": ",".join([
                        "temperature_2m", "precipitation", "rain",
                        "wind_speed_10m", "wind_direction_10m",
                        "cloud_cover", "weather_code",
                    ]),
                    "timezone": "UTC",
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.debug("weather: fetch failed zone=%s — %s", cache_key, exc)
        return None

    cur = data.get("current", {})
    code = int(cur.get("weather_code") or 0)
    result: dict[str, Any] = {
        "temperature_c": cur.get("temperature_2m"),
        "precipitation_mm": cur.get("precipitation"),
        "rain_mm": cur.get("rain"),
        "wind_speed_kmh": cur.get("wind_speed_10m"),
        "cloud_cover_pct": cur.get("cloud_cover"),
        "weather_desc": _WMO_CODES.get(code, f"mã WMO={code}"),
        "observed_at": cur.get("time", ""),
        "location": location_name,
        "zone_id": zone_id,
    }
    _cache[cache_key] = (now, result)
    return result
