"""HCMC weather fetcher with 15-minute in-process cache (Open-Meteo, no API key)."""

import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
_HCMC_LAT = 10.7757
_HCMC_LON = 106.7009
_CACHE_TTL = 900  # 15 min

_WMO_CODES: dict[int, str] = {
    0: "trời quang", 1: "ít mây", 2: "nửa nhiều mây", 3: "u ám",
    45: "sương mù", 48: "sương mù đóng băng",
    51: "mưa phùn nhẹ", 53: "mưa phùn vừa", 55: "mưa phùn dày",
    61: "mưa nhỏ", 63: "mưa vừa", 65: "mưa to",
    80: "mưa rào nhẹ", 81: "mưa rào vừa", 82: "mưa rào mạnh",
    95: "giông bão", 96: "giông bão kèm mưa đá", 99: "giông bão mạnh",
}

_cache: tuple[float, dict[str, Any]] | None = None


async def fetch_current_weather() -> dict[str, Any] | None:
    """Fetch current weather for HCMC with 15-min in-process cache."""
    global _cache
    now = time.monotonic()
    if _cache and (now - _cache[0]) < _CACHE_TTL:
        return _cache[1]

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                _OPENMETEO_URL,
                params={
                    "latitude": _HCMC_LAT,
                    "longitude": _HCMC_LON,
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
        logger.debug("weather: fetch failed — %s", exc)
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
    }
    _cache = (now, result)
    return result
