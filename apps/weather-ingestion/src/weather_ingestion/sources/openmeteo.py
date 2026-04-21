"""Typed Open-Meteo fetcher returning WeatherObservation objects per zone.

Entry points:
  fetch_all_zones()           → list[WeatherObservation] for all 6 HCMC zones (current hour)
  fetch_archive_all_zones()   → list[WeatherObservation] for all zones over a date range
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from urbanpulse_core.models.weather import WeatherObservation

logger = logging.getLogger(__name__)

_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# 6 HCMC metropolitan zones — coordinates from zones.json
ZONES: dict[str, tuple[float, float, str]] = {
    "zone1_urban_core":          (10.7755334, 106.6989747, "Urban Core"),
    "zone2_eastern_innovation":  (10.8666897, 106.7793007, "Eastern Innovation"),
    "zone3_northern_industrial": (11.1031199, 106.5495773, "Northern Industrial"),
    "zone4_southern_port":       (10.7629962, 106.7722453, "Southern Port"),
    "zone5_western_periurban":   (10.6866869, 106.5773591, "Western Peri-urban"),
    "zone6_southern_coastal":    (10.5842275, 107.0367637, "Southern Coastal"),
}

_HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "rain",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
    "weather_code",
]

_WMO_CODES: dict[int, str] = {
    0: "trời quang", 1: "ít mây", 2: "nửa nhiều mây", 3: "u ám",
    45: "sương mù", 48: "sương mù đóng băng",
    51: "mưa phùn nhẹ", 53: "mưa phùn vừa", 55: "mưa phùn dày",
    61: "mưa nhỏ", 63: "mưa vừa", 65: "mưa to",
    71: "tuyết nhẹ", 73: "tuyết vừa", 75: "tuyết nặng",
    80: "mưa rào nhẹ", 81: "mưa rào vừa", 82: "mưa rào mạnh",
    85: "mưa tuyết nhẹ", 86: "mưa tuyết nặng",
    95: "giông bão", 96: "giông bão kèm mưa đá", 99: "giông bão mạnh kèm mưa đá lớn",
}

_WIND_DIRS: list[tuple[float, str]] = [
    (22.5, "Bắc"), (67.5, "Đông Bắc"), (112.5, "Đông"), (157.5, "Đông Nam"),
    (202.5, "Nam"), (247.5, "Tây Nam"), (292.5, "Tây"), (337.5, "Tây Bắc"), (360.0, "Bắc"),
]


def _wind_direction_name(degrees: float | None) -> str:
    if degrees is None:
        return ""
    for threshold, name in _WIND_DIRS:
        if degrees < threshold:
            return name
    return "Bắc"


def _parse_hourly(data: dict[str, Any], location_id: str) -> list[WeatherObservation]:
    hourly: dict[str, list[Any]] = data["hourly"]
    obs_list: list[WeatherObservation] = []
    for i, time_str in enumerate(hourly["time"]):
        hour_utc = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
        hour_utc = hour_utc.replace(minute=0, second=0, microsecond=0)
        raw_code = hourly["weather_code"][i]
        code = int(raw_code) if raw_code is not None else 0
        wind_deg = hourly["wind_direction_10m"][i]
        obs_list.append(WeatherObservation(
            location_id=location_id,
            hour_utc=hour_utc,
            temperature_c=hourly["temperature_2m"][i],
            precipitation_mm=hourly["precipitation"][i],
            rain_mm=hourly["rain"][i],
            wind_speed_kmh=hourly["wind_speed_10m"][i],
            wind_direction_deg=wind_deg,
            wind_direction_name=_wind_direction_name(wind_deg),
            cloud_cover_pct=hourly["cloud_cover"][i],
            weather_code=code,
            weather_desc=_WMO_CODES.get(code, f"mã WMO={code}"),
        ))
    return obs_list


def _fetch_current_hour_for_zone(
    location_id: str, lat: float, lon: float
) -> WeatherObservation | None:
    """Fetch the most recently completed hour for a single zone."""
    try:
        resp = requests.get(
            _FORECAST_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": ",".join(_HOURLY_VARS),
                "past_days": 1,
                "forecast_days": 1,
                "timezone": "UTC",
                "timeformat": "iso8601",
            },
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.error("openmeteo: forecast fetch failed zone=%s — %s", location_id, exc)
        return None

    observations = _parse_hourly(resp.json(), location_id)
    if not observations:
        return None

    now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    completed = [o for o in observations if o.hour_utc < now_hour and o.temperature_c is not None]
    if not completed:
        completed = [o for o in observations if o.temperature_c is not None]
    if not completed:
        return None

    return max(completed, key=lambda o: o.hour_utc)


def fetch_all_zones() -> list[WeatherObservation]:
    """Fetch the most recently completed hour for all 6 HCMC zones.

    Returns one WeatherObservation per zone (up to 6). Zones that fail are
    skipped silently so a single API error does not block the others.
    """
    results: list[WeatherObservation] = []
    for zone_id, (lat, lon, name) in ZONES.items():
        obs = _fetch_current_hour_for_zone(zone_id, lat, lon)
        if obs is not None:
            results.append(obs)
            logger.info(
                "openmeteo: %s (%s) %s %.1f°C",
                zone_id, name, obs.weather_desc, obs.temperature_c or 0.0,
            )
        else:
            logger.warning("openmeteo: no data for zone=%s", zone_id)
    return results


def fetch_archive_all_zones(start: date, end: date) -> list[WeatherObservation]:
    """Fetch hourly archive weather for all 6 zones over [start, end] inclusive.

    Fetches each zone in 30-day chunks. Total rows = 6 x days x 24.
    """
    all_obs: list[WeatherObservation] = []
    for zone_id, (lat, lon, name) in ZONES.items():
        zone_obs = _fetch_archive_zone(zone_id, lat, lon, start, end)
        all_obs.extend(zone_obs)
        logger.info(
            "openmeteo: archive zone=%s (%s) → %d records", zone_id, name, len(zone_obs)
        )
    return all_obs


def _fetch_archive_zone(
    location_id: str, lat: float, lon: float, start: date, end: date
) -> list[WeatherObservation]:
    """Fetch archive data for a single zone in 30-day chunks."""
    all_obs: list[WeatherObservation] = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(end, chunk_start + timedelta(days=29))
        try:
            resp = requests.get(
                _ARCHIVE_URL,
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    "hourly": ",".join(_HOURLY_VARS),
                    "timezone": "UTC",
                    "timeformat": "iso8601",
                },
                timeout=60,
            )
            resp.raise_for_status()
            all_obs.extend(_parse_hourly(resp.json(), location_id))
        except Exception as exc:
            logger.error(
                "openmeteo: archive failed zone=%s %s→%s — %s",
                location_id, chunk_start, chunk_end, exc,
            )
        chunk_start = chunk_end + timedelta(days=1)
    return all_obs
