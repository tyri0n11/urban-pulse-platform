"""Open-Meteo weather data source connector for Ho Chi Minh City.

Free API — no key required.
API docs: https://open-meteo.com/en/docs

Used by:
  - apps/batch/src/batch/jobs/rag_indexer.py  (indexes past 7 days into ChromaDB)
  - (future) ingestion orchestrator if real-time weather stream is needed
"""

import logging
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

# HCMC city centre (Bến Thành roundabout)
_LAT: float = 10.7757
_LON: float = 106.7009
_API_URL: str = "https://api.open-meteo.com/v1/forecast"

# WMO weather interpretation codes → Vietnamese description
_WMO_CODES: dict[int, str] = {
    0: "trời quang",
    1: "ít mây",
    2: "nửa nhiều mây",
    3: "u ám",
    45: "sương mù",
    48: "sương mù đóng băng",
    51: "mưa phùn nhẹ",
    53: "mưa phùn vừa",
    55: "mưa phùn dày",
    61: "mưa nhỏ",
    63: "mưa vừa",
    65: "mưa to",
    71: "tuyết nhẹ",
    73: "tuyết vừa",
    75: "tuyết nặng",
    80: "mưa rào nhẹ",
    81: "mưa rào vừa",
    82: "mưa rào mạnh",
    85: "mưa tuyết nhẹ",
    86: "mưa tuyết nặng",
    95: "giông bão",
    96: "giông bão kèm mưa đá",
    99: "giông bão mạnh kèm mưa đá lớn",
}

_WIND_DIRS: list[tuple[float, str]] = [
    (22.5, "Bắc"),
    (67.5, "Đông Bắc"),
    (112.5, "Đông"),
    (157.5, "Đông Nam"),
    (202.5, "Nam"),
    (247.5, "Tây Nam"),
    (292.5, "Tây"),
    (337.5, "Tây Bắc"),
    (360.0, "Bắc"),
]


def _wind_direction_name(degrees: float | None) -> str:
    if degrees is None:
        return "không rõ"
    for threshold, name in _WIND_DIRS:
        if degrees < threshold:
            return name
    return "Bắc"


def fetch_hourly_weather(past_days: int = 7) -> list[dict[str, Any]]:
    """Fetch hourly weather data for Ho Chi Minh City from Open-Meteo.

    Args:
        past_days: Number of past days to include (max 92 on free tier).
                   Default 7 aligns with the anomaly_events RAG lookback window.

    Returns:
        List of dicts with keys:
          hour_utc            datetime (UTC, tz-aware)
          temperature_c       float | None
          precipitation_mm    float | None
          rain_mm             float | None
          wind_speed_kmh      float | None
          wind_direction_deg  float | None
          cloud_cover_pct     float | None
          weather_code        int
          weather_desc        str   (Vietnamese)
    """
    params: dict[str, Any] = {
        "latitude": _LAT,
        "longitude": _LON,
        "hourly": ",".join([
            "temperature_2m",
            "precipitation",
            "rain",
            "wind_speed_10m",
            "wind_direction_10m",
            "cloud_cover",
            "weather_code",
        ]),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "UTC",
        "timeformat": "iso8601",
    }

    resp = requests.get(_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    hourly: dict[str, list[Any]] = data["hourly"]
    rows: list[dict[str, Any]] = []

    for i, time_str in enumerate(hourly["time"]):
        hour_utc = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
        raw_code = hourly["weather_code"][i]
        code = int(raw_code) if raw_code is not None else 0
        wind_deg = hourly["wind_direction_10m"][i]

        rows.append({
            "hour_utc": hour_utc,
            "temperature_c": hourly["temperature_2m"][i],
            "precipitation_mm": hourly["precipitation"][i],
            "rain_mm": hourly["rain"][i],
            "wind_speed_kmh": hourly["wind_speed_10m"][i],
            "wind_direction_deg": wind_deg,
            "wind_direction_name": _wind_direction_name(wind_deg),
            "cloud_cover_pct": hourly["cloud_cover"][i],
            "weather_code": code,
            "weather_desc": _WMO_CODES.get(code, f"mã WMO={code}"),
        })

    logger.info("openmeteo: fetched %d hourly records (past_days=%d)", len(rows), past_days)
    return rows
