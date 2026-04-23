"""Bootstrap weather historical data: Open-Meteo archive → MinIO bronze → silver → gold.

Writes Parquet files directly to MinIO bronze (bypassing Kafka) to avoid flooding
the streaming consumer with thousands of backfill messages.

Fetches from the Open-Meteo archive API inline for all 6 HCMC zones.
"""

import io
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from urbanpulse_core.config import settings
from urbanpulse_core.models.weather import WeatherObservation

logger = logging.getLogger(__name__)

_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_BRONZE_PREFIX = "urban-pulse/bronze/weather-hcmc-bronze"

# 6 HCMC zones — mirrors zones.json
_ZONES: dict[str, tuple[float, float]] = {
    "zone1_urban_core":          (10.7755334, 106.6989747),
    "zone2_eastern_innovation":  (10.8666897, 106.7793007),
    "zone3_northern_industrial": (11.1031199, 106.5495773),
    "zone4_southern_port":       (10.7629962, 106.7722453),
    "zone5_western_periurban":   (10.6866869, 106.5773591),
    "zone6_southern_coastal":    (10.5842275, 107.0367637),
}

_HOURLY_VARS = [
    "temperature_2m", "precipitation", "rain",
    "wind_speed_10m", "wind_direction_10m",
    "cloud_cover", "weather_code",
]

_WMO_CODES: dict[int, str] = {
    0: "trời quang", 1: "ít mây", 2: "nửa nhiều mây", 3: "u ám",
    45: "sương mù", 48: "sương mù đóng băng",
    51: "mưa phùn nhẹ", 53: "mưa phùn vừa", 55: "mưa phùn dày",
    61: "mưa nhỏ", 63: "mưa vừa", 65: "mưa to",
    80: "mưa rào nhẹ", 81: "mưa rào vừa", 82: "mưa rào mạnh",
    95: "giông bão", 96: "giông bão kèm mưa đá", 99: "giông bão mạnh",
}

_WIND_DIRS: list[tuple[float, str]] = [
    (22.5, "Bắc"), (67.5, "Đông Bắc"), (112.5, "Đông"), (157.5, "Đông Nam"),
    (202.5, "Nam"), (247.5, "Tây Nam"), (292.5, "Tây"), (337.5, "Tây Bắc"), (360.0, "Bắc"),
]

_ARROW_SCHEMA = pa.schema([
    pa.field("location_id", pa.string(), nullable=False),
    pa.field("hour_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("temperature_c", pa.float64()),
    pa.field("precipitation_mm", pa.float64()),
    pa.field("rain_mm", pa.float64()),
    pa.field("wind_speed_kmh", pa.float64()),
    pa.field("wind_direction_deg", pa.float64()),
    pa.field("wind_direction_name", pa.string()),
    pa.field("cloud_cover_pct", pa.float64()),
    pa.field("weather_code", pa.int32()),
    pa.field("weather_desc", pa.string()),
    pa.field("source", pa.string()),
])


def _wind_direction_name(degrees: float | None) -> str:
    if degrees is None:
        return ""
    for threshold, name in _WIND_DIRS:
        if degrees < threshold:
            return name
    return "Bắc"


def _fetch_archive_zone(
    location_id: str, lat: float, lon: float, start: date, end: date
) -> list[WeatherObservation]:
    """Fetch archive data for one zone in 30-day chunks."""
    obs: list[WeatherObservation] = []
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
            hourly = resp.json()["hourly"]
            for i, time_str in enumerate(hourly["time"]):
                hour_utc = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
                hour_utc = hour_utc.replace(minute=0, second=0, microsecond=0)
                raw_code = hourly["weather_code"][i]
                code = int(raw_code) if raw_code is not None else 0
                wind_deg = hourly["wind_direction_10m"][i]
                obs.append(WeatherObservation(
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
            logger.info(
                "weather_bootstrap: zone=%s %s→%s fetched %d records",
                location_id, chunk_start, chunk_end, len(hourly["time"]),
            )
        except Exception as exc:
            logger.error(
                "weather_bootstrap: archive failed zone=%s %s→%s — %s",
                location_id, chunk_start, chunk_end, exc,
            )
        chunk_start = chunk_end + timedelta(days=1)
    return obs


def _fetch_archive_all_zones(start: date, end: date) -> list[WeatherObservation]:
    """Fetch archive weather for all 6 zones. Total rows = 6 x days x 24."""
    all_obs: list[WeatherObservation] = []
    for zone_id, (lat, lon) in _ZONES.items():
        zone_obs = _fetch_archive_zone(zone_id, lat, lon, start, end)
        all_obs.extend(zone_obs)
        logger.info("weather_bootstrap: zone=%s → %d records", zone_id, len(zone_obs))
    return all_obs


def _get_s3fs() -> "s3fs.S3FileSystem":  # type: ignore[name-defined]  # noqa: F821
    import s3fs
    return s3fs.S3FileSystem(
        endpoint_url=f"http://{settings.minio_endpoint}",
        key=settings.minio_access_key,
        secret=settings.minio_secret_key,
    )


def _obs_list_to_arrow(obs_list: list[Any]) -> pa.Table:
    return pa.table(
        {
            "location_id": [o.location_id for o in obs_list],
            "hour_utc": pa.array([o.hour_utc for o in obs_list], type=pa.timestamp("us", tz="UTC")),
            "temperature_c": [o.temperature_c for o in obs_list],
            "precipitation_mm": [o.precipitation_mm for o in obs_list],
            "rain_mm": [o.rain_mm for o in obs_list],
            "wind_speed_kmh": [o.wind_speed_kmh for o in obs_list],
            "wind_direction_deg": [o.wind_direction_deg for o in obs_list],
            "wind_direction_name": [o.wind_direction_name for o in obs_list],
            "cloud_cover_pct": [o.cloud_cover_pct for o in obs_list],
            "weather_code": pa.array([o.weather_code for o in obs_list], type=pa.int32()),
            "weather_desc": [o.weather_desc for o in obs_list],
            "source": [o.source for o in obs_list],
        },
        schema=_ARROW_SCHEMA,
    )


def write_bronze(obs_list: list[Any]) -> int:
    """Write WeatherObservation records as partitioned Parquet files to MinIO bronze.

    Groups by (year, month, day, hour) — all zones in one partition file per hour.
    Filename is deterministic (bootstrap.parquet) so re-runs overwrite, not duplicate.

    Returns the total number of records written.
    """
    if not obs_list:
        return 0

    fs = _get_s3fs()
    buckets: dict[tuple[int, int, int, int], list[Any]] = defaultdict(list)
    for obs in obs_list:
        h = obs.hour_utc
        buckets[(h.year, h.month, h.day, h.hour)].append(obs)

    total = 0
    for (yr, mo, dy, hr), chunk in sorted(buckets.items()):
        path = (
            f"{_BRONZE_PREFIX}/"
            f"year={yr:04d}/month={mo:02d}/day={dy:02d}/hour={hr:02d}/"
            f"bootstrap.parquet"
        )
        table = _obs_list_to_arrow(chunk)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        with fs.open(path, "wb") as f:
            f.write(buf.getvalue())
        total += len(chunk)

    logger.info(
        "weather_bootstrap: wrote %d records to MinIO bronze (%d partitions)",
        total, len(buckets),
    )
    return total


def backfill_to_bronze(from_date: str, to_date: str) -> int:
    """Fetch weather for all 6 zones from Open-Meteo archive and write to MinIO bronze.

    Args:
        from_date: ISO date string, e.g. "2026-01-01"
        to_date:   ISO date string, e.g. "2026-04-21"

    Returns the total number of records written (6 zones x hours x days).
    """
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)

    logger.info(
        "weather_bootstrap: fetching archive for %d zones %s → %s",
        len(_ZONES), start, end,
    )
    obs_list = _fetch_archive_all_zones(start, end)

    if not obs_list:
        logger.warning("weather_bootstrap: no data returned from archive API")
        return 0

    return write_bronze(obs_list)
