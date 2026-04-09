"""RAG indexer job — reads gold tables + anomaly history + weather, writes to ChromaDB.

Runs every hour via Prefect. Upsert-based so re-running is always safe.

Three index passes:
  1. anomaly_events   — last 7 days of anomalous windows from PostgreSQL
  2. traffic_patterns — full gold.traffic_hourly aggregated by (route, dow, hour)
                        (refreshed on retrain flow, not every hour)
  3. external_context — past 7 days of hourly weather for HCMC from Open-Meteo
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import httpx
import psycopg2

from rag.client import get_chroma_client
from rag.indexer import index_anomaly_events, index_traffic_patterns, index_weather_hours

logger = logging.getLogger(__name__)

_PG_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://urbanpulse:urbanpulse@postgres:5432/urbanpulse",
)

_ANOMALY_LOOKBACK_DAYS = 7


def _fetch_anomaly_events() -> list[dict[str, Any]]:
    """Fetch anomaly windows from PostgreSQL for the last N days."""
    since = datetime.now(timezone.utc) - timedelta(days=_ANOMALY_LOOKBACK_DAYS)
    conn = psycopg2.connect(_PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (o.route_id, o.window_start)
                    o.route_id,
                    o.window_start,
                    o.mean_heavy_ratio,
                    COALESCE(o.mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
                    COALESCE(o.max_severe_segments, 0)   AS max_severe_segments,
                    o.duration_zscore,
                    o.is_anomaly,
                    o.observation_count,
                    COALESCE(
                        CASE WHEN i.score_count > 0
                             THEN i.anomaly_count::float / i.score_count >= 0.5
                             ELSE i.iforest_anomaly
                        END,
                        false
                    ) AS iforest_anomaly,
                    COALESCE(
                        CASE WHEN i.score_count > 0
                             THEN i.both_count::float / i.score_count >= 0.5
                             ELSE i.both_anomaly
                        END,
                        false
                    ) AS both_anomaly
                FROM online_route_features o
                LEFT JOIN route_iforest_scores i
                    ON o.route_id = i.route_id
                   AND o.window_start = i.window_start
                WHERE o.window_start >= %s
                  AND (o.is_anomaly = true OR i.iforest_anomaly = true)
                ORDER BY o.route_id, o.window_start, o.updated_at DESC
                """,
                (since,),
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()

    logger.info("rag_indexer: fetched %d anomaly events from Postgres", len(rows))
    return rows


def _fetch_traffic_patterns(catalog: Any) -> list[dict[str, Any]]:
    """Aggregate gold.traffic_hourly by (route_id, dow, hour_of_day) for pattern indexing."""
    from pyiceberg.exceptions import NoSuchTableError
    try:
        gold_table = catalog.load_table("gold.traffic_hourly")
    except NoSuchTableError:
        logger.warning("rag_indexer: gold.traffic_hourly not found — skip pattern indexing")
        return []

    gold_arrow = gold_table.scan().to_arrow()
    if gold_arrow.num_rows == 0:
        return []

    con = duckdb.connect()
    con.register("gold", gold_arrow)
    result = con.execute(
        """
        SELECT
            route_id,
            CAST(EXTRACT(DOW  FROM hour_utc) AS INTEGER) AS dow,
            CAST(EXTRACT(HOUR FROM hour_utc) AS INTEGER) AS hour_of_day,
            AVG(avg_heavy_ratio)                          AS avg_heavy_ratio,
            AVG(COALESCE(avg_moderate_ratio, 0.0))        AS avg_moderate_ratio,
            AVG(avg_duration_minutes)                     AS avg_duration_minutes,
            COUNT(*)                                      AS observation_count
        FROM gold
        WHERE avg_heavy_ratio IS NOT NULL
        GROUP BY route_id, dow, hour_of_day
        ORDER BY route_id, dow, hour_of_day
        """
    ).fetchall()

    cols = ["route_id", "dow", "hour_of_day", "avg_heavy_ratio",
            "avg_moderate_ratio", "avg_duration_minutes", "observation_count"]
    rows = [dict(zip(cols, row)) for row in result]
    logger.info("rag_indexer: fetched %d traffic pattern rows from gold", len(rows))
    return rows


_OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
# HCMC city centre (Bến Thành)
_HCMC_LAT = 10.7757
_HCMC_LON = 106.7009

_WMO_CODES: dict[int, str] = {
    0: "trời quang", 1: "ít mây", 2: "nửa nhiều mây", 3: "u ám",
    45: "sương mù", 48: "sương mù đóng băng",
    51: "mưa phùn nhẹ", 53: "mưa phùn vừa", 55: "mưa phùn dày",
    61: "mưa nhỏ", 63: "mưa vừa", 65: "mưa to",
    71: "tuyết nhẹ", 73: "tuyết vừa", 75: "tuyết nặng",
    80: "mưa rào nhẹ", 81: "mưa rào vừa", 82: "mưa rào mạnh",
    85: "mưa tuyết nhẹ", 86: "mưa tuyết nặng",
    95: "giông bão", 96: "giông bão kèm mưa đá", 99: "giông bão mạnh",
}

_WIND_DIRS: list[tuple[float, str]] = [
    (22.5, "Bắc"), (67.5, "Đông Bắc"), (112.5, "Đông"), (157.5, "Đông Nam"),
    (202.5, "Nam"), (247.5, "Tây Nam"), (292.5, "Tây"), (337.5, "Tây Bắc"), (360.0, "Bắc"),
]


def _wind_dir_name(deg: float | None) -> str:
    if deg is None:
        return "không rõ"
    for threshold, name in _WIND_DIRS:
        if deg < threshold:
            return name
    return "Bắc"


def _fetch_weather_context(past_days: int = 7) -> list[dict[str, Any]]:
    """Fetch hourly weather for HCMC from Open-Meteo and convert to external_context rows.

    Uses httpx (already a batch dependency). Falls back silently on network error
    so a weather API outage never blocks the rest of the indexing pipeline.

    Returns rows suitable for index_weather_hours().
    """
    try:
        resp = httpx.get(
            _OPENMETEO_URL,
            params={
                "latitude": _HCMC_LAT,
                "longitude": _HCMC_LON,
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
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("rag_indexer: Open-Meteo fetch failed — %s", exc)
        return []

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
            "wind_direction_name": _wind_dir_name(wind_deg),
            "cloud_cover_pct": hourly["cloud_cover"][i],
            "weather_code": code,
            "weather_desc": _WMO_CODES.get(code, f"mã WMO={code}"),
        })

    logger.info("rag_indexer: fetched %d weather hours from Open-Meteo", len(rows))
    return rows


def run(catalog: Any, index_patterns: bool = False) -> dict[str, int]:
    """Main entry point called by Prefect task.

    Args:
        catalog:        PyIceberg catalog (passed from pipeline)
        index_patterns: if True, re-index traffic patterns (expensive full scan).
                        Set True on first run or after gold rebuild.
    """
    client = get_chroma_client()
    counts: dict[str, int] = {}

    # Always index recent anomaly events
    anomaly_rows = _fetch_anomaly_events()
    counts["anomaly_events"] = index_anomaly_events(client, anomaly_rows)

    # Pattern indexing is expensive — only when explicitly requested
    if index_patterns:
        pattern_rows = _fetch_traffic_patterns(catalog)
        counts["traffic_patterns"] = index_traffic_patterns(client, pattern_rows)

    # Always fetch and index weather context from Open-Meteo (free API, ~168 rows/run)
    weather_rows = _fetch_weather_context(past_days=7)
    counts["weather_hours"] = index_weather_hours(client, weather_rows)

    return counts
