"""Batch job: aggregates weather silver hourly data into gold daily Iceberg table.

Runs hourly. Overwrites the current day's gold partition with fresh aggregates
so the row accumulates through the day as more hourly silver rows arrive.
"""

import logging
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
)

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

logger = logging.getLogger(__name__)

_SILVER_TABLE = "silver.weather_hourly"
_GOLD_NS = "gold"
_GOLD_TABLE = "gold.weather_daily"

_GOLD_SCHEMA = Schema(
    NestedField(1, "location_id", StringType(), required=True),
    NestedField(2, "date_utc", DateType(), required=True),
    NestedField(3, "min_temp_c", DoubleType()),
    NestedField(4, "max_temp_c", DoubleType()),
    NestedField(5, "avg_temp_c", DoubleType()),
    NestedField(6, "total_precipitation_mm", DoubleType()),
    NestedField(7, "total_rain_mm", DoubleType()),
    NestedField(8, "max_wind_speed_kmh", DoubleType()),
    NestedField(9, "dominant_weather_code", IntegerType()),
    NestedField(10, "dominant_weather_desc", StringType()),
    NestedField(11, "rainy_hours", IntegerType()),
    NestedField(12, "observation_count", IntegerType()),
)

_GOLD_ARROW_SCHEMA = pa.schema([
    pa.field("location_id", pa.string(), nullable=False),
    pa.field("date_utc", pa.date32(), nullable=False),
    pa.field("min_temp_c", pa.float64()),
    pa.field("max_temp_c", pa.float64()),
    pa.field("avg_temp_c", pa.float64()),
    pa.field("total_precipitation_mm", pa.float64()),
    pa.field("total_rain_mm", pa.float64()),
    pa.field("max_wind_speed_kmh", pa.float64()),
    pa.field("dominant_weather_code", pa.int32()),
    pa.field("dominant_weather_desc", pa.string()),
    pa.field("rainy_hours", pa.int32()),
    pa.field("observation_count", pa.int32()),
])

# Two-step aggregation:
# 1. daily_stats: scalar aggregates per (location_id, date_utc)
# 2. dominant_code: modal weather_code per day (using window function for mode)
_AGG_SQL = """
WITH daily_stats AS (
    SELECT
        location_id,
        CAST(DATE_TRUNC('day', hour_utc) AS DATE)                    AS date_utc,
        MIN(temperature_c)                                            AS min_temp_c,
        MAX(temperature_c)                                            AS max_temp_c,
        AVG(temperature_c)                                            AS avg_temp_c,
        SUM(COALESCE(precipitation_mm, 0.0))                         AS total_precipitation_mm,
        SUM(COALESCE(rain_mm, 0.0))                                   AS total_rain_mm,
        MAX(wind_speed_kmh)                                           AS max_wind_speed_kmh,
        CAST(SUM(CASE WHEN rain_mm > 0 THEN 1 ELSE 0 END) AS INTEGER) AS rainy_hours,
        CAST(COUNT(*) AS INTEGER)                                     AS observation_count
    FROM silver
    WHERE hour_utc IS NOT NULL
    GROUP BY 1, 2
),
code_counts AS (
    SELECT
        location_id,
        CAST(DATE_TRUNC('day', hour_utc) AS DATE) AS date_utc,
        weather_code,
        MAX(weather_desc)                          AS weather_desc,
        COUNT(*)                                   AS cnt
    FROM silver
    WHERE hour_utc IS NOT NULL
    GROUP BY 1, 2, 3
),
dominant_code AS (
    SELECT
        location_id,
        date_utc,
        weather_code                                            AS dominant_weather_code,
        weather_desc                                            AS dominant_weather_desc,
        ROW_NUMBER() OVER (
            PARTITION BY location_id, date_utc
            ORDER BY cnt DESC, weather_code
        ) AS rn
    FROM code_counts
)
SELECT
    s.location_id,
    s.date_utc,
    s.min_temp_c,
    s.max_temp_c,
    s.avg_temp_c,
    s.total_precipitation_mm,
    s.total_rain_mm,
    s.max_wind_speed_kmh,
    CAST(d.dominant_weather_code AS INTEGER)  AS dominant_weather_code,
    d.dominant_weather_desc,
    s.rainy_hours,
    s.observation_count
FROM daily_stats s
JOIN dominant_code d
  ON s.location_id = d.location_id AND s.date_utc = d.date_utc AND d.rn = 1
"""


def _get_catalog() -> Catalog:
    return get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )


def _ensure_gold_table(catalog: Catalog) -> Table:
    try:
        catalog.load_namespace_properties(_GOLD_NS)
    except NoSuchNamespaceError:
        catalog.create_namespace(_GOLD_NS)
        logger.info("Created Iceberg namespace: %s", _GOLD_NS)

    try:
        return catalog.load_table(_GOLD_TABLE)
    except NoSuchTableError:
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=MonthTransform(), name="date_utc_month"
            ),
        )
        table = catalog.create_table(
            _GOLD_TABLE,
            schema=_GOLD_SCHEMA,
            partition_spec=partition_spec,
        )
        logger.info("Created Iceberg table: %s", _GOLD_TABLE)
        return table


def _scan_to_table(scan_result: object) -> pa.Table:
    if isinstance(scan_result, pa.Table):
        return scan_result
    return scan_result.read_all()  # type: ignore[union-attr]


def _aggregate_silver(silver_arrow: pa.Table) -> pa.Table:
    import duckdb
    con = duckdb.connect()
    con.register("silver", silver_arrow)
    result = _scan_to_table(con.execute(_AGG_SQL).arrow())
    return result.cast(_GOLD_ARROW_SCHEMA)


def run(catalog: Catalog | None = None) -> int:
    """Aggregate today's silver weather records into gold daily table.

    Re-runs are idempotent — overwrites the current day's partition.
    Returns the number of rows written (typically 1 per location per day).
    """
    if catalog is None:
        catalog = _get_catalog()

    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    try:
        silver_table = catalog.load_table(_SILVER_TABLE)
    except Exception as exc:
        logger.warning("silver_to_gold_weather: silver table not found — %s", exc)
        return 0

    silver_arrow = _scan_to_table(
        silver_table.scan(
            row_filter=And(
                GreaterThanOrEqual("hour_utc", day_start.isoformat()),
                LessThan("hour_utc", day_end.isoformat()),
            ),
        ).to_arrow()
    )

    if silver_arrow.num_rows == 0:
        logger.info("silver_to_gold_weather: no silver data for today %s — skipping", day_start.date())
        return 0

    gold_arrow = _aggregate_silver(silver_arrow)
    row_count = gold_arrow.num_rows

    gold_table = _ensure_gold_table(catalog)
    gold_table.overwrite(
        gold_arrow,
        overwrite_filter=And(
            GreaterThanOrEqual("date_utc", day_start.date().isoformat()),
            LessThan("date_utc", day_end.date().isoformat()),
        ),
    )

    print(f"silver_to_gold_weather: upserted {row_count} rows for {day_start.date()}")
    logger.info(
        "silver_to_gold_weather: upserted %d rows for %s", row_count, day_start.date()
    )
    return row_count


def bootstrap(catalog: Catalog | None = None) -> int:
    """Full re-aggregate: scan ALL weather silver data into gold daily table.

    Overwrites the entire gold.weather_daily table. Idempotent.
    Returns the total number of rows written.
    """
    if catalog is None:
        catalog = _get_catalog()

    try:
        silver_table = catalog.load_table(_SILVER_TABLE)
    except Exception as exc:
        logger.warning("silver_to_gold_weather bootstrap: silver table not found — %s", exc)
        return 0

    silver_arrow = _scan_to_table(silver_table.scan().to_arrow())

    if silver_arrow.num_rows == 0:
        logger.info("silver_to_gold_weather bootstrap: no silver data — skipping")
        return 0

    gold_arrow = _aggregate_silver(silver_arrow)
    row_count = gold_arrow.num_rows

    gold_table = _ensure_gold_table(catalog)
    gold_table.overwrite(gold_arrow)

    logger.info("silver_to_gold_weather bootstrap: wrote %d rows", row_count)
    return row_count
