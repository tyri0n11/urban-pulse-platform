"""Batch job: aggregates weather silver hourly data into gold.weather_hourly.

Averages all 6 HCMC zone observations into a single city-level row per hour.
Consistent with gold.traffic_hourly (same hourly granularity).

Schema: hour_utc, avg_temperature_c, avg_precipitation_mm, avg_rain_mm,
        max_wind_speed_kmh, dominant_weather_code, dominant_weather_desc,
        rainy_zones (count of zones with rain), zone_count
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
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    TimestamptzType,
    StringType,
)

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

logger = logging.getLogger(__name__)

_SILVER_TABLE = "silver.weather_hourly"
_GOLD_NS = "gold"
_GOLD_TABLE = "gold.weather_hourly"

_GOLD_SCHEMA = Schema(
    NestedField(1, "hour_utc", TimestamptzType(), required=True),
    NestedField(2, "avg_temperature_c", DoubleType()),
    NestedField(3, "avg_precipitation_mm", DoubleType()),
    NestedField(4, "avg_rain_mm", DoubleType()),
    NestedField(5, "max_wind_speed_kmh", DoubleType()),
    NestedField(6, "dominant_weather_code", IntegerType()),
    NestedField(7, "dominant_weather_desc", StringType()),
    NestedField(8, "rainy_zones", IntegerType()),
    NestedField(9, "zone_count", IntegerType()),
)

_GOLD_ARROW_SCHEMA = pa.schema([
    pa.field("hour_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("avg_temperature_c", pa.float64()),
    pa.field("avg_precipitation_mm", pa.float64()),
    pa.field("avg_rain_mm", pa.float64()),
    pa.field("max_wind_speed_kmh", pa.float64()),
    pa.field("dominant_weather_code", pa.int32()),
    pa.field("dominant_weather_desc", pa.string()),
    pa.field("rainy_zones", pa.int32()),
    pa.field("zone_count", pa.int32()),
])

# Average 6 zones → 1 city-level row per hour.
# dominant_weather_code = mode across zones (most frequent code that hour).
_AGG_SQL = """
WITH zone_data AS (
    SELECT
        hour_utc,
        AVG(temperature_c)                                                      AS avg_temperature_c,
        AVG(COALESCE(precipitation_mm, 0.0))                                    AS avg_precipitation_mm,
        AVG(COALESCE(rain_mm, 0.0))                                             AS avg_rain_mm,
        MAX(wind_speed_kmh)                                                     AS max_wind_speed_kmh,
        CAST(SUM(CASE WHEN COALESCE(rain_mm, 0) > 0.5 THEN 1 ELSE 0 END) AS INTEGER) AS rainy_zones,
        CAST(COUNT(*) AS INTEGER)                                               AS zone_count
    FROM silver
    WHERE hour_utc IS NOT NULL
    GROUP BY hour_utc
),
code_counts AS (
    SELECT
        hour_utc,
        weather_code,
        MAX(weather_desc) AS weather_desc,
        COUNT(*)          AS cnt
    FROM silver
    WHERE hour_utc IS NOT NULL
    GROUP BY hour_utc, weather_code
),
dominant AS (
    SELECT
        hour_utc,
        weather_code AS dominant_weather_code,
        weather_desc AS dominant_weather_desc,
        ROW_NUMBER() OVER (
            PARTITION BY hour_utc
            ORDER BY cnt DESC, weather_code
        ) AS rn
    FROM code_counts
)
SELECT
    z.hour_utc,
    z.avg_temperature_c,
    z.avg_precipitation_mm,
    z.avg_rain_mm,
    z.max_wind_speed_kmh,
    CAST(d.dominant_weather_code AS INTEGER) AS dominant_weather_code,
    d.dominant_weather_desc,
    z.rainy_zones,
    z.zone_count
FROM zone_data z
JOIN dominant d ON z.hour_utc = d.hour_utc AND d.rn = 1
ORDER BY z.hour_utc
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
                source_id=1, field_id=1000, transform=DayTransform(), name="hour_utc_day"
            ),
        )
        table = catalog.create_table(
            _GOLD_TABLE,
            schema=_GOLD_SCHEMA,
            partition_spec=partition_spec,
        )
        logger.info("Created Iceberg table: %s", _GOLD_TABLE)
        return table


def _scan_to_arrow(scan_result: object) -> pa.Table:
    if isinstance(scan_result, pa.Table):
        return scan_result
    return scan_result.read_all()  # type: ignore[union-attr]


def _aggregate_silver(silver_arrow: pa.Table) -> pa.Table:
    import duckdb
    logger.info(
        "_aggregate_silver: input %d rows, schema=%s",
        silver_arrow.num_rows,
        silver_arrow.schema,
    )
    con = duckdb.connect()
    con.register("silver", silver_arrow)
    try:
        result = con.execute(_AGG_SQL).fetch_arrow_table()
    except Exception:
        logger.exception("_aggregate_silver: DuckDB query failed")
        raise
    logger.info("_aggregate_silver: DuckDB produced %d rows, casting schema", result.num_rows)
    try:
        return result.cast(_GOLD_ARROW_SCHEMA)
    except Exception:
        logger.exception(
            "_aggregate_silver: schema cast failed — result schema: %s, target: %s",
            result.schema,
            _GOLD_ARROW_SCHEMA,
        )
        raise


def run(catalog: Catalog | None = None) -> int:
    """Aggregate today's silver weather into gold.weather_hourly.

    Overwrites today's day partition. Idempotent — safe to re-run every hour.
    Returns number of rows written (typically 24 × completed hours today).
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

    silver_arrow = _scan_to_arrow(
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
            GreaterThanOrEqual("hour_utc", day_start.isoformat()),
            LessThan("hour_utc", day_end.isoformat()),
        ),
    )

    print(f"silver_to_gold_weather: upserted {row_count} hourly rows for {day_start.date()}")
    logger.info("silver_to_gold_weather: upserted %d rows for %s", row_count, day_start.date())
    return row_count


def bootstrap(catalog: Catalog | None = None) -> int:
    """Full re-aggregate: all silver weather → gold.weather_hourly.

    Overwrites entire gold table. Idempotent.
    Returns total rows written (1 row per hour across full history).
    """
    logger.info("silver_to_gold_weather bootstrap: starting")
    if catalog is None:
        catalog = _get_catalog()

    try:
        silver_table = catalog.load_table(_SILVER_TABLE)
        logger.info("silver_to_gold_weather bootstrap: loaded silver table OK")
    except Exception as exc:
        logger.warning("silver_to_gold_weather bootstrap: silver table not found — %s", exc)
        return 0

    logger.info("silver_to_gold_weather bootstrap: scanning all silver rows")
    try:
        silver_arrow = _scan_to_arrow(silver_table.scan().to_arrow())
    except Exception:
        logger.exception("silver_to_gold_weather bootstrap: silver scan failed")
        raise
    logger.info("silver_to_gold_weather bootstrap: scanned %d silver rows", silver_arrow.num_rows)

    if silver_arrow.num_rows == 0:
        logger.info("silver_to_gold_weather bootstrap: no silver data — skipping")
        return 0

    gold_arrow = _aggregate_silver(silver_arrow)
    row_count = gold_arrow.num_rows
    logger.info("silver_to_gold_weather bootstrap: aggregated → %d gold rows", row_count)

    try:
        gold_table = _ensure_gold_table(catalog)
        logger.info("silver_to_gold_weather bootstrap: writing to gold table")
        gold_table.overwrite(gold_arrow)
    except Exception:
        logger.exception("silver_to_gold_weather bootstrap: gold overwrite failed")
        raise

    logger.info("silver_to_gold_weather bootstrap: done — wrote %d hourly rows", row_count)
    return row_count
