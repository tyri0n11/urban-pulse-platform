"""Batch job: aggregates silver data into gold-layer Iceberg table.

Runs hourly. Only scans the current hour's silver partition (incremental),
matching the microbatch pattern used by bronze_to_silver.
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
    StringType,
    TimestamptzType,
)

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

logger = logging.getLogger(__name__)

_SILVER_TABLE = "silver.traffic_route"
_GOLD_NS = "gold"
_GOLD_TABLE = "gold.traffic_hourly"

_GOLD_SCHEMA = Schema(
    NestedField(1, "route_id", StringType(), required=True),
    NestedField(2, "origin", StringType(), required=True),
    NestedField(3, "destination", StringType(), required=True),
    NestedField(4, "hour_utc", TimestamptzType(), required=True),
    NestedField(5, "observation_count", IntegerType()),
    NestedField(6, "avg_duration_minutes", DoubleType()),
    NestedField(7, "p95_duration_minutes", DoubleType()),
    NestedField(8, "avg_heavy_ratio", DoubleType()),
    NestedField(9, "avg_moderate_ratio", DoubleType()),
    NestedField(10, "max_severe_segments", IntegerType()),
)

_GOLD_ARROW_SCHEMA = pa.schema([
    pa.field("route_id", pa.string(), nullable=False),
    pa.field("origin", pa.string(), nullable=False),
    pa.field("destination", pa.string(), nullable=False),
    pa.field("hour_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("observation_count", pa.int32()),
    pa.field("avg_duration_minutes", pa.float64()),
    pa.field("p95_duration_minutes", pa.float64()),
    pa.field("avg_heavy_ratio", pa.float64()),
    pa.field("avg_moderate_ratio", pa.float64()),
    pa.field("max_severe_segments", pa.int32()),
])

_AGG_SQL = """
    SELECT
        route_id,
        origin,
        destination,
        DATE_TRUNC('hour', timestamp_utc)                              AS hour_utc,
        CAST(COUNT(*) AS INTEGER)                                      AS observation_count,
        AVG(duration_minutes)                                           AS avg_duration_minutes,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_minutes) AS p95_duration_minutes,
        AVG(congestion_heavy_ratio)                                     AS avg_heavy_ratio,
        AVG(congestion_moderate_ratio)                                  AS avg_moderate_ratio,
        CAST(MAX(congestion_severe_segments) AS INTEGER)               AS max_severe_segments
    FROM silver
    WHERE timestamp_utc IS NOT NULL
    GROUP BY route_id, origin, destination, DATE_TRUNC('hour', timestamp_utc)
"""


def _get_catalog() -> Catalog:
    return get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )


def _ensure_gold_table(catalog: Catalog) -> Table:
    """Create the gold namespace and table if they don't exist."""
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
                source_id=4, field_id=1000, transform=DayTransform(), name="hour_utc_day"
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
    """Convert PyIceberg scan result to Arrow Table regardless of version.

    Older PyIceberg returns pa.Table, newer returns RecordBatchReader.
    """
    if isinstance(scan_result, pa.Table):
        return scan_result
    # RecordBatchReader or similar
    return scan_result.read_all()  # type: ignore[union-attr]


def _aggregate_silver(silver_arrow: pa.Table) -> pa.Table:
    """Aggregate silver Arrow table into gold schema using DuckDB."""
    import duckdb

    con = duckdb.connect()
    con.register("silver", silver_arrow)
    result = _scan_to_table(con.execute(_AGG_SQL).arrow())
    return result.cast(_GOLD_ARROW_SCHEMA)


def run() -> int:
    """Aggregate the current hour's silver records into gold Iceberg table.

    Reads only the current hour's partition from silver (incremental scan),
    aggregates, and upserts into gold Iceberg table.
    Idempotent — overwrites the same hour window on re-run.

    Returns the number of rows written.
    """
    catalog = _get_catalog()

    now = datetime.now(timezone.utc)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)

    # Read current hour from silver
    silver_table = catalog.load_table(_SILVER_TABLE)
    silver_arrow = silver_table.scan(
        row_filter=And(
            GreaterThanOrEqual("timestamp_utc", hour_start.isoformat()),
            LessThan("timestamp_utc", hour_end.isoformat()),
        ),
    ).to_arrow()
    silver_arrow = _scan_to_table(silver_arrow)

    if silver_arrow.num_rows == 0:
        logger.info("silver_to_gold: no silver data for hour %s — skipping", hour_start)
        return 0

    gold_arrow = _aggregate_silver(silver_arrow)
    row_count = gold_arrow.num_rows

    # Upsert into gold Iceberg table (overwrite current hour)
    gold_table = _ensure_gold_table(catalog)
    gold_table.overwrite(
        gold_arrow,
        overwrite_filter=And(
            GreaterThanOrEqual("hour_utc", hour_start.isoformat()),
            LessThan("hour_utc", hour_end.isoformat()),
        ),
    )

    print(f"silver_to_gold: upserted {row_count} rows for hour {hour_start}")
    logger.info("silver_to_gold: upserted %d rows for hour %s", row_count, hour_start)
    return row_count


def bootstrap() -> int:
    """Full re-aggregate: scan ALL silver data into gold Iceberg table.

    Reads all silver records, aggregates by hour, and overwrites the
    entire gold table. Idempotent.

    Returns the total number of rows written.
    """
    catalog = _get_catalog()

    silver_table = catalog.load_table(_SILVER_TABLE)
    silver_arrow = _scan_to_table(silver_table.scan().to_arrow())

    if silver_arrow.num_rows == 0:
        logger.info("silver_to_gold bootstrap: no silver data — skipping")
        return 0

    gold_arrow = _aggregate_silver(silver_arrow)
    row_count = gold_arrow.num_rows

    # Overwrite entire gold table
    gold_table = _ensure_gold_table(catalog)
    gold_table.overwrite(gold_arrow)

    logger.info("silver_to_gold bootstrap: wrote %d rows", row_count)
    return row_count
