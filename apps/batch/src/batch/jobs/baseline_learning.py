"""Batch job: computes traffic baselines for anomaly detection.

Reads from the gold Iceberg table and writes baseline statistics
to a baseline Iceberg table for anomaly scoring.
"""

import logging

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
)

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

logger = logging.getLogger(__name__)

_GOLD_HOURLY_TABLE = "gold.traffic_hourly"
_GOLD_NS = "gold"
_BASELINE_TABLE = "gold.traffic_baseline"

_BASELINE_SCHEMA = Schema(
    NestedField(1, "route_id", StringType(), required=True),
    NestedField(2, "day_of_week", IntegerType(), required=True),
    NestedField(3, "hour_of_day", IntegerType(), required=True),
    NestedField(4, "baseline_duration_mean", DoubleType()),
    NestedField(5, "baseline_duration_stddev", DoubleType()),
    NestedField(6, "baseline_heavy_ratio_mean", DoubleType()),
    NestedField(7, "sample_count", IntegerType()),
)

_BASELINE_ARROW_SCHEMA = pa.schema([
    pa.field("route_id", pa.string(), nullable=False),
    pa.field("day_of_week", pa.int32(), nullable=False),
    pa.field("hour_of_day", pa.int32(), nullable=False),
    pa.field("baseline_duration_mean", pa.float64()),
    pa.field("baseline_duration_stddev", pa.float64()),
    pa.field("baseline_heavy_ratio_mean", pa.float64()),
    pa.field("sample_count", pa.int32()),
])


def _get_catalog() -> Catalog:
    return get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )


def _ensure_baseline_table(catalog: Catalog) -> Table:
    """Create the baseline table if it doesn't exist."""
    try:
        catalog.load_namespace_properties(_GOLD_NS)
    except NoSuchNamespaceError:
        catalog.create_namespace(_GOLD_NS)

    try:
        return catalog.load_table(_BASELINE_TABLE)
    except NoSuchTableError:
        table = catalog.create_table(
            _BASELINE_TABLE,
            schema=_BASELINE_SCHEMA,
        )
        logger.info("Created Iceberg table: %s", _BASELINE_TABLE)
        return table


def run() -> int:
    """Compute per-route, per-day-of-week/hour-of-day baselines.

    Reads ALL gold hourly data, computes mean/stddev per route per
    (day_of_week, hour_of_day), and overwrites the baseline table.
    Baseline needs full history to compute accurate statistics.

    Returns the number of rows written.
    """
    catalog = _get_catalog()

    # Read all gold hourly data
    try:
        gold_table = catalog.load_table(_GOLD_HOURLY_TABLE)
    except NoSuchTableError:
        logger.info("baseline_learning: gold table does not exist yet — skipping")
        return 0

    gold_scan = gold_table.scan().to_arrow()
    gold_arrow = gold_scan if isinstance(gold_scan, pa.Table) else gold_scan.read_all()

    if gold_arrow.num_rows == 0:
        logger.info("baseline_learning: no gold data yet — skipping")
        return 0

    # Compute baselines with DuckDB
    con = duckdb.connect()
    con.register("gold", gold_arrow)
    baseline_arrow = con.execute("""
        SELECT
            route_id,
            CAST(EXTRACT(DOW  FROM hour_utc) AS INTEGER) AS day_of_week,
            CAST(EXTRACT(HOUR FROM hour_utc) AS INTEGER) AS hour_of_day,
            AVG(avg_duration_minutes)                      AS baseline_duration_mean,
            STDDEV(avg_duration_minutes)                   AS baseline_duration_stddev,
            AVG(avg_heavy_ratio)                           AS baseline_heavy_ratio_mean,
            CAST(COUNT(*) AS INTEGER)                      AS sample_count
        FROM gold
        GROUP BY route_id, day_of_week, hour_of_day
    """).arrow()

    if not isinstance(baseline_arrow, pa.Table):
        baseline_arrow = baseline_arrow.read_all()
    baseline_arrow = baseline_arrow.cast(_BASELINE_ARROW_SCHEMA)
    row_count = baseline_arrow.num_rows

    # Overwrite entire baseline table (always recompute from full history)
    baseline_table = _ensure_baseline_table(catalog)
    baseline_table.overwrite(baseline_arrow)

    logger.info("baseline_learning: wrote %d rows to %s", row_count, _BASELINE_TABLE)
    return row_count
