"""Batch job: promotes raw bronze data to cleaned silver Iceberg table."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

logger = logging.getLogger(__name__)

_BRONZE_BASE = "s3://urban-pulse/bronze/traffic-route-bronze"
_BRONZE_PATH = f"{_BRONZE_BASE}/year=2026/month=*/day=*/hour=*/*.parquet"

_SILVER_NS = "silver"
_SILVER_TABLE = "silver.traffic_route"

_SILVER_SCHEMA = Schema(
    NestedField(1, "route_id", StringType(), required=True),
    NestedField(2, "origin", StringType(), required=True),
    NestedField(3, "destination", StringType(), required=True),
    NestedField(4, "distance_meters", DoubleType()),
    NestedField(5, "duration_ms", DoubleType()),
    NestedField(6, "duration_minutes", DoubleType()),
    NestedField(7, "congestion_heavy_ratio", DoubleType()),
    NestedField(8, "congestion_moderate_ratio", DoubleType()),
    NestedField(9, "congestion_low_ratio", DoubleType()),
    NestedField(10, "congestion_severe_segments", IntegerType()),
    NestedField(11, "congestion_total_segments", IntegerType()),
    NestedField(12, "timestamp_utc", TimestampType()),
    NestedField(13, "source", StringType()),
)

_ARROW_SCHEMA = pa.schema([
    pa.field("route_id", pa.string(), nullable=False),
    pa.field("origin", pa.string(), nullable=False),
    pa.field("destination", pa.string(), nullable=False),
    pa.field("distance_meters", pa.float64()),
    pa.field("duration_ms", pa.float64()),
    pa.field("duration_minutes", pa.float64()),
    pa.field("congestion_heavy_ratio", pa.float64()),
    pa.field("congestion_moderate_ratio", pa.float64()),
    pa.field("congestion_low_ratio", pa.float64()),
    pa.field("congestion_severe_segments", pa.int32()),
    pa.field("congestion_total_segments", pa.int32()),
    pa.field("timestamp_utc", pa.timestamp("us")),
    pa.field("source", pa.string()),
])


def _get_catalog() -> Catalog:
    return get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )


def _ensure_table(catalog: Catalog) -> Table:
    """Create the silver namespace and table if they don't exist."""
    try:
        catalog.load_namespace_properties(_SILVER_NS)
    except NoSuchNamespaceError:
        catalog.create_namespace(_SILVER_NS)
        logger.info("Created Iceberg namespace: %s", _SILVER_NS)

    try:
        return catalog.load_table(_SILVER_TABLE)
    except NoSuchTableError:
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=12, field_id=1000, transform=DayTransform(), name="timestamp_utc_day"
            ),
        )
        table = catalog.create_table(
            _SILVER_TABLE,
            schema=_SILVER_SCHEMA,
            partition_spec=partition_spec,
        )
        logger.info("Created Iceberg table: %s", _SILVER_TABLE)
        return table


def _cast_to_arrow_schema(raw: pa.Table) -> pa.Table:
    """Cast a single bronze parquet table to the silver Arrow schema.

    Handles mixed types across files (e.g. string vs timestamp, int64 vs double).
    """
    columns: dict[str, pa.Array] = {}
    for field in _ARROW_SCHEMA:
        if field.name not in raw.schema.names:
            # Missing column → fill with nulls
            columns[field.name] = pa.nulls(len(raw), type=field.type)
            continue
        col = raw.column(field.name)
        # If source is string but target is timestamp, parse ISO 8601
        if pa.types.is_string(col.type) and pa.types.is_timestamp(field.type):
            parsed = pa.compute.cast(col, pa.timestamp("us", tz="UTC"))
            col = pa.compute.cast(parsed, field.type)
        elif col.type != field.type:
            col = col.cast(field.type)
        columns[field.name] = col
    return pa.table(columns, schema=_ARROW_SCHEMA)


def _read_bronze_parquet(path: str) -> pa.Table | None:
    """Read bronze parquet files from MinIO via s3fs, return None if no files."""
    import s3fs

    fs = s3fs.S3FileSystem(
        endpoint_url=f"http://{settings.minio_endpoint}",
        key=settings.minio_access_key,
        secret=settings.minio_secret_key,
    )

    # Strip s3:// prefix for glob
    glob_pattern = path.removeprefix("s3://")
    files = fs.glob(glob_pattern)
    if not files:
        return None

    tables = []
    for f in files:
        t = pq.read_table(fs.open(f))
        tables.append(_cast_to_arrow_schema(t))

    return pa.concat_tables(tables)


def _filter_nulls(table: pa.Table) -> pa.Table:
    """Filter out rows with null required fields."""
    mask = pa.compute.and_(
        pa.compute.is_valid(table.column("route_id")),
        pa.compute.is_valid(table.column("timestamp_utc")),
    )
    return table.filter(mask)


def microbatch(catalog: Catalog | None = None) -> int:
    """Promote yesterday's bronze records to silver Iceberg table.

    Scoped to a single day's partition — no full scan needed.
    Returns the number of rows appended.
    """
    if catalog is None:
        catalog = _get_catalog()

    table = _ensure_table(catalog)

    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    bronze_path = (
        f"{_BRONZE_BASE}/year={yesterday.year}/month={yesterday.month:02d}"
        f"/day={yesterday.day:02d}/hour=*/*.parquet"
    )
    logger.info("bronze_to_silver microbatch: reading %s", bronze_path)

    raw = _read_bronze_parquet(bronze_path)
    if raw is None or len(raw) == 0:
        logger.info("bronze_to_silver microbatch: no bronze files for %s — skipping", bronze_path)
        return 0

    silver = _filter_nulls(raw)
    row_count = len(silver)

    table.append(silver)
    logger.info("bronze_to_silver microbatch: appended %d rows → %s", row_count, _SILVER_TABLE)
    return row_count


def bootstrap(catalog: Catalog | None = None) -> int:
    """Full backfill: promote ALL bronze data to silver Iceberg table."""
    logger.info("bronze_to_silver bootstrap: full backfill")
    if catalog is None:
        catalog = _get_catalog()

    table = _ensure_table(catalog)

    raw = _read_bronze_parquet(_BRONZE_PATH)
    if raw is None or len(raw) == 0:
        logger.info("bronze_to_silver bootstrap: no bronze files found")
        return 0

    silver = _filter_nulls(raw)
    row_count = len(silver)

    table.append(silver)
    logger.info("bronze_to_silver bootstrap: appended %d rows → %s", row_count, _SILVER_TABLE)
    return row_count
