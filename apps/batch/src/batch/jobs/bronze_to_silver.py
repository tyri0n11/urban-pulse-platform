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
    TimestamptzType,
)

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

logger = logging.getLogger(__name__)

_BRONZE_BASE = "s3://urban-pulse/bronze/traffic-route-bronze"
_BRONZE_PATH = f"{_BRONZE_BASE}/year=*/month=*/day=*/hour=*/*.parquet"

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
    NestedField(12, "timestamp_utc", TimestamptzType()),
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
    pa.field("timestamp_utc", pa.timestamp("us", tz="UTC")),
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


_BRONZE_TO_SILVER_RENAME: dict[str, str] = {
    "heavy_ratio": "congestion_heavy_ratio",
    "moderate_ratio": "congestion_moderate_ratio",
    "low_ratio": "congestion_low_ratio",
    "severe_segments": "congestion_severe_segments",
    "total_segments": "congestion_total_segments",
}

_FALLBACK_VALUES: dict[str, float | int] = {
    "congestion_heavy_ratio": 0.0,
    "congestion_moderate_ratio": 0.0,
    "congestion_low_ratio": 0.0,
    "congestion_severe_segments": 0,
    "congestion_total_segments": 0,
}


def _cast_to_arrow_schema(raw: pa.Table) -> pa.Table:
    """Cast a single bronze parquet table to the silver Arrow schema.

    Handles mixed types across files (e.g. string vs timestamp, int64 vs double),
    bronze→silver column renaming, and null fallbacks for congestion fields.
    """
    # Rename bronze columns to silver names
    rename_map = {}
    for old, new in _BRONZE_TO_SILVER_RENAME.items():
        if old in raw.schema.names and new not in raw.schema.names:
            rename_map[old] = new
    if rename_map:
        raw = raw.rename_columns(
            [rename_map.get(name, name) for name in raw.schema.names]
        )

    columns: dict[str, pa.Array] = {}
    for field in _ARROW_SCHEMA:
        if field.name not in raw.schema.names:
            # Missing column → use fallback if available, otherwise nulls
            if field.name in _FALLBACK_VALUES:
                columns[field.name] = pa.array(
                    [_FALLBACK_VALUES[field.name]] * len(raw), type=field.type
                )
            else:
                columns[field.name] = pa.nulls(len(raw), type=field.type)
            continue
        col = raw.column(field.name)
        # Normalize timestamps → pa.timestamp("us", tz="UTC")
        if pa.types.is_timestamp(field.type):
            if pa.types.is_string(col.type):
                # Old bronze data: ISO 8601 strings like "2026-01-26T18:06:14+00"
                col = col.cast(pa.timestamp("us", tz="UTC"))
            elif pa.types.is_timestamp(col.type) and col.type.tz is None:
                # Tz-naive timestamp: assume UTC
                col = pa.compute.assume_timezone(col, timezone="UTC")
            if col.type != field.type:
                col = col.cast(field.type)
        elif col.type != field.type:
            col = col.cast(field.type)
        # Fill nulls with fallback values
        if field.name in _FALLBACK_VALUES:
            scalar = pa.scalar(_FALLBACK_VALUES[field.name], type=field.type)
            col = pa.compute.if_else(pa.compute.is_null(col), scalar, col)
        columns[field.name] = col
    return pa.table(columns, schema=_ARROW_SCHEMA)


def _get_s3fs() -> "s3fs.S3FileSystem":  # type: ignore[name-defined]
    import s3fs

    return s3fs.S3FileSystem(
        endpoint_url=f"http://{settings.minio_endpoint}",
        key=settings.minio_access_key,
        secret=settings.minio_secret_key,
    )


def _read_bronze_parquet(path: str) -> pa.Table | None:
    """Read bronze parquet files from MinIO via s3fs, return None if no files."""
    fs = _get_s3fs()

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
    """Upsert the current hour's bronze data into silver.

    Each run reads only the current hour's bronze partition and overwrites
    the same hour window in silver.  No watermark needed — idempotent and
    always O(one hour of data).

    Returns the number of rows written.
    """
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    if catalog is None:
        catalog = _get_catalog()

    table = _ensure_table(catalog)
    now = datetime.now(timezone.utc)

    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)

    bronze_path = (
        f"{_BRONZE_BASE}/year={hour_start.year}/month={hour_start.month:02d}"
        f"/day={hour_start.day:02d}/hour={hour_start.hour:02d}/*.parquet"
    )
    logger.info("bronze_to_silver microbatch: reading %s", bronze_path)

    raw = _read_bronze_parquet(bronze_path)
    if raw is None or len(raw) == 0:
        logger.info("bronze_to_silver microbatch: no bronze files for current hour — skipping")
        return 0

    silver = _filter_nulls(raw)
    row_count = len(silver)

    table.overwrite(
        silver,
        overwrite_filter=And(
            GreaterThanOrEqual("timestamp_utc", hour_start.isoformat()),
            LessThan("timestamp_utc", hour_end.isoformat()),
        ),
    )
    logger.info("bronze_to_silver microbatch: upserted %d rows for hour %s", row_count, hour_start)
    return row_count


def bootstrap(catalog: Catalog | None = None) -> int:
    """Full backfill: promote ALL bronze data to silver Iceberg table.

    Safe to re-run — deduplicates against existing silver rows by
    (route_id, timestamp_utc) before appending.
    """
    logger.info("bronze_to_silver bootstrap: full backfill")
    if catalog is None:
        catalog = _get_catalog()

    table = _ensure_table(catalog)

    raw = _read_bronze_parquet(_BRONZE_PATH)
    if raw is None or len(raw) == 0:
        logger.info("bronze_to_silver bootstrap: no bronze files found")
        return 0

    silver = _filter_nulls(raw)

    # Deduplicate against existing silver data
    existing = table.scan(selected_fields=("route_id", "timestamp_utc")).to_arrow()
    if len(existing) > 0:
        existing_keys = set(
            zip(
                existing.column("route_id").to_pylist(),
                existing.column("timestamp_utc").to_pylist(),
            )
        )
        new_keys = list(
            zip(
                silver.column("route_id").to_pylist(),
                silver.column("timestamp_utc").to_pylist(),
            )
        )
        mask = pa.array([k not in existing_keys for k in new_keys])
        silver = silver.filter(mask)
        logger.info(
            "bronze_to_silver bootstrap: %d existing rows filtered, %d new rows to append",
            len(existing),
            len(silver),
        )

    row_count = len(silver)
    if row_count == 0:
        logger.info("bronze_to_silver bootstrap: no new rows to append")
        return 0

    table.append(silver)
    logger.info("bronze_to_silver bootstrap: appended %d rows → %s", row_count, _SILVER_TABLE)
    return row_count


def backfill(
    from_dt: datetime,
    to_dt: datetime | None = None,
    catalog: Catalog | None = None,
) -> int:
    """Backfill silver for a specific time range by re-processing bronze hour partitions.

    Iterates every hour in [from_dt, to_dt] and overwrites that window in silver.
    Safe to re-run — each hour is a clean overwrite, not an append.

    Args:
        from_dt: Start of backfill range (inclusive).
        to_dt:   End of backfill range (inclusive). Defaults to now.

    Returns:
        Total number of rows written.
    """
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    if catalog is None:
        catalog = _get_catalog()

    table = _ensure_table(catalog)

    if to_dt is None:
        to_dt = datetime.now(timezone.utc)

    # Normalise to hour boundaries
    hour = from_dt.replace(minute=0, second=0, microsecond=0, tzinfo=from_dt.tzinfo or timezone.utc)
    end = to_dt.replace(minute=0, second=0, microsecond=0, tzinfo=to_dt.tzinfo or timezone.utc)

    total = 0
    while hour <= end:
        hour_end = hour + timedelta(hours=1)
        path = (
            f"{_BRONZE_BASE}/year={hour.year}/month={hour.month:02d}"
            f"/day={hour.day:02d}/hour={hour.hour:02d}/*.parquet"
        )
        raw = _read_bronze_parquet(path)
        if raw is None or len(raw) == 0:
            logger.info("bronze_to_silver backfill: no data for %s — skipping", hour)
            hour = hour_end
            continue

        silver = _filter_nulls(raw)
        row_count = len(silver)

        table.overwrite(
            silver,
            overwrite_filter=And(
                GreaterThanOrEqual("timestamp_utc", hour.isoformat()),
                LessThan("timestamp_utc", hour_end.isoformat()),
            ),
        )
        logger.info("bronze_to_silver backfill: wrote %d rows for hour %s", row_count, hour)
        total += row_count
        hour = hour_end

    logger.info("bronze_to_silver backfill: done — %d total rows written", total)
    return total
