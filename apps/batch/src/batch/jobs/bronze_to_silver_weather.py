"""Batch job: promotes weather bronze Parquet to silver Iceberg table."""

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

_BRONZE_BASE = "s3://urban-pulse/bronze/weather-hcmc-bronze"
_BRONZE_PATH = f"{_BRONZE_BASE}/year=*/month=*/day=*/hour=*/*.parquet"

_SILVER_NS = "silver"
_SILVER_TABLE = "silver.weather_hourly"

_SILVER_SCHEMA = Schema(
    NestedField(1, "location_id", StringType(), required=True),
    NestedField(2, "hour_utc", TimestamptzType(), required=True),
    NestedField(3, "temperature_c", DoubleType()),
    NestedField(4, "precipitation_mm", DoubleType()),
    NestedField(5, "rain_mm", DoubleType()),
    NestedField(6, "wind_speed_kmh", DoubleType()),
    NestedField(7, "wind_direction_deg", DoubleType()),
    NestedField(8, "wind_direction_name", StringType()),
    NestedField(9, "cloud_cover_pct", DoubleType()),
    NestedField(10, "weather_code", IntegerType()),
    NestedField(11, "weather_desc", StringType()),
    NestedField(12, "source", StringType()),
)

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


def _get_catalog() -> Catalog:
    return get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )


def _ensure_table(catalog: Catalog) -> Table:
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
                source_id=2, field_id=1000, transform=DayTransform(), name="hour_utc_day"
            ),
        )
        table = catalog.create_table(
            _SILVER_TABLE,
            schema=_SILVER_SCHEMA,
            partition_spec=partition_spec,
        )
        logger.info("Created Iceberg table: %s", _SILVER_TABLE)
        return table


def _get_s3fs() -> "s3fs.S3FileSystem":  # type: ignore[name-defined]  # noqa: F821
    import s3fs
    return s3fs.S3FileSystem(
        endpoint_url=f"http://{settings.minio_endpoint}",
        key=settings.minio_access_key,
        secret=settings.minio_secret_key,
    )


def _cast_to_arrow_schema(raw: pa.Table) -> pa.Table:
    columns: dict[str, pa.Array] = {}
    for field in _ARROW_SCHEMA:
        if field.name not in raw.schema.names:
            columns[field.name] = pa.nulls(len(raw), type=field.type)
            continue
        col = raw.column(field.name)
        if pa.types.is_timestamp(field.type):
            if pa.types.is_string(col.type):
                col = col.cast(pa.timestamp("us", tz="UTC"))
            elif pa.types.is_timestamp(col.type) and col.type.tz is None:
                col = pa.compute.assume_timezone(col, timezone="UTC")
            if col.type != field.type:
                col = col.cast(field.type)
        elif col.type != field.type:
            col = col.cast(field.type)
        columns[field.name] = col
    return pa.table(columns, schema=_ARROW_SCHEMA)


def _read_bronze_parquet(path: str) -> pa.Table | None:
    fs = _get_s3fs()
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
    mask = pa.compute.and_(
        pa.compute.is_valid(table.column("location_id")),
        pa.compute.is_valid(table.column("hour_utc")),
    )
    return table.filter(mask)


def microbatch(catalog: Catalog | None = None) -> int:
    """Upsert the current and previous hour's bronze data into silver.

    Checks both current and previous hour because the weather ingestion service
    publishes after the hour completes, so the most recent data may be in the
    previous hour's partition at the time microbatch runs.

    Returns the number of rows written.
    """
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    if catalog is None:
        catalog = _get_catalog()

    table = _ensure_table(catalog)
    now = datetime.now(timezone.utc)
    hour_start = now.replace(minute=0, second=0, microsecond=0)

    total = 0
    for offset in (0, -1):
        h = hour_start + timedelta(hours=offset)
        h_end = h + timedelta(hours=1)
        bronze_path = (
            f"{_BRONZE_BASE}/year={h.year}/month={h.month:02d}"
            f"/day={h.day:02d}/hour={h.hour:02d}/*.parquet"
        )
        raw = _read_bronze_parquet(bronze_path)
        if raw is None or len(raw) == 0:
            continue

        silver = _filter_nulls(raw)
        row_count = len(silver)
        table.overwrite(
            silver,
            overwrite_filter=And(
                GreaterThanOrEqual("hour_utc", h.isoformat()),
                LessThan("hour_utc", h_end.isoformat()),
            ),
        )
        print(f"bronze_to_silver_weather microbatch: upserted {row_count} rows for hour {h}")
        logger.info(
            "bronze_to_silver_weather microbatch: upserted %d rows for hour %s", row_count, h
        )
        total += row_count

    if total == 0:
        print("bronze_to_silver_weather microbatch: no new weather data — skipping")
        logger.info("bronze_to_silver_weather microbatch: no new weather data — skipping")

    return total


def bootstrap(catalog: Catalog | None = None) -> int:
    """Full backfill: promote ALL weather bronze data to silver Iceberg table.

    Deduplicates against existing silver rows by (location_id, hour_utc).
    Safe to re-run.

    Returns the number of rows written.
    """
    logger.info("bronze_to_silver_weather bootstrap: full backfill")
    if catalog is None:
        catalog = _get_catalog()

    table = _ensure_table(catalog)

    raw = _read_bronze_parquet(_BRONZE_PATH)
    if raw is None or len(raw) == 0:
        logger.info("bronze_to_silver_weather bootstrap: no bronze files found")
        return 0

    silver = _filter_nulls(raw)

    existing = table.scan(selected_fields=("location_id", "hour_utc")).to_arrow()
    if len(existing) > 0:
        existing_keys = set(
            zip(
                existing.column("location_id").to_pylist(),
                existing.column("hour_utc").to_pylist(),
            )
        )
        new_keys = list(
            zip(
                silver.column("location_id").to_pylist(),
                silver.column("hour_utc").to_pylist(),
            )
        )
        mask = pa.array([k not in existing_keys for k in new_keys])
        silver = silver.filter(mask)
        logger.info(
            "bronze_to_silver_weather bootstrap: %d existing rows filtered, %d new rows to append",
            len(existing),
            len(silver),
        )

    row_count = len(silver)
    if row_count == 0:
        logger.info("bronze_to_silver_weather bootstrap: no new rows to append")
        return 0

    table.append(silver)
    logger.info(
        "bronze_to_silver_weather bootstrap: appended %d rows → %s", row_count, _SILVER_TABLE
    )
    return row_count
