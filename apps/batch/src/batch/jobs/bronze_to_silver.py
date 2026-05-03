"""Batch job: promotes raw bronze data to cleaned silver Iceberg table."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException, NoSuchNamespaceError, NoSuchTableError
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

_BRONZE_BASE = "s3://urban-pulse/bronze/vietmap-raw"
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


def _parse_raw_response(raw_json: str | None) -> tuple[float, float, list[dict[str, object]]]:
    """Extract distance, duration_ms, and congestion segments from a raw VietMap JSON string."""
    import json as _json

    if not raw_json:
        return 0.0, 0.0, []
    try:
        data: dict[str, object] = _json.loads(raw_json)
    except (ValueError, TypeError):
        return 0.0, 0.0, []
    first: dict[str, object] = (data.get("paths") or [{}])[0]  # type: ignore[assignment]
    distance = float(first.get("distance", 0.0))  # type: ignore[arg-type]
    duration_ms = float(first.get("time", 0.0))  # type: ignore[arg-type]
    congestion_segs: list[dict[str, object]] = (
        first.get("annotations", {}).get("congestion", [])  # type: ignore[union-attr]
    )
    return distance, duration_ms, congestion_segs


def _calc_congestion_row(
    segs: list[dict[str, object]],
) -> tuple[float, float, float, int, int]:
    """Return (heavy, moderate, low ratios, severe_segments, total_segments)."""
    if not segs:
        return 0.0, 0.0, 0.0, 0, 0
    total = len(segs)
    counts: dict[str, int] = {"heavy": 0, "moderate": 0, "low": 0, "severe": 0}
    for seg in segs:
        v = seg.get("value", "")
        if isinstance(v, str) and v in counts:
            counts[v] += 1
    return (
        round(counts["heavy"] / total, 3),
        round(counts["moderate"] / total, 3),
        round(counts["low"] / total, 3),
        counts["severe"],
        total,
    )


def _cast_to_arrow_schema(raw: pa.Table) -> pa.Table:
    """Parse raw bronze Parquet (vietmap-raw format) and promote to Silver Arrow schema."""
    route_ids = raw.column("route_id").to_pylist()
    origins = raw.column("origin").to_pylist()
    destinations = raw.column("destination").to_pylist()
    raw_responses = raw.column("raw_response").to_pylist()

    # Normalize timestamp column
    ts_col = raw.column("timestamp_utc")
    if pa.types.is_string(ts_col.type):
        ts_col = ts_col.cast(pa.timestamp("us", tz="UTC"))
    elif pa.types.is_timestamp(ts_col.type) and ts_col.type.tz is None:
        ts_col = pa.compute.assume_timezone(ts_col, timezone="UTC")
    if ts_col.type != pa.timestamp("us", tz="UTC"):
        ts_col = ts_col.cast(pa.timestamp("us", tz="UTC"))

    distance_meters_list, duration_ms_list, duration_minutes_list = [], [], []
    heavy_ratios, moderate_ratios, low_ratios = [], [], []
    severe_segs_list, total_segs_list = [], []

    for raw_json in raw_responses:
        distance, duration_ms, cong_segs = _parse_raw_response(raw_json)
        heavy, moderate, low, severe, total = _calc_congestion_row(cong_segs)
        distance_meters_list.append(distance)
        duration_ms_list.append(duration_ms)
        duration_minutes_list.append(round(duration_ms / 60000, 1))
        heavy_ratios.append(heavy)
        moderate_ratios.append(moderate)
        low_ratios.append(low)
        severe_segs_list.append(severe)
        total_segs_list.append(total)

    n = len(route_ids)
    return pa.table(
        {
            "route_id": pa.array(route_ids, type=pa.string()),
            "origin": pa.array(origins, type=pa.string()),
            "destination": pa.array(destinations, type=pa.string()),
            "distance_meters": pa.array(distance_meters_list, type=pa.float64()),
            "duration_ms": pa.array(duration_ms_list, type=pa.float64()),
            "duration_minutes": pa.array(duration_minutes_list, type=pa.float64()),
            "congestion_heavy_ratio": pa.array(heavy_ratios, type=pa.float64()),
            "congestion_moderate_ratio": pa.array(moderate_ratios, type=pa.float64()),
            "congestion_low_ratio": pa.array(low_ratios, type=pa.float64()),
            "congestion_severe_segments": pa.array(severe_segs_list, type=pa.int32()),
            "congestion_total_segments": pa.array(total_segs_list, type=pa.int32()),
            "timestamp_utc": ts_col,
            "source": pa.array(["ingestion.vietmap"] * n, type=pa.string()),
        },
        schema=_ARROW_SCHEMA,
    )


def _get_s3fs() -> "s3fs.S3FileSystem":  # type: ignore[name-defined]  # noqa: F821
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
        print("bronze_to_silver microbatch: no bronze files for current hour — skipping")
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
    print(f"bronze_to_silver microbatch: upserted {row_count} rows for hour {hour_start}")
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

    # Retry on commit conflict: microbatch may write a new snapshot between our
    # dedup-scan and our append. Re-scan existing keys and retry up to 3 times.
    all_bronze_keys = list(
        zip(silver.column("route_id").to_pylist(), silver.column("timestamp_utc").to_pylist())
    )
    raw_silver = silver
    for attempt in range(3):
        try:
            table.append(raw_silver)
            logger.info("bronze_to_silver bootstrap: appended %d rows → %s", len(raw_silver), _SILVER_TABLE)
            return len(raw_silver)
        except CommitFailedException:
            if attempt == 2:
                raise
            logger.warning("bootstrap: commit conflict (attempt %d/3) — re-scanning existing rows", attempt + 1)
            existing = table.scan(selected_fields=("route_id", "timestamp_utc")).to_arrow()
            existing_keys = set(
                zip(existing.column("route_id").to_pylist(), existing.column("timestamp_utc").to_pylist())
            )
            mask = pa.array([k not in existing_keys for k in all_bronze_keys])
            raw_silver = silver.filter(mask)
            if len(raw_silver) == 0:
                logger.info("bronze_to_silver bootstrap: no new rows after re-scan — already up to date")
                return 0

    return 0


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
