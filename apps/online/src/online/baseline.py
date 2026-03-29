"""Load batch baseline from Iceberg for online z-score computation.

Called at Faust app startup and refreshed every 6 hours via a timer.
Returns a dict keyed by route_id scoped to the CURRENT (dow, hour) slot
so the in-memory lookup is O(1) during stream processing.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import pyarrow as pa

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BaselineEntry:
    mean: float
    stddev: float
    heavy_ratio_mean: float = 0.0


def load_baseline() -> dict[str, BaselineEntry]:
    """Scan gold.traffic_baseline and return entries matching the current (dow, hour).

    Uses the same DOW convention as baseline_learning.py:
    EXTRACT(DOW ...) → 0 = Sunday … 6 = Saturday.
    """
    from urbanpulse_core.config import settings
    from urbanpulse_infra.iceberg import get_iceberg_catalog
    from pyiceberg.exceptions import NoSuchTableError

    catalog = get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )

    try:
        table = catalog.load_table("gold.traffic_baseline")
    except NoSuchTableError:
        logger.info("baseline: gold.traffic_baseline not found — z-scores disabled")
        return {}

    scan = table.scan().to_arrow()
    arrow: pa.Table = scan if isinstance(scan, pa.Table) else scan.read_all()

    if arrow.num_rows == 0:
        return {}

    # Current slot
    now = datetime.now(timezone.utc)
    # Python: Monday=0 … Sunday=6  →  SQL DOW: Sunday=0 … Saturday=6
    dow = (now.weekday() + 1) % 7
    hour = now.hour

    result: dict[str, BaselineEntry] = {}
    for row in arrow.to_pylist():
        if row["day_of_week"] == dow and row["hour_of_day"] == hour:
            mean = float(row["baseline_duration_mean"] or 0.0)
            stddev = float(row["baseline_duration_stddev"] or 0.0)
            heavy_ratio_mean = float(row.get("baseline_heavy_ratio_mean") or 0.0)
            result[row["route_id"]] = BaselineEntry(mean=mean, stddev=stddev, heavy_ratio_mean=heavy_ratio_mean)

    logger.info("baseline: loaded %d entries for dow=%d hour=%d", len(result), dow, hour)
    return result
