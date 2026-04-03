"""Feature engineering pipeline for traffic anomaly models.

Joins gold hourly observations with baseline statistics to compute
deviation-based features used by anomaly detection models.
"""

import logging

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError

logger = logging.getLogger(__name__)

_GOLD_TABLE = "gold.traffic_hourly"

# Features used by anomaly detection models
FEATURE_COLUMNS = [
    "avg_heavy_ratio",
    "avg_moderate_ratio",
    "avg_low_ratio",
    "max_severe_segments",
    "hour_of_day",
    "day_of_week",
]

_FEATURE_SQL = """
    SELECT
        route_id,
        hour_utc,
        avg_heavy_ratio,
        avg_moderate_ratio,
        COALESCE(avg_low_ratio, 0.0)                          AS avg_low_ratio,
        CAST(max_severe_segments AS DOUBLE)                   AS max_severe_segments,
        CAST(EXTRACT(HOUR FROM hour_utc) AS DOUBLE)           AS hour_of_day,
        CAST(EXTRACT(DOW  FROM hour_utc) AS DOUBLE)           AS day_of_week
    FROM gold
    WHERE avg_heavy_ratio IS NOT NULL
      AND avg_moderate_ratio IS NOT NULL
"""


def _scan_to_table(scan_result: object) -> pa.Table:
    """Convert PyIceberg scan result to Arrow Table."""
    if isinstance(scan_result, pa.Table):
        return scan_result
    return scan_result.read_all()  # type: ignore[union-attr]


def build_features(catalog: Catalog, route_id: str | None = None) -> pa.Table | None:
    """Build feature table from gold hourly data.

    Args:
        catalog: PyIceberg catalog instance.
        route_id: If provided, filter features to this route only.

    Returns an Arrow table with columns: route_id, hour_utc, + FEATURE_COLUMNS.
    Returns None if gold table is empty/missing.
    """
    try:
        gold_table = catalog.load_table(_GOLD_TABLE)
    except NoSuchTableError as exc:
        logger.warning("build_features: table not found — %s", exc)
        return None

    gold_arrow = _scan_to_table(gold_table.scan().to_arrow())

    if gold_arrow.num_rows == 0:
        logger.info("build_features: no data in gold — skipping")
        return None

    con = duckdb.connect()
    con.register("gold", gold_arrow)
    result = con.execute(_FEATURE_SQL).arrow()
    if not isinstance(result, pa.Table):
        result = result.read_all()

    if route_id is not None:
        import pyarrow.compute as pc
        result = result.filter(pc.equal(result.column("route_id"), route_id))

    logger.info(
        "build_features: %d rows, %d features%s",
        result.num_rows,
        len(FEATURE_COLUMNS),
        f" (route={route_id})" if route_id else "",
    )
    return result


def list_route_ids(catalog: Catalog) -> list[str]:
    """Return all distinct route_ids from gold.traffic_hourly."""
    try:
        gold_table = catalog.load_table(_GOLD_TABLE)
    except NoSuchTableError:
        return []
    gold_arrow = _scan_to_table(gold_table.scan().to_arrow())
    if gold_arrow.num_rows == 0:
        return []
    import pyarrow.compute as pc
    return pc.unique(gold_arrow.column("route_id")).to_pylist()
