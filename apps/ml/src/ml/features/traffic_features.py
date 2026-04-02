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
_BASELINE_TABLE = "gold.traffic_baseline"

# Features used by anomaly detection models
FEATURE_COLUMNS = [
    "duration_zscore",
    "heavy_ratio_deviation",
    "p95_to_mean_ratio",
    "observation_count",
    "max_severe_segments",
]

_FEATURE_SQL = """
    SELECT
        g.route_id,
        g.hour_utc,

        -- Z-score: how many stddevs away from baseline
        CASE
            WHEN b.baseline_duration_stddev > 0
            THEN (g.avg_duration_minutes - b.baseline_duration_mean)
                 / b.baseline_duration_stddev
            ELSE 0.0
        END AS duration_zscore,

        -- Deviation from baseline heavy ratio
        g.avg_heavy_ratio - COALESCE(b.baseline_heavy_ratio_mean, 0.0)
            AS heavy_ratio_deviation,

        -- P95/mean ratio — spikiness indicator
        CASE
            WHEN g.avg_duration_minutes > 0
            THEN g.p95_duration_minutes / g.avg_duration_minutes
            ELSE 1.0
        END AS p95_to_mean_ratio,

        CAST(g.observation_count AS DOUBLE) AS observation_count,
        CAST(g.max_severe_segments AS DOUBLE) AS max_severe_segments

    FROM gold AS g
    LEFT JOIN baseline AS b
        ON  g.route_id = b.route_id
        AND CAST(EXTRACT(DOW  FROM g.hour_utc) AS INTEGER) = b.day_of_week
        AND CAST(EXTRACT(HOUR FROM g.hour_utc) AS INTEGER) = b.hour_of_day
    WHERE b.route_id IS NOT NULL
"""


def _scan_to_table(scan_result: object) -> pa.Table:
    """Convert PyIceberg scan result to Arrow Table."""
    if isinstance(scan_result, pa.Table):
        return scan_result
    return scan_result.read_all()  # type: ignore[union-attr]


def build_features(catalog: Catalog, route_id: str | None = None) -> pa.Table | None:
    """Build feature table by joining gold with baseline.

    Args:
        catalog: PyIceberg catalog instance.
        route_id: If provided, filter features to this route only.

    Returns an Arrow table with columns: route_id, hour_utc, + FEATURE_COLUMNS.
    Returns None if either gold or baseline table is empty/missing.
    """
    try:
        gold_table = catalog.load_table(_GOLD_TABLE)
        baseline_table = catalog.load_table(_BASELINE_TABLE)
    except NoSuchTableError as exc:
        logger.warning("build_features: table not found — %s", exc)
        return None

    gold_arrow = _scan_to_table(gold_table.scan().to_arrow())
    baseline_arrow = _scan_to_table(baseline_table.scan().to_arrow())

    if gold_arrow.num_rows == 0 or baseline_arrow.num_rows == 0:
        logger.info("build_features: no data in gold or baseline — skipping")
        return None

    con = duckdb.connect()
    con.register("gold", gold_arrow)
    con.register("baseline", baseline_arrow)
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
