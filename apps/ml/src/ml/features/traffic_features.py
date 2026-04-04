"""Feature engineering pipeline for traffic anomaly models.

Joins gold hourly data with per-(route, day_of_week, hour_of_day) baselines
to produce deviation-based features. IsolationForest learns the distribution
of *how much* traffic deviates from the expected pattern for that time slot.

Feature columns (FEATURE_COLUMNS):
    heavy_ratio_deviation    = avg_heavy_ratio   - baseline_mean   (raw delta)
    moderate_ratio_deviation = avg_moderate_ratio - baseline_mean
    heavy_ratio_zscore       = deviation / baseline_stddev          (normalised)
    moderate_ratio_zscore    = deviation / baseline_stddev
    max_severe_segments      (absolute, no baseline)

The per-route baseline dict is also returned by load_baseline_for_route()
and saved as baseline.json alongside the model artifact in MLflow, so
the serving layer can reconstruct the same features at scoring time.
"""

import logging

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError

logger = logging.getLogger(__name__)

_GOLD_TABLE     = "gold.traffic_hourly"
_BASELINE_TABLE = "gold.traffic_baseline"

# Deviation-based features fed to IsolationForest
# ORDER must match prediction_service._FEATURE_ORDER
FEATURE_COLUMNS = [
    "heavy_ratio_deviation",
    "moderate_ratio_deviation",
    "heavy_ratio_zscore",
    "moderate_ratio_zscore",
    "max_severe_segments",
]

_FEATURE_SQL = """
    SELECT
        g.route_id,
        g.hour_utc,
        (g.avg_heavy_ratio    - b.baseline_heavy_ratio_mean)           AS heavy_ratio_deviation,
        (COALESCE(g.avg_moderate_ratio, 0.0)
                              - b.baseline_moderate_ratio_mean)         AS moderate_ratio_deviation,
        (g.avg_heavy_ratio    - b.baseline_heavy_ratio_mean)
            / b.baseline_heavy_ratio_stddev                             AS heavy_ratio_zscore,
        (COALESCE(g.avg_moderate_ratio, 0.0)
                              - b.baseline_moderate_ratio_mean)
            / b.baseline_moderate_ratio_stddev                          AS moderate_ratio_zscore,
        CAST(COALESCE(g.max_severe_segments, 0) AS DOUBLE)              AS max_severe_segments
    FROM gold g
    JOIN baseline b
      ON  g.route_id = b.route_id
      AND CAST(EXTRACT(DOW  FROM g.hour_utc) AS INTEGER) = b.day_of_week
      AND CAST(EXTRACT(HOUR FROM g.hour_utc) AS INTEGER) = b.hour_of_day
    WHERE g.avg_heavy_ratio    IS NOT NULL
      AND g.avg_moderate_ratio IS NOT NULL
"""


def _scan_to_table(scan_result: object) -> pa.Table:
    if isinstance(scan_result, pa.Table):
        return scan_result
    return scan_result.read_all()  # type: ignore[union-attr]


def build_features(
    catalog: Catalog,
    route_id: str | None = None,
) -> pa.Table | None:
    """Build deviation-based feature table from gold + baseline data.

    Returns Arrow table with columns: route_id, hour_utc, + FEATURE_COLUMNS.
    Returns None if gold or baseline tables are empty/missing.
    """
    try:
        gold_table     = catalog.load_table(_GOLD_TABLE)
        baseline_table = catalog.load_table(_BASELINE_TABLE)
    except NoSuchTableError as exc:
        logger.warning("build_features: table not found — %s", exc)
        return None

    gold_arrow     = _scan_to_table(gold_table.scan().to_arrow())
    baseline_arrow = _scan_to_table(baseline_table.scan().to_arrow())

    if gold_arrow.num_rows == 0 or baseline_arrow.num_rows == 0:
        logger.info("build_features: insufficient data — skipping")
        return None

    con = duckdb.connect()
    con.register("gold",     gold_arrow)
    con.register("baseline", baseline_arrow)
    result = con.execute(_FEATURE_SQL).arrow()
    if not isinstance(result, pa.Table):
        result = result.read_all()

    if route_id is not None:
        import pyarrow.compute as pc
        result = result.filter(pc.equal(result.column("route_id"), route_id))

    logger.info(
        "build_features: %d rows, %d features%s",
        result.num_rows, len(FEATURE_COLUMNS),
        f" (route={route_id})" if route_id else "",
    )
    return result


def load_baseline_for_route(
    catalog: Catalog,
    route_id: str,
) -> dict[str, dict[str, float]]:
    """Return per-(dow, hour) baseline stats for one route from Iceberg.

    Keys are "{day_of_week}_{hour_of_day}" strings (JSON-serialisable).
    Values: heavy_mean, heavy_stddev, mod_mean, mod_stddev.

    Returned dict is saved as baseline.json in the MLflow artifact so the
    serving layer can load it without touching Iceberg.
    """
    try:
        baseline_table = catalog.load_table(_BASELINE_TABLE)
    except NoSuchTableError:
        logger.warning("load_baseline_for_route: gold.traffic_baseline not found")
        return {}

    arrow = _scan_to_table(baseline_table.scan().to_arrow())
    if arrow.num_rows == 0:
        return {}

    import pyarrow.compute as pc
    route_rows = arrow.filter(pc.equal(arrow.column("route_id"), route_id))

    result: dict[str, dict[str, float]] = {}
    for row in route_rows.to_pylist():
        key = f"{int(row['day_of_week'])}_{int(row['hour_of_day'])}"
        result[key] = {
            "heavy_mean":   float(row.get("baseline_heavy_ratio_mean") or 0.0),
            "heavy_stddev": max(float(row.get("baseline_heavy_ratio_stddev") or 0.02), 0.001),
            "mod_mean":     float(row.get("baseline_moderate_ratio_mean") or 0.0),
            "mod_stddev":   max(float(row.get("baseline_moderate_ratio_stddev") or 0.02), 0.001),
        }

    logger.info("load_baseline_for_route: route=%s, %d slots", route_id, len(result))
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
