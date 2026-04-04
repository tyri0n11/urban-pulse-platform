"""Feature engineering pipeline for traffic anomaly models.

Reads gold hourly data and produces cyclical time-encoded features so that
IsolationForest learns the joint distribution of (traffic ratios, time-of-day,
day-of-week) directly — no external baseline lookup required.

Feature columns (FEATURE_COLUMNS):
    avg_heavy_ratio      — fraction of heavy vehicles (0–1)
    avg_moderate_ratio   — fraction of moderate vehicles (0–1)
    max_severe_segments  — absolute count of severe congestion segments
    hour_sin / hour_cos  — cyclical encoding of hour-of-day (period 24h)
    dow_sin  / dow_cos   — cyclical encoding of day-of-week (period 7d)

The sin/cos encoding makes 23:00 and 00:00 neighbours in feature space, and
Monday/Sunday neighbours across the week boundary.  IsolationForest therefore
learns that "heavy traffic at 08:00 Mon" is normal while "heavy traffic at
03:00 Sun" is unusual — without any separate baseline table.
"""

import logging
import math

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError

logger = logging.getLogger(__name__)

_GOLD_TABLE = "gold.traffic_hourly"

# ORDER must match prediction_service._FEATURE_ORDER
FEATURE_COLUMNS = [
    "avg_heavy_ratio",
    "avg_moderate_ratio",
    "max_severe_segments",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
]

_FEATURE_SQL = """
    SELECT
        route_id,
        hour_utc,
        avg_heavy_ratio,
        COALESCE(avg_moderate_ratio, 0.0)                               AS avg_moderate_ratio,
        CAST(COALESCE(max_severe_segments, 0) AS DOUBLE)                AS max_severe_segments,
        SIN(2 * PI() * CAST(EXTRACT(HOUR FROM hour_utc) AS DOUBLE) / 24.0) AS hour_sin,
        COS(2 * PI() * CAST(EXTRACT(HOUR FROM hour_utc) AS DOUBLE) / 24.0) AS hour_cos,
        SIN(2 * PI() * CAST(EXTRACT(DOW  FROM hour_utc) AS DOUBLE) / 7.0)  AS dow_sin,
        COS(2 * PI() * CAST(EXTRACT(DOW  FROM hour_utc) AS DOUBLE) / 7.0)  AS dow_cos
    FROM gold
    WHERE avg_heavy_ratio    IS NOT NULL
      AND avg_moderate_ratio IS NOT NULL
"""


def _scan_to_table(scan_result: object) -> pa.Table:
    if isinstance(scan_result, pa.Table):
        return scan_result
    return scan_result.read_all()  # type: ignore[union-attr]


def build_features(
    catalog: Catalog,
    route_id: str | None = None,
) -> pa.Table | None:
    """Build cyclical time-encoded feature table from gold hourly data.

    Returns Arrow table with columns: route_id, hour_utc, + FEATURE_COLUMNS.
    Returns None if the gold table is missing or empty.
    """
    try:
        gold_table = catalog.load_table(_GOLD_TABLE)
    except NoSuchTableError as exc:
        logger.warning("build_features: table not found — %s", exc)
        return None

    gold_arrow = _scan_to_table(gold_table.scan().to_arrow())
    if gold_arrow.num_rows == 0:
        logger.info("build_features: no gold data yet — skipping")
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
        result.num_rows, len(FEATURE_COLUMNS),
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


def cyclical_time_features(hour: int, dow: int) -> tuple[float, float, float, float]:
    """Compute (hour_sin, hour_cos, dow_sin, dow_cos) for a given hour and day-of-week.

    dow follows SQL convention: Sunday=0 … Saturday=6.
    Used by the serving layer to build the same feature vector as at training time.
    """
    hour_sin = math.sin(2 * math.pi * hour / 24)
    hour_cos = math.cos(2 * math.pi * hour / 24)
    dow_sin  = math.sin(2 * math.pi * dow  / 7)
    dow_cos  = math.cos(2 * math.pi * dow  / 7)
    return hour_sin, hour_cos, dow_sin, dow_cos
