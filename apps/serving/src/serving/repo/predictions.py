"""DB access for prediction_history table."""

from typing import Any

import asyncpg

from serving.utils.serializers import row_to_dict


async def fetch_history_aggregated(conn: asyncpg.Connection, hours: int) -> list[dict[str, Any]]:
    """Prediction history per route per hour — for trend charts."""
    rows = await conn.fetch(
        """
        SELECT
            route_id,
            DATE_TRUNC('hour', scored_at)      AS hour,
            COUNT(*)                            AS tick_count,
            AVG(iforest_score)                  AS avg_iforest_score,
            AVG(iforest_anomaly::int)           AS iforest_anomaly_rate,
            AVG(zscore_anomaly::int)            AS zscore_anomaly_rate,
            AVG(both_anomaly::int)              AS both_anomaly_rate,
            AVG(duration_zscore)                AS avg_duration_zscore,
            AVG(mean_duration_minutes)          AS avg_duration_minutes,
            AVG(mean_heavy_ratio)               AS avg_heavy_ratio
        FROM prediction_history
        WHERE scored_at >= NOW() - ($1 * INTERVAL '1 hour') AND avg_iforest_score > 0
        GROUP BY route_id, hour
        ORDER BY hour DESC, route_id
        """,
        hours,
    )
    return [row_to_dict(r) for r in rows]


async def fetch_route_ticks(
    conn: asyncpg.Connection, route_id: str, hours: int
) -> list[dict[str, Any]]:
    """Raw 15-second prediction ticks for a single route."""
    rows = await conn.fetch(
        """
        SELECT
            id, scored_at, window_start,
            iforest_score, iforest_anomaly, zscore_anomaly, both_anomaly,
            duration_zscore, mean_duration_minutes, mean_heavy_ratio
        FROM prediction_history
        WHERE route_id = $1
          AND scored_at >= NOW() - ($2 * INTERVAL '1 hour')
        ORDER BY scored_at DESC
        """,
        route_id,
        hours,
    )
    return [row_to_dict(r) for r in rows]
