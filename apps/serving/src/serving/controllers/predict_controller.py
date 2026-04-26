"""Predict controller — orchestrates feature fetch + IForest scoring."""

from typing import Any

import asyncpg

from serving.repo import online as online_repo
from serving.services import prediction_service


async def predict_all(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await online_repo.fetch_latest_for_scoring(conn)
    if not rows:
        return []
    results = prediction_service.score_rows(rows)
    return [
        {
            "route_id": r.route_id,
            "iforest_score": r.iforest_score,
            "iforest_anomaly": r.iforest_anomaly,
            "zscore_anomaly": r.zscore_anomaly,
            "both_anomaly": r.both_anomaly,
        }
        for r in results
    ]


async def predict_route(conn: asyncpg.Connection, route_id: str) -> dict[str, Any] | None:
    row = await online_repo.fetch_route_for_scoring(conn, route_id)
    if row is None:
        return None
    results = prediction_service.score_rows([row])
    r = results[0]
    return {
        "route_id": r.route_id,
        "iforest_score": r.iforest_score,
        "iforest_anomaly": r.iforest_anomaly,
        "zscore_anomaly": r.zscore_anomaly,
        "both_anomaly": r.both_anomaly,
    }
