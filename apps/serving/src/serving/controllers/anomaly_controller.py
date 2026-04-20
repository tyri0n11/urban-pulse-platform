"""Anomaly controller — orchestrates repo queries + IForest scoring."""

from typing import Any

import asyncpg

from serving.repo import anomalies as anomaly_repo
from serving.repo import online as online_repo
from serving.services import prediction_service


async def get_current_anomalies(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """Latest features scored by both signals; returns only anomalous routes."""
    rows = await online_repo.fetch_latest_for_anomaly_check(conn)
    if not rows:
        return []

    try:
        predictions = prediction_service.score_rows(rows)
        pred_by_route = {p.route_id: p for p in predictions}
    except Exception:
        pred_by_route = {}

    result = []
    for row in rows:
        rid = row["route_id"]
        pred = pred_by_route.get(rid)
        is_zscore = bool(row.get("is_anomaly", False))
        is_iforest = bool(pred.iforest_anomaly) if pred else False
        zscore_val = float(row.get("duration_zscore") or 0)

        # Skip iforest-only anomalies with negative z-score — route is flowing
        # better than normal; IForest flags unusual patterns but it's not congestion.
        if not is_zscore and is_iforest and zscore_val < 0:
            continue
        if not (is_zscore or is_iforest):
            continue

        entry = {**row}
        if pred:
            entry["iforest_anomaly"] = pred.iforest_anomaly
            entry["iforest_score"] = pred.iforest_score
            entry["both_anomaly"] = pred.both_anomaly
        result.append(entry)

    result.sort(key=lambda x: (
        not x.get("both_anomaly", False),
        not x.get("is_anomaly", False),
        -abs(x.get("duration_zscore") or 0),
    ))
    return result
