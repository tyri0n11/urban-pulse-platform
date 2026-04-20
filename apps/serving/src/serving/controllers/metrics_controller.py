"""Metrics controller — applies business rules on top of raw repo data."""

from typing import Any

import asyncpg

from serving.repo import metrics as metrics_repo


def _clamp_zscore(row: dict[str, Any]) -> dict[str, Any]:
    z = row.get("duration_zscore")
    if z is not None:
        row["duration_zscore"] = max(-30.0, min(30.0, float(z)))
    return row


async def get_leaderboard(conn: asyncpg.Connection, limit: int) -> list[dict[str, Any]]:
    rows = await metrics_repo.fetch_leaderboard_raw(conn)
    sorted_rows = sorted(
        [_clamp_zscore(r) for r in rows],
        key=lambda x: abs(x.get("duration_zscore") or 0),
        reverse=True,
    )
    return sorted_rows[:limit]
