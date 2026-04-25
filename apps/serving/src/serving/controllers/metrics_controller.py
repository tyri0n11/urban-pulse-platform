"""Metrics controller — applies business rules on top of raw repo data."""

import logging
from datetime import datetime
from typing import Any

import asyncpg
from zoneinfo import ZoneInfo

from serving.repo import metrics as metrics_repo

logger = logging.getLogger(__name__)

_HCMC_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


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


async def fetch_heatmap_external_context(
    route_ids: list[str],
    weather: dict[str, Any] | None,
) -> str:
    """Fetch weather + RAG context for heatmap analysis.

    Retrieves:
    - Current weather formatted as a prompt section
    - 1 anomaly event + 1 traffic pattern per top route (up to 3 routes)
    - 2 recent external_context (weather history) chunks
    """
    sections: list[str] = []

    # --- Current weather ---
    if weather:
        temp = weather.get("temperature_c")
        rain = weather.get("rain_mm") or weather.get("precipitation_mm") or 0.0
        wind = weather.get("wind_speed_kmh")
        desc = weather.get("weather_desc", "")
        w_line = (
            f"Condition: {desc}"
            + (f", {temp:.1f}°C" if temp is not None else "")
            + (f", rain {float(rain):.1f} mm" if float(rain) > 0 else ", no rain")
            + (f", wind {wind:.1f} km/h" if wind is not None else "")
        )
        sections.append(f"=== CURRENT WEATHER (HCMC) ===\n{w_line}")

    # --- RAG context for top anomalous routes ---
    if route_ids:
        try:
            from rag.client import get_chroma_client
            from rag.retriever import retrieve_for_route, format_chunks_for_prompt

            now_local = datetime.now(_HCMC_TZ)
            hour, dow = now_local.hour, (now_local.weekday() + 1) % 7

            chroma = get_chroma_client()
            all_chunks = []
            for route_id in route_ids[:3]:
                chunks = retrieve_for_route(
                    chroma, route_id, hour=hour, dow=dow,
                    n_anomaly=1, n_pattern=1, n_external=1,
                )
                all_chunks.extend(chunks)

            if all_chunks:
                # Deduplicate by text, sort by score
                seen: set[str] = set()
                unique = []
                for c in sorted(all_chunks, key=lambda x: x.score):
                    if c.text not in seen:
                        seen.add(c.text)
                        unique.append(c)
                sections.append(format_chunks_for_prompt(unique[:6]))
        except Exception as exc:
            logger.debug("fetch_heatmap_external_context: RAG failed — %s", exc)

    return "\n\n".join(sections)
