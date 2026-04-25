"""Explain controller — fetches route data, RAG context, and weather for LLM explanation."""

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg
from fastapi import HTTPException

from serving.repo import online as online_repo
from serving.services import prediction_service
from serving.utils.routes import load_routes_coords

_HCMC_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


async def fetch_route_data(route_id: str, conn: asyncpg.Connection) -> dict[str, Any]:
    """Fetch latest features + IForest score + route metadata for the explain endpoint."""
    row = await online_repo.fetch_for_explain(conn, route_id)
    if row is None:
        raise HTTPException(404, f"No data for route '{route_id}'")

    coords = load_routes_coords()
    meta = coords.get(route_id, {})
    row["origin"] = meta.get("origin", route_id)
    row["destination"] = meta.get("destination", "")

    try:
        preds = prediction_service.score_rows([row])
        if preds:
            row["iforest_anomaly"] = preds[0].iforest_anomaly and preds[0].iforest_score > 0
    except Exception:
        row["iforest_anomaly"] = False

    return row


async def fetch_rag_context(
    route_id: str, row: dict[str, Any]
) -> tuple[str, list[Any]]:
    """Retrieve RAG chunks for a route; returns (formatted_context, chunks)."""
    from rag.client import get_chroma_client
    from rag.retriever import retrieve_for_route, format_chunks_for_prompt

    window_start = row.get("updated_at")
    if window_start and hasattr(window_start, "astimezone"):
        ts_local = window_start.astimezone(_HCMC_TZ)
        hour, dow = ts_local.hour, (ts_local.weekday() + 1) % 7
    else:
        now_local = datetime.now(_HCMC_TZ)
        hour, dow = now_local.hour, (now_local.weekday() + 1) % 7

    try:
        chroma = get_chroma_client()
        chunks = retrieve_for_route(chroma, route_id, hour=hour, dow=dow)
        return format_chunks_for_prompt(chunks), chunks
    except Exception:
        return "", []


def build_explain_prompt(
    row: dict[str, Any],
    lang: str,
    rag_context: str = "",
    weather: dict[str, Any] | None = None,
) -> str:
    heavy = row.get("mean_heavy_ratio") or 0.0
    moderate = row.get("mean_moderate_ratio") or 0.0
    low = row.get("mean_low_ratio") or 0.0
    severe = row.get("max_severe_segments") or 0
    obs = row.get("observation_count") or 0
    is_z = row.get("is_anomaly", False)
    is_if = row.get("iforest_anomaly", False)
    origin = row.get("origin") or row.get("route_id", "unknown")
    dest = row.get("destination") or ""

    signal = []
    if is_z and is_if:
        signal.append("BOTH Z-Score lẫn IsolationForest (độ tin cậy cao nhất)")
    elif is_z:
        signal.append("Z-Score (heavy_ratio bất thường so với baseline lịch sử)")
    elif is_if:
        signal.append("IsolationForest (cấu trúc congestion bất thường đa chiều)")

    lang_note = (
        "QUAN TRỌNG: Toàn bộ phân tích phải bằng tiếng Việt. Tuyệt đối không dùng tiếng Anh."
        if lang == "vi"
        else "IMPORTANT: Write the entire analysis in English."
    )

    section_labels = (
        ("### Quan sát", "### Nguyên nhân", "### Đánh giá")
        if lang == "vi"
        else ("### Observation", "### Root Cause", "### Assessment")
    )

    parts = [
        lang_note,
        "",
        "=== ANOMALY DATA ===",
        f"Route: {origin} → {dest}",
        f"Heavy congestion ratio (heavy_ratio): {heavy:.1%}",
        f"Moderate congestion ratio (moderate_ratio): {moderate:.1%}",
        f"Low congestion ratio (low_ratio): {low:.1%}",
        f"Max severe segments: {severe}",
        f"Observations in window: {obs}",
        f"Anomaly signal: {', '.join(signal) if signal else 'none'}",
    ]

    if weather:
        temp = weather.get("temperature_c")
        rain = weather.get("rain_mm") or weather.get("precipitation_mm") or 0.0
        wind = weather.get("wind_speed_kmh")
        desc = weather.get("weather_desc", "")
        parts += [
            "",
            "=== CURRENT WEATHER (HCMC) ===",
            f"Condition: {desc}"
            + (f", {temp:.1f}°C" if temp is not None else "")
            + (f", rain {rain:.1f} mm" if float(rain) > 0 else ", no rain")
            + (f", wind {wind:.1f} km/h" if wind is not None else ""),
        ]

    if rag_context:
        parts += ["", rag_context]

    parts += [
        "",
        "Write exactly 3 sections using these ### headings in order:",
        f"{section_labels[0]} — what specifically is anomalous: signal type, numbers vs baseline, severity.",
        f"{section_labels[1]} — why this is happening: traffic patterns, time-of-day, HCMC geography, weather if relevant.",
        f"{section_labels[2]} — severity level and one concrete recommendation.",
        f"2–3 sentences per section. {lang_note}",
    ]
    return "\n".join(parts)
