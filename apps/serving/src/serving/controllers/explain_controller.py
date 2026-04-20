"""Explain controller — fetches route data, RAG context, and weather for LLM explanation."""

from datetime import datetime, timezone
from typing import Any

import asyncpg
from fastapi import HTTPException

from serving.repo import online as online_repo
from serving.services import prediction_service
from serving.utils.routes import load_routes_coords
from serving.utils.weather import fetch_current_weather


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
        ts = window_start.astimezone(timezone.utc)
        hour, dow = ts.hour, (ts.weekday() + 1) % 7
    else:
        now = datetime.now(timezone.utc)
        hour, dow = now.hour, (now.weekday() + 1) % 7

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

    lang_instruction = "Trả lời bằng tiếng Việt." if lang == "vi" else "Respond in English."

    parts = [
        f"=== DỮ LIỆU TẮC NGHẼN BẤT THƯỜNG ===\n"
        f"Tuyến: {origin} → {dest}\n"
        f"Tỉ lệ đoạn TẮC NẶNG (heavy_ratio): {heavy:.1%}\n"
        f"Tỉ lệ đoạn CHẬM VỪA (moderate_ratio): {moderate:.1%}\n"
        f"Tỉ lệ đoạn ÍT TẮC (low_ratio): {low:.1%}\n"
        f"Đoạn NGHIÊM TRỌNG NHẤT (severe_segments): {severe}\n"
        f"Số quan sát trong cửa sổ: {obs}\n"
        f"Tín hiệu bất thường: {', '.join(signal) if signal else 'none'}"
    ]

    if weather:
        temp = weather.get("temperature_c")
        rain = weather.get("rain_mm") or weather.get("precipitation_mm") or 0.0
        wind = weather.get("wind_speed_kmh")
        desc = weather.get("weather_desc", "")
        parts.append(
            f"\n=== THỜI TIẾT HIỆN TẠI TP.HCM (Open-Meteo) ===\n"
            f"Điều kiện: {desc}"
            + (f", {temp:.1f}°C" if temp is not None else "")
            + (f", mưa {rain:.1f} mm" if float(rain) > 0 else ", không mưa")
            + (f", gió {wind:.1f} km/h" if wind is not None else "")
        )

    if rag_context:
        parts.append(f"\n{rag_context}")

    parts.append(
        f"\n{lang_instruction}\n"
        f"Dựa vào dữ liệu tắc nghẽn, thời tiết hiện tại, mẫu giao thông lịch sử "
        f"và context ở trên (nếu có), hãy giải thích trong 2–3 câu tại sao tuyến "
        f"đường này đang bất thường — đề cập cụ thể đến thời tiết nếu có khả năng liên quan:"
    )
    return "\n".join(parts)
