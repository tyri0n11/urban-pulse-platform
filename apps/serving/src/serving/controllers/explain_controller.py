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
            row["iforest_anomaly"] = preds[0].iforest_anomaly
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


def _anomaly_direction(is_z: bool, is_if: bool, zscore: float) -> tuple[str, str]:
    """Return (direction_tag, plain_description) for the anomaly."""
    if is_z:
        # Z-score is one-sided (only fires on HIGH heavy_ratio) → always congestion
        return "HIGHER_THAN_NORMAL", "congestion spike — heavy_ratio significantly above baseline"
    if is_if:
        if zscore < 0:
            return "LOWER_THAN_NORMAL", "unusually free-flowing — heavy_ratio significantly below baseline"
        return "HIGHER_THAN_NORMAL", "unusual congestion pattern detected by IsolationForest"
    return "UNKNOWN", "anomaly detected"


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
    zscore = float(row.get("duration_zscore") or 0.0)
    is_z = row.get("is_anomaly", False)
    is_if = row.get("iforest_anomaly", False)
    origin = row.get("origin") or row.get("route_id", "unknown")
    dest = row.get("destination") or ""

    # Convert window_start / updated_at to HCMC local time for display
    ts_raw = row.get("window_start") or row.get("updated_at")
    if ts_raw and hasattr(ts_raw, "astimezone"):
        ts_local = ts_raw.astimezone(_HCMC_TZ)
        ts_str = ts_local.strftime("%H:%M %Z %A")
    else:
        ts_str = "unknown"

    direction_tag, direction_desc = _anomaly_direction(is_z, is_if, zscore)

    signal_parts = []
    if is_z and is_if:
        signal_parts.append("BOTH Z-Score and IsolationForest (highest confidence)")
    elif is_z:
        signal_parts.append("Z-Score only")
    elif is_if:
        signal_parts.append("IsolationForest only")
    signal_str = signal_parts[0] if signal_parts else "none"

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

    direction_warning = (
        "CRITICAL — ANOMALY DIRECTION: "
        + (
            f"direction={direction_tag}. {direction_desc}. "
            f"heavy_ratio={heavy:.1%}, zscore={zscore:+.2f}. "
            + (
                "The route is LESS congested than normal — DO NOT describe this as congestion or traffic jam. "
                "Describe it as unusually free-flowing or lighter-than-normal traffic."
                if direction_tag == "LOWER_THAN_NORMAL"
                else "The route is MORE congested than normal."
            )
        )
    )

    parts = [
        lang_note,
        "",
        direction_warning,
        "",
        "=== ANOMALY DATA ===",
        f"Route: {origin} → {dest}",
        f"Observation time (HCMC local): {ts_str}",
        f"Anomaly signal: {signal_str}",
        f"Anomaly direction: {direction_tag} — {direction_desc}",
        f"Heavy congestion ratio (heavy_ratio): {heavy:.1%}",
        f"Moderate congestion ratio (moderate_ratio): {moderate:.1%}",
        f"Low congestion ratio (low_ratio): {low:.1%}",
        f"Max severe segments: {severe}",
        f"Heavy-ratio z-score vs baseline: {zscore:+.2f} (positive = more congested, negative = less congested)",
        f"Observations in window: {obs}",
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

    obs_instruction = (
        f"{section_labels[0]} — state the anomaly direction explicitly "
        f"({'route is unusually FREE-FLOWING, lighter traffic than normal' if direction_tag == 'LOWER_THAN_NORMAL' else 'route has HIGHER congestion than normal'}). "
        "Report exact numbers and compare to baseline from RAG context if available."
    )

    parts += [
        "",
        "Write exactly 3 sections using these ### headings in order:",
        obs_instruction,
        f"{section_labels[1]} — why this is happening: traffic patterns, time-of-day, HCMC geography, weather if relevant.",
        f"{section_labels[2]} — severity level and one concrete recommendation.",
        f"2–3 sentences per section. {lang_note}",
    ]
    return "\n".join(parts)
