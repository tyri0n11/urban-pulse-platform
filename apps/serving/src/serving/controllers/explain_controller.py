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
    zscore_threshold = row.get("zscore_threshold")
    is_z = row.get("is_anomaly", False)
    is_if = row.get("iforest_anomaly", False)
    origin = row.get("origin") or row.get("route_id", "unknown")
    dest = row.get("destination") or ""

    # Convert window_start / updated_at to HCMC local time for display
    ts_raw = row.get("window_start") or row.get("updated_at")
    if ts_raw and hasattr(ts_raw, "astimezone"):
        ts_local = ts_raw.astimezone(_HCMC_TZ)
        ts_str = ts_local.strftime("%H:%M (UTC+7 — Giờ TP.HCM) %A")
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

    if direction_tag == "LOWER_THAN_NORMAL":
        direction_warning = (
            "CRITICAL — ANOMALY DIRECTION: LOWER_THAN_NORMAL.\n"
            f"heavy_ratio={heavy:.1%} is BELOW the historical baseline (zscore={zscore:+.2f}).\n"
            "This route is unusually FREE-FLOWING — traffic is lighter than normal.\n"
            "DO NOT use words like: tắc nghẽn, ùn tắc, congestion, traffic jam, bottleneck.\n"
            "DO NOT say traffic exceeds normal or is higher than usual.\n"
            "ONLY describe this as: thông thoáng bất thường, lưu thông nhẹ hơn bình thường, free-flowing."
        )
    else:
        direction_warning = (
            "CRITICAL — ANOMALY DIRECTION: HIGHER_THAN_NORMAL.\n"
            f"heavy_ratio={heavy:.1%} is ABOVE the historical baseline (zscore={zscore:+.2f}).\n"
            "This route has MORE congestion than normal."
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
        "Z-Score anomaly threshold for this route (dynamic, from p99 of historical z-scores): "
        + (f"{zscore_threshold:.2f}" if zscore_threshold is not None else "unknown (no baseline yet)"),
        f"Observations in window: {obs}",
    ]

    if weather:
        temp = weather.get("temperature_c")
        rain = weather.get("rain_mm") or weather.get("precipitation_mm") or 0.0
        wind = weather.get("wind_speed_kmh")
        desc = weather.get("weather_desc", "")
        location = weather.get("location", "TP.HCM")
        parts += [
            "",
            f"=== CURRENT WEATHER ({location}) ===",
            f"Condition: {desc}"
            + (f", {temp:.1f}°C" if temp is not None else "")
            + (f", rain {rain:.1f} mm" if float(rain) > 0 else ", no rain")
            + (f", wind {wind:.1f} km/h" if wind is not None else ""),
        ]

    if rag_context:
        parts += ["", rag_context]

    # Repeat direction lock right before generation instructions (small LLMs lose context mid-prompt)
    if direction_tag == "LOWER_THAN_NORMAL":
        direction_lock = (
            f"REMEMBER: direction=LOWER_THAN_NORMAL. heavy_ratio={heavy:.1%} is below baseline. "
            "Do NOT write about congestion or heavy traffic in any section. "
            "The metric 'heavy_ratio' is just a field name — do NOT translate it as 'tắc nghẽn nặng'. "
            "Call it 'tỷ lệ heavy_ratio' or 'chỉ số lưu lượng nặng'."
        )
        obs_content = (
            f"Describe that tỷ lệ heavy_ratio={heavy:.1%} và zscore={zscore:+.2f} "
            "cho thấy tuyến đường thông thoáng bất thường, nhẹ hơn mức bình thường lịch sử. "
            "Only cite numbers from the data above."
        )
        cause_content = (
            "Explain why traffic is lighter than usual: "
            "time-of-day, day-of-week, weather, HCMC geography. "
            "Do NOT say traffic is heavy."
        )
        assess_content = (
            "State this is low-severity (free-flowing is positive). "
            "One brief observation or recommendation."
        )
    else:
        direction_lock = (
            f"REMEMBER: direction=HIGHER_THAN_NORMAL. heavy_ratio={heavy:.1%} is above baseline (zscore={zscore:+.2f}). "
            "The metric 'heavy_ratio' is just a field name — do NOT translate it as 'tắc nghẽn nặng'; call it 'tỷ lệ heavy_ratio'."
        )
        obs_content = (
            f"Describe that tỷ lệ heavy_ratio={heavy:.1%} và zscore={zscore:+.2f} "
            "cho thấy lưu lượng nặng cao hơn mức bình thường. Only cite numbers from the data above."
        )
        cause_content = (
            "Explain why traffic is heavier than usual: "
            "traffic patterns, time-of-day, HCMC geography, weather if relevant."
        )
        assess_content = "Severity level and one concrete recommendation."

    h0, h1, h2 = section_labels
    parts += [
        "",
        direction_lock,
        "",
        "Write exactly 3 sections. Use ONLY these headings (no extra text after the heading):",
        f"{h0}",
        f"{h1}",
        f"{h2}",
        "",
        f"Content guide for {h0}: {obs_content}",
        f"Content guide for {h1}: {cause_content}",
        f"Content guide for {h2}: {assess_content}",
        f"2–3 sentences per section. {lang_note}",
    ]
    return "\n".join(parts)
