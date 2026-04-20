"""Chat controller — assembles live system snapshot and user prompts for LLM."""

import re
from typing import Any

import asyncpg

from serving.repo import online as online_repo
from serving.services import prediction_service
from serving.utils.weather import fetch_current_weather


def _short_name(route_id: str) -> str:
    parts = route_id.split("_to_")
    if len(parts) != 2:
        return route_id

    def clean(s: str) -> str:
        return re.sub(r"^zone\d+_", "", s).replace("_", " ").title()

    return f"{clean(parts[0])} → {clean(parts[1])}"


async def fetch_system_snapshot(conn: asyncpg.Connection) -> dict[str, Any]:
    """Build a concise current-state summary from Postgres + live weather."""
    row_list = await online_repo.fetch_for_chat_snapshot(conn)

    if not row_list:
        return {"total_routes": 0, "anomalies": [], "top_congested": [], "avg_lag_ms": 0}

    iforest_by_route: dict[str, bool] = {}
    try:
        preds = prediction_service.score_rows(row_list)
        iforest_by_route = {p.route_id: p.iforest_anomaly and p.iforest_score > 0 for p in preds}
    except Exception:
        pass

    def _signal(row: dict[str, Any]) -> str:
        z = row.get("is_anomaly", False)
        i = iforest_by_route.get(row["route_id"], False)
        if z and i:
            return "BOTH"
        if z:
            return "ZSCORE"
        if i:
            return "IFOREST"
        return "normal"

    anomalies = []
    for row in row_list:
        sig = _signal(row)
        if sig != "normal":
            anomalies.append({
                "route": _short_name(row["route_id"]),
                "signal": sig,
                "heavy_pct": round((row["mean_heavy_ratio"] or 0) * 100, 1),
                "moderate_pct": round((row["mean_moderate_ratio"] or 0) * 100, 1),
                "severe_seg": int(row["max_severe_segments"] or 0),
            })

    anomalies.sort(key=lambda x: x["heavy_pct"], reverse=True)

    top_congested = sorted(
        row_list, key=lambda r: r["mean_heavy_ratio"] or 0, reverse=True
    )[:5]

    avg_lag = int(
        sum(r["last_ingest_lag_ms"] or 0 for r in row_list) / max(len(row_list), 1)
    )

    latest_ts = max(
        (r["updated_at"] for r in row_list if r["updated_at"]), default=None
    )

    weather = await fetch_current_weather()

    return {
        "total_routes": len(row_list),
        "anomaly_count": len(anomalies),
        "anomalies": anomalies[:8],
        "top_congested": [
            {
                "route": _short_name(r["route_id"]),
                "heavy_pct": round((r["mean_heavy_ratio"] or 0) * 100, 1),
                "moderate_pct": round((r["mean_moderate_ratio"] or 0) * 100, 1),
            }
            for r in top_congested
        ],
        "avg_lag_ms": avg_lag,
        "snapshot_time": latest_ts.strftime("%Y-%m-%d %H:%M:%S UTC") if latest_ts else "unknown",
        "weather": weather,
    }


def build_user_prompt(snapshot: dict[str, Any], message: str, lang: str) -> str:
    """User-turn prompt: live snapshot + question. Geo context lives in system prompt."""
    anomaly_count = snapshot["anomaly_count"]
    total = snapshot["total_routes"]
    avg_lag = snapshot["avg_lag_ms"]
    snapshot_time = snapshot.get("snapshot_time", "unknown")

    lines = [
        "=== LIVE SNAPSHOT (real-time congestion) ===",
        f"Thời điểm snapshot: {snapshot_time}",
        f"Đang giám sát {total} tuyến · Độ trễ cảm biến TB: {avg_lag} ms",
        f"Bất thường hiện tại: {anomaly_count}/{total} tuyến",
        "Ngưỡng tham chiếu: heavy_ratio > ~15% = tắc nghẽn nặng, < 5% = thông thoáng bình thường",
        "",
    ]

    if snapshot["anomalies"]:
        lines.append("Tuyến bất thường (sắp xếp theo heavy_ratio cao nhất):")
        for a in snapshot["anomalies"]:
            lines.append(
                f"  [{a['signal']}] {a['route']}: "
                f"heavy={a['heavy_pct']:.1f}%, moderate={a['moderate_pct']:.1f}%, "
                f"severe_segments={a['severe_seg']}"
            )
    else:
        lines.append("Không có bất thường tại thời điểm này.")

    if snapshot["top_congested"]:
        lines.append("")
        lines.append("Top tắc nghẽn (heavy_ratio cao nhất):")
        for r in snapshot["top_congested"]:
            lines.append(
                f"  {r['route']}: heavy={r['heavy_pct']:.1f}%, moderate={r['moderate_pct']:.1f}%"
            )

    weather = snapshot.get("weather")
    if weather:
        temp = weather.get("temperature_c")
        rain = weather.get("rain_mm") or weather.get("precipitation_mm") or 0.0
        wind = weather.get("wind_speed_kmh")
        cloud = weather.get("cloud_cover_pct")
        desc = weather.get("weather_desc", "")
        lines.append("")
        lines.append("Thời tiết hiện tại TP.HCM (Open-Meteo):")
        lines.append(
            f"  {desc}"
            + (f", {temp:.1f}°C" if temp is not None else "")
            + (f", mưa {rain:.1f} mm" if rain > 0 else ", không mưa")
            + (f", gió {wind:.1f} km/h" if wind is not None else "")
            + (f", mây {cloud:.0f}%" if cloud is not None else "")
        )

    lang_note = "Trả lời bằng tiếng Việt." if lang == "vi" else "Respond in English."
    lines += ["", "=== END SNAPSHOT ===", "", lang_note, "", f"Câu hỏi: {message}"]

    return "\n".join(lines)
