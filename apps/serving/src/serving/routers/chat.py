"""Router: conversational AI assistant about Urban Pulse system state.

Endpoint
--------
POST /chat
    Body: {"message": "...", "lang": "vi"}
    Fetches a live system snapshot from Postgres, injects it as context,
    and streams an LLM response via Ollama SSE.

GET /chat/context
    Returns the raw system snapshot (useful for debugging / FE preloading).
"""

import json
import logging
import os
import time
from typing import Any, AsyncGenerator

import asyncpg
import httpx
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from serving.dependencies import get_db
from serving.geo_knowledge import build_system_prompt

router = APIRouter(prefix="/chat", tags=["chat"])
logger = logging.getLogger(__name__)

_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")

# Open-Meteo — HCMC city centre, no API key required
_OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
_HCMC_LAT = 10.7757
_HCMC_LON = 106.7009
_WEATHER_CACHE_TTL = 900  # 15 min — weather changes slowly

_WMO_CODES: dict[int, str] = {
    0: "trời quang", 1: "ít mây", 2: "nửa nhiều mây", 3: "u ám",
    45: "sương mù", 48: "sương mù đóng băng",
    51: "mưa phùn nhẹ", 53: "mưa phùn vừa", 55: "mưa phùn dày",
    61: "mưa nhỏ", 63: "mưa vừa", 65: "mưa to",
    80: "mưa rào nhẹ", 81: "mưa rào vừa", 82: "mưa rào mạnh",
    95: "giông bão", 96: "giông bão kèm mưa đá", 99: "giông bão mạnh",
}

_weather_cache: tuple[float, dict[str, Any]] | None = None


async def _fetch_current_weather() -> dict[str, Any] | None:
    """Fetch current weather for HCMC from Open-Meteo with 15-min in-process cache."""
    global _weather_cache
    now = time.monotonic()
    if _weather_cache and (now - _weather_cache[0]) < _WEATHER_CACHE_TTL:
        return _weather_cache[1]

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                _OPENMETEO_URL,
                params={
                    "latitude": _HCMC_LAT,
                    "longitude": _HCMC_LON,
                    "current": ",".join([
                        "temperature_2m",
                        "precipitation",
                        "rain",
                        "wind_speed_10m",
                        "wind_direction_10m",
                        "cloud_cover",
                        "weather_code",
                    ]),
                    "timezone": "UTC",
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.debug("chat: weather fetch failed — %s", exc)
        return None

    cur = data.get("current", {})
    code = int(cur.get("weather_code") or 0)
    result: dict[str, Any] = {
        "temperature_c": cur.get("temperature_2m"),
        "precipitation_mm": cur.get("precipitation"),
        "rain_mm": cur.get("rain"),
        "wind_speed_kmh": cur.get("wind_speed_10m"),
        "cloud_cover_pct": cur.get("cloud_cover"),
        "weather_desc": _WMO_CODES.get(code, f"mã WMO={code}"),
        "observed_at": cur.get("time", ""),
    }
    _weather_cache = (now, result)
    return result

# Built once at import — grounding + geo knowledge + assistant role
_SYSTEM = build_system_prompt(
    "You are Urban Pulse AI, an intelligent assistant embedded in the Urban Pulse "
    "real-time monitoring dashboard for Ho Chi Minh City metro region. "
    "You receive a live system snapshot that includes: "
    "(1) current congestion metrics and anomaly alerts for all monitored routes, "
    "(2) current weather conditions for HCMC from Open-Meteo. "
    "Congestion is measured by heavy_ratio (% road segments heavily congested), "
    "moderate_ratio (% moderately slow), and severe_segments (worst spots). "
    "When asked about weather, use the weather data in the snapshot to answer directly — "
    "never say weather data is unavailable if it is present in the snapshot. "
    "Answer questions concisely. Reference real road names (Xa lộ Hà Nội, QL13, etc.) "
    "and HCMC district names when relevant. "
    "Do not add greetings or sign-offs. "
    "Keep answers under 4 sentences unless the user explicitly asks for more detail."
)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000)
    lang: str = Field(default="vi", pattern="^(vi|en)$")


async def _fetch_system_snapshot(conn: asyncpg.Connection) -> dict[str, Any]:
    """Fetch a concise current-state summary from Postgres + live weather."""

    # Latest features per route
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id,
            is_anomaly,
            mean_heavy_ratio,
            COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
            max_severe_segments,
            observation_count,
            last_ingest_lag_ms,
            updated_at
        FROM online_route_features
        ORDER BY route_id, updated_at DESC
        """
    )

    if not rows:
        return {"total_routes": 0, "anomalies": [], "top_congested": [], "avg_lag_ms": 0}

    row_list = [dict(r) for r in rows]

    # Run IForest scoring (best-effort)
    iforest_by_route: dict[str, bool] = {}
    try:
        from serving.services.prediction_service import score_rows
        preds = score_rows(row_list)
        iforest_by_route = {p.route_id: p.iforest_anomaly for p in preds}
    except Exception:
        pass

    # Summarise each route
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

    def _short_name(route_id: str) -> str:
        import re
        parts = route_id.split("_to_")
        if len(parts) != 2:
            return route_id
        def clean(s: str) -> str:
            return re.sub(r"^zone\d+_", "", s).replace("_", " ").title()
        return f"{clean(parts[0])} → {clean(parts[1])}"

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

    # Sort by heavy_ratio desc (most congested first)
    anomalies.sort(key=lambda x: x["heavy_pct"], reverse=True)

    top_congested = sorted(
        row_list,
        key=lambda r: r["mean_heavy_ratio"] or 0,
        reverse=True,
    )[:5]

    avg_lag = int(
        sum(r["last_ingest_lag_ms"] or 0 for r in row_list) / max(len(row_list), 1)
    )

    # Latest snapshot timestamp (most recent updated_at across all routes)
    latest_ts = max((r["updated_at"] for r in row_list if r["updated_at"]), default=None)

    weather = await _fetch_current_weather()

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


def _build_user_prompt(snapshot: dict[str, Any], message: str, lang: str) -> str:
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


async def _stream_ollama(system: str, prompt: str) -> AsyncGenerator[str, None]:
    """Stream SSE chunks using separate system field for stronger grounding."""
    payload = {
        "model": _MODEL,
        "system": system,   # Ollama treats this as persistent context
        "prompt": prompt,
        "stream": True,
        "options": {"temperature": 0.4, "num_predict": 300},
    }
    timeout = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", f"{_OLLAMA_URL}/api/generate", json=payload) as resp:
                if resp.status_code != 200:
                    yield f"data: {json.dumps({'error': f'Ollama {resp.status_code}'})}\n\n"
                    return
                async for line in resp.aiter_lines():
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    chunk = data.get("response", "")
                    if chunk:
                        yield f"data: {json.dumps({'chunk': chunk})}\n\n"
                    if data.get("done"):
                        yield f"data: {json.dumps({'done': True})}\n\n"
                        return
    except httpx.TimeoutException:
        yield f"data: {json.dumps({'error': 'LLM timeout — thử lại sau'})}\n\n"
    except httpx.HTTPError as exc:
        logger.error("chat: Ollama connection error: %s", exc)
        yield f"data: {json.dumps({'error': 'Không thể kết nối tới Ollama'})}\n\n"


@router.post("")
async def chat(
    req: ChatRequest,
    conn: asyncpg.Connection = Depends(get_db),
) -> StreamingResponse:
    """Stream a context-aware LLM response about current Urban Pulse state."""
    snapshot = await _fetch_system_snapshot(conn)
    user_prompt = _build_user_prompt(snapshot, req.message, req.lang)
    logger.info("chat: lang=%s msg_len=%d anomalies=%d", req.lang, len(req.message), snapshot["anomaly_count"])

    return StreamingResponse(
        _stream_ollama(_SYSTEM, user_prompt),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.get("/context")
async def chat_context(
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    """Return current system snapshot (for debug / FE preload)."""
    return await _fetch_system_snapshot(conn)
