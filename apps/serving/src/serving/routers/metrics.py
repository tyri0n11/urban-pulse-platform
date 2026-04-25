"""Router: traffic metrics endpoints."""

from datetime import datetime
from typing import Any, Optional

import asyncpg
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from serving.controllers.metrics_controller import get_leaderboard, fetch_heatmap_external_context
from serving.dependencies import get_db
from serving.repo import metrics as metrics_repo
from serving.services.llm_service import stream_ollama
from serving.utils.weather import fetch_current_weather

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/routes")
async def route_metrics(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await metrics_repo.fetch_route_metrics(conn)


@router.get("/routes/{route_id}")
async def route_duration_trend(
    route_id: str,
    hours: int = Query(default=24, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await metrics_repo.fetch_route_trend(conn, route_id, hours)


@router.get("/zones")
async def zone_metrics(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await metrics_repo.fetch_zone_metrics(conn)


@router.get("/leaderboard")
async def congestion_leaderboard(
    limit: int = Query(default=10, ge=1, le=50),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await get_leaderboard(conn, limit)


_ANALYZE_SYSTEM_BASE = (
    "You are a traffic analyst for Ho Chi Minh City (HCMC), Vietnam. "
    "You will be given structured heatmap data from a real-time traffic monitoring system. "
    "Analyze ONLY the data provided — never invent route names, hours, or statistics. "
    "Routes are labeled as 'Zone X → Zone Y'. Always write 'Zone' (never 'Zona'). "
    "Use HCMC domain knowledge only to explain causes and give recommendations."
)

_SECTIONS = {
    "vi": [
        "1. **Mẫu bất thường đa tín hiệu** — các tuyến và giờ cụ thể có bất thường xác nhận (cả IF lẫn Z-score); các giờ nhiều tuyến cùng bất thường đồng thời.",
        "2. **Nguyên nhân gốc rễ** — dựa trên đặc điểm giao thông TP.HCM (giờ cao điểm, hành lang công nghiệp, logistics cảng, địa lý vùng).",
        "3. **Tín hiệu đơn lẻ** — Z-score hoặc IF đơn lẻ cho thấy tắc nghẽn đang hình thành hay tan dần.",
        "4. **Khuyến nghị** — hành động cụ thể cho các giờ và tuyến được xác định.",
    ],
    "en": [
        "1. **Dual-signal anomaly patterns** — which routes and hours had confirmed anomalies (both IF and Z-score); identify hours where multiple routes were congested simultaneously.",
        "2. **Root causes** — based on HCMC traffic knowledge (rush hours, industrial corridors, port logistics, zone geography).",
        "3. **Single-signal notes** — whether Z-score-only or IF-only signals suggest emerging or dissipating congestion.",
        "4. **Recommendations** — specific actions for the identified peak hours and routes.",
    ],
}

_LANG_INSTRUCTIONS = {
    "vi": "QUAN TRỌNG: Toàn bộ phân tích phải viết bằng tiếng Việt. Tuyệt đối không dùng tiếng Anh dù chỉ một từ.",
    "en": "IMPORTANT: Write the entire analysis in English.",
}


class HeatmapAnalyzeRequest(BaseModel):
    context: str = Field(..., min_length=10, max_length=6000)
    lang: str = Field(default="en", pattern="^(vi|en)$")
    route_ids: list[str] = Field(default_factory=list, max_length=4)


def _build_analyze_prompt(context: str, lang: str, external: str) -> str:
    lang_note = _LANG_INSTRUCTIONS.get(lang, _LANG_INSTRUCTIONS["en"])
    sections = "\n".join(_SECTIONS.get(lang, _SECTIONS["en"]))
    concise = "3–4 câu mỗi mục. Không chào hỏi." if lang == "vi" else "3–4 sentences per section. No greetings."
    parts = [
        lang_note,
        f"\nProvide analysis in 4 sections:\n{sections}\n{concise}",
        f"\n=== HEATMAP DATA ===\n{context}",
    ]
    if external:
        parts.append(external)
    parts.append(f"\n{lang_note}")
    return "\n\n".join(parts)


@router.post("/analyze")
async def heatmap_analyze(req: HeatmapAnalyzeRequest) -> StreamingResponse:
    """Stream LLM analysis of heatmap data. All prompt engineering is server-side."""
    weather = await fetch_current_weather()
    external = await fetch_heatmap_external_context(req.route_ids, weather)

    lang_note = _LANG_INSTRUCTIONS.get(req.lang, _LANG_INSTRUCTIONS["en"])
    system = f"{_ANALYZE_SYSTEM_BASE} {lang_note}"
    user_prompt = _build_analyze_prompt(req.context, req.lang, external)
    return StreamingResponse(
        stream_ollama(system, user_prompt, temperature=0.3, num_predict=700),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/heatmap")
async def zscore_heatmap(
    hours: int = Query(default=24, ge=1, le=720),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    if start is not None and end is not None:
        return await metrics_repo.fetch_heatmap_range(conn, start, end)
    return await metrics_repo.fetch_heatmap_hours(conn, hours)
