"""Router: traffic metrics endpoints."""

from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

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
    "Use HCMC domain knowledge only to explain causes and give recommendations. "
    "CRITICAL — signal definitions: "
    "Z-score (duration_zscore) is the ONLY numerical score in this data. "
    "IsolationForest (IF) is a BINARY signal — it is either 'flagged' or 'not flagged', never a number. "
    "NEVER write 'IF: <number>' or assign any numerical value to IF. "
    "When referencing IF, write 'IF flagged', 'IF anomaly detected', or 'both signals confirmed' — nothing else. "
    "If you see 'iforest_anomaly: true', say 'IsolationForest flagged this route'. "
    "Do not invent, estimate, or approximate any IF score."
)

_DOW_VI = ["Chủ nhật", "Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy"]
_DOW_EN = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
_HCMC_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

_SECTIONS = {
    "vi": [
        "1. **Mẫu bất thường đa tín hiệu** — tuyến nào, giờ nào (UTC+7) được xác nhận bởi CẢ HAI tín hiệu (IF flagged VÀ Z-score cao); giờ nào có nhiều tuyến cùng bất thường.",
        "2. **Nguyên nhân gốc rễ** — giải thích tại sao khoảng giờ đó trên tuyến đó lại bất thường, liên kết với đặc điểm giao thông TP.HCM (peak sáng/chiều, KCN, cảng, cuối tuần vs ngày thường).",
        "3. **Tín hiệu đơn lẻ** — Z-score-only hoặc IF-only: dùng Z-score (số) để định lượng, chỉ nói 'IF flagged/không flagged' — không dùng số cho IF. Phân tích tín hiệu này chỉ bất thường đang hình thành hay tan dần.",
        "4. **Khuyến nghị theo khung giờ** — mỗi khuyến nghị PHẢI gắn với giờ cụ thể (VD: '07:00–09:00'), ngày trong tuần nếu có, và tuyến cụ thể. Ưu tiên các giờ tương quan cao (≥3 tuyến cùng lúc).",
    ],
    "en": [
        "1. **Dual-signal anomaly patterns** — which routes and hours (UTC+7) had BOTH signals confirmed (IF flagged AND elevated Z-score); identify hours where multiple routes were congested simultaneously.",
        "2. **Root causes** — explain WHY that time window on that corridor was anomalous, linking to HCMC traffic patterns (morning/evening peak, industrial zones, port logistics, weekday vs weekend).",
        "3. **Single-signal notes** — Z-score-only or IF-only cases: quantify with Z-score values only; describe IF as 'flagged' or 'not flagged' — never a number. Indicate whether this suggests emerging or dissipating congestion.",
        "4. **Time-anchored recommendations** — every recommendation MUST specify an exact hour range (e.g. '07:00–09:00 UTC+7'), day-of-week where relevant, and the specific route. Prioritise correlated hours (≥3 routes simultaneously).",
    ],
}

_PEAK_HOURS_NOTE = {
    "vi": "Giờ cao điểm điển hình TP.HCM: sáng 07:00–09:00 UTC+7, chiều 17:00–19:00 UTC+7. KCN/cảng: sớm 05:00–08:00.",
    "en": "Typical HCMC peak hours: morning 07:00–09:00 UTC+7, evening 17:00–19:00 UTC+7. Industrial/port: early 05:00–08:00.",
}

_LANG_INSTRUCTIONS = {
    "vi": "QUAN TRỌNG: Toàn bộ phân tích phải viết bằng tiếng Việt. Tuyệt đối không dùng tiếng Anh dù chỉ một từ.",
    "en": "IMPORTANT: Write the entire analysis in English.",
}


class HeatmapAnalyzeRequest(BaseModel):
    context: str = Field(..., min_length=10, max_length=6000)
    lang: str = Field(default="en", pattern="^(vi|en)$")
    route_ids: list[str] = Field(default_factory=list, max_length=4)
    window_from: Optional[str] = Field(default=None, description="ISO datetime — start of analysis window")
    window_to: Optional[str] = Field(default=None, description="ISO datetime — end of analysis window")


def _format_window_header(lang: str, window_from: str | None, window_to: str | None) -> str:
    """Build a time-context block: current HCMC time + analysis window with day-of-week."""
    now_hcmc = datetime.now(_HCMC_TZ)
    dow_names = _DOW_VI if lang == "vi" else _DOW_EN
    now_str = f"{dow_names[now_hcmc.weekday() + 1 if now_hcmc.weekday() < 6 else 0]}, {now_hcmc.strftime('%Y-%m-%d %H:%M')} UTC+7"

    lines = [
        "=== TIME CONTEXT ===",
        f"Current HCMC time: {now_str}",
    ]

    if window_from or window_to:
        try:
            frm = datetime.fromisoformat(window_from).astimezone(_HCMC_TZ) if window_from else None
            to_ = datetime.fromisoformat(window_to).astimezone(_HCMC_TZ) if window_to else None
            frm_str = f"{dow_names[frm.weekday() + 1 if frm.weekday() < 6 else 0]} {frm.strftime('%Y-%m-%d %H:%M')}" if frm else "?"
            to_str  = f"{dow_names[to_.weekday() + 1 if to_.weekday() < 6 else 0]} {to_.strftime('%Y-%m-%d %H:%M')}" if to_ else "?"
            span_h = int((to_ - frm).total_seconds() / 3600) if (frm and to_) else None
            span_note = f" ({span_h}h window)" if span_h else ""
            lines.append(f"Analysis window (UTC+7): {frm_str} → {to_str}{span_note}")
        except Exception:
            pass

    lines.append(_PEAK_HOURS_NOTE.get(lang, _PEAK_HOURS_NOTE["en"]))
    return "\n".join(lines)


def _build_analyze_prompt(
    context: str,
    lang: str,
    external: str,
    window_from: str | None = None,
    window_to: str | None = None,
) -> str:
    lang_note = _LANG_INSTRUCTIONS.get(lang, _LANG_INSTRUCTIONS["en"])
    sections = "\n".join(_SECTIONS.get(lang, _SECTIONS["en"]))
    concise = (
        "3–4 câu mỗi mục. Không chào hỏi. Mục 4 phải có giờ cụ thể (UTC+7) cho mỗi khuyến nghị."
        if lang == "vi"
        else "3–4 sentences per section. No greetings. Section 4 must include an explicit hour range (UTC+7) for every recommendation."
    )
    time_header = _format_window_header(lang, window_from, window_to)
    parts = [
        lang_note,
        time_header,
        f"Provide analysis in 4 sections:\n{sections}\n{concise}",
        f"=== HEATMAP DATA ===\n{context}",
    ]
    if external:
        parts.append(external)
    parts.append(lang_note)
    return "\n\n".join(parts)


@router.post("/analyze")
async def heatmap_analyze(req: HeatmapAnalyzeRequest) -> StreamingResponse:
    """Stream LLM analysis of heatmap data. All prompt engineering is server-side."""
    weather = await fetch_current_weather()
    external = await fetch_heatmap_external_context(req.route_ids, weather)

    lang_note = _LANG_INSTRUCTIONS.get(req.lang, _LANG_INSTRUCTIONS["en"])
    system = f"{_ANALYZE_SYSTEM_BASE} {lang_note}"
    user_prompt = _build_analyze_prompt(
        req.context, req.lang, external, req.window_from, req.window_to
    )
    return StreamingResponse(
        stream_ollama(system, user_prompt, temperature=0.3),
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
